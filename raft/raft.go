// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	// https://blog.csdn.net/weixin_42663840/article/details/100056484
	// Next: the index for next send
	// Match: confrimed index by Follower. Next - 1 - Match means number of inflight logs
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)

	// peers either from ConfState.Nodes or Config.peers
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both Config.peers and ConfState.Nodes")
		}
		peers = cs.Nodes
	}

	r := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		Lead:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// keep track of progress
	for _, node := range peers {
		r.Prs[node] = &Progress{
			Next: 1,
		}
	}

	// initilze as Follower
	r.becomeFollower(r.Term, None)

	// print information
	var nodesStrs []string
	for _, n := range peers {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	log.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	logTerm, err := r.RaftLog.Term(pr.Next - 1)
	if err != nil {
		panic(err)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   pr.Next - 1,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.isElectionTimeout() {
			r.electionElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				From:    r.id,
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Infof("raft: %x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}

	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("raft: %x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	log.Infof("raft: %x became leader at term %d", r.id, r.Term)

	// TODO: pendingConf
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType == pb.MessageType_MsgHup {
		if r.State != StateLeader {
			log.Infof("%x is starting a new election at term %d", r.id, r.Term)
			r.campaign()
		} else {
			log.Debugf("%x ignoring MsgHup because already leader", r.id)
		}
		return nil
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		log.Infof("raft: %x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		// ignore
		log.Infof("raft: %x [term: %d] received a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	// m.Term >= r.Term
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.heartbeatElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			if (r.Vote == None || r.Vote == m.From) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
				r.electionElapsed = 0
				log.Infof("raft: %x [logterm: %d, index: %d, vote: %x] voted for %x [logterm: %d, index: %d] at term %d",
					r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			} else {
				log.Infof("raft: %x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
					r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(r.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(r.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			log.Infof("raft: %x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %x",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		case pb.MessageType_MsgRequestVoteResponse:
			gr := r.poll(m.From, !m.Reject)
			log.Infof("raft: %x [q:%d] has received %d votes and %d vote rejections",
				r.id, r.quorum(), gr, len(r.votes)-gr)
			switch r.quorum() {
			case gr:
				r.becomeLeader()
				r.bcastAppend()
			case len(r.votes) - gr:
				r.becomeFollower(r.Term, None)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.bcastHeartbeat()
		case pb.MessageType_MsgHeartbeatResponse:
			if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			})
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	mlastIndex := m.Index + uint64(len(m.Entries))
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   mlastIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = m.Commit
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// rset function
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.votes = make(map[uint64]bool)
	for i := range r.Prs {
		r.Prs[i] = &Progress{
			Next: r.RaftLog.LastIndex() + 1,
		}
		if i == r.id {
			r.Prs[i].Match = r.RaftLog.LastIndex()
		}
	}
	// TODO: pendingConf
}

// isElectionTimeout returns true if r.electionElapsed is greater than the
// randomized election timeout in (electiontimeout, 2 * electiontimeout - 1).
// Otherwise, it returns false.
func (r *Raft) isElectionTimeout() bool {
	d := r.electionElapsed - r.electionTimeout
	if d < 0 {
		return false
	}
	return d > rand.Int()%r.electionTimeout
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if r.quorum() == r.poll(r.id, true) {
		r.becomeLeader()
		return
	}
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		log.Infof("raft: %x [logterm: %d, index: %d] sent vote request to %x at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), i, r.Term)

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      i,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
	}
}

func (r *Raft) poll(id uint64, v bool) (granted int) {
	if v {
		log.Infof("raft: %x received vote from %x at term %d", r.id, id, r.Term)
	} else {
		log.Infof("raft: %x received vote rejection from %x at term %d", r.id, id, r.Term)
	}

	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// bcastAppend sends RRPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
func (r *Raft) bcastAppend() {
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
}

// bcastHeartbeat sends RRPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendHeartbeat(i)
	}
}
