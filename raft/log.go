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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("storage must not be nil")
	}

	log := &RaftLog{
		storage: storage,
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex + 1

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.FirstIndex())
	if l.committed+1 > off {
		return l.slice(off, l.committed+1)
	}
	return nil
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if size := len(l.entries); size != 0 {
		return l.stabled + uint64(size) - 1
	}

	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.FirstIndex()-1 || i > l.LastIndex() {
		// valid term range [first index - 1, last index]
		return 0, nil
	}

	// return term of the entry at index i, if there is any
	if i >= l.stabled && len(l.entries) != 0 {
		last := l.stabled + uint64(len(l.entries)) - 1
		if i <= last {
			return l.entries[i-l.stabled].Term, nil
		}

		return 0, nil
	}

	return l.storage.Term(i)
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	lastTerm, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return lastTerm
}

// Entries return new entries after given index, [i, lastIndex +1)
func (l *RaftLog) Entries(i uint64) (ents []*pb.Entry) {
	if i > l.LastIndex() {
		return nil
	}

	entries := l.slice(i, l.LastIndex()+1)
	for i := range entries {
		ents = append(ents, &entries[i])
	}
	return ents
}

// allEntries returns all entries in the log.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.slice(l.FirstIndex(), l.LastIndex()+1)
}

func (l *RaftLog) slice(lo uint64, hi uint64) []pb.Entry {
	if lo > hi {
		log.Panicf("raft: invalid slice %d > %d", lo, hi)
	}

	if lo < l.FirstIndex() || hi > l.LastIndex()+1 {
		log.Panicf("raft: slice[%d,%d) out of bound [%d,%d]", lo, hi, l.FirstIndex(), l.LastIndex())
	}

	if lo == hi {
		return nil
	}

	var ents []pb.Entry
	if lo < l.stabled {
		// stabled log entries are persisted to storage
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled))
		if err != nil {
			panic(err)
		}
		ents = storedEnts
	}

	if hi > l.stabled {
		// unstabled log entries are in l.entries
		unstable := l.entries[max(lo, l.stabled)-l.stabled : hi-l.stabled]
		if len(ents) > 0 {
			ents = append([]pb.Entry{}, ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}

	return ents
}

// given (lastIndex, term) log is up-to-date if
// 		term > term of last entry log  Or
//		term == term of last entry log && index >= index of last entry log
func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lasti >= l.LastIndex())
}

// Append entries to raft log
func (l *RaftLog) Append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	after := ents[0].Index - 1
	if after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}

	switch {
	case after == l.stabled+uint64(len(l.entries))-1:
		log.Infof("raftlog: directly append the unstable entries from index %d", after+1)
		l.entries = append(l.entries, ents...)
	case after < l.stabled:
		log.Infof("raftlog: replace the unstable entries from index %d", after+1)
		l.stabled = after + 1
		l.entries = ents
	default:
		// stabled.....after.....last
		log.Infof("raftlog: truncate the unstable entries to index %d", after)
		l.entries = append([]pb.Entry{}, l.slice(l.stabled, after+1)...)
		l.entries = append(l.entries, ents...)
	}

	return l.LastIndex()
}

func (l *RaftLog) maybeCommit(index, term uint64) bool {
	iterm, err := l.Term(index)
	if err != nil {
		panic(err)
	}

	if index > l.committed && iterm == term {
		l.commitTo(index)
		return true
	}
	return false
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	lastnewi = index + uint64(len(ents))

	if l.matchTerm(index, logTerm) {
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			entries := make([]pb.Entry, 0, len(ents))
			for i := range ents {
				entries = append(entries, *ents[i])
			}
			l.Append(entries[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}

	return 0, false
}

// findConflict finds the index of the conflict.
// It returns the index of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries does
// not contains new entries, the index of the first new entry will be returned.
func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				t, _ := l.Term(ne.Index)
				log.Infof("raftlog: found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, t, ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	lterm, err := l.Term(i)
	if err != nil {
		return false
	}
	return lterm == term
}
