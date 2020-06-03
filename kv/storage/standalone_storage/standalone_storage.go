package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

// NewStandAloneStorage create kv engine
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	os.MkdirAll(kvPath, os.ModePerm)

	kvDB := engine_util.CreateDB("kv", conf)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")

	return &StandAloneStorage{engines: engines, config: conf}
}

// Start nothing to do
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

// Stop close engines.Kv
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Kv.Close()
	return err
}

// Reader return StandAloneReader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			if err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value); err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			if err := engine_util.DeleteCF(s.engines.Kv, delete.Cf, delete.Key); err != nil {
				return err
			}
		}
	}
	return nil
}
