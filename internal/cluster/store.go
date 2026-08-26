package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	logsBucket   = []byte("logs")
	stableBucket = []byte("stable")
)

type durableStore struct{ db *bolt.DB }

func openDurableStore(path string, timeout time.Duration) (*durableStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(stableBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &durableStore{db: db}, nil
}

func (s *durableStore) Close() error { return s.db.Close() }
func logKey(index uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, index)
	return key
}
func (s *durableStore) FirstIndex() (uint64, error) {
	var v uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(logsBucket).Cursor().First()
		if k != nil {
			v = binary.BigEndian.Uint64(k)
		}
		return nil
	})
	return v, err
}
func (s *durableStore) LastIndex() (uint64, error) {
	var v uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(logsBucket).Cursor().Last()
		if k != nil {
			v = binary.BigEndian.Uint64(k)
		}
		return nil
	})
	return v, err
}
func (s *durableStore) GetLog(index uint64, target *raft.Log) error {
	return s.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(logsBucket).Get(logKey(index))
		if encoded == nil {
			return raft.ErrLogNotFound
		}
		return json.Unmarshal(encoded, target)
	})
}
func (s *durableStore) StoreLog(log *raft.Log) error { return s.StoreLogs([]*raft.Log{log}) }
func (s *durableStore) StoreLogs(logs []*raft.Log) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(logsBucket)
		for _, entry := range logs {
			if entry == nil || entry.Index == 0 {
				return fmt.Errorf("invalid Raft log")
			}
			encoded, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err = bucket.Put(logKey(entry.Index), encoded); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *durableStore) DeleteRange(min, max uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(logsBucket).Cursor()
		for k, _ := c.Seek(logKey(min)); k != nil && binary.BigEndian.Uint64(k) <= max; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *durableStore) Set(key, value []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(stableBucket).Put(key, value) })
}
func (s *durableStore) Get(key []byte) ([]byte, error) {
	var result []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(stableBucket).Get(key)
		if value == nil {
			return nil
		}
		result = append([]byte(nil), value...)
		return nil
	})
	return result, err
}
func (s *durableStore) SetUint64(key []byte, value uint64) error {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return s.Set(key, encoded)
}
func (s *durableStore) GetUint64(key []byte) (uint64, error) {
	encoded, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return 0, nil
	}
	if len(encoded) != 8 {
		return 0, fmt.Errorf("stable uint64 is malformed")
	}
	return binary.BigEndian.Uint64(encoded), nil
}

var _ raft.LogStore = (*durableStore)(nil)
var _ raft.StableStore = (*durableStore)(nil)
