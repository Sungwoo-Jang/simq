package boltrepo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func TestApplyReplicatedAtomicallyStoresMutationIndexAndReplay(t *testing.T) {
	repository, err := Open(Config{Path: t.TempDir() + "/queue.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	calls := 0
	apply := func() ([]byte, error) {
		calls++
		_, err := repository.Create(queue.Queue{Name: "orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod})
		return []byte(`{"queue":"orders"}`), err
	}
	first, err := repository.ApplyReplicated(8, "request-1/1", apply)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.ApplyReplicated(9, "request-1/1", apply)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || first.Duplicate || !second.Duplicate || !bytes.Equal(first.Response, second.Response) {
		t.Fatalf("unexpected replay: calls=%d first=%+v second=%+v", calls, first, second)
	}
	if index, err := repository.AppliedRaftIndex(); err != nil || index != 9 {
		t.Fatalf("applied index = %d, %v", index, err)
	}
	if err := repository.Health(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenMigratesSchemaV6ToV7AndKeepsBackup(t *testing.T) {
	path := t.TempDir() + "/queue.db"
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(tenantUsageBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(replicationProposalsBucket); err != nil {
			return err
		}
		metadata := tx.Bucket(metadataBucket)
		if err := metadata.Delete(appliedRaftIndexKey); err != nil {
			return err
		}
		if err := metadata.Delete(commandProtocolVersionKey); err != nil {
			return err
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersionV6)
		return metadata.Put(schemaVersionKey, version)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if index, err := repository.AppliedRaftIndex(); err != nil || index != 0 {
		t.Fatalf("migrated index=%d err=%v", index, err)
	}
	if info, err := os.Stat(path + schemaV6BackupSuffix); err != nil || info.Size() == 0 {
		t.Fatalf("schema-v6 backup=%v err=%v", info, err)
	}
}

func TestApplyReplicatedRollsBackMutationWhenCallbackFails(t *testing.T) {
	repository, err := Open(Config{Path: t.TempDir() + "/queue.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	want := errors.New("stop")
	_, err = repository.ApplyReplicated(3, "request-2/1", func() ([]byte, error) {
		if _, err := repository.Create(queue.Queue{Name: "orders", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
			return nil, err
		}
		return nil, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("apply error = %v", err)
	}
	if _, found, err := repository.Get("orders"); err != nil || found {
		t.Fatalf("rolled-back queue lookup: found=%v err=%v", found, err)
	}
	if index, err := repository.AppliedRaftIndex(); err != nil || index != 0 {
		t.Fatalf("applied index = %d, %v", index, err)
	}
}

func TestSnapshotRestoreReplacesWholeReplicatedState(t *testing.T) {
	repository, err := Open(Config{Path: t.TempDir() + "/queue.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if _, err := repository.ApplyReplicated(4, "request-3/1", func() ([]byte, error) {
		_, err := repository.Create(queue.Queue{Name: "before", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod})
		return []byte(`{"ok":true}`), err
	}); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := repository.WriteSnapshot(&snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Create(queue.Queue{Name: "after", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
		t.Fatal(err)
	}
	if err := repository.RestoreSnapshot(bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.Get("before"); err != nil || !found {
		t.Fatalf("snapshot queue lookup: found=%v err=%v", found, err)
	}
	if _, found, err := repository.Get("after"); err != nil || found {
		t.Fatalf("post-snapshot queue lookup: found=%v err=%v", found, err)
	}
	if index, err := repository.AppliedRaftIndex(); err != nil || index != 4 {
		t.Fatalf("restored index = %d, %v", index, err)
	}
}

func TestSnapshotRestoreRejectsCorruptionWithoutReplacingLiveState(t *testing.T) {
	repository, err := Open(Config{Path: t.TempDir() + "/queue.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if _, err := repository.Create(queue.Queue{Name: "live", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}); err != nil {
		t.Fatal(err)
	}
	if err := repository.RestoreSnapshot(bytes.NewReader([]byte("not a bbolt database"))); err == nil {
		t.Fatal("corrupt snapshot was accepted")
	}
	if _, found, err := repository.Get("live"); err != nil || !found {
		t.Fatalf("live state after rejected restore: found=%v err=%v", found, err)
	}
}
