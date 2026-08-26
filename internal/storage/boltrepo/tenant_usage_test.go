package boltrepo

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func quotaQueue(id, name string) queue.Queue {
	return queue.Queue{ID: id, Name: name, VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}
}
func quotaMessage(id, name, body string) queue.Message {
	digest := md5.Sum([]byte(body))
	return queue.Message{ID: id, QueueName: name, Body: body, MD5OfBody: hex.EncodeToString(digest[:])}
}

func TestStorageQuotaIsAtomicWithQueueAndMessageUsage(t *testing.T) {
	repository, err := Open(Config{Path: t.TempDir() + "/queue.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	repository.SetStorageQuota(queue.StorageQuota{MaxQueues: 1, MaxMessages: 1, MaxPayloadBytes: 1024})
	created, err := repository.Create(quotaQueue("q_00000000000000000000000000000001", "orders"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Create(quotaQueue("q_00000000000000000000000000000002", "blocked")); !errors.Is(err, queue.ErrQuotaExceeded) {
		t.Fatalf("second queue error=%v", err)
	}
	now := time.Now().UTC()
	if _, err := repository.Enqueue(queue.EnqueueCommand{QueueName: created.Name, QueueID: created.ID, Now: now, Message: quotaMessage("m_00000000000000000000000000000001", created.Name, "one")}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Enqueue(queue.EnqueueCommand{QueueName: created.Name, QueueID: created.ID, Now: now, Message: quotaMessage("m_00000000000000000000000000000002", created.Name, "two")}); !errors.Is(err, queue.ErrQuotaExceeded) {
		t.Fatalf("second message error=%v", err)
	}
	if err := repository.PurgeQueue(queue.QueueRef{Name: created.Name, ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Enqueue(queue.EnqueueCommand{QueueName: created.Name, QueueID: created.ID, Now: now, Message: quotaMessage("m_00000000000000000000000000000002", created.Name, "two")}); err != nil {
		t.Fatalf("enqueue after purge: %v", err)
	}
	if err := repository.Health(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenMigratesSchemaV7ThroughV8AndV9ToV10AndRebuildsUsage(t *testing.T) {
	path := t.TempDir() + "/queue.db"
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Create(quotaQueue("q_00000000000000000000000000000001", "orders")); err != nil {
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
		if err := deleteM8Buckets(tx); err != nil {
			return err
		}
		if err := tx.DeleteBucket(tenantUsageBucket); err != nil {
			return err
		}
		protocol := make([]byte, 4)
		binary.BigEndian.PutUint32(protocol, replicatedCommandProtocolVersionV1)
		if err := tx.Bucket(metadataBucket).Put(commandProtocolVersionKey, protocol); err != nil {
			return err
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersionV7)
		return tx.Bucket(metadataBucket).Put(schemaVersionKey, version)
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
	var usage tenantUsageRecord
	if err := repository.db.View(func(tx *bolt.Tx) error {
		var err error
		usage, err = readTenantUsage(tx, legacyTenantUsageKey)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if usage.Queues != 1 || usage.Messages != 0 {
		t.Fatalf("rebuilt usage=%+v", usage)
	}
	if info, err := os.Stat(path + schemaV7BackupSuffix); err != nil || info.Size() == 0 {
		t.Fatalf("schema-v7 backup=%v err=%v", info, err)
	}
	if info, err := os.Stat(path + schemaV8BackupSuffix); err != nil || info.Size() == 0 {
		t.Fatalf("schema-v8 backup=%v err=%v", info, err)
	}
	if info, err := os.Stat(path + schemaV9BackupSuffix); err != nil || info.Size() == 0 {
		t.Fatalf("schema-v9 backup=%v err=%v", info, err)
	}
}

func TestShardCatalogRevisionBindingIsImmutable(t *testing.T) {
	path := t.TempDir() + "/catalog.db"
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	first := "00112233445566778899aabbccddeeff"
	if err := repository.BindShardCatalogRevision(first); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.BindShardCatalogRevision(first); err != nil {
		t.Fatal(err)
	}
	if err := repository.BindShardCatalogRevision("ffeeddccbbaa99887766554433221100"); !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("changed revision error=%v", err)
	}
}
