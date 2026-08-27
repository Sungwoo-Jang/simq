package boltrepo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func createBoltFIFO(t *testing.T, service *queue.Service, name string) queue.Queue {
	t.Helper()
	value, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name, Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestFIFOCommitFailureRollsBackSequenceDedupAndMessage(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	now := time.Date(2026, 8, 25, 18, 0, 0, 0, time.UTC)
	service := queue.NewService(repository, queue.WithClock(&failureClock{now: now}), queue.WithMessageIDGenerator(func() string { return "failed-fifo-message" }))
	fifo := createBoltFIFO(t, service, "rollback.fifo")
	original := repository.updateFn
	commitFailure := errors.New("injected FIFO commit failure")
	repository.updateFn = func(callback func(*bolt.Tx) error) error {
		tx, err := repository.db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := callback(tx); err != nil {
			return err
		}
		if err := tx.Rollback(); err != nil {
			return err
		}
		return commitFailure
	}
	_, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "secret", MessageGroupID: "group", MessageDeduplicationID: "dedup"})
	if !errors.Is(err, commitFailure) || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("failed send error = %v", err)
	}
	repository.updateFn = original
	if err := repository.db.View(func(tx *bolt.Tx) error {
		var sequence fifoSequenceRecord
		if err := decodeRecord(tx.Bucket(fifoSequencesBucket).Get([]byte(fifo.Name)), &sequence); err != nil {
			return err
		}
		if sequence.Next != 0 || tx.Bucket(messagesBucket).Get([]byte("failed-fifo-message")) != nil || tx.Bucket(fifoMessagesBucket).Get([]byte("failed-fifo-message")) != nil || tx.Bucket(fifoDedupBucket).Get(fifoScopedKey(fifo.ID, "dedup")) != nil {
			t.Fatalf("failed FIFO transaction leaked state: sequence=%#v", sequence)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	committed, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "committed", MessageGroupID: "group", MessageDeduplicationID: "dedup"})
	if err != nil || committed.SequenceNumber != 1 {
		t.Fatalf("send after rollback = %#v, %v", committed, err)
	}
}

func TestFIFODedupSequenceAndReceiveAttemptSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
	now := time.Date(2026, 8, 25, 18, 30, 0, 0, time.UTC)
	clock := &failureClock{now: now}
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	var ids int
	service := queue.NewService(repository, queue.WithClock(clock), queue.WithMessageIDGenerator(func() string { ids++; return fmt.Sprintf("restart-%d", ids) }))
	fifo := createBoltFIFO(t, service, "restart.fifo")
	first, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "one", MessageGroupID: "group", MessageDeduplicationID: "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "two", MessageGroupID: "other", MessageDeduplicationID: "two"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiveRequestAttemptID: "restart-attempt"})
	if err != nil || len(attempt) != 1 || attempt[0].ID != first.ID {
		t.Fatalf("first receive = %#v, %v", attempt, err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	service = queue.NewService(repository, queue.WithClock(clock), queue.WithMessageIDGenerator(func() string { ids++; return fmt.Sprintf("restart-%d", ids) }))
	duplicate, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "ignored", MessageGroupID: "different", MessageDeduplicationID: "one"})
	if err != nil || duplicate.ID != first.ID || duplicate.SequenceNumber != first.SequenceNumber {
		t.Fatalf("dedup after restart = %#v, %v", duplicate, err)
	}
	replayed, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiveRequestAttemptID: "restart-attempt"})
	if err != nil || len(replayed) != 1 || replayed[0].ReceiptHandle != attempt[0].ReceiptHandle || replayed[0].ReceiveCount != attempt[0].ReceiveCount {
		t.Fatalf("attempt after restart = %#v, %v", replayed, err)
	}
	clock.now = now.Add(5 * time.Minute)
	third, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "three", MessageGroupID: "third", MessageDeduplicationID: "one"})
	if err != nil || third.ID == first.ID || third.SequenceNumber != second.SequenceNumber+1 {
		t.Fatalf("sequence after restart and expiry = %#v, %v", third, err)
	}
}

func TestOpenMigratesSchemaV5ToV6AndKeepsBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
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
		if err := deleteM8Buckets(tx); err != nil {
			return err
		}
		if err := tx.DeleteBucket(tenantUsageBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(replicationProposalsBucket); err != nil {
			return err
		}
		for _, name := range [][]byte{fifoQueuesBucket, fifoSequencesBucket, fifoMessagesBucket, fifoDedupBucket, fifoAttemptsBucket} {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}
		if err := tx.Bucket(metadataBucket).Delete(appliedRaftIndexKey); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Delete(commandProtocolVersionKey); err != nil {
			return err
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersionV5)
		return tx.Bucket(metadataBucket).Put(schemaVersionKey, version)
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	repository, err = Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		version, err := transactionSchemaVersion(tx)
		if err != nil {
			return err
		}
		if version != schemaVersion {
			t.Fatalf("schema version = %d, want %d", version, schemaVersion)
		}
		for _, name := range [][]byte{fifoQueuesBucket, fifoSequencesBucket, fifoMessagesBucket, fifoDedupBucket, fifoAttemptsBucket} {
			if tx.Bucket(name) == nil {
				t.Fatalf("FIFO bucket %q is missing", name)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	backup := path + schemaV5BackupSuffix
	if info, err := os.Stat(backup); err != nil || info.Size() == 0 || !privateModeMatches(info, 0o600) {
		t.Fatalf("schema-v5 backup = %#v, %v", info, err)
	}
	if err := validateV5Backup(backup); err != nil {
		t.Fatalf("validate schema-v5 backup: %v", err)
	}
}

func TestOpenRejectsMissingFIFOMessageMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simq.db")
	repository, err := Open(Config{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	service := queue.NewService(repository, queue.WithMessageIDGenerator(func() string { return "corrupt-fifo-message" }))
	fifo := createBoltFIFO(t, service, "corrupt.fifo")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "body", MessageGroupID: "group", MessageDeduplicationID: "dedup"}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket(fifoMessagesBucket).Delete([]byte("corrupt-fifo-message")) }); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Path: path}); !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Open error = %v, want ErrRepositoryCorrupt", err)
	}
}

func TestFIFOExpirationRemovesActiveMetadataButKeepsDedup(t *testing.T) {
	repository, _ := openFailureTestRepository(t)
	defer repository.Close()
	now := time.Date(2026, 8, 25, 19, 0, 0, 0, time.UTC)
	service := queue.NewService(repository, queue.WithClock(&failureClock{now: now}), queue.WithMessageIDGenerator(func() string { return "expiring-fifo-message" }))
	fifo, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "expiry.fifo", Attributes: map[string]string{"FifoQueue": "true", "MessageRetentionPeriod": "60"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "body", MessageGroupID: "group", MessageDeduplicationID: "dedup"}); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.Expire(queue.ExpireCommand{Now: now.Add(60 * time.Second)}); err != nil || count != 1 {
		t.Fatalf("Expire = %d, %v", count, err)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(fifoMessagesBucket).Get([]byte("expiring-fifo-message")) != nil {
			t.Fatal("expired FIFO message metadata remains")
		}
		if tx.Bucket(fifoDedupBucket).Get(fifoScopedKey(fifo.ID, "dedup")) == nil {
			t.Fatal("expiration removed FIFO deduplication record")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Health(); err != nil {
		t.Fatalf("health after FIFO expiration = %v", err)
	}
}
