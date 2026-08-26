package boltrepo

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

func deleteM8Buckets(tx *bolt.Tx) error {
	for _, name := range [][]byte{tenantOwnershipBucket, tenantMigrationsBucket, tenantFencesBucket, tenantBundlesBucket} {
		if err := tx.DeleteBucket(name); err != nil {
			return err
		}
	}
	return nil
}

func TestOpenMigratesValidSchemaV1AndKeepsBackup(t *testing.T) {
	path, sentAt := createLegacyV1Database(t)
	repository, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open migration: %v", err)
	}

	storedQueue, found, err := repository.Get("orders")
	wantQueue := queue.Queue{
		ID:                     storedQueue.ID,
		Name:                   "orders",
		LegacyURLAllowed:       true,
		VisibilityTimeout:      10,
		DelaySeconds:           0,
		MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod,
	}
	if !wellFormedQueueID(storedQueue.ID) {
		t.Fatalf("migrated queue ID = %q", storedQueue.ID)
	}
	if err != nil || !found || storedQueue != wantQueue {
		t.Fatalf("migrated queue = %#v, %v, %v; want %#v", storedQueue, found, err, wantQueue)
	}
	if err := repository.db.View(func(tx *bolt.Tx) error {
		version, err := transactionSchemaVersion(tx)
		if err != nil {
			return err
		}
		if version != schemaVersion {
			t.Fatalf("schema version = %d, want %d", version, schemaVersion)
		}
		return tx.Bucket(messagesBucket).ForEach(func(key, value []byte) error {
			var record messageRecord
			if err := decodeRecord(value, &record); err != nil {
				return err
			}
			availableAt := time.UnixMilli(sentAt.UnixMilli()).UTC()
			if record.Version != recordVersion || record.AvailableAtUnixNanos != availableAt.UnixNano() || record.ExpiresAtUnixNanos != availableAt.Add(96*time.Hour).UnixNano() {
				t.Fatalf("migrated message = %#v", record)
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("inspect migration: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backupPath := path + schemaV1BackupSuffix
	backupInfo, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("Stat backup: %v", err)
	}
	if !privateModeMatches(backupInfo, 0o600) || backupInfo.Size() == 0 {
		t.Fatalf("backup info = %#v", backupInfo)
	}
	if err := validateV1Backup(backupPath); err != nil {
		t.Fatalf("validate backup: %v", err)
	}
	backupBefore, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("ReadFile backup: %v", err)
	}
	repository, err = Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("reopen migrated database: %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	backupAfter, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("ReadFile backup after reopen: %v", err)
	}
	if string(backupAfter) != string(backupBefore) {
		t.Fatal("reopen replaced the version 1 backup")
	}
	if info, err := os.Stat(path + schemaV2BackupSuffix); err != nil || !privateModeMatches(info, 0o600) || info.Size() == 0 {
		t.Fatalf("schema v2 backup after sequential migration = %#v, %v", info, err)
	}
	if err := validateV2Backup(path + schemaV2BackupSuffix); err != nil {
		t.Fatalf("validate sequential schema v2 backup: %v", err)
	}
	if info, err := os.Stat(path + schemaV3BackupSuffix); err != nil || !privateModeMatches(info, 0o600) || info.Size() == 0 {
		t.Fatalf("schema v3 backup after sequential migration = %#v, %v", info, err)
	}
	if err := validateV3Backup(path + schemaV3BackupSuffix); err != nil {
		t.Fatalf("validate sequential schema v3 backup: %v", err)
	}
}

func TestOpenMigratesSchemaV2ClaimWithoutChangingLifecycleOrReceipts(t *testing.T) {
	path, source := createSchemaV2Database(t)
	repository, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open migration: %v", err)
	}
	defer repository.Close()
	if err := repository.db.View(func(tx *bolt.Tx) error {
		version, err := transactionSchemaVersion(tx)
		if err != nil {
			return err
		}
		if version != schemaVersion {
			t.Fatalf("schema version = %d, want %d", version, schemaVersion)
		}
		var migrated messageRecord
		if err := decodeRecord(tx.Bucket(messagesBucket).Get([]byte(source.ID)), &migrated); err != nil {
			return err
		}
		if migrated.AvailableAtUnixNanos != source.AvailableAtUnixNanos || migrated.ExpiresAtUnixNanos != source.ExpiresAtUnixNanos || migrated.VisibilityDeadlineUnixNanos != source.VisibilityDeadlineUnixNanos || migrated.ReceiveCount != source.ReceiveCount || migrated.ReceiveGeneration != source.ReceiveGeneration || migrated.ReceiptHandle != source.ReceiptHandle || migrated.MessageAttributes == nil || len(migrated.MessageAttributes) != 0 || migrated.MD5OfMessageAttributes != "" {
			t.Fatalf("migrated message changed authoritative state: %#v", migrated)
		}
		var receipt receiptRecord
		if err := decodeRecord(tx.Bucket(receiptsBucket).Get([]byte(source.ReceiptHandle)), &receipt); err != nil {
			return err
		}
		if receipt.MessageID != source.ID || receipt.Generation != source.ReceiveGeneration || receipt.QueueName != source.QueueName {
			t.Fatalf("migrated receipt = %#v", receipt)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect migrated database: %v", err)
	}
	backupPath := path + schemaV2BackupSuffix
	if info, err := os.Stat(backupPath); err != nil || !privateModeMatches(info, 0o600) || info.Size() == 0 {
		t.Fatalf("schema v2 backup = %#v, %v", info, err)
	}
	if err := validateV2Backup(backupPath); err != nil {
		t.Fatalf("validate schema v2 backup: %v", err)
	}
}

func TestOpenMigratesEmptySchemaV2Database(t *testing.T) {
	path, source := createSchemaV2Database(t)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(messagesBucket).Delete([]byte(source.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(messageIDsBucket).Delete([]byte(source.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(receiptsBucket).Delete([]byte(source.ReceiptHandle)); err != nil {
			return err
		}
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(source.QueueName))
		key, _ := ordered.Cursor().First()
		return ordered.Delete(key)
	}); err != nil {
		_ = db.Close()
		t.Fatalf("empty schema v2 fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}
	repository, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open empty migration: %v", err)
	}
	defer repository.Close()
	if err := repository.db.View(validateTransaction); err != nil {
		t.Fatalf("validate migrated empty database: %v", err)
	}
	if err := validateV2Backup(path + schemaV2BackupSuffix); err != nil {
		t.Fatalf("validate empty schema v2 backup: %v", err)
	}
}

func TestOpenRejectsCorruptSchemaV2BeforeMigration(t *testing.T) {
	path, source := createSchemaV2Database(t)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		var record messageRecordV2
		if err := decodeRecord(tx.Bucket(messagesBucket).Get([]byte(source.ID)), &record); err != nil {
			return err
		}
		record.MD5OfBody = "corrupt"
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(messagesBucket).Put([]byte(source.ID), encoded)
	}); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("raw Close: %v", err)
	}
	_, err = Open(Config{Path: path, OpenTimeout: time.Second})
	if err == nil || !errors.Is(err, queue.ErrRepositoryCorrupt) {
		t.Fatalf("Open corrupt schema v2 = %v", err)
	}
	if _, statErr := os.Stat(path + schemaV2BackupSuffix); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("corrupt source unexpectedly produced backup: %v", statErr)
	}
}

func TestOpenReusesValidSchemaV2BackupAndRejectsInvalidOne(t *testing.T) {
	t.Run("reuse", func(t *testing.T) {
		path, _ := createSchemaV2Database(t)
		db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
		if err != nil {
			t.Fatalf("raw Open: %v", err)
		}
		if err := ensureMigrationBackup(db, path, schemaV2BackupSuffix, validateV2Backup); err != nil {
			_ = db.Close()
			t.Fatalf("ensure backup: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("raw Close: %v", err)
		}
		before, err := os.ReadFile(path + schemaV2BackupSuffix)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		repository, err := Open(Config{Path: path, OpenTimeout: time.Second})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		_ = repository.Close()
		after, err := os.ReadFile(path + schemaV2BackupSuffix)
		if err != nil || string(after) != string(before) {
			t.Fatalf("valid backup was replaced: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		path, _ := createSchemaV2Database(t)
		if err := os.WriteFile(path+schemaV2BackupSuffix, []byte("invalid backup"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		_, err := Open(Config{Path: path, OpenTimeout: time.Second})
		if err == nil || !errors.Is(err, queue.ErrRepositoryUnavailable) {
			t.Fatalf("Open error = %v", err)
		}
		db, openErr := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
		if openErr != nil {
			t.Fatalf("raw Open: %v", openErr)
		}
		defer db.Close()
		if err := db.View(validateTransactionV2); err != nil {
			t.Fatalf("source changed after invalid backup: %v", err)
		}
	})
}

func TestMigrateV2TransactionRollsBackAllRecordChanges(t *testing.T) {
	path, _ := createSchemaV2Database(t)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	defer db.Close()
	sentinel := errors.New("injected schema v2 migration failure")
	err = db.Update(func(tx *bolt.Tx) error {
		if err := migrateV2Transaction(tx); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("migration error = %v", err)
	}
	if err := db.View(validateTransactionV2); err != nil {
		t.Fatalf("rolled-back database is not valid schema v2: %v", err)
	}
}

func TestOpenRejectsInvalidExistingMigrationBackupWithoutChangingV1(t *testing.T) {
	path, _ := createLegacyV1Database(t)
	backupPath := path + schemaV1BackupSuffix
	if err := os.WriteFile(backupPath, []byte("not a database"), 0o600); err != nil {
		t.Fatalf("WriteFile backup: %v", err)
	}
	_, err := Open(Config{Path: path, OpenTimeout: time.Second})
	if err == nil || !errors.Is(err, queue.ErrRepositoryUnavailable) {
		t.Fatalf("Open error = %v, want unavailable invalid backup failure", err)
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		version, err := transactionSchemaVersion(tx)
		if err != nil {
			return err
		}
		if version != legacySchemaVersion {
			t.Fatalf("schema version after failed backup validation = %d", version)
		}
		return validateTransactionV1(tx)
	}); err != nil {
		t.Fatalf("version 1 database changed after failure: %v", err)
	}
}

func TestMigrateV1TransactionRollsBackAllRecordChanges(t *testing.T) {
	path, _ := createLegacyV1Database(t)
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw Open: %v", err)
	}
	defer db.Close()
	sentinel := errors.New("injected migration write failure")
	err = db.Update(func(tx *bolt.Tx) error {
		if err := migrateV1Transaction(tx); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("migration error = %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error { return validateTransactionV1(tx) }); err != nil {
		t.Fatalf("rolled-back database is not valid version 1: %v", err)
	}
}

func createLegacyV1Database(t *testing.T) (string, time.Time) {
	t.Helper()
	directory := secureTempDirectory(t)
	path := filepath.Join(directory, "simq.db")
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	sentAt := time.Date(2026, time.August, 23, 11, 12, 13, 456789123, time.UTC)
	body := "legacy-body"
	digest := md5.Sum([]byte(body))
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range baseRequiredBuckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, legacySchemaVersion)
		if err := tx.Bucket(metadataBucket).Put(schemaVersionKey, version); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Put(installationIDKey, []byte("00112233445566778899aabbccddeeff")); err != nil {
			return err
		}
		queueValue, err := encodeRecord(queueRecordV1{
			Version: legacyRecordVersion,
			Name:    "orders",
			Attributes: map[string]string{
				"VisibilityTimeout": "10",
			},
		})
		if err != nil {
			return err
		}
		if err := tx.Bucket(queuesBucket).Put([]byte("orders"), queueValue); err != nil {
			return err
		}
		ordered, err := tx.Bucket(messageOrderBucket).CreateBucket([]byte("orders"))
		if err != nil {
			return err
		}
		messageID := "legacy-message"
		messageValue, err := encodeRecord(messageRecordV1{
			Version:          legacyRecordVersion,
			ID:               messageID,
			QueueName:        "orders",
			Body:             body,
			MD5OfBody:        hex.EncodeToString(digest[:]),
			SentAtUnixMillis: sentAt.UnixMilli(),
		})
		if err != nil {
			return err
		}
		if err := tx.Bucket(messagesBucket).Put([]byte(messageID), messageValue); err != nil {
			return err
		}
		idValue, err := encodeRecord(messageIDRecordV1{Version: legacyRecordVersion, ID: messageID})
		if err != nil {
			return err
		}
		if err := tx.Bucket(messageIDsBucket).Put([]byte(messageID), idValue); err != nil {
			return err
		}
		sequence := make([]byte, 8)
		binary.BigEndian.PutUint64(sequence, 1)
		return ordered.Put(sequence, []byte(messageID))
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("create version 1 database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close version 1 database: %v", err)
	}
	return path, sentAt
}

func createSchemaV2Database(t *testing.T) (string, messageRecordV2) {
	t.Helper()
	directory := secureTempDirectory(t)
	path := filepath.Join(directory, "simq.db")
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	sentAt := time.Date(2026, time.August, 23, 12, 13, 14, 123000000, time.UTC)
	body := "schema-v2-body"
	digest := md5.Sum([]byte(body))
	receiptHandle := "rh_00000000000000000000000000000001"
	message := messageRecordV2{
		Version:                     recordVersionV2,
		ID:                          "schema-v2-message",
		QueueName:                   "orders",
		Body:                        body,
		MD5OfBody:                   hex.EncodeToString(digest[:]),
		SentAtUnixMillis:            sentAt.UnixMilli(),
		ReceiptHandle:               receiptHandle,
		ReceiveCount:                1,
		ReceiveGeneration:           1,
		FirstReceivedAtUnixMillis:   sentAt.Add(time.Second).UnixMilli(),
		VisibilityDeadlineUnixNanos: sentAt.Add(31 * time.Second).UnixNano(),
		AvailableAtUnixNanos:        sentAt.Add(500 * time.Millisecond).UnixNano(),
		ExpiresAtUnixNanos:          sentAt.Add(60 * time.Second).UnixNano(),
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range baseRequiredBuckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		version := make([]byte, 4)
		binary.BigEndian.PutUint32(version, schemaVersionV2)
		if err := tx.Bucket(metadataBucket).Put(schemaVersionKey, version); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Put(installationIDKey, []byte("00112233445566778899aabbccddeeff")); err != nil {
			return err
		}
		queueValue, err := encodeRecord(newQueueRecordV2(queue.Queue{Name: "orders", VisibilityTimeout: 30, DelaySeconds: 5, MessageRetentionPeriod: 60}))
		if err != nil {
			return err
		}
		if err := tx.Bucket(queuesBucket).Put([]byte("orders"), queueValue); err != nil {
			return err
		}
		ordered, err := tx.Bucket(messageOrderBucket).CreateBucket([]byte("orders"))
		if err != nil {
			return err
		}
		messageValue, err := encodeRecord(message)
		if err != nil {
			return err
		}
		if err := tx.Bucket(messagesBucket).Put([]byte(message.ID), messageValue); err != nil {
			return err
		}
		idValue, err := encodeRecord(messageIDRecordV2{Version: recordVersionV2, ID: message.ID})
		if err != nil {
			return err
		}
		if err := tx.Bucket(messageIDsBucket).Put([]byte(message.ID), idValue); err != nil {
			return err
		}
		receiptValue, err := encodeRecord(receiptRecordV2{Version: recordVersionV2, Handle: receiptHandle, QueueName: "orders", MessageID: message.ID, Generation: 1})
		if err != nil {
			return err
		}
		if err := tx.Bucket(receiptsBucket).Put([]byte(receiptHandle), receiptValue); err != nil {
			return err
		}
		sequence := make([]byte, 8)
		binary.BigEndian.PutUint64(sequence, 1)
		return ordered.Put(sequence, []byte(message.ID))
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("create schema v2 database: %v", err)
	}
	if err := db.View(validateTransactionV2); err != nil {
		_ = db.Close()
		t.Fatalf("validate schema v2 fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close schema v2 database: %v", err)
	}
	return path, message
}
