package boltrepo

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

const (
	schemaV1BackupSuffix  = ".schema-v1.bak"
	schemaV2BackupSuffix  = ".schema-v2.bak"
	schemaV3BackupSuffix  = ".schema-v3.bak"
	schemaV4BackupSuffix  = ".schema-v4.bak"
	schemaV5BackupSuffix  = ".schema-v5.bak"
	schemaV6BackupSuffix  = ".schema-v6.bak"
	schemaV7BackupSuffix  = ".schema-v7.bak"
	schemaV8BackupSuffix  = ".schema-v8.bak"
	schemaV9BackupSuffix  = ".schema-v9.bak"
	schemaV10BackupSuffix = ".schema-v10.bak"
)

func migrateV10Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV10) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV10BackupSuffix, validateV10Backup); err != nil {
		return fmt.Errorf("create schema version 10 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV10Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 10: %w", err)
	}
	return nil
}

func migrateV9Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV9) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV9BackupSuffix, validateV9Backup); err != nil {
		return fmt.Errorf("create schema version 9 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV9Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 9: %w", err)
	}
	return nil
}

func migrateV8Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV8) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV8BackupSuffix, validateV8Backup); err != nil {
		return fmt.Errorf("create schema version 8 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV8Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 8: %w", err)
	}
	return nil
}

func migrateV7Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV7) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV7BackupSuffix, validateV7Backup); err != nil {
		return fmt.Errorf("create schema version 7 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV7Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 7: %w", err)
	}
	return nil
}

func migrateV6Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV6) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV6BackupSuffix, validateV6Backup); err != nil {
		return fmt.Errorf("create schema version 6 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV6Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 6: %w", err)
	}
	return nil
}

func migrateV5Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV5) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV5BackupSuffix, validateV5Backup); err != nil {
		return fmt.Errorf("create schema version 5 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV5Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 5: %w", err)
	}
	return nil
}

func migrateV4Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV4) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV4BackupSuffix, validateV4Backup); err != nil {
		return fmt.Errorf("create schema version 4 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV4Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 4: %w", err)
	}
	return nil
}

func migrateV1Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, validateTransactionV1)
	}); err != nil {
		return err
	}
	if err := ensureV1MigrationBackup(db, path); err != nil {
		return fmt.Errorf("create schema version 1 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, migrateV1Transaction)
	}); err != nil {
		return fmt.Errorf("migrate schema version 1: %w", err)
	}
	return nil
}

func migrateV3Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransactionV3) }); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV3BackupSuffix, validateV3Backup); err != nil {
		return fmt.Errorf("create schema version 3 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return callTransactionSafely(tx, migrateV3Transaction) }); err != nil {
		return fmt.Errorf("migrate schema version 3: %w", err)
	}
	return nil
}

func migrateV2Database(db *bolt.DB, path string) error {
	if err := db.View(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, validateTransactionV2)
	}); err != nil {
		return err
	}
	if err := ensureMigrationBackup(db, path, schemaV2BackupSuffix, validateV2Backup); err != nil {
		return fmt.Errorf("create schema version 2 backup: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, migrateV2Transaction)
	}); err != nil {
		return fmt.Errorf("migrate schema version 2: %w", err)
	}
	return nil
}

func ensureV1MigrationBackup(db *bolt.DB, path string) error {
	return ensureMigrationBackup(db, path, schemaV1BackupSuffix, validateV1Backup)
}

func ensureMigrationBackup(db *bolt.DB, path, suffix string, validate func(string) error) error {
	backupPath := path + suffix
	if _, err := os.Lstat(backupPath); err == nil {
		return validate(backupPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	temporaryPath := backupPath + ".tmp"
	if _, err := os.Lstat(temporaryPath); err == nil {
		return fmt.Errorf("temporary migration backup already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(temporaryPath, 0o600)
	}); err != nil {
		return err
	}
	if err := syncFile(temporaryPath); err != nil {
		return err
	}
	if err := validate(temporaryPath); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, backupPath); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := validate(backupPath); err != nil {
			return err
		}
	}
	if err := os.Remove(temporaryPath); err != nil {
		return err
	}
	removeTemporary = false
	return syncDirectory(filepath.Dir(path))
}

func validateV1Backup(path string) error {
	return validateMigrationBackup(path, validateTransactionV1)
}

func validateV2Backup(path string) error {
	return validateMigrationBackup(path, validateTransactionV2)
}

func validateV3Backup(path string) error { return validateMigrationBackup(path, validateTransactionV3) }
func validateV4Backup(path string) error { return validateMigrationBackup(path, validateTransactionV4) }
func validateV5Backup(path string) error { return validateMigrationBackup(path, validateTransactionV5) }
func validateV6Backup(path string) error { return validateMigrationBackup(path, validateTransactionV6) }
func validateV7Backup(path string) error { return validateMigrationBackup(path, validateTransactionV7) }
func validateV8Backup(path string) error { return validateMigrationBackup(path, validateTransactionV8) }
func validateV9Backup(path string) error { return validateMigrationBackup(path, validateTransactionV9) }
func validateV10Backup(path string) error {
	return validateMigrationBackup(path, validateTransactionV10)
}

func validateMigrationBackup(path string, validate func(*bolt.Tx) error) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || !privateModeMatches(info, 0o600) || info.Size() == 0 {
		return fmt.Errorf("migration backup must be a non-empty regular 0600 file")
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	validationError := db.View(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, validate)
	})
	return errors.Join(validationError, db.Close())
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func migrateV1Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != legacySchemaVersion {
		return corruptf("unsupported schema version %d", version)
	}
	if err := rewriteBucketRecords(tx.Bucket(queuesBucket), func(key, value []byte) ([]byte, error) {
		var legacy queueRecordV1
		if err := decodeRecord(value, &legacy); err != nil {
			return nil, err
		}
		domain, err := legacy.domain()
		if err != nil {
			return nil, err
		}
		return encodeRecord(newQueueRecordV2(domain))
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(messagesBucket), func(key, value []byte) ([]byte, error) {
		var legacy messageRecordV1
		if err := decodeRecord(value, &legacy); err != nil {
			return nil, err
		}
		migrated, err := legacy.migrate()
		if err != nil {
			return nil, err
		}
		return encodeRecord(migrated)
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(messageIDsBucket), func(key, value []byte) ([]byte, error) {
		var legacy messageIDRecordV1
		if err := decodeRecord(value, &legacy); err != nil {
			return nil, err
		}
		if err := legacy.validate(key); err != nil {
			return nil, err
		}
		return encodeRecord(messageIDRecordV2{Version: recordVersionV2, ID: legacy.ID})
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(receiptsBucket), func(key, value []byte) ([]byte, error) {
		var legacy receiptRecordV1
		if err := decodeRecord(value, &legacy); err != nil {
			return nil, err
		}
		if err := legacy.validate(key); err != nil {
			return nil, err
		}
		return encodeRecord(receiptRecordV2{
			Version:    recordVersionV2,
			Handle:     legacy.Handle,
			QueueName:  legacy.QueueName,
			MessageID:  legacy.MessageID,
			Generation: legacy.Generation,
		})
	}); err != nil {
		return err
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV2)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion)
}

func migrateV2Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV2 {
		return corruptf("unsupported schema version %d", version)
	}
	if err := rewriteBucketRecords(tx.Bucket(queuesBucket), func(key, value []byte) ([]byte, error) {
		var source queueRecordV2
		if err := decodeRecord(value, &source); err != nil {
			return nil, err
		}
		domain, err := source.domain()
		if err != nil {
			return nil, err
		}
		return encodeRecord(newQueueRecord(domain))
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(messagesBucket), func(key, value []byte) ([]byte, error) {
		var source messageRecordV2
		if err := decodeRecord(value, &source); err != nil {
			return nil, err
		}
		migrated, err := source.migrate()
		if err != nil {
			return nil, err
		}
		return encodeRecord(migrated)
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(messageIDsBucket), func(key, value []byte) ([]byte, error) {
		var source messageIDRecordV2
		if err := decodeRecord(value, &source); err != nil {
			return nil, err
		}
		if err := source.validate(key); err != nil {
			return nil, err
		}
		return encodeRecord(messageIDRecord{Version: recordVersion, ID: source.ID})
	}); err != nil {
		return err
	}
	if err := rewriteBucketRecords(tx.Bucket(receiptsBucket), func(key, value []byte) ([]byte, error) {
		var source receiptRecordV2
		if err := decodeRecord(value, &source); err != nil {
			return nil, err
		}
		if err := source.validate(key); err != nil {
			return nil, err
		}
		return encodeRecord(receiptRecord{
			Version:    recordVersion,
			Handle:     source.Handle,
			QueueName:  source.QueueName,
			MessageID:  source.MessageID,
			Generation: source.Generation,
		})
	}); err != nil {
		return err
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV3)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion)
}

func migrateV3Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV3 {
		return corruptf("unsupported schema version %d", version)
	}
	for _, name := range [][]byte{queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket, queueTombstonesBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	installationID := string(tx.Bucket(metadataBucket).Get(installationIDKey))
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error {
		name := string(key)
		digest := sha256.Sum256([]byte(installationID + "\x00" + name))
		queueID := "q_" + hex.EncodeToString(digest[:16])
		identity, err := encodeRecord(queueIdentityRecord{Version: administrationRecordVersion, Name: name, ID: queueID, LegacyURLAllowed: true})
		if err != nil {
			return err
		}
		tags, err := encodeRecord(queueTagsRecord{Version: administrationRecordVersion, QueueID: queueID, Tags: map[string]string{}})
		if err != nil {
			return err
		}
		permissions, err := encodeRecord(queuePermissionsRecord{Version: administrationRecordVersion, QueueID: queueID, Permissions: []queue.Permission{}})
		if err != nil {
			return err
		}
		if err := tx.Bucket(queueIdentitiesBucket).Put(key, identity); err != nil {
			return err
		}
		if err := tx.Bucket(queueTagsBucket).Put(key, tags); err != nil {
			return err
		}
		return tx.Bucket(queuePermissionsBucket).Put(key, permissions)
	}); err != nil {
		return err
	}
	revision := make([]byte, 8)
	if err := tx.Bucket(metadataBucket).Put(namespaceRevisionKey, revision); err != nil {
		return err
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV4)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion)
}

func migrateV4Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV4 {
		return corruptf("unsupported schema version %d", version)
	}
	for _, name := range [][]byte{redrivePoliciesBucket, redriveOriginsBucket, moveTasksBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	if err := tx.Bucket(metadataBucket).Put(redriveRevisionKey, make([]byte, 8)); err != nil {
		return err
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV5)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion)
}

func migrateV5Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV5 {
		return corruptf("unsupported schema version %d", version)
	}
	for _, name := range [][]byte{fifoQueuesBucket, fifoSequencesBucket, fifoMessagesBucket, fifoDedupBucket, fifoAttemptsBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV6)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion)
}

func migrateV6Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV6 {
		return corruptf("unsupported schema version %d", version)
	}
	if _, err := tx.CreateBucket(replicationProposalsBucket); err != nil {
		return err
	}
	metadata := tx.Bucket(metadataBucket)
	if err := metadata.Put(appliedRaftIndexKey, make([]byte, 8)); err != nil {
		return err
	}
	protocol := make([]byte, 4)
	binary.BigEndian.PutUint32(protocol, replicatedCommandProtocolVersionV1)
	if err := metadata.Put(commandProtocolVersionKey, protocol); err != nil {
		return err
	}
	encodedVersion := make([]byte, 4)
	binary.BigEndian.PutUint32(encodedVersion, schemaVersionV7)
	return metadata.Put(schemaVersionKey, encodedVersion)
}

func migrateV7Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV7 {
		return corruptf("unsupported schema version %d", version)
	}
	if _, err := tx.CreateBucket(tenantUsageBucket); err != nil {
		return err
	}
	if err := rebuildTenantUsage(tx); err != nil {
		return err
	}
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, schemaVersionV8)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encoded)
}

func migrateV8Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV8 {
		return corruptf("unsupported schema version %d", version)
	}
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, schemaVersionV9)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encoded)
}

func migrateV9Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV9 {
		return corruptf("unsupported schema version %d", version)
	}
	for _, name := range [][]byte{tenantOwnershipBucket, tenantMigrationsBucket, tenantFencesBucket, tenantBundlesBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	protocol := make([]byte, 4)
	binary.BigEndian.PutUint32(protocol, replicatedCommandProtocolVersionV2)
	if err := tx.Bucket(metadataBucket).Put(commandProtocolVersionKey, protocol); err != nil {
		return err
	}
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, schemaVersionV10)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encoded)
}

func migrateV10Transaction(tx *bolt.Tx) error {
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != schemaVersionV10 {
		return corruptf("unsupported schema version %d", version)
	}
	for _, name := range [][]byte{topologyCatalogBucket, topologyOperationsBucket, topologyTombstonesBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	protocol := make([]byte, 4)
	binary.BigEndian.PutUint32(protocol, replicatedCommandProtocolVersion)
	if err := tx.Bucket(metadataBucket).Put(commandProtocolVersionKey, protocol); err != nil {
		return err
	}
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, schemaVersion)
	return tx.Bucket(metadataBucket).Put(schemaVersionKey, encoded)
}

func rewriteBucketRecords(bucket *bolt.Bucket, rewrite func(key, value []byte) ([]byte, error)) error {
	if bucket == nil {
		return corruptf("required migration bucket is missing")
	}
	type replacement struct {
		key   []byte
		value []byte
	}
	var replacements []replacement
	if err := bucket.ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("migration bucket contains an invalid entry")
		}
		encoded, err := rewrite(key, value)
		if err != nil {
			return err
		}
		replacements = append(replacements, replacement{
			key:   append([]byte(nil), key...),
			value: encoded,
		})
		return nil
	}); err != nil {
		return err
	}
	for _, replacement := range replacements {
		if err := bucket.Put(replacement.key, replacement.value); err != nil {
			return err
		}
	}
	return nil
}
