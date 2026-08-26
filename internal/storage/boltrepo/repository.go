package boltrepo

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

const DefaultOpenTimeout = time.Second

type Config struct {
	Path        string
	OpenTimeout time.Duration
}

type Repository struct {
	mu           sync.RWMutex
	transactions sync.RWMutex
	db           *bolt.DB
	closed       bool
	updateFn     func(func(*bolt.Tx) error) error
	path         string
	openTimeout  time.Duration
	// ambientTx is set only while the single-threaded replicated FSM invokes a
	// normal repository operation. ClusterRepository is the exclusive owner in
	// that mode; callers must not bypass it.
	ambientTx    atomic.Pointer[bolt.Tx]
	storageQuota queue.StorageQuota
}

var _ queue.Repository = (*Repository)(nil)

func (r *Repository) SetStorageQuota(quota queue.StorageQuota) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.storageQuota = quota
}

func Open(config Config) (*Repository, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("open bbolt repository: data path is required")
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = DefaultOpenTimeout
	}
	path := filepath.Clean(config.Path)
	directory := filepath.Dir(path)
	if err := prepareDataDirectory(directory); err != nil {
		return nil, fmt.Errorf("open bbolt repository: %w", err)
	}

	created, err := prepareDataFile(path)
	if err != nil {
		return nil, fmt.Errorf("open bbolt repository: %w", err)
	}
	db, err := openBolt(path, config.OpenTimeout)
	if err != nil {
		if !created && (errors.Is(err, bolt.ErrInvalid) || errors.Is(err, bolt.ErrVersionMismatch) || errors.Is(err, bolt.ErrChecksum) || errors.Is(err, queue.ErrRepositoryCorrupt)) {
			return nil, corruptf("physical database metadata is invalid: %v", err)
		}
		return nil, unavailable("open bbolt repository", err)
	}
	closeOnError := func(openError error) (*Repository, error) {
		if closeError := db.Close(); closeError != nil {
			return nil, fmt.Errorf("%w; close after failed open: %v", openError, closeError)
		}
		return nil, openError
	}

	if created {
		installationID, err := newInstallationID()
		if err != nil {
			return closeOnError(unavailable("initialize bbolt repository", err))
		}
		if err := db.Update(func(tx *bolt.Tx) error {
			return callTransactionSafely(tx, func(tx *bolt.Tx) error {
				return initializeSchema(tx, installationID)
			})
		}); err != nil {
			return closeOnError(unavailable("initialize bbolt repository", err))
		}
	}
	var actualSchemaVersion uint32
	if err := db.View(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, func(tx *bolt.Tx) error {
			version, err := transactionSchemaVersion(tx)
			actualSchemaVersion = version
			return err
		})
	}); err != nil {
		if !errors.Is(err, queue.ErrRepositoryCorrupt) {
			err = unavailable("read bbolt schema version", err)
		}
		return closeOnError(err)
	}
	if actualSchemaVersion == legacySchemaVersion {
		if err := migrateV1Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV2
	}
	if actualSchemaVersion == schemaVersionV2 {
		if err := migrateV2Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV3
	}
	if actualSchemaVersion == schemaVersionV3 {
		if err := migrateV3Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV4
	}
	if actualSchemaVersion == schemaVersionV4 {
		if err := migrateV4Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV5
	}
	if actualSchemaVersion == schemaVersionV5 {
		if err := migrateV5Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV6
	}
	if actualSchemaVersion == schemaVersionV6 {
		if err := migrateV6Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV7
	}
	if actualSchemaVersion == schemaVersionV7 {
		if err := migrateV7Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersionV8
	}
	if actualSchemaVersion == schemaVersionV8 {
		if err := migrateV8Database(db, path); err != nil {
			if !errors.Is(err, queue.ErrRepositoryCorrupt) {
				err = unavailable("migrate bbolt repository", err)
			}
			return closeOnError(err)
		}
		actualSchemaVersion = schemaVersion
	}
	if actualSchemaVersion != schemaVersion {
		return closeOnError(corruptf("unsupported schema version %d", actualSchemaVersion))
	}
	if err := db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransaction) }); err != nil {
		if !errors.Is(err, queue.ErrRepositoryCorrupt) {
			err = unavailable("validate bbolt repository", err)
		}
		return closeOnError(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return closeOnError(unavailable("stat bbolt repository", err))
	}
	if !privateModeMatches(info, 0o600) {
		return closeOnError(fmt.Errorf("open bbolt repository: database file mode must be 0600"))
	}

	repository := &Repository{db: db, path: path, openTimeout: config.OpenTimeout}
	repository.updateFn = db.Update
	return repository, nil
}

func (r *Repository) BindShardCatalogRevision(revision string) error {
	decoded, err := hex.DecodeString(revision)
	if err != nil || len(decoded) != 16 || revision != strings.ToLower(revision) {
		return fmt.Errorf("shard catalog revision must be 32 lowercase hexadecimal characters")
	}
	return r.update(func(tx *bolt.Tx) error {
		metadata := tx.Bucket(metadataBucket)
		current := metadata.Get(shardCatalogRevisionKey)
		if current != nil && string(current) != revision {
			return corruptf("shard catalog revision changed from the bound value")
		}
		return metadata.Put(shardCatalogRevisionKey, []byte(revision))
	})
}

func openBolt(path string, openTimeout time.Duration) (database *bolt.DB, err error) {
	defer func() {
		if recover() != nil {
			database = nil
			err = corruptf("bbolt open failed unexpectedly")
		}
	}()
	return bolt.Open(path, 0o600, &bolt.Options{
		Timeout:    openTimeout,
		NoGrowSync: false,
		NoSync:     false,
	})
}

func prepareDataDirectory(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create data directory: %w", err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure data directory: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat data directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("data directory path is not a directory")
	}
	if !privateModeMatches(info, 0o700) {
		return fmt.Errorf("data directory mode must be 0700")
	}
	return nil
}

func prepareDataFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() {
			return false, fmt.Errorf("database path is not a regular file")
		}
		if !privateModeMatches(info, 0o600) {
			return false, fmt.Errorf("database file mode must be 0600")
		}
		if info.Size() == 0 {
			return false, corruptf("existing database file is empty")
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("stat database file: %w", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return false, fmt.Errorf("create database file: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close database placeholder: %w", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return false, fmt.Errorf("sync data directory: %w", err)
	}
	return true, nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		// os.File.Sync on a directory handle is not supported on Windows.
		// File contents and bbolt commits are still explicitly synchronized.
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func privateModeMatches(info os.FileInfo, expected os.FileMode) bool {
	return runtime.GOOS == "windows" || info.Mode().Perm() == expected
}

func newInstallationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate installation ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func (r *Repository) Create(candidate queue.Queue) (queue.Queue, error) {
	if candidate.ID == "" {
		generated, err := newStoredQueueID()
		if err != nil {
			return queue.Queue{}, wrapOperationError("create queue", err)
		}
		candidate.ID = generated
	}
	var result queue.Queue
	err := r.update(func(tx *bolt.Tx) error {
		queues := tx.Bucket(queuesBucket)
		if encoded := queues.Get([]byte(candidate.Name)); encoded != nil {
			var existingRecord queueRecord
			if err := decodeRecord(encoded, &existingRecord); err != nil {
				return err
			}
			existing, err := existingRecord.domain()
			if err != nil {
				return err
			}
			if existing.Name != candidate.Name {
				return corruptf("queue record key does not match its name")
			}
			if encodedFIFO := tx.Bucket(fifoQueuesBucket).Get([]byte(candidate.Name)); encodedFIFO != nil {
				var fifo fifoQueueRecord
				if err := decodeRecord(encodedFIFO, &fifo); err != nil {
					return err
				}
				existing.FIFO, existing.ContentBasedDeduplication = true, fifo.ContentBasedDeduplication
			}
			if existing.Name != candidate.Name || existing.VisibilityTimeout != candidate.VisibilityTimeout || existing.DelaySeconds != candidate.DelaySeconds || existing.MessageRetentionPeriod != candidate.MessageRetentionPeriod || existing.FIFO != candidate.FIFO || existing.ContentBasedDeduplication != candidate.ContentBasedDeduplication {
				return queue.ErrQueueAlreadyExists
			}
			result = existing
			return nil
		}
		if err := checkTenantQuota(tx, candidate.Name, r.storageQuota, 1, 0, 0); err != nil {
			return err
		}

		orders := tx.Bucket(messageOrderBucket)
		if orders.Bucket([]byte(candidate.Name)) != nil {
			return corruptf("message order bucket exists without queue record")
		}
		if _, err := orders.CreateBucket([]byte(candidate.Name)); err != nil {
			return fmt.Errorf("create queue order: %w", err)
		}
		if !wellFormedQueueID(candidate.ID) {
			return queue.ErrQueueIDUnavailable
		}
		duplicateID := false
		if err := tx.Bucket(queueIdentitiesBucket).ForEach(func(key, value []byte) error {
			var identity queueIdentityRecord
			if err := decodeRecord(value, &identity); err != nil {
				return err
			}
			if identity.ID == candidate.ID {
				duplicateID = true
			}
			return nil
		}); err != nil {
			return err
		}
		if duplicateID {
			return queue.ErrQueueIDUnavailable
		}
		candidate.LegacyURLAllowed = tx.Bucket(queueTombstonesBucket).Get([]byte(candidate.Name)) == nil
		encoded, err := encodeRecord(newQueueRecord(candidate))
		if err != nil {
			return err
		}
		if err := queues.Put([]byte(candidate.Name), encoded); err != nil {
			return fmt.Errorf("store queue: %w", err)
		}
		identityValue, err := encodeRecord(queueIdentityRecord{Version: administrationRecordVersion, Name: candidate.Name, ID: candidate.ID, LegacyURLAllowed: candidate.LegacyURLAllowed})
		if err != nil {
			return err
		}
		tagsValue, err := encodeRecord(queueTagsRecord{Version: administrationRecordVersion, QueueID: candidate.ID, Tags: map[string]string{}})
		if err != nil {
			return err
		}
		permissionsValue, err := encodeRecord(queuePermissionsRecord{Version: administrationRecordVersion, QueueID: candidate.ID, Permissions: []queue.Permission{}})
		if err != nil {
			return err
		}
		if err := tx.Bucket(queueIdentitiesBucket).Put([]byte(candidate.Name), identityValue); err != nil {
			return err
		}
		if err := tx.Bucket(queueTagsBucket).Put([]byte(candidate.Name), tagsValue); err != nil {
			return err
		}
		if err := tx.Bucket(queuePermissionsBucket).Put([]byte(candidate.Name), permissionsValue); err != nil {
			return err
		}
		if candidate.FIFO {
			config, err := encodeRecord(fifoQueueRecord{Version: fifoRecordVersion, QueueID: candidate.ID, ContentBasedDeduplication: candidate.ContentBasedDeduplication})
			if err != nil {
				return err
			}
			sequence, err := encodeRecord(fifoSequenceRecord{Version: fifoRecordVersion, QueueID: candidate.ID, Next: 0})
			if err != nil {
				return err
			}
			if err := tx.Bucket(fifoQueuesBucket).Put([]byte(candidate.Name), config); err != nil {
				return err
			}
			if err := tx.Bucket(fifoSequencesBucket).Put([]byte(candidate.Name), sequence); err != nil {
				return err
			}
		}
		if err := incrementNamespaceRevision(tx); err != nil {
			return err
		}
		if err := changeTenantUsage(tx, candidate.Name, 1, 0, 0); err != nil {
			return err
		}
		result = candidate
		return nil
	})
	if err != nil {
		return queue.Queue{}, wrapOperationError("create queue", err)
	}
	return result, nil
}

func newStoredQueueID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "q_" + hex.EncodeToString(value), nil
}

func (r *Repository) Get(name string) (queue.Queue, bool, error) {
	var result queue.Queue
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		value, ok, err := getQueue(tx, name)
		if err != nil {
			return err
		}
		result, found = value, ok
		return nil
	})
	if err != nil {
		return queue.Queue{}, false, wrapOperationError("get queue", err)
	}
	return result, found, nil
}

func (r *Repository) GetByRef(ref queue.QueueRef) (queue.Queue, bool, error) {
	var result queue.Queue
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		value, ok, err := getQueueByRef(tx, ref)
		if err != nil {
			return err
		}
		result, found = value, ok
		return nil
	})
	if err != nil {
		return queue.Queue{}, false, wrapOperationError("get queue by identity", err)
	}
	return result, found, nil
}

func (r *Repository) SetAttributes(command queue.QueueAttributesCommand) (queue.Queue, error) {
	var result queue.Queue
	err := r.update(func(tx *bolt.Tx) error {
		value, found, err := getQueueByRef(tx, queue.QueueRef{Name: command.QueueName, ID: command.QueueID})
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrQueueDoesNotExist
		}
		if command.VisibilityTimeout != nil {
			value.VisibilityTimeout = *command.VisibilityTimeout
		}
		if command.DelaySeconds != nil {
			value.DelaySeconds = *command.DelaySeconds
		}
		if command.MessageRetentionPeriod != nil {
			value.MessageRetentionPeriod = *command.MessageRetentionPeriod
		}
		if command.RedrivePolicySet {
			policies := tx.Bucket(redrivePoliciesBucket)
			if command.RedrivePolicy == nil {
				if err := policies.Delete([]byte(command.QueueName)); err != nil {
					return err
				}
			} else {
				policy := command.RedrivePolicy
				if err := queue.ValidateRedrivePolicy(policy); err != nil || policy.DeadLetterTarget == (queue.QueueRef{Name: value.Name, ID: value.ID}) {
					return &queue.InvalidRequestError{Message: "RedrivePolicy target is invalid"}
				}
				target, found, err := getQueueByRef(tx, policy.DeadLetterTarget)
				if err != nil {
					return err
				} else if !found {
					return queue.ErrQueueDoesNotExist
				}
				if target.FIFO != value.FIFO {
					return &queue.InvalidRequestError{Message: "RedrivePolicy source and target must have the same queue type"}
				}
				seen := map[queue.QueueRef]struct{}{{Name: value.Name, ID: value.ID}: {}}
				for cursor := policy.DeadLetterTarget; ; {
					if _, duplicate := seen[cursor]; duplicate {
						return &queue.InvalidRequestError{Message: "RedrivePolicy must not create a cycle"}
					}
					seen[cursor] = struct{}{}
					next, found, err := getQueueByRef(tx, cursor)
					if err != nil {
						return err
					}
					if !found || next.RedrivePolicy == nil {
						break
					}
					cursor = next.RedrivePolicy.DeadLetterTarget
				}
				record, err := encodeRecord(redrivePolicyRecord{Version: redriveRecordVersion, SourceName: value.Name, SourceID: value.ID, TargetName: policy.DeadLetterTarget.Name, TargetID: policy.DeadLetterTarget.ID, MaxReceiveCount: policy.MaxReceiveCount})
				if err != nil {
					return err
				}
				if err := policies.Put([]byte(value.Name), record); err != nil {
					return err
				}
			}
			if err := incrementRedriveRevision(tx); err != nil {
				return err
			}
			value.RedrivePolicy = command.RedrivePolicy
		}
		encoded, err := encodeRecord(newQueueRecord(value))
		if err != nil {
			return err
		}
		if err := tx.Bucket(queuesBucket).Put([]byte(command.QueueName), encoded); err != nil {
			return fmt.Errorf("store queue attributes: %w", err)
		}
		result = value
		return nil
	})
	if err != nil {
		return queue.Queue{}, wrapOperationError("set queue attributes", err)
	}
	return result, nil
}

func (r *Repository) Enqueue(command queue.EnqueueCommand) (queue.Message, error) {
	var stored queue.Message
	err := r.update(func(tx *bolt.Tx) error {
		queueValue, found, err := getQueueByRef(tx, queue.QueueRef{Name: command.QueueName, ID: command.QueueID})
		if err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		message := command.Message
		if message.QueueName != command.QueueName {
			return fmt.Errorf("message queue does not match enqueue queue")
		}
		if queueValue.FIFO {
			if message.MessageGroupID == "" || message.MessageDeduplicationID == "" {
				return &queue.InvalidRequestError{Message: "FIFO message metadata is required"}
			}
			if err := cleanupFIFODedup(tx, queueValue.ID, command.Now); err != nil {
				return err
			}
			key := fifoScopedKey(queueValue.ID, message.MessageDeduplicationID)
			if encodedDuplicate := tx.Bucket(fifoDedupBucket).Get(key); encodedDuplicate != nil {
				var duplicate fifoDedupRecord
				if err := decodeRecord(encodedDuplicate, &duplicate); err != nil {
					return err
				}
				stored = queue.Message{ID: duplicate.MessageID, QueueName: queueValue.Name, MD5OfBody: duplicate.MD5OfBody, MD5OfMessageAttributes: duplicate.MD5OfMessageAttributes, MessageGroupID: duplicate.GroupID, MessageDeduplicationID: duplicate.DeduplicationID, SequenceNumber: duplicate.Sequence}
				return nil
			}
			message.SequenceNumber, err = allocateFIFOSequence(tx, queueValue)
			if err != nil {
				return err
			}
		} else if message.MessageGroupID != "" || message.MessageDeduplicationID != "" || message.SequenceNumber != 0 {
			return &queue.InvalidRequestError{Message: "FIFO message metadata is not valid for a Standard queue"}
		}
		if err := checkTenantQuota(tx, command.QueueName, r.storageQuota, 0, 1, payloadSize(message)); err != nil {
			return err
		}
		delaySeconds := queueValue.DelaySeconds
		if command.DelaySeconds != nil {
			delaySeconds = *command.DelaySeconds
		}
		message.SentAtMillis = command.Now.UnixMilli()
		message.AvailableAt = command.Now.Add(time.Duration(delaySeconds) * time.Second)
		message.ExpiresAt = command.Now.Add(time.Duration(queueValue.MessageRetentionPeriod) * time.Second)
		if err := newMessageRecord(message).validate(); err != nil {
			return err
		}
		messageIDs := tx.Bucket(messageIDsBucket)
		if messageIDs.Get([]byte(message.ID)) != nil || tx.Bucket(messagesBucket).Get([]byte(message.ID)) != nil {
			return queue.ErrMessageIDExists
		}

		messageValue, err := encodeRecord(newMessageRecord(message))
		if err != nil {
			return err
		}
		idValue, err := encodeRecord(messageIDRecord{Version: recordVersion, ID: message.ID})
		if err != nil {
			return err
		}
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(command.QueueName))
		if ordered == nil {
			return corruptf("queue is missing its message order bucket")
		}
		sequence, err := ordered.NextSequence()
		if err != nil {
			return fmt.Errorf("allocate message order: %w", err)
		}
		if sequence == 0 {
			return fmt.Errorf("message order sequence exhausted")
		}
		sequenceKey := make([]byte, 8)
		binary.BigEndian.PutUint64(sequenceKey, sequence)
		if err := tx.Bucket(messagesBucket).Put([]byte(message.ID), messageValue); err != nil {
			return fmt.Errorf("store message: %w", err)
		}
		if err := messageIDs.Put([]byte(message.ID), idValue); err != nil {
			return fmt.Errorf("store message ID history: %w", err)
		}
		if err := ordered.Put(sequenceKey, []byte(message.ID)); err != nil {
			return fmt.Errorf("store message order: %w", err)
		}
		if queueValue.FIFO {
			dedupMD5OfBody := message.MD5OfBody
			dedupMD5OfAttributes := message.MD5OfMessageAttributes
			if message.PlaintextMD5OfBody != "" {
				dedupMD5OfBody = message.PlaintextMD5OfBody
				dedupMD5OfAttributes = message.PlaintextMD5OfMessageAttributes
			}
			fifoMessage, err := encodeRecord(fifoMessageRecord{Version: fifoRecordVersion, MessageID: message.ID, QueueID: queueValue.ID, GroupID: message.MessageGroupID, DeduplicationID: message.MessageDeduplicationID, Sequence: message.SequenceNumber})
			if err != nil {
				return err
			}
			dedup, err := encodeRecord(fifoDedupRecord{Version: fifoRecordVersion, QueueID: queueValue.ID, DeduplicationID: message.MessageDeduplicationID, GroupID: message.MessageGroupID, MessageID: message.ID, MD5OfBody: dedupMD5OfBody, MD5OfMessageAttributes: dedupMD5OfAttributes, Sequence: message.SequenceNumber, ExpiresAtUnixNanos: command.Now.Add(5 * time.Minute).UnixNano()})
			if err != nil {
				return err
			}
			if err := tx.Bucket(fifoMessagesBucket).Put([]byte(message.ID), fifoMessage); err != nil {
				return err
			}
			if err := tx.Bucket(fifoDedupBucket).Put(fifoScopedKey(queueValue.ID, message.MessageDeduplicationID), dedup); err != nil {
				return err
			}
		}
		if err := changeTenantUsage(tx, command.QueueName, 0, 1, int64(payloadSize(message))); err != nil {
			return err
		}
		stored = message
		return nil
	})
	if err != nil {
		return queue.Message{}, wrapOperationError("enqueue message", err)
	}
	return stored, nil
}

func (r *Repository) Claim(input queue.ClaimInput) (queue.ClaimResult, error) {
	var claimResult queue.ClaimResult
	err := r.update(func(tx *bolt.Tx) error {
		if input.MaxNumberOfMessages < 1 || input.MaxNumberOfMessages > 10 {
			return fmt.Errorf("claim message limit is outside repository contract")
		}
		queueValue, found, err := getQueueByRef(tx, queue.QueueRef{Name: input.QueueName, ID: input.QueueID})
		if err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		if !queueValue.FIFO && input.ReceiveRequestAttemptID != "" {
			return &queue.InvalidRequestError{Message: "ReceiveRequestAttemptId is valid only for FIFO queues"}
		}
		if queueValue.FIFO {
			if err := cleanupFIFOAttempts(tx, queueValue.ID, input.Now); err != nil {
				return err
			}
			if input.ReceiveRequestAttemptID != "" {
				if encodedAttempt := tx.Bucket(fifoAttemptsBucket).Get(fifoScopedKey(queueValue.ID, input.ReceiveRequestAttemptID)); encodedAttempt != nil {
					messages, err := decodeFIFOAttempt(encodedAttempt, input.ReceiveRequestAttemptID, queueValue.ID)
					if err != nil {
						return err
					}
					claimResult.Messages = messages
					claimResult.AttemptReplayed = true
					return nil
				}
			}
		}
		visibilityTimeout := time.Duration(queueValue.VisibilityTimeout) * time.Second
		if input.VisibilityTimeout != nil {
			visibilityTimeout = *input.VisibilityTimeout
		}
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(input.QueueName))
		if ordered == nil {
			return corruptf("queue is missing its message order bucket")
		}

		type selectedRecord struct {
			record messageRecord
			fifo   *fifoMessageRecord
		}
		selected := make([]selectedRecord, 0, input.MaxNumberOfMessages)
		var nextTransitionNanos int64
		considerTransition := func(candidate int64) {
			if candidate > input.Now.UnixNano() && (nextTransitionNanos == 0 || candidate < nextTransitionNanos) {
				nextTransitionNanos = candidate
			}
		}
		seenGroups := make(map[string]struct{})
		cursor := ordered.Cursor()
		for orderKey, messageID := cursor.First(); orderKey != nil; orderKey, messageID = cursor.Next() {
			if messageID == nil || len(orderKey) != 8 || binary.BigEndian.Uint64(orderKey) == 0 {
				return corruptf("message order entry is malformed")
			}
			encoded := tx.Bucket(messagesBucket).Get(messageID)
			if encoded == nil {
				return corruptf("message order refers to a missing message")
			}
			var record messageRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			if err := record.validate(); err != nil {
				return err
			}
			if record.QueueName != input.QueueName || record.ID != string(messageID) {
				return corruptf("message order crosses queue or ID boundary")
			}
			if record.ExpiresAtUnixNanos <= input.Now.UnixNano() {
				if err := tx.Bucket(messagesBucket).Delete(messageID); err != nil {
					return fmt.Errorf("delete expired message: %w", err)
				}
				if err := tx.Bucket(redriveOriginsBucket).Delete(messageID); err != nil {
					return err
				}
				if err := tx.Bucket(fifoMessagesBucket).Delete(messageID); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return fmt.Errorf("delete expired message order: %w", err)
				}
				continue
			}
			var fifo *fifoMessageRecord
			if queueValue.FIFO {
				encodedFIFO := tx.Bucket(fifoMessagesBucket).Get(messageID)
				if encodedFIFO == nil {
					return corruptf("FIFO message metadata is missing")
				}
				var metadata fifoMessageRecord
				if err := decodeRecord(encodedFIFO, &metadata); err != nil {
					return err
				}
				if metadata.Version != fifoRecordVersion || metadata.MessageID != record.ID || metadata.QueueID != queueValue.ID || metadata.GroupID == "" || metadata.DeduplicationID == "" || metadata.Sequence == 0 {
					return corruptf("FIFO message metadata is invalid")
				}
				if _, blocked := seenGroups[metadata.GroupID]; blocked {
					continue
				}
				seenGroups[metadata.GroupID] = struct{}{}
				fifo = &metadata
			}
			if record.AvailableAtUnixNanos > input.Now.UnixNano() {
				considerTransition(record.AvailableAtUnixNanos)
				considerTransition(record.ExpiresAtUnixNanos)
				continue
			}
			if record.ReceiveGeneration > 0 && record.VisibilityDeadlineUnixNanos > input.Now.UnixNano() {
				considerTransition(record.VisibilityDeadlineUnixNanos)
				considerTransition(record.ExpiresAtUnixNanos)
				continue
			}
			if queueValue.RedrivePolicy != nil && record.ReceiveCount >= queueValue.RedrivePolicy.MaxReceiveCount {
				target := queueValue.RedrivePolicy.DeadLetterTarget
				targetQueue, found, err := getQueueByRef(tx, target)
				if err != nil {
					return err
				} else if !found {
					return queue.ErrQueueDoesNotExist
				}
				if targetQueue.FIFO != queueValue.FIFO {
					return corruptf("redrive source and target queue types differ")
				}
				if queueValue.FIFO {
					fifo.Sequence, err = allocateFIFOSequence(tx, targetQueue)
					if err != nil {
						return err
					}
					fifo.QueueID = targetQueue.ID
					encodedFIFO, err := encodeRecord(*fifo)
					if err != nil {
						return err
					}
					if err := tx.Bucket(fifoMessagesBucket).Put(messageID, encodedFIFO); err != nil {
						return err
					}
				}
				record.QueueName = target.Name
				record.ReceiptHandle = ""
				record.ReceiveCount = 0
				record.FirstReceivedAtUnixMillis = 0
				record.VisibilityDeadlineUnixNanos = 0
				record.AvailableAtUnixNanos = input.Now.UnixNano()
				encodedMoved, err := encodeRecord(record)
				if err != nil {
					return err
				}
				destinationOrder := tx.Bucket(messageOrderBucket).Bucket([]byte(target.Name))
				if destinationOrder == nil {
					return corruptf("redrive target is missing its message order bucket")
				}
				sequence, err := destinationOrder.NextSequence()
				if err != nil || sequence == 0 {
					return fmt.Errorf("allocate redrive order: %w", err)
				}
				sequenceKey := make([]byte, 8)
				binary.BigEndian.PutUint64(sequenceKey, sequence)
				origin, err := encodeRecord(redriveOriginRecord{Version: redriveRecordVersion, MessageID: record.ID, SourceName: queueValue.Name, SourceID: queueValue.ID})
				if err != nil {
					return err
				}
				if err := tx.Bucket(messagesBucket).Put(messageID, encodedMoved); err != nil {
					return err
				}
				if err := destinationOrder.Put(sequenceKey, messageID); err != nil {
					return err
				}
				if err := tx.Bucket(redriveOriginsBucket).Put(messageID, origin); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
				claimResult.MovedTo = append(claimResult.MovedTo, target)
				continue
			}
			selected = append(selected, selectedRecord{record: record, fifo: fifo})
			if len(selected) == input.MaxNumberOfMessages {
				break
			}
		}
		if len(selected) == 0 {
			claimResult.Messages = []queue.Message{}
			if nextTransitionNanos != 0 {
				claimResult.NextTransition = time.Unix(0, nextTransitionNanos).UTC()
			}
			if queueValue.FIFO && input.ReceiveRequestAttemptID != "" && input.StoreEmptyAttempt {
				return storeFIFOAttempt(tx, queueValue.ID, input.ReceiveRequestAttemptID, []queue.Message{}, input.Now)
			}
			return nil
		}
		if len(input.ReceiptHandles) < len(selected) {
			return queue.ErrReceiptUnavailable
		}
		receipts := tx.Bucket(receiptsBucket)
		seen := make(map[string]struct{}, len(selected))
		for index := range selected {
			handle := input.ReceiptHandles[index]
			if !wellFormedReceiptHandle(handle) {
				return queue.ErrReceiptUnavailable
			}
			if _, duplicate := seen[handle]; duplicate || receipts.Get([]byte(handle)) != nil {
				return queue.ErrReceiptHandleExists
			}
			seen[handle] = struct{}{}
		}

		result := make([]queue.Message, 0, len(selected))
		for index := range selected {
			record := selected[index].record
			if record.ReceiveCount == ^uint64(0) || record.ReceiveGeneration == ^uint64(0) {
				return fmt.Errorf("receive metadata overflow")
			}
			record.ReceiveCount++
			record.ReceiveGeneration++
			record.ReceiptHandle = input.ReceiptHandles[index]
			record.VisibilityDeadlineUnixNanos = input.Now.Add(visibilityTimeout).UnixNano()
			if record.ReceiveCount == 1 {
				record.FirstReceivedAtUnixMillis = input.Now.UnixMilli()
			}
			messageValue, err := encodeRecord(record)
			if err != nil {
				return err
			}
			receiptValue, err := encodeRecord(receiptRecord{
				Version:    recordVersion,
				Handle:     record.ReceiptHandle,
				QueueName:  input.QueueName,
				MessageID:  record.ID,
				Generation: record.ReceiveGeneration,
			})
			if err != nil {
				return err
			}
			if err := tx.Bucket(messagesBucket).Put([]byte(record.ID), messageValue); err != nil {
				return fmt.Errorf("store claimed message: %w", err)
			}
			if err := receipts.Put([]byte(record.ReceiptHandle), receiptValue); err != nil {
				return fmt.Errorf("store receipt history: %w", err)
			}
			domainMessage, err := record.domain()
			if err != nil {
				return err
			}
			if selected[index].fifo != nil {
				domainMessage.MessageGroupID = selected[index].fifo.GroupID
				domainMessage.MessageDeduplicationID = selected[index].fifo.DeduplicationID
				domainMessage.SequenceNumber = selected[index].fifo.Sequence
			}
			result = append(result, domainMessage)
		}
		claimResult.Messages = result
		if queueValue.FIFO && input.ReceiveRequestAttemptID != "" {
			return storeFIFOAttempt(tx, queueValue.ID, input.ReceiveRequestAttemptID, result, input.Now)
		}
		return nil
	})
	if err != nil {
		return queue.ClaimResult{}, wrapOperationError("claim messages", err)
	}
	return claimResult, nil
}

func (r *Repository) Delete(queueName, queueID, receiptHandle string) error {
	err := r.update(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, queue.QueueRef{Name: queueName, ID: queueID}); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		encodedReceipt := tx.Bucket(receiptsBucket).Get([]byte(receiptHandle))
		if encodedReceipt == nil {
			return queue.ErrReceiptHandleIsInvalid
		}
		var receipt receiptRecord
		if err := decodeRecord(encodedReceipt, &receipt); err != nil {
			return err
		}
		if err := receipt.validate([]byte(receiptHandle)); err != nil {
			return err
		}
		if receipt.QueueName != queueName {
			return queue.ErrReceiptHandleIsInvalid
		}

		messages := tx.Bucket(messagesBucket)
		encodedMessage := messages.Get([]byte(receipt.MessageID))
		if encodedMessage == nil {
			return nil
		}
		var message messageRecord
		if err := decodeRecord(encodedMessage, &message); err != nil {
			return err
		}
		if err := message.validate(); err != nil {
			return err
		}
		if message.QueueName != queueName {
			return nil
		}
		if message.ReceiptHandle != receiptHandle || message.ReceiveGeneration != receipt.Generation {
			return nil
		}

		return deleteActiveMessage(tx, queueName, receipt.MessageID)
	})
	return wrapOperationError("delete message", err)
}

func (r *Repository) ChangeVisibility(command queue.ChangeVisibilityCommand) error {
	err := r.update(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, queue.QueueRef{Name: command.QueueName, ID: command.QueueID}); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}

		encodedReceipt := tx.Bucket(receiptsBucket).Get([]byte(command.ReceiptHandle))
		if encodedReceipt == nil {
			return queue.ErrReceiptHandleIsInvalid
		}
		var receipt receiptRecord
		if err := decodeRecord(encodedReceipt, &receipt); err != nil {
			return err
		}
		if err := receipt.validate([]byte(command.ReceiptHandle)); err != nil {
			return err
		}
		if receipt.QueueName != command.QueueName {
			return queue.ErrReceiptHandleIsInvalid
		}
		encodedMessageID := tx.Bucket(messageIDsBucket).Get([]byte(receipt.MessageID))
		if encodedMessageID == nil {
			return corruptf("receipt is missing message ID history")
		}
		var messageID messageIDRecord
		if err := decodeRecord(encodedMessageID, &messageID); err != nil {
			return err
		}
		if err := messageID.validate([]byte(receipt.MessageID)); err != nil {
			return err
		}

		messages := tx.Bucket(messagesBucket)
		encodedMessage := messages.Get([]byte(receipt.MessageID))
		if encodedMessage == nil {
			return nil
		}
		var message messageRecord
		if err := decodeRecord(encodedMessage, &message); err != nil {
			return err
		}
		if err := message.validate(); err != nil {
			return err
		}
		if message.ID != receipt.MessageID {
			return corruptf("receipt and message ID bindings differ")
		}
		if message.QueueName != command.QueueName {
			return nil
		}
		if receipt.Generation > message.ReceiveGeneration {
			return corruptf("receipt generation exceeds active message generation")
		}
		if message.ExpiresAtUnixNanos <= command.Now.UnixNano() {
			return deleteActiveMessage(tx, command.QueueName, message.ID)
		}
		if receipt.Generation < message.ReceiveGeneration {
			return nil
		}
		if message.ReceiptHandle != command.ReceiptHandle {
			return corruptf("current message receipt binding is inconsistent")
		}

		message.VisibilityDeadlineUnixNanos = command.Now.Add(command.VisibilityTimeout).UnixNano()
		messageValue, err := encodeRecord(message)
		if err != nil {
			return err
		}
		if err := messages.Put([]byte(message.ID), messageValue); err != nil {
			return fmt.Errorf("store visibility deadline: %w", err)
		}
		return nil
	})
	return wrapOperationError("change message visibility", err)
}

func (r *Repository) Expire(command queue.ExpireCommand) (int, error) {
	expired := 0
	err := r.update(func(tx *bolt.Tx) error {
		orders := tx.Bucket(messageOrderBucket)
		messages := tx.Bucket(messagesBucket)
		return orders.ForEach(func(queueKey, value []byte) error {
			if value != nil {
				return corruptf("message order bucket contains a non-bucket entry")
			}
			if tx.Bucket(queuesBucket).Get(queueKey) == nil {
				return corruptf("message order exists for a missing queue")
			}
			ordered := orders.Bucket(queueKey)
			cursor := ordered.Cursor()
			for orderKey, messageID := cursor.First(); orderKey != nil; orderKey, messageID = cursor.Next() {
				if messageID == nil || len(orderKey) != 8 || binary.BigEndian.Uint64(orderKey) == 0 {
					return corruptf("message order entry is malformed")
				}
				encoded := messages.Get(messageID)
				if encoded == nil {
					return corruptf("message order refers to a missing message")
				}
				var record messageRecord
				if err := decodeRecord(encoded, &record); err != nil {
					return err
				}
				if err := record.validate(); err != nil {
					return err
				}
				if record.QueueName != string(queueKey) || record.ID != string(messageID) {
					return corruptf("message order crosses queue or ID boundary")
				}
				if record.ExpiresAtUnixNanos > command.Now.UnixNano() {
					continue
				}
				message, err := record.domain()
				if err != nil {
					return err
				}
				if err := changeTenantUsage(tx, record.QueueName, 0, -1, -int64(payloadSize(message))); err != nil {
					return err
				}
				if err := messages.Delete(messageID); err != nil {
					return fmt.Errorf("delete expired message: %w", err)
				}
				if err := tx.Bucket(redriveOriginsBucket).Delete(messageID); err != nil {
					return err
				}
				if err := tx.Bucket(fifoMessagesBucket).Delete(messageID); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return fmt.Errorf("delete expired message order: %w", err)
				}
				expired++
			}
			return nil
		})
	})
	if err != nil {
		return 0, wrapOperationError("expire messages", err)
	}
	return expired, nil
}

func (r *Repository) ListQueues(command queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	result := queue.ListQueuesPage{Queues: []queue.Queue{}}
	err := r.view(func(tx *bolt.Tx) error {
		revision, err := namespaceRevision(tx)
		if err != nil {
			return err
		}
		if command.ExpectedRevision != nil && *command.ExpectedRevision != revision {
			return queue.ErrInvalidPaginationToken
		}
		result.NamespaceRevision = revision
		limit := command.Limit
		if limit < 1 {
			limit = 1000
		}
		cursor := tx.Bucket(queuesBucket).Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			name := string(key)
			if name <= command.After || len(name) < len(command.Prefix) || name[:len(command.Prefix)] != command.Prefix {
				continue
			}
			if len(result.Queues) == limit {
				result.HasMore = true
				break
			}
			value, found, err := getQueue(tx, name)
			if err != nil {
				return err
			}
			if !found {
				return corruptf("listed queue disappeared")
			}
			result.Queues = append(result.Queues, value)
		}
		return nil
	})
	if err != nil {
		return queue.ListQueuesPage{}, wrapOperationError("list queues", err)
	}
	return result, nil
}

func (r *Repository) DeleteQueue(ref queue.QueueRef) error {
	err := r.update(func(tx *bolt.Tx) error {
		value, found, err := getQueueByRef(tx, ref)
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrQueueDoesNotExist
		}
		if err := tx.Bucket(redrivePoliciesBucket).ForEach(func(key, encoded []byte) error {
			var policy redrivePolicyRecord
			if err := decodeRecord(encoded, &policy); err != nil {
				return err
			}
			if policy.SourceName != ref.Name && policy.TargetName == ref.Name && policy.TargetID == value.ID {
				return queue.ErrQueueInUse
			}
			return nil
		}); err != nil {
			return err
		}
		if err := tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
			var record moveTaskRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			task, err := record.domain()
			if err != nil {
				return err
			}
			if task.Status == queue.MoveTaskRunning && (task.Source == ref || task.Destination != nil && *task.Destination == ref) {
				return queue.ErrQueueInUse
			}
			return nil
		}); err != nil {
			return err
		}
		if err := purgeQueueState(tx, ref.Name, false); err != nil {
			return err
		}
		if err := changeTenantUsage(tx, ref.Name, -1, 0, 0); err != nil {
			return err
		}
		if err := tx.Bucket(messageOrderBucket).DeleteBucket([]byte(ref.Name)); err != nil {
			return err
		}
		for _, bucket := range [][]byte{queuesBucket, queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket} {
			if err := tx.Bucket(bucket).Delete([]byte(ref.Name)); err != nil {
				return err
			}
		}
		for _, bucket := range [][]byte{fifoQueuesBucket, fifoSequencesBucket} {
			if err := tx.Bucket(bucket).Delete([]byte(ref.Name)); err != nil {
				return err
			}
		}
		for _, bucket := range [][]byte{fifoDedupBucket, fifoAttemptsBucket} {
			if err := deleteFIFOQueueRecords(tx.Bucket(bucket), value.ID); err != nil {
				return err
			}
		}
		if tx.Bucket(redrivePoliciesBucket).Get([]byte(ref.Name)) != nil {
			if err := tx.Bucket(redrivePoliciesBucket).Delete([]byte(ref.Name)); err != nil {
				return err
			}
			if err := incrementRedriveRevision(tx); err != nil {
				return err
			}
		}
		var taskKeys [][]byte
		if err := tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
			var record moveTaskRecord
			if err := decodeRecord(encoded, &record); err != nil {
				return err
			}
			if record.SourceName == ref.Name && record.SourceID == value.ID {
				taskKeys = append(taskKeys, append([]byte(nil), key...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, key := range taskKeys {
			if err := tx.Bucket(moveTasksBucket).Delete(key); err != nil {
				return err
			}
		}
		tombstone, err := encodeRecord(queueTombstoneRecord{Version: administrationRecordVersion, Name: ref.Name, LastQueueID: value.ID})
		if err != nil {
			return err
		}
		if err := tx.Bucket(queueTombstonesBucket).Put([]byte(ref.Name), tombstone); err != nil {
			return err
		}
		return incrementNamespaceRevision(tx)
	})
	return wrapOperationError("delete queue", err)
}

func (r *Repository) PurgeQueue(ref queue.QueueRef) error {
	err := r.update(func(tx *bolt.Tx) error {
		value, found, err := getQueueByRef(tx, ref)
		if err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		if err := purgeQueueState(tx, ref.Name, true); err != nil {
			return err
		}
		return deleteFIFOQueueRecords(tx.Bucket(fifoAttemptsBucket), value.ID)
	})
	return wrapOperationError("purge queue", err)
}

func purgeQueueState(tx *bolt.Tx, queueName string, recreateOrder bool) error {
	orders := tx.Bucket(messageOrderBucket)
	ordered := orders.Bucket([]byte(queueName))
	if ordered == nil {
		return corruptf("queue is missing its message order bucket")
	}
	var messageIDs [][]byte
	if err := ordered.ForEach(func(key, value []byte) error {
		messageIDs = append(messageIDs, append([]byte(nil), value...))
		return nil
	}); err != nil {
		return err
	}
	for _, messageID := range messageIDs {
		encoded := tx.Bucket(messagesBucket).Get(messageID)
		if encoded == nil {
			return corruptf("purged message is missing")
		}
		var record messageRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		message, err := record.domain()
		if err != nil {
			return err
		}
		if err := changeTenantUsage(tx, queueName, 0, -1, -int64(payloadSize(message))); err != nil {
			return err
		}
		if err := tx.Bucket(messagesBucket).Delete(messageID); err != nil {
			return err
		}
		if err := tx.Bucket(redriveOriginsBucket).Delete(messageID); err != nil {
			return err
		}
		if err := tx.Bucket(fifoMessagesBucket).Delete(messageID); err != nil {
			return err
		}
	}
	var receiptKeys [][]byte
	if err := tx.Bucket(receiptsBucket).ForEach(func(key, value []byte) error {
		var record receiptRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if record.QueueName == queueName {
			receiptKeys = append(receiptKeys, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range receiptKeys {
		if err := tx.Bucket(receiptsBucket).Delete(key); err != nil {
			return err
		}
	}
	if recreateOrder {
		if err := orders.DeleteBucket([]byte(queueName)); err != nil {
			return err
		}
		if _, err := orders.CreateBucket([]byte(queueName)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) TagQueue(ref queue.QueueRef, updates map[string]string) error {
	err := r.update(func(tx *bolt.Tx) error {
		value, found, err := getQueueByRef(tx, ref)
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getTagsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		for key, tagValue := range updates {
			record.Tags[key] = tagValue
		}
		if len(record.Tags) > 50 {
			return queue.ErrOverLimit
		}
		record.QueueID = value.ID
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(queueTagsBucket).Put([]byte(ref.Name), encoded)
	})
	return wrapOperationError("tag queue", err)
}

func (r *Repository) UntagQueue(ref queue.QueueRef, keys []string) error {
	err := r.update(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, ref); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getTagsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		for _, key := range keys {
			delete(record.Tags, key)
		}
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(queueTagsBucket).Put([]byte(ref.Name), encoded)
	})
	return wrapOperationError("untag queue", err)
}

func (r *Repository) ListQueueTags(ref queue.QueueRef) (map[string]string, error) {
	result := map[string]string{}
	err := r.view(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, ref); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getTagsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		for key, value := range record.Tags {
			result[key] = value
		}
		return nil
	})
	if err != nil {
		return nil, wrapOperationError("list queue tags", err)
	}
	return result, nil
}

func getTagsRecord(tx *bolt.Tx, name string) (queueTagsRecord, error) {
	encoded := tx.Bucket(queueTagsBucket).Get([]byte(name))
	if encoded == nil {
		return queueTagsRecord{}, corruptf("queue tag metadata is missing")
	}
	var record queueTagsRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return queueTagsRecord{}, err
	}
	return record, nil
}

func (r *Repository) AddPermission(ref queue.QueueRef, permission queue.Permission) error {
	err := r.update(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, ref); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getPermissionsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		replaced := false
		for index := range record.Permissions {
			if record.Permissions[index].Label == permission.Label {
				record.Permissions[index] = permission
				replaced = true
				break
			}
		}
		if !replaced {
			if len(record.Permissions) >= 20 {
				return queue.ErrOverLimit
			}
			record.Permissions = append(record.Permissions, permission)
		}
		record.Permissions = sortedPermissionCopy(record.Permissions)
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(queuePermissionsBucket).Put([]byte(ref.Name), encoded)
	})
	return wrapOperationError("add permission", err)
}

func (r *Repository) RemovePermission(ref queue.QueueRef, label string) error {
	err := r.update(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, ref); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getPermissionsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		remaining := record.Permissions[:0]
		for _, permission := range record.Permissions {
			if permission.Label != label {
				remaining = append(remaining, permission)
			}
		}
		record.Permissions = remaining
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		return tx.Bucket(queuePermissionsBucket).Put([]byte(ref.Name), encoded)
	})
	return wrapOperationError("remove permission", err)
}

func (r *Repository) ListPermissions(ref queue.QueueRef) ([]queue.Permission, error) {
	var result []queue.Permission
	err := r.view(func(tx *bolt.Tx) error {
		if _, found, err := getQueueByRef(tx, ref); err != nil {
			return err
		} else if !found {
			return queue.ErrQueueDoesNotExist
		}
		record, err := getPermissionsRecord(tx, ref.Name)
		if err != nil {
			return err
		}
		result = make([]queue.Permission, len(record.Permissions))
		for index, permission := range record.Permissions {
			result[index] = permission
			result[index].AWSAccountIDs = append([]string(nil), permission.AWSAccountIDs...)
			result[index].Actions = append([]string(nil), permission.Actions...)
		}
		return nil
	})
	if err != nil {
		return nil, wrapOperationError("list permissions", err)
	}
	return result, nil
}

func getPermissionsRecord(tx *bolt.Tx, name string) (queuePermissionsRecord, error) {
	encoded := tx.Bucket(queuePermissionsBucket).Get([]byte(name))
	if encoded == nil {
		return queuePermissionsRecord{}, corruptf("queue permission metadata is missing")
	}
	var record queuePermissionsRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return queuePermissionsRecord{}, err
	}
	return record, nil
}

func deleteActiveMessage(tx *bolt.Tx, queueName, messageID string) error {
	ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(queueName))
	if ordered == nil {
		return corruptf("queue is missing its message order bucket")
	}
	var orderKey []byte
	cursor := ordered.Cursor()
	for key, orderedMessageID := cursor.First(); key != nil; key, orderedMessageID = cursor.Next() {
		if orderedMessageID == nil || len(key) != 8 || binary.BigEndian.Uint64(key) == 0 {
			return corruptf("message order entry is malformed")
		}
		if string(orderedMessageID) != messageID {
			continue
		}
		if orderKey != nil {
			return corruptf("message appears more than once in message order")
		}
		orderKey = append([]byte(nil), key...)
	}
	if orderKey == nil {
		return corruptf("message is missing from message order")
	}
	encoded := tx.Bucket(messagesBucket).Get([]byte(messageID))
	if encoded == nil {
		return corruptf("active message is missing")
	}
	var record messageRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return err
	}
	message, err := record.domain()
	if err != nil {
		return err
	}
	if err := changeTenantUsage(tx, queueName, 0, -1, -int64(payloadSize(message))); err != nil {
		return err
	}
	if err := tx.Bucket(messagesBucket).Delete([]byte(messageID)); err != nil {
		return fmt.Errorf("delete active message: %w", err)
	}
	if err := tx.Bucket(redriveOriginsBucket).Delete([]byte(messageID)); err != nil {
		return err
	}
	if err := tx.Bucket(fifoMessagesBucket).Delete([]byte(messageID)); err != nil {
		return err
	}
	if err := ordered.Delete(orderKey); err != nil {
		return fmt.Errorf("delete message order: %w", err)
	}
	return nil
}

func (r *Repository) Health() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return queue.ErrRepositoryClosed
	}
	r.transactions.Lock()
	defer r.transactions.Unlock()
	err := r.db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, validateTransaction) })
	return wrapOperationError("check repository health", err)
}

func (r *Repository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if err := r.db.Close(); err != nil {
		return unavailable("close bbolt repository", err)
	}
	return nil
}

func (r *Repository) update(callback func(*bolt.Tx) error) error {
	if tx := r.ambientTx.Load(); tx != nil {
		return callTransactionSafely(tx, callback)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return queue.ErrRepositoryClosed
	}
	r.transactions.RLock()
	defer r.transactions.RUnlock()
	return r.updateFn(func(tx *bolt.Tx) error { return callTransactionSafely(tx, callback) })
}

func (r *Repository) view(callback func(*bolt.Tx) error) error {
	if tx := r.ambientTx.Load(); tx != nil {
		return callTransactionSafely(tx, callback)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return queue.ErrRepositoryClosed
	}
	r.transactions.RLock()
	defer r.transactions.RUnlock()
	return r.db.View(func(tx *bolt.Tx) error { return callTransactionSafely(tx, callback) })
}

func callTransactionSafely(tx *bolt.Tx, callback func(*bolt.Tx) error) (err error) {
	defer func() {
		if recover() != nil {
			err = corruptf("storage transaction failed unexpectedly")
		}
	}()
	return callback(tx)
}

func getQueue(tx *bolt.Tx, name string) (queue.Queue, bool, error) {
	encoded := tx.Bucket(queuesBucket).Get([]byte(name))
	if encoded == nil {
		return queue.Queue{}, false, nil
	}
	var record queueRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return queue.Queue{}, false, err
	}
	value, err := record.domain()
	if err != nil {
		return queue.Queue{}, false, err
	}
	if value.Name != name {
		return queue.Queue{}, false, corruptf("queue record key does not match its name")
	}
	encodedIdentity := tx.Bucket(queueIdentitiesBucket).Get([]byte(name))
	if encodedIdentity == nil {
		return queue.Queue{}, false, corruptf("queue is missing identity metadata")
	}
	var identity queueIdentityRecord
	if err := decodeRecord(encodedIdentity, &identity); err != nil {
		return queue.Queue{}, false, err
	}
	if identity.Version != administrationRecordVersion || identity.Name != name || !wellFormedQueueID(identity.ID) {
		return queue.Queue{}, false, corruptf("queue identity record is invalid")
	}
	value.ID = identity.ID
	value.LegacyURLAllowed = identity.LegacyURLAllowed
	if encodedFIFO := tx.Bucket(fifoQueuesBucket).Get([]byte(name)); encodedFIFO != nil {
		var fifo fifoQueueRecord
		if err := decodeRecord(encodedFIFO, &fifo); err != nil {
			return queue.Queue{}, false, err
		}
		if fifo.Version != fifoRecordVersion || fifo.QueueID != value.ID {
			return queue.Queue{}, false, corruptf("FIFO queue record is invalid")
		}
		value.FIFO, value.ContentBasedDeduplication = true, fifo.ContentBasedDeduplication
	}
	encodedPolicy := tx.Bucket(redrivePoliciesBucket).Get([]byte(name))
	if encodedPolicy != nil {
		var policy redrivePolicyRecord
		if err := decodeRecord(encodedPolicy, &policy); err != nil {
			return queue.Queue{}, false, err
		}
		if policy.Version != redriveRecordVersion || policy.SourceName != name || policy.SourceID != value.ID || !validStoredQueueName(policy.TargetName) || !wellFormedQueueID(policy.TargetID) || policy.MaxReceiveCount < 1 || policy.MaxReceiveCount > 1000 {
			return queue.Queue{}, false, corruptf("redrive policy record is invalid")
		}
		value.RedrivePolicy = &queue.RedrivePolicy{DeadLetterTarget: queue.QueueRef{Name: policy.TargetName, ID: policy.TargetID}, MaxReceiveCount: policy.MaxReceiveCount}
	}
	return value, true, nil
}

func getQueueByRef(tx *bolt.Tx, ref queue.QueueRef) (queue.Queue, bool, error) {
	value, found, err := getQueue(tx, ref.Name)
	if err != nil || !found {
		return value, found, err
	}
	if ref.ID != "" && ref.ID != value.ID || ref.ID == "" && !value.LegacyURLAllowed {
		return queue.Queue{}, false, nil
	}
	return value, true, nil
}

func namespaceRevision(tx *bolt.Tx) (uint64, error) {
	value := tx.Bucket(metadataBucket).Get(namespaceRevisionKey)
	if len(value) != 8 {
		return 0, corruptf("namespace revision is missing or malformed")
	}
	return binary.BigEndian.Uint64(value), nil
}

func incrementNamespaceRevision(tx *bolt.Tx) error {
	current, err := namespaceRevision(tx)
	if err != nil {
		return err
	}
	if current == ^uint64(0) {
		return fmt.Errorf("namespace revision exhausted")
	}
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, current+1)
	return tx.Bucket(metadataBucket).Put(namespaceRevisionKey, value)
}

func redriveRevision(tx *bolt.Tx) (uint64, error) {
	value := tx.Bucket(metadataBucket).Get(redriveRevisionKey)
	if len(value) != 8 {
		return 0, corruptf("redrive revision is missing or malformed")
	}
	return binary.BigEndian.Uint64(value), nil
}

func incrementRedriveRevision(tx *bolt.Tx) error {
	current, err := redriveRevision(tx)
	if err != nil {
		return err
	}
	if current == ^uint64(0) {
		return fmt.Errorf("redrive revision exhausted")
	}
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, current+1)
	return tx.Bucket(metadataBucket).Put(redriveRevisionKey, value)
}

func fifoScopedKey(queueID, value string) []byte { return []byte(queueID + "\x00" + value) }

func allocateFIFOSequence(tx *bolt.Tx, value queue.Queue) (uint64, error) {
	encoded := tx.Bucket(fifoSequencesBucket).Get([]byte(value.Name))
	if encoded == nil {
		return 0, corruptf("FIFO sequence record is missing")
	}
	var record fifoSequenceRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return 0, err
	}
	if record.Version != fifoRecordVersion || record.QueueID != value.ID || record.Next == ^uint64(0) {
		return 0, corruptf("FIFO sequence record is invalid or exhausted")
	}
	record.Next++
	updated, err := encodeRecord(record)
	if err != nil {
		return 0, err
	}
	if err := tx.Bucket(fifoSequencesBucket).Put([]byte(value.Name), updated); err != nil {
		return 0, err
	}
	return record.Next, nil
}

func deleteFIFOQueueRecords(bucket *bolt.Bucket, queueID string) error {
	prefix := []byte(queueID + "\x00")
	cursor := bucket.Cursor()
	for key, _ := cursor.Seek(prefix); key != nil && len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix); key, _ = cursor.Next() {
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func cleanupFIFODedup(tx *bolt.Tx, queueID string, now time.Time) error {
	prefix := []byte(queueID + "\x00")
	cursor := tx.Bucket(fifoDedupBucket).Cursor()
	for key, encoded := cursor.Seek(prefix); key != nil && len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix); key, encoded = cursor.Next() {
		var record fifoDedupRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.ExpiresAtUnixNanos <= now.UnixNano() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
	}
	return nil
}

func cleanupFIFOAttempts(tx *bolt.Tx, queueID string, now time.Time) error {
	prefix := []byte(queueID + "\x00")
	cursor := tx.Bucket(fifoAttemptsBucket).Cursor()
	for key, encoded := cursor.Seek(prefix); key != nil && len(key) >= len(prefix) && string(key[:len(prefix)]) == string(prefix); key, encoded = cursor.Next() {
		var record fifoAttemptRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.ExpiresAtUnixNanos <= now.UnixNano() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
	}
	return nil
}

func storeFIFOAttempt(tx *bolt.Tx, queueID, attemptID string, messages []queue.Message, now time.Time) error {
	record := fifoAttemptRecord{Version: fifoRecordVersion, QueueID: queueID, AttemptID: attemptID, Messages: make([]fifoAttemptMessageRecord, len(messages)), ExpiresAtUnixNanos: now.Add(5 * time.Minute).UnixNano()}
	for index := range messages {
		record.Messages[index] = fifoAttemptMessageRecord{Message: newMessageRecord(messages[index]), GroupID: messages[index].MessageGroupID, DeduplicationID: messages[index].MessageDeduplicationID, Sequence: messages[index].SequenceNumber}
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		return err
	}
	return tx.Bucket(fifoAttemptsBucket).Put(fifoScopedKey(queueID, attemptID), encoded)
}

func decodeFIFOAttempt(encoded []byte, attemptID, queueID string) ([]queue.Message, error) {
	var record fifoAttemptRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return nil, err
	}
	if record.Version != fifoRecordVersion || record.QueueID != queueID || record.AttemptID != attemptID || record.Messages == nil {
		return nil, corruptf("FIFO receive attempt record is invalid")
	}
	result := make([]queue.Message, len(record.Messages))
	for index := range record.Messages {
		item := record.Messages[index]
		value, err := item.Message.domain()
		if err != nil {
			return nil, err
		}
		if item.GroupID == "" || item.DeduplicationID == "" || item.Sequence == 0 {
			return nil, corruptf("FIFO receive attempt message is invalid")
		}
		value.MessageGroupID, value.MessageDeduplicationID, value.SequenceNumber = item.GroupID, item.DeduplicationID, item.Sequence
		result[index] = value
	}
	return result, nil
}

func wrapOperationError(operation string, err error) error {
	var invalidRequest *queue.InvalidRequestError
	if errors.As(err, &invalidRequest) {
		return err
	}
	if err == nil || errors.Is(err, queue.ErrQueueAlreadyExists) || errors.Is(err, queue.ErrQueueDoesNotExist) || errors.Is(err, queue.ErrQueueIDUnavailable) || errors.Is(err, queue.ErrMessageIDExists) || errors.Is(err, queue.ErrReceiptHandleExists) || errors.Is(err, queue.ErrReceiptUnavailable) || errors.Is(err, queue.ErrReceiptHandleIsInvalid) || errors.Is(err, queue.ErrInvalidPaginationToken) || errors.Is(err, queue.ErrOverLimit) || errors.Is(err, queue.ErrQueueInUse) || errors.Is(err, queue.ErrMoveTaskAlreadyRunning) || errors.Is(err, queue.ErrMoveTaskNotRunning) || errors.Is(err, queue.ErrMoveTaskDoesNotExist) || errors.Is(err, queue.ErrQueueIsNotDeadLetter) || errors.Is(err, queue.ErrQuotaExceeded) {
		return err
	}
	if errors.Is(err, queue.ErrRepositoryUnavailable) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return unavailable(operation, err)
}

func unavailable(operation string, err error) error {
	return fmt.Errorf("%s: %w: %w", operation, queue.ErrRepositoryUnavailable, err)
}
