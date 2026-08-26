package boltrepo

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

const (
	schemaVersion       uint32 = 9
	schemaVersionV8     uint32 = 8
	schemaVersionV7     uint32 = 7
	schemaVersionV6     uint32 = 6
	schemaVersionV5     uint32 = 5
	schemaVersionV4     uint32 = 4
	schemaVersionV3     uint32 = 3
	schemaVersionV2     uint32 = 2
	legacySchemaVersion uint32 = 1
)

var (
	metadataBucket             = []byte("metadata")
	queuesBucket               = []byte("queues")
	messagesBucket             = []byte("messages")
	messageOrderBucket         = []byte("message_order")
	messageIDsBucket           = []byte("message_ids")
	receiptsBucket             = []byte("receipts")
	queueIdentitiesBucket      = []byte("queue_identities")
	queueTagsBucket            = []byte("queue_tags")
	queuePermissionsBucket     = []byte("queue_permissions")
	queueTombstonesBucket      = []byte("queue_tombstones")
	redrivePoliciesBucket      = []byte("redrive_policies")
	redriveOriginsBucket       = []byte("redrive_origins")
	moveTasksBucket            = []byte("move_tasks")
	fifoQueuesBucket           = []byte("fifo_queues")
	fifoSequencesBucket        = []byte("fifo_sequences")
	fifoMessagesBucket         = []byte("fifo_messages")
	fifoDedupBucket            = []byte("fifo_dedup")
	fifoAttemptsBucket         = []byte("fifo_attempts")
	replicationProposalsBucket = []byte("replication_proposals")
	tenantUsageBucket          = []byte("tenant_usage")
	schemaVersionKey           = []byte("schema_version")
	installationIDKey          = []byte("installation_id")
	namespaceRevisionKey       = []byte("namespace_revision")
	redriveRevisionKey         = []byte("redrive_revision")
	appliedRaftIndexKey        = []byte("applied_raft_index")
	commandProtocolVersionKey  = []byte("command_protocol_version")
	shardCatalogRevisionKey    = []byte("shard_catalog_revision")
	baseRequiredBuckets        = [][]byte{metadataBucket, queuesBucket, messagesBucket, messageOrderBucket, messageIDsBucket, receiptsBucket}
	requiredBucketsV4          = [][]byte{metadataBucket, queuesBucket, messagesBucket, messageOrderBucket, messageIDsBucket, receiptsBucket, queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket, queueTombstonesBucket}
	requiredBucketsV5          = [][]byte{metadataBucket, queuesBucket, messagesBucket, messageOrderBucket, messageIDsBucket, receiptsBucket, queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket, queueTombstonesBucket, redrivePoliciesBucket, redriveOriginsBucket, moveTasksBucket}
	requiredBucketsV6          = [][]byte{metadataBucket, queuesBucket, messagesBucket, messageOrderBucket, messageIDsBucket, receiptsBucket, queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket, queueTombstonesBucket, redrivePoliciesBucket, redriveOriginsBucket, moveTasksBucket, fifoQueuesBucket, fifoSequencesBucket, fifoMessagesBucket, fifoDedupBucket, fifoAttemptsBucket}
	requiredBucketsV7          = append(append([][]byte(nil), requiredBucketsV6...), replicationProposalsBucket)
	requiredBuckets            = append(append([][]byte(nil), requiredBucketsV7...), tenantUsageBucket)
)

func initializeSchema(tx *bolt.Tx, installationID string) error {
	for _, name := range requiredBuckets {
		if _, err := tx.CreateBucket(name); err != nil {
			return fmt.Errorf("create required bucket: %w", err)
		}
	}
	metadata := tx.Bucket(metadataBucket)
	version := make([]byte, 4)
	binary.BigEndian.PutUint32(version, schemaVersion)
	if err := metadata.Put(schemaVersionKey, version); err != nil {
		return fmt.Errorf("write schema version: %w", err)
	}
	if err := metadata.Put(installationIDKey, []byte(installationID)); err != nil {
		return fmt.Errorf("write installation ID: %w", err)
	}
	revision := make([]byte, 8)
	if err := metadata.Put(namespaceRevisionKey, revision); err != nil {
		return fmt.Errorf("write namespace revision: %w", err)
	}
	if err := metadata.Put(redriveRevisionKey, revision); err != nil {
		return fmt.Errorf("write redrive revision: %w", err)
	}
	if err := metadata.Put(appliedRaftIndexKey, make([]byte, 8)); err != nil {
		return fmt.Errorf("write applied Raft index: %w", err)
	}
	protocol := make([]byte, 4)
	binary.BigEndian.PutUint32(protocol, replicatedCommandProtocolVersion)
	if err := metadata.Put(commandProtocolVersionKey, protocol); err != nil {
		return fmt.Errorf("write command protocol version: %w", err)
	}
	return nil
}

func transactionSchemaVersion(tx *bolt.Tx) (uint32, error) {
	metadata := tx.Bucket(metadataBucket)
	if metadata == nil {
		return 0, corruptf("required bucket %q is missing", metadataBucket)
	}
	version := metadata.Get(schemaVersionKey)
	if len(version) != 4 {
		return 0, corruptf("schema version is missing or malformed")
	}
	return binary.BigEndian.Uint32(version), nil
}

func validateTransaction(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBuckets, schemaVersion, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	if err := validateAdministrationBuckets(tx); err != nil {
		return err
	}
	if err := validateRedriveBuckets(tx); err != nil {
		return err
	}
	if err := validateFIFOBuckets(tx); err != nil {
		return err
	}
	if err := validateReplicationState(tx); err != nil {
		return err
	}
	return validateTenantUsage(tx)
}

func validateTransactionV8(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBuckets, schemaVersionV8, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	if err := validateAdministrationBuckets(tx); err != nil {
		return err
	}
	if err := validateRedriveBuckets(tx); err != nil {
		return err
	}
	if err := validateFIFOBuckets(tx); err != nil {
		return err
	}
	if err := validateReplicationState(tx); err != nil {
		return err
	}
	return validateTenantUsage(tx)
}

func validateTransactionV7(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBucketsV7, schemaVersionV7, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	if err := validateAdministrationBuckets(tx); err != nil {
		return err
	}
	if err := validateRedriveBuckets(tx); err != nil {
		return err
	}
	if err := validateFIFOBuckets(tx); err != nil {
		return err
	}
	return validateReplicationState(tx)
}

func validateTransactionV6(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBucketsV6, schemaVersionV6, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	if err := validateAdministrationBuckets(tx); err != nil {
		return err
	}
	if err := validateRedriveBuckets(tx); err != nil {
		return err
	}
	return validateFIFOBuckets(tx)
}

func validateTransactionV5(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBucketsV5, schemaVersionV5, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	if err := validateAdministrationBuckets(tx); err != nil {
		return err
	}
	return validateRedriveBuckets(tx)
}

func validateTransactionV4(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, requiredBucketsV4, schemaVersionV4, true); err != nil {
		return err
	}
	if err := validateDataBuckets(tx); err != nil {
		return err
	}
	return validateAdministrationBuckets(tx)
}

func validateTransactionV3(tx *bolt.Tx) error {
	if err := validateSchemaEnvelope(tx, baseRequiredBuckets, schemaVersionV3, false); err != nil {
		return err
	}
	return validateDataBuckets(tx)
}

func validateSchemaEnvelope(tx *bolt.Tx, buckets [][]byte, expectedVersion uint32, administration bool) error {
	var physicalError error
	for err := range tx.Check() {
		if physicalError == nil {
			physicalError = err
		}
	}
	if physicalError != nil {
		return corruptf("physical database check failed: %v", physicalError)
	}
	knownBuckets := make(map[string]struct{}, len(buckets))
	for _, name := range buckets {
		knownBuckets[string(name)] = struct{}{}
	}
	if err := tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
		if bucket == nil {
			return corruptf("top-level schema contains a non-bucket entry")
		}
		if _, ok := knownBuckets[string(name)]; !ok {
			return corruptf("schema contains unknown top-level bucket %q", name)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, name := range buckets {
		if tx.Bucket(name) == nil {
			return corruptf("required bucket %q is missing", name)
		}
	}

	metadata := tx.Bucket(metadataBucket)
	if err := metadata.ForEach(func(key, value []byte) error {
		if value == nil {
			return corruptf("metadata contains a nested bucket")
		}
		switch string(key) {
		case string(schemaVersionKey), string(installationIDKey):
			return nil
		case string(namespaceRevisionKey):
			if administration {
				return nil
			}
			return corruptf("metadata contains unknown key %q", key)
		case string(redriveRevisionKey):
			if expectedVersion >= schemaVersionV5 {
				return nil
			}
			return corruptf("metadata contains unknown key %q", key)
		case string(appliedRaftIndexKey), string(commandProtocolVersionKey):
			if expectedVersion >= schemaVersionV7 {
				return nil
			}
			return corruptf("metadata contains unknown key %q", key)
		case string(shardCatalogRevisionKey):
			if expectedVersion >= schemaVersion {
				return nil
			}
			return corruptf("metadata contains unknown key %q", key)
		default:
			return corruptf("metadata contains unknown key %q", key)
		}
	}); err != nil {
		return err
	}
	version := metadata.Get(schemaVersionKey)
	if len(version) != 4 {
		return corruptf("schema version is missing or malformed")
	}
	if actual := binary.BigEndian.Uint32(version); actual != expectedVersion {
		return corruptf("unsupported schema version %d", actual)
	}
	installationID := metadata.Get(installationIDKey)
	if len(installationID) != 32 {
		return corruptf("installation ID is missing or malformed")
	}
	decodedInstallationID, err := hex.DecodeString(string(installationID))
	if err != nil || len(decodedInstallationID) != 16 {
		return corruptf("installation ID is missing or malformed")
	}
	if administration && len(metadata.Get(namespaceRevisionKey)) != 8 {
		return corruptf("namespace revision is missing or malformed")
	}
	if expectedVersion >= schemaVersionV5 && len(metadata.Get(redriveRevisionKey)) != 8 {
		return corruptf("redrive revision is missing or malformed")
	}
	if expectedVersion >= schemaVersionV7 {
		if len(metadata.Get(appliedRaftIndexKey)) != 8 {
			return corruptf("applied Raft index is missing or malformed")
		}
		protocol := metadata.Get(commandProtocolVersionKey)
		if len(protocol) != 4 || binary.BigEndian.Uint32(protocol) != replicatedCommandProtocolVersion {
			return corruptf("command protocol version is missing or unsupported")
		}
	}
	if revision := metadata.Get(shardCatalogRevisionKey); revision != nil {
		decoded, err := hex.DecodeString(string(revision))
		if expectedVersion < schemaVersion || err != nil || len(decoded) != 16 || string(revision) != strings.ToLower(string(revision)) {
			return corruptf("shard catalog revision is malformed or unsupported")
		}
	}
	return nil
}

func validateDataBuckets(tx *bolt.Tx) error {
	queues := make(map[string]queueRecord)
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("queue bucket contains an invalid entry")
		}
		var record queueRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if _, err := record.domain(); err != nil {
			return err
		}
		if record.Name != string(key) {
			return corruptf("queue record key does not match its name")
		}
		queues[record.Name] = record
		return nil
	}); err != nil {
		return err
	}

	messages := make(map[string]messageRecord)
	if err := tx.Bucket(messagesBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("message bucket contains an invalid entry")
		}
		var record messageRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if err := record.validate(); err != nil {
			return err
		}
		if record.ID != string(key) {
			return corruptf("message record key does not match its ID")
		}
		if _, ok := queues[record.QueueName]; !ok {
			return corruptf("message refers to a missing queue")
		}
		messages[record.ID] = record
		return nil
	}); err != nil {
		return err
	}

	issuedMessageIDs := make(map[string]struct{})
	if err := tx.Bucket(messageIDsBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("message ID bucket contains an invalid entry")
		}
		var record messageIDRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if err := record.validate(key); err != nil {
			return err
		}
		issuedMessageIDs[record.ID] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	for id := range messages {
		if _, ok := issuedMessageIDs[id]; !ok {
			return corruptf("active message is missing ID history")
		}
	}

	orderedMessages := make(map[string]struct{})
	orders := tx.Bucket(messageOrderBucket)
	if err := orders.ForEach(func(queueKey, value []byte) error {
		if value != nil {
			return corruptf("message order bucket contains a non-bucket entry")
		}
		queueName := string(queueKey)
		if _, ok := queues[queueName]; !ok {
			return corruptf("message order exists for a missing queue")
		}
		ordered := orders.Bucket(queueKey)
		return ordered.ForEach(func(sequenceKey, messageID []byte) error {
			if len(sequenceKey) != 8 || binary.BigEndian.Uint64(sequenceKey) == 0 || len(messageID) == 0 {
				return corruptf("message order entry is malformed")
			}
			record, ok := messages[string(messageID)]
			if !ok || record.QueueName != queueName {
				return corruptf("message order entry is dangling or crosses queues")
			}
			if _, duplicate := orderedMessages[record.ID]; duplicate {
				return corruptf("active message appears more than once in message order")
			}
			orderedMessages[record.ID] = struct{}{}
			return nil
		})
	}); err != nil {
		return err
	}
	for queueName := range queues {
		if orders.Bucket([]byte(queueName)) == nil {
			return corruptf("queue is missing its message order bucket")
		}
	}
	for id := range messages {
		if _, ok := orderedMessages[id]; !ok {
			return corruptf("active message is missing from message order")
		}
	}

	receipts := make(map[string]receiptRecord)
	if err := tx.Bucket(receiptsBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("receipt bucket contains an invalid entry")
		}
		var record receiptRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if err := record.validate(key); err != nil {
			return err
		}
		if _, ok := queues[record.QueueName]; !ok {
			return corruptf("receipt refers to a missing queue")
		}
		if _, ok := issuedMessageIDs[record.MessageID]; !ok {
			return corruptf("receipt refers to an unissued message ID")
		}
		if message, ok := messages[record.MessageID]; ok && message.QueueName == record.QueueName {
			if record.Generation > message.ReceiveGeneration {
				return corruptf("receipt binding conflicts with active message")
			}
		}
		receipts[record.Handle] = record
		return nil
	}); err != nil {
		return err
	}
	for _, message := range messages {
		if message.ReceiptHandle == "" {
			continue
		}
		receipt, ok := receipts[message.ReceiptHandle]
		if !ok || receipt.MessageID != message.ID || receipt.QueueName != message.QueueName || receipt.Generation != message.ReceiveGeneration {
			return corruptf("active message current receipt binding is missing or inconsistent")
		}
	}
	return nil
}

func validateAdministrationBuckets(tx *bolt.Tx) error {
	queues := make(map[string]struct{})
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error { queues[string(key)] = struct{}{}; return nil }); err != nil {
		return err
	}
	identities := make(map[string]queueIdentityRecord)
	ids := make(map[string]struct{})
	if err := tx.Bucket(queueIdentitiesBucket).ForEach(func(key, value []byte) error {
		if value == nil {
			return corruptf("queue identity bucket contains a nested bucket")
		}
		var record queueIdentityRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		name := string(key)
		if record.Version != administrationRecordVersion || record.Name != name || !validStoredQueueName(name) || !wellFormedQueueID(record.ID) {
			return corruptf("queue identity record is invalid")
		}
		if _, ok := queues[name]; !ok {
			return corruptf("queue identity refers to a missing queue")
		}
		if _, duplicate := ids[record.ID]; duplicate {
			return corruptf("queue identity is duplicated")
		}
		ids[record.ID] = struct{}{}
		identities[name] = record
		return nil
	}); err != nil {
		return err
	}
	for name := range queues {
		if _, ok := identities[name]; !ok {
			return corruptf("queue is missing identity metadata")
		}
	}
	validateSets := func(bucketName []byte, permissions bool) error {
		seen := make(map[string]struct{})
		err := tx.Bucket(bucketName).ForEach(func(key, value []byte) error {
			if value == nil {
				return corruptf("administrative metadata contains a nested bucket")
			}
			name := string(key)
			identity, ok := identities[name]
			if !ok {
				return corruptf("administrative metadata refers to a missing queue")
			}
			if permissions {
				var record queuePermissionsRecord
				if err := decodeRecord(value, &record); err != nil {
					return err
				}
				if record.Version != administrationRecordVersion || record.QueueID != identity.ID || record.Permissions == nil || len(record.Permissions) > 20 {
					return corruptf("queue permission metadata is invalid")
				}
				previous := ""
				for index, permission := range record.Permissions {
					if index > 0 && permission.Label <= previous || queue.ValidatePermissionMetadata(permission) != nil {
						return corruptf("queue permission statement is invalid")
					}
					previous = permission.Label
				}
			} else {
				var record queueTagsRecord
				if err := decodeRecord(value, &record); err != nil {
					return err
				}
				if record.Version != administrationRecordVersion || record.QueueID != identity.ID || record.Tags == nil || len(record.Tags) > 50 {
					return corruptf("queue tag metadata is invalid")
				}
				for key, value := range record.Tags {
					if !utf8.ValidString(key) || len(key) < 1 || len(key) > 128 || strings.HasPrefix(strings.ToLower(key), "simq:") || !utf8.ValidString(value) || len(value) > 256 {
						return corruptf("queue tag is invalid")
					}
				}
			}
			seen[name] = struct{}{}
			return nil
		})
		if err != nil {
			return err
		}
		for name := range queues {
			if _, ok := seen[name]; !ok {
				return corruptf("queue is missing administrative metadata")
			}
		}
		return nil
	}
	if err := validateSets(queueTagsBucket, false); err != nil {
		return err
	}
	if err := validateSets(queuePermissionsBucket, true); err != nil {
		return err
	}
	return tx.Bucket(queueTombstonesBucket).ForEach(func(key, value []byte) error {
		if value == nil {
			return corruptf("queue tombstone bucket contains a nested bucket")
		}
		var record queueTombstoneRecord
		if err := decodeRecord(value, &record); err != nil {
			return err
		}
		if record.Version != administrationRecordVersion || record.Name != string(key) || !validStoredQueueName(record.Name) || !wellFormedQueueID(record.LastQueueID) {
			return corruptf("queue tombstone is invalid")
		}
		if identity, live := identities[record.Name]; live && identity.ID == record.LastQueueID {
			return corruptf("queue tombstone points at live generation")
		}
		return nil
	})
}

func wellFormedQueueID(value string) bool {
	if len(value) != 34 || !strings.HasPrefix(value, "q_") {
		return false
	}
	for _, character := range value[2:] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func validateRedriveBuckets(tx *bolt.Tx) error {
	live := make(map[queue.QueueRef]struct{})
	if err := tx.Bucket(queueIdentitiesBucket).ForEach(func(key, encoded []byte) error {
		var identity queueIdentityRecord
		if err := decodeRecord(encoded, &identity); err != nil {
			return err
		}
		live[queue.QueueRef{Name: identity.Name, ID: identity.ID}] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	policies := make(map[queue.QueueRef]queue.QueueRef)
	if err := tx.Bucket(redrivePoliciesBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("redrive policy bucket contains a nested bucket")
		}
		var record redrivePolicyRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		source := queue.QueueRef{Name: record.SourceName, ID: record.SourceID}
		target := queue.QueueRef{Name: record.TargetName, ID: record.TargetID}
		if record.Version != redriveRecordVersion || string(key) != record.SourceName || record.MaxReceiveCount < 1 || record.MaxReceiveCount > 1000 || source == target {
			return corruptf("redrive policy record is invalid")
		}
		if _, ok := live[source]; !ok {
			return corruptf("redrive policy source is missing")
		}
		if _, ok := live[target]; !ok {
			return corruptf("redrive policy target is missing")
		}
		if fifoQueues := tx.Bucket(fifoQueuesBucket); fifoQueues != nil && (fifoQueues.Get([]byte(record.SourceName)) != nil) != (fifoQueues.Get([]byte(record.TargetName)) != nil) {
			return corruptf("redrive policy crosses Standard and FIFO queue types")
		}
		policies[source] = target
		return nil
	}); err != nil {
		return err
	}
	for source := range policies {
		seen := map[queue.QueueRef]struct{}{}
		for cursor := source; ; {
			if _, duplicate := seen[cursor]; duplicate {
				return corruptf("redrive policy graph contains a cycle")
			}
			seen[cursor] = struct{}{}
			next, ok := policies[cursor]
			if !ok {
				break
			}
			cursor = next
		}
	}
	if err := tx.Bucket(redriveOriginsBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("redrive origin bucket contains a nested bucket")
		}
		var record redriveOriginRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.Version != redriveRecordVersion || record.MessageID != string(key) || tx.Bucket(messagesBucket).Get(key) == nil || !validStoredQueueName(record.SourceName) || !wellFormedQueueID(record.SourceID) {
			return corruptf("redrive origin record is invalid")
		}
		if fifoQueues := tx.Bucket(fifoQueuesBucket); fifoQueues != nil {
			var message messageRecord
			if err := decodeRecord(tx.Bucket(messagesBucket).Get(key), &message); err != nil {
				return err
			}
			if _, sourceLive := live[queue.QueueRef{Name: record.SourceName, ID: record.SourceID}]; sourceLive && (fifoQueues.Get([]byte(record.SourceName)) != nil) != (fifoQueues.Get([]byte(message.QueueName)) != nil) {
				return corruptf("redrive origin crosses Standard and FIFO queue types")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return tx.Bucket(moveTasksBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("move task bucket contains a nested bucket")
		}
		var record moveTaskRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		task, err := record.domain()
		if err != nil {
			return err
		}
		if task.Handle != string(key) {
			return corruptf("move task key does not match its handle")
		}
		if task.Status == queue.MoveTaskRunning {
			if _, ok := live[task.Source]; !ok {
				return corruptf("running move task source is missing")
			}
			if task.Destination != nil {
				if _, ok := live[*task.Destination]; !ok {
					return corruptf("running move task destination is missing")
				}
				if fifoQueues := tx.Bucket(fifoQueuesBucket); fifoQueues != nil && (fifoQueues.Get([]byte(task.Source.Name)) != nil) != (fifoQueues.Get([]byte(task.Destination.Name)) != nil) {
					return corruptf("running move task crosses Standard and FIFO queue types")
				}
			}
		}
		return nil
	})
}

func validateFIFOBuckets(tx *bolt.Tx) error {
	type liveQueue struct {
		name string
		id   string
	}
	liveByName := make(map[string]liveQueue)
	liveByID := make(map[string]liveQueue)
	if err := tx.Bucket(queueIdentitiesBucket).ForEach(func(key, encoded []byte) error {
		var identity queueIdentityRecord
		if err := decodeRecord(encoded, &identity); err != nil {
			return err
		}
		value := liveQueue{name: string(key), id: identity.ID}
		liveByName[value.name], liveByID[value.id] = value, value
		return nil
	}); err != nil {
		return err
	}

	fifoByName := make(map[string]fifoQueueRecord)
	if err := tx.Bucket(fifoQueuesBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("FIFO queue bucket contains a nested bucket")
		}
		name := string(key)
		live, ok := liveByName[name]
		if !ok {
			return corruptf("FIFO queue metadata refers to a missing queue")
		}
		var record fifoQueueRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.Version != fifoRecordVersion || record.QueueID != live.id || !strings.HasSuffix(name, ".fifo") {
			return corruptf("FIFO queue metadata is invalid")
		}
		fifoByName[name] = record
		return nil
	}); err != nil {
		return err
	}
	for name := range liveByName {
		_, fifo := fifoByName[name]
		if strings.HasSuffix(name, ".fifo") != fifo {
			return corruptf("queue name and FIFO metadata disagree")
		}
	}

	sequences := make(map[string]fifoSequenceRecord)
	if err := tx.Bucket(fifoSequencesBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("FIFO sequence bucket contains a nested bucket")
		}
		name := string(key)
		fifo, ok := fifoByName[name]
		if !ok {
			return corruptf("FIFO sequence refers to a non-FIFO queue")
		}
		var record fifoSequenceRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.Version != fifoRecordVersion || record.QueueID != fifo.QueueID {
			return corruptf("FIFO sequence record is invalid")
		}
		sequences[name] = record
		return nil
	}); err != nil {
		return err
	}
	for name := range fifoByName {
		if _, ok := sequences[name]; !ok {
			return corruptf("FIFO queue is missing its sequence record")
		}
	}

	activeFIFO := make(map[string]fifoMessageRecord)
	activeSequences := make(map[string]map[uint64]struct{})
	if err := tx.Bucket(fifoMessagesBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("FIFO message bucket contains a nested bucket")
		}
		messageValue := tx.Bucket(messagesBucket).Get(key)
		if messageValue == nil {
			return corruptf("FIFO message metadata refers to a missing message")
		}
		var message messageRecord
		if err := decodeRecord(messageValue, &message); err != nil {
			return err
		}
		live, ok := liveByName[message.QueueName]
		if !ok {
			return corruptf("FIFO message refers to a missing queue")
		}
		sequenceRecord, fifo := sequences[message.QueueName]
		var record fifoMessageRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if !fifo || record.Version != fifoRecordVersion || record.MessageID != string(key) || record.MessageID != message.ID || record.QueueID != live.id || !validFIFOIdentifier(record.GroupID) || !validFIFOIdentifier(record.DeduplicationID) || record.Sequence == 0 || record.Sequence > sequenceRecord.Next {
			return corruptf("FIFO message metadata is invalid")
		}
		if activeSequences[live.id] == nil {
			activeSequences[live.id] = make(map[uint64]struct{})
		}
		if _, duplicate := activeSequences[live.id][record.Sequence]; duplicate {
			return corruptf("FIFO sequence is duplicated among active messages")
		}
		activeSequences[live.id][record.Sequence] = struct{}{}
		activeFIFO[record.MessageID] = record
		return nil
	}); err != nil {
		return err
	}
	if err := tx.Bucket(messagesBucket).ForEach(func(key, encoded []byte) error {
		var message messageRecord
		if err := decodeRecord(encoded, &message); err != nil {
			return err
		}
		_, fifoQueue := fifoByName[message.QueueName]
		_, fifoMessage := activeFIFO[string(key)]
		if fifoQueue != fifoMessage {
			return corruptf("active message and FIFO metadata disagree")
		}
		return nil
	}); err != nil {
		return err
	}

	if err := tx.Bucket(fifoDedupBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("FIFO deduplication bucket contains a nested bucket")
		}
		var record fifoDedupRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		live, ok := liveByID[record.QueueID]
		sequence, fifo := sequences[live.name]
		if !ok || !fifo || record.Version != fifoRecordVersion || string(key) != string(fifoScopedKey(record.QueueID, record.DeduplicationID)) || !validFIFOIdentifier(record.DeduplicationID) || !validFIFOIdentifier(record.GroupID) || record.MessageID == "" || tx.Bucket(messageIDsBucket).Get([]byte(record.MessageID)) == nil || !validMD5(record.MD5OfBody) || record.MD5OfMessageAttributes != "" && !validMD5(record.MD5OfMessageAttributes) || record.Sequence == 0 || record.Sequence > sequence.Next || record.ExpiresAtUnixNanos <= 0 {
			return corruptf("FIFO deduplication record is invalid")
		}
		return nil
	}); err != nil {
		return err
	}

	return tx.Bucket(fifoAttemptsBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("FIFO receive attempt bucket contains a nested bucket")
		}
		var record fifoAttemptRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		live, ok := liveByID[record.QueueID]
		sequence, fifo := sequences[live.name]
		if !ok || !fifo || record.Version != fifoRecordVersion || string(key) != string(fifoScopedKey(record.QueueID, record.AttemptID)) || !validFIFOIdentifier(record.AttemptID) || record.Messages == nil || record.ExpiresAtUnixNanos <= 0 {
			return corruptf("FIFO receive attempt record is invalid")
		}
		for _, item := range record.Messages {
			message, err := item.Message.domain()
			if err != nil {
				return err
			}
			if message.QueueName != live.name || !validFIFOIdentifier(item.GroupID) || !validFIFOIdentifier(item.DeduplicationID) || item.Sequence == 0 || item.Sequence > sequence.Next {
				return corruptf("FIFO receive attempt message is invalid")
			}
		}
		return nil
	})
}

func validFIFOIdentifier(value string) bool {
	if len(value) < 1 || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validMD5(value string) bool {
	if len(value) != 32 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sortedPermissionCopy(values []queue.Permission) []queue.Permission {
	result := append([]queue.Permission(nil), values...)
	sort.Slice(result, func(i, j int) bool { return result[i].Label < result[j].Label })
	return result
}

func corruptf(format string, arguments ...interface{}) error {
	return fmt.Errorf("%w: %s", queue.ErrRepositoryCorrupt, fmt.Sprintf(format, arguments...))
}
