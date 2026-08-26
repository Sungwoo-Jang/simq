package boltrepo

import (
	"encoding/binary"
	"encoding/hex"

	bolt "go.etcd.io/bbolt"
)

// validateTransactionV1 is intentionally independent of the current codecs.
// Migration is allowed only after the complete legacy graph passes these rules.
func validateTransactionV1(tx *bolt.Tx) error {
	var physicalError error
	for err := range tx.Check() {
		if physicalError == nil {
			physicalError = err
		}
	}
	if physicalError != nil {
		return corruptf("physical database check failed: %v", physicalError)
	}
	knownBuckets := make(map[string]struct{}, len(baseRequiredBuckets))
	for _, name := range baseRequiredBuckets {
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
	for _, name := range baseRequiredBuckets {
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
		default:
			return corruptf("metadata contains unknown key %q", key)
		}
	}); err != nil {
		return err
	}
	version, err := transactionSchemaVersion(tx)
	if err != nil {
		return err
	}
	if version != legacySchemaVersion {
		return corruptf("unsupported schema version %d", version)
	}
	installationID := metadata.Get(installationIDKey)
	if len(installationID) != 32 {
		return corruptf("installation ID is missing or malformed")
	}
	decodedInstallationID, err := hex.DecodeString(string(installationID))
	if err != nil || len(decodedInstallationID) != 16 {
		return corruptf("installation ID is missing or malformed")
	}

	queues := make(map[string]queueRecordV1)
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("queue bucket contains an invalid entry")
		}
		var record queueRecordV1
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

	messages := make(map[string]messageRecordV1)
	if err := tx.Bucket(messagesBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("message bucket contains an invalid entry")
		}
		var record messageRecordV1
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
		var record messageIDRecordV1
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

	receipts := make(map[string]receiptRecordV1)
	if err := tx.Bucket(receiptsBucket).ForEach(func(key, value []byte) error {
		if value == nil || len(key) == 0 {
			return corruptf("receipt bucket contains an invalid entry")
		}
		var record receiptRecordV1
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
		if message, ok := messages[record.MessageID]; ok {
			if message.QueueName != record.QueueName || record.Generation > message.ReceiveGeneration {
				return corruptf("receipt binding conflicts with active message")
			}
		}
		receipts[record.Handle] = record
		return nil
	}); err != nil {
		return err
	}
	for _, message := range messages {
		if message.ReceiveGeneration == 0 {
			continue
		}
		receipt, ok := receipts[message.ReceiptHandle]
		if !ok || receipt.MessageID != message.ID || receipt.QueueName != message.QueueName || receipt.Generation != message.ReceiveGeneration {
			return corruptf("active message current receipt binding is missing or inconsistent")
		}
	}
	return nil
}
