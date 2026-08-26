package boltrepo

import (
	"encoding/hex"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

const tenantUsageRecordVersion uint32 = 1
const legacyTenantUsageKey = "legacy"

type tenantUsageRecord struct {
	Version                        uint32 `json:"version"`
	Namespace                      string `json:"namespace"`
	Queues, Messages, PayloadBytes uint64
}

func tenantNamespaceForQueue(name string) string {
	if len(name) >= 68 && strings.HasPrefix(name, "tn_") && name[67] == '_' {
		namespace := name[3:67]
		if decoded, err := hex.DecodeString(namespace); err == nil && len(decoded) == 32 {
			return namespace
		}
	}
	return legacyTenantUsageKey
}
func payloadSize(message queue.Message) uint64 {
	size := uint64(len(message.Body))
	for name, attribute := range message.MessageAttributes {
		size += uint64(len(name) + len(attribute.DataType) + len(attribute.StringValue) + len(attribute.BinaryValue))
	}
	return size
}
func readTenantUsage(tx *bolt.Tx, namespace string) (tenantUsageRecord, error) {
	encoded := tx.Bucket(tenantUsageBucket).Get([]byte(namespace))
	if encoded == nil {
		return tenantUsageRecord{Version: tenantUsageRecordVersion, Namespace: namespace}, nil
	}
	var record tenantUsageRecord
	if err := decodeRecord(encoded, &record); err != nil {
		return tenantUsageRecord{}, err
	}
	if record.Version != tenantUsageRecordVersion || record.Namespace != namespace {
		return tenantUsageRecord{}, corruptf("tenant usage record is invalid")
	}
	return record, nil
}
func writeTenantUsage(tx *bolt.Tx, record tenantUsageRecord) error {
	bucket := tx.Bucket(tenantUsageBucket)
	if record.Queues == 0 && record.Messages == 0 && record.PayloadBytes == 0 {
		return bucket.Delete([]byte(record.Namespace))
	}
	encoded, err := encodeRecord(record)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(record.Namespace), encoded)
}
func changeTenantUsage(tx *bolt.Tx, queueName string, queues, messages int64, payloadBytes int64) error {
	namespace := tenantNamespaceForQueue(queueName)
	record, err := readTenantUsage(tx, namespace)
	if err != nil {
		return err
	}
	nextQueues, ok := applyDelta(record.Queues, queues)
	if !ok {
		return corruptf("tenant queue usage underflow")
	}
	nextMessages, ok := applyDelta(record.Messages, messages)
	if !ok {
		return corruptf("tenant message usage underflow")
	}
	nextBytes, ok := applyDelta(record.PayloadBytes, payloadBytes)
	if !ok {
		return corruptf("tenant payload usage underflow")
	}
	record.Queues, record.Messages, record.PayloadBytes = nextQueues, nextMessages, nextBytes
	return writeTenantUsage(tx, record)
}
func applyDelta(value uint64, delta int64) (uint64, bool) {
	if delta >= 0 {
		next := value + uint64(delta)
		return next, next >= value
	}
	amount := uint64(-delta)
	if amount > value {
		return 0, false
	}
	return value - amount, true
}

func rebuildTenantUsage(tx *bolt.Tx) error {
	bucket := tx.Bucket(tenantUsageBucket)
	if bucket == nil {
		return corruptf("tenant usage bucket is missing")
	}
	var keys [][]byte
	if err := bucket.ForEach(func(key, value []byte) error { keys = append(keys, append([]byte(nil), key...)); return nil }); err != nil {
		return err
	}
	for _, key := range keys {
		if err := bucket.Delete(key); err != nil {
			return err
		}
	}
	usage := make(map[string]tenantUsageRecord)
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error {
		namespace := tenantNamespaceForQueue(string(key))
		record := usage[namespace]
		record.Version = tenantUsageRecordVersion
		record.Namespace = namespace
		record.Queues++
		usage[namespace] = record
		return nil
	}); err != nil {
		return err
	}
	if err := tx.Bucket(messagesBucket).ForEach(func(key, encoded []byte) error {
		var record messageRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		message, err := record.domain()
		if err != nil {
			return err
		}
		namespace := tenantNamespaceForQueue(record.QueueName)
		current := usage[namespace]
		current.Version = tenantUsageRecordVersion
		current.Namespace = namespace
		current.Messages++
		current.PayloadBytes += payloadSize(message)
		usage[namespace] = current
		return nil
	}); err != nil {
		return err
	}
	for _, record := range usage {
		if err := writeTenantUsage(tx, record); err != nil {
			return err
		}
	}
	return nil
}

func validateTenantUsage(tx *bolt.Tx) error {
	expected := make(map[string]tenantUsageRecord)
	if err := tx.Bucket(queuesBucket).ForEach(func(key, value []byte) error {
		namespace := tenantNamespaceForQueue(string(key))
		record := expected[namespace]
		record.Version = tenantUsageRecordVersion
		record.Namespace = namespace
		record.Queues++
		expected[namespace] = record
		return nil
	}); err != nil {
		return err
	}
	if err := tx.Bucket(messagesBucket).ForEach(func(key, encoded []byte) error {
		var stored messageRecord
		if err := decodeRecord(encoded, &stored); err != nil {
			return err
		}
		message, err := stored.domain()
		if err != nil {
			return err
		}
		namespace := tenantNamespaceForQueue(stored.QueueName)
		record := expected[namespace]
		record.Version = tenantUsageRecordVersion
		record.Namespace = namespace
		record.Messages++
		record.PayloadBytes += payloadSize(message)
		expected[namespace] = record
		return nil
	}); err != nil {
		return err
	}
	seen := make(map[string]struct{})
	if err := tx.Bucket(tenantUsageBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("tenant usage contains a nested bucket")
		}
		namespace := string(key)
		record, err := readTenantUsage(tx, namespace)
		if err != nil {
			return err
		}
		want, ok := expected[namespace]
		if !ok || record != want {
			return corruptf("tenant usage does not match authoritative state")
		}
		seen[namespace] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	for namespace := range expected {
		if _, ok := seen[namespace]; !ok {
			return corruptf("tenant usage is missing for namespace %q", namespace)
		}
	}
	return nil
}

func tenantUsageForName(tx *bolt.Tx, name string) (tenantUsageRecord, error) {
	if tx.Bucket(tenantUsageBucket) == nil {
		return tenantUsageRecord{}, fmt.Errorf("tenant usage is unavailable")
	}
	return readTenantUsage(tx, tenantNamespaceForQueue(name))
}

func checkTenantQuota(tx *bolt.Tx, name string, quota queue.StorageQuota, addQueues, addMessages, addBytes uint64) error {
	usage, err := tenantUsageForName(tx, name)
	if err != nil {
		return err
	}
	exceeds := func(current, add, limit uint64) bool { return limit > 0 && (current > limit || add > limit-current) }
	if exceeds(usage.Queues, addQueues, quota.MaxQueues) || exceeds(usage.Messages, addMessages, quota.MaxMessages) || exceeds(usage.PayloadBytes, addBytes, quota.MaxPayloadBytes) {
		return queue.ErrQuotaExceeded
	}
	return nil
}
