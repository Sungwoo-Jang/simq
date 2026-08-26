package boltrepo

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"simq/internal/queue"
)

const (
	recordVersion       uint32 = 3
	recordVersionV2     uint32 = 2
	legacyRecordVersion uint32 = 1
)

type queueRecord struct {
	Version    uint32            `json:"version"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
}

const administrationRecordVersion uint32 = 1

type queueIdentityRecord struct {
	Version          uint32 `json:"version"`
	Name             string `json:"name"`
	ID               string `json:"id"`
	LegacyURLAllowed bool   `json:"legacy_url_allowed"`
}

type queueTagsRecord struct {
	Version uint32            `json:"version"`
	QueueID string            `json:"queue_id"`
	Tags    map[string]string `json:"tags"`
}

type queuePermissionsRecord struct {
	Version     uint32             `json:"version"`
	QueueID     string             `json:"queue_id"`
	Permissions []queue.Permission `json:"permissions"`
}

type queueTombstoneRecord struct {
	Version     uint32 `json:"version"`
	Name        string `json:"name"`
	LastQueueID string `json:"last_queue_id"`
}

const redriveRecordVersion uint32 = 1
const fifoRecordVersion uint32 = 1

type fifoQueueRecord struct {
	Version                   uint32 `json:"version"`
	QueueID                   string `json:"queue_id"`
	ContentBasedDeduplication bool   `json:"content_based_deduplication"`
}
type fifoSequenceRecord struct {
	Version uint32 `json:"version"`
	QueueID string `json:"queue_id"`
	Next    uint64 `json:"next"`
}
type fifoMessageRecord struct {
	Version         uint32 `json:"version"`
	MessageID       string `json:"message_id"`
	QueueID         string `json:"queue_id"`
	GroupID         string `json:"group_id"`
	DeduplicationID string `json:"deduplication_id"`
	Sequence        uint64 `json:"sequence"`
}
type fifoDedupRecord struct {
	Version                uint32 `json:"version"`
	QueueID                string `json:"queue_id"`
	DeduplicationID        string `json:"deduplication_id"`
	GroupID                string `json:"group_id"`
	MessageID              string `json:"message_id"`
	MD5OfBody              string `json:"md5_of_body"`
	MD5OfMessageAttributes string `json:"md5_of_message_attributes"`
	Sequence               uint64 `json:"sequence"`
	ExpiresAtUnixNanos     int64  `json:"expires_at_unix_nanos"`
}
type fifoAttemptRecord struct {
	Version            uint32                     `json:"version"`
	QueueID            string                     `json:"queue_id"`
	AttemptID          string                     `json:"attempt_id"`
	Messages           []fifoAttemptMessageRecord `json:"messages"`
	ExpiresAtUnixNanos int64                      `json:"expires_at_unix_nanos"`
}
type fifoAttemptMessageRecord struct {
	Message         messageRecord `json:"message"`
	GroupID         string        `json:"group_id"`
	DeduplicationID string        `json:"deduplication_id"`
	Sequence        uint64        `json:"sequence"`
}

type redrivePolicyRecord struct {
	Version         uint32 `json:"version"`
	SourceName      string `json:"source_name"`
	SourceID        string `json:"source_id"`
	TargetName      string `json:"target_name"`
	TargetID        string `json:"target_id"`
	MaxReceiveCount uint64 `json:"max_receive_count"`
}

type redriveOriginRecord struct {
	Version    uint32 `json:"version"`
	MessageID  string `json:"message_id"`
	SourceName string `json:"source_name"`
	SourceID   string `json:"source_id"`
}

type moveTaskRecord struct {
	Version              uint32 `json:"version"`
	Handle               string `json:"handle"`
	SourceName           string `json:"source_name"`
	SourceID             string `json:"source_id"`
	DestinationName      string `json:"destination_name"`
	DestinationID        string `json:"destination_id"`
	MaxMessagesPerSecond int    `json:"max_messages_per_second"`
	StartedAtMillis      int64  `json:"started_at_millis"`
	Status               string `json:"status"`
	FailureReason        string `json:"failure_reason"`
	MessagesMoved        uint64 `json:"messages_moved"`
	MessagesToMove       uint64 `json:"messages_to_move"`
	Boundary             uint64 `json:"boundary"`
}

func newMoveTaskRecord(value queue.MessageMoveTask) moveTaskRecord {
	record := moveTaskRecord{Version: redriveRecordVersion, Handle: value.Handle, SourceName: value.Source.Name, SourceID: value.Source.ID, MaxMessagesPerSecond: value.MaxNumberOfMessagesPerSecond, StartedAtMillis: value.StartedAtMillis, Status: string(value.Status), FailureReason: value.FailureReason, MessagesMoved: value.ApproximateNumberOfMessagesMoved, MessagesToMove: value.ApproximateNumberOfMessagesToMove, Boundary: value.Boundary}
	if value.Destination != nil {
		record.DestinationName, record.DestinationID = value.Destination.Name, value.Destination.ID
	}
	return record
}

func (r moveTaskRecord) domain() (queue.MessageMoveTask, error) {
	status := queue.MoveTaskStatus(r.Status)
	if r.Version != redriveRecordVersion || r.Handle == "" || !wellFormedQueueID(r.SourceID) || !validStoredQueueName(r.SourceName) || r.MaxMessagesPerSecond < 1 || r.MaxMessagesPerSecond > 500 || r.MessagesMoved > r.MessagesToMove || (status != queue.MoveTaskRunning && status != queue.MoveTaskCompleted && status != queue.MoveTaskCancelled && status != queue.MoveTaskFailed) {
		return queue.MessageMoveTask{}, corruptf("message move task record is invalid")
	}
	if (r.DestinationName == "") != (r.DestinationID == "") || r.DestinationName != "" && (!validStoredQueueName(r.DestinationName) || !wellFormedQueueID(r.DestinationID)) {
		return queue.MessageMoveTask{}, corruptf("message move task destination is invalid")
	}
	if status == queue.MoveTaskFailed && r.FailureReason == "" || status != queue.MoveTaskFailed && r.FailureReason != "" {
		return queue.MessageMoveTask{}, corruptf("message move task failure state is invalid")
	}
	result := queue.MessageMoveTask{Handle: r.Handle, Source: queue.QueueRef{Name: r.SourceName, ID: r.SourceID}, MaxNumberOfMessagesPerSecond: r.MaxMessagesPerSecond, StartedAtMillis: r.StartedAtMillis, Status: status, FailureReason: r.FailureReason, ApproximateNumberOfMessagesMoved: r.MessagesMoved, ApproximateNumberOfMessagesToMove: r.MessagesToMove, Boundary: r.Boundary}
	if r.DestinationName != "" {
		result.Destination = &queue.QueueRef{Name: r.DestinationName, ID: r.DestinationID}
	}
	return result, nil
}

type messageRecord struct {
	Version                     uint32                   `json:"version"`
	ID                          string                   `json:"id"`
	QueueName                   string                   `json:"queue_name"`
	Body                        string                   `json:"body"`
	MD5OfBody                   string                   `json:"md5_of_body"`
	SentAtUnixMillis            int64                    `json:"sent_at_unix_millis"`
	ReceiptHandle               string                   `json:"receipt_handle"`
	ReceiveCount                uint64                   `json:"receive_count"`
	ReceiveGeneration           uint64                   `json:"receive_generation"`
	FirstReceivedAtUnixMillis   int64                    `json:"first_received_at_unix_millis"`
	VisibilityDeadlineUnixNanos int64                    `json:"visibility_deadline_unix_nanos"`
	AvailableAtUnixNanos        int64                    `json:"available_at_unix_nanos"`
	ExpiresAtUnixNanos          int64                    `json:"expires_at_unix_nanos"`
	MessageAttributes           []messageAttributeRecord `json:"message_attributes"`
	MD5OfMessageAttributes      string                   `json:"md5_of_message_attributes"`
}

type messageAttributeRecord struct {
	Version     uint32 `json:"version"`
	Name        string `json:"name"`
	DataType    string `json:"data_type"`
	StringValue string `json:"string_value"`
	BinaryValue []byte `json:"binary_value"`
}

type queueRecordV1 struct {
	Version    uint32            `json:"version"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
}

type messageRecordV1 struct {
	Version                     uint32 `json:"version"`
	ID                          string `json:"id"`
	QueueName                   string `json:"queue_name"`
	Body                        string `json:"body"`
	MD5OfBody                   string `json:"md5_of_body"`
	SentAtUnixMillis            int64  `json:"sent_at_unix_millis"`
	ReceiptHandle               string `json:"receipt_handle"`
	ReceiveCount                uint64 `json:"receive_count"`
	ReceiveGeneration           uint64 `json:"receive_generation"`
	FirstReceivedAtUnixMillis   int64  `json:"first_received_at_unix_millis"`
	VisibilityDeadlineUnixNanos int64  `json:"visibility_deadline_unix_nanos"`
}

type queueRecordV2 struct {
	Version    uint32            `json:"version"`
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
}

type messageRecordV2 struct {
	Version                     uint32 `json:"version"`
	ID                          string `json:"id"`
	QueueName                   string `json:"queue_name"`
	Body                        string `json:"body"`
	MD5OfBody                   string `json:"md5_of_body"`
	SentAtUnixMillis            int64  `json:"sent_at_unix_millis"`
	ReceiptHandle               string `json:"receipt_handle"`
	ReceiveCount                uint64 `json:"receive_count"`
	ReceiveGeneration           uint64 `json:"receive_generation"`
	FirstReceivedAtUnixMillis   int64  `json:"first_received_at_unix_millis"`
	VisibilityDeadlineUnixNanos int64  `json:"visibility_deadline_unix_nanos"`
	AvailableAtUnixNanos        int64  `json:"available_at_unix_nanos"`
	ExpiresAtUnixNanos          int64  `json:"expires_at_unix_nanos"`
}

type messageIDRecordV1 struct {
	Version uint32 `json:"version"`
	ID      string `json:"id"`
}

type receiptRecordV1 struct {
	Version    uint32 `json:"version"`
	Handle     string `json:"handle"`
	QueueName  string `json:"queue_name"`
	MessageID  string `json:"message_id"`
	Generation uint64 `json:"generation"`
}

type messageIDRecordV2 struct {
	Version uint32 `json:"version"`
	ID      string `json:"id"`
}

type receiptRecordV2 struct {
	Version    uint32 `json:"version"`
	Handle     string `json:"handle"`
	QueueName  string `json:"queue_name"`
	MessageID  string `json:"message_id"`
	Generation uint64 `json:"generation"`
}

type messageIDRecord struct {
	Version uint32 `json:"version"`
	ID      string `json:"id"`
}

type receiptRecord struct {
	Version    uint32 `json:"version"`
	Handle     string `json:"handle"`
	QueueName  string `json:"queue_name"`
	MessageID  string `json:"message_id"`
	Generation uint64 `json:"generation"`
}

func newQueueRecord(value queue.Queue) queueRecord {
	return queueRecord{
		Version: recordVersion,
		Name:    value.Name,
		Attributes: map[string]string{
			"VisibilityTimeout":      strconv.Itoa(value.VisibilityTimeout),
			"DelaySeconds":           strconv.Itoa(value.DelaySeconds),
			"MessageRetentionPeriod": strconv.Itoa(value.MessageRetentionPeriod),
		},
	}
}

func newQueueRecordV2(value queue.Queue) queueRecordV2 {
	return queueRecordV2{
		Version: recordVersionV2,
		Name:    value.Name,
		Attributes: map[string]string{
			"VisibilityTimeout":      strconv.Itoa(value.VisibilityTimeout),
			"DelaySeconds":           strconv.Itoa(value.DelaySeconds),
			"MessageRetentionPeriod": strconv.Itoa(value.MessageRetentionPeriod),
		},
	}
}

func (r queueRecord) domain() (queue.Queue, error) {
	if r.Version != recordVersion {
		return queue.Queue{}, corruptf("unsupported queue record version %d", r.Version)
	}
	if !validStoredQueueName(r.Name) || r.Attributes == nil || len(r.Attributes) != 3 {
		return queue.Queue{}, corruptf("queue record has missing or unsupported fields")
	}
	value, ok := r.Attributes["VisibilityTimeout"]
	if !ok || value == "" {
		return queue.Queue{}, corruptf("queue record is missing VisibilityTimeout")
	}
	visibility, err := strconv.Atoi(value)
	if err != nil || visibility < 0 || visibility > 43200 {
		return queue.Queue{}, corruptf("queue record has invalid VisibilityTimeout")
	}
	delay, err := storedDecimalAttribute(r.Attributes, "DelaySeconds", 0, 900)
	if err != nil {
		return queue.Queue{}, err
	}
	retention, err := storedDecimalAttribute(r.Attributes, "MessageRetentionPeriod", 60, 1209600)
	if err != nil {
		return queue.Queue{}, err
	}
	return queue.Queue{
		Name:                   r.Name,
		VisibilityTimeout:      visibility,
		DelaySeconds:           delay,
		MessageRetentionPeriod: retention,
	}, nil
}

func (r queueRecordV2) domain() (queue.Queue, error) {
	if r.Version != recordVersionV2 {
		return queue.Queue{}, corruptf("unsupported queue record version %d", r.Version)
	}
	if !validStoredQueueName(r.Name) || r.Attributes == nil || len(r.Attributes) != 3 {
		return queue.Queue{}, corruptf("queue record has missing or unsupported fields")
	}
	visibility, err := storedDecimalAttribute(r.Attributes, "VisibilityTimeout", 0, 43200)
	if err != nil {
		return queue.Queue{}, err
	}
	delay, err := storedDecimalAttribute(r.Attributes, "DelaySeconds", 0, 900)
	if err != nil {
		return queue.Queue{}, err
	}
	retention, err := storedDecimalAttribute(r.Attributes, "MessageRetentionPeriod", 60, 1209600)
	if err != nil {
		return queue.Queue{}, err
	}
	return queue.Queue{Name: r.Name, VisibilityTimeout: visibility, DelaySeconds: delay, MessageRetentionPeriod: retention}, nil
}

func storedDecimalAttribute(attributes map[string]string, name string, minimum, maximum int) (int, error) {
	value, ok := attributes[name]
	if !ok || value == "" {
		return 0, corruptf("queue record is missing %s", name)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, corruptf("queue record has invalid %s", name)
	}
	return parsed, nil
}

func newMessageRecord(value queue.Message) messageRecord {
	record := messageRecord{
		Version:                   recordVersion,
		ID:                        value.ID,
		QueueName:                 value.QueueName,
		Body:                      value.Body,
		MD5OfBody:                 value.MD5OfBody,
		SentAtUnixMillis:          value.SentAtMillis,
		ReceiptHandle:             value.ReceiptHandle,
		ReceiveCount:              value.ReceiveCount,
		ReceiveGeneration:         value.ReceiveGeneration,
		FirstReceivedAtUnixMillis: value.FirstReceivedAtMillis,
		AvailableAtUnixNanos:      value.AvailableAt.UnixNano(),
		ExpiresAtUnixNanos:        value.ExpiresAt.UnixNano(),
		MD5OfMessageAttributes:    value.MD5OfMessageAttributes,
	}
	names := make([]string, 0, len(value.MessageAttributes))
	for name := range value.MessageAttributes {
		names = append(names, name)
	}
	sort.Strings(names)
	record.MessageAttributes = make([]messageAttributeRecord, 0, len(names))
	for _, name := range names {
		attribute := value.MessageAttributes[name]
		record.MessageAttributes = append(record.MessageAttributes, messageAttributeRecord{
			Version:     recordVersion,
			Name:        name,
			DataType:    attribute.DataType,
			StringValue: attribute.StringValue,
			BinaryValue: append([]byte(nil), attribute.BinaryValue...),
		})
	}
	if value.ReceiveGeneration > 0 {
		record.VisibilityDeadlineUnixNanos = value.VisibilityDeadline.UnixNano()
	}
	return record
}

func (r messageRecord) domain() (queue.Message, error) {
	if err := r.validate(); err != nil {
		return queue.Message{}, err
	}
	value := queue.Message{
		ID:                     r.ID,
		QueueName:              r.QueueName,
		Body:                   r.Body,
		MD5OfBody:              r.MD5OfBody,
		SentAtMillis:           r.SentAtUnixMillis,
		ReceiptHandle:          r.ReceiptHandle,
		ReceiveCount:           r.ReceiveCount,
		ReceiveGeneration:      r.ReceiveGeneration,
		FirstReceivedAtMillis:  r.FirstReceivedAtUnixMillis,
		AvailableAt:            time.Unix(0, r.AvailableAtUnixNanos).UTC(),
		ExpiresAt:              time.Unix(0, r.ExpiresAtUnixNanos).UTC(),
		MD5OfMessageAttributes: r.MD5OfMessageAttributes,
	}
	if len(r.MessageAttributes) > 0 {
		value.MessageAttributes = make(map[string]queue.MessageAttribute, len(r.MessageAttributes))
		for _, attribute := range r.MessageAttributes {
			value.MessageAttributes[attribute.Name] = queue.MessageAttribute{
				DataType:    attribute.DataType,
				StringValue: attribute.StringValue,
				BinaryValue: append([]byte(nil), attribute.BinaryValue...),
			}
		}
	}
	if r.ReceiveGeneration > 0 {
		value.VisibilityDeadline = time.Unix(0, r.VisibilityDeadlineUnixNanos).UTC()
	}
	return value, nil
}

func (r messageRecord) validate() error {
	if r.Version != recordVersion {
		return corruptf("unsupported message record version %d", r.Version)
	}
	encrypted := strings.HasPrefix(r.Body, "simqenc1:")
	maximumBodyBytes := queue.MaxMessageBodyBytes
	if encrypted {
		maximumBodyBytes = 4 * queue.MaxMessageBodyBytes
	}
	if r.ID == "" || r.QueueName == "" || len(r.Body) < 1 || len(r.Body) > maximumBodyBytes || !utf8.ValidString(r.Body) {
		return corruptf("message record has invalid required fields")
	}
	digest := md5.Sum([]byte(r.Body))
	if r.MD5OfBody != hex.EncodeToString(digest[:]) {
		return corruptf("message record body digest does not match")
	}
	if r.MessageAttributes == nil {
		return corruptf("message attribute records are missing")
	}
	attributes := make(map[string]queue.MessageAttribute, len(r.MessageAttributes))
	previousName := ""
	for index, attribute := range r.MessageAttributes {
		if attribute.Version != recordVersion || attribute.Name == "" || index > 0 && attribute.Name <= previousName {
			return corruptf("message attribute records are invalid or not strictly sorted")
		}
		previousName = attribute.Name
		attributes[attribute.Name] = queue.MessageAttribute{
			DataType:    attribute.DataType,
			StringValue: attribute.StringValue,
			BinaryValue: append([]byte(nil), attribute.BinaryValue...),
		}
	}
	if encrypted && len(attributes) != 0 {
		return corruptf("encrypted message contains plaintext attribute records")
	}
	if !encrypted {
		if err := queue.ValidateMessageAttributes(r.Body, attributes); err != nil {
			return corruptf("message attribute record is invalid")
		}
	}
	attributeDigest, err := queue.MessageAttributesMD5(attributes)
	if err != nil || attributeDigest != r.MD5OfMessageAttributes {
		return corruptf("message attribute digest does not match")
	}
	sentAt := time.UnixMilli(r.SentAtUnixMillis).UTC()
	availableAt := time.Unix(0, r.AvailableAtUnixNanos).UTC()
	expiresAt := time.Unix(0, r.ExpiresAtUnixNanos).UTC()
	if availableAt.Before(sentAt) || !expiresAt.After(sentAt) {
		return corruptf("message record has invalid lifecycle deadlines")
	}
	if r.ReceiveGeneration < r.ReceiveCount {
		return corruptf("message receive generation is behind its count")
	}
	if r.ReceiptHandle == "" {
		if r.ReceiveCount != 0 || r.FirstReceivedAtUnixMillis != 0 || r.VisibilityDeadlineUnixNanos != 0 {
			return corruptf("message without a current receipt has claim metadata")
		}
		return nil
	}
	if r.ReceiveCount == 0 || r.ReceiveGeneration == 0 || !wellFormedReceiptHandle(r.ReceiptHandle) {
		return corruptf("received message has invalid current receipt")
	}
	return nil
}

func (r messageRecordV2) validate() error {
	if r.Version != recordVersionV2 {
		return corruptf("unsupported message record version %d", r.Version)
	}
	if r.ID == "" || r.QueueName == "" || len(r.Body) < 1 || len(r.Body) > queue.MaxMessageBodyBytes || !utf8.ValidString(r.Body) {
		return corruptf("message record has invalid required fields")
	}
	digest := md5.Sum([]byte(r.Body))
	if r.MD5OfBody != hex.EncodeToString(digest[:]) {
		return corruptf("message record body digest does not match")
	}
	sentAt := time.UnixMilli(r.SentAtUnixMillis).UTC()
	availableAt := time.Unix(0, r.AvailableAtUnixNanos).UTC()
	expiresAt := time.Unix(0, r.ExpiresAtUnixNanos).UTC()
	if availableAt.Before(sentAt) || !expiresAt.After(sentAt) {
		return corruptf("message record has invalid lifecycle deadlines")
	}
	if r.ReceiveCount != r.ReceiveGeneration {
		return corruptf("message receive count and generation differ")
	}
	if r.ReceiveGeneration == 0 {
		if r.ReceiptHandle != "" || r.FirstReceivedAtUnixMillis != 0 || r.VisibilityDeadlineUnixNanos != 0 {
			return corruptf("unreceived message has claim metadata")
		}
		return nil
	}
	if !wellFormedReceiptHandle(r.ReceiptHandle) {
		return corruptf("received message has invalid current receipt")
	}
	return nil
}

func (r messageRecordV2) migrate() (messageRecord, error) {
	if err := r.validate(); err != nil {
		return messageRecord{}, err
	}
	return messageRecord{
		Version:                     recordVersion,
		ID:                          r.ID,
		QueueName:                   r.QueueName,
		Body:                        r.Body,
		MD5OfBody:                   r.MD5OfBody,
		SentAtUnixMillis:            r.SentAtUnixMillis,
		ReceiptHandle:               r.ReceiptHandle,
		ReceiveCount:                r.ReceiveCount,
		ReceiveGeneration:           r.ReceiveGeneration,
		FirstReceivedAtUnixMillis:   r.FirstReceivedAtUnixMillis,
		VisibilityDeadlineUnixNanos: r.VisibilityDeadlineUnixNanos,
		AvailableAtUnixNanos:        r.AvailableAtUnixNanos,
		ExpiresAtUnixNanos:          r.ExpiresAtUnixNanos,
		MessageAttributes:           make([]messageAttributeRecord, 0),
		MD5OfMessageAttributes:      "",
	}, nil
}

func (r queueRecordV1) domain() (queue.Queue, error) {
	if r.Version != legacyRecordVersion {
		return queue.Queue{}, corruptf("unsupported queue record version %d", r.Version)
	}
	if !validStoredQueueName(r.Name) || r.Attributes == nil || len(r.Attributes) != 1 {
		return queue.Queue{}, corruptf("queue record has missing or unsupported fields")
	}
	visibility, err := storedDecimalAttribute(r.Attributes, "VisibilityTimeout", 0, 43200)
	if err != nil {
		return queue.Queue{}, err
	}
	return queue.Queue{
		Name:                   r.Name,
		VisibilityTimeout:      visibility,
		DelaySeconds:           queue.DefaultDelaySeconds,
		MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod,
	}, nil
}

func (r messageRecordV1) validate() error {
	if r.Version != legacyRecordVersion {
		return corruptf("unsupported message record version %d", r.Version)
	}
	if r.ID == "" || r.QueueName == "" || len(r.Body) < 1 || len(r.Body) > queue.MaxMessageBodyBytes || !utf8.ValidString(r.Body) {
		return corruptf("message record has invalid required fields")
	}
	digest := md5.Sum([]byte(r.Body))
	if r.MD5OfBody != hex.EncodeToString(digest[:]) {
		return corruptf("message record body digest does not match")
	}
	if r.ReceiveCount != r.ReceiveGeneration {
		return corruptf("message receive count and generation differ")
	}
	if r.ReceiveGeneration == 0 {
		if r.ReceiptHandle != "" || r.FirstReceivedAtUnixMillis != 0 || r.VisibilityDeadlineUnixNanos != 0 {
			return corruptf("unreceived message has claim metadata")
		}
		return nil
	}
	if !wellFormedReceiptHandle(r.ReceiptHandle) {
		return corruptf("received message has invalid current receipt")
	}
	return nil
}

func (r messageRecordV1) migrate() (messageRecordV2, error) {
	if err := r.validate(); err != nil {
		return messageRecordV2{}, err
	}
	sentAt := time.UnixMilli(r.SentAtUnixMillis).UTC()
	availableAtNanos := sentAt.UnixNano()
	expiresAtNanos := sentAt.Add(time.Duration(queue.DefaultMessageRetentionPeriod) * time.Second).UnixNano()
	if !time.Unix(0, availableAtNanos).UTC().Equal(sentAt) || !time.Unix(0, expiresAtNanos).UTC().Equal(sentAt.Add(time.Duration(queue.DefaultMessageRetentionPeriod)*time.Second)) {
		return messageRecordV2{}, corruptf("message sent timestamp cannot be represented in schema version 2")
	}
	return messageRecordV2{
		Version:                     recordVersionV2,
		ID:                          r.ID,
		QueueName:                   r.QueueName,
		Body:                        r.Body,
		MD5OfBody:                   r.MD5OfBody,
		SentAtUnixMillis:            r.SentAtUnixMillis,
		ReceiptHandle:               r.ReceiptHandle,
		ReceiveCount:                r.ReceiveCount,
		ReceiveGeneration:           r.ReceiveGeneration,
		FirstReceivedAtUnixMillis:   r.FirstReceivedAtUnixMillis,
		VisibilityDeadlineUnixNanos: r.VisibilityDeadlineUnixNanos,
		AvailableAtUnixNanos:        availableAtNanos,
		ExpiresAtUnixNanos:          expiresAtNanos,
	}, nil
}

func (r messageIDRecordV1) validate(key []byte) error {
	if r.Version != legacyRecordVersion || r.ID == "" || r.ID != string(key) {
		return corruptf("message ID history record is invalid")
	}
	return nil
}

func (r messageIDRecordV2) validate(key []byte) error {
	if r.Version != recordVersionV2 || r.ID == "" || r.ID != string(key) {
		return corruptf("message ID history record is invalid")
	}
	return nil
}

func (r receiptRecordV2) validate(key []byte) error {
	if r.Version != recordVersionV2 || r.Handle != string(key) || !wellFormedReceiptHandle(r.Handle) || r.QueueName == "" || r.MessageID == "" || r.Generation == 0 {
		return corruptf("receipt history record is invalid")
	}
	return nil
}

func (r receiptRecordV1) validate(key []byte) error {
	if r.Version != legacyRecordVersion || r.Handle != string(key) || !wellFormedReceiptHandle(r.Handle) || r.QueueName == "" || r.MessageID == "" || r.Generation == 0 {
		return corruptf("receipt history record is invalid")
	}
	return nil
}

func (r messageIDRecord) validate(key []byte) error {
	if r.Version != recordVersion || r.ID == "" || r.ID != string(key) {
		return corruptf("message ID history record is invalid")
	}
	return nil
}

func (r receiptRecord) validate(key []byte) error {
	if r.Version != recordVersion || r.Handle != string(key) || !wellFormedReceiptHandle(r.Handle) || r.QueueName == "" || r.MessageID == "" || r.Generation == 0 {
		return corruptf("receipt history record is invalid")
	}
	return nil
}

func encodeRecord(value interface{}) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode storage record: %w", err)
	}
	return encoded, nil
}

func decodeRecord(encoded []byte, destination interface{}) error {
	if len(encoded) == 0 {
		return corruptf("storage record is empty")
	}
	if err := validateRecordDocument(encoded, destination); err != nil {
		return err
	}
	if _, ok := destination.(*messageRecord); ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			return corruptf("decode storage record: %v", err)
		}
		rawAttributes := bytes.TrimSpace(fields["message_attributes"])
		if bytes.Equal(rawAttributes, []byte("null")) {
			return corruptf("message attribute records are missing")
		}
		var records []json.RawMessage
		if err := json.Unmarshal(rawAttributes, &records); err != nil || records == nil {
			return corruptf("message attribute records are malformed")
		}
		for _, rawRecord := range records {
			var record messageAttributeRecord
			if err := validateRecordDocument(rawRecord, &record); err != nil {
				return err
			}
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return corruptf("decode storage record: %v", err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return corruptf("storage record contains trailing data")
	}
	return nil
}

func validateRecordDocument(encoded []byte, destination interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := consumeRecordJSONValue(decoder); err != nil {
		return corruptf("decode storage record: %v", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return corruptf("storage record contains trailing data")
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || fields == nil {
		return corruptf("storage record must be a JSON object")
	}
	typeValue := reflect.TypeOf(destination)
	if typeValue.Kind() != reflect.Ptr || typeValue.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("decode storage record destination must be a struct pointer")
	}
	recordType := typeValue.Elem()
	for index := 0; index < recordType.NumField(); index++ {
		fieldName := strings.Split(recordType.Field(index).Tag.Get("json"), ",")[0]
		if fieldName == "" || fieldName == "-" {
			continue
		}
		if _, ok := fields[fieldName]; !ok {
			return corruptf("storage record is missing required field %q", fieldName)
		}
	}
	return nil
}

func consumeRecordJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object field name is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeRecordJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("invalid JSON object")
		}
		return nil
	case '[':
		for decoder.More() {
			if err := consumeRecordJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("invalid JSON array")
		}
		return nil
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func validStoredQueueName(name string) bool {
	if strings.HasPrefix(name, "tn_") {
		if len(name) <= 68 || len(name) > 148 || name[67] != '_' {
			return false
		}
		for _, character := range name[3:67] {
			if character < '0' || character > '9' && character < 'a' || character > 'f' {
				return false
			}
		}
		name = name[68:]
	}
	if len(name) < 1 || len(name) > 80 {
		return false
	}
	base := strings.TrimSuffix(name, ".fifo")
	if base == "" {
		return false
	}
	for _, character := range base {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func wellFormedReceiptHandle(handle string) bool {
	if len(handle) >= 68 && strings.HasPrefix(handle, "tn_") {
		digest, err := hex.DecodeString(handle[3:67])
		if err != nil || len(digest) != 32 || handle[67] != '_' {
			return false
		}
		handle = handle[68:]
	}
	if len(handle) != 35 || !strings.HasPrefix(handle, "rh_") {
		return false
	}
	for _, character := range handle[3:] {
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F' {
			continue
		}
		return false
	}
	return true
}
