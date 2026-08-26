package queue

import (
	"fmt"
	"time"
)

const (
	DefaultVisibilityTimeout      = 30
	DefaultDelaySeconds           = 0
	DefaultMessageRetentionPeriod = 345600
)

const (
	MaxMessageBytes     = 1 << 20
	MaxMessageBodyBytes = MaxMessageBytes
)

type Queue struct {
	ID                        string
	Name                      string
	LegacyURLAllowed          bool
	VisibilityTimeout         int
	DelaySeconds              int
	MessageRetentionPeriod    int
	RedrivePolicy             *RedrivePolicy
	FIFO                      bool
	ContentBasedDeduplication bool
}

type Message struct {
	ID                     string
	QueueName              string
	Body                   string
	MD5OfBody              string
	SentAtMillis           int64
	AvailableAt            time.Time
	ExpiresAt              time.Time
	ReceiptHandle          string
	ReceiveCount           uint64
	ReceiveGeneration      uint64
	FirstReceivedAtMillis  int64
	VisibilityDeadline     time.Time
	MessageAttributes      map[string]MessageAttribute
	MD5OfMessageAttributes string
	// Plaintext digests are transient repository-boundary metadata used to
	// preserve FIFO duplicate acknowledgements while the stored body digest
	// authenticates ciphertext. They are never serialized into message records.
	PlaintextMD5OfBody              string
	PlaintextMD5OfMessageAttributes string
	MessageGroupID                  string
	MessageDeduplicationID          string
	SequenceNumber                  uint64
}

type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

type CreateQueueInput struct {
	QueueName  string
	Attributes map[string]string
}

type GetQueueInput struct {
	QueueName string
}

type SendMessageInput struct {
	QueueName              string
	QueueID                string
	MessageBody            string
	DelaySeconds           *int
	MessageAttributes      map[string]MessageAttribute
	MessageGroupID         string
	MessageDeduplicationID string
}

type ReceiveMessageInput struct {
	QueueName               string
	QueueID                 string
	MaxNumberOfMessages     *int
	VisibilityTimeout       *int
	WaitTimeSeconds         *int
	MessageAttributeNames   []string
	ReceiveRequestAttemptID string
}

type GetQueueAttributesInput struct {
	QueueName      string
	QueueID        string
	AttributeNames []string
}

type SetQueueAttributesInput struct {
	QueueName  string
	QueueID    string
	Attributes map[string]string
}

type EnqueueCommand struct {
	QueueName    string
	QueueID      string
	Now          time.Time
	DelaySeconds *int
	Message      Message
}

type QueueAttributesCommand struct {
	QueueName              string
	QueueID                string
	VisibilityTimeout      *int
	DelaySeconds           *int
	MessageRetentionPeriod *int
	RedrivePolicySet       bool
	RedrivePolicy          *RedrivePolicy
}

type DeleteMessageInput struct {
	QueueName     string
	QueueID       string
	ReceiptHandle string
}

type ChangeMessageVisibilityInput struct {
	QueueName         string
	QueueID           string
	ReceiptHandle     string
	VisibilityTimeout int
}

type BatchRequestError struct {
	Code    string
	Message string
}

func (e *BatchRequestError) Error() string { return e.Message }

type BatchFailure struct {
	ID    string
	Error error
}

type SendMessageBatchEntry struct {
	ID                     string
	MessageBody            string
	DelaySeconds           *int
	MessageAttributes      map[string]MessageAttribute
	MessageGroupID         string
	MessageDeduplicationID string
}

type SendMessageBatchInput struct {
	QueueName string
	QueueID   string
	Entries   []SendMessageBatchEntry
}

type SendMessageBatchSuccess struct {
	ID      string
	Message Message
}

type SendMessageBatchResult struct {
	Successful []SendMessageBatchSuccess
	Failed     []BatchFailure
}

type DeleteMessageBatchEntry struct {
	ID            string
	ReceiptHandle string
}

type DeleteMessageBatchInput struct {
	QueueName string
	QueueID   string
	Entries   []DeleteMessageBatchEntry
}

type BatchResult struct {
	Successful []string
	Failed     []BatchFailure
}

type ChangeMessageVisibilityBatchEntry struct {
	ID                string
	ReceiptHandle     string
	VisibilityTimeout int
}

type ChangeMessageVisibilityBatchInput struct {
	QueueName string
	QueueID   string
	Entries   []ChangeMessageVisibilityBatchEntry
}

type ClaimInput struct {
	QueueName               string
	QueueID                 string
	Now                     time.Time
	MaxNumberOfMessages     int
	VisibilityTimeout       *time.Duration
	ReceiptHandles          []string
	ReceiveRequestAttemptID string
	StoreEmptyAttempt       bool
}

type ClaimResult struct {
	Messages        []Message
	NextTransition  time.Time
	MovedTo         []QueueRef
	AttemptReplayed bool
}

type ChangeVisibilityCommand struct {
	QueueName         string
	QueueID           string
	ReceiptHandle     string
	Now               time.Time
	VisibilityTimeout time.Duration
}

type ExpireCommand struct {
	Now time.Time
}

type QueueRef struct {
	Name string
	ID   string
}

type ListQueuesInput struct {
	QueueNamePrefix string
	MaxResults      *int
	NextToken       string
}

type ListQueuesCommand struct {
	Prefix           string
	After            string
	ExpectedRevision *uint64
	Limit            int
}

type ListQueuesPage struct {
	Queues            []Queue
	NamespaceRevision uint64
	HasMore           bool
}

type TagQueueInput struct {
	QueueName string
	QueueID   string
	Tags      map[string]string
}

type UntagQueueInput struct {
	QueueName string
	QueueID   string
	TagKeys   []string
}

type Permission struct {
	Label         string   `json:"Label"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
	Actions       []string `json:"Actions"`
}

type AddPermissionInput struct {
	QueueName     string
	QueueID       string
	Label         string
	AWSAccountIDs []string
	Actions       []string
}

type RemovePermissionInput struct {
	QueueName string
	QueueID   string
	Label     string
}

type InvalidRequestError struct {
	Message string
}

func (e *InvalidRequestError) Error() string {
	return e.Message
}

func invalidRequest(format string, arguments ...interface{}) error {
	return &InvalidRequestError{Message: fmt.Sprintf(format, arguments...)}
}
