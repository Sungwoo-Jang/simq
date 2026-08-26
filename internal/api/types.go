package api

import (
	"simq/internal/auth"
	"simq/internal/observability"
	"simq/internal/queue"
)

type RequestIDGenerator func() string

type Config struct {
	PublicBaseURL      string
	MaxBodyBytes       int64
	RequestIDGenerator RequestIDGenerator
	ClusterAdminToken  string
	AuthVerifier       auth.Verifier
	RateLimiter        *observability.RateLimiter
	Metrics            *observability.Metrics
	MetricsToken       string
	Auditor            observability.Auditor
}

type queueURLResponse struct {
	QueueURL  string `json:"QueueUrl"`
	RequestID string `json:"RequestId"`
}

type sendMessageRequest struct {
	QueueURL               string
	MessageBody            string
	DelaySeconds           *int
	MessageAttributes      map[string]queue.MessageAttribute
	MessageGroupID         string
	MessageDeduplicationID string
}

type sendMessageResponse struct {
	MessageID              string `json:"MessageId"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	SequenceNumber         string `json:"SequenceNumber,omitempty"`
	RequestID              string `json:"RequestId"`
}

type sendMessageBatchRequest struct {
	QueueURL string
	Entries  []queue.SendMessageBatchEntry
}
type deleteMessageBatchRequest struct {
	QueueURL string
	Entries  []queue.DeleteMessageBatchEntry
}
type changeMessageVisibilityBatchRequest struct {
	QueueURL string
	Entries  []queue.ChangeMessageVisibilityBatchEntry
}

type batchFailureResponse struct {
	ID          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}
type sendMessageBatchSuccessResponse struct {
	ID                     string `json:"Id"`
	MessageID              string `json:"MessageId"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	SequenceNumber         string `json:"SequenceNumber,omitempty"`
}
type sendMessageBatchResponse struct {
	Successful []sendMessageBatchSuccessResponse `json:"Successful"`
	Failed     []batchFailureResponse            `json:"Failed"`
	RequestID  string                            `json:"RequestId"`
}
type batchSuccessResponse struct {
	ID string `json:"Id"`
}
type batchResponse struct {
	Successful []batchSuccessResponse `json:"Successful"`
	Failed     []batchFailureResponse `json:"Failed"`
	RequestID  string                 `json:"RequestId"`
}

type receiveMessageRequest struct {
	QueueURL                string
	MaxNumberOfMessages     *int
	VisibilityTimeout       *int
	WaitTimeSeconds         *int
	MessageAttributeNames   []string
	ReceiveRequestAttemptID string
}

type receiveMessageResponse struct {
	Messages  []receivedMessage `json:"Messages"`
	RequestID string            `json:"RequestId"`
}

type receivedMessage struct {
	MessageID              string                           `json:"MessageId"`
	ReceiptHandle          string                           `json:"ReceiptHandle"`
	MD5OfBody              string                           `json:"MD5OfBody"`
	Body                   string                           `json:"Body"`
	Attributes             receivedMessageAttributes        `json:"Attributes"`
	MessageAttributes      map[string]messageAttributeValue `json:"MessageAttributes,omitempty"`
	MD5OfMessageAttributes string                           `json:"MD5OfMessageAttributes,omitempty"`
	MessageGroupID         string                           `json:"MessageGroupId,omitempty"`
	MessageDeduplicationID string                           `json:"MessageDeduplicationId,omitempty"`
	SequenceNumber         string                           `json:"SequenceNumber,omitempty"`
}

type messageAttributeValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue,omitempty"`
	BinaryValue []byte `json:"BinaryValue,omitempty"`
}

type receivedMessageAttributes struct {
	ApproximateReceiveCount          string `json:"ApproximateReceiveCount"`
	SentTimestamp                    string `json:"SentTimestamp"`
	ApproximateFirstReceiveTimestamp string `json:"ApproximateFirstReceiveTimestamp"`
}

type deleteMessageRequest struct {
	QueueURL      string
	ReceiptHandle string
}

type deleteMessageResponse struct {
	RequestID string `json:"RequestId"`
}

type changeMessageVisibilityRequest struct {
	QueueURL          string
	ReceiptHandle     string
	VisibilityTimeout int
}

type changeMessageVisibilityResponse struct {
	RequestID string `json:"RequestId"`
}

type getQueueAttributesRequest struct {
	QueueURL       string
	AttributeNames []string
}

type getQueueAttributesResponse struct {
	Attributes map[string]string `json:"Attributes"`
	RequestID  string            `json:"RequestId"`
}

type setQueueAttributesRequest struct {
	QueueURL   string
	Attributes map[string]string
}

type setQueueAttributesResponse struct {
	RequestID string `json:"RequestId"`
}

type queueURLRequest struct {
	QueueURL string `json:"QueueUrl"`
}

type listQueuesRequest struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
	MaxResults      *int   `json:"MaxResults"`
	NextToken       string `json:"NextToken"`
}

type listQueuesResponse struct {
	QueueURLs []string `json:"QueueUrls"`
	NextToken string   `json:"NextToken,omitempty"`
	RequestID string   `json:"RequestId"`
}

type tagQueueRequest struct {
	QueueURL string            `json:"QueueUrl"`
	Tags     map[string]string `json:"Tags"`
}

type untagQueueRequest struct {
	QueueURL string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

type listQueueTagsResponse struct {
	Tags      map[string]string `json:"Tags"`
	RequestID string            `json:"RequestId"`
}

type addPermissionRequest struct {
	QueueURL      string   `json:"QueueUrl"`
	Label         string   `json:"Label"`
	AWSAccountIDs []string `json:"AWSAccountIds"`
	Actions       []string `json:"Actions"`
}

type removePermissionRequest struct {
	QueueURL string `json:"QueueUrl"`
	Label    string `json:"Label"`
}

type listDeadLetterSourceQueuesRequest struct {
	QueueURL   string `json:"QueueUrl"`
	MaxResults *int   `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type listDeadLetterSourceQueuesResponse struct {
	QueueURLs []string `json:"QueueUrls"`
	NextToken string   `json:"NextToken,omitempty"`
	RequestID string   `json:"RequestId"`
}

type startMessageMoveTaskRequest struct {
	SourceARN                    string `json:"SourceArn"`
	DestinationARN               string `json:"DestinationArn,omitempty"`
	MaxNumberOfMessagesPerSecond *int   `json:"MaxNumberOfMessagesPerSecond,omitempty"`
}

type startMessageMoveTaskResponse struct {
	TaskHandle string `json:"TaskHandle"`
	RequestID  string `json:"RequestId"`
}

type cancelMessageMoveTaskRequest struct {
	TaskHandle string `json:"TaskHandle"`
}
type cancelMessageMoveTaskResponse struct {
	ApproximateNumberOfMessagesMoved uint64 `json:"ApproximateNumberOfMessagesMoved"`
	RequestID                        string `json:"RequestId"`
}

type listMessageMoveTasksRequest struct {
	SourceARN  string `json:"SourceArn"`
	MaxResults *int   `json:"MaxResults,omitempty"`
}

type messageMoveTaskResponse struct {
	TaskHandle                        string `json:"TaskHandle"`
	SourceARN                         string `json:"SourceArn"`
	DestinationARN                    string `json:"DestinationArn,omitempty"`
	MaxNumberOfMessagesPerSecond      int    `json:"MaxNumberOfMessagesPerSecond"`
	StartedTimestamp                  int64  `json:"StartedTimestamp"`
	Status                            string `json:"Status"`
	FailureReason                     string `json:"FailureReason,omitempty"`
	ApproximateNumberOfMessagesMoved  uint64 `json:"ApproximateNumberOfMessagesMoved"`
	ApproximateNumberOfMessagesToMove uint64 `json:"ApproximateNumberOfMessagesToMove"`
}

type listMessageMoveTasksResponse struct {
	Results   []messageMoveTaskResponse `json:"Results"`
	RequestID string                    `json:"RequestId"`
}

type errorEnvelope struct {
	Error     errorBody `json:"Error"`
	RequestID string    `json:"RequestId"`
}

type errorBody struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

type healthResponse struct {
	Status string `json:"Status"`
}
