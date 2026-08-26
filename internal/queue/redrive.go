package queue

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const queueARNPrefix = "arn:simq:sqs:::"

type RedrivePolicy struct {
	DeadLetterTarget QueueRef
	MaxReceiveCount  uint64
}

type ListDeadLetterSourcesInput struct {
	Target     QueueRef
	MaxResults *int
	NextToken  string
}

type ListDeadLetterSourcesCommand struct {
	Target           QueueRef
	After            string
	ExpectedRevision *uint64
	Limit            int
}

type ListDeadLetterSourcesPage struct {
	Queues          []Queue
	RedriveRevision uint64
	HasMore         bool
}

type MoveTaskStatus string

const (
	MoveTaskRunning   MoveTaskStatus = "RUNNING"
	MoveTaskCompleted MoveTaskStatus = "COMPLETED"
	MoveTaskCancelled MoveTaskStatus = "CANCELLED"
	MoveTaskFailed    MoveTaskStatus = "FAILED"
)

type MessageMoveTask struct {
	Handle                            string
	Source                            QueueRef
	Destination                       *QueueRef
	MaxNumberOfMessagesPerSecond      int
	StartedAtMillis                   int64
	Status                            MoveTaskStatus
	FailureReason                     string
	ApproximateNumberOfMessagesMoved  uint64
	ApproximateNumberOfMessagesToMove uint64
	Boundary                          uint64
}

type StartMessageMoveTaskInput struct {
	Source               QueueRef
	Destination          *QueueRef
	MaxMessagesPerSecond *int
}

type ListMessageMoveTasksInput struct {
	Source     QueueRef
	MaxResults *int
}

type StartMoveTaskCommand struct {
	Handle               string
	Source               QueueRef
	Destination          *QueueRef
	MaxMessagesPerSecond int
	Now                  time.Time
}

type MoveTaskStepResult struct {
	Task        MessageMoveTask
	Destination *QueueRef
	Terminal    bool
}

func QueueARN(ref QueueRef) string {
	return queueARNPrefix + ref.Name + "/" + ref.ID
}

func ParseQueueARN(value string) (QueueRef, error) {
	if !strings.HasPrefix(value, queueARNPrefix) {
		return QueueRef{}, invalidRequest("queue ARN is invalid")
	}
	remainder := strings.TrimPrefix(value, queueARNPrefix)
	if strings.Count(remainder, "/") != 1 {
		return QueueRef{}, invalidRequest("queue ARN is invalid")
	}
	parts := strings.SplitN(remainder, "/", 2)
	ref := QueueRef{Name: parts[0], ID: parts[1]}
	if err := validateQueueRef(ref); err != nil || ref.ID == "" {
		return QueueRef{}, invalidRequest("queue ARN is invalid")
	}
	return ref, nil
}

func redrivePolicyJSON(policy *RedrivePolicy) (string, error) {
	if policy == nil {
		return "", nil
	}
	encoded, err := json.Marshal(struct {
		DeadLetterTargetARN string `json:"deadLetterTargetArn"`
		MaxReceiveCount     string `json:"maxReceiveCount"`
	}{QueueARN(policy.DeadLetterTarget), strconv.FormatUint(policy.MaxReceiveCount, 10)})
	return string(encoded), err
}

func parseRedrivePolicy(value string) (*RedrivePolicy, error) {
	if value == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, invalidRequest("RedrivePolicy must be valid JSON with exactly deadLetterTargetArn and maxReceiveCount")
	}
	fields := make(map[string]string, 2)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok || key != "deadLetterTargetArn" && key != "maxReceiveCount" {
			return nil, invalidRequest("RedrivePolicy must be valid JSON with exactly deadLetterTargetArn and maxReceiveCount")
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, invalidRequest("RedrivePolicy fields must be unique")
		}
		var fieldValue string
		if err := decoder.Decode(&fieldValue); err != nil {
			return nil, invalidRequest("RedrivePolicy values must be strings")
		}
		fields[key] = fieldValue
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return nil, invalidRequest("RedrivePolicy must contain one JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, invalidRequest("RedrivePolicy must contain one JSON object")
	}
	deadLetterTargetARN, maxReceiveCount := fields["deadLetterTargetArn"], fields["maxReceiveCount"]
	if len(fields) != 2 || deadLetterTargetARN == "" || maxReceiveCount == "" {
		return nil, invalidRequest("RedrivePolicy must contain deadLetterTargetArn and maxReceiveCount")
	}
	target, err := ParseQueueARN(deadLetterTargetARN)
	if err != nil {
		return nil, invalidRequest("RedrivePolicy deadLetterTargetArn is invalid")
	}
	count, err := strconv.ParseUint(maxReceiveCount, 10, 64)
	if err != nil || count < 1 || count > 1000 || strconv.FormatUint(count, 10) != maxReceiveCount {
		return nil, invalidRequest("RedrivePolicy maxReceiveCount must be a decimal string between 1 and 1000")
	}
	return &RedrivePolicy{DeadLetterTarget: target, MaxReceiveCount: count}, nil
}

func cloneRedrivePolicy(value *RedrivePolicy) *RedrivePolicy {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func ValidateRedrivePolicy(value *RedrivePolicy) error {
	if value == nil || value.MaxReceiveCount < 1 || value.MaxReceiveCount > 1000 {
		return fmt.Errorf("invalid redrive policy")
	}
	if err := validateQueueRef(value.DeadLetterTarget); err != nil || value.DeadLetterTarget.ID == "" {
		return fmt.Errorf("invalid redrive target")
	}
	return nil
}
