package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"simq/internal/api"
	"simq/internal/queue"
)

const storageSecret = "/secret/storage/path body-secret receipt-secret"

type unavailableRepository struct{}

func (unavailableRepository) Create(queue.Queue) (queue.Queue, error) {
	return queue.Queue{}, storageUnavailableError()
}

func (unavailableRepository) Get(string) (queue.Queue, bool, error) {
	return queue.Queue{}, false, storageUnavailableError()
}
func (unavailableRepository) GetByRef(queue.QueueRef) (queue.Queue, bool, error) {
	return queue.Queue{}, false, storageUnavailableError()
}
func (unavailableRepository) ListQueues(queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	return queue.ListQueuesPage{}, storageUnavailableError()
}
func (unavailableRepository) DeleteQueue(queue.QueueRef) error { return storageUnavailableError() }
func (unavailableRepository) PurgeQueue(queue.QueueRef) error  { return storageUnavailableError() }
func (unavailableRepository) TagQueue(queue.QueueRef, map[string]string) error {
	return storageUnavailableError()
}
func (unavailableRepository) UntagQueue(queue.QueueRef, []string) error {
	return storageUnavailableError()
}
func (unavailableRepository) ListQueueTags(queue.QueueRef) (map[string]string, error) {
	return nil, storageUnavailableError()
}
func (unavailableRepository) AddPermission(queue.QueueRef, queue.Permission) error {
	return storageUnavailableError()
}
func (unavailableRepository) RemovePermission(queue.QueueRef, string) error {
	return storageUnavailableError()
}
func (unavailableRepository) ListPermissions(queue.QueueRef) ([]queue.Permission, error) {
	return nil, storageUnavailableError()
}

func (unavailableRepository) SetAttributes(queue.QueueAttributesCommand) (queue.Queue, error) {
	return queue.Queue{}, storageUnavailableError()
}

func (unavailableRepository) Enqueue(queue.EnqueueCommand) (queue.Message, error) {
	return queue.Message{}, storageUnavailableError()
}

func (unavailableRepository) Claim(queue.ClaimInput) (queue.ClaimResult, error) {
	return queue.ClaimResult{}, storageUnavailableError()
}

func (unavailableRepository) Delete(string, string, string) error {
	return storageUnavailableError()
}

func (unavailableRepository) ChangeVisibility(queue.ChangeVisibilityCommand) error {
	return storageUnavailableError()
}

func (unavailableRepository) Expire(queue.ExpireCommand) (int, error) {
	return 0, storageUnavailableError()
}

func (unavailableRepository) Health() error { return storageUnavailableError() }
func (unavailableRepository) Close() error  { return nil }

func storageUnavailableError() error {
	return fmt.Errorf("%w: %s", queue.ErrRepositoryUnavailable, storageSecret)
}

func TestRepositoryFailuresReturn503WithoutSensitiveDetails(t *testing.T) {
	service := queue.NewService(unavailableRepository{})
	server := api.NewServer(api.Config{
		PublicBaseURL: publicBaseURL,
		RequestIDGenerator: func() string {
			return fixedRequestID
		},
	}, service)
	local := &localServer{URL: "http://simq.test", handler: server}

	requests := []struct {
		path string
		body string
	}{
		{path: "/v1/sqs/CreateQueue", body: `{"QueueName":"orders"}`},
		{path: "/v1/sqs/GetQueueUrl", body: `{"QueueName":"orders"}`},
		{path: "/v1/sqs/SendMessage", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body-secret"}`},
		{path: "/v1/sqs/ReceiveMessage", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{path: "/v1/sqs/ReceiveMessage", body: `{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":20}`},
		{path: "/v1/sqs/DeleteMessage", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_00000000000000000000000000000001"}`},
		{path: "/v1/sqs/ChangeMessageVisibility", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_00000000000000000000000000000001","VisibilityTimeout":30}`},
		{path: "/v1/sqs/GetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["All"]}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":"60"}}`},
		{path: "/v1/sqs/ListQueues", body: `{}`},
		{path: "/v1/sqs/DeleteQueue", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{path: "/v1/sqs/PurgeQueue", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{path: "/v1/sqs/TagQueue", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Tags":{"env":"secret"}}`},
		{path: "/v1/sqs/ListQueueTags", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{path: "/v1/sqs/AddPermission", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Label":"writers","AWSAccountIds":["111111111111"],"Actions":["SendMessage"]}`},
	}
	for _, request := range requests {
		t.Run(request.path, func(t *testing.T) {
			response := mustPostAction(t, local, request.path, request.body)
			requireError(t, response, http.StatusServiceUnavailable, "ServiceUnavailable")
			if strings.Contains(string(response.RawBody), storageSecret) || strings.Contains(string(response.RawBody), "body-secret") || strings.Contains(string(response.RawBody), "receipt-secret") {
				t.Fatalf("response leaked storage details: %s", response.RawBody)
			}
		})
	}

	ready, err := local.Client().Get(local.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want 503", ready.StatusCode)
	}
	health, err := local.Client().Get(local.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer health.Body.Close()
	if health.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", health.StatusCode)
	}
}
