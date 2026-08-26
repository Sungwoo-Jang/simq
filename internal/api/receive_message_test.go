package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
)

type apiManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *apiManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *apiManualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func apiDeterministicReceiptHandle(sequence uint64) string {
	return fmt.Sprintf("rh_%032x", sequence)
}

func newReceiveAPIServer(t *testing.T, visibilityTimeout string) (*localServer, *queue.MemoryRepository, *apiManualClock) {
	t.Helper()
	testClock := &apiManualClock{now: time.Date(2026, time.August, 23, 1, 2, 3, 456000000, time.UTC)}
	var receiptSequence atomic.Uint64
	server, repository := newTestServerWithOptions(
		t,
		2<<20,
		queue.WithClock(testClock),
		queue.WithReceiptHandleGenerator(func() string {
			return apiDeterministicReceiptHandle(receiptSequence.Add(1))
		}),
	)
	body := fmt.Sprintf(`{"QueueName":"orders","Attributes":{"VisibilityTimeout":%q}}`, visibilityTimeout)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}
	return server, repository, testClock
}

func TestReceiveMessageHTTPVisibilityLifecycle(t *testing.T) {
	server, _, testClock := newReceiveAPIServer(t, "10")
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{
		"QueueUrl":"http://localhost:9324/queues/orders",
		"MessageBody":"order-created"
	}`)
	if sent.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d, want 200", sent.StatusCode)
	}

	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
		t.Fatalf("first ReceiveMessage = %#v", first)
	}
	message := first.Body.Messages[0]
	if message.MessageID != sent.Body.MessageID || message.Body != "order-created" {
		t.Fatalf("received message = %#v", message)
	}
	if message.MD5OfBody != sent.Body.MD5OfMessageBody || message.ReceiptHandle == "" {
		t.Fatalf("received message = %#v", message)
	}
	wantTimestamp := fmt.Sprintf("%d", testClock.Now().UnixMilli())
	if message.Attributes["ApproximateReceiveCount"] != "1" ||
		message.Attributes["SentTimestamp"] != wantTimestamp ||
		message.Attributes["ApproximateFirstReceiveTimestamp"] != wantTimestamp {
		t.Fatalf("Attributes = %#v", message.Attributes)
	}

	hidden := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if hidden.StatusCode != http.StatusOK || len(hidden.Body.Messages) != 0 {
		t.Fatalf("receive before deadline = %#v", hidden)
	}
	testClock.Advance(10 * time.Second)
	second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if second.StatusCode != http.StatusOK || len(second.Body.Messages) != 1 {
		t.Fatalf("receive at deadline = %#v", second)
	}
	if second.Body.Messages[0].ReceiptHandle == message.ReceiptHandle {
		t.Fatal("re-receive reused receipt handle")
	}
	if second.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "2" {
		t.Fatalf("second receive attributes = %#v", second.Body.Messages[0].Attributes)
	}
}

func TestReceiveMessageEmptyQueueReturnsMessagesArray(t *testing.T) {
	server, _, _ := newReceiveAPIServer(t, "30")
	response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if response.StatusCode != http.StatusOK || len(response.Body.Messages) != 0 {
		t.Fatalf("ReceiveMessage = %#v", response)
	}
	if !strings.Contains(string(response.RawBody), `"Messages":[]`) {
		t.Fatalf("response body = %s, want an empty Messages array", response.RawBody)
	}
}

func TestReceiveMessageHonorsBatchLimitAndVisibilityOverride(t *testing.T) {
	server, _, _ := newReceiveAPIServer(t, "30")
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"QueueUrl":"%s/queues/orders","MessageBody":"body-%d"}`, publicBaseURL, i)
		if response := mustPostAction(t, server, "/v1/sqs/SendMessage", body); response.StatusCode != http.StatusOK {
			t.Fatalf("SendMessage status = %d", response.StatusCode)
		}
	}

	body := `{"QueueUrl":"http://localhost:9324/queues/orders","MaxNumberOfMessages":2,"VisibilityTimeout":0}`
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", body)
	if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 2 {
		t.Fatalf("first ReceiveMessage = %#v", first)
	}
	second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", body)
	if second.StatusCode != http.StatusOK || len(second.Body.Messages) != 2 {
		t.Fatalf("second ReceiveMessage = %#v", second)
	}
}

func TestReceiveMessageReturnsQueueDoesNotExist(t *testing.T) {
	server, repository := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/missing"}`)
	requireError(t, response, http.StatusNotFound, "QueueDoesNotExist")
	if repository.MessageCount("missing") != 0 {
		t.Fatal("missing queue was mutated")
	}
}

func TestReceiveMessageRejectsInvalidRequestsWithoutClaiming(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: ``},
		{name: "missing QueueUrl", body: `{}`},
		{name: "foreign QueueUrl", body: `{"QueueUrl":"http://other.example/queues/orders"}`},
		{name: "MaxNumberOfMessages zero", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MaxNumberOfMessages":0}`},
		{name: "MaxNumberOfMessages over ten", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MaxNumberOfMessages":11}`},
		{name: "MaxNumberOfMessages wrong type", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MaxNumberOfMessages":"1"}`},
		{name: "MaxNumberOfMessages fractional", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MaxNumberOfMessages":1.5}`},
		{name: "VisibilityTimeout negative", body: `{"QueueUrl":"http://localhost:9324/queues/orders","VisibilityTimeout":-1}`},
		{name: "VisibilityTimeout too large", body: `{"QueueUrl":"http://localhost:9324/queues/orders","VisibilityTimeout":43201}`},
		{name: "VisibilityTimeout wrong type", body: `{"QueueUrl":"http://localhost:9324/queues/orders","VisibilityTimeout":"30"}`},
		{name: "unknown field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Unknown":1}`},
		{name: "duplicate field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","QueueUrl":"http://localhost:9324/queues/other"}`},
		{name: "malformed", body: `{"QueueUrl":`},
		{name: "trailing", body: `{"QueueUrl":"http://localhost:9324/queues/orders"} {}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _, _ := newReceiveAPIServer(t, "30")
			sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`)
			if sent.StatusCode != http.StatusOK {
				t.Fatalf("SendMessage status = %d", sent.StatusCode)
			}

			response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			valid := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
			if valid.StatusCode != http.StatusOK || len(valid.Body.Messages) != 1 || valid.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "1" {
				t.Fatalf("valid receive after invalid request = %#v", valid)
			}
		})
	}
}

func TestTwentyConcurrentHTTPReceivesClaimOneMessage(t *testing.T) {
	server, _, _ := newReceiveAPIServer(t, "30")
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d", response.StatusCode)
	}

	const consumers = 20
	results := make(chan actionResponse, consumers)
	errorsChannel := make(chan error, consumers)
	var wait sync.WaitGroup
	wait.Add(consumers)
	for i := 0; i < consumers; i++ {
		go func() {
			defer wait.Done()
			response, err := postAction(server.Client(), server.URL+"/v1/sqs/ReceiveMessage", "", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- response
		}()
	}
	wait.Wait()
	close(results)
	close(errorsChannel)

	for err := range errorsChannel {
		t.Error(err)
	}
	claims := 0
	for response := range results {
		if response.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", response.StatusCode)
		}
		claims += len(response.Body.Messages)
	}
	if claims != 1 {
		t.Fatalf("successful HTTP claims = %d, want 1", claims)
	}
}
