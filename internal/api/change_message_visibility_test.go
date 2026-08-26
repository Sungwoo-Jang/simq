package api_test

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/api"
	"simq/internal/queue"
)

func TestChangeMessageVisibilityHTTPFlowUsesCommandTimestamp(t *testing.T) {
	server, repository, testClock := newReceiveAPIServer(t, "10")
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`)
	if sent.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", sent)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
		t.Fatalf("ReceiveMessage = %#v", first)
	}
	receipt := first.Body.Messages[0].ReceiptHandle
	before := repository.Messages("orders")

	testClock.Advance(5 * time.Second)
	changed := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", receipt, 20))
	if changed.StatusCode != http.StatusOK || changed.Body.Error != nil || changed.Body.RequestID != fixedRequestID {
		t.Fatalf("ChangeMessageVisibility = %#v", changed)
	}
	after := repository.Messages("orders")
	want := before[0]
	want.VisibilityDeadline = testClock.Now().Add(20 * time.Second)
	if len(after) != 1 || !reflect.DeepEqual(after[0], want) {
		t.Fatalf("visibility change = %#v, want deadline-only change %#v", after, want)
	}

	testClock.Advance(20*time.Second - time.Nanosecond)
	hidden := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if hidden.StatusCode != http.StatusOK || len(hidden.Body.Messages) != 0 {
		t.Fatalf("receive before changed deadline = %#v", hidden)
	}
	testClock.Advance(time.Nanosecond)
	second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if second.StatusCode != http.StatusOK || len(second.Body.Messages) != 1 {
		t.Fatalf("receive at changed deadline = %#v", second)
	}
	if second.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "2" || second.Body.Messages[0].ReceiptHandle == receipt {
		t.Fatalf("second receive = %#v", second.Body.Messages[0])
	}
}

func TestChangeMessageVisibilityHTTPTimeoutBoundaries(t *testing.T) {
	for _, timeout := range []int{0, 43200} {
		t.Run(fmt.Sprintf("%d", timeout), func(t *testing.T) {
			server, repository, testClock := newReceiveAPIServer(t, "30")
			if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
				t.Fatalf("SendMessage = %#v", response)
			}
			first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
			if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
				t.Fatalf("ReceiveMessage = %#v", first)
			}
			receipt := first.Body.Messages[0].ReceiptHandle
			before := repository.Messages("orders")[0]
			response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", receipt, timeout))
			if response.StatusCode != http.StatusOK {
				t.Fatalf("ChangeMessageVisibility = %#v", response)
			}
			after := repository.Messages("orders")[0]
			want := before
			want.VisibilityDeadline = testClock.Now().Add(time.Duration(timeout) * time.Second)
			if !reflect.DeepEqual(after, want) {
				t.Fatalf("message after boundary timeout = %#v, want %#v", after, want)
			}
			if timeout == 0 {
				second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
				if second.StatusCode != http.StatusOK || len(second.Body.Messages) != 1 || second.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "2" {
					t.Fatalf("immediate receive after zero timeout = %#v", second)
				}
			}
		})
	}
}

func TestChangeMessageVisibilityHTTPStaleAndConsumedHandlesAreNoOps(t *testing.T) {
	server, repository, testClock := newReceiveAPIServer(t, "1")
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", response)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	testClock.Advance(time.Second)
	second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if len(first.Body.Messages) != 1 || len(second.Body.Messages) != 1 {
		t.Fatalf("receives = %#v, %#v", first, second)
	}

	stale := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", first.Body.Messages[0].ReceiptHandle, 100))
	if stale.StatusCode != http.StatusOK {
		t.Fatalf("stale ChangeMessageVisibility = %#v", stale)
	}
	testClock.Advance(time.Second)
	third := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if third.StatusCode != http.StatusOK || len(third.Body.Messages) != 1 || third.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "3" {
		t.Fatalf("stale handle changed latest deadline = %#v", third)
	}

	currentReceipt := third.Body.Messages[0].ReceiptHandle
	deleted := mustPostAction(t, server, "/v1/sqs/DeleteMessage", fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, publicBaseURL+"/queues/orders", currentReceipt))
	if deleted.StatusCode != http.StatusOK {
		t.Fatalf("DeleteMessage = %#v", deleted)
	}
	consumed := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", currentReceipt, 100))
	if consumed.StatusCode != http.StatusOK {
		t.Fatalf("consumed ChangeMessageVisibility = %#v", consumed)
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatal("consumed visibility handle resurrected deleted message")
	}
}

func TestChangeMessageVisibilityHTTPReceiptAndQueueErrorsDoNotMutate(t *testing.T) {
	server, repository, _ := newReceiveAPIServer(t, "30")
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"payments"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue(payments) = %#v", response)
	}
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", response)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
		t.Fatalf("ReceiveMessage = %#v", first)
	}
	receipt := first.Body.Messages[0].ReceiptHandle
	before := repository.Messages("orders")[0]

	requests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "malformed", body: changeVisibilityBody("orders", "bad", 1), wantStatus: http.StatusBadRequest, wantCode: "ReceiptHandleIsInvalid"},
		{name: "unissued", body: changeVisibilityBody("orders", "rh_ffffffffffffffffffffffffffffffff", 1), wantStatus: http.StatusBadRequest, wantCode: "ReceiptHandleIsInvalid"},
		{name: "corrupted", body: changeVisibilityBody("orders", receipt[:len(receipt)-1]+"f", 1), wantStatus: http.StatusBadRequest, wantCode: "ReceiptHandleIsInvalid"},
		{name: "cross queue", body: changeVisibilityBody("payments", receipt, 1), wantStatus: http.StatusBadRequest, wantCode: "ReceiptHandleIsInvalid"},
		{name: "missing queue", body: changeVisibilityBody("missing", receipt, 1), wantStatus: http.StatusNotFound, wantCode: "QueueDoesNotExist"},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", request.body)
			requireError(t, response, request.wantStatus, request.wantCode)
			if got := repository.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before) {
				t.Fatalf("error mutated orders state: %#v", got)
			}
		})
	}
}

func TestChangeMessageVisibilityHTTPRejectsInvalidRequestsWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		body func(string) string
	}{
		{name: "missing QueueUrl", body: func(handle string) string { return fmt.Sprintf(`{"ReceiptHandle":%q,"VisibilityTimeout":1}`, handle) }},
		{name: "missing ReceiptHandle", body: func(string) string { return `{"QueueUrl":"http://localhost:9324/queues/orders","VisibilityTimeout":1}` }},
		{name: "missing VisibilityTimeout", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q}`, handle)
		}},
		{name: "wrong QueueUrl type", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":1,"ReceiptHandle":%q,"VisibilityTimeout":1}`, handle)
		}},
		{name: "wrong ReceiptHandle type", body: func(string) string {
			return `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":1,"VisibilityTimeout":1}`
		}},
		{name: "wrong VisibilityTimeout type", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q,"VisibilityTimeout":"1"}`, handle)
		}},
		{name: "fractional VisibilityTimeout", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q,"VisibilityTimeout":1.5}`, handle)
		}},
		{name: "negative VisibilityTimeout", body: func(handle string) string { return changeVisibilityBody("orders", handle, -1) }},
		{name: "VisibilityTimeout too large", body: func(handle string) string { return changeVisibilityBody("orders", handle, 43201) }},
		{name: "unknown field", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q,"VisibilityTimeout":1,"Extra":true}`, handle)
		}},
		{name: "duplicate field", body: func(handle string) string {
			return fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q,"ReceiptHandle":%q,"VisibilityTimeout":1}`, handle, handle)
		}},
		{name: "malformed JSON", body: func(string) string { return `{"QueueUrl":` }},
		{name: "trailing JSON", body: func(handle string) string { return changeVisibilityBody("orders", handle, 1) + ` {}` }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository, _ := newReceiveAPIServer(t, "30")
			if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
				t.Fatalf("SendMessage = %#v", response)
			}
			first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
			if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
				t.Fatalf("ReceiveMessage = %#v", first)
			}
			receipt := first.Body.Messages[0].ReceiptHandle
			before := repository.Messages("orders")[0]
			response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", test.body(receipt))
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if got := repository.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before) {
				t.Fatalf("invalid request mutated state: %#v", got)
			}
		})
	}
}

func TestChangeMessageVisibilityHTTPRejectsOversizedRequest(t *testing.T) {
	testClock := &apiManualClock{now: time.Unix(100, 0)}
	var receiptSequence atomic.Uint64
	server, repository := newTestServerWithOptions(t, 256,
		queue.WithClock(testClock),
		queue.WithReceiptHandleGenerator(func() string { return apiDeterministicReceiptHandle(receiptSequence.Add(1)) }),
	)
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", response)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if len(first.Body.Messages) != 1 {
		t.Fatalf("ReceiveMessage = %#v", first)
	}
	before := repository.Messages("orders")[0]
	body := fmt.Sprintf(`{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":%q,"VisibilityTimeout":1,"Padding":%q}`, first.Body.Messages[0].ReceiptHandle, strings.Repeat("x", 300))
	response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", body)
	requireError(t, response, http.StatusRequestEntityTooLarge, "RequestTooLarge")
	if got := repository.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before) {
		t.Fatalf("oversized request mutated state: %#v", got)
	}
}

type changeFailureRepository struct {
	queue.Repository
	err error
}

func (r changeFailureRepository) ChangeVisibility(queue.ChangeVisibilityCommand) error {
	return r.err
}

func TestChangeMessageVisibilityHTTPRepositoryFailureIs503AndRollsBack(t *testing.T) {
	backing := queue.NewMemoryRepository()
	testClock := &apiManualClock{now: time.Unix(100, 123)}
	var receiptSequence atomic.Uint64
	service := queue.NewService(
		changeFailureRepository{Repository: backing, err: storageUnavailableError()},
		queue.WithClock(testClock),
		queue.WithReceiptHandleGenerator(func() string { return apiDeterministicReceiptHandle(receiptSequence.Add(1)) }),
	)
	server := &localServer{URL: "http://simq.test", handler: api.NewServer(api.Config{
		PublicBaseURL: publicBaseURL,
		RequestIDGenerator: func() string {
			return fixedRequestID
		},
	}, service)}
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body-secret"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", response)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	receipt := first.Body.Messages[0].ReceiptHandle
	before := backing.Messages("orders")[0]
	response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", receipt, 60))
	requireError(t, response, http.StatusServiceUnavailable, "ServiceUnavailable")
	if strings.Contains(string(response.RawBody), storageSecret) || strings.Contains(string(response.RawBody), "body-secret") || strings.Contains(string(response.RawBody), receipt) {
		t.Fatalf("response leaked sensitive details: %s", response.RawBody)
	}
	if got := backing.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before) {
		t.Fatalf("repository failure mutated state: %#v", got)
	}
}

func TestChangeMessageVisibilityHTTPUnexpectedFailureIs500(t *testing.T) {
	backing := queue.NewMemoryRepository()
	service := queue.NewService(changeFailureRepository{Repository: backing, err: errors.New("unexpected")})
	server := &localServer{URL: "http://simq.test", handler: api.NewServer(api.Config{
		PublicBaseURL: publicBaseURL,
		RequestIDGenerator: func() string {
			return fixedRequestID
		},
	}, service)}
	response := mustPostAction(t, server, "/v1/sqs/ChangeMessageVisibility", changeVisibilityBody("orders", "rh_00000000000000000000000000000001", 1))
	requireError(t, response, http.StatusInternalServerError, "InternalError")
}

func changeVisibilityBody(queueName, receiptHandle string, timeout int) string {
	return fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q,"VisibilityTimeout":%d}`, publicBaseURL+"/queues/"+queueName, receiptHandle, timeout)
}
