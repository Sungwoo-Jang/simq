package api_test

import (
	"net/http"
	"testing"
	"time"
)

func TestDeleteMessageHTTPCompletesMessageLifecycle(t *testing.T) {
	server, repository, _ := newReceiveAPIServer(t, "30")
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`)
	if sent.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d", sent.StatusCode)
	}
	received := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if received.StatusCode != http.StatusOK || len(received.Body.Messages) != 1 {
		t.Fatalf("ReceiveMessage = %#v", received)
	}

	body := `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"` + received.Body.Messages[0].ReceiptHandle + `"}`
	deleted := mustPostAction(t, server, "/v1/sqs/DeleteMessage", body)
	if deleted.StatusCode != http.StatusOK || deleted.Body.RequestID == "" {
		t.Fatalf("DeleteMessage = %#v", deleted)
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatal("message was not deleted")
	}
	repeated := mustPostAction(t, server, "/v1/sqs/DeleteMessage", body)
	if repeated.StatusCode != http.StatusOK {
		t.Fatalf("repeated DeleteMessage = %#v", repeated)
	}
	finalReceive := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if finalReceive.StatusCode != http.StatusOK || len(finalReceive.Body.Messages) != 0 {
		t.Fatalf("ReceiveMessage after delete = %#v", finalReceive)
	}
}

func TestDeleteMessageHTTPStaleHandleIsNoOp(t *testing.T) {
	server, repository, testClock := newReceiveAPIServer(t, "10")
	if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d", response.StatusCode)
	}
	first := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if first.StatusCode != http.StatusOK || len(first.Body.Messages) != 1 {
		t.Fatalf("first ReceiveMessage = %#v", first)
	}
	testClock.Advance(10 * time.Second)
	second := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if second.StatusCode != http.StatusOK || len(second.Body.Messages) != 1 {
		t.Fatalf("second ReceiveMessage = %#v", second)
	}

	staleBody := `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"` + first.Body.Messages[0].ReceiptHandle + `"}`
	staleDelete := mustPostAction(t, server, "/v1/sqs/DeleteMessage", staleBody)
	if staleDelete.StatusCode != http.StatusOK {
		t.Fatalf("stale DeleteMessage = %#v", staleDelete)
	}
	if repository.MessageCount("orders") != 1 {
		t.Fatal("stale handle deleted the latest generation")
	}
	currentBody := `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"` + second.Body.Messages[0].ReceiptHandle + `"}`
	currentDelete := mustPostAction(t, server, "/v1/sqs/DeleteMessage", currentBody)
	if currentDelete.StatusCode != http.StatusOK || repository.MessageCount("orders") != 0 {
		t.Fatalf("current DeleteMessage = %#v, count = %d", currentDelete, repository.MessageCount("orders"))
	}
}

func TestDeleteMessageReturnsStableErrorsWithoutMutation(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "missing QueueUrl", body: `{"ReceiptHandle":"rh_00000000000000000000000000000001"}`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "missing ReceiptHandle", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`, wantStatus: 400, wantCode: "ReceiptHandleIsInvalid"},
		{name: "malformed ReceiptHandle", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"bad"}`, wantStatus: 400, wantCode: "ReceiptHandleIsInvalid"},
		{name: "unissued ReceiptHandle", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_ffffffffffffffffffffffffffffffff"}`, wantStatus: 400, wantCode: "ReceiptHandleIsInvalid"},
		{name: "wrong QueueUrl type", body: `{"QueueUrl":1,"ReceiptHandle":"rh_00000000000000000000000000000001"}`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "wrong ReceiptHandle type", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":1}`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "unknown field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_00000000000000000000000000000001","Extra":true}`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "duplicate field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_00000000000000000000000000000001","ReceiptHandle":"rh_00000000000000000000000000000002"}`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "malformed JSON", body: `{"QueueUrl":`, wantStatus: 400, wantCode: "InvalidRequest"},
		{name: "trailing JSON", body: `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"rh_00000000000000000000000000000001"} {}`, wantStatus: 400, wantCode: "InvalidRequest"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository, _ := newReceiveAPIServer(t, "30")
			if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"}`); response.StatusCode != http.StatusOK {
				t.Fatalf("SendMessage status = %d", response.StatusCode)
			}
			received := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
			if received.StatusCode != http.StatusOK || len(received.Body.Messages) != 1 {
				t.Fatalf("ReceiveMessage = %#v", received)
			}

			response := mustPostAction(t, server, "/v1/sqs/DeleteMessage", test.body)
			requireError(t, response, test.wantStatus, test.wantCode)
			if repository.MessageCount("orders") != 1 {
				t.Fatal("invalid delete mutated message state")
			}
			validBody := `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"` + received.Body.Messages[0].ReceiptHandle + `"}`
			if valid := mustPostAction(t, server, "/v1/sqs/DeleteMessage", validBody); valid.StatusCode != http.StatusOK {
				t.Fatalf("valid DeleteMessage after invalid request = %#v", valid)
			}
		})
	}
}

func TestDeleteMessageMissingQueueReturnsQueueDoesNotExist(t *testing.T) {
	server, _ := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/DeleteMessage", `{
		"QueueUrl":"http://localhost:9324/queues/missing",
		"ReceiptHandle":"rh_00000000000000000000000000000001"
	}`)
	requireError(t, response, http.StatusNotFound, "QueueDoesNotExist")
}
