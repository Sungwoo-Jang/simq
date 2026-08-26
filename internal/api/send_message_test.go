package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSendMessageStoresMessageAndReturnsDigest(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{
		"QueueUrl":"http://localhost:9324/queues/orders",
		"MessageBody":"order-created"
	}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d, want 200", response.StatusCode)
	}
	if response.Body.MessageID == "" {
		t.Fatal("MessageId is empty")
	}
	if response.Body.MD5OfMessageBody != "bb493e6546e1863734c792e8ea97e3ba" {
		t.Fatalf("MD5OfMessageBody = %q", response.Body.MD5OfMessageBody)
	}
	if repository.MessageCount("orders") != 1 {
		t.Fatalf("message count = %d, want 1", repository.MessageCount("orders"))
	}
	stored := repository.Messages("orders")
	if len(stored) != 1 || stored[0].ID != response.Body.MessageID || stored[0].Body != "order-created" {
		t.Fatalf("stored messages = %#v", stored)
	}
}

func TestSendMessageAcceptsIntegerDelaySeconds(t *testing.T) {
	server, repository := newTestServer(t)
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","DelaySeconds":10}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", response)
	}
	stored := repository.Messages("orders")
	if len(stored) != 1 || stored[0].AvailableAt.Sub(time.UnixMilli(stored[0].SentAtMillis)) < 10*time.Second {
		t.Fatalf("stored delayed message = %#v", stored)
	}
}

func TestSendMessageReturnsQueueDoesNotExistWithoutMutation(t *testing.T) {
	server, repository := newTestServer(t)

	response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{
		"QueueUrl":"http://localhost:9324/queues/missing",
		"MessageBody":"order-created"
	}`)
	requireError(t, response, http.StatusNotFound, "QueueDoesNotExist")
	if repository.MessageCount("missing") != 0 {
		t.Fatalf("missing queue message count = %d, want 0", repository.MessageCount("missing"))
	}
}

func TestSendMessageRejectsInvalidRequestsWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ``},
		{name: "missing QueueUrl", body: `{"MessageBody":"body"}`},
		{name: "empty QueueUrl", body: `{"QueueUrl":"","MessageBody":"body"}`},
		{name: "foreign QueueUrl", body: `{"QueueUrl":"http://other.example/queues/orders","MessageBody":"body"}`},
		{name: "nested QueueUrl path", body: `{"QueueUrl":"http://localhost:9324/queues/orders/other","MessageBody":"body"}`},
		{name: "wrong QueueUrl type", body: `{"QueueUrl":123,"MessageBody":"body"}`},
		{name: "missing MessageBody", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{name: "empty MessageBody", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":""}`},
		{name: "wrong MessageBody type", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":123}`},
		{name: "unknown field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","Unknown":1}`},
		{name: "wrong delay type", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","DelaySeconds":"1"}`},
		{name: "fractional delay", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","DelaySeconds":1.5}`},
		{name: "negative delay", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","DelaySeconds":-1}`},
		{name: "delay too large", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","DelaySeconds":901}`},
		{name: "malformed", body: `{"QueueUrl":`},
		{name: "invalid UTF-8", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"` + string([]byte{0xff}) + `"}`},
		{name: "trailing JSON", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"} {}`},
		{name: "duplicate field", body: `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","MessageBody":"other"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
			if created.StatusCode != http.StatusOK {
				t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
			}

			response := mustPostAction(t, server, "/v1/sqs/SendMessage", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.MessageCount("orders") != 0 {
				t.Fatalf("message count = %d after invalid request, want 0", repository.MessageCount("orders"))
			}
		})
	}
}

func TestSendMessageBodySizeBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		bodySize   int
		wantStatus int
	}{
		{name: "one byte", bodySize: 1, wantStatus: http.StatusOK},
		{name: "one MiB", bodySize: 1 << 20, wantStatus: http.StatusOK},
		{name: "over one MiB", bodySize: (1 << 20) + 1, wantStatus: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
			if created.StatusCode != http.StatusOK {
				t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
			}
			encoded, err := json.Marshal(map[string]string{
				"QueueUrl":    publicBaseURL + "/queues/orders",
				"MessageBody": strings.Repeat("a", test.bodySize),
			})
			if err != nil {
				t.Fatal(err)
			}

			response := mustPostAction(t, server, "/v1/sqs/SendMessage", string(encoded))
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			wantCount := 0
			if test.wantStatus == http.StatusOK {
				wantCount = 1
			} else {
				requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			}
			if repository.MessageCount("orders") != wantCount {
				t.Fatalf("message count = %d, want %d", repository.MessageCount("orders"), wantCount)
			}
		})
	}
}

func TestSendMessageUsesConfiguredPublicBaseURLNotHost(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	response, err := postAction(server.Client(), server.URL+"/v1/sqs/SendMessage", "attacker.example", `{
		"QueueUrl":"http://localhost:9324/queues/orders",
		"MessageBody":"body"
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage status = %d, want 200", response.StatusCode)
	}
	if repository.MessageCount("orders") != 1 {
		t.Fatalf("message count = %d, want 1", repository.MessageCount("orders"))
	}
}

func TestConcurrentSendMessageUsesUniqueIDs(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	const requests = 20
	ids := make(chan string, requests)
	errors := make(chan error, requests)
	var wait sync.WaitGroup
	wait.Add(requests)
	for i := 0; i < requests; i++ {
		i := i
		go func() {
			defer wait.Done()
			body := fmt.Sprintf(`{"QueueUrl":"%s/queues/orders","MessageBody":"body-%d"}`, publicBaseURL, i)
			response, err := postAction(server.Client(), server.URL+"/v1/sqs/SendMessage", "", body)
			if err != nil {
				errors <- err
				return
			}
			if response.StatusCode != http.StatusOK {
				errors <- fmt.Errorf("status = %d, want 200", response.StatusCode)
				return
			}
			ids <- response.Body.MessageID
		}()
	}
	wait.Wait()
	close(ids)
	close(errors)

	for err := range errors {
		t.Error(err)
	}
	seen := make(map[string]struct{}, requests)
	for id := range ids {
		if id == "" {
			t.Error("empty MessageId")
			continue
		}
		if _, exists := seen[id]; exists {
			t.Errorf("duplicate MessageId %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != requests {
		t.Fatalf("unique MessageIds = %d, want %d", len(seen), requests)
	}
	if repository.MessageCount("orders") != requests {
		t.Fatalf("message count = %d, want %d", repository.MessageCount("orders"), requests)
	}
}

func TestSendMessageKeepsQueuesIsolated(t *testing.T) {
	server, repository := newTestServer(t)
	for _, queueName := range []string{"orders", "payments"} {
		created := mustPostAction(t, server, "/v1/sqs/CreateQueue", fmt.Sprintf(`{"QueueName":%q}`, queueName))
		if created.StatusCode != http.StatusOK {
			t.Fatalf("CreateQueue(%s) status = %d, want 200", queueName, created.StatusCode)
		}
	}

	for _, test := range []struct {
		queueName string
		body      string
	}{
		{queueName: "orders", body: "order-created"},
		{queueName: "payments", body: "payment-created"},
	} {
		requestBody := fmt.Sprintf(`{"QueueUrl":"%s/queues/%s","MessageBody":%q}`, publicBaseURL, test.queueName, test.body)
		response := mustPostAction(t, server, "/v1/sqs/SendMessage", requestBody)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("SendMessage(%s) status = %d, want 200", test.queueName, response.StatusCode)
		}
	}

	orders := repository.Messages("orders")
	payments := repository.Messages("payments")
	if len(orders) != 1 || orders[0].QueueName != "orders" || orders[0].Body != "order-created" {
		t.Fatalf("orders messages = %#v", orders)
	}
	if len(payments) != 1 || payments[0].QueueName != "payments" || payments[0].Body != "payment-created" {
		t.Fatalf("payments messages = %#v", payments)
	}
}
