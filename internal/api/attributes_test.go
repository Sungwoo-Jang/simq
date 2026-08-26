package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"simq/internal/api"
	"simq/internal/queue"
)

func TestMessageAttributesHTTPFlowAndProjection(t *testing.T) {
	server, _ := newTestServer(t)
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"0"}}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{
		"QueueUrl":"http://localhost:9324/queues/orders",
		"MessageBody":"body",
		"MessageAttributes":{
			"EventType":{"DataType":"String.event","StringValue":"OrderCreated"},
			"Attempt":{"DataType":"Number","StringValue":"1"},
			"Payload":{"DataType":"Binary.data","BinaryValue":"AQID"}
		}
	}`)
	if sent.StatusCode != http.StatusOK || sent.Body.MD5OfMessageAttributes == "" {
		t.Fatalf("SendMessage = %#v", sent)
	}

	none := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if none.StatusCode != http.StatusOK || len(none.Body.Messages) != 1 || len(none.Body.Messages[0].MessageAttributes) != 0 || none.Body.Messages[0].MD5OfMessageAttributes != "" {
		t.Fatalf("ReceiveMessage omitted projection = %#v", none)
	}
	if strings.Contains(string(none.RawBody), `"MessageAttributes"`) || strings.Contains(string(none.RawBody), `"MD5OfMessageAttributes"`) {
		t.Fatalf("empty projection fields were not omitted: %s", none.RawBody)
	}

	selected := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageAttributeNames":["EventType","Payload"]}`)
	if selected.StatusCode != http.StatusOK || len(selected.Body.Messages) != 1 || len(selected.Body.Messages[0].MessageAttributes) != 2 || selected.Body.Messages[0].MessageAttributes["EventType"].StringValue != "OrderCreated" || selected.Body.Messages[0].MessageAttributes["Payload"].BinaryValue != "AQID" || selected.Body.Messages[0].MD5OfMessageAttributes == "" {
		t.Fatalf("selected ReceiveMessage = %#v", selected)
	}
	if selected.Body.Messages[0].MD5OfMessageAttributes == sent.Body.MD5OfMessageAttributes {
		t.Fatal("projection digest unexpectedly equals full-set digest")
	}

	all := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageAttributeNames":["All"]}`)
	if all.StatusCode != http.StatusOK || len(all.Body.Messages) != 1 || len(all.Body.Messages[0].MessageAttributes) != 3 || all.Body.Messages[0].MD5OfMessageAttributes != sent.Body.MD5OfMessageAttributes {
		t.Fatalf("all ReceiveMessage = %#v", all)
	}
}

func TestSendMessageHTTPEmptyAttributesEqualOmission(t *testing.T) {
	server, _ := newTestServer(t)
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	for _, suffix := range []string{"", `,"MessageAttributes":{}`} {
		response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body"`+suffix+`}`)
		if response.StatusCode != http.StatusOK || response.Body.MD5OfMessageAttributes != "" || strings.Contains(string(response.RawBody), "MD5OfMessageAttributes") {
			t.Fatalf("empty attributes response = %#v, %s", response, response.RawBody)
		}
	}
}

func TestSendMessageHTTPRejectsMalformedMessageAttributesWithoutMutation(t *testing.T) {
	eleven := make([]string, 0, 11)
	for index := 0; index < 11; index++ {
		eleven = append(eleven, fmt.Sprintf(`"A%d":{"DataType":"String","StringValue":"x"}`, index))
	}
	tests := []struct {
		name       string
		attributes string
	}{
		{name: "null", attributes: `null`},
		{name: "array", attributes: `[]`},
		{name: "value null", attributes: `{"A":null}`},
		{name: "unknown nested field", attributes: `{"A":{"DataType":"String","StringValue":"secret-value","Unknown":true}}`},
		{name: "duplicate nested field", attributes: `{"A":{"DataType":"String","StringValue":"one","StringValue":"two"}}`},
		{name: "invalid base64", attributes: `{"A":{"DataType":"Binary","BinaryValue":"not-base64!"}}`},
		{name: "wrong value field", attributes: `{"A":{"DataType":"Binary","StringValue":"AQID"}}`},
		{name: "invalid number", attributes: `{"A":{"DataType":"Number","StringValue":"NaN"}}`},
		{name: "eleven", attributes: `{` + strings.Join(eleven, ",") + `}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
				t.Fatalf("CreateQueue = %#v", response)
			}
			body := `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","MessageAttributes":` + test.attributes + `}`
			response := mustPostAction(t, server, "/v1/sqs/SendMessage", body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.MessageCount("orders") != 0 {
				t.Fatal("invalid attributes enqueued a message")
			}
			if strings.Contains(string(response.RawBody), "secret-value") || strings.Contains(string(response.RawBody), "AQID") {
				t.Fatalf("error exposed attribute value: %s", response.RawBody)
			}
		})
	}
}

func TestReceiveMessageHTTPRejectsInvalidAttributeSelectionsWithoutClaim(t *testing.T) {
	for _, selection := range []string{`["All","A"]`, `["A","A"]`, `[".*"]`, `["A.*"]`, `null`, `[1]`} {
		t.Run(selection, func(t *testing.T) {
			server, _, _ := newReceiveAPIServer(t, "30")
			if response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","MessageAttributes":{"A":{"DataType":"String","StringValue":"secret"}}}`); response.StatusCode != http.StatusOK {
				t.Fatalf("SendMessage = %#v", response)
			}
			invalid := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageAttributeNames":`+selection+`}`)
			requireError(t, invalid, http.StatusBadRequest, "InvalidRequest")
			valid := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageAttributeNames":["All"]}`)
			if valid.StatusCode != http.StatusOK || len(valid.Body.Messages) != 1 || valid.Body.Messages[0].Attributes["ApproximateReceiveCount"] != "1" {
				t.Fatalf("invalid projection claimed message: %#v", valid)
			}
		})
	}
}

func TestGetAndSetQueueAttributesHTTPContract(t *testing.T) {
	server, _ := newTestServer(t)
	if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	empty := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/orders"}`)
	if empty.StatusCode != http.StatusOK || len(empty.Body.Attributes) != 0 {
		t.Fatalf("GetQueueAttributes(empty) = %#v", empty)
	}
	set := mustPostAction(t, server, "/v1/sqs/SetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":"60","DelaySeconds":"5"}}`)
	if set.StatusCode != http.StatusOK {
		t.Fatalf("SetQueueAttributes = %#v", set)
	}
	all := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["All"]}`)
	if all.StatusCode != http.StatusOK || all.Body.Attributes["VisibilityTimeout"] != "60" || all.Body.Attributes["DelaySeconds"] != "5" || all.Body.Attributes["MessageRetentionPeriod"] != "345600" {
		t.Fatalf("GetQueueAttributes(All) = %#v", all)
	}
	exact := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["DelaySeconds"]}`)
	if exact.StatusCode != http.StatusOK || len(exact.Body.Attributes) != 1 || exact.Body.Attributes["DelaySeconds"] != "5" {
		t.Fatalf("GetQueueAttributes(exact) = %#v", exact)
	}
	missing := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/missing","AttributeNames":["All"]}`)
	requireError(t, missing, http.StatusNotFound, "QueueDoesNotExist")
	missingSet := mustPostAction(t, server, "/v1/sqs/SetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/missing","Attributes":{"VisibilityTimeout":"1"}}`)
	requireError(t, missingSet, http.StatusNotFound, "QueueDoesNotExist")
}

func TestQueueAttributesHTTPRejectsAmbiguousAndInvalidRequests(t *testing.T) {
	tests := []struct {
		path string
		body string
	}{
		{path: "/v1/sqs/GetQueueAttributes", body: `{}`},
		{path: "/v1/sqs/GetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["Unknown"]}`},
		{path: "/v1/sqs/GetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["All","DelaySeconds"]}`},
		{path: "/v1/sqs/GetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","AttributeNames":["DelaySeconds","DelaySeconds"]}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders"}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":null}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{}}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":60}}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"Unknown":"1"}}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":"1","VisibilityTimeout":"2"}}`},
		{path: "/v1/sqs/SetQueueAttributes", body: `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":"1"}} {}`},
	}
	for _, test := range tests {
		t.Run(test.path+test.body, func(t *testing.T) {
			server, repository := newTestServer(t)
			if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
				t.Fatalf("CreateQueue = %#v", response)
			}
			response := mustPostAction(t, server, test.path, test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			stored, found, err := repository.Get("orders")
			if err != nil || !found || stored.VisibilityTimeout != 30 || stored.DelaySeconds != 0 || stored.MessageRetentionPeriod != 345600 {
				t.Fatalf("invalid request mutated queue: %#v, %v, %v", stored, found, err)
			}
		})
	}
}

type attributeEnqueueFailureRepository struct{ queue.Repository }

func (attributeEnqueueFailureRepository) Enqueue(queue.EnqueueCommand) (queue.Message, error) {
	return queue.Message{}, storageUnavailableError()
}

type queueAttributeFailureRepository struct{ queue.Repository }

func (queueAttributeFailureRepository) SetAttributes(queue.QueueAttributesCommand) (queue.Queue, error) {
	return queue.Queue{}, storageUnavailableError()
}

func TestAttributeMutationStorageFailuresReturn503WithoutPartialStateOrValues(t *testing.T) {
	t.Run("message attributes", func(t *testing.T) {
		backing := queue.NewMemoryRepository()
		service := queue.NewService(attributeEnqueueFailureRepository{Repository: backing})
		server := &localServer{URL: "http://simq.test", handler: api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }}, service)}
		if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
			t.Fatalf("CreateQueue = %#v", response)
		}
		response := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body-secret","MessageAttributes":{"Sensitive":{"DataType":"String","StringValue":"attribute-secret"}}}`)
		requireError(t, response, http.StatusServiceUnavailable, "ServiceUnavailable")
		if backing.MessageCount("orders") != 0 || strings.Contains(string(response.RawBody), "body-secret") || strings.Contains(string(response.RawBody), "attribute-secret") {
			t.Fatalf("failed enqueue state/response = %d, %s", backing.MessageCount("orders"), response.RawBody)
		}
	})

	t.Run("queue attributes", func(t *testing.T) {
		backing := queue.NewMemoryRepository()
		service := queue.NewService(queueAttributeFailureRepository{Repository: backing})
		server := &localServer{URL: "http://simq.test", handler: api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }}, service)}
		if response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.StatusCode != http.StatusOK {
			t.Fatalf("CreateQueue = %#v", response)
		}
		response := mustPostAction(t, server, "/v1/sqs/SetQueueAttributes", `{"QueueUrl":"http://localhost:9324/queues/orders","Attributes":{"VisibilityTimeout":"60","DelaySeconds":"5"}}`)
		requireError(t, response, http.StatusServiceUnavailable, "ServiceUnavailable")
		stored, found, err := backing.Get("orders")
		if err != nil || !found || stored.VisibilityTimeout != 30 || stored.DelaySeconds != 0 {
			t.Fatalf("failed Set mutated queue: %#v, %v, %v", stored, found, err)
		}
	})
}
