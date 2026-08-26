package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestFIFOHTTPSingleBatchAndReceiveFields(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders.fifo","Attributes":{"FifoQueue":"true"}}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %d: %s", created.StatusCode, created.RawBody)
	}
	queueURL := publicBaseURL + "/queues/orders.fifo"
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"one","MessageGroupId":"g1","MessageDeduplicationId":"d1"}`)
	if sent.StatusCode != http.StatusOK || sent.Body.MessageID == "" || sent.Body.SequenceNumber != "1" {
		t.Fatalf("SendMessage = %#v, %s", sent, sent.RawBody)
	}
	duplicate := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"ignored","MessageGroupId":"other","MessageDeduplicationId":"d1"}`)
	if duplicate.StatusCode != http.StatusOK || duplicate.Body.MessageID != sent.Body.MessageID || duplicate.Body.SequenceNumber != "1" || repository.MessageCount("orders.fifo") != 1 {
		t.Fatalf("duplicate SendMessage = %#v", duplicate)
	}

	batch := mustPostAction(t, server, "/v1/sqs/SendMessageBatch", `{"QueueUrl":"`+queueURL+`","Entries":[{"Id":"a","MessageBody":"two","MessageGroupId":"g2","MessageDeduplicationId":"d2"},{"Id":"b","MessageBody":"three","MessageGroupId":"g3","MessageDeduplicationId":"d3"}]}`)
	if batch.StatusCode != http.StatusOK {
		t.Fatalf("SendMessageBatch = %d: %s", batch.StatusCode, batch.RawBody)
	}
	var batchBody batchHTTPResponse
	if err := json.Unmarshal(batch.RawBody, &batchBody); err != nil {
		t.Fatal(err)
	}
	if len(batchBody.Successful) != 2 || batchBody.Successful[0].SequenceNumber != "2" || batchBody.Successful[1].SequenceNumber != "3" || len(batchBody.Failed) != 0 {
		t.Fatalf("FIFO batch = %#v", batchBody)
	}

	received := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10,"ReceiveRequestAttemptId":"attempt-1"}`)
	if received.StatusCode != http.StatusOK || len(received.Body.Messages) != 3 {
		t.Fatalf("ReceiveMessage = %#v, %s", received, received.RawBody)
	}
	for index, message := range received.Body.Messages {
		if message.MessageGroupID == "" || message.MessageDeduplicationID == "" || message.SequenceNumber == "" {
			t.Fatalf("FIFO message[%d] = %#v", index, message)
		}
	}
	replayed := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10,"ReceiveRequestAttemptId":"attempt-1"}`)
	if replayed.StatusCode != http.StatusOK || string(replayed.RawBody) != string(received.RawBody) {
		t.Fatalf("ReceiveMessage replay = %s, first = %s", replayed.RawBody, received.RawBody)
	}
}

func TestFIFOHTTPStrictValidation(t *testing.T) {
	server, repository := newTestServer(t)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders.fifo","Attributes":{"FifoQueue":"true"}}`)
	tests := []struct {
		name string
		path string
		body string
	}{
		{"group type", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","MessageBody":"body","MessageGroupId":1,"MessageDeduplicationId":"d"}`},
		{"dedup type", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","MessageBody":"body","MessageGroupId":"g","MessageDeduplicationId":1}`},
		{"empty group", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","MessageBody":"body","MessageGroupId":"","MessageDeduplicationId":"d"}`},
		{"empty dedup", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","MessageBody":"body","MessageGroupId":"g","MessageDeduplicationId":""}`},
		{"FIFO field on Standard", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","MessageGroupId":"g","MessageDeduplicationId":"d"}`},
		{"empty FIFO field on Standard", "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"body","MessageGroupId":""}`},
		{"batch group type", "/v1/sqs/SendMessageBatch", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","Entries":[{"Id":"a","MessageBody":"body","MessageGroupId":1,"MessageDeduplicationId":"d"}]}`},
		{"empty attempt", "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","ReceiveRequestAttemptId":""}`},
		{"attempt type", "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders.fifo","ReceiveRequestAttemptId":1}`},
		{"attempt on Standard", "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiveRequestAttemptId":"attempt"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeStandard := repository.MessageCount("orders")
			beforeFIFO := repository.MessageCount("orders.fifo")
			response := mustPostAction(t, server, test.path, test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.MessageCount("orders") != beforeStandard || repository.MessageCount("orders.fifo") != beforeFIFO {
				t.Fatal("invalid FIFO request mutated queues")
			}
		})
	}
}
