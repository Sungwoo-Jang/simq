package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

type batchHTTPFailure struct {
	ID          string `json:"Id"`
	Code        string `json:"Code"`
	SenderFault bool   `json:"SenderFault"`
}
type batchHTTPSuccess struct {
	ID                     string `json:"Id"`
	MessageID              string `json:"MessageId"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes"`
	SequenceNumber         string `json:"SequenceNumber"`
}
type batchHTTPResponse struct {
	Successful []batchHTTPSuccess `json:"Successful"`
	Failed     []batchHTTPFailure `json:"Failed"`
	RequestID  string             `json:"RequestId"`
}

func TestBatchHTTPPartialResultsStrictJSONAndRequestErrors(t *testing.T) {
	server, repository := newTestServer(t)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	queueURL := publicBaseURL + "/queues/orders"
	body := fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"first","MessageBody":"one"},{"Id":"bad","MessageBody":""},{"Id":"third","MessageBody":"three","MessageAttributes":{"Kind":{"DataType":"String.event","StringValue":"created"}}}]}`, queueURL)
	response := mustPostAction(t, server, "/v1/sqs/SendMessageBatch", body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", response.StatusCode, response.RawBody)
	}
	var decoded batchHTTPResponse
	if err := json.Unmarshal(response.RawBody, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Successful) != 2 || decoded.Successful[0].ID != "first" || decoded.Successful[1].ID != "third" || decoded.Successful[1].MD5OfMessageAttributes == "" || len(decoded.Failed) != 1 || decoded.Failed[0].ID != "bad" || decoded.Failed[0].Code != "InvalidRequest" || !decoded.Failed[0].SenderFault || decoded.RequestID != fixedRequestID {
		t.Fatalf("batch response = %#v", decoded)
	}

	tests := []struct{ name, body, code string }{
		{"empty", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[]}`, queueURL), "EmptyBatchRequest"},
		{"duplicate", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"same","MessageBody":"a"},{"Id":"same","MessageBody":"b"}]}`, queueURL), "BatchEntryIdsNotDistinct"},
		{"invalid id", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"bad.id","MessageBody":"a"}]}`, queueURL), "InvalidBatchEntryId"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := repository.MessageCount("orders")
			value := mustPostAction(t, server, "/v1/sqs/SendMessageBatch", test.body)
			requireError(t, value, http.StatusBadRequest, test.code)
			if repository.MessageCount("orders") != before {
				t.Fatal("request-level error mutated queue")
			}
		})
	}
	unknown := mustPostAction(t, server, "/v1/sqs/SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"x","MessageBody":"secret","Unknown":true}]}`, queueURL))
	requireError(t, unknown, http.StatusBadRequest, "InvalidRequest")
	if strings.Contains(string(unknown.RawBody), "secret") {
		t.Fatal("error leaked body")
	}
}

func TestSendMessageBatchAggregateSizeBoundary(t *testing.T) {
	server, repository := newTestServer(t)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	queueURL := publicBaseURL + "/queues/orders"
	exact := fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"a","MessageBody":%q},{"Id":"b","MessageBody":"b"}]}`, queueURL, strings.Repeat("a", (1<<20)-1))
	response := mustPostAction(t, server, "/v1/sqs/SendMessageBatch", exact)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("exact boundary = %d: %s", response.StatusCode, response.RawBody)
	}
	before := repository.MessageCount("orders")
	over := fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"a","MessageBody":%q},{"Id":"b","MessageBody":"bb"}]}`, queueURL, strings.Repeat("a", (1<<20)-1))
	response = mustPostAction(t, server, "/v1/sqs/SendMessageBatch", over)
	requireError(t, response, http.StatusBadRequest, "BatchRequestTooLong")
	if repository.MessageCount("orders") != before {
		t.Fatal("oversized batch mutated queue")
	}
}
