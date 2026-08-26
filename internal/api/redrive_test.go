package api_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestM3RedriveHTTPContract(t *testing.T) {
	server, _ := newTestServer(t)
	source := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"source"}`)
	dlq := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"dead"}`)
	if source.StatusCode != http.StatusOK || dlq.StatusCode != http.StatusOK {
		t.Fatal("queue setup failed")
	}

	sourceAttributes := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"`+source.Body.QueueURL+`","AttributeNames":["QueueArn"]}`)
	dlqAttributes := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", `{"QueueUrl":"`+dlq.Body.QueueURL+`","AttributeNames":["QueueArn"]}`)
	sourceARN, dlqARN := sourceAttributes.Body.Attributes["QueueArn"], dlqAttributes.Body.Attributes["QueueArn"]
	if sourceARN == "" || dlqARN == "" {
		t.Fatalf("ARN attributes = %#v %#v", sourceAttributes.Body.Attributes, dlqAttributes.Body.Attributes)
	}

	policyBytes, _ := json.Marshal(map[string]string{"deadLetterTargetArn": dlqARN, "maxReceiveCount": "2"})
	setBody, _ := json.Marshal(map[string]any{"QueueUrl": source.Body.QueueURL, "Attributes": map[string]string{"RedrivePolicy": string(policyBytes)}})
	set := mustPostAction(t, server, "/v1/sqs/SetQueueAttributes", string(setBody))
	if set.StatusCode != http.StatusOK {
		t.Fatalf("set policy = %d %s", set.StatusCode, set.RawBody)
	}

	listed := mustPostAction(t, server, "/v1/sqs/ListDeadLetterSourceQueues", `{"QueueUrl":"`+dlq.Body.QueueURL+`","MaxResults":10}`)
	if listed.StatusCode != http.StatusOK || len(listed.Body.QueueURLs) != 1 || listed.Body.QueueURLs[0] != source.Body.QueueURL {
		t.Fatalf("sources = %d %s", listed.StatusCode, listed.RawBody)
	}

	deleteTarget := mustPostAction(t, server, "/v1/sqs/DeleteQueue", `{"QueueUrl":"`+dlq.Body.QueueURL+`"}`)
	requireError(t, deleteTarget, http.StatusConflict, "ResourceInUse")

	start := mustPostAction(t, server, "/v1/sqs/StartMessageMoveTask", `{"SourceArn":"`+dlqARN+`"}`)
	if start.StatusCode != http.StatusOK {
		t.Fatalf("start = %d %s", start.StatusCode, start.RawBody)
	}
	var started struct {
		TaskHandle string `json:"TaskHandle"`
	}
	if err := json.Unmarshal(start.RawBody, &started); err != nil || started.TaskHandle == "" {
		t.Fatalf("start response = %s, %v", start.RawBody, err)
	}

	list := mustPostAction(t, server, "/v1/sqs/ListMessageMoveTasks", `{"SourceArn":"`+dlqARN+`","MaxResults":10}`)
	if list.StatusCode != http.StatusOK {
		t.Fatalf("list tasks = %d %s", list.StatusCode, list.RawBody)
	}
	var taskList struct {
		Results []struct{ TaskHandle, Status string } `json:"Results"`
	}
	if err := json.Unmarshal(list.RawBody, &taskList); err != nil || len(taskList.Results) != 1 || taskList.Results[0].TaskHandle != started.TaskHandle || taskList.Results[0].Status != "COMPLETED" {
		t.Fatalf("task list = %s, %v", list.RawBody, err)
	}

	cancel := mustPostAction(t, server, "/v1/sqs/CancelMessageMoveTask", `{"TaskHandle":"`+started.TaskHandle+`"}`)
	requireError(t, cancel, http.StatusConflict, "ResourceNotInUse")
}

func TestM3ActionsRejectNullOptionalFields(t *testing.T) {
	server, _ := newTestServer(t)
	for _, test := range []struct{ path, body string }{
		{"/v1/sqs/ListDeadLetterSourceQueues", `{"QueueUrl":"http://localhost:9324/queues/dead/q_00000000000000000000000000000001","MaxResults":null}`},
		{"/v1/sqs/StartMessageMoveTask", `{"SourceArn":"arn:simq:sqs:::dead/q_00000000000000000000000000000001","DestinationArn":null}`},
		{"/v1/sqs/ListMessageMoveTasks", `{"SourceArn":"arn:simq:sqs:::dead/q_00000000000000000000000000000001","MaxResults":null}`},
	} {
		response := mustPostAction(t, server, test.path, test.body)
		requireError(t, response, http.StatusBadRequest, "InvalidRequest")
	}
}
