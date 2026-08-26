package api_test

import (
	"fmt"
	"net/http"
	"testing"
)

func TestAdministrationActionsThroughHTTP(t *testing.T) {
	server, _ := newTestServer(t)
	alpha := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"alpha"}`)
	orders := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if alpha.StatusCode != http.StatusOK || orders.StatusCode != http.StatusOK {
		t.Fatal("create failed")
	}

	first := mustPostAction(t, server, "/v1/sqs/ListQueues", `{"MaxResults":1}`)
	if first.StatusCode != http.StatusOK || len(first.Body.QueueURLs) != 1 || first.Body.QueueURLs[0] != alpha.Body.QueueURL || first.Body.NextToken == "" {
		t.Fatalf("ListQueues = %#v", first)
	}
	second := mustPostAction(t, server, "/v1/sqs/ListQueues", fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, first.Body.NextToken))
	if second.StatusCode != http.StatusOK || len(second.Body.QueueURLs) != 1 || second.Body.QueueURLs[0] != orders.Body.QueueURL || second.Body.NextToken != "" {
		t.Fatalf("ListQueues page 2 = %#v", second)
	}

	tagged := mustPostAction(t, server, "/v1/sqs/TagQueue", fmt.Sprintf(`{"QueueUrl":%q,"Tags":{"env":"prod","team":"queue"}}`, orders.Body.QueueURL))
	if tagged.StatusCode != http.StatusOK {
		t.Fatalf("TagQueue = %#v", tagged)
	}
	untagged := mustPostAction(t, server, "/v1/sqs/UntagQueue", fmt.Sprintf(`{"QueueUrl":%q,"TagKeys":["team"]}`, orders.Body.QueueURL))
	if untagged.StatusCode != http.StatusOK {
		t.Fatalf("UntagQueue = %#v", untagged)
	}
	listedTags := mustPostAction(t, server, "/v1/sqs/ListQueueTags", fmt.Sprintf(`{"QueueUrl":%q}`, orders.Body.QueueURL))
	if listedTags.StatusCode != http.StatusOK || len(listedTags.Body.Tags) != 1 || listedTags.Body.Tags["env"] != "prod" {
		t.Fatalf("ListQueueTags = %#v", listedTags)
	}

	permission := mustPostAction(t, server, "/v1/sqs/AddPermission", fmt.Sprintf(`{"QueueUrl":%q,"Label":"writers","AWSAccountIds":["111111111111"],"Actions":["SendMessage"]}`, orders.Body.QueueURL))
	if permission.StatusCode != http.StatusOK {
		t.Fatalf("AddPermission = %#v", permission)
	}
	policy := mustPostAction(t, server, "/v1/sqs/GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["Policy"]}`, orders.Body.QueueURL))
	if policy.StatusCode != http.StatusOK || policy.Body.Attributes["Policy"] == "" {
		t.Fatalf("Policy = %#v", policy)
	}
	removed := mustPostAction(t, server, "/v1/sqs/RemovePermission", fmt.Sprintf(`{"QueueUrl":%q,"Label":"writers"}`, orders.Body.QueueURL))
	if removed.StatusCode != http.StatusOK {
		t.Fatalf("RemovePermission = %#v", removed)
	}

	mustPostAction(t, server, "/v1/sqs/SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"gone"}`, orders.Body.QueueURL))
	purged := mustPostAction(t, server, "/v1/sqs/PurgeQueue", fmt.Sprintf(`{"QueueUrl":%q}`, orders.Body.QueueURL))
	if purged.StatusCode != http.StatusOK {
		t.Fatalf("PurgeQueue = %#v", purged)
	}
	empty := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, orders.Body.QueueURL))
	if empty.StatusCode != http.StatusOK || len(empty.Body.Messages) != 0 {
		t.Fatalf("after purge = %#v", empty)
	}

	deleted := mustPostAction(t, server, "/v1/sqs/DeleteQueue", fmt.Sprintf(`{"QueueUrl":%q}`, orders.Body.QueueURL))
	if deleted.StatusCode != http.StatusOK {
		t.Fatalf("DeleteQueue = %#v", deleted)
	}
	recreated := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if recreated.StatusCode != http.StatusOK || recreated.Body.QueueURL == orders.Body.QueueURL {
		t.Fatalf("recreated = %#v", recreated)
	}
	stale := mustPostAction(t, server, "/v1/sqs/SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"stale"}`, orders.Body.QueueURL))
	requireError(t, stale, http.StatusNotFound, "QueueDoesNotExist")
	legacy := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"stale"}`)
	requireError(t, legacy, http.StatusNotFound, "QueueDoesNotExist")
}

func TestAdministrationStrictJSONAndStableErrors(t *testing.T) {
	server, _ := newTestServer(t)
	for _, test := range []struct{ path, body, code string }{
		{"/v1/sqs/ListQueues", `{"MaxResults":null}`, "InvalidRequest"},
		{"/v1/sqs/ListQueues", `{"Unknown":1}`, "InvalidRequest"},
		{"/v1/sqs/ListQueues", `{"MaxResults":1,"MaxResults":2}`, "InvalidRequest"},
		{"/v1/sqs/DeleteQueue", `{}`, "InvalidRequest"},
		{"/v1/sqs/TagQueue", `{"QueueUrl":"bad","Tags":{}}`, "InvalidRequest"},
	} {
		response := mustPostAction(t, server, test.path, test.body)
		requireError(t, response, http.StatusBadRequest, test.code)
	}
	invalidToken := mustPostAction(t, server, "/v1/sqs/ListQueues", `{"MaxResults":1,"NextToken":"not-a-token"}`)
	requireError(t, invalidToken, http.StatusBadRequest, "InvalidPaginationToken")
}
