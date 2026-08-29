package sqssemantics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSimQBackendFollowsLeaderHintWithoutChangingOperationID(t *testing.T) {
	var followerOperation string
	var leaderCalls atomic.Int32
	leader := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		leaderCalls.Add(1)
		if got := request.Header.Get("X-SimQ-Operation-Id"); got == "" || got != followerOperation {
			t.Errorf("leader operation ID = %q, follower = %q", got, followerOperation)
		}
		_ = json.NewEncoder(response).Encode(map[string]string{"MessageId": "message"})
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		followerOperation = request.Header.Get("X-SimQ-Operation-Id")
		response.Header().Set("X-SimQ-Leader", leader.URL)
		response.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(response).Encode(map[string]any{"Error": map[string]string{"Code": "NotLeader"}})
	}))
	defer follower.Close()

	backend, err := NewSimQBackend(SimQOptions{Endpoints: []string{follower.URL, leader.URL}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := backend.Send(context.Background(), Queue{URL: "http://queue.invalid/queues/q/id"}, SendInput{Body: "body"})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "message" || leaderCalls.Load() != 1 {
		t.Fatalf("result=%+v leader calls=%d", result, leaderCalls.Load())
	}
}

func TestSimQBackendRejectsCredentialInEndpointShape(t *testing.T) {
	for _, endpoint := range []string{"not-an-endpoint", "http://user:password@localhost:9324"} {
		if _, err := NewSimQBackend(SimQOptions{Endpoints: []string{endpoint}}); err == nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
}
