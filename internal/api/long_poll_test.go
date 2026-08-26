package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
)

type apiWaitTimer struct {
	channel chan time.Time
	stopped atomic.Bool
}

func (t *apiWaitTimer) C() <-chan time.Time { return t.channel }
func (t *apiWaitTimer) Stop() bool          { return !t.stopped.Swap(true) }

type apiTimerRequest struct {
	duration time.Duration
	timer    *apiWaitTimer
}

type apiWaitTimerFactory struct {
	requests chan apiTimerRequest
}

func newAPIWaitTimerFactory() *apiWaitTimerFactory {
	return &apiWaitTimerFactory{requests: make(chan apiTimerRequest, 32)}
}

func (f *apiWaitTimerFactory) New(duration time.Duration) queue.WaitTimer {
	timer := &apiWaitTimer{channel: make(chan time.Time, 1)}
	f.requests <- apiTimerRequest{duration: duration, timer: timer}
	return timer
}

func (f *apiWaitTimerFactory) next(t *testing.T) apiTimerRequest {
	t.Helper()
	select {
	case value := <-f.requests:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll handler did not create timer")
		return apiTimerRequest{}
	}
}

func startAPIAction(server *localServer, ctx context.Context, path, body string) <-chan *httptest.ResponseRecorder {
	result := make(chan *httptest.ResponseRecorder, 1)
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	go func() {
		recorder := httptest.NewRecorder()
		server.handler.ServeHTTP(recorder, request)
		result <- recorder
	}()
	return result
}

func finishAPIAction(t *testing.T, result <-chan *httptest.ResponseRecorder) (*httptest.ResponseRecorder, responseEnvelope) {
	t.Helper()
	select {
	case recorder := <-result:
		var body responseEnvelope
		if recorder.Body.Len() > 0 {
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v; body=%q", err, recorder.Body.String())
			}
		}
		return recorder, body
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll handler did not finish")
		return nil, responseEnvelope{}
	}
}

func TestReceiveMessageHTTPLongPollTimeoutAndSendWake(t *testing.T) {
	clock := &apiManualClock{now: time.Date(2026, time.August, 23, 17, 0, 0, 0, time.UTC)}
	timers := newAPIWaitTimerFactory()
	server, repository := newTestServerWithOptions(t, 2<<20, queue.WithClock(clock), queue.WithWaitTimerFactory(timers.New))
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", created)
	}

	timed := startAPIAction(server, context.Background(), "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":20}`)
	timer := timers.next(t)
	if timer.duration != 20*time.Second {
		t.Fatalf("timeout duration = %v, want 20s", timer.duration)
	}
	clock.Advance(20 * time.Second)
	timer.timer.channel <- clock.Now()
	recorder, body := finishAPIAction(t, timed)
	if recorder.Code != http.StatusOK || len(body.Messages) != 0 || body.RequestID != fixedRequestID || recorder.Header().Get("X-SimQ-Request-Id") != fixedRequestID {
		t.Fatalf("timed response = code %d, body %#v, header %#v", recorder.Code, body, recorder.Header())
	}

	woken := startAPIAction(server, context.Background(), "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":20}`)
	_ = timers.next(t)
	sent := mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"wake-body"}`)
	if sent.StatusCode != http.StatusOK {
		t.Fatalf("SendMessage = %#v", sent)
	}
	recorder, body = finishAPIAction(t, woken)
	if recorder.Code != http.StatusOK || len(body.Messages) != 1 || body.Messages[0].Body != "wake-body" || repository.MessageCount("orders") != 1 {
		t.Fatalf("send wake = code %d, body %#v", recorder.Code, body)
	}
}

func TestReceiveMessageHTTPWaitTimeStrictValidationAndBoundaries(t *testing.T) {
	server, repository := newTestServer(t)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)

	for _, wait := range []int{1, 20} {
		mustPostAction(t, server, "/v1/sqs/SendMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","MessageBody":"boundary"}`)
		response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","VisibilityTimeout":0,"WaitTimeSeconds":`+jsonInteger(wait)+`}`)
		if response.StatusCode != http.StatusOK || len(response.Body.Messages) != 1 {
			t.Fatalf("WaitTimeSeconds %d response = %#v", wait, response)
		}
		mustPostAction(t, server, "/v1/sqs/DeleteMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","ReceiptHandle":"`+response.Body.Messages[0].ReceiptHandle+`"}`)
	}
	for _, body := range []string{
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":-1}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":21}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":1.5}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":"1"}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":null}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":0,"WaitTimeSeconds":1}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":0,"Unknown":1}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":0} {}`,
	} {
		response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", body)
		if response.StatusCode != http.StatusBadRequest || response.Body.Error == nil || response.Body.Error.Code != "InvalidRequest" {
			t.Fatalf("invalid wait body %q = %#v", body, response)
		}
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatalf("invalid waits mutated queue: %d messages", repository.MessageCount("orders"))
	}

	for _, body := range []string{
		`{"QueueUrl":"http://localhost:9324/queues/orders"}`,
		`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":0}`,
	} {
		response := mustPostAction(t, server, "/v1/sqs/ReceiveMessage", body)
		if response.StatusCode != http.StatusOK || len(response.Body.Messages) != 0 {
			t.Fatalf("short poll %q = %#v", body, response)
		}
	}
}

func TestReceiveMessageHTTPContextCancellationDoesNotMutate(t *testing.T) {
	clock := &apiManualClock{now: time.Date(2026, time.August, 23, 18, 0, 0, 0, time.UTC)}
	timers := newAPIWaitTimerFactory()
	server, repository := newTestServerWithOptions(t, 2<<20, queue.WithClock(clock), queue.WithWaitTimerFactory(timers.New))
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	ctx, cancel := context.WithCancel(context.Background())
	result := startAPIAction(server, ctx, "/v1/sqs/ReceiveMessage", `{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":20}`)
	_ = timers.next(t)
	cancel()
	recorder, _ := finishAPIAction(t, result)
	if recorder.Body.Len() != 0 || repository.MessageCount("orders") != 0 {
		t.Fatalf("cancelled response body=%q message count=%d", recorder.Body.String(), repository.MessageCount("orders"))
	}
}

func jsonInteger(value int) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
