package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simq/internal/api"
	"simq/internal/queue"
)

const (
	publicBaseURL  = "http://localhost:9324"
	fixedRequestID = "request-id"
)

type responseEnvelope struct {
	QueueURL               string                    `json:"QueueUrl"`
	QueueURLs              []string                  `json:"QueueUrls"`
	NextToken              string                    `json:"NextToken"`
	Tags                   map[string]string         `json:"Tags"`
	MessageID              string                    `json:"MessageId"`
	MD5OfMessageBody       string                    `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string                    `json:"MD5OfMessageAttributes"`
	SequenceNumber         string                    `json:"SequenceNumber"`
	Messages               []receivedMessageEnvelope `json:"Messages"`
	Attributes             map[string]string         `json:"Attributes"`
	RequestID              string                    `json:"RequestId"`
	Error                  *responseError            `json:"Error"`
}

type receivedMessageEnvelope struct {
	MessageID              string                              `json:"MessageId"`
	ReceiptHandle          string                              `json:"ReceiptHandle"`
	MD5OfBody              string                              `json:"MD5OfBody"`
	Body                   string                              `json:"Body"`
	Attributes             map[string]string                   `json:"Attributes"`
	MessageAttributes      map[string]messageAttributeEnvelope `json:"MessageAttributes"`
	MD5OfMessageAttributes string                              `json:"MD5OfMessageAttributes"`
	MessageGroupID         string                              `json:"MessageGroupId"`
	MessageDeduplicationID string                              `json:"MessageDeduplicationId"`
	SequenceNumber         string                              `json:"SequenceNumber"`
}

type messageAttributeEnvelope struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue"`
	BinaryValue string `json:"BinaryValue"`
}

type responseError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

type actionResponse struct {
	StatusCode int
	Header     http.Header
	Body       responseEnvelope
	RawBody    []byte
}

type localServer struct {
	URL     string
	handler http.Handler
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func (s *localServer) Client() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := httptest.NewRecorder()
		s.handler.ServeHTTP(response, request)
		return response.Result(), nil
	})}
}

func newTestServer(t *testing.T) (*localServer, *queue.MemoryRepository) {
	t.Helper()
	return newTestServerWithLimit(t, 2<<20)
}

func newTestServerWithLimit(t *testing.T, maxBodyBytes int64) (*localServer, *queue.MemoryRepository) {
	t.Helper()
	return newTestServerWithOptions(t, maxBodyBytes)
}

func newTestServerWithOptions(t *testing.T, maxBodyBytes int64, options ...queue.ServiceOption) (*localServer, *queue.MemoryRepository) {
	t.Helper()
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository, options...)
	server := api.NewServer(api.Config{
		PublicBaseURL: publicBaseURL,
		MaxBodyBytes:  maxBodyBytes,
		RequestIDGenerator: func() string {
			return fixedRequestID
		},
	}, service)
	return &localServer{URL: "http://simq.test", handler: server}, repository
}

func postAction(client *http.Client, endpoint, host, body string) (actionResponse, error) {
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return actionResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if host != "" {
		request.Host = host
	}

	response, err := client.Do(request)
	if err != nil {
		return actionResponse{}, err
	}
	defer response.Body.Close()

	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return actionResponse{}, fmt.Errorf("Content-Type = %q, want application/json", response.Header.Get("Content-Type"))
	}
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return actionResponse{}, err
	}

	var envelope responseEnvelope
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	if err := decoder.Decode(&envelope); err != nil {
		return actionResponse{}, fmt.Errorf("decode response: %w (body %q)", err, responseBody)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return actionResponse{}, fmt.Errorf("response contains trailing JSON: %v", err)
	}
	if envelope.RequestID == "" {
		return actionResponse{}, fmt.Errorf("response body has empty RequestId")
	}
	if got := response.Header.Get("X-SimQ-Request-Id"); got != envelope.RequestID {
		return actionResponse{}, fmt.Errorf("request ID header = %q, body = %q", got, envelope.RequestID)
	}

	return actionResponse{StatusCode: response.StatusCode, Header: response.Header, Body: envelope, RawBody: responseBody}, nil
}

func mustPostAction(t *testing.T, server *localServer, path, body string) actionResponse {
	t.Helper()
	response, err := postAction(server.Client(), server.URL+path, "", body)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func requireError(t *testing.T, response actionResponse, status int, code string) {
	t.Helper()
	if response.StatusCode != status {
		t.Fatalf("status = %d, want %d", response.StatusCode, status)
	}
	if response.Body.Error == nil {
		t.Fatal("response has no Error envelope")
	}
	if response.Body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", response.Body.Error.Code, code)
	}
	if response.Body.Error.Message == "" {
		t.Fatal("error message is empty")
	}
}

func TestCreateQueueUsesDefaultVisibilityTimeout(t *testing.T) {
	server, repository := newTestServer(t)

	response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if !strings.HasPrefix(response.Body.QueueURL, publicBaseURL+"/queues/orders/q_") {
		t.Fatalf("QueueUrl = %q", response.Body.QueueURL)
	}
	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("orders queue was not stored")
	}
	if stored.VisibilityTimeout != 30 {
		t.Fatalf("VisibilityTimeout = %d, want 30", stored.VisibilityTimeout)
	}
}

func TestCreateQueueUsesExplicitVisibilityTimeout(t *testing.T) {
	server, repository := newTestServer(t)

	response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"45"}}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || stored.VisibilityTimeout != 45 {
		t.Fatalf("stored queue = %#v, found = %v; want VisibilityTimeout 45", stored, ok)
	}
}

func TestCreateQueueIsIdempotent(t *testing.T) {
	server, repository := newTestServer(t)
	body := `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"30"}}`

	first := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
	second := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
	if first.StatusCode != http.StatusOK || second.StatusCode != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want 200, 200", first.StatusCode, second.StatusCode)
	}
	if first.Body.QueueURL != second.Body.QueueURL {
		t.Fatalf("QueueUrls differ: %q and %q", first.Body.QueueURL, second.Body.QueueURL)
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
}

func TestCreateQueueNamesAndURLsAreCaseSensitive(t *testing.T) {
	server, repository := newTestServer(t)
	lower := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	upper := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"Orders"}`)

	if lower.StatusCode != http.StatusOK || upper.StatusCode != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want 200, 200", lower.StatusCode, upper.StatusCode)
	}
	if lower.Body.QueueURL == upper.Body.QueueURL {
		t.Fatalf("case-distinct queues returned the same QueueUrl %q", lower.Body.QueueURL)
	}
	if repository.Count() != 2 {
		t.Fatalf("repository count = %d, want 2", repository.Count())
	}
}

func TestCreateQueueConflictingAttributes(t *testing.T) {
	server, repository := newTestServer(t)
	mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"30"}}`)

	response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"31"}}`)
	requireError(t, response, http.StatusConflict, "QueueAlreadyExists")
	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || stored.VisibilityTimeout != 30 {
		t.Fatalf("stored queue changed: %#v, found = %v", stored, ok)
	}
}

func TestCreateQueueRejectsInvalidNames(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{}`},
		{name: "empty", body: `{"QueueName":""}`},
		{name: "over 80", body: `{"QueueName":"` + strings.Repeat("a", 81) + `"}`},
		{name: "invalid character", body: `{"QueueName":"order.items"}`},
		{name: "fifo", body: `{"QueueName":"orders.fifo"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.Count() != 0 {
				t.Fatalf("repository count = %d after invalid request", repository.Count())
			}
		})
	}
}

func TestCreateQueueVisibilityTimeoutBoundaries(t *testing.T) {
	for _, value := range []string{"0", "43200"} {
		t.Run(value, func(t *testing.T) {
			server, repository := newTestServer(t)
			body := fmt.Sprintf(`{"QueueName":"orders","Attributes":{"VisibilityTimeout":%q}}`, value)
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.StatusCode)
			}
			stored, ok, err := repository.Get("orders")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !ok {
				t.Fatal("queue not stored")
			}
			want := 0
			if value == "43200" {
				want = 43200
			}
			if stored.VisibilityTimeout != want {
				t.Fatalf("VisibilityTimeout = %d, want %d", stored.VisibilityTimeout, want)
			}
		})
	}
}

func TestCreateQueueRejectsInvalidVisibilityTimeouts(t *testing.T) {
	for _, value := range []string{"-1", "43201", "not-a-number"} {
		t.Run(value, func(t *testing.T) {
			server, repository := newTestServer(t)
			body := fmt.Sprintf(`{"QueueName":"orders","Attributes":{"VisibilityTimeout":%q}}`, value)
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.Count() != 0 {
				t.Fatalf("repository count = %d after invalid request", repository.Count())
			}
		})
	}
}

func TestCreateQueueAcceptsLifecycleAttributes(t *testing.T) {
	server, repository := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{
		"QueueName":"orders",
		"Attributes":{"VisibilityTimeout":"10","DelaySeconds":"20","MessageRetentionPeriod":"60"}
	}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue = %#v", response)
	}
	stored, found, err := repository.Get("orders")
	if err != nil || !found || stored.VisibilityTimeout != 10 || stored.DelaySeconds != 20 || stored.MessageRetentionPeriod != 60 {
		t.Fatalf("stored queue = %#v, %v, %v", stored, found, err)
	}
}

func TestCreateQueueRejectsUnknownFieldsAndAttributes(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"QueueName":"orders","Typo":true}`},
		{name: "case-mismatched field", body: `{"queueName":"orders"}`},
		{name: "unknown attribute", body: `{"QueueName":"orders","Attributes":{"Unknown":"1"}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.Count() != 0 {
				t.Fatalf("repository count = %d after invalid request", repository.Count())
			}
		})
	}
}

func TestCreateQueueRejectsInvalidJSONWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ``},
		{name: "malformed", body: `{"QueueName":`},
		{name: "trailing JSON", body: `{"QueueName":"orders"} {}`},
		{name: "wrong QueueName type", body: `{"QueueName":123}`},
		{name: "wrong Attributes type", body: `{"QueueName":"orders","Attributes":[]}`},
		{name: "null Attributes", body: `{"QueueName":"orders","Attributes":null}`},
		{name: "wrong attribute type", body: `{"QueueName":"orders","Attributes":{"VisibilityTimeout":30}}`},
		{name: "duplicate field", body: `{"QueueName":"orders","QueueName":"other"}`},
		{name: "duplicate attribute", body: `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"30","VisibilityTimeout":"31"}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.Count() != 0 {
				t.Fatalf("repository count = %d after invalid request", repository.Count())
			}
		})
	}
}

func TestCreateQueueRejectsOversizedBodyWithoutSideEffects(t *testing.T) {
	server, repository := newTestServerWithLimit(t, 64)
	body := `{"QueueName":"orders","Padding":"` + strings.Repeat("x", 128) + `"}`
	response := mustPostAction(t, server, "/v1/sqs/CreateQueue", body)
	requireError(t, response, http.StatusRequestEntityTooLarge, "RequestTooLarge")
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after oversized request", repository.Count())
	}
}

func TestCreateQueueUsesConfiguredPublicBaseURLNotHost(t *testing.T) {
	server, _ := newTestServer(t)

	response, err := postAction(server.Client(), server.URL+"/v1/sqs/CreateQueue", "attacker.example", `{"QueueName":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if !strings.HasPrefix(response.Body.QueueURL, publicBaseURL+"/queues/orders/q_") {
		t.Fatalf("QueueUrl = %q, want configured base URL", response.Body.QueueURL)
	}
}

func TestRequestIDMatchesHeaderAndBodyOnSuccessAndError(t *testing.T) {
	server, _ := newTestServer(t)

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "success", body: `{"QueueName":"orders"}`},
		{name: "error", body: `{"QueueName":"invalid.name"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := mustPostAction(t, server, "/v1/sqs/CreateQueue", test.body)
			if response.Body.RequestID != fixedRequestID {
				t.Fatalf("RequestId = %q, want %q", response.Body.RequestID, fixedRequestID)
			}
			if response.Header.Get("X-SimQ-Request-Id") != fixedRequestID {
				t.Fatalf("request ID header = %q, want %q", response.Header.Get("X-SimQ-Request-Id"), fixedRequestID)
			}
		})
	}
}

func TestUnknownActionReturns404(t *testing.T) {
	server, _ := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/NoSuchAction", `{}`)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.StatusCode)
	}
	if response.Body.Error == nil || response.Body.Error.Code == "" {
		t.Fatal("404 response does not use the error envelope")
	}
}

func TestActionNamesAreCaseSensitive(t *testing.T) {
	server, repository := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/createqueue", `{"QueueName":"orders"}`)
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.StatusCode)
	}
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after case-mismatched action", repository.Count())
	}
}

func TestImplementedM3ActionUsesRequestValidation(t *testing.T) {
	server, _ := newTestServer(t)
	response := mustPostAction(t, server, "/v1/sqs/ListDeadLetterSourceQueues", `{}`)
	requireError(t, response, http.StatusBadRequest, "InvalidRequest")
}

func TestConcurrentIdenticalCreateQueueRequestsAreIdempotent(t *testing.T) {
	server, repository := newTestServer(t)
	const requests = 20
	results := make(chan error, requests)
	var wait sync.WaitGroup
	wait.Add(requests)

	for i := 0; i < requests; i++ {
		go func() {
			defer wait.Done()
			response, err := postAction(server.Client(), server.URL+"/v1/sqs/CreateQueue", "", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"30"}}`)
			if err == nil && response.StatusCode != http.StatusOK {
				err = fmt.Errorf("status = %d, want 200", response.StatusCode)
			}
			if err == nil && !strings.HasPrefix(response.Body.QueueURL, publicBaseURL+"/queues/orders/q_") {
				err = fmt.Errorf("QueueUrl = %q", response.Body.QueueURL)
			}
			results <- err
		}()
	}
	wait.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
}

func TestConcurrentConflictingCreateQueueRequestsLeaveOneQueue(t *testing.T) {
	server, repository := newTestServer(t)
	const requests = 20
	statuses := make(chan int, requests)
	errors := make(chan error, requests)
	var wait sync.WaitGroup
	wait.Add(requests)

	for i := 0; i < requests; i++ {
		i := i
		go func() {
			defer wait.Done()
			visibility := "30"
			if i%2 == 1 {
				visibility = "31"
			}
			body := fmt.Sprintf(`{"QueueName":"orders","Attributes":{"VisibilityTimeout":%q}}`, visibility)
			response, err := postAction(server.Client(), server.URL+"/v1/sqs/CreateQueue", "", body)
			if err != nil {
				errors <- err
				return
			}
			if response.StatusCode == http.StatusConflict && (response.Body.Error == nil || response.Body.Error.Code != "QueueAlreadyExists") {
				errors <- fmt.Errorf("conflict response = %#v", response.Body)
				return
			}
			statuses <- response.StatusCode
		}()
	}
	wait.Wait()
	close(statuses)
	close(errors)

	for err := range errors {
		t.Error(err)
	}
	successes, conflicts := 0, 0
	for status := range statuses {
		switch status {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			conflicts++
		default:
			t.Errorf("unexpected status %d", status)
		}
	}
	if successes != 10 || conflicts != 10 {
		t.Fatalf("successes = %d, conflicts = %d; want 10 and 10", successes, conflicts)
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || (stored.VisibilityTimeout != 30 && stored.VisibilityTimeout != 31) {
		t.Fatalf("stored queue = %#v, found = %v", stored, ok)
	}
}

func TestActionRequiresJSONContentType(t *testing.T) {
	server, repository := newTestServer(t)
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/sqs/CreateQueue", strings.NewReader(`{"QueueName":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "text/plain")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after wrong content type", repository.Count())
	}
}

func TestHealthAndReadyEndpoints(t *testing.T) {
	server, _ := newTestServer(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			response, err := server.Client().Get(server.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.StatusCode)
			}
			mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
			if err != nil || mediaType != "application/json" {
				t.Fatalf("Content-Type = %q", response.Header.Get("Content-Type"))
			}
			var body struct {
				Status string `json:"Status"`
			}
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Status != "ok" {
				t.Fatalf("Status = %q, want ok", body.Status)
			}
		})
	}
}

func TestGetQueueURLReturnsCreateQueueURL(t *testing.T) {
	server, _ := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	got := mustPostAction(t, server, "/v1/sqs/GetQueueUrl", `{"QueueName":"orders"}`)
	if got.StatusCode != http.StatusOK {
		t.Fatalf("GetQueueUrl status = %d, want 200", got.StatusCode)
	}
	if got.Body.QueueURL != created.Body.QueueURL {
		t.Fatalf("GetQueueUrl QueueUrl = %q, CreateQueue QueueUrl = %q", got.Body.QueueURL, created.Body.QueueURL)
	}
}

func TestGetQueueURLReturnsQueueDoesNotExist(t *testing.T) {
	server, repository := newTestServer(t)

	response := mustPostAction(t, server, "/v1/sqs/GetQueueUrl", `{"QueueName":"missing"}`)
	requireError(t, response, http.StatusNotFound, "QueueDoesNotExist")
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after missing queue lookup, want 0", repository.Count())
	}
}

func TestGetQueueURLIsCaseSensitive(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	response := mustPostAction(t, server, "/v1/sqs/GetQueueUrl", `{"QueueName":"Orders"}`)
	requireError(t, response, http.StatusNotFound, "QueueDoesNotExist")
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d after case-mismatched lookup, want 1", repository.Count())
	}
}

func TestGetQueueURLRejectsInvalidRequestsWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ``},
		{name: "missing QueueName", body: `{}`},
		{name: "empty QueueName", body: `{"QueueName":""}`},
		{name: "wrong QueueName type", body: `{"QueueName":123}`},
		{name: "invalid QueueName", body: `{"QueueName":"orders.invalid"}`},
		{name: "unknown field", body: `{"QueueName":"orders","Unknown":true}`},
		{name: "malformed", body: `{"QueueName":`},
		{name: "trailing JSON", body: `{"QueueName":"orders"} {}`},
		{name: "duplicate QueueName", body: `{"QueueName":"orders","QueueName":"other"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, repository := newTestServer(t)
			created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"existing"}`)
			if created.StatusCode != http.StatusOK {
				t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
			}

			response := mustPostAction(t, server, "/v1/sqs/GetQueueUrl", test.body)
			requireError(t, response, http.StatusBadRequest, "InvalidRequest")
			if repository.Count() != 1 {
				t.Fatalf("repository count = %d after invalid lookup, want 1", repository.Count())
			}
		})
	}
}

func TestGetQueueURLUsesConfiguredPublicBaseURLNotHost(t *testing.T) {
	server, _ := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	response, err := postAction(server.Client(), server.URL+"/v1/sqs/GetQueueUrl", "attacker.example", `{"QueueName":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GetQueueUrl status = %d, want 200", response.StatusCode)
	}
	if !strings.HasPrefix(response.Body.QueueURL, publicBaseURL+"/queues/orders/q_") {
		t.Fatalf("QueueUrl = %q, want configured public base URL", response.Body.QueueURL)
	}
}

func TestGetQueueURLRequestIDMatchesOnSuccessAndError(t *testing.T) {
	server, _ := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "success", body: `{"QueueName":"orders"}`},
		{name: "error", body: `{"QueueName":"missing"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := mustPostAction(t, server, "/v1/sqs/GetQueueUrl", test.body)
			if response.Body.RequestID != fixedRequestID {
				t.Fatalf("RequestId = %q, want %q", response.Body.RequestID, fixedRequestID)
			}
			if response.Header.Get("X-SimQ-Request-Id") != fixedRequestID {
				t.Fatalf("request ID header = %q, want %q", response.Header.Get("X-SimQ-Request-Id"), fixedRequestID)
			}
		})
	}
}

func TestConcurrentGetQueueURLRequestsReturnSameURL(t *testing.T) {
	server, repository := newTestServer(t)
	created := mustPostAction(t, server, "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("CreateQueue status = %d, want 200", created.StatusCode)
	}

	const requests = 20
	results := make(chan error, requests)
	var wait sync.WaitGroup
	wait.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wait.Done()
			response, err := postAction(server.Client(), server.URL+"/v1/sqs/GetQueueUrl", "", `{"QueueName":"orders"}`)
			if err == nil && response.StatusCode != http.StatusOK {
				err = fmt.Errorf("status = %d, want 200", response.StatusCode)
			}
			if err == nil && !strings.HasPrefix(response.Body.QueueURL, publicBaseURL+"/queues/orders/q_") {
				err = fmt.Errorf("QueueUrl = %q", response.Body.QueueURL)
			}
			results <- err
		}()
	}
	wait.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d after concurrent lookups, want 1", repository.Count())
	}
}
