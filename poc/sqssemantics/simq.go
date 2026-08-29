package sqssemantics

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SimQOptions struct {
	Endpoints []string
	Token     string
	Client    *http.Client
}

type SimQBackend struct {
	client    *http.Client
	token     string
	endpoints []string
	mu        sync.Mutex
	preferred int
}

func NewSimQBackend(options SimQOptions) (*SimQBackend, error) {
	if len(options.Endpoints) == 0 {
		return nil, errors.New("at least one SimQ endpoint is required")
	}
	endpoints := make([]string, 0, len(options.Endpoints))
	for _, raw := range options.Endpoints {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("invalid SimQ endpoint %q", raw)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("unsupported SimQ endpoint scheme %q", parsed.Scheme)
		}
		parsed.Path = strings.TrimRight(parsed.Path, "/")
		endpoints = append(endpoints, strings.TrimRight(parsed.String(), "/"))
	}
	client := options.Client
	if client == nil {
		client = &http.Client{Timeout: 25 * time.Second}
	}
	return &SimQBackend{client: client, token: options.Token, endpoints: endpoints}, nil
}

func (*SimQBackend) Name() string { return "simq" }

func (b *SimQBackend) CreateQueue(ctx context.Context, name string, options QueueOptions) (Queue, error) {
	attributes := map[string]string{"VisibilityTimeout": strconv.Itoa(seconds(options.VisibilityTimeout))}
	if options.FIFO {
		attributes["FifoQueue"] = "true"
	}
	if options.ContentBasedDeduplication {
		attributes["ContentBasedDeduplication"] = "true"
	}
	var response struct {
		QueueURL string `json:"QueueUrl"`
	}
	if err := b.action(ctx, "CreateQueue", map[string]any{"QueueName": name, "Attributes": attributes}, true, &response); err != nil {
		return Queue{}, err
	}
	queue := Queue{URL: response.QueueURL}
	if queue.URL == "" {
		return Queue{}, errors.New("SimQ create response omitted queue URL")
	}
	attributesResponse, err := b.queueAttributes(ctx, queue, []string{"QueueArn"})
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = b.DeleteQueue(cleanupCtx, queue)
		cancel()
		return Queue{}, fmt.Errorf("get created queue ARN: %w", err)
	}
	queue.ARN = attributesResponse["QueueArn"]
	if queue.ARN == "" {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = b.DeleteQueue(cleanupCtx, queue)
		cancel()
		return Queue{}, errors.New("SimQ create response omitted queue URL or ARN")
	}
	return queue, nil
}

func (b *SimQBackend) DeleteQueue(ctx context.Context, queue Queue) error {
	return b.action(ctx, "DeleteQueue", map[string]any{"QueueUrl": queue.URL}, true, nil)
}

func (b *SimQBackend) Send(ctx context.Context, queue Queue, input SendInput) (SendResult, error) {
	body := map[string]any{"QueueUrl": queue.URL, "MessageBody": input.Body}
	if input.GroupID != "" {
		body["MessageGroupId"] = input.GroupID
	}
	if input.DeduplicationID != "" {
		body["MessageDeduplicationId"] = input.DeduplicationID
	}
	var response struct {
		MessageID string `json:"MessageId"`
	}
	if err := b.action(ctx, "SendMessage", body, true, &response); err != nil {
		return SendResult{}, err
	}
	if response.MessageID == "" {
		return SendResult{}, errors.New("SimQ send response omitted message ID")
	}
	return SendResult{MessageID: response.MessageID}, nil
}

func (b *SimQBackend) Receive(ctx context.Context, queue Queue, input ReceiveInput) ([]Message, error) {
	body := map[string]any{
		"QueueUrl":            queue.URL,
		"MaxNumberOfMessages": input.MaxMessages,
		"WaitTimeSeconds":     seconds(input.WaitTime),
	}
	if input.VisibilityTimeout != 0 {
		body["VisibilityTimeout"] = seconds(input.VisibilityTimeout)
	}
	var response struct {
		Messages []struct {
			MessageID     string `json:"MessageId"`
			ReceiptHandle string `json:"ReceiptHandle"`
			Body          string `json:"Body"`
			GroupID       string `json:"MessageGroupId"`
			Attributes    struct {
				ReceiveCount string `json:"ApproximateReceiveCount"`
			} `json:"Attributes"`
		} `json:"Messages"`
	}
	if err := b.action(ctx, "ReceiveMessage", body, true, &response); err != nil {
		return nil, err
	}
	messages := make([]Message, len(response.Messages))
	for index, item := range response.Messages {
		count, err := strconv.Atoi(item.Attributes.ReceiveCount)
		if err != nil {
			return nil, errors.New("SimQ receive response contained invalid receive count")
		}
		messages[index] = Message{MessageID: item.MessageID, ReceiptHandle: item.ReceiptHandle, Body: item.Body, ReceiveCount: count, GroupID: item.GroupID}
	}
	return messages, nil
}

func (b *SimQBackend) Delete(ctx context.Context, queue Queue, receipt string) error {
	return b.action(ctx, "DeleteMessage", map[string]any{"QueueUrl": queue.URL, "ReceiptHandle": receipt}, true, nil)
}

func (b *SimQBackend) ChangeVisibility(ctx context.Context, queue Queue, receipt string, visibility time.Duration) error {
	return b.action(ctx, "ChangeMessageVisibility", map[string]any{
		"QueueUrl":          queue.URL,
		"ReceiptHandle":     receipt,
		"VisibilityTimeout": seconds(visibility),
	}, true, nil)
}

func (b *SimQBackend) ConfigureRedrive(ctx context.Context, source, target Queue, maxReceiveCount int) error {
	policy, err := json.Marshal(map[string]string{
		"deadLetterTargetArn": target.ARN,
		"maxReceiveCount":     strconv.Itoa(maxReceiveCount),
	})
	if err != nil {
		return err
	}
	return b.action(ctx, "SetQueueAttributes", map[string]any{
		"QueueUrl": source.URL,
		"Attributes": map[string]string{
			"RedrivePolicy": string(policy),
		},
	}, true, nil)
}

func (b *SimQBackend) queueAttributes(ctx context.Context, queue Queue, names []string) (map[string]string, error) {
	var response struct {
		Attributes map[string]string `json:"Attributes"`
	}
	err := b.action(ctx, "GetQueueAttributes", map[string]any{"QueueUrl": queue.URL, "AttributeNames": names}, false, &response)
	return response.Attributes, err
}

func (b *SimQBackend) action(ctx context.Context, action string, body any, mutating bool, destination any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode SimQ %s request: %w", action, err)
	}
	operationID := ""
	if mutating {
		operationID, err = randomOperationID()
		if err != nil {
			return err
		}
	}
	endpoints := b.orderedEndpoints()
	var lastErr error
	for attempt := 0; attempt < len(endpoints)*4; attempt++ {
		endpoint := endpoints[attempt%len(endpoints)]
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/sqs/"+action, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		if operationID != "" {
			request.Header.Set("X-SimQ-Operation-Id", operationID)
		}
		if b.token != "" {
			request.Header.Set("Authorization", "Bearer "+b.token)
		}
		response, err := b.client.Do(request)
		if err != nil {
			lastErr = fmt.Errorf("SimQ %s request failed", action)
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		_ = response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read SimQ %s response: %w", action, readErr)
		}
		if response.StatusCode == http.StatusServiceUnavailable {
			if hint := response.Header.Get("X-SimQ-Leader"); hint != "" {
				if index := b.matchEndpoint(hint); index >= 0 {
					b.setPreferred(index)
					endpoints = b.orderedEndpoints()
				}
			}
			lastErr = fmt.Errorf("SimQ %s had no available leader", action)
			if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if response.StatusCode != http.StatusOK {
			var failure struct {
				Error struct {
					Code string `json:"Code"`
				} `json:"Error"`
			}
			_ = json.Unmarshal(responseBody, &failure)
			if failure.Error.Code == "" {
				failure.Error.Code = "HTTP " + response.Status
			}
			return fmt.Errorf("SimQ %s failed: %s", action, failure.Error.Code)
		}
		b.setPreferred(b.endpointIndex(endpoint))
		if destination != nil {
			if err := json.Unmarshal(responseBody, destination); err != nil {
				return fmt.Errorf("decode SimQ %s response: %w", action, err)
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("SimQ %s failed", action)
	}
	return lastErr
}

func (b *SimQBackend) orderedEndpoints() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ordered := make([]string, 0, len(b.endpoints))
	for offset := 0; offset < len(b.endpoints); offset++ {
		ordered = append(ordered, b.endpoints[(b.preferred+offset)%len(b.endpoints)])
	}
	return ordered
}

func (b *SimQBackend) setPreferred(index int) {
	if index < 0 {
		return
	}
	b.mu.Lock()
	b.preferred = index
	b.mu.Unlock()
}

func (b *SimQBackend) endpointIndex(endpoint string) int {
	for index, candidate := range b.endpoints {
		if candidate == endpoint {
			return index
		}
	}
	return -1
}

func (b *SimQBackend) matchEndpoint(raw string) int {
	hint, err := url.Parse(raw)
	if err != nil {
		return -1
	}
	for index, rawCandidate := range b.endpoints {
		candidate, err := url.Parse(rawCandidate)
		if err == nil && candidate.Scheme == hint.Scheme && candidate.Port() == hint.Port() && sameEndpointHost(candidate.Hostname(), hint.Hostname()) {
			return index
		}
	}
	return -1
}

func sameEndpointHost(left, right string) bool {
	if strings.EqualFold(left, right) {
		return true
	}
	return left == "127.0.0.1" && strings.EqualFold(right, "localhost") ||
		right == "127.0.0.1" && strings.EqualFold(left, "localhost")
}

func randomOperationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate operation ID: %w", err)
	}
	return "poc-" + hex.EncodeToString(value), nil
}

func seconds(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}
	return int((duration + time.Second - 1) / time.Second)
}
