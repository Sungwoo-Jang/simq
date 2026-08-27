//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	defaultAdminToken = "local-integration-admin"
	clusterWait       = 60 * time.Second
)

type testConfig struct {
	URLs        []string
	ComposeFile string
	Project     string
	AdminToken  string
}

type actionResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

type queueURLResponse struct {
	QueueURL string `json:"QueueUrl"`
}

type sendResponse struct {
	MessageID      string `json:"MessageId"`
	SequenceNumber string `json:"SequenceNumber"`
}

type receiveResponse struct {
	Messages []struct {
		MessageID      string `json:"MessageId"`
		ReceiptHandle  string `json:"ReceiptHandle"`
		Body           string `json:"Body"`
		SequenceNumber string `json:"SequenceNumber"`
	} `json:"Messages"`
}

type membersResponse struct {
	Members []struct {
		ID string `json:"ID"`
	} `json:"Members"`
}

func TestThreeNodeDockerClusterSurvivesLeaderKillAndRestart(t *testing.T) {
	config := loadConfig(t)
	client := &http.Client{Timeout: 3 * time.Second}
	waitForHealth(t, client, config.URLs)

	queueName := fmt.Sprintf("integration-%d.fifo", time.Now().UnixNano())
	createBody := fmt.Sprintf(`{"QueueName":%q,"Attributes":{"FifoQueue":"true"}}`, queueName)
	leader, created := waitForLeaderAction(t, client, config.URLs, "/v1/sqs/CreateQueue", createBody, "integration-create", "")
	var queue queueURLResponse
	decodeJSON(t, created.Body, &queue)
	if queue.QueueURL == "" {
		t.Fatalf("CreateQueue omitted QueueUrl: %s", created.Body)
	}
	assertFollowerHints(t, client, config.URLs, leader, queueName)

	firstBody := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"first","MessageGroupId":"group","MessageDeduplicationId":"first"}`, queue.QueueURL)
	first := mustAction(t, client, leader, "/v1/sqs/SendMessage", firstBody, "integration-send-first", "")
	var firstSent sendResponse
	decodeJSON(t, first.Body, &firstSent)
	if firstSent.MessageID == "" || firstSent.SequenceNumber != "1" {
		t.Fatalf("first send response=%s", first.Body)
	}
	secondBody := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"second","MessageGroupId":"group","MessageDeduplicationId":"second"}`, queue.QueueURL)
	second := mustAction(t, client, leader, "/v1/sqs/SendMessage", secondBody, "integration-send-second", "")
	var secondSent sendResponse
	decodeJSON(t, second.Body, &secondSent)
	if secondSent.MessageID == "" || secondSent.SequenceNumber != "2" {
		t.Fatalf("second send response=%s", second.Body)
	}

	firstLeaderService := serviceForURL(t, leader)
	compose(t, config, "kill", "-s", "KILL", firstLeaderService)
	t.Cleanup(func() { _ = composeCommand(config, "start", "n1", "n2", "n3").Run() })
	remaining := withoutURL(config.URLs, leader)
	newLeader, _ := waitForLeaderAction(t, client, remaining, "/v1/sqs/GetQueueUrl", fmt.Sprintf(`{"QueueName":%q}`, queueName), "", "")
	if newLeader == leader {
		t.Fatal("leader did not change after its container stopped")
	}

	replayed := mustAction(t, client, newLeader, "/v1/sqs/SendMessage", firstBody, "integration-send-first", "")
	var replayedSend sendResponse
	decodeJSON(t, replayed.Body, &replayedSend)
	if replayedSend.MessageID != firstSent.MessageID || replayedSend.SequenceNumber != firstSent.SequenceNumber {
		t.Fatalf("operation replay changed acknowledgement: first=%+v replay=%+v", firstSent, replayedSend)
	}

	firstReceived := receiveOne(t, client, newLeader, queue.QueueURL, "integration-receive-first", "attempt-first")
	if firstReceived.MessageID != firstSent.MessageID || firstReceived.Body != "first" || firstReceived.SequenceNumber != "1" {
		t.Fatalf("first received=%+v", firstReceived)
	}
	deleteMessage(t, client, newLeader, queue.QueueURL, firstReceived.ReceiptHandle, "integration-delete-first")
	secondReceived := receiveOne(t, client, newLeader, queue.QueueURL, "integration-receive-second", "attempt-second")
	if secondReceived.MessageID != secondSent.MessageID || secondReceived.Body != "second" || secondReceived.SequenceNumber != "2" {
		t.Fatalf("second received=%+v", secondReceived)
	}
	deleteMessage(t, client, newLeader, queue.QueueURL, secondReceived.ReceiptHandle, "integration-delete-second")

	mustAction(t, client, newLeader, "/v1/cluster/snapshot", `{}`, "", config.AdminToken)
	compose(t, config, "start", firstLeaderService)
	waitForHealth(t, client, []string{leader})
	waitForMembers(t, client, newLeader, config.AdminToken, 3)

	secondLeaderService := serviceForURL(t, newLeader)
	compose(t, config, "kill", "-s", "KILL", secondLeaderService)
	secondRemaining := withoutURL(config.URLs, newLeader)
	finalLeader, final := waitForLeaderAction(t, client, secondRemaining, "/v1/sqs/GetQueueUrl", fmt.Sprintf(`{"QueueName":%q}`, queueName), "", "")
	if finalLeader == newLeader {
		t.Fatal("second leader did not change after its container stopped")
	}
	var finalQueue queueURLResponse
	decodeJSON(t, final.Body, &finalQueue)
	if finalQueue.QueueURL != queue.QueueURL {
		t.Fatalf("queue URL changed across failovers: before=%q after=%q", queue.QueueURL, finalQueue.QueueURL)
	}
	compose(t, config, "start", secondLeaderService)
	waitForHealth(t, client, config.URLs)
}

func loadConfig(t *testing.T) testConfig {
	t.Helper()
	composeFile := os.Getenv("SIMQ_INTEGRATION_COMPOSE_FILE")
	if composeFile == "" {
		t.Fatal("SIMQ_INTEGRATION_COMPOSE_FILE is required; use scripts/integration.ps1 or scripts/integration.sh")
	}
	urls := []string{"http://127.0.0.1:19324", "http://127.0.0.1:19325", "http://127.0.0.1:19326"}
	if raw := os.Getenv("SIMQ_INTEGRATION_URLS"); raw != "" {
		urls = strings.Split(raw, ",")
	}
	if len(urls) != 3 {
		t.Fatalf("SIMQ_INTEGRATION_URLS must contain exactly three comma-separated URLs")
	}
	project := os.Getenv("SIMQ_INTEGRATION_PROJECT")
	if project == "" {
		project = "simq-integration"
	}
	adminToken := os.Getenv("SIMQ_INTEGRATION_ADMIN_TOKEN")
	if adminToken == "" {
		adminToken = defaultAdminToken
	}
	return testConfig{URLs: urls, ComposeFile: composeFile, Project: project, AdminToken: adminToken}
}

func waitForHealth(t *testing.T, client *http.Client, urls []string) {
	t.Helper()
	deadline := time.Now().Add(clusterWait)
	pending := append([]string(nil), urls...)
	for len(pending) > 0 && time.Now().Before(deadline) {
		next := pending[:0]
		for _, base := range pending {
			response, err := client.Get(base + "/healthz")
			if err != nil {
				next = append(next, base)
				continue
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				next = append(next, base)
			}
		}
		pending = next
		if len(pending) > 0 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if len(pending) > 0 {
		t.Fatalf("nodes did not become healthy: %v", pending)
	}
}

func waitForLeaderAction(t *testing.T, client *http.Client, urls []string, path, body, operationID, adminToken string) (string, actionResponse) {
	t.Helper()
	deadline := time.Now().Add(clusterWait)
	var last string
	for time.Now().Before(deadline) {
		for _, base := range urls {
			response, err := action(client, base, path, body, operationID, adminToken)
			if err != nil {
				last = err.Error()
				continue
			}
			if response.Status == http.StatusOK {
				return base, response
			}
			last = fmt.Sprintf("%s returned %d: %s", base, response.Status, response.Body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no leader completed %s: %s", path, last)
	return "", actionResponse{}
}

func mustAction(t *testing.T, client *http.Client, base, path, body, operationID, adminToken string) actionResponse {
	t.Helper()
	response, err := action(client, base, path, body, operationID, adminToken)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("%s%s returned %d: %s", base, path, response.Status, response.Body)
	}
	return response
}

func action(client *http.Client, base, path, body, operationID, adminToken string) (actionResponse, error) {
	request, err := http.NewRequest(http.MethodPost, base+path, bytes.NewBufferString(body))
	if err != nil {
		return actionResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if operationID != "" {
		request.Header.Set("X-SimQ-Operation-Id", operationID)
	}
	if adminToken != "" {
		request.Header.Set("Authorization", "Bearer "+adminToken)
	}
	response, err := client.Do(request)
	if err != nil {
		return actionResponse{}, err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return actionResponse{}, err
	}
	return actionResponse{Status: response.StatusCode, Header: response.Header.Clone(), Body: encoded}, nil
}

func assertFollowerHints(t *testing.T, client *http.Client, urls []string, leader, queueName string) {
	t.Helper()
	wantLeader := strings.Replace(leader, "127.0.0.1", "localhost", 1)
	for _, base := range urls {
		if base == leader {
			continue
		}
		response, err := action(client, base, "/v1/sqs/GetQueueUrl", fmt.Sprintf(`{"QueueName":%q}`, queueName), "", "")
		if err != nil {
			t.Fatal(err)
		}
		if response.Status != http.StatusServiceUnavailable || response.Header.Get("X-SimQ-Leader") != wantLeader {
			t.Fatalf("follower %s status=%d leader=%q want=%q body=%s", base, response.Status, response.Header.Get("X-SimQ-Leader"), wantLeader, response.Body)
		}
	}
}

func receiveOne(t *testing.T, client *http.Client, leader, queueURL, operationID, attemptID string) struct {
	MessageID      string `json:"MessageId"`
	ReceiptHandle  string `json:"ReceiptHandle"`
	Body           string `json:"Body"`
	SequenceNumber string `json:"SequenceNumber"`
} {
	t.Helper()
	body := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"ReceiveRequestAttemptId":%q}`, queueURL, attemptID)
	response := mustAction(t, client, leader, "/v1/sqs/ReceiveMessage", body, operationID, "")
	var decoded receiveResponse
	decodeJSON(t, response.Body, &decoded)
	if len(decoded.Messages) != 1 {
		t.Fatalf("ReceiveMessage returned %d messages: %s", len(decoded.Messages), response.Body)
	}
	return decoded.Messages[0]
}

func deleteMessage(t *testing.T, client *http.Client, leader, queueURL, receipt, operationID string) {
	t.Helper()
	body := fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, queueURL, receipt)
	mustAction(t, client, leader, "/v1/sqs/DeleteMessage", body, operationID, "")
}

func waitForMembers(t *testing.T, client *http.Client, leader, token string, count int) {
	t.Helper()
	deadline := time.Now().Add(clusterWait)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequest(http.MethodGet, leader+"/v1/cluster/members", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(request)
		if err == nil {
			encoded, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
			var decoded membersResponse
			if response.StatusCode == http.StatusOK && json.Unmarshal(encoded, &decoded) == nil && len(decoded.Members) == count {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster did not report %d members", count)
}

func decodeJSON(t *testing.T, encoded []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(encoded, destination); err != nil {
		t.Fatalf("decode response %s: %v", encoded, err)
	}
}

func serviceForURL(t *testing.T, base string) string {
	t.Helper()
	services := map[string]string{
		"http://127.0.0.1:19324": "n1",
		"http://127.0.0.1:19325": "n2",
		"http://127.0.0.1:19326": "n3",
	}
	service, ok := services[base]
	if !ok {
		t.Fatalf("no compose service mapping for %q", base)
	}
	return service
}

func withoutURL(values []string, excluded string) []string {
	result := make([]string, 0, len(values)-1)
	for _, value := range values {
		if value != excluded {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func compose(t *testing.T, config testConfig, arguments ...string) {
	t.Helper()
	command := composeCommand(config, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}

func composeCommand(config testConfig, arguments ...string) *exec.Cmd {
	base := []string{"compose", "-p", config.Project, "-f", config.ComposeFile}
	return exec.Command("docker", append(base, arguments...)...)
}
