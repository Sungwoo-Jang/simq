//go:build secureintegration

package integration

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const secureWait = 90 * time.Second

type secureConfig struct {
	URLs, Tokens             []string
	FixtureDir, ComposeFile  string
	Project, RaftNetwork     string
	AdminToken, MetricsToken string
	Client                   *http.Client
}

type secureResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

type secureQueueResponse struct {
	QueueURL string `json:"QueueUrl"`
}

type secureReceiveResponse struct {
	Messages []struct {
		Body, ReceiptHandle string
		SequenceNumber      string `json:"SequenceNumber"`
	} `json:"Messages"`
}

func TestSecureMultiShardChaos(t *testing.T) {
	config := loadSecureConfig(t)
	waitSecureHealth(t, config, config.URLs)
	assertShardCatalog(t, config)

	queues := make([]secureQueueResponse, 2)
	for index, token := range config.Tokens {
		leader, created := waitSecureLeaderAction(t, config, config.URLs, token, "/v1/sqs/CreateQueue", `{"QueueName":"orders.fifo","Attributes":{"FifoQueue":"true"}}`, fmt.Sprintf("m10-create-%d", index))
		decodeSecureJSON(t, created.Body, &queues[index])
		if queues[index].QueueURL == "" {
			t.Fatalf("tenant %d CreateQueue omitted QueueUrl: %s", index, created.Body)
		}
		body := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":%q,"MessageGroupId":"group","MessageDeduplicationId":%q}`, queues[index].QueueURL, fmt.Sprintf("tenant-%d", index), fmt.Sprintf("tenant-%d", index))
		mustSecureAction(t, config, leader, token, "/v1/sqs/SendMessage", body, fmt.Sprintf("m10-send-tenant-%d", index))
	}

	s1Leader, _ := waitSecureLeaderAction(t, config, config.URLs, config.Tokens[1], "/v1/sqs/GetQueueUrl", `{"QueueName":"orders.fifo"}`, "")
	isolatedLeader, _ := waitSecureLeaderAction(t, config, config.URLs, config.Tokens[0], "/v1/sqs/GetQueueUrl", `{"QueueName":"orders.fifo"}`, "")
	if s1Leader == "" || isolatedLeader == "" {
		t.Fatal("both tenant shards must have leaders")
	}

	isolateService := secureServiceForURL(t, isolatedLeader)
	containerID := secureComposeOutput(t, config, "ps", "-q", isolateService)
	if containerID == "" {
		t.Fatalf("compose returned no container for %s", isolateService)
	}
	secureDocker(t, "network", "disconnect", "-f", config.RaftNetwork, containerID)
	reconnected := false
	t.Cleanup(func() {
		if !reconnected {
			_ = exec.Command("docker", "network", "connect", "--alias", isolateService+"-raft", config.RaftNetwork, containerID).Run()
		}
		_ = secureComposeCommand(config, "start", "n1", "n2", "n3").Run()
	})

	remaining := withoutSecureURL(config.URLs, isolatedLeader)
	majorityLeader, _ := waitSecureLeaderAction(t, config, remaining, config.Tokens[0], "/v1/sqs/GetQueueUrl", `{"QueueName":"orders.fifo"}`, "")
	waitSecureStatus(t, config, isolatedLeader, config.Tokens[0], "/v1/sqs/GetQueueUrl", `{"QueueName":"orders.fifo"}`, http.StatusServiceUnavailable)
	duringBody := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"during-partition","MessageGroupId":"group","MessageDeduplicationId":"during-partition"}`, queues[0].QueueURL)
	mustSecureAction(t, config, majorityLeader, config.Tokens[0], "/v1/sqs/SendMessage", duringBody, "m10-send-during-partition")

	secureDocker(t, "network", "connect", "--alias", isolateService+"-raft", config.RaftNetwork, containerID)
	reconnected = true
	waitForLeaderHint(t, config, isolatedLeader, config.Tokens[0])

	majorityService := secureServiceForURL(t, majorityLeader)
	secureCompose(t, config, "kill", "-s", "KILL", majorityService)
	remainingAfterKill := withoutSecureURL(config.URLs, majorityLeader)
	finalLeader, _ := waitSecureLeaderAction(t, config, remainingAfterKill, config.Tokens[0], "/v1/sqs/GetQueueUrl", `{"QueueName":"orders.fifo"}`, "")

	for index, want := range []string{"tenant-0", "during-partition"} {
		body := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"ReceiveRequestAttemptId":%q}`, queues[0].QueueURL, fmt.Sprintf("attempt-%d", index))
		response := mustSecureAction(t, config, finalLeader, config.Tokens[0], "/v1/sqs/ReceiveMessage", body, fmt.Sprintf("m10-receive-%d", index))
		var received secureReceiveResponse
		decodeSecureJSON(t, response.Body, &received)
		if len(received.Messages) != 1 || received.Messages[0].Body != want || received.Messages[0].SequenceNumber != fmt.Sprintf("%d", index+1) {
			t.Fatalf("FIFO receive %d=%+v want body=%q", index, received.Messages, want)
		}
		deleteBody := fmt.Sprintf(`{"QueueUrl":%q,"ReceiptHandle":%q}`, queues[0].QueueURL, received.Messages[0].ReceiptHandle)
		mustSecureAction(t, config, finalLeader, config.Tokens[0], "/v1/sqs/DeleteMessage", deleteBody, fmt.Sprintf("m10-delete-%d", index))
	}
	secureCompose(t, config, "start", majorityService)
	waitSecureHealth(t, config, config.URLs)

	otherBody := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"ReceiveRequestAttemptId":"tenant-one"}`, queues[1].QueueURL)
	otherLeader, response := waitSecureLeaderAction(t, config, config.URLs, config.Tokens[1], "/v1/sqs/ReceiveMessage", otherBody, "m10-receive-tenant-1")
	var other secureReceiveResponse
	decodeSecureJSON(t, response.Body, &other)
	if otherLeader == "" || len(other.Messages) != 1 || other.Messages[0].Body != "tenant-1" {
		t.Fatalf("tenant isolation receive=%+v", other.Messages)
	}
}

func TestDisasterRecoveryPrepare(t *testing.T) {
	config := loadSecureConfig(t)
	waitSecureHealth(t, config, config.URLs)
	leader, created := waitSecureLeaderAction(t, config, config.URLs, config.Tokens[1], "/v1/sqs/CreateQueue", `{"QueueName":"recovery.fifo","Attributes":{"FifoQueue":"true"}}`, "m10-recovery-create")
	var queue secureQueueResponse
	decodeSecureJSON(t, created.Body, &queue)
	body := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"recovered-after-volume-restore","MessageGroupId":"recovery","MessageDeduplicationId":"recovery"}`, queue.QueueURL)
	mustSecureAction(t, config, leader, config.Tokens[1], "/v1/sqs/SendMessage", body, "m10-recovery-send")
	for _, shard := range []string{"s0", "s1"} {
		waitSecureAdminAction(t, config, "/v1/cluster/shards/snapshot", fmt.Sprintf(`{"ShardId":%q}`, shard))
	}
}

func TestDisasterRecoveryVerify(t *testing.T) {
	config := loadSecureConfig(t)
	waitSecureHealth(t, config, config.URLs)
	leader, found := waitSecureLeaderAction(t, config, config.URLs, config.Tokens[1], "/v1/sqs/GetQueueUrl", `{"QueueName":"recovery.fifo"}`, "")
	var queue secureQueueResponse
	decodeSecureJSON(t, found.Body, &queue)
	body := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"ReceiveRequestAttemptId":"recovery-verify"}`, queue.QueueURL)
	response := mustSecureAction(t, config, leader, config.Tokens[1], "/v1/sqs/ReceiveMessage", body, "m10-recovery-receive")
	var received secureReceiveResponse
	decodeSecureJSON(t, response.Body, &received)
	if len(received.Messages) != 1 || received.Messages[0].Body != "recovered-after-volume-restore" || received.Messages[0].SequenceNumber != "1" {
		t.Fatalf("restored message=%+v", received.Messages)
	}
}

func loadSecureConfig(t *testing.T) secureConfig {
	t.Helper()
	fixtureDir := os.Getenv("SIMQ_SECURE_FIXTURE_DIR")
	composeFile := os.Getenv("SIMQ_SECURE_COMPOSE_FILE")
	if fixtureDir == "" || composeFile == "" {
		t.Fatal("use scripts/secure-integration.ps1 or scripts/secure-integration.sh")
	}
	caPEM, err := os.ReadFile(filepath.Join(fixtureDir, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("fixture CA contains no certificate")
	}
	client := &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}}}
	tokens := make([]string, 2)
	for index, shard := range []string{"s0", "s1"} {
		encoded, readErr := os.ReadFile(filepath.Join(fixtureDir, "tenant-"+shard+".jwt"))
		if readErr != nil {
			t.Fatal(readErr)
		}
		tokens[index] = strings.TrimSpace(string(encoded))
	}
	environment, err := os.ReadFile(filepath.Join(fixtureDir, "environment.env"))
	if err != nil {
		t.Fatal(err)
	}
	values := parseEnvironment(string(environment))
	return secureConfig{
		URLs: []string{"https://localhost:19424", "https://localhost:19425", "https://localhost:19426"}, Tokens: tokens,
		FixtureDir: fixtureDir, ComposeFile: composeFile, Project: "simq-secure-integration", RaftNetwork: "simq-secure-raft",
		AdminToken: values["SIMQ_CLUSTER_ADMIN_TOKEN"], MetricsToken: values["SIMQ_METRICS_TOKEN"], Client: client,
	}
}

func parseEnvironment(encoded string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(encoded, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && key != "" {
			result[key] = value
		}
	}
	return result
}

func waitSecureHealth(t *testing.T, config secureConfig, urls []string) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	pending := append([]string(nil), urls...)
	for len(pending) > 0 && time.Now().Before(deadline) {
		next := pending[:0]
		for _, base := range pending {
			response, err := config.Client.Get(base + "/healthz")
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
		t.Fatalf("secure nodes did not become healthy: %v", pending)
	}
}

func waitSecureLeaderAction(t *testing.T, config secureConfig, urls []string, token, path, body, operationID string) (string, secureResponse) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	var last string
	for time.Now().Before(deadline) {
		for _, base := range urls {
			response, err := secureAction(config.Client, base, token, path, body, operationID, "")
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
	t.Fatalf("no secure leader completed %s: %s", path, last)
	return "", secureResponse{}
}

func mustSecureAction(t *testing.T, config secureConfig, base, token, path, body, operationID string) secureResponse {
	t.Helper()
	response, err := secureAction(config.Client, base, token, path, body, operationID, "")
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != http.StatusOK {
		t.Fatalf("%s%s returned %d: %s", base, path, response.Status, response.Body)
	}
	return response
}

func secureAction(client *http.Client, base, token, path, body, operationID, adminToken string) (secureResponse, error) {
	request, err := http.NewRequest(http.MethodPost, base+path, bytes.NewBufferString(body))
	if err != nil {
		return secureResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if adminToken != "" {
		request.Header.Set("Authorization", "Bearer "+adminToken)
	}
	if operationID != "" {
		request.Header.Set("X-SimQ-Operation-Id", operationID)
	}
	response, err := client.Do(request)
	if err != nil {
		return secureResponse{}, err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return secureResponse{}, err
	}
	return secureResponse{Status: response.StatusCode, Header: response.Header.Clone(), Body: encoded}, nil
}

func waitSecureStatus(t *testing.T, config secureConfig, base, token, path, body string, status int) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	for time.Now().Before(deadline) {
		response, err := secureAction(config.Client, base, token, path, body, "", "")
		if err == nil && response.Status == status {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not reach HTTP %d", base, status)
}

func waitForLeaderHint(t *testing.T, config secureConfig, base, token string) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	for time.Now().Before(deadline) {
		response, err := secureAction(config.Client, base, token, "/v1/sqs/ListQueues", `{}`, "", "")
		if err == nil && (response.Status == http.StatusOK || response.Status == http.StatusServiceUnavailable && response.Header.Get("X-SimQ-Leader") != "") {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not rejoin a leader", base)
}

func assertShardCatalog(t *testing.T, config secureConfig) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	for time.Now().Before(deadline) {
		for _, base := range config.URLs {
			request, _ := http.NewRequest(http.MethodGet, base+"/v1/cluster/shards", nil)
			request.Header.Set("Authorization", "Bearer "+config.AdminToken)
			response, err := config.Client.Do(request)
			if err != nil {
				continue
			}
			encoded, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
			var decoded struct {
				Shards []struct{ ID string }
			}
			if response.StatusCode == http.StatusOK && json.Unmarshal(encoded, &decoded) == nil {
				ids := make([]string, 0, len(decoded.Shards))
				for _, shard := range decoded.Shards {
					ids = append(ids, shard.ID)
				}
				sort.Strings(ids)
				if strings.Join(ids, ",") == "s0,s1" {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("secure cluster did not report both shards")
}

func waitSecureAdminAction(t *testing.T, config secureConfig, path, body string) {
	t.Helper()
	deadline := time.Now().Add(secureWait)
	for time.Now().Before(deadline) {
		for _, base := range config.URLs {
			response, err := secureAction(config.Client, base, "", path, body, "", config.AdminToken)
			if err == nil && response.Status == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("admin action %s did not succeed", path)
}

func decodeSecureJSON(t *testing.T, encoded []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(encoded, destination); err != nil {
		t.Fatalf("decode %s: %v", encoded, err)
	}
}

func secureServiceForURL(t *testing.T, base string) string {
	t.Helper()
	services := map[string]string{"https://localhost:19424": "n1", "https://localhost:19425": "n2", "https://localhost:19426": "n3"}
	service, ok := services[base]
	if !ok {
		t.Fatalf("no secure service for %s", base)
	}
	return service
}

func withoutSecureURL(values []string, excluded string) []string {
	result := make([]string, 0, len(values)-1)
	for _, value := range values {
		if value != excluded {
			result = append(result, value)
		}
	}
	return result
}

func secureCompose(t *testing.T, config secureConfig, arguments ...string) {
	t.Helper()
	command := secureComposeCommand(config, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}

func secureComposeOutput(t *testing.T, config secureConfig, arguments ...string) string {
	t.Helper()
	output, err := secureComposeCommand(config, arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func secureComposeCommand(config secureConfig, arguments ...string) *exec.Cmd {
	base := []string{"compose", "-p", config.Project, "-f", config.ComposeFile}
	return exec.Command("docker", append(base, arguments...)...)
}

func secureDocker(t *testing.T, arguments ...string) {
	t.Helper()
	output, err := exec.Command("docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}
