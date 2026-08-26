package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/api"
	"simq/internal/queue"
)

func TestConfigDefaultsToDurableStorage(t *testing.T) {
	for _, name := range []string{"SIMQ_ADDR", "SIMQ_PUBLIC_BASE_URL", "SIMQ_MAX_BODY_BYTES", "SIMQ_STORAGE", "SIMQ_DATA_PATH", "SIMQ_BBOLT_OPEN_TIMEOUT", "SIMQ_RETENTION_SWEEP_INTERVAL", "SIMQ_CLUSTER_NODE_ID", "SIMQ_CLUSTER_BIND", "SIMQ_CLUSTER_ADVERTISE", "SIMQ_CLUSTER_DATA_DIR", "SIMQ_CLUSTER_BOOTSTRAP", "SIMQ_CLUSTER_API_URLS", "SIMQ_CLUSTER_INITIAL_VOTERS", "SIMQ_CLUSTER_ADMIN_TOKEN", "SIMQ_SHARD_MANIFEST", "SIMQ_CLUSTER_TLS_CERT_FILE", "SIMQ_CLUSTER_TLS_KEY_FILE", "SIMQ_CLUSTER_TLS_CA_FILE", "SIMQ_CLUSTER_TLS_SERVER_NAME", "SIMQ_SECURITY_MODE", "SIMQ_OIDC_ISSUER", "SIMQ_OIDC_AUDIENCE", "SIMQ_OIDC_TENANT_CLAIM", "SIMQ_OIDC_ROLES_CLAIM", "SIMQ_ENCRYPTION_ACTIVE_KEY", "SIMQ_ENCRYPTION_KEYS", "SIMQ_TLS_CERT_FILE", "SIMQ_TLS_KEY_FILE", "SIMQ_LEGACY_TENANT", "SIMQ_TENANT_MAX_QUEUES", "SIMQ_TENANT_MAX_MESSAGES", "SIMQ_TENANT_MAX_PAYLOAD_BYTES", "SIMQ_TENANT_REQUESTS_PER_SECOND", "SIMQ_TENANT_REQUEST_BURST", "SIMQ_METRICS_TOKEN", "SIMQ_AUDIT_PATH"} {
		t.Setenv(name, "")
	}
	config, err := configFromEnvironment()
	if err != nil {
		t.Fatalf("configFromEnvironment: %v", err)
	}
	if config.storage != "bbolt" || config.dataPath != "./data/simq.db" || config.bboltOpenTimeout != time.Second {
		t.Fatalf("durable defaults = %#v", config)
	}
	if config.maxBodyBytes != defaultMaxBodyBytes {
		t.Fatalf("maxBodyBytes = %d", config.maxBodyBytes)
	}
	if config.retentionSweepInterval != time.Minute {
		t.Fatalf("retentionSweepInterval = %v", config.retentionSweepInterval)
	}
}

func TestOIDCSecurityConfigIsExplicitAndComplete(t *testing.T) {
	t.Setenv("SIMQ_SECURITY_MODE", "oidc")
	if _, err := configFromEnvironment(); err == nil {
		t.Fatal("OIDC mode accepted incomplete configuration")
	}
	t.Setenv("SIMQ_OIDC_ISSUER", "https://issuer.example")
	t.Setenv("SIMQ_OIDC_AUDIENCE", "simq")
	t.Setenv("SIMQ_LEGACY_TENANT", "legacy")
	t.Setenv("SIMQ_TLS_CERT_FILE", "server.pem")
	t.Setenv("SIMQ_TLS_KEY_FILE", "server-key.pem")
	t.Setenv("SIMQ_ENCRYPTION_ACTIVE_KEY", "k1")
	t.Setenv("SIMQ_ENCRYPTION_KEYS", "k1="+base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv("SIMQ_METRICS_TOKEN", "metrics-secret")
	t.Setenv("SIMQ_AUDIT_PATH", "audit.jsonl")
	t.Setenv("SIMQ_TENANT_MAX_QUEUES", "7")
	t.Setenv("SIMQ_TENANT_REQUESTS_PER_SECOND", "12.5")
	config, err := configFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.storageQuota.MaxQueues != 7 || config.ratePerSecond != 12.5 || len(config.encryptionKeys["k1"]) != 32 {
		t.Fatalf("OIDC config = %#v", config)
	}
}

func TestShardManifestRequiresSecurePlannedNodeWithoutLegacyBind(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "shards.json")
	manifest := `{
  "version": 1,
  "default_shard": "s0",
  "shards": [
    {"id":"s0","bootstrap_node":"n1","replicas":[
      {"node_id":"n1","raft_address":"127.0.0.1:7101","api_url":"https://n1.example","failure_domain":"az-a","initial_voter":true},
      {"node_id":"n2","raft_address":"127.0.0.1:7102","api_url":"https://n2.example","failure_domain":"az-b","initial_voter":true},
      {"node_id":"n3","raft_address":"127.0.0.1:7103","api_url":"https://n3.example","failure_domain":"az-c","initial_voter":true}
    ]},
    {"id":"s1","bootstrap_node":"n1","replicas":[
      {"node_id":"n1","raft_address":"127.0.0.1:7201","api_url":"https://n1.example","failure_domain":"az-a","initial_voter":true},
      {"node_id":"n2","raft_address":"127.0.0.1:7202","api_url":"https://n2.example","failure_domain":"az-b","initial_voter":true},
      {"node_id":"n3","raft_address":"127.0.0.1:7203","api_url":"https://n3.example","failure_domain":"az-c","initial_voter":true}
    ]}
  ]
}`
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SIMQ_SHARD_MANIFEST", manifestPath)
	if _, err := configFromEnvironment(); err == nil || !strings.Contains(err.Error(), "SIMQ_CLUSTER_NODE_ID") {
		t.Fatalf("manifest without node ID error = %v", err)
	}

	t.Setenv("SIMQ_CLUSTER_NODE_ID", "n1")
	t.Setenv("SIMQ_SECURITY_MODE", "oidc")
	t.Setenv("SIMQ_OIDC_ISSUER", "https://issuer.example")
	t.Setenv("SIMQ_OIDC_AUDIENCE", "simq")
	t.Setenv("SIMQ_LEGACY_TENANT", "legacy")
	t.Setenv("SIMQ_TLS_CERT_FILE", "server.pem")
	t.Setenv("SIMQ_TLS_KEY_FILE", "server-key.pem")
	t.Setenv("SIMQ_ENCRYPTION_ACTIVE_KEY", "k1")
	t.Setenv("SIMQ_ENCRYPTION_KEYS", "k1="+base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv("SIMQ_METRICS_TOKEN", "metrics-secret")
	t.Setenv("SIMQ_AUDIT_PATH", "audit.jsonl")
	t.Setenv("SIMQ_CLUSTER_ADMIN_TOKEN", "cluster-secret")
	t.Setenv("SIMQ_CLUSTER_BIND", "")
	config, err := configFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.shardManifestPath != manifestPath || config.clusterNodeID != "n1" || config.clusterBindAddress != "" {
		t.Fatalf("shard config = %#v", config)
	}
}

type testMessageExpirer struct {
	mu    sync.Mutex
	calls int
	seen  chan struct{}
	err   error
}

func (e *testMessageExpirer) ExpireMessages() (int, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	select {
	case e.seen <- struct{}{}:
	default:
	}
	return 0, e.err
}

func TestRetentionSweeperRetriesAndRedactsFailure(t *testing.T) {
	secret := "/secret/storage/path body-secret receipt-secret"
	expirer := &testMessageExpirer{seen: make(chan struct{}, 2), err: fmt.Errorf("%s", secret)}
	logs := &bytes.Buffer{}
	logger := log.New(logs, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runRetentionSweeper(ctx, logger, expirer, time.Millisecond)
		close(done)
	}()
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case <-expirer.seen:
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatal("retention sweeper did not retry")
		}
	}
	cancel()
	<-done
	if strings.Contains(logs.String(), secret) || !strings.Contains(logs.String(), "retention sweep failed") {
		t.Fatalf("retention sweep logs = %q", logs.String())
	}
}

func TestConfigRejectsInvalidStorageSettings(t *testing.T) {
	t.Run("storage", func(t *testing.T) {
		t.Setenv("SIMQ_STORAGE", "unknown")
		if _, err := configFromEnvironment(); err == nil {
			t.Fatal("config accepted unknown storage")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		t.Setenv("SIMQ_BBOLT_OPEN_TIMEOUT", "0s")
		if _, err := configFromEnvironment(); err == nil {
			t.Fatal("config accepted zero bbolt timeout")
		}
	})
	t.Run("retention sweep interval", func(t *testing.T) {
		t.Setenv("SIMQ_RETENTION_SWEEP_INTERVAL", "0s")
		if _, err := configFromEnvironment(); err == nil {
			t.Fatal("config accepted zero retention sweep interval")
		}
	})
}

type shutdownWaitTimer struct {
	channel chan time.Time
	stopped atomic.Bool
}

func (t *shutdownWaitTimer) C() <-chan time.Time { return t.channel }
func (t *shutdownWaitTimer) Stop() bool          { return !t.stopped.Swap(true) }

func TestHTTPServerShutdownInterruptsLongPollBeforeRepositoryClose(t *testing.T) {
	repository := queue.NewMemoryRepository()
	timerCreated := make(chan struct{}, 1)
	service := queue.NewService(repository, queue.WithWaitTimerFactory(func(time.Duration) queue.WaitTimer {
		timerCreated <- struct{}{}
		return &shutdownWaitTimer{channel: make(chan time.Time)}
	}))
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	handler := api.NewServer(api.Config{PublicBaseURL: "http://localhost:9324", RequestIDGenerator: func() string { return "shutdown-request" }}, service)
	server := &http.Server{Handler: handler}
	registerLongPollShutdown(server, service)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPost, "/v1/sqs/ReceiveMessage", strings.NewReader(`{"QueueUrl":"http://localhost:9324/queues/orders","WaitTimeSeconds":20}`))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		done <- response
	}()
	select {
	case <-timerCreated:
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not enter wait state")
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case response := <-done:
		if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"Code":"ServiceUnavailable"`) || response.Header().Get("X-SimQ-Request-Id") != "shutdown-request" {
			t.Fatalf("shutdown response = %d, %s, %#v", response.Code, response.Body.String(), response.Header())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown did not release long poll")
	}
	if err := repository.Health(); err != nil {
		t.Fatalf("repository was closed or changed before caller close: %v", err)
	}
}

func TestSIGKILLRestartRecoversHTTPState(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	dataPath := filepath.Join(directory, "simq.db")
	address := reserveAddress(t)
	baseURL := "http://" + address
	client := &http.Client{Timeout: time.Second}
	allLogs := strings.Builder{}

	process := startHelperProcess(t, address, baseURL, dataPath)
	waitForReady(t, client, baseURL, process)
	postJSON(t, client, baseURL+"/v1/sqs/CreateQueue", `{"QueueName":"orders","Attributes":{"VisibilityTimeout":"300"}}`)
	sent := postJSON(t, client, baseURL+"/v1/sqs/SendMessage", fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"restart-smoke-body","MessageAttributes":{"RestartKind":{"DataType":"String.event","StringValue":"restart-attribute-secret"},"RestartBinary":{"DataType":"Binary.data","BinaryValue":"AQID"}}}`, baseURL+"/queues/orders"))
	if messageID, _ := sent["MessageId"].(string); messageID == "" {
		t.Fatalf("SendMessage response = %#v", sent)
	}
	attributeDigest, _ := sent["MD5OfMessageAttributes"].(string)
	if attributeDigest == "" {
		t.Fatalf("SendMessage attribute digest = %#v", sent)
	}
	batch := postJSON(t, client, baseURL+"/v1/sqs/SendMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"delayed-a","MessageBody":"restart-delayed-body","DelaySeconds":900},{"Id":"delayed-b","MessageBody":"restart-batch-body","DelaySeconds":900}]}`, baseURL+"/queues/orders"))
	if successful, _ := batch["Successful"].([]interface{}); len(successful) != 2 {
		t.Fatalf("SendMessageBatch response = %#v", batch)
	}
	postJSON(t, client, baseURL+"/v1/sqs/SetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"Attributes":{"VisibilityTimeout":"240","DelaySeconds":"5","MessageRetentionPeriod":"120"}}`, baseURL+"/queues/orders"))
	allLogs.WriteString(killHelperProcess(t, process))

	process = startHelperProcess(t, address, baseURL, dataPath)
	waitForReady(t, client, baseURL, process)
	queueAttributes := postJSON(t, client, baseURL+"/v1/sqs/GetQueueAttributes", fmt.Sprintf(`{"QueueUrl":%q,"AttributeNames":["All"]}`, baseURL+"/queues/orders"))
	recoveredSettings, ok := queueAttributes["Attributes"].(map[string]interface{})
	if !ok || recoveredSettings["VisibilityTimeout"] != "240" || recoveredSettings["DelaySeconds"] != "5" || recoveredSettings["MessageRetentionPeriod"] != "120" {
		t.Fatalf("queue attributes after restart = %#v", queueAttributes)
	}
	received := postJSON(t, client, baseURL+"/v1/sqs/ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q,"WaitTimeSeconds":20,"MessageAttributeNames":["All"]}`, baseURL+"/queues/orders"))
	messages := responseMessages(t, received)
	if len(messages) != 1 || messages[0]["Body"] != "restart-smoke-body" {
		t.Fatalf("ReceiveMessage after send restart = %#v", received)
	}
	receipt, _ := messages[0]["ReceiptHandle"].(string)
	if receipt == "" {
		t.Fatalf("missing receipt in %#v", messages[0])
	}
	messageAttributes, ok := messages[0]["MessageAttributes"].(map[string]interface{})
	if !ok || messages[0]["MD5OfMessageAttributes"] != attributeDigest {
		t.Fatalf("message attributes after restart = %#v", messages[0])
	}
	restartKind, ok := messageAttributes["RestartKind"].(map[string]interface{})
	if !ok || restartKind["StringValue"] != "restart-attribute-secret" {
		t.Fatalf("string attribute after restart = %#v", messageAttributes)
	}
	restartBinary, ok := messageAttributes["RestartBinary"].(map[string]interface{})
	if !ok || restartBinary["BinaryValue"] != "AQID" {
		t.Fatalf("binary attribute after restart = %#v", messageAttributes)
	}
	postJSON(t, client, baseURL+"/v1/sqs/ChangeMessageVisibilityBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"zero","ReceiptHandle":%q,"VisibilityTimeout":0}]}`, baseURL+"/queues/orders", receipt))
	allLogs.WriteString(killHelperProcess(t, process))

	process = startHelperProcess(t, address, baseURL, dataPath)
	waitForReady(t, client, baseURL, process)
	rereceived := postJSON(t, client, baseURL+"/v1/sqs/ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, baseURL+"/queues/orders"))
	rereceivedMessages := responseMessages(t, rereceived)
	if len(rereceivedMessages) != 1 || rereceivedMessages[0]["Body"] != "restart-smoke-body" {
		t.Fatalf("ReceiveMessage after zero visibility restart = %#v", rereceived)
	}
	secondReceipt, _ := rereceivedMessages[0]["ReceiptHandle"].(string)
	if secondReceipt == "" || secondReceipt == receipt {
		t.Fatalf("re-receive receipt = %q, first = %q", secondReceipt, receipt)
	}
	attributes, ok := rereceivedMessages[0]["Attributes"].(map[string]interface{})
	if !ok || attributes["ApproximateReceiveCount"] != "2" {
		t.Fatalf("re-receive attributes = %#v", rereceivedMessages[0]["Attributes"])
	}
	postJSON(t, client, baseURL+"/v1/sqs/ChangeMessageVisibilityBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"extend","ReceiptHandle":%q,"VisibilityTimeout":300}]}`, baseURL+"/queues/orders", secondReceipt))
	allLogs.WriteString(killHelperProcess(t, process))

	process = startHelperProcess(t, address, baseURL, dataPath)
	waitForReady(t, client, baseURL, process)
	hidden := postJSON(t, client, baseURL+"/v1/sqs/ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, baseURL+"/queues/orders"))
	if messages := responseMessages(t, hidden); len(messages) != 0 {
		t.Fatalf("in-flight message visible after restart = %#v", hidden)
	}
	postJSON(t, client, baseURL+"/v1/sqs/DeleteMessageBatch", fmt.Sprintf(`{"QueueUrl":%q,"Entries":[{"Id":"delete","ReceiptHandle":%q}]}`, baseURL+"/queues/orders", secondReceipt))
	allLogs.WriteString(killHelperProcess(t, process))

	process = startHelperProcess(t, address, baseURL, dataPath)
	waitForReady(t, client, baseURL, process)
	deleted := postJSON(t, client, baseURL+"/v1/sqs/ReceiveMessage", fmt.Sprintf(`{"QueueUrl":%q}`, baseURL+"/queues/orders"))
	if messages := responseMessages(t, deleted); len(messages) != 0 {
		t.Fatalf("deleted message recovered after restart = %#v", deleted)
	}
	allLogs.WriteString(killHelperProcess(t, process))

	logs := allLogs.String()
	if strings.Contains(logs, "restart-smoke-body") || strings.Contains(logs, "restart-delayed-body") || strings.Contains(logs, "restart-batch-body") || strings.Contains(logs, "restart-attribute-secret") || strings.Contains(logs, receipt) || strings.Contains(logs, secondReceipt) {
		t.Fatalf("process logs exposed message body or receipt: %s", logs)
	}
}

func TestSimQProcessHelper(t *testing.T) {
	if os.Getenv("SIMQ_TEST_HELPER_PROCESS") != "1" {
		return
	}
	if runtime.GOOS == "windows" {
		listener, err := net.Listen("tcp", os.Getenv("SIMQ_TEST_CRASH_ADDR"))
		if err != nil {
			t.Fatalf("listen for crash trigger: %v", err)
		}
		go func() {
			connection, err := listener.Accept()
			if err == nil {
				_ = connection.Close()
				// Windows environments may deny TerminateProcess to the test
				// parent. Exit from the child without running shutdown hooks,
				// defers, or repository Close to preserve the crash boundary.
				os.Exit(137)
			}
		}()
	}
	main()
}

func TestClusterProcessesPreserveAcknowledgedWriteAcrossLeaderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process cluster smoke test")
	}
	root := t.TempDir()
	raftAddresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	httpAddresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	apiURLs := make([]string, 3)
	for index, address := range httpAddresses {
		apiURLs[index] = "http://" + address
	}
	voters := fmt.Sprintf("n1=%s,n2=%s,n3=%s", raftAddresses[0], raftAddresses[1], raftAddresses[2])
	urls := fmt.Sprintf("n1=%s,n2=%s,n3=%s", apiURLs[0], apiURLs[1], apiURLs[2])
	processes := make([]*helperProcess, 3)
	processes[1] = startClusterHelperProcess(t, "n2", httpAddresses[1], raftAddresses[1], apiURLs[1], filepath.Join(root, "n2"), false, voters, urls)
	processes[2] = startClusterHelperProcess(t, "n3", httpAddresses[2], raftAddresses[2], apiURLs[2], filepath.Join(root, "n3"), false, voters, urls)
	processes[0] = startClusterHelperProcess(t, "n1", httpAddresses[0], raftAddresses[0], apiURLs[0], filepath.Join(root, "n1"), true, voters, urls)
	client := &http.Client{Timeout: 2 * time.Second}
	leader := waitForClusterLeader(t, client, apiURLs, nil)
	request, err := http.NewRequest(http.MethodPost, apiURLs[leader]+"/v1/sqs/CreateQueue", strings.NewReader(`{"QueueName":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-SimQ-Operation-Id", "process-create-orders")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.StatusCode, encoded)
	}
	_ = killHelperProcess(t, processes[leader])
	remaining := map[int]bool{leader: true}
	newLeader := waitForClusterLeader(t, client, apiURLs, remaining)
	listRequest, _ := http.NewRequest(http.MethodPost, apiURLs[newLeader]+"/v1/sqs/ListQueues", strings.NewReader(`{}`))
	listRequest.Header.Set("Content-Type", "application/json")
	listResponse, err := client.Do(listRequest)
	if err != nil {
		t.Fatal(err)
	}
	listed, _ := io.ReadAll(listResponse.Body)
	_ = listResponse.Body.Close()
	if listResponse.StatusCode != http.StatusOK || !bytes.Contains(listed, []byte("/queues/orders")) {
		t.Fatalf("failover list status=%d body=%s", listResponse.StatusCode, listed)
	}
}

func startClusterHelperProcess(t *testing.T, id, httpAddress, raftAddress, baseURL, dataDir string, bootstrap bool, voters, urls string) *helperProcess {
	t.Helper()
	logs := &bytes.Buffer{}
	crashAddress := ""
	if runtime.GOOS == "windows" {
		crashAddress = reserveAddress(t)
	}
	command := exec.Command(os.Args[0], "-test.run=TestSimQProcessHelper")
	command.Env = append(os.Environ(), "SIMQ_TEST_HELPER_PROCESS=1", "SIMQ_ADDR="+httpAddress, "SIMQ_PUBLIC_BASE_URL="+baseURL, "SIMQ_STORAGE=bbolt", "SIMQ_DATA_PATH="+filepath.Join(dataDir, "standalone.db"), "SIMQ_CLUSTER_NODE_ID="+id, "SIMQ_CLUSTER_BIND="+raftAddress, "SIMQ_CLUSTER_ADVERTISE="+raftAddress, "SIMQ_CLUSTER_DATA_DIR="+dataDir, "SIMQ_CLUSTER_BOOTSTRAP="+strconv.FormatBool(bootstrap), "SIMQ_CLUSTER_INITIAL_VOTERS="+voters, "SIMQ_CLUSTER_API_URLS="+urls, "SIMQ_RETENTION_SWEEP_INTERVAL=1h", "SIMQ_TEST_CRASH_ADDR="+crashAddress)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &helperProcess{command: command, logs: logs, crashAddress: crashAddress}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = killHelperProcess(t, process)
		}
	})
	return process
}

func waitForClusterLeader(t *testing.T, client *http.Client, urls []string, skip map[int]bool) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for index, url := range urls {
			if skip[index] {
				continue
			}
			request, _ := http.NewRequest(http.MethodPost, url+"/v1/sqs/ListQueues", strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return index
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for index := range urls {
		if !skip[index] {
			t.Logf("node %d logs: %s", index, "unavailable")
		}
	}
	t.Fatal("cluster leader was not elected")
	return -1
}

type helperProcess struct {
	command      *exec.Cmd
	logs         *bytes.Buffer
	crashAddress string
}

func startHelperProcess(t *testing.T, address, baseURL, dataPath string) *helperProcess {
	t.Helper()
	logs := &bytes.Buffer{}
	crashAddress := ""
	if runtime.GOOS == "windows" {
		crashAddress = reserveAddress(t)
	}
	command := exec.Command(os.Args[0], "-test.run=TestSimQProcessHelper")
	command.Env = append(os.Environ(),
		"SIMQ_TEST_HELPER_PROCESS=1",
		"SIMQ_ADDR="+address,
		"SIMQ_PUBLIC_BASE_URL="+baseURL,
		"SIMQ_STORAGE=bbolt",
		"SIMQ_DATA_PATH="+dataPath,
		"SIMQ_BBOLT_OPEN_TIMEOUT=200ms",
		"SIMQ_RETENTION_SWEEP_INTERVAL=1h",
		"SIMQ_TEST_CRASH_ADDR="+crashAddress,
	)
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	process := &helperProcess{command: command, logs: logs, crashAddress: crashAddress}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			if runtime.GOOS == "windows" {
				_, _ = net.DialTimeout("tcp", crashAddress, 100*time.Millisecond)
			} else {
				_ = command.Process.Kill()
			}
			_ = command.Wait()
		}
	})
	return process
}

func waitForReady(t *testing.T, client *http.Client, baseURL string, process *helperProcess) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(baseURL + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	logs := killHelperProcess(t, process)
	t.Fatalf("server did not become ready; logs: %s", logs)
}

func killHelperProcess(t *testing.T, process *helperProcess) string {
	t.Helper()
	if process.command.Process == nil || process.command.ProcessState != nil {
		return process.logs.String()
	}
	if runtime.GOOS == "windows" {
		connection, err := net.DialTimeout("tcp", process.crashAddress, time.Second)
		if err != nil {
			t.Fatalf("crash helper: %v; logs: %s", err, process.logs.String())
		}
		_ = connection.Close()
	} else if err := process.command.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	if err := process.command.Wait(); err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("wait helper: %v", err)
		}
	}
	return process.logs.String()
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved address: %v", err)
	}
	return address
}

func postJSON(t *testing.T, client *http.Client, endpoint, body string) map[string]interface{} {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("POST %s: %v", endpoint, err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d, body = %s", endpoint, response.StatusCode, encoded)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return decoded
}

func responseMessages(t *testing.T, response map[string]interface{}) []map[string]interface{} {
	t.Helper()
	rawMessages, ok := response["Messages"].([]interface{})
	if !ok {
		t.Fatalf("Messages field = %#v", response["Messages"])
	}
	messages := make([]map[string]interface{}, 0, len(rawMessages))
	for _, raw := range rawMessages {
		message, ok := raw.(map[string]interface{})
		if !ok {
			t.Fatalf("message = %#v", raw)
		}
		messages = append(messages, message)
	}
	return messages
}
