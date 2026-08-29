//go:build kindintegration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

const kindWait = 3 * time.Minute

type kindPodList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"metadata"`
		Status struct {
			Phase             string `json:"phase"`
			ContainerStatuses []struct {
				Ready bool `json:"ready"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

type kindActionResponse struct {
	Status int
	Header http.Header
	Body   []byte
}

type kindQueueURLResponse struct {
	QueueURL string `json:"QueueUrl"`
}

type kindReceiveResponse struct {
	Messages []struct {
		Body, ReceiptHandle string
		SequenceNumber      string `json:"SequenceNumber"`
	} `json:"Messages"`
}

type portForward struct {
	command *exec.Cmd
	logs    *lockedBuffer
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestKindStatefulSetLeaderReplacement(t *testing.T) {
	contextName := os.Getenv("SIMQ_KIND_CONTEXT")
	if contextName == "" {
		t.Fatal("SIMQ_KIND_CONTEXT is required; use scripts/kind-integration.ps1 or scripts/kind-integration.sh")
	}
	const baseURL = "http://127.0.0.1:19524"
	client := &http.Client{Timeout: 3 * time.Second}
	forward := startKindPortForward(t, contextName)
	waitKindHTTP(t, client, baseURL, forward)

	created := kindAction(t, client, baseURL, "/v1/sqs/CreateQueue", `{"QueueName":"kind-orders.fifo","Attributes":{"FifoQueue":"true"}}`, "kind-create")
	var queue kindQueueURLResponse
	decodeKindJSON(t, created.Body, &queue)
	sendBody := fmt.Sprintf(`{"QueueUrl":%q,"MessageBody":"survives-leader-pod-replacement","MessageGroupId":"group","MessageDeduplicationId":"kind-message"}`, queue.QueueURL)
	kindAction(t, client, baseURL, "/v1/sqs/SendMessage", sendBody, "kind-send")

	leaderName, leaderUID := waitKindReadyPod(t, contextName, "")
	pvcName := "data-" + leaderName
	pvcUID := kindResourceUID(t, contextName, "pvc", pvcName)
	stopKindPortForward(forward)
	kubectl(t, contextName, "delete", "pod", leaderName, "--grace-period=0", "--force", "--wait=false")
	waitKindRecreatedPod(t, contextName, leaderName, leaderUID)
	if recoveredPVCUID := kindResourceUID(t, contextName, "pvc", pvcName); recoveredPVCUID != pvcUID {
		t.Fatalf("leader PVC changed after Pod replacement: before=%q after=%q", pvcUID, recoveredPVCUID)
	}
	waitKindReadyPod(t, contextName, leaderUID)

	forward = startKindPortForward(t, contextName)
	waitKindHTTP(t, client, baseURL, forward)
	found := kindAction(t, client, baseURL, "/v1/sqs/GetQueueUrl", `{"QueueName":"kind-orders.fifo"}`, "")
	var recovered kindQueueURLResponse
	decodeKindJSON(t, found.Body, &recovered)
	if recovered.QueueURL != queue.QueueURL {
		t.Fatalf("queue URL changed after Pod replacement: before=%q after=%q", queue.QueueURL, recovered.QueueURL)
	}
	receiveBody := fmt.Sprintf(`{"QueueUrl":%q,"MaxNumberOfMessages":1,"ReceiveRequestAttemptId":"kind-receive"}`, queue.QueueURL)
	received := kindAction(t, client, baseURL, "/v1/sqs/ReceiveMessage", receiveBody, "kind-receive")
	var messages kindReceiveResponse
	decodeKindJSON(t, received.Body, &messages)
	if len(messages.Messages) != 1 || messages.Messages[0].Body != "survives-leader-pod-replacement" || messages.Messages[0].SequenceNumber != "1" {
		t.Fatalf("recovered messages=%+v", messages.Messages)
	}
}

func kindResourceUID(t *testing.T, contextName, kind, name string) string {
	t.Helper()
	output, err := exec.Command(kindKubectl(), "--context", contextName, "-n", "simq-m10", "get", kind, name, "-o", "jsonpath={.metadata.uid}").CombinedOutput()
	if err != nil {
		t.Fatalf("read %s/%s UID: %v\n%s", kind, name, err, output)
	}
	uid := strings.TrimSpace(string(output))
	if uid == "" {
		t.Fatalf("%s/%s has an empty UID", kind, name)
	}
	return uid
}

func kindAction(t *testing.T, client *http.Client, base, path, body, operationID string) kindActionResponse {
	t.Helper()
	deadline := time.Now().Add(kindWait)
	var last string
	for time.Now().Before(deadline) {
		request, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		if operationID != "" {
			request.Header.Set("X-SimQ-Operation-Id", operationID)
		}
		response, err := client.Do(request)
		if err != nil {
			last = err.Error()
			time.Sleep(250 * time.Millisecond)
			continue
		}
		encoded, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return kindActionResponse{Status: response.StatusCode, Header: response.Header.Clone(), Body: encoded}
		}
		last = fmt.Sprintf("status=%d body=%s", response.StatusCode, encoded)
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("kind action %s failed: %s", path, last)
	return kindActionResponse{}
}

func decodeKindJSON(t *testing.T, encoded []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(encoded, destination); err != nil {
		t.Fatalf("decode %s: %v", encoded, err)
	}
}

func startKindPortForward(t *testing.T, contextName string) *portForward {
	t.Helper()
	logs := &lockedBuffer{}
	command := exec.Command(kindKubectl(), "--context", contextName, "-n", "simq-m10", "port-forward", "service/simq-api", "19524:9324")
	command.Stdout, command.Stderr = logs, logs
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	result := &portForward{command: command, logs: logs}
	t.Cleanup(func() { stopKindPortForward(result) })
	return result
}

func stopKindPortForward(forward *portForward) {
	if forward == nil || forward.command == nil || forward.command.Process == nil || forward.command.ProcessState != nil {
		return
	}
	_ = forward.command.Process.Kill()
	_ = forward.command.Wait()
}

func waitKindHTTP(t *testing.T, client *http.Client, base string, forward *portForward) {
	t.Helper()
	deadline := time.Now().Add(kindWait)
	for time.Now().Before(deadline) {
		response, err := client.Get(base + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if forward.command.ProcessState != nil {
			t.Fatalf("port-forward exited: %s", forward.logs.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("port-forward did not become ready: %s", forward.logs.String())
}

func waitKindReadyPod(t *testing.T, contextName, replacedUID string) (string, string) {
	t.Helper()
	deadline := time.Now().Add(kindWait)
	for time.Now().Before(deadline) {
		output, err := exec.Command(kindKubectl(), "--context", contextName, "-n", "simq-m10", "get", "pods", "-l", "app=simq", "-o", "json").Output()
		if err == nil {
			var pods kindPodList
			if json.Unmarshal(output, &pods) == nil && len(pods.Items) == 3 {
				for _, pod := range pods.Items {
					ready := pod.Status.Phase == "Running" && len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].Ready
					if ready && (replacedUID == "" || pod.Metadata.UID != replacedUID) {
						return pod.Metadata.Name, pod.Metadata.UID
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("kind cluster did not expose a replacement ready leader")
	return "", ""
}

func waitKindRecreatedPod(t *testing.T, contextName, name, previousUID string) {
	t.Helper()
	deadline := time.Now().Add(kindWait)
	for time.Now().Before(deadline) {
		output, err := exec.Command(kindKubectl(), "--context", contextName, "-n", "simq-m10", "get", "pod", name, "-o", "json").Output()
		if err == nil {
			var pod struct {
				Metadata struct {
					UID string `json:"uid"`
				} `json:"metadata"`
				Status struct {
					Phase string `json:"phase"`
				} `json:"status"`
			}
			if json.Unmarshal(output, &pod) == nil && pod.Metadata.UID != "" && pod.Metadata.UID != previousUID && pod.Status.Phase == "Running" {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Pod %s was not recreated after deleting UID %s", name, previousUID)
}

func kubectl(t *testing.T, contextName string, arguments ...string) {
	t.Helper()
	base := []string{"--context", contextName, "-n", "simq-m10"}
	output, err := exec.Command(kindKubectl(), append(base, arguments...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
}

func kindKubectl() string {
	if configured := os.Getenv("SIMQ_KUBECTL"); configured != "" {
		return configured
	}
	return "kubectl"
}
