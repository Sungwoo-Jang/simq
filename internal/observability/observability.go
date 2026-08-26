package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu           sync.Mutex
	maxTenants   int
	tenants      map[string]struct{}
	requests     map[metricKey]uint64
	latencyNanos map[metricKey]uint64
}
type metricKey struct{ Tenant, Action, Result string }

func NewMetrics(maxTenants int) *Metrics {
	if maxTenants <= 0 {
		maxTenants = 1024
	}
	return &Metrics{maxTenants: maxTenants, tenants: make(map[string]struct{}), requests: make(map[metricKey]uint64), latencyNanos: make(map[metricKey]uint64)}
}
func Hash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}
func (m *Metrics) Record(tenant, action, result string, duration time.Duration) {
	tenant = Hash(tenant)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[tenant]; !ok {
		if len(m.tenants) >= m.maxTenants {
			tenant = "overflow"
		} else {
			m.tenants[tenant] = struct{}{}
		}
	}
	key := metricKey{tenant, fixedAction(action), fixedResult(result)}
	m.requests[key]++
	m.latencyNanos[key] += uint64(max(duration, 0))
}
func (m *Metrics) WritePrometheus(writer io.Writer) error {
	m.mu.Lock()
	keys := make([]metricKey, 0, len(m.requests))
	counts := make(map[metricKey]uint64, len(m.requests))
	latencies := make(map[metricKey]uint64, len(m.requests))
	for key, value := range m.requests {
		keys = append(keys, key)
		counts[key] = value
		latencies[key] = m.latencyNanos[key]
	}
	m.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Tenant != keys[j].Tenant {
			return keys[i].Tenant < keys[j].Tenant
		}
		if keys[i].Action != keys[j].Action {
			return keys[i].Action < keys[j].Action
		}
		return keys[i].Result < keys[j].Result
	})
	if _, err := fmt.Fprintln(writer, "# TYPE simq_http_requests_total counter"); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "simq_http_requests_total{tenant_hash=%q,action=%q,result=%q} %d\n", key.Tenant, key.Action, key.Result, counts[key]); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, "# TYPE simq_http_request_duration_seconds_total counter"); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := fmt.Fprintf(writer, "simq_http_request_duration_seconds_total{tenant_hash=%q,action=%q,result=%q} %.9f\n", key.Tenant, key.Action, key.Result, float64(latencies[key])/float64(time.Second)); err != nil {
			return err
		}
	}
	return nil
}
func fixedAction(path string) string {
	value := strings.TrimPrefix(path, "/v1/sqs/")
	switch value {
	case "CreateQueue", "GetQueueUrl", "SendMessage", "SendMessageBatch", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "GetQueueAttributes", "SetQueueAttributes", "ListQueues", "DeleteQueue", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags", "AddPermission", "RemovePermission", "ListDeadLetterSourceQueues", "StartMessageMoveTask", "CancelMessageMoveTask", "ListMessageMoveTasks":
		return value
	default:
		return "unknown"
	}
}
func fixedResult(value string) string {
	switch value {
	case "2xx", "4xx", "5xx":
		return value
	default:
		return "other"
	}
}

type AuditEvent struct {
	Time          time.Time `json:"time"`
	RequestID     string    `json:"request_id"`
	TraceID       string    `json:"trace_id,omitempty"`
	PrincipalHash string    `json:"principal_hash"`
	TenantHash    string    `json:"tenant_hash"`
	Action        string    `json:"action"`
	Resource      string    `json:"resource"`
	Result        string    `json:"result"`
}
type Auditor interface{ Record(AuditEvent) error }
type FileAuditor struct {
	mu   sync.Mutex
	file *os.File
}

func OpenFileAuditor(path string) (*FileAuditor, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("audit path must be a regular file")
	}
	return &FileAuditor{file: file}, nil
}
func (a *FileAuditor) Record(event AuditEvent) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.file.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return a.file.Sync()
}
func (a *FileAuditor) Close() error { a.mu.Lock(); defer a.mu.Unlock(); return a.file.Close() }
