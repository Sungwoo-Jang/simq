package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"simq/internal/auth"
	"simq/internal/observability"
	"simq/internal/ownership"
	"simq/internal/queue"
)

const defaultMaxBodyBytes int64 = 2 << 20

type operationScopedContextKey struct{}
type authenticatedContextKey struct{}

type Server struct {
	publicBaseURL      string
	maxBodyBytes       int64
	requestIDGenerator RequestIDGenerator
	queueService       *queue.Service
	clusterAdminToken  string
	authVerifier       auth.Verifier
	rateLimiter        *observability.RateLimiter
	metrics            *observability.Metrics
	metricsToken       string
	auditor            observability.Auditor
}

func NewServer(config Config, queueService *queue.Service) *Server {
	maxBodyBytes := config.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultMaxBodyBytes
	}
	requestIDGenerator := config.RequestIDGenerator
	if requestIDGenerator == nil {
		requestIDGenerator = randomRequestID
	}
	return &Server{
		publicBaseURL:      strings.TrimRight(config.PublicBaseURL, "/"),
		maxBodyBytes:       maxBodyBytes,
		requestIDGenerator: requestIDGenerator,
		queueService:       queueService,
		clusterAdminToken:  config.ClusterAdminToken,
		authVerifier:       config.AuthVerifier,
		rateLimiter:        config.RateLimiter,
		metrics:            config.Metrics,
		metricsToken:       config.MetricsToken,
		auditor:            config.Auditor,
	}
}

func (s *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/metrics" && s.metrics != nil {
		s.serveMetrics(response, request)
		return
	}
	if strings.HasPrefix(request.URL.Path, "/v1/cluster/") && s.clusterAdminToken != "" {
		s.serveClusterAdmin(response, request)
		return
	}
	if request.Method == http.MethodGet {
		switch request.URL.Path {
		case "/healthz":
			writeJSON(response, http.StatusOK, healthResponse{Status: "ok"})
			return
		case "/readyz":
			if err := s.queueService.Health(); err != nil {
				writeJSON(response, http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
				return
			}
			writeJSON(response, http.StatusOK, healthResponse{Status: "ok"})
			return
		}
	}
	if s.authVerifier != nil && request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/v1/sqs/") && request.Context().Value(authenticatedContextKey{}) == nil {
		requestID := s.nextRequestID()
		response.Header().Set("X-SimQ-Request-Id", requestID)
		traceID := traceIDFromRequest(request)
		started := time.Now()
		token, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok {
			writeError(response, http.StatusUnauthorized, "Unauthenticated", "A valid bearer token is required.", requestID)
			s.recordSecurity(auth.Principal{}, request.URL.Path, "4xx", requestID, traceID, time.Since(started))
			return
		}
		principal, err := s.authVerifier.Verify(request.Context(), token)
		if err != nil {
			writeError(response, http.StatusUnauthorized, "Unauthenticated", "A valid bearer token is required.", requestID)
			s.recordSecurity(auth.Principal{}, request.URL.Path, "4xx", requestID, traceID, time.Since(started))
			return
		}
		clustered, leader, _ := s.queueService.ClusterRoute()
		if s.rateLimiter != nil && (!clustered || leader) && !s.rateLimiter.Allow(principal.Tenant, started) {
			writeQuotaExceeded(response, requestID)
			s.recordSecurity(principal, request.URL.Path, "4xx", requestID, traceID, time.Since(started))
			return
		}
		if !authorized(principal, request.URL.Path) {
			writeError(response, http.StatusForbidden, "AccessDenied", "The principal is not authorized for this action.", requestID)
			s.recordSecurity(principal, request.URL.Path, "4xx", requestID, traceID, time.Since(started))
			return
		}
		scoped := s.scoped(s.queueService.WithTenant(principal.Tenant))
		request = request.WithContext(context.WithValue(request.Context(), authenticatedContextKey{}, principal))
		tracked := &statusWriter{ResponseWriter: response}
		scoped.ServeHTTP(tracked, request)
		result := statusClass(tracked.status)
		finalRequestID := response.Header().Get("X-SimQ-Request-Id")
		s.recordSecurity(principal, request.URL.Path, result, finalRequestID, traceID, time.Since(started))
		return
	}
	if clustered, leader, leaderURL := s.queueService.ClusterRoute(); clustered && !leader && request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/v1/sqs/") {
		requestID := s.nextRequestID()
		response.Header().Set("X-SimQ-Request-Id", requestID)
		if leaderURL != "" {
			response.Header().Set("X-SimQ-Leader", leaderURL)
		}
		writeError(response, http.StatusServiceUnavailable, "NotLeader", "The request must be sent to the current cluster leader.", requestID)
		return
	}
	if s.queueService.Clustered() && isMutatingAction(request.Method, request.URL.Path) && request.Context().Value(operationScopedContextKey{}) == nil {
		operationID := request.Header.Get("X-SimQ-Operation-Id")
		if !validOperationID(operationID) {
			requestID := s.nextRequestID()
			response.Header().Set("X-SimQ-Request-Id", requestID)
			writeError(response, http.StatusBadRequest, "InvalidRequest", "X-SimQ-Operation-Id must be 1 to 128 UTF-8 bytes without control characters.", requestID)
			return
		}
		scoped := s.scoped(s.queueService.WithOperationID(operationID))
		request = request.WithContext(context.WithValue(request.Context(), operationScopedContextKey{}, true))
		scoped.ServeHTTP(response, request)
		return
	}

	requestID := s.nextRequestID()
	response.Header().Set("X-SimQ-Request-Id", requestID)
	if request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/v1/sqs/") {
		switch request.URL.Path {
		case "/v1/sqs/CreateQueue":
			s.createQueue(response, request, requestID)
			return
		case "/v1/sqs/GetQueueUrl":
			s.getQueueURL(response, request, requestID)
			return
		case "/v1/sqs/SendMessage":
			s.sendMessage(response, request, requestID)
			return
		case "/v1/sqs/SendMessageBatch":
			s.sendMessageBatch(response, request, requestID)
			return
		case "/v1/sqs/ReceiveMessage":
			s.receiveMessage(response, request, requestID)
			return
		case "/v1/sqs/DeleteMessage":
			s.deleteMessage(response, request, requestID)
			return
		case "/v1/sqs/DeleteMessageBatch":
			s.deleteMessageBatch(response, request, requestID)
			return
		case "/v1/sqs/ChangeMessageVisibility":
			s.changeMessageVisibility(response, request, requestID)
			return
		case "/v1/sqs/ChangeMessageVisibilityBatch":
			s.changeMessageVisibilityBatch(response, request, requestID)
			return
		case "/v1/sqs/GetQueueAttributes":
			s.getQueueAttributes(response, request, requestID)
			return
		case "/v1/sqs/SetQueueAttributes":
			s.setQueueAttributes(response, request, requestID)
			return
		case "/v1/sqs/ListQueues":
			s.listQueues(response, request, requestID)
			return
		case "/v1/sqs/DeleteQueue":
			s.deleteQueue(response, request, requestID)
			return
		case "/v1/sqs/PurgeQueue":
			s.purgeQueue(response, request, requestID)
			return
		case "/v1/sqs/TagQueue":
			s.tagQueue(response, request, requestID)
			return
		case "/v1/sqs/UntagQueue":
			s.untagQueue(response, request, requestID)
			return
		case "/v1/sqs/ListQueueTags":
			s.listQueueTags(response, request, requestID)
			return
		case "/v1/sqs/AddPermission":
			s.addPermission(response, request, requestID)
			return
		case "/v1/sqs/RemovePermission":
			s.removePermission(response, request, requestID)
			return
		case "/v1/sqs/ListDeadLetterSourceQueues":
			s.listDeadLetterSourceQueues(response, request, requestID)
			return
		case "/v1/sqs/StartMessageMoveTask":
			s.startMessageMoveTask(response, request, requestID)
			return
		case "/v1/sqs/CancelMessageMoveTask":
			s.cancelMessageMoveTask(response, request, requestID)
			return
		case "/v1/sqs/ListMessageMoveTasks":
			s.listMessageMoveTasks(response, request, requestID)
			return
		default:
			if isKnownAction(request.URL.Path) {
				writeError(response, http.StatusNotImplemented, "NotImplemented", "The requested action is not implemented.", requestID)
				return
			}
		}
	}

	writeError(response, http.StatusNotFound, "UnknownAction", "The requested action does not exist.", requestID)
}

func (s *Server) scoped(service *queue.Service) *Server {
	return &Server{publicBaseURL: s.publicBaseURL, maxBodyBytes: s.maxBodyBytes, requestIDGenerator: s.requestIDGenerator, queueService: service, clusterAdminToken: s.clusterAdminToken, authVerifier: s.authVerifier, rateLimiter: s.rateLimiter, metrics: s.metrics, metricsToken: s.metricsToken, auditor: s.auditor}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *statusWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(value)
}
func statusClass(status int) string {
	if status >= 200 && status < 300 {
		return "2xx"
	}
	if status >= 400 && status < 500 {
		return "4xx"
	}
	if status >= 500 {
		return "5xx"
	}
	return "other"
}
func traceIDFromRequest(request *http.Request) string {
	value := request.Header.Get("traceparent")
	parts := strings.Split(value, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(parts[1]); err != nil || parts[1] == strings.Repeat("0", 32) {
		return ""
	}
	return parts[1]
}
func (s *Server) recordSecurity(principal auth.Principal, path, result, requestID, traceID string, duration time.Duration) {
	if s.metrics != nil {
		s.metrics.Record(principal.Tenant, path, result, duration)
	}
	if s.auditor != nil {
		_ = s.auditor.Record(observability.AuditEvent{Time: time.Now().UTC(), RequestID: requestID, TraceID: traceID, PrincipalHash: observability.Hash(principal.Issuer + "\x00" + principal.Subject), TenantHash: observability.Hash(principal.Tenant), Action: strings.TrimPrefix(path, "/v1/sqs/"), Resource: path, Result: result})
	}
}
func (s *Server) serveMetrics(response http.ResponseWriter, request *http.Request) {
	want := "Bearer " + s.metricsToken
	got := request.Header.Get("Authorization")
	if s.metricsToken == "" || len(got) != len(want) || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		response.WriteHeader(http.StatusUnauthorized)
		return
	}
	response.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response.WriteHeader(http.StatusOK)
	_ = s.metrics.WritePrometheus(response)
}
func bearerToken(header string) (string, bool) {
	if !strings.HasPrefix(header, "Bearer ") || strings.Count(header, " ") != 1 {
		return "", false
	}
	token := strings.TrimPrefix(header, "Bearer ")
	return token, token != ""
}
func authorized(principal auth.Principal, path string) bool {
	if principal.HasRole("simq.admin") {
		return true
	}
	switch path {
	case "/v1/sqs/GetQueueUrl", "/v1/sqs/GetQueueAttributes", "/v1/sqs/ListQueues", "/v1/sqs/ListQueueTags", "/v1/sqs/ListDeadLetterSourceQueues", "/v1/sqs/ListMessageMoveTasks":
		return principal.HasRole("simq.reader")
	case "/v1/sqs/SendMessage", "/v1/sqs/SendMessageBatch":
		return principal.HasRole("simq.producer")
	case "/v1/sqs/ReceiveMessage", "/v1/sqs/DeleteMessage", "/v1/sqs/DeleteMessageBatch", "/v1/sqs/ChangeMessageVisibility", "/v1/sqs/ChangeMessageVisibilityBatch":
		return principal.HasRole("simq.consumer")
	default:
		return false
	}
}

type clusterMembershipRequest struct {
	ShardID           string  `json:"ShardId,omitempty"`
	Action            string  `json:"Action"`
	NodeID            string  `json:"NodeId"`
	RaftAddress       string  `json:"RaftAddress"`
	MinCommandVersion *uint32 `json:"MinCommandVersion"`
	MaxCommandVersion *uint32 `json:"MaxCommandVersion"`
}

type tenantMigrationView struct {
	MigrationID      string `json:"MigrationId"`
	TenantDigest     string `json:"TenantDigest"`
	SourceShard      string `json:"SourceShard"`
	DestinationShard string `json:"DestinationShard"`
	SourceEpoch      uint64 `json:"SourceEpoch"`
	DestinationEpoch uint64 `json:"DestinationEpoch"`
	Phase            string `json:"Phase"`
	BundleHash       string `json:"BundleHash,omitempty"`
}

type tenantMigrationStatusView struct {
	Migration     tenantMigrationView `json:"Migration"`
	NextAction    string              `json:"NextAction,omitempty"`
	NextShard     string              `json:"NextShard,omitempty"`
	NextLeaderURL string              `json:"NextLeaderUrl,omitempty"`
	LocalLeader   bool                `json:"LocalLeader"`
}

func migrationView(value ownership.Migration) tenantMigrationView {
	return tenantMigrationView{MigrationID: value.ID, TenantDigest: value.TenantDigest, SourceShard: value.SourceShard, DestinationShard: value.DestinationShard, SourceEpoch: value.SourceEpoch, DestinationEpoch: value.DestinationEpoch, Phase: string(value.Phase), BundleHash: value.BundleHash}
}

func migrationStatusView(value ownership.Status) tenantMigrationStatusView {
	return tenantMigrationStatusView{Migration: migrationView(value.Migration), NextAction: value.NextAction, NextShard: value.NextShard, NextLeaderURL: value.NextLeaderURL, LocalLeader: value.LocalLeader}
}

func (s *Server) serveClusterAdmin(response http.ResponseWriter, request *http.Request) {
	requestID := s.nextRequestID()
	response.Header().Set("X-SimQ-Request-Id", requestID)
	want := "Bearer " + s.clusterAdminToken
	got := request.Header.Get("Authorization")
	if len(got) != len(want) || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		writeError(response, http.StatusUnauthorized, "Unauthorized", "A valid cluster administration token is required.", requestID)
		return
	}
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/tenant-migrations":
		limit := 100
		if raw := request.URL.Query().Get("Limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 1000 {
				writeError(response, http.StatusBadRequest, "InvalidRequest", "Limit must be from 1 through 1000.", requestID)
				return
			}
			limit = parsed
		}
		migrations, err := s.queueService.ListTenantMigrations(limit)
		if err != nil {
			s.writeTenantMigrationError(response, requestID, ownership.Status{}, err)
			return
		}
		items := make([]tenantMigrationView, len(migrations))
		for index, migration := range migrations {
			items[index] = migrationView(migration)
		}
		writeJSON(response, http.StatusOK, struct {
			Migrations []tenantMigrationView `json:"Migrations"`
			RequestID  string                `json:"RequestId"`
		}{items, requestID})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/tenant-migrations/status":
		status, err := s.queueService.TenantMigrationStatus(request.URL.Query().Get("MigrationId"))
		if err != nil {
			s.writeTenantMigrationError(response, requestID, status, err)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Status    tenantMigrationStatusView `json:"Status"`
			RequestID string                    `json:"RequestId"`
		}{migrationStatusView(status), requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/tenant-migrations":
		var input struct {
			MigrationID, TenantDigest, DestinationShard string
		}
		if !decodeStrictAdminJSON(response, request, requestID, &input) {
			return
		}
		migration, err := s.queueService.BeginTenantMigration(input.MigrationID, input.TenantDigest, input.DestinationShard)
		if err != nil {
			s.writeTenantMigrationError(response, requestID, ownership.Status{}, err)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Migration tenantMigrationView `json:"Migration"`
			RequestID string              `json:"RequestId"`
		}{migrationView(migration), requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/tenant-migrations/advance":
		var input struct{ MigrationID string }
		if !decodeStrictAdminJSON(response, request, requestID, &input) {
			return
		}
		status, err := s.queueService.AdvanceTenantMigration(input.MigrationID)
		if err != nil {
			s.writeTenantMigrationError(response, requestID, status, err)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Status    tenantMigrationStatusView `json:"Status"`
			RequestID string                    `json:"RequestId"`
		}{migrationStatusView(status), requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/tenant-migrations/abort":
		var input struct{ MigrationID string }
		if !decodeStrictAdminJSON(response, request, requestID, &input) {
			return
		}
		status, err := s.queueService.AbortTenantMigration(input.MigrationID)
		if err != nil {
			s.writeTenantMigrationError(response, requestID, status, err)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Status    tenantMigrationStatusView `json:"Status"`
			RequestID string                    `json:"RequestId"`
		}{migrationStatusView(status), requestID})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/shards":
		statuses, err := s.queueService.ShardStatuses()
		if err != nil {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Shards    []queue.ShardStatus `json:"Shards"`
			RequestID string              `json:"RequestId"`
		}{statuses, requestID})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/shards/members":
		shardID := request.URL.Query().Get("ShardId")
		if shardID == "" {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "ShardId is required.", requestID)
			return
		}
		members, err := s.queueService.ShardMembers(shardID)
		if err != nil {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			ShardID   string                `json:"ShardId"`
			Members   []queue.ClusterMember `json:"Members"`
			RequestID string                `json:"RequestId"`
		}{shardID, members, requestID})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/protocol":
		writeJSON(response, http.StatusOK, struct {
			MinCommandVersion uint32 `json:"MinCommandVersion"`
			MaxCommandVersion uint32 `json:"MaxCommandVersion"`
			RequestID         string `json:"RequestId"`
		}{2, 2, requestID})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/cluster/members":
		members, err := s.queueService.ClusterMembers()
		if err != nil {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			Members   []queue.ClusterMember `json:"Members"`
			RequestID string                `json:"RequestId"`
		}{members, requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/members":
		var input clusterMembershipRequest
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "The membership request is malformed.", requestID)
			return
		}
		adding := input.Action == "add-voter" || input.Action == "add-nonvoter"
		compatible := !adding || (input.MinCommandVersion != nil && input.MaxCommandVersion != nil && *input.MinCommandVersion <= 2 && *input.MaxCommandVersion >= 2)
		if input.NodeID == "" || adding && input.RaftAddress == "" || !compatible {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "A valid Action, NodeId, and required RaftAddress are required.", requestID)
			return
		}
		if err := s.queueService.ReconfigureCluster(input.Action, input.NodeID, input.RaftAddress); err != nil {
			if errors.Is(err, queue.ErrRepositoryUnavailable) {
				writeStorageUnavailable(response, requestID)
			} else {
				writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
			}
			return
		}
		writeJSON(response, http.StatusOK, struct {
			RequestID string `json:"RequestId"`
		}{requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/shards/members":
		var input clusterMembershipRequest
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "The shard membership request is malformed.", requestID)
			return
		}
		adding := input.Action == "add-voter" || input.Action == "add-nonvoter"
		compatible := !adding || input.MinCommandVersion != nil && input.MaxCommandVersion != nil && *input.MinCommandVersion <= 2 && *input.MaxCommandVersion >= 2
		if input.ShardID == "" || input.NodeID == "" || adding && input.RaftAddress == "" || !compatible {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "Valid ShardId, Action, NodeId, protocol range, and required RaftAddress are required.", requestID)
			return
		}
		if err := s.queueService.ReconfigureShard(input.ShardID, input.Action, input.NodeID, input.RaftAddress); err != nil {
			if errors.Is(err, queue.ErrRepositoryUnavailable) {
				writeStorageUnavailable(response, requestID)
			} else {
				writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
			}
			return
		}
		writeJSON(response, http.StatusOK, struct {
			RequestID string `json:"RequestId"`
		}{requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/snapshot":
		if err := s.queueService.TriggerClusterSnapshot(); err != nil {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			RequestID string `json:"RequestId"`
		}{requestID})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/cluster/shards/snapshot":
		var input struct {
			ShardID string `json:"ShardId"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || input.ShardID == "" {
			writeError(response, http.StatusBadRequest, "InvalidRequest", "ShardId is required.", requestID)
			return
		}
		if err := s.queueService.TriggerShardSnapshot(input.ShardID); err != nil {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeJSON(response, http.StatusOK, struct {
			RequestID string `json:"RequestId"`
		}{requestID})
	default:
		writeError(response, http.StatusNotFound, "UnknownAction", "The requested cluster action does not exist.", requestID)
	}
}

func decodeStrictAdminJSON(response http.ResponseWriter, request *http.Request, requestID string, destination any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "The migration request is malformed.", requestID)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "The migration request has trailing content.", requestID)
		return false
	}
	return true
}

func (s *Server) writeTenantMigrationError(response http.ResponseWriter, requestID string, status ownership.Status, err error) {
	if status.NextLeaderURL != "" {
		response.Header().Set("X-SimQ-Leader", status.NextLeaderURL)
	}
	var invalid *queue.InvalidRequestError
	switch {
	case errors.As(err, &invalid):
		writeError(response, http.StatusBadRequest, "InvalidRequest", invalid.Error(), requestID)
	case errors.Is(err, queue.ErrMigrationDoesNotExist):
		writeError(response, http.StatusNotFound, "MigrationDoesNotExist", "The tenant migration does not exist.", requestID)
	case errors.Is(err, queue.ErrMigrationAlreadyExists), errors.Is(err, queue.ErrTenantOwnershipConflict):
		writeError(response, http.StatusConflict, "MigrationConflict", "The tenant migration conflicts with authoritative state.", requestID)
	case errors.Is(err, queue.ErrRepositoryUnavailable):
		writeStorageUnavailable(response, requestID)
	default:
		writeError(response, http.StatusInternalServerError, "InternalError", "The migration request could not be completed.", requestID)
	}
}

func validOperationID(value string) bool {
	if len(value) < 1 || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func isMutatingAction(method, path string) bool {
	if method != http.MethodPost || !strings.HasPrefix(path, "/v1/sqs/") {
		return false
	}
	switch path {
	case "/v1/sqs/GetQueueUrl", "/v1/sqs/GetQueueAttributes", "/v1/sqs/ListQueues", "/v1/sqs/ListQueueTags", "/v1/sqs/ListDeadLetterSourceQueues", "/v1/sqs/ListMessageMoveTasks":
		return false
	case "/v1/sqs/CreateQueue", "/v1/sqs/SendMessage", "/v1/sqs/SendMessageBatch", "/v1/sqs/ReceiveMessage", "/v1/sqs/DeleteMessage", "/v1/sqs/DeleteMessageBatch", "/v1/sqs/ChangeMessageVisibility", "/v1/sqs/ChangeMessageVisibilityBatch", "/v1/sqs/SetQueueAttributes", "/v1/sqs/DeleteQueue", "/v1/sqs/PurgeQueue", "/v1/sqs/TagQueue", "/v1/sqs/UntagQueue", "/v1/sqs/AddPermission", "/v1/sqs/RemovePermission", "/v1/sqs/StartMessageMoveTask", "/v1/sqs/CancelMessageMoveTask":
		return true
	default:
		return false
	}
}

func (s *Server) createQueue(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeCreateQueueRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	created, err := s.queueService.CreateQueue(input)
	if err != nil {
		if errors.Is(err, queue.ErrQuotaExceeded) {
			writeQuotaExceeded(response, requestID)
			return
		}
		if errors.Is(err, queue.ErrQueueAlreadyExists) {
			writeError(response, http.StatusConflict, "QueueAlreadyExists", "A queue with this name already exists with different attributes.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	writeJSON(response, http.StatusOK, queueURLResponse{
		QueueURL:  s.queueURL(created),
		RequestID: requestID,
	})
}

func (s *Server) getQueueURL(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeGetQueueRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	found, err := s.queueService.GetQueue(input)
	if err != nil {
		if errors.Is(err, queue.ErrQueueDoesNotExist) {
			writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	writeJSON(response, http.StatusOK, queueURLResponse{
		QueueURL:  s.queueURL(found),
		RequestID: requestID,
	})
}

func (s *Server) sendMessage(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeSendMessageRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	message, err := s.queueService.SendMessage(queue.SendMessageInput{
		QueueName:              queueRef.Name,
		QueueID:                queueRef.ID,
		MessageBody:            input.MessageBody,
		DelaySeconds:           input.DelaySeconds,
		MessageAttributes:      input.MessageAttributes,
		MessageGroupID:         input.MessageGroupID,
		MessageDeduplicationID: input.MessageDeduplicationID,
	})
	if err != nil {
		if errors.Is(err, queue.ErrQuotaExceeded) {
			writeQuotaExceeded(response, requestID)
			return
		}
		if errors.Is(err, queue.ErrQueueDoesNotExist) {
			writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	writeJSON(response, http.StatusOK, sendMessageResponse{
		MessageID:              message.ID,
		MD5OfMessageBody:       message.MD5OfBody,
		MD5OfMessageAttributes: message.MD5OfMessageAttributes,
		SequenceNumber:         fifoSequenceString(message.SequenceNumber),
		RequestID:              requestID,
	})
}

func (s *Server) sendMessageBatch(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	input, err := decodeSendMessageBatchRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	result, err := s.queueService.SendMessageBatch(queue.SendMessageBatchInput{QueueName: queueRef.Name, QueueID: queueRef.ID, Entries: input.Entries})
	if err != nil {
		s.writeBatchRequestError(response, err, requestID)
		return
	}
	successful := make([]sendMessageBatchSuccessResponse, 0, len(result.Successful))
	for _, entry := range result.Successful {
		successful = append(successful, sendMessageBatchSuccessResponse{ID: entry.ID, MessageID: entry.Message.ID, MD5OfMessageBody: entry.Message.MD5OfBody, MD5OfMessageAttributes: entry.Message.MD5OfMessageAttributes, SequenceNumber: fifoSequenceString(entry.Message.SequenceNumber)})
	}
	writeJSON(response, http.StatusOK, sendMessageBatchResponse{Successful: successful, Failed: batchFailures(result.Failed), RequestID: requestID})
}

func (s *Server) receiveMessage(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeReceiveMessageRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	messages, err := s.queueService.ReceiveMessageContext(request.Context(), queue.ReceiveMessageInput{
		QueueName:               queueRef.Name,
		QueueID:                 queueRef.ID,
		MaxNumberOfMessages:     input.MaxNumberOfMessages,
		VisibilityTimeout:       input.VisibilityTimeout,
		WaitTimeSeconds:         input.WaitTimeSeconds,
		MessageAttributeNames:   input.MessageAttributeNames,
		ReceiveRequestAttemptID: input.ReceiveRequestAttemptID,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if errors.Is(err, queue.ErrQueueDoesNotExist) {
			writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	responseMessages := make([]receivedMessage, 0, len(messages))
	for _, message := range messages {
		messageAttributes := make(map[string]messageAttributeValue, len(message.MessageAttributes))
		for name, attribute := range message.MessageAttributes {
			messageAttributes[name] = messageAttributeValue{
				DataType:    attribute.DataType,
				StringValue: attribute.StringValue,
				BinaryValue: append([]byte(nil), attribute.BinaryValue...),
			}
		}
		if len(messageAttributes) == 0 {
			messageAttributes = nil
		}
		responseMessages = append(responseMessages, receivedMessage{
			MessageID:              message.ID,
			ReceiptHandle:          message.ReceiptHandle,
			MD5OfBody:              message.MD5OfBody,
			Body:                   message.Body,
			MessageAttributes:      messageAttributes,
			MD5OfMessageAttributes: message.MD5OfMessageAttributes,
			MessageGroupID:         message.MessageGroupID,
			MessageDeduplicationID: message.MessageDeduplicationID,
			SequenceNumber:         fifoSequenceString(message.SequenceNumber),
			Attributes: receivedMessageAttributes{
				ApproximateReceiveCount:          strconv.FormatUint(message.ReceiveCount, 10),
				SentTimestamp:                    strconv.FormatInt(message.SentAtMillis, 10),
				ApproximateFirstReceiveTimestamp: strconv.FormatInt(message.FirstReceivedAtMillis, 10),
			},
		})
	}
	writeJSON(response, http.StatusOK, receiveMessageResponse{
		Messages:  responseMessages,
		RequestID: requestID,
	})
}

func (s *Server) deleteMessage(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeDeleteMessageRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	err = s.queueService.DeleteMessage(queue.DeleteMessageInput{
		QueueName:     queueRef.Name,
		QueueID:       queueRef.ID,
		ReceiptHandle: input.ReceiptHandle,
	})
	if err != nil {
		if errors.Is(err, queue.ErrQueueDoesNotExist) {
			writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
			return
		}
		if errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
			writeError(response, http.StatusBadRequest, "ReceiptHandleIsInvalid", "The receipt handle is invalid.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) deleteMessageBatch(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	input, err := decodeDeleteMessageBatchRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	result, err := s.queueService.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: queueRef.Name, QueueID: queueRef.ID, Entries: input.Entries})
	if err != nil {
		s.writeBatchRequestError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, batchResponse{Successful: batchSuccesses(result.Successful), Failed: batchFailures(result.Failed), RequestID: requestID})
}

func (s *Server) changeMessageVisibility(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}

	input, err := decodeChangeMessageVisibilityRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}

	err = s.queueService.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         queueRef.Name,
		QueueID:           queueRef.ID,
		ReceiptHandle:     input.ReceiptHandle,
		VisibilityTimeout: input.VisibilityTimeout,
	})
	if err != nil {
		if errors.Is(err, queue.ErrQueueDoesNotExist) {
			writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
			return
		}
		if errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
			writeError(response, http.StatusBadRequest, "ReceiptHandleIsInvalid", "The receipt handle is invalid.", requestID)
			return
		}
		var invalidRequest *queue.InvalidRequestError
		if errors.As(err, &invalidRequest) {
			writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
			return
		}
		if errors.Is(err, queue.ErrRepositoryUnavailable) {
			writeStorageUnavailable(response, requestID)
			return
		}
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
		return
	}

	writeJSON(response, http.StatusOK, changeMessageVisibilityResponse{RequestID: requestID})
}

func (s *Server) changeMessageVisibilityBatch(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	input, err := decodeChangeMessageVisibilityBatchRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	result, err := s.queueService.ChangeMessageVisibilityBatch(queue.ChangeMessageVisibilityBatchInput{QueueName: queueRef.Name, QueueID: queueRef.ID, Entries: input.Entries})
	if err != nil {
		s.writeBatchRequestError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, batchResponse{Successful: batchSuccesses(result.Successful), Failed: batchFailures(result.Failed), RequestID: requestID})
}

func (s *Server) writeBatchRequestError(response http.ResponseWriter, err error, requestID string) {
	var batchError *queue.BatchRequestError
	if errors.As(err, &batchError) {
		writeError(response, http.StatusBadRequest, batchError.Code, batchError.Message, requestID)
		return
	}
	s.writeQueueActionError(response, err, requestID)
}

func batchSuccesses(ids []string) []batchSuccessResponse {
	result := make([]batchSuccessResponse, len(ids))
	for index, id := range ids {
		result[index] = batchSuccessResponse{ID: id}
	}
	return result
}
func batchFailures(failures []queue.BatchFailure) []batchFailureResponse {
	result := make([]batchFailureResponse, 0, len(failures))
	for _, failure := range failures {
		value := batchFailureResponse{ID: failure.ID, Code: "InternalError", Message: "The entry could not be completed.", SenderFault: false}
		var invalid *queue.InvalidRequestError
		switch {
		case errors.As(failure.Error, &invalid):
			value.Code, value.Message, value.SenderFault = "InvalidRequest", invalid.Error(), true
		case errors.Is(failure.Error, queue.ErrReceiptHandleIsInvalid):
			value.Code, value.Message, value.SenderFault = "ReceiptHandleIsInvalid", "The receipt handle is invalid.", true
		case errors.Is(failure.Error, queue.ErrQueueDoesNotExist):
			value.Code, value.Message, value.SenderFault = "QueueDoesNotExist", "The specified queue does not exist.", true
		case errors.Is(failure.Error, queue.ErrQuotaExceeded):
			value.Code, value.Message, value.SenderFault = "QuotaExceeded", "The tenant storage quota is exceeded.", true
		case errors.Is(failure.Error, queue.ErrRepositoryUnavailable):
			value.Code, value.Message = "ServiceUnavailable", "Durable storage is unavailable."
		}
		result = append(result, value)
	}
	return result
}

func (s *Server) getQueueAttributes(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	input, err := decodeGetQueueAttributesRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	attributes, err := s.queueService.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: queueRef.Name, QueueID: queueRef.ID, AttributeNames: input.AttributeNames})
	if err != nil {
		s.writeQueueActionError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, getQueueAttributesResponse{Attributes: attributes, RequestID: requestID})
}

func (s *Server) setQueueAttributes(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	input, err := decodeSetQueueAttributesRequest(body)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	queueRef, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	err = s.queueService.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: queueRef.Name, QueueID: queueRef.ID, Attributes: input.Attributes})
	if err != nil {
		s.writeQueueActionError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, setQueueAttributesResponse{RequestID: requestID})
}

func (s *Server) listQueues(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input listQueuesRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(body, &fields)
	if raw, present := fields["MaxResults"]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "MaxResults must be an integer", requestID)
		return
	}
	page, next, err := s.queueService.ListQueues(queue.ListQueuesInput{QueueNamePrefix: input.QueueNamePrefix, MaxResults: input.MaxResults, NextToken: input.NextToken})
	if err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	urls := make([]string, len(page.Queues))
	for index, value := range page.Queues {
		urls[index] = s.queueURL(value)
	}
	writeJSON(response, http.StatusOK, listQueuesResponse{QueueURLs: urls, NextToken: next, RequestID: requestID})
}

func (s *Server) deleteQueue(response http.ResponseWriter, request *http.Request, requestID string) {
	s.queueURLMutation(response, request, requestID, s.queueService.DeleteQueue)
}
func (s *Server) purgeQueue(response http.ResponseWriter, request *http.Request, requestID string) {
	s.queueURLMutation(response, request, requestID, s.queueService.PurgeQueue)
}

func (s *Server) queueURLMutation(response http.ResponseWriter, request *http.Request, requestID string, mutate func(queue.QueueRef) error) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input queueURLRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if err := mutate(ref); err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) tagQueue(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input tagQueueRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if err := s.queueService.TagQueue(queue.TagQueueInput{QueueName: ref.Name, QueueID: ref.ID, Tags: input.Tags}); err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) untagQueue(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input untagQueueRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if err := s.queueService.UntagQueue(queue.UntagQueueInput{QueueName: ref.Name, QueueID: ref.ID, TagKeys: input.TagKeys}); err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) listQueueTags(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input queueURLRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	tags, err := s.queueService.ListQueueTags(ref)
	if err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, listQueueTagsResponse{Tags: tags, RequestID: requestID})
}

func (s *Server) addPermission(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input addPermissionRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	err = s.queueService.AddPermission(queue.AddPermissionInput{QueueName: ref.Name, QueueID: ref.ID, Label: input.Label, AWSAccountIDs: input.AWSAccountIDs, Actions: input.Actions})
	if err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) removePermission(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input removePermissionRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	err = s.queueService.RemovePermission(queue.RemovePermissionInput{QueueName: ref.Name, QueueID: ref.ID, Label: input.Label})
	if err != nil {
		s.writeAdministrativeError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, deleteMessageResponse{RequestID: requestID})
}

func (s *Server) writeAdministrativeError(response http.ResponseWriter, err error, requestID string) {
	var invalid *queue.InvalidRequestError
	switch {
	case errors.Is(err, queue.ErrQueueDoesNotExist):
		writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
	case errors.Is(err, queue.ErrInvalidPaginationToken):
		writeError(response, http.StatusBadRequest, "InvalidPaginationToken", "The pagination token is invalid or expired.", requestID)
	case errors.Is(err, queue.ErrOverLimit):
		writeError(response, http.StatusBadRequest, "OverLimit", "The requested administrative metadata exceeds a limit.", requestID)
	case errors.Is(err, queue.ErrQueueInUse):
		writeError(response, http.StatusConflict, "ResourceInUse", "The queue is referenced by redrive configuration or a running move task.", requestID)
	case errors.As(err, &invalid):
		writeError(response, http.StatusBadRequest, "InvalidRequest", invalid.Error(), requestID)
	case errors.Is(err, queue.ErrRepositoryUnavailable):
		writeStorageUnavailable(response, requestID)
	default:
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
	}
}

func (s *Server) listDeadLetterSourceQueues(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input listDeadLetterSourceQueuesRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if containsJSONNull(body, "QueueUrl", "MaxResults", "NextToken") {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "request fields must not be null", requestID)
		return
	}
	ref, err := s.queueRefFromURL(input.QueueURL)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	page, next, err := s.queueService.ListDeadLetterSourceQueues(queue.ListDeadLetterSourcesInput{Target: ref, MaxResults: input.MaxResults, NextToken: input.NextToken})
	if err != nil {
		s.writeRedriveError(response, err, requestID)
		return
	}
	urls := make([]string, len(page.Queues))
	for index, value := range page.Queues {
		urls[index] = s.queueURL(value)
	}
	writeJSON(response, http.StatusOK, listDeadLetterSourceQueuesResponse{QueueURLs: urls, NextToken: next, RequestID: requestID})
}

func (s *Server) startMessageMoveTask(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input startMessageMoveTaskRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if containsJSONNull(body, "SourceArn", "DestinationArn", "MaxNumberOfMessagesPerSecond") {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "request fields must not be null", requestID)
		return
	}
	source, err := queue.ParseQueueARN(input.SourceARN)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	var destination *queue.QueueRef
	if input.DestinationARN != "" {
		parsed, err := queue.ParseQueueARN(input.DestinationARN)
		if err != nil {
			writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
			return
		}
		destination = &parsed
	}
	task, err := s.queueService.StartMessageMoveTask(queue.StartMessageMoveTaskInput{Source: source, Destination: destination, MaxMessagesPerSecond: input.MaxNumberOfMessagesPerSecond})
	if err != nil {
		s.writeRedriveError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, startMessageMoveTaskResponse{TaskHandle: task.Handle, RequestID: requestID})
}

func (s *Server) cancelMessageMoveTask(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input cancelMessageMoveTaskRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	task, err := s.queueService.CancelMessageMoveTask(input.TaskHandle)
	if err != nil {
		s.writeRedriveError(response, err, requestID)
		return
	}
	writeJSON(response, http.StatusOK, cancelMessageMoveTaskResponse{ApproximateNumberOfMessagesMoved: task.ApproximateNumberOfMessagesMoved, RequestID: requestID})
}

func (s *Server) listMessageMoveTasks(response http.ResponseWriter, request *http.Request, requestID string) {
	body, ok := s.readActionBody(response, request, requestID)
	if !ok {
		return
	}
	var input listMessageMoveTasksRequest
	if err := decodeStrictRequest(body, &input); err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	if containsJSONNull(body, "SourceArn", "MaxResults") {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "request fields must not be null", requestID)
		return
	}
	source, err := queue.ParseQueueARN(input.SourceARN)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", err.Error(), requestID)
		return
	}
	tasks, err := s.queueService.ListMessageMoveTasks(queue.ListMessageMoveTasksInput{Source: source, MaxResults: input.MaxResults})
	if err != nil {
		s.writeRedriveError(response, err, requestID)
		return
	}
	results := make([]messageMoveTaskResponse, len(tasks))
	for index, task := range tasks {
		value := messageMoveTaskResponse{TaskHandle: task.Handle, SourceARN: queue.QueueARN(task.Source), MaxNumberOfMessagesPerSecond: task.MaxNumberOfMessagesPerSecond, StartedTimestamp: task.StartedAtMillis, Status: string(task.Status), FailureReason: task.FailureReason, ApproximateNumberOfMessagesMoved: task.ApproximateNumberOfMessagesMoved, ApproximateNumberOfMessagesToMove: task.ApproximateNumberOfMessagesToMove}
		if task.Destination != nil {
			value.DestinationARN = queue.QueueARN(*task.Destination)
		}
		results[index] = value
	}
	writeJSON(response, http.StatusOK, listMessageMoveTasksResponse{Results: results, RequestID: requestID})
}

func (s *Server) writeRedriveError(response http.ResponseWriter, err error, requestID string) {
	var invalid *queue.InvalidRequestError
	switch {
	case errors.Is(err, queue.ErrQueueDoesNotExist):
		writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
	case errors.Is(err, queue.ErrInvalidPaginationToken):
		writeError(response, http.StatusBadRequest, "InvalidPaginationToken", "The pagination token is invalid or expired.", requestID)
	case errors.Is(err, queue.ErrQueueIsNotDeadLetter):
		writeError(response, http.StatusBadRequest, "UnsupportedOperation", "The source queue is not configured as a dead-letter queue.", requestID)
	case errors.Is(err, queue.ErrMoveTaskAlreadyRunning):
		writeError(response, http.StatusConflict, "MessageMoveTaskAlreadyRunning", "A message move task is already running for this source.", requestID)
	case errors.Is(err, queue.ErrMoveTaskNotRunning):
		writeError(response, http.StatusConflict, "ResourceNotInUse", "The message move task is not running.", requestID)
	case errors.Is(err, queue.ErrMoveTaskDoesNotExist):
		writeError(response, http.StatusNotFound, "ResourceNotFound", "The message move task does not exist.", requestID)
	case errors.As(err, &invalid):
		writeError(response, http.StatusBadRequest, "InvalidRequest", invalid.Error(), requestID)
	case errors.Is(err, queue.ErrRepositoryUnavailable):
		writeStorageUnavailable(response, requestID)
	default:
		writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
	}
}

func containsJSONNull(body []byte, names ...string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	for _, name := range names {
		if raw, present := fields[name]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return true
		}
	}
	return false
}

func decodeStrictRequest(body []byte, destination any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return fmt.Errorf("invalid JSON request: %v", err)
	}
	if bytes.TrimSpace(body)[0] != '{' {
		return fmt.Errorf("the request body must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON request: %v", err)
	}
	return nil
}

func (s *Server) writeQueueActionError(response http.ResponseWriter, err error, requestID string) {
	if errors.Is(err, queue.ErrQuotaExceeded) {
		writeQuotaExceeded(response, requestID)
		return
	}
	if errors.Is(err, queue.ErrQueueDoesNotExist) {
		writeError(response, http.StatusNotFound, "QueueDoesNotExist", "The specified queue does not exist.", requestID)
		return
	}
	var invalidRequest *queue.InvalidRequestError
	if errors.As(err, &invalidRequest) {
		writeError(response, http.StatusBadRequest, "InvalidRequest", invalidRequest.Error(), requestID)
		return
	}
	if errors.Is(err, queue.ErrRepositoryUnavailable) {
		writeStorageUnavailable(response, requestID)
		return
	}
	writeError(response, http.StatusInternalServerError, "InternalError", "The request could not be completed.", requestID)
}

func writeStorageUnavailable(response http.ResponseWriter, requestID string) {
	writeError(response, http.StatusServiceUnavailable, "ServiceUnavailable", "Durable storage is unavailable.", requestID)
}

func writeQuotaExceeded(response http.ResponseWriter, requestID string) {
	writeError(response, http.StatusTooManyRequests, "QuotaExceeded", "The tenant quota is exceeded.", requestID)
}

func (s *Server) readActionBody(response http.ResponseWriter, request *http.Request, requestID string) ([]byte, bool) {
	if !hasJSONContentType(request.Header.Get("Content-Type")) {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "Content-Type must be application/json.", requestID)
		return nil, false
	}

	if request.ContentLength > s.maxBodyBytes {
		writeError(response, http.StatusRequestEntityTooLarge, "RequestTooLarge", "The request body is too large.", requestID)
		return nil, false
	}
	bodyReader := &io.LimitedReader{R: request.Body, N: s.maxBodyBytes + 1}
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "The request body could not be read.", requestID)
		return nil, false
	}
	if int64(len(body)) > s.maxBodyBytes {
		writeError(response, http.StatusRequestEntityTooLarge, "RequestTooLarge", "The request body is too large.", requestID)
		return nil, false
	}
	if !utf8.Valid(body) {
		writeError(response, http.StatusBadRequest, "InvalidRequest", "The request body must be valid UTF-8.", requestID)
		return nil, false
	}
	return body, true
}

func (s *Server) queueURL(value queue.Queue) string {
	return s.publicBaseURL + "/queues/" + value.Name + "/" + value.ID
}

func (s *Server) queueRefFromURL(queueURL string) (queue.QueueRef, error) {
	prefix := s.publicBaseURL + "/queues/"
	if !strings.HasPrefix(queueURL, prefix) {
		return queue.QueueRef{}, fmt.Errorf("QueueUrl is not a SimQ queue URL")
	}
	parts := strings.Split(strings.TrimPrefix(queueURL, prefix), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" || len(parts) == 2 && parts[1] == "" {
		return queue.QueueRef{}, fmt.Errorf("QueueUrl is not a SimQ queue URL")
	}
	ref := queue.QueueRef{Name: parts[0]}
	if len(parts) == 2 {
		ref.ID = parts[1]
	}
	return ref, nil
}

func (s *Server) nextRequestID() string {
	requestID := s.requestIDGenerator()
	if requestID == "" {
		return randomRequestID()
	}
	return requestID
}

func randomRequestID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "request-id-unavailable"
	}
	return hex.EncodeToString(value)
}

func fifoSequenceString(value uint64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatUint(value, 10)
}

func hasJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}

func decodeCreateQueueRequest(body []byte) (queue.CreateQueueInput, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return queue.CreateQueueInput{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return queue.CreateQueueInput{}, fmt.Errorf("invalid JSON request: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return queue.CreateQueueInput{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueName" && name != "Attributes" {
			return queue.CreateQueueInput{}, fmt.Errorf("unknown field %q", name)
		}
	}

	var input queue.CreateQueueInput
	if rawName, ok := fields["QueueName"]; ok {
		name, err := decodeJSONString(rawName)
		if err != nil {
			return queue.CreateQueueInput{}, fmt.Errorf("QueueName must be a string")
		}
		input.QueueName = name
	}

	if rawAttributes, ok := fields["Attributes"]; ok {
		trimmed := bytes.TrimSpace(rawAttributes)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return queue.CreateQueueInput{}, fmt.Errorf("Attributes must be an object")
		}
		var attributes map[string]json.RawMessage
		if err := json.Unmarshal(rawAttributes, &attributes); err != nil || attributes == nil {
			return queue.CreateQueueInput{}, fmt.Errorf("Attributes must be an object")
		}
		input.Attributes = make(map[string]string, len(attributes))
		for name, rawValue := range attributes {
			value, err := decodeJSONString(rawValue)
			if err != nil {
				return queue.CreateQueueInput{}, fmt.Errorf("queue attribute %q must be a string", name)
			}
			input.Attributes[name] = value
		}
	}

	return input, nil
}

func decodeGetQueueRequest(body []byte) (queue.GetQueueInput, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return queue.GetQueueInput{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return queue.GetQueueInput{}, fmt.Errorf("invalid JSON request: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return queue.GetQueueInput{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueName" {
			return queue.GetQueueInput{}, fmt.Errorf("unknown field %q", name)
		}
	}

	var input queue.GetQueueInput
	if rawName, ok := fields["QueueName"]; ok {
		name, err := decodeJSONString(rawName)
		if err != nil {
			return queue.GetQueueInput{}, fmt.Errorf("QueueName must be a string")
		}
		input.QueueName = name
	}
	return input, nil
}

func decodeSendMessageRequest(body []byte) (sendMessageRequest, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return sendMessageRequest{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return sendMessageRequest{}, fmt.Errorf("invalid JSON request")
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return sendMessageRequest{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueUrl" && name != "MessageBody" && name != "DelaySeconds" && name != "MessageAttributes" && name != "MessageGroupId" && name != "MessageDeduplicationId" {
			return sendMessageRequest{}, fmt.Errorf("unknown field %q", name)
		}
	}

	var input sendMessageRequest
	if rawQueueURL, ok := fields["QueueUrl"]; ok {
		queueURL, err := decodeJSONString(rawQueueURL)
		if err != nil {
			return sendMessageRequest{}, fmt.Errorf("QueueUrl must be a string")
		}
		input.QueueURL = queueURL
	}
	if rawMessageBody, ok := fields["MessageBody"]; ok {
		messageBody, err := decodeJSONString(rawMessageBody)
		if err != nil {
			return sendMessageRequest{}, fmt.Errorf("MessageBody must be a string")
		}
		input.MessageBody = messageBody
	}
	if rawDelay, ok := fields["DelaySeconds"]; ok {
		delay, err := decodeJSONInt(rawDelay)
		if err != nil {
			return sendMessageRequest{}, fmt.Errorf("DelaySeconds must be an integer")
		}
		input.DelaySeconds = &delay
	}
	if rawAttributes, ok := fields["MessageAttributes"]; ok {
		attributes, err := decodeMessageAttributes(rawAttributes)
		if err != nil {
			return sendMessageRequest{}, err
		}
		input.MessageAttributes = attributes
	}
	if raw, ok := fields["MessageGroupId"]; ok {
		value, err := decodeJSONString(raw)
		if err != nil {
			return sendMessageRequest{}, fmt.Errorf("MessageGroupId must be a string")
		}
		if value == "" {
			return sendMessageRequest{}, fmt.Errorf("MessageGroupId must not be empty")
		}
		input.MessageGroupID = value
	}
	if raw, ok := fields["MessageDeduplicationId"]; ok {
		value, err := decodeJSONString(raw)
		if err != nil {
			return sendMessageRequest{}, fmt.Errorf("MessageDeduplicationId must be a string")
		}
		if value == "" {
			return sendMessageRequest{}, fmt.Errorf("MessageDeduplicationId must not be empty")
		}
		input.MessageDeduplicationID = value
	}
	return input, nil
}

func decodeBatchEnvelope(body []byte) (string, []json.RawMessage, error) {
	fields, err := decodeActionObject(body, "QueueUrl", "Entries")
	if err != nil {
		return "", nil, err
	}
	rawURL, ok := fields["QueueUrl"]
	if !ok {
		return "", nil, fmt.Errorf("QueueUrl is required")
	}
	queueURL, err := decodeJSONString(rawURL)
	if err != nil {
		return "", nil, fmt.Errorf("QueueUrl must be a string")
	}
	rawEntries, ok := fields["Entries"]
	if !ok {
		return "", nil, fmt.Errorf("Entries is required")
	}
	trimmed := bytes.TrimSpace(rawEntries)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return "", nil, fmt.Errorf("Entries must be an array")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(trimmed, &entries); err != nil || entries == nil {
		return "", nil, fmt.Errorf("Entries must be an array")
	}
	return queueURL, entries, nil
}

func decodeSendMessageBatchRequest(body []byte) (sendMessageBatchRequest, error) {
	queueURL, rawEntries, err := decodeBatchEnvelope(body)
	if err != nil {
		return sendMessageBatchRequest{}, err
	}
	result := sendMessageBatchRequest{QueueURL: queueURL, Entries: make([]queue.SendMessageBatchEntry, len(rawEntries))}
	for index, raw := range rawEntries {
		fields, err := decodeActionObject(raw, "Id", "MessageBody", "DelaySeconds", "MessageAttributes", "MessageGroupId", "MessageDeduplicationId")
		if err != nil {
			return sendMessageBatchRequest{}, fmt.Errorf("entry %d: %v", index, err)
		}
		idRaw, ok := fields["Id"]
		if !ok {
			return sendMessageBatchRequest{}, fmt.Errorf("entry %d: Id is required", index)
		}
		id, err := decodeJSONString(idRaw)
		if err != nil {
			return sendMessageBatchRequest{}, fmt.Errorf("entry %d: Id must be a string", index)
		}
		bodyRaw, ok := fields["MessageBody"]
		if !ok {
			return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageBody is required", index)
		}
		messageBody, err := decodeJSONString(bodyRaw)
		if err != nil {
			return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageBody must be a string", index)
		}
		entry := queue.SendMessageBatchEntry{ID: id, MessageBody: messageBody}
		if raw, ok := fields["DelaySeconds"]; ok {
			value, err := decodeJSONInt(raw)
			if err != nil {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: DelaySeconds must be an integer", index)
			}
			entry.DelaySeconds = &value
		}
		if raw, ok := fields["MessageAttributes"]; ok {
			entry.MessageAttributes, err = decodeMessageAttributes(raw)
			if err != nil {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: %v", index, err)
			}
		}
		if raw, ok := fields["MessageGroupId"]; ok {
			entry.MessageGroupID, err = decodeJSONString(raw)
			if err != nil {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageGroupId must be a string", index)
			}
			if entry.MessageGroupID == "" {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageGroupId must not be empty", index)
			}
		}
		if raw, ok := fields["MessageDeduplicationId"]; ok {
			entry.MessageDeduplicationID, err = decodeJSONString(raw)
			if err != nil {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageDeduplicationId must be a string", index)
			}
			if entry.MessageDeduplicationID == "" {
				return sendMessageBatchRequest{}, fmt.Errorf("entry %d: MessageDeduplicationId must not be empty", index)
			}
		}
		result.Entries[index] = entry
	}
	return result, nil
}

func decodeDeleteMessageBatchRequest(body []byte) (deleteMessageBatchRequest, error) {
	queueURL, rawEntries, err := decodeBatchEnvelope(body)
	if err != nil {
		return deleteMessageBatchRequest{}, err
	}
	result := deleteMessageBatchRequest{QueueURL: queueURL, Entries: make([]queue.DeleteMessageBatchEntry, len(rawEntries))}
	for index, raw := range rawEntries {
		fields, err := decodeActionObject(raw, "Id", "ReceiptHandle")
		if err != nil {
			return deleteMessageBatchRequest{}, fmt.Errorf("entry %d: %v", index, err)
		}
		idRaw, ok := fields["Id"]
		if !ok {
			return deleteMessageBatchRequest{}, fmt.Errorf("entry %d: Id is required", index)
		}
		id, err := decodeJSONString(idRaw)
		if err != nil {
			return deleteMessageBatchRequest{}, fmt.Errorf("entry %d: Id must be a string", index)
		}
		receiptRaw, ok := fields["ReceiptHandle"]
		if !ok {
			return deleteMessageBatchRequest{}, fmt.Errorf("entry %d: ReceiptHandle is required", index)
		}
		receipt, err := decodeJSONString(receiptRaw)
		if err != nil {
			return deleteMessageBatchRequest{}, fmt.Errorf("entry %d: ReceiptHandle must be a string", index)
		}
		result.Entries[index] = queue.DeleteMessageBatchEntry{ID: id, ReceiptHandle: receipt}
	}
	return result, nil
}

func decodeChangeMessageVisibilityBatchRequest(body []byte) (changeMessageVisibilityBatchRequest, error) {
	queueURL, rawEntries, err := decodeBatchEnvelope(body)
	if err != nil {
		return changeMessageVisibilityBatchRequest{}, err
	}
	result := changeMessageVisibilityBatchRequest{QueueURL: queueURL, Entries: make([]queue.ChangeMessageVisibilityBatchEntry, len(rawEntries))}
	for index, raw := range rawEntries {
		fields, err := decodeActionObject(raw, "Id", "ReceiptHandle", "VisibilityTimeout")
		if err != nil {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: %v", index, err)
		}
		idRaw, ok := fields["Id"]
		if !ok {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: Id is required", index)
		}
		id, err := decodeJSONString(idRaw)
		if err != nil {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: Id must be a string", index)
		}
		receiptRaw, ok := fields["ReceiptHandle"]
		if !ok {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: ReceiptHandle is required", index)
		}
		receipt, err := decodeJSONString(receiptRaw)
		if err != nil {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: ReceiptHandle must be a string", index)
		}
		visibilityRaw, ok := fields["VisibilityTimeout"]
		if !ok {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: VisibilityTimeout is required", index)
		}
		visibility, err := decodeJSONInt(visibilityRaw)
		if err != nil {
			return changeMessageVisibilityBatchRequest{}, fmt.Errorf("entry %d: VisibilityTimeout must be an integer", index)
		}
		result.Entries[index] = queue.ChangeMessageVisibilityBatchEntry{ID: id, ReceiptHandle: receipt, VisibilityTimeout: visibility}
	}
	return result, nil
}

func decodeReceiveMessageRequest(body []byte) (receiveMessageRequest, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return receiveMessageRequest{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return receiveMessageRequest{}, fmt.Errorf("invalid JSON request: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return receiveMessageRequest{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueUrl" && name != "MaxNumberOfMessages" && name != "VisibilityTimeout" && name != "WaitTimeSeconds" && name != "MessageAttributeNames" && name != "ReceiveRequestAttemptId" {
			return receiveMessageRequest{}, fmt.Errorf("unknown field %q", name)
		}
	}

	var input receiveMessageRequest
	if rawQueueURL, ok := fields["QueueUrl"]; ok {
		queueURL, err := decodeJSONString(rawQueueURL)
		if err != nil {
			return receiveMessageRequest{}, fmt.Errorf("QueueUrl must be a string")
		}
		input.QueueURL = queueURL
	}
	if rawMaximum, ok := fields["MaxNumberOfMessages"]; ok {
		maximum, err := decodeJSONInt(rawMaximum)
		if err != nil {
			return receiveMessageRequest{}, fmt.Errorf("MaxNumberOfMessages must be an integer")
		}
		input.MaxNumberOfMessages = &maximum
	}
	if rawVisibility, ok := fields["VisibilityTimeout"]; ok {
		visibility, err := decodeJSONInt(rawVisibility)
		if err != nil {
			return receiveMessageRequest{}, fmt.Errorf("VisibilityTimeout must be an integer")
		}
		input.VisibilityTimeout = &visibility
	}
	if rawWait, ok := fields["WaitTimeSeconds"]; ok {
		wait, err := decodeJSONInt(rawWait)
		if err != nil {
			return receiveMessageRequest{}, fmt.Errorf("WaitTimeSeconds must be an integer")
		}
		input.WaitTimeSeconds = &wait
	}
	if rawNames, ok := fields["MessageAttributeNames"]; ok {
		names, err := decodeStringArray(rawNames, "MessageAttributeNames")
		if err != nil {
			return receiveMessageRequest{}, err
		}
		input.MessageAttributeNames = names
	}
	if raw, ok := fields["ReceiveRequestAttemptId"]; ok {
		value, err := decodeJSONString(raw)
		if err != nil {
			return receiveMessageRequest{}, fmt.Errorf("ReceiveRequestAttemptId must be a string")
		}
		if value == "" {
			return receiveMessageRequest{}, fmt.Errorf("ReceiveRequestAttemptId must not be empty")
		}
		input.ReceiveRequestAttemptID = value
	}
	return input, nil
}

func decodeDeleteMessageRequest(body []byte) (deleteMessageRequest, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return deleteMessageRequest{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return deleteMessageRequest{}, fmt.Errorf("invalid JSON request: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return deleteMessageRequest{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueUrl" && name != "ReceiptHandle" {
			return deleteMessageRequest{}, fmt.Errorf("unknown field %q", name)
		}
	}

	var input deleteMessageRequest
	if rawQueueURL, ok := fields["QueueUrl"]; ok {
		queueURL, err := decodeJSONString(rawQueueURL)
		if err != nil {
			return deleteMessageRequest{}, fmt.Errorf("QueueUrl must be a string")
		}
		input.QueueURL = queueURL
	}
	if rawReceiptHandle, ok := fields["ReceiptHandle"]; ok {
		receiptHandle, err := decodeJSONString(rawReceiptHandle)
		if err != nil {
			return deleteMessageRequest{}, fmt.Errorf("ReceiptHandle must be a string")
		}
		input.ReceiptHandle = receiptHandle
	}
	return input, nil
}

func decodeChangeMessageVisibilityRequest(body []byte) (changeMessageVisibilityRequest, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return changeMessageVisibilityRequest{}, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return changeMessageVisibilityRequest{}, fmt.Errorf("invalid JSON request: %v", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return changeMessageVisibilityRequest{}, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		if name != "QueueUrl" && name != "ReceiptHandle" && name != "VisibilityTimeout" {
			return changeMessageVisibilityRequest{}, fmt.Errorf("unknown field %q", name)
		}
	}

	rawQueueURL, ok := fields["QueueUrl"]
	if !ok {
		return changeMessageVisibilityRequest{}, fmt.Errorf("QueueUrl is required")
	}
	queueURL, err := decodeJSONString(rawQueueURL)
	if err != nil {
		return changeMessageVisibilityRequest{}, fmt.Errorf("QueueUrl must be a string")
	}
	rawReceiptHandle, ok := fields["ReceiptHandle"]
	if !ok {
		return changeMessageVisibilityRequest{}, fmt.Errorf("ReceiptHandle is required")
	}
	receiptHandle, err := decodeJSONString(rawReceiptHandle)
	if err != nil {
		return changeMessageVisibilityRequest{}, fmt.Errorf("ReceiptHandle must be a string")
	}
	rawVisibility, ok := fields["VisibilityTimeout"]
	if !ok {
		return changeMessageVisibilityRequest{}, fmt.Errorf("VisibilityTimeout is required")
	}
	visibility, err := decodeJSONInt(rawVisibility)
	if err != nil {
		return changeMessageVisibilityRequest{}, fmt.Errorf("VisibilityTimeout must be an integer")
	}

	return changeMessageVisibilityRequest{
		QueueURL:          queueURL,
		ReceiptHandle:     receiptHandle,
		VisibilityTimeout: visibility,
	}, nil
}

func decodeGetQueueAttributesRequest(body []byte) (getQueueAttributesRequest, error) {
	fields, err := decodeActionObject(body, "QueueUrl", "AttributeNames")
	if err != nil {
		return getQueueAttributesRequest{}, err
	}
	var input getQueueAttributesRequest
	if raw, ok := fields["QueueUrl"]; ok {
		input.QueueURL, err = decodeJSONString(raw)
		if err != nil {
			return getQueueAttributesRequest{}, fmt.Errorf("QueueUrl must be a string")
		}
	}
	if raw, ok := fields["AttributeNames"]; ok {
		input.AttributeNames, err = decodeStringArray(raw, "AttributeNames")
		if err != nil {
			return getQueueAttributesRequest{}, err
		}
	}
	return input, nil
}

func decodeSetQueueAttributesRequest(body []byte) (setQueueAttributesRequest, error) {
	fields, err := decodeActionObject(body, "QueueUrl", "Attributes")
	if err != nil {
		return setQueueAttributesRequest{}, err
	}
	var input setQueueAttributesRequest
	if raw, ok := fields["QueueUrl"]; ok {
		input.QueueURL, err = decodeJSONString(raw)
		if err != nil {
			return setQueueAttributesRequest{}, fmt.Errorf("QueueUrl must be a string")
		}
	}
	rawAttributes, ok := fields["Attributes"]
	if !ok {
		return setQueueAttributesRequest{}, fmt.Errorf("Attributes is required")
	}
	trimmed := bytes.TrimSpace(rawAttributes)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return setQueueAttributesRequest{}, fmt.Errorf("Attributes must be an object")
	}
	var rawValues map[string]json.RawMessage
	if err := json.Unmarshal(rawAttributes, &rawValues); err != nil || rawValues == nil {
		return setQueueAttributesRequest{}, fmt.Errorf("Attributes must be an object")
	}
	input.Attributes = make(map[string]string, len(rawValues))
	for name, raw := range rawValues {
		value, err := decodeJSONString(raw)
		if err != nil {
			return setQueueAttributesRequest{}, fmt.Errorf("queue attribute %q must be a string", name)
		}
		input.Attributes[name] = value
	}
	return input, nil
}

func decodeActionObject(body []byte, allowed ...string) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("the request body must contain a JSON object")
	}
	if err := validateJSONDocument(body); err != nil {
		return nil, fmt.Errorf("invalid JSON request: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("the request body must be a JSON object")
	}
	for name := range fields {
		known := false
		for _, candidate := range allowed {
			if name == candidate {
				known = true
				break
			}
		}
		if !known {
			return nil, fmt.Errorf("unknown field %q", name)
		}
	}
	return fields, nil
}

func decodeStringArray(raw json.RawMessage, field string) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, fmt.Errorf("%s must be an array of strings", field)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(trimmed, &values); err != nil || values == nil {
		return nil, fmt.Errorf("%s must be an array of strings", field)
	}
	result := make([]string, len(values))
	for index := range values {
		value, err := decodeJSONString(values[index])
		if err != nil {
			return nil, fmt.Errorf("%s must be an array of strings", field)
		}
		result[index] = value
	}
	return result, nil
}

func decodeMessageAttributes(raw json.RawMessage) (map[string]queue.MessageAttribute, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("MessageAttributes must be an object")
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &values); err != nil || values == nil {
		return nil, fmt.Errorf("MessageAttributes must be an object")
	}
	result := make(map[string]queue.MessageAttribute, len(values))
	for name, rawValue := range values {
		valueBytes := bytes.TrimSpace(rawValue)
		if len(valueBytes) == 0 || valueBytes[0] != '{' {
			return nil, fmt.Errorf("each message attribute must be an object")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(valueBytes, &fields); err != nil || fields == nil {
			return nil, fmt.Errorf("each message attribute must be an object")
		}
		for field := range fields {
			if field != "DataType" && field != "StringValue" && field != "BinaryValue" {
				return nil, fmt.Errorf("a message attribute contains an unknown field")
			}
		}
		var attribute queue.MessageAttribute
		if dataType, ok := fields["DataType"]; ok {
			value, err := decodeJSONString(dataType)
			if err != nil {
				return nil, fmt.Errorf("message attribute DataType must be a string")
			}
			attribute.DataType = value
		}
		if stringValue, ok := fields["StringValue"]; ok {
			value, err := decodeJSONString(stringValue)
			if err != nil {
				return nil, fmt.Errorf("message attribute StringValue must be a string")
			}
			attribute.StringValue = value
		}
		if binaryValue, ok := fields["BinaryValue"]; ok {
			encoded, err := decodeJSONString(binaryValue)
			if err != nil {
				return nil, fmt.Errorf("message attribute BinaryValue must be a base64 string")
			}
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				decoded, err = base64.RawStdEncoding.DecodeString(encoded)
			}
			if err != nil {
				return nil, fmt.Errorf("message attribute BinaryValue must be valid base64")
			}
			attribute.BinaryValue = decoded
		}
		result[name] = attribute
	}
	return result, nil
}

func decodeJSONString(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", fmt.Errorf("not a JSON string")
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeJSONInt(raw json.RawMessage) (int, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return 0, fmt.Errorf("not a JSON integer")
	}
	value, err := strconv.Atoi(string(trimmed))
	if err != nil {
		return 0, fmt.Errorf("not a JSON integer")
	}
	return value, nil
}

func validateJSONDocument(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object field name is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("invalid JSON object")
		}
		return nil
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("invalid JSON array")
		}
		return nil
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func isKnownAction(path string) bool {
	switch path {
	case "/v1/sqs/SendMessage",
		"/v1/sqs/SendMessageBatch",
		"/v1/sqs/ReceiveMessage",
		"/v1/sqs/DeleteMessage",
		"/v1/sqs/DeleteMessageBatch",
		"/v1/sqs/ChangeMessageVisibility",
		"/v1/sqs/ChangeMessageVisibilityBatch",
		"/v1/sqs/DeleteQueue",
		"/v1/sqs/PurgeQueue",
		"/v1/sqs/GetQueueUrl",
		"/v1/sqs/GetQueueAttributes",
		"/v1/sqs/SetQueueAttributes",
		"/v1/sqs/ListQueues",
		"/v1/sqs/TagQueue",
		"/v1/sqs/UntagQueue",
		"/v1/sqs/ListQueueTags",
		"/v1/sqs/AddPermission",
		"/v1/sqs/RemovePermission",
		"/v1/sqs/ListDeadLetterSourceQueues",
		"/v1/sqs/StartMessageMoveTask",
		"/v1/sqs/CancelMessageMoveTask",
		"/v1/sqs/ListMessageMoveTasks":
		return true
	default:
		return false
	}
}
