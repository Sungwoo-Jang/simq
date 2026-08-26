package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"simq/internal/api"
	"simq/internal/auth"
	"simq/internal/cluster"
	"simq/internal/observability"
	"simq/internal/queue"
	"simq/internal/shard"
	"simq/internal/storage/boltrepo"
	"simq/internal/tenant"
)

const (
	defaultMaxBodyBytes           int64 = 2 << 20
	defaultStorage                      = "bbolt"
	defaultDataPath                     = "./data/simq.db"
	defaultBboltOpenTimeout             = time.Second
	defaultRetentionSweepInterval       = time.Minute
	gracefulShutdownTimeout             = 10 * time.Second
)

type runtimeConfig struct {
	address                 string
	publicBaseURL           string
	maxBodyBytes            int64
	storage                 string
	dataPath                string
	bboltOpenTimeout        time.Duration
	retentionSweepInterval  time.Duration
	clusterNodeID           string
	clusterBindAddress      string
	clusterAdvertiseAddress string
	clusterDataDir          string
	clusterBootstrap        bool
	clusterAPIURLs          map[string]string
	clusterInitialVoters    map[string]string
	clusterAdminToken       string
	shardManifestPath       string
	clusterTLSCertificate   string
	clusterTLSPrivateKey    string
	clusterTLSCA            string
	clusterTLSServerName    string
	securityMode            string
	oidcIssuer              string
	oidcAudience            string
	oidcTenantClaim         string
	oidcRolesClaim          string
	encryptionActiveKey     string
	encryptionKeys          map[string][]byte
	tlsCertificatePath      string
	tlsPrivateKeyPath       string
	legacyTenant            string
	storageQuota            queue.StorageQuota
	ratePerSecond           float64
	rateBurst               int
	metricsToken            string
	auditPath               string
}

func main() {
	logger := log.New(os.Stderr, "simq: ", log.LstdFlags)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger); err != nil {
		logger.Fatal(err)
	}
}

func run(ctx context.Context, logger *log.Logger) error {
	config, err := configFromEnvironment()
	if err != nil {
		return err
	}
	repository, err := openRepository(config)
	if err != nil {
		return err
	}
	var verifier auth.Verifier
	var metrics *observability.Metrics
	var limiter *observability.RateLimiter
	var auditor *observability.FileAuditor
	if config.securityMode == "oidc" {
		if config.storage != "bbolt" {
			return errors.Join(fmt.Errorf("OIDC security mode requires SIMQ_STORAGE=bbolt"), repository.Close())
		}
		quotaRepository, ok := repository.(queue.StorageQuotaRepository)
		if !ok {
			return errors.Join(fmt.Errorf("secure storage does not support authoritative quotas"), repository.Close())
		}
		quotaRepository.SetStorageQuota(config.storageQuota)
		payloadCipher, cipherErr := tenant.NewPayloadCipher(config.encryptionActiveKey, config.encryptionKeys)
		if cipherErr != nil {
			return errors.Join(cipherErr, repository.Close())
		}
		repository = tenant.New(repository, payloadCipher, config.legacyTenant)
		oidcVerifier, verifyErr := auth.NewOIDCVerifier(ctx, auth.Config{Issuer: config.oidcIssuer, Audience: config.oidcAudience, TenantClaim: config.oidcTenantClaim, RolesClaim: config.oidcRolesClaim})
		if verifyErr != nil {
			return errors.Join(fmt.Errorf("initialize OIDC: %w", verifyErr), repository.Close())
		}
		verifier = oidcVerifier
		metrics = observability.NewMetrics(1024)
		limiter = observability.NewRateLimiter(config.ratePerSecond, config.rateBurst, 10_000)
		openedAuditor, auditErr := observability.OpenFileAuditor(config.auditPath)
		if auditErr != nil {
			return errors.Join(fmt.Errorf("open audit sink: %w", auditErr), repository.Close())
		}
		auditor = openedAuditor
	}

	queueService := queue.NewService(repository)
	if config.clusterNodeID == "" {
		if err := queueService.ResumeMessageMoveTasks(); err != nil {
			return errors.Join(fmt.Errorf("resume message move tasks: %w", err), repository.Close())
		}
	}
	handler := api.NewServer(api.Config{
		PublicBaseURL:     config.publicBaseURL,
		MaxBodyBytes:      config.maxBodyBytes,
		ClusterAdminToken: config.clusterAdminToken,
		AuthVerifier:      verifier,
		RateLimiter:       limiter,
		Metrics:           metrics,
		MetricsToken:      config.metricsToken,
		Auditor:           auditor,
	}, queueService)
	server := &http.Server{
		Addr:              config.address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if config.securityMode == "oidc" {
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	registerLongPollShutdown(server, queueService)
	listener, err := net.Listen("tcp", config.address)
	if err != nil {
		return errors.Join(fmt.Errorf("listen on configured address: %w", err), repository.Close())
	}

	serveErrors := make(chan error, 1)
	go func() {
		if config.securityMode == "oidc" {
			serveErrors <- server.ServeTLS(listener, config.tlsCertificatePath, config.tlsPrivateKeyPath)
		} else {
			serveErrors <- server.Serve(listener)
		}
	}()
	sweeperContext, stopSweeper := context.WithCancel(ctx)
	if config.clusterNodeID != "" {
		go runClusterWorkerResumer(sweeperContext, queueService)
	}
	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		runRetentionSweeper(sweeperContext, logger, queueService, config.retentionSweepInterval)
	}()
	logger.Printf("listening on %s with public base URL %s and storage %s", config.address, config.publicBaseURL, config.storage)

	var serveError error
	var shutdownError error
	select {
	case serveError = <-serveErrors:
	case <-ctx.Done():
		logger.Print("shutdown requested")
		shutdownContext, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		shutdownError = server.Shutdown(shutdownContext)
		cancel()
		if shutdownError != nil {
			_ = server.Close()
		}
		serveError = <-serveErrors
	}
	if errors.Is(serveError, http.ErrServerClosed) {
		serveError = nil
	}
	stopSweeper()
	<-sweeperDone
	closeError := repository.Close()
	var auditCloseError error
	if auditor != nil {
		auditCloseError = auditor.Close()
	}
	return errors.Join(serveError, shutdownError, closeError, auditCloseError)
}

func runClusterWorkerResumer(ctx context.Context, service *queue.Service) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = service.ResumeMessageMoveTasks()
		}
	}
}

func registerLongPollShutdown(server *http.Server, service *queue.Service) {
	server.RegisterOnShutdown(service.ShutdownLongPolling)
}

func configFromEnvironment() (runtimeConfig, error) {
	config := runtimeConfig{
		address:                 environmentOrDefault("SIMQ_ADDR", ":9324"),
		publicBaseURL:           environmentOrDefault("SIMQ_PUBLIC_BASE_URL", "http://localhost:9324"),
		storage:                 environmentOrDefault("SIMQ_STORAGE", defaultStorage),
		dataPath:                environmentOrDefault("SIMQ_DATA_PATH", defaultDataPath),
		bboltOpenTimeout:        defaultBboltOpenTimeout,
		retentionSweepInterval:  defaultRetentionSweepInterval,
		clusterNodeID:           os.Getenv("SIMQ_CLUSTER_NODE_ID"),
		clusterBindAddress:      os.Getenv("SIMQ_CLUSTER_BIND"),
		clusterAdvertiseAddress: os.Getenv("SIMQ_CLUSTER_ADVERTISE"),
		clusterAPIURLs:          make(map[string]string),
		clusterInitialVoters:    make(map[string]string),
		clusterAdminToken:       os.Getenv("SIMQ_CLUSTER_ADMIN_TOKEN"),
		shardManifestPath:       os.Getenv("SIMQ_SHARD_MANIFEST"),
		clusterTLSCertificate:   os.Getenv("SIMQ_CLUSTER_TLS_CERT_FILE"),
		clusterTLSPrivateKey:    os.Getenv("SIMQ_CLUSTER_TLS_KEY_FILE"),
		clusterTLSCA:            os.Getenv("SIMQ_CLUSTER_TLS_CA_FILE"),
		clusterTLSServerName:    os.Getenv("SIMQ_CLUSTER_TLS_SERVER_NAME"),
		securityMode:            environmentOrDefault("SIMQ_SECURITY_MODE", "disabled"),
		oidcIssuer:              os.Getenv("SIMQ_OIDC_ISSUER"),
		oidcAudience:            os.Getenv("SIMQ_OIDC_AUDIENCE"),
		oidcTenantClaim:         environmentOrDefault("SIMQ_OIDC_TENANT_CLAIM", "simq_tenant"),
		oidcRolesClaim:          environmentOrDefault("SIMQ_OIDC_ROLES_CLAIM", "simq_roles"),
		encryptionActiveKey:     os.Getenv("SIMQ_ENCRYPTION_ACTIVE_KEY"),
		tlsCertificatePath:      os.Getenv("SIMQ_TLS_CERT_FILE"),
		tlsPrivateKeyPath:       os.Getenv("SIMQ_TLS_KEY_FILE"),
		legacyTenant:            os.Getenv("SIMQ_LEGACY_TENANT"),
		storageQuota:            queue.StorageQuota{MaxQueues: 10_000, MaxMessages: 1_000_000, MaxPayloadBytes: 10 << 30},
		ratePerSecond:           100,
		rateBurst:               200,
		metricsToken:            os.Getenv("SIMQ_METRICS_TOKEN"),
		auditPath:               os.Getenv("SIMQ_AUDIT_PATH"),
	}
	config.clusterDataDir = environmentOrDefault("SIMQ_CLUSTER_DATA_DIR", config.dataPath+".cluster")

	maxBodyBytes := os.Getenv("SIMQ_MAX_BODY_BYTES")
	if maxBodyBytes == "" {
		config.maxBodyBytes = defaultMaxBodyBytes
	} else {
		parsed, err := strconv.ParseInt(maxBodyBytes, 10, 64)
		if err != nil || parsed <= 0 {
			return runtimeConfig{}, fmt.Errorf("SIMQ_MAX_BODY_BYTES must be a positive decimal integer, got %q", maxBodyBytes)
		}
		config.maxBodyBytes = parsed
	}

	openTimeout := os.Getenv("SIMQ_BBOLT_OPEN_TIMEOUT")
	if openTimeout != "" {
		parsed, err := time.ParseDuration(openTimeout)
		if err != nil || parsed <= 0 {
			return runtimeConfig{}, fmt.Errorf("SIMQ_BBOLT_OPEN_TIMEOUT must be a positive Go duration, got %q", openTimeout)
		}
		config.bboltOpenTimeout = parsed
	}
	sweepInterval := os.Getenv("SIMQ_RETENTION_SWEEP_INTERVAL")
	if sweepInterval != "" {
		parsed, err := time.ParseDuration(sweepInterval)
		if err != nil || parsed <= 0 {
			return runtimeConfig{}, fmt.Errorf("SIMQ_RETENTION_SWEEP_INTERVAL must be a positive Go duration, got %q", sweepInterval)
		}
		config.retentionSweepInterval = parsed
	}
	if config.storage != "bbolt" && config.storage != "memory" {
		return runtimeConfig{}, fmt.Errorf("SIMQ_STORAGE must be bbolt or memory, got %q", config.storage)
	}
	if config.securityMode != "disabled" && config.securityMode != "oidc" {
		return runtimeConfig{}, fmt.Errorf("SIMQ_SECURITY_MODE must be disabled or oidc")
	}
	if config.securityMode == "oidc" {
		if config.oidcIssuer == "" || config.oidcAudience == "" {
			return runtimeConfig{}, fmt.Errorf("SIMQ_OIDC_ISSUER and SIMQ_OIDC_AUDIENCE are required in OIDC mode")
		}
		if config.legacyTenant == "" {
			return runtimeConfig{}, fmt.Errorf("SIMQ_LEGACY_TENANT is required to bind the pre-M6 namespace in OIDC mode")
		}
		if config.metricsToken == "" || config.auditPath == "" {
			return runtimeConfig{}, fmt.Errorf("SIMQ_METRICS_TOKEN and SIMQ_AUDIT_PATH are required in OIDC mode")
		}
		if raw := os.Getenv("SIMQ_TENANT_REQUESTS_PER_SECOND"); raw != "" {
			value, err := strconv.ParseFloat(raw, 64)
			if err != nil || value <= 0 {
				return runtimeConfig{}, fmt.Errorf("SIMQ_TENANT_REQUESTS_PER_SECOND must be positive")
			}
			config.ratePerSecond = value
		}
		if raw := os.Getenv("SIMQ_TENANT_REQUEST_BURST"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				return runtimeConfig{}, fmt.Errorf("SIMQ_TENANT_REQUEST_BURST must be positive")
			}
			config.rateBurst = value
		}
		limits := []struct {
			name   string
			target *uint64
		}{
			{"SIMQ_TENANT_MAX_QUEUES", &config.storageQuota.MaxQueues},
			{"SIMQ_TENANT_MAX_MESSAGES", &config.storageQuota.MaxMessages},
			{"SIMQ_TENANT_MAX_PAYLOAD_BYTES", &config.storageQuota.MaxPayloadBytes},
		}
		for _, limit := range limits {
			if raw := os.Getenv(limit.name); raw != "" {
				value, err := strconv.ParseUint(raw, 10, 64)
				if err != nil || value == 0 {
					return runtimeConfig{}, fmt.Errorf("%s must be a positive decimal integer", limit.name)
				}
				*limit.target = value
			}
		}
		if config.tlsCertificatePath == "" || config.tlsPrivateKeyPath == "" {
			return runtimeConfig{}, fmt.Errorf("SIMQ_TLS_CERT_FILE and SIMQ_TLS_KEY_FILE are required in OIDC mode")
		}
		keys, err := parseEncryptionKeys(os.Getenv("SIMQ_ENCRYPTION_KEYS"))
		if err != nil {
			return runtimeConfig{}, err
		}
		config.encryptionKeys = keys
		if config.encryptionActiveKey == "" || keys[config.encryptionActiveKey] == nil {
			return runtimeConfig{}, fmt.Errorf("SIMQ_ENCRYPTION_ACTIVE_KEY must select a configured encryption key")
		}
	}
	if config.shardManifestPath != "" && config.clusterNodeID == "" {
		return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_NODE_ID is required with SIMQ_SHARD_MANIFEST")
	}
	if config.clusterNodeID != "" {
		if config.storage != "bbolt" {
			return runtimeConfig{}, fmt.Errorf("cluster mode requires SIMQ_STORAGE=bbolt")
		}
		if config.shardManifestPath != "" {
			if config.securityMode != "oidc" {
				return runtimeConfig{}, fmt.Errorf("SIMQ_SHARD_MANIFEST requires SIMQ_SECURITY_MODE=oidc")
			}
			if config.clusterAdminToken == "" {
				return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_ADMIN_TOKEN is required with SIMQ_SHARD_MANIFEST")
			}
			if _, err := shard.LoadManifest(config.shardManifestPath, config.clusterNodeID); err != nil {
				return runtimeConfig{}, err
			}
		} else if config.clusterBindAddress == "" {
			return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_BIND is required when SIMQ_CLUSTER_NODE_ID is set")
		}
		clusterTLSConfigured := config.clusterTLSCertificate != "" || config.clusterTLSPrivateKey != "" || config.clusterTLSCA != "" || config.clusterTLSServerName != ""
		if clusterTLSConfigured && (config.clusterTLSCertificate == "" || config.clusterTLSPrivateKey == "" || config.clusterTLSCA == "") {
			return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_TLS_CERT_FILE, SIMQ_CLUSTER_TLS_KEY_FILE, and SIMQ_CLUSTER_TLS_CA_FILE must be configured together")
		}
		if config.shardManifestPath == "" && config.clusterAdvertiseAddress == "" {
			config.clusterAdvertiseAddress = config.clusterBindAddress
		}
		if raw := os.Getenv("SIMQ_CLUSTER_BOOTSTRAP"); raw != "" {
			value, err := strconv.ParseBool(raw)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_BOOTSTRAP must be true or false, got %q", raw)
			}
			config.clusterBootstrap = value
		}
		config.clusterAPIURLs[config.clusterNodeID] = config.publicBaseURL
		if raw := os.Getenv("SIMQ_CLUSTER_API_URLS"); raw != "" {
			for _, entry := range strings.Split(raw, ",") {
				parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
				if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
					return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_API_URLS must contain node=url pairs")
				}
				config.clusterAPIURLs[parts[0]] = strings.TrimRight(parts[1], "/")
			}
		}
		if raw := os.Getenv("SIMQ_CLUSTER_INITIAL_VOTERS"); raw != "" {
			for _, entry := range strings.Split(raw, ",") {
				parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
				if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
					return runtimeConfig{}, fmt.Errorf("SIMQ_CLUSTER_INITIAL_VOTERS must contain node=raft-address pairs")
				}
				config.clusterInitialVoters[parts[0]] = parts[1]
			}
		}
	}
	return config, nil
}

func parseEncryptionKeys(raw string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	if raw == "" {
		return result, fmt.Errorf("SIMQ_ENCRYPTION_KEYS is required in OIDC mode")
	}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("SIMQ_ENCRYPTION_KEYS must contain id=base64-key pairs")
		}
		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			decoded, err = base64.RawStdEncoding.DecodeString(parts[1])
		}
		if err != nil || len(decoded) != 32 {
			return nil, fmt.Errorf("encryption key %q must decode to exactly 32 bytes", parts[0])
		}
		if _, duplicate := result[parts[0]]; duplicate {
			return nil, fmt.Errorf("encryption key ID %q is duplicated", parts[0])
		}
		result[parts[0]] = decoded
	}
	return result, nil
}

type messageExpirer interface {
	ExpireMessages() (int, error)
}

func runRetentionSweeper(ctx context.Context, logger *log.Logger, expirer messageExpirer, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := expirer.ExpireMessages(); err != nil {
				logger.Print("retention sweep failed")
			}
		}
	}
}

func openRepository(config runtimeConfig) (queue.Repository, error) {
	if config.shardManifestPath != "" {
		manifest, err := shard.LoadManifest(config.shardManifestPath, config.clusterNodeID)
		if err != nil {
			return nil, err
		}
		repository, err := shard.OpenClustered(manifest, shard.ClusterNodeConfig{
			NodeID:             config.clusterNodeID,
			DataDir:            config.clusterDataDir,
			OpenTimeout:        config.bboltOpenTimeout,
			TLSCertificateFile: config.clusterTLSCertificate,
			TLSPrivateKeyFile:  config.clusterTLSPrivateKey,
			TLSCAFile:          config.clusterTLSCA,
			TLSServerName:      config.clusterTLSServerName,
		})
		if err != nil {
			return nil, fmt.Errorf("start sharded storage: %w", err)
		}
		return repository, nil
	}
	if config.clusterNodeID != "" {
		repository, err := cluster.Open(cluster.Config{
			NodeID:             config.clusterNodeID,
			BindAddress:        config.clusterBindAddress,
			AdvertiseAddress:   config.clusterAdvertiseAddress,
			DataDir:            config.clusterDataDir,
			Bootstrap:          config.clusterBootstrap,
			OpenTimeout:        config.bboltOpenTimeout,
			APIURLs:            config.clusterAPIURLs,
			InitialVoters:      config.clusterInitialVoters,
			TLSCertificateFile: config.clusterTLSCertificate,
			TLSPrivateKeyFile:  config.clusterTLSPrivateKey,
			TLSCAFile:          config.clusterTLSCA,
			TLSServerName:      config.clusterTLSServerName,
		})
		if err != nil {
			return nil, fmt.Errorf("start clustered storage: %w", err)
		}
		return repository, nil
	}
	switch config.storage {
	case "memory":
		return queue.NewMemoryRepository(), nil
	case "bbolt":
		repository, err := boltrepo.Open(boltrepo.Config{
			Path:        config.dataPath,
			OpenTimeout: config.bboltOpenTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("start durable storage: %w", err)
		}
		return repository, nil
	default:
		return nil, fmt.Errorf("unsupported storage %q", config.storage)
	}
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
