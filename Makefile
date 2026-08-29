.PHONY: test race verify

GO_TOOLCHAIN := go1.26.7
GO_CACHE := $(CURDIR)/.cache/go-build

test:
	GOCACHE=$(GO_CACHE) GOTOOLCHAIN=$(GO_TOOLCHAIN) go test ./...

race:
	GOCACHE=$(GO_CACHE) GOTOOLCHAIN=$(GO_TOOLCHAIN) go test -race ./...

verify:
	./scripts/verify.sh

.PHONY: integration
integration:
	./scripts/integration.sh

.PHONY: secure-integration
secure-integration:
	./scripts/secure-integration.sh

.PHONY: alerts
alerts:
	./scripts/alerts.sh

.PHONY: kind-integration
kind-integration:
	./scripts/kind-integration.sh

.PHONY: qualify-secure
qualify-secure:
	./scripts/qualify-secure.sh
