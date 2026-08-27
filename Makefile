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
