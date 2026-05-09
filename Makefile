# go-hbase top-level Makefile.
#
# Phase coverage:
#   T02 — go-build, go-test, go-lint, go-tidy.
#   Later tasks add: java-build, java-test, proto, deps, test-integration, ...
#
# Conventions:
#   * Targets that compile/test the Go side are prefixed `go-*`.
#   * Targets that compile/test the Java side are prefixed `java-*`.
#   * `make all` runs the full local check (currently Go only).

SHELL := bash

GO          ?= go
GOLANGCILINT?= golangci-lint
MVN         ?= mvn
MVN_FLAGS   ?= -B -ntp

GO_PKGS     := ./...
GO_BUILD_FLAGS := -trimpath

.PHONY: all
all: go-lint go-build go-test java-lint java-build java-test ## Run the full local check (Go + Java).

.PHONY: go-build
go-build: ## Compile every Go package.
	$(GO) build $(GO_BUILD_FLAGS) $(GO_PKGS)

.PHONY: go-test
go-test: ## Run Go tests with the race detector.
	$(GO) test -race -count=1 $(GO_PKGS)

.PHONY: go-lint
go-lint: ## Run golangci-lint on every Go package.
	$(GOLANGCILINT) run $(GO_PKGS)

.PHONY: go-tidy
go-tidy: ## Tidy the Go module file.
	$(GO) mod tidy

.PHONY: java-build
java-build: ## Compile Java sources.
	$(MVN) $(MVN_FLAGS) compile

.PHONY: java-test
java-test: ## Run Java tests + JaCoCo.
	$(MVN) $(MVN_FLAGS) verify

.PHONY: java-lint
java-lint: ## Check Java formatting (spotless).
	$(MVN) $(MVN_FLAGS) spotless:check

.PHONY: java-format
java-format: ## Apply Java auto-formatting.
	$(MVN) $(MVN_FLAGS) spotless:apply

.PHONY: check-structure
check-structure: ## Verify repo skeleton matches SPEC.md §4 (T01 verifier).
	./tools/check-structure.sh

.PHONY: clean
clean: ## Remove build artefacts.
	rm -rf bin/
	$(MVN) $(MVN_FLAGS) clean -q || true

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Available targets:\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
