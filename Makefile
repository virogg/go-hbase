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

GO_PKGS     := ./...
GO_BUILD_FLAGS := -trimpath

.PHONY: all
all: go-lint go-build go-test ## Run the local Go check (lint + build + test).

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

.PHONY: check-structure
check-structure: ## Verify repo skeleton matches SPEC.md §4 (T01 verifier).
	./tools/check-structure.sh

.PHONY: clean
clean: ## Remove build artefacts.
	rm -rf bin/

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Available targets:\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
