# go-hbase top-level Makefile.
#
# Conventions:
#   * Language-specific targets are prefixed `go-*` or `java-*`.
#   * Aggregate targets (`build`, `test`, `lint`, `deps`) run both sides.
#   * `make all` is the full local check: lint + build + test, Go and Java.

SHELL := bash

GO            ?= go
GOLANGCILINT  ?= golangci-lint
MVN           ?= mvn
MVN_FLAGS     ?= -B -ntp

GO_PKGS        := ./...
GO_BUILD_FLAGS := -trimpath

# ELF embedded into the bridge jar resources (T18). The Java supervisor
# extracts it from src/main/resources/bin/<os>-<arch>/ at startup.
# Linux x86-64 is the only target per SPEC.md.
GO_RUNTIME_OUT := src/main/resources/bin/linux-amd64/hbasecop-runtime

# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------

.PHONY: all
all: lint build test ## Run the full local check (Go + Java: lint + build + test).

.PHONY: build
build: go-build java-build ## Build Go binaries and Java jar.

.PHONY: test
test: go-test java-test ## Run all unit tests (Go race + JUnit + JaCoCo).

.PHONY: lint
lint: go-lint java-lint ## Run all linters/formatters in check mode.

.PHONY: deps
deps: deps-shmem go-deps java-deps ## Download all Go and Java dependencies (CI cache warm-up).

# ---------------------------------------------------------------------------
# Third-party (java-go-shmem)
# ---------------------------------------------------------------------------

SHMEM_SUBMODULE := third_party/java-go-shmem
SHMEM_POM       := $(SHMEM_SUBMODULE)/java/pom.xml

.PHONY: deps-shmem
deps-shmem: $(SHMEM_POM) ## Install the local java-go-shmem jar into ~/.m2 (no-op for Go: replace directive).
	$(MVN) $(MVN_FLAGS) install -DskipTests -f $(SHMEM_POM)

$(SHMEM_POM):
	@echo "$(SHMEM_SUBMODULE) is empty — run: git submodule update --init --recursive" >&2
	@exit 1

.PHONY: proto
proto: ## Regenerate protobuf for both languages (real wiring lands in T11).
	./tools/proto-gen.sh

# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------

.PHONY: go-build
go-build: go-build-runtime ## Compile every Go package and place the runtime ELF in resources.
	$(GO) build $(GO_BUILD_FLAGS) $(GO_PKGS)

.PHONY: go-build-runtime
go-build-runtime: ## Build cmd/hbasecop-runtime into the bridge jar resources (Linux x86-64).
	@mkdir -p $(dir $(GO_RUNTIME_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(GO_RUNTIME_OUT) ./cmd/hbasecop-runtime

.PHONY: go-test
go-test: ## Run Go tests with the race detector.
	$(GO) test -race -count=1 $(GO_PKGS)

.PHONY: go-lint
go-lint: ## Run golangci-lint on every Go package.
	$(GOLANGCILINT) run $(GO_PKGS)

.PHONY: go-tidy
go-tidy: ## Tidy the Go module file.
	$(GO) mod tidy

.PHONY: go-deps
go-deps: ## Download Go module deps to the local cache.
	$(GO) mod download

# ---------------------------------------------------------------------------
# Java
# ---------------------------------------------------------------------------

.PHONY: java-build
java-build: go-build-runtime ## Compile Java sources (after staging the Go runtime ELF).
	$(MVN) $(MVN_FLAGS) compile

.PHONY: java-test
java-test: go-build-runtime ## Run Java tests + JaCoCo + spotless (mvn verify).
	$(MVN) $(MVN_FLAGS) verify

.PHONY: java-lint
java-lint: ## Check Java formatting (spotless).
	$(MVN) $(MVN_FLAGS) spotless:check

.PHONY: java-format
java-format: ## Apply Java auto-formatting (spotless:apply).
	$(MVN) $(MVN_FLAGS) spotless:apply

.PHONY: java-deps
java-deps: ## Resolve Java deps to the local Maven cache (offline-prep for CI).
	$(MVN) $(MVN_FLAGS) dependency:go-offline

# ---------------------------------------------------------------------------
# End-to-end (T19): Java↔Go cross-language ping/pong with latency report.
# ---------------------------------------------------------------------------

.PHONY: test-e2e-ping
test-e2e-ping: go-build-runtime ## T19: spawn real Go runtime, run 10k PING/PONG, log latency.
	$(MVN) $(MVN_FLAGS) test -Dtest=PingPongE2ETest -DfailIfNoTests=false

.PHONY: demo-ping
demo-ping: test-e2e-ping ## Public demo alias for the T19 ping/pong run.

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

.PHONY: check-structure
check-structure: ## Verify repo skeleton matches SPEC.md §4 (T01 verifier).
	./tools/check-structure.sh

.PHONY: ci-lint
ci-lint: ## Lint GitHub Actions workflows with actionlint.
	actionlint .github/workflows/*.yml

.PHONY: license-check
license-check: ## Verify Apache 2.0 SPDX header on every source file.
	./tools/license-header.sh --check

.PHONY: license-fix
license-fix: ## Insert missing Apache 2.0 SPDX headers in place.
	./tools/license-header.sh --fix

.PHONY: clean
clean: ## Remove build artefacts.
	rm -rf bin/
	$(MVN) $(MVN_FLAGS) clean -q || true

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Available targets:\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
