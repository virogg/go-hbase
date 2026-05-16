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

# T25: per-example coproc-jar staging — each example ships its own Go ELF
# at the same classpath path the bridge supervisor extracts from
# (bin/linux-amd64/hbasecop-runtime).
COUNTER_OBSERVER_DIR := examples/counter-observer
COUNTER_OBSERVER_OUT := $(COUNTER_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

FAULT_OBSERVER_DIR := examples/fault-observer
FAULT_OBSERVER_OUT := $(FAULT_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

FILTER_OBSERVER_DIR := examples/filter-observer
FILTER_OBSERVER_OUT := $(FILTER_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

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
# Examples (T25): counter-observer reference coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-counter
go-build-counter: ## T25: build counter-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(COUNTER_OBSERVER_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(COUNTER_OBSERVER_OUT) ./$(COUNTER_OBSERVER_DIR)

.PHONY: counter-observer-jar
counter-observer-jar: go-build-counter ## T25: build deployable coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(COUNTER_OBSERVER_DIR)/pom.xml package
	@unzip -l $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from counter-observer.jar" >&2; exit 1; }
	@unzip -l $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/counter/CounterRegionObserver.class' || \
	  { echo "ERROR: CounterRegionObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: counter-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T36): fault-injection coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-fault
go-build-fault: ## T36: build fault-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(FAULT_OBSERVER_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(FAULT_OBSERVER_OUT) ./$(FAULT_OBSERVER_DIR)

.PHONY: fault-observer-jar
fault-observer-jar: go-build-fault ## T36: build fault-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(FAULT_OBSERVER_DIR)/pom.xml package
	@unzip -l $(FAULT_OBSERVER_DIR)/target/fault-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from fault-observer.jar" >&2; exit 1; }
	@unzip -l $(FAULT_OBSERVER_DIR)/target/fault-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/fault/FaultRegionObserver.class' || \
	  { echo "ERROR: FaultRegionObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(FAULT_OBSERVER_DIR)/target/fault-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: fault-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T43): read-path filter coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-filter
go-build-filter: ## T43: build filter-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(FILTER_OBSERVER_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(FILTER_OBSERVER_OUT) ./$(FILTER_OBSERVER_DIR)

.PHONY: filter-observer-jar
filter-observer-jar: go-build-filter ## T43: build filter-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(FILTER_OBSERVER_DIR)/pom.xml package
	@unzip -l $(FILTER_OBSERVER_DIR)/target/filter-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from filter-observer.jar" >&2; exit 1; }
	@unzip -l $(FILTER_OBSERVER_DIR)/target/filter-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/filter/FilterRegionObserver.class' || \
	  { echo "ERROR: FilterRegionObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(FILTER_OBSERVER_DIR)/target/filter-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: filter-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Integration (T26): HBase 2.5 standalone dev cluster.
# ---------------------------------------------------------------------------

HBASE_COMPOSE     := test/integration/docker-compose.yml
HBASE_COMPOSE_CMD := docker compose -f $(HBASE_COMPOSE)

.PHONY: hbase-up
hbase-up: ## T26: start HBase 2.5 standalone dev cluster, wait for master web UI.
	@mkdir -p test/integration/coproc-jars
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh

.PHONY: hbase-down
hbase-down: ## T26: stop and remove the HBase dev cluster (preserves built image).
	$(HBASE_COMPOSE_CMD) down

.PHONY: hbase-clean
hbase-clean: ## T26: stop, remove volumes, and drop the locally-built image.
	$(HBASE_COMPOSE_CMD) down -v --rmi local

.PHONY: hbase-logs
hbase-logs: ## T26: tail HBase dev cluster logs.
	$(HBASE_COMPOSE_CMD) logs -f hbase

.PHONY: hbase-status
hbase-status: ## T26: curl the master-status page (smoke check post-up).
	curl -fsS -o /dev/null -w 'master-status: %{http_code}\n' \
		http://localhost:16010/master-status

# ---------------------------------------------------------------------------
# Integration (T27): live PrePut → Go-observer counter on real HBase.
# ---------------------------------------------------------------------------

COPROC_JAR_STAGED := test/integration/coproc-jars/counter-observer.jar
FAULT_COPROC_JAR_STAGED := test/integration/coproc-jars/fault-observer.jar
FILTER_COPROC_JAR_STAGED := test/integration/coproc-jars/filter-observer.jar

.PHONY: demo-counter
demo-counter: ## CP-γ: public demo — Put on HBase triggers Go observer counter; leaves cluster up.
	./tools/demo-counter.sh

.PHONY: test-integration
test-integration: counter-observer-jar ## T27: full IT — bring up HBase, run PrePutCounterIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar $(COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=PrePutCounterIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T36): fault-injection matrix on real HBase.
# ---------------------------------------------------------------------------

.PHONY: test-fault
test-fault: fault-observer-jar ## T36: full IT — bring up HBase, run FaultMatrixIT (10 cases), tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(FAULT_OBSERVER_DIR)/target/fault-observer.jar $(FAULT_COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=FaultMatrixIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-fault.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T43): read-path hooks (preGetOp, preScannerOpen, preScannerNext)
# verified end-to-end on real HBase via the filter-observer coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: test-integration-read
test-integration-read: filter-observer-jar ## T43: full IT — bring up HBase, run ReadPathFilterIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(FILTER_OBSERVER_DIR)/target/filter-observer.jar $(FILTER_COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=ReadPathFilterIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-read.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T44): batch hooks (preBatchMutate) verified end-to-end with
# partial-block semantics via Table.batch on the filter-observer coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: test-integration-batch
test-integration-batch: filter-observer-jar ## T44: full IT — bring up HBase, run BatchPartialBlockIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(FILTER_OBSERVER_DIR)/target/filter-observer.jar $(FILTER_COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=BatchPartialBlockIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-batch.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

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
