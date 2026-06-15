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

# T25: per-example coproc-jar staging - each example ships its own Go ELF
# at the same classpath path the bridge supervisor extracts from
# (bin/linux-amd64/hbasecop-runtime).
COUNTER_OBSERVER_DIR := examples/counter-observer
COUNTER_OBSERVER_OUT := $(COUNTER_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

FAULT_OBSERVER_DIR := examples/fault-observer
FAULT_OBSERVER_OUT := $(FAULT_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

FILTER_OBSERVER_DIR := examples/filter-observer
FILTER_OBSERVER_OUT := $(FILTER_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

MASTER_POLICY_DIR := examples/master-policy-observer
MASTER_POLICY_OUT := $(MASTER_POLICY_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

RS_POLICY_DIR := examples/rs-policy-observer
RS_POLICY_OUT := $(RS_POLICY_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

AUDIT_OBSERVER_DIR := examples/audit-observer
AUDIT_OBSERVER_OUT := $(AUDIT_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

TTL_VALIDATOR_DIR := examples/ttl-validator
TTL_VALIDATOR_OUT := $(TTL_VALIDATOR_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

WAL_OBSERVER_DIR := examples/wal-observer
WAL_OBSERVER_OUT := $(WAL_OBSERVER_DIR)/src/main/resources/bin/linux-amd64/hbasecop-runtime

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

# Recorded pin of the java-go-shmem submodule. This is the project's single
# most load-bearing dependency (the Java<->Go shared-memory IPC) yet it sits
# OUTSIDE the usual integrity nets: the Go side consumes it via a `replace`
# directive (so go.sum carries no checksum for it) and the Java side is a local
# SNAPSHOT (mutable). `verify-deps` is the in-repo backstop - it asserts the
# checked-out submodule is exactly this commit with no tracked-source drift, so
# a bump or tampering can't slip through unreviewed. Bumping the dependency means
# updating this SHA and docs/dep-shmem.md in the same change.
SHMEM_EXPECTED_SHA := ef35ad6d413899b4497aca191b9cc4dcca4f98bc

.PHONY: deps-shmem
deps-shmem: $(SHMEM_POM) ## Install the local java-go-shmem jar into ~/.m2 (no-op for Go: replace directive).
	$(MVN) $(MVN_FLAGS) install -DskipTests -f $(SHMEM_POM)

.PHONY: verify-deps
verify-deps: $(SHMEM_POM) ## Supply-chain guard: java-go-shmem submodule matches the recorded pin.
	@actual=$$(git -C $(SHMEM_SUBMODULE) rev-parse HEAD); \
	  if [ "$$actual" != "$(SHMEM_EXPECTED_SHA)" ]; then \
	    echo "FAIL: java-go-shmem at $$actual, expected pinned $(SHMEM_EXPECTED_SHA)." >&2; \
	    echo "      If intentional, update SHMEM_EXPECTED_SHA + docs/dep-shmem.md in this change." >&2; \
	    exit 1; \
	  fi
	@git -C $(SHMEM_SUBMODULE) diff --quiet HEAD || { \
	  echo "FAIL: java-go-shmem tracked sources modified vs the pinned commit (possible tampering)." >&2; \
	  exit 1; }
	@echo "verify-deps: java-go-shmem pinned at $(SHMEM_EXPECTED_SHA), tracked sources clean."

$(SHMEM_POM):
	@echo "$(SHMEM_SUBMODULE) is empty - run: git submodule update --init --recursive" >&2
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

.PHONY: hbasecop-build
hbasecop-build: ## T71: build the coproc-jar packaging CLI (host arch, target/bin/hbasecop-build).
	@mkdir -p target/bin
	$(GO) build $(GO_BUILD_FLAGS) -o target/bin/hbasecop-build ./cmd/hbasecop-build

.PHONY: go-test
go-test: ## Run Go tests with the race detector.
	$(GO) test -race -count=1 $(GO_PKGS)

# The POSIX named-shm backend (SPEC §3 "POSIX shm + mmap") is cgo-only and
# guarded by the `posixshm` build tag, so the default go-test (which builds
# without it) never compiles or exercises it. This target compiles the cgo
# backend and runs its runtime round-trip so the SPEC-advertised backend is
# actually covered. Requires a C toolchain; Linux/macOS only.
.PHONY: go-test-posixshm
go-test-posixshm: ## Compile + test the cgo POSIX shared-memory backend.
	CGO_ENABLED=1 $(GO) test -tags posixshm -count=1 ./internal/shmem/...

# SPEC §5 documents `test-go`/`test-java`/`test-bench`; keep the language-first
# names canonical and provide these as aliases so the spec commands work.
.PHONY: test-go
test-go: go-test ## SPEC §5 alias for go-test.

.PHONY: test-java
test-java: java-test ## SPEC §5 alias for java-test.

.PHONY: test-bench
test-bench: bench-region-concurrency bench-latency ## SPEC §5 alias for the bench suite.

FUZZTIME ?= 30s
.PHONY: fuzz
fuzz: ## T83: run the wire-codec fuzzer (override duration with FUZZTIME=...).
	$(GO) test ./internal/wire/ -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime $(FUZZTIME)

# T83 Java side: jazzer (libFuzzer) over bridge/wire/Decoder, seeded with the
# golden corpus. Without JAZZER_FUZZ=1 the same test replays seeds only and
# rides along in every `mvn verify`. Fuzz duration is the @FuzzTest
# maxDuration (10m); JAVA_FUZZ_RUNS repeats the run for longer campaigns.
JAVA_FUZZ_RUNS ?= 1
.PHONY: java-fuzz
java-fuzz: ## T83: run the Java wire-decoder fuzzer (jazzer, 10m per run).
	for i in $$(seq 1 $(JAVA_FUZZ_RUNS)); do \
	  JAZZER_FUZZ=1 $(MVN) $(MVN_FLAGS) test -Dtest=DecoderFuzzTest -DfailIfNoTests=false || exit $$?; \
	done

# Coverage gate set excludes generated protobuf, thin mains, and examples -
# the gate measures hand-written, testable code.
GO_COVER_PKGS := $(shell $(GO) list ./... | grep -vE '/(examples|internal/wire/hbasepb|internal/wire/hookpb|internal/wire/wirepb|internal/wiregolden|cmd/wire-golden|cmd/hbasecop-runtime|tools/gen-builder|tools/gen-wiretypes|test/bench/noop-observer)$$')
# SPEC §7 gate: Go hand-written line coverage ≥80% (generated protobuf, thin
# mains and examples excluded above; generated files in covered packages are
# stripped from the profile by the "DO NOT EDIT" marker). Met as of Phase-7.
GO_COVER_MIN  ?= 80.0
.PHONY: go-cover
go-cover: ## SPEC §7 gate: fail if Go line coverage (hand-written) drops below 80%.
	$(GO) test -race -covermode=atomic -coverprofile=coverage.out $(GO_COVER_PKGS)
	@gen=$$(grep -rlE '^// Code generated .* DO NOT EDIT\.$$' --include='*.go' pkg cmd internal | sort -u | sed 's/[.]/\\./g' | paste -sd'|' -); \
	  if [ -n "$$gen" ]; then grep -vE "($$gen):" coverage.out > coverage.cov; else cp coverage.out coverage.cov; fi; \
	  total=$$($(GO) tool cover -func=coverage.cov | awk '/^total:/ {print $$3}'); \
	  rm -f coverage.cov; \
	  echo "Go line coverage (excl. generated): $$total (gate: $(GO_COVER_MIN)%)"; \
	  pct=$$(printf '%s' "$$total" | tr -d '%'); \
	  awk "BEGIN { exit !($$pct >= $(GO_COVER_MIN)) }" \
	    || { echo "FAIL: Go line coverage $$total < $(GO_COVER_MIN)% (SPEC §7)"; exit 1; }

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
# Bench (T62): multi-region dispatch throughput vs GOMAXPROCS.
# ---------------------------------------------------------------------------

.PHONY: bench-region-concurrency
bench-region-concurrency: ## T62: throughput bench across N regions × {1,2,4,8} cores.
	$(GO) test -run='^$$' -bench=BenchmarkRegionConcurrencyThroughput \
		-benchtime=1s -cpu=1,2,4,8 ./pkg/hbasecop/

# ---------------------------------------------------------------------------
# Bench (T81): per-hook latency overhead vs a Java-only no-op observer.
# ---------------------------------------------------------------------------

# Standalone bench ELF: a silent no-op RegionObserver. Staged outside
# src/main/resources so it never leaks into the shipped bridge jar; the root
# pom maps test/bench/bin onto the test classpath as bench/.
BENCH_NOOP_OUT := test/bench/bin/linux-amd64/noop-runtime

.PHONY: go-build-bench-noop
go-build-bench-noop: ## T81: build the no-op bench observer ELF (Linux x86-64).
	@mkdir -p $(dir $(BENCH_NOOP_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(BENCH_NOOP_OUT) ./test/bench/noop-observer

# Gate (SPEC §7.6): prePut p50 overhead < BENCH_P50_MAX_US µs; asserted
# inside LatencyBenchIT, so the mvn exit code is the gate. JaCoCo is skipped:
# latency is measured uninstrumented.
BENCH_P50_MAX_US ?= 100
.PHONY: bench-latency
bench-latency: go-build-bench-noop ## T81: p50/p95/p99 prePut/postPut overhead bench + gate.
	$(MVN) $(MVN_FLAGS) test -Dtest=LatencyBenchIT -DfailIfNoTests=false \
		-Djacoco.skip=true -Dbench.prePut.p50.max.us=$(BENCH_P50_MAX_US)

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
# Examples (T51): master-policy coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-master-policy
go-build-master-policy: ## T51: build master-policy-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(MASTER_POLICY_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(MASTER_POLICY_OUT) ./$(MASTER_POLICY_DIR)

.PHONY: master-policy-observer-jar
master-policy-observer-jar: go-build-master-policy ## T51: build master-policy-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(MASTER_POLICY_DIR)/pom.xml package
	@unzip -l $(MASTER_POLICY_DIR)/target/master-policy-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from master-policy-observer.jar" >&2; exit 1; }
	@unzip -l $(MASTER_POLICY_DIR)/target/master-policy-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/masterpolicy/PolicyMasterObserver.class' || \
	  { echo "ERROR: PolicyMasterObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(MASTER_POLICY_DIR)/target/master-policy-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/MasterObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: master-policy-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T52): rs-policy region-server coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-rs-policy
go-build-rs-policy: ## T52: build rs-policy-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(RS_POLICY_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(RS_POLICY_OUT) ./$(RS_POLICY_DIR)

.PHONY: rs-policy-observer-jar
rs-policy-observer-jar: go-build-rs-policy ## T52: build rs-policy-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(RS_POLICY_DIR)/pom.xml package
	@unzip -l $(RS_POLICY_DIR)/target/rs-policy-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from rs-policy-observer.jar" >&2; exit 1; }
	@unzip -l $(RS_POLICY_DIR)/target/rs-policy-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/rspolicy/RsPolicyRegionServerObserver.class' || \
	  { echo "ERROR: RsPolicyRegionServerObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(RS_POLICY_DIR)/target/rs-policy-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionServerObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: rs-policy-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T72): post-hook audit coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-audit
go-build-audit: ## T72: build audit-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(AUDIT_OBSERVER_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(AUDIT_OBSERVER_OUT) ./$(AUDIT_OBSERVER_DIR)

.PHONY: audit-observer-jar
audit-observer-jar: go-build-audit ## T72: build audit-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(AUDIT_OBSERVER_DIR)/pom.xml package
	@unzip -l $(AUDIT_OBSERVER_DIR)/target/audit-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from audit-observer.jar" >&2; exit 1; }
	@unzip -l $(AUDIT_OBSERVER_DIR)/target/audit-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/audit/AuditRegionObserver.class' || \
	  { echo "ERROR: AuditRegionObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(AUDIT_OBSERVER_DIR)/target/audit-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: audit-observer.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T73): pre-hook TTL-validation coproc-jar.
# ---------------------------------------------------------------------------

.PHONY: go-build-ttl
go-build-ttl: ## T73: build ttl-validator Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(TTL_VALIDATOR_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(TTL_VALIDATOR_OUT) ./$(TTL_VALIDATOR_DIR)

.PHONY: ttl-validator-jar
ttl-validator-jar: go-build-ttl ## T73: build ttl-validator coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(TTL_VALIDATOR_DIR)/pom.xml package
	@unzip -l $(TTL_VALIDATOR_DIR)/target/ttl-validator.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from ttl-validator.jar" >&2; exit 1; }
	@unzip -l $(TTL_VALIDATOR_DIR)/target/ttl-validator.jar | \
	  grep -q 'com/virogg/hbasecop/examples/ttl/TtlValidatorRegionObserver.class' || \
	  { echo "ERROR: TtlValidatorRegionObserver class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(TTL_VALIDATOR_DIR)/target/ttl-validator.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/RegionObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: ttl-validator.jar -- bridge shaded, Go ELF embedded"

# ---------------------------------------------------------------------------
# Examples (T82): no-op WAL-observer coproc-jar (bench support).
# ---------------------------------------------------------------------------

.PHONY: go-build-wal-observer
go-build-wal-observer: ## T82: build wal-observer Go ELF into example resources (Linux x86-64).
	@mkdir -p $(dir $(WAL_OBSERVER_OUT))
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o $(WAL_OBSERVER_OUT) ./$(WAL_OBSERVER_DIR)

.PHONY: wal-observer-jar
wal-observer-jar: go-build-wal-observer ## T82: build wal-observer coproc-jar (installs bridge into ~/.m2 first).
	$(MVN) $(MVN_FLAGS) install -DskipTests
	$(MVN) $(MVN_FLAGS) -f $(WAL_OBSERVER_DIR)/pom.xml package
	@unzip -l $(WAL_OBSERVER_DIR)/target/wal-observer.jar | \
	  grep -q 'bin/linux-amd64/hbasecop-runtime' || \
	  { echo "ERROR: Go ELF missing from wal-observer.jar" >&2; exit 1; }
	@unzip -l $(WAL_OBSERVER_DIR)/target/wal-observer.jar | \
	  grep -q 'com/virogg/hbasecop/examples/walbench/WalBenchWALCoprocessor.class' || \
	  { echo "ERROR: WalBenchWALCoprocessor class missing from coproc-jar" >&2; exit 1; }
	@unzip -l $(WAL_OBSERVER_DIR)/target/wal-observer.jar | \
	  grep -q 'com/virogg/hbasecop/bridge/observer/WALObserverAdapter.class' || \
	  { echo "ERROR: bridge classes not shaded into coproc-jar" >&2; exit 1; }
	@echo "OK: wal-observer.jar -- bridge shaded, Go ELF embedded"

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
demo-counter: ## CP-γ: public demo - Put on HBase triggers Go observer counter; leaves cluster up.
	./tools/demo-counter.sh

.PHONY: test-integration
test-integration: counter-observer-jar ## T27: full IT - bring up HBase, run PrePutCounterIT, tear down.
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
# Integration (T63): refcounted SharedRuntime - N regions share one Go process.
# ---------------------------------------------------------------------------

.PHONY: test-integration-shared
test-integration-shared: counter-observer-jar ## T63: full IT - bring up HBase, run SharedRegionProcessIT (4-way pre-split, expect one Go pid), tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar $(COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=SharedRegionProcessIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-shared.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T36): fault-injection matrix on real HBase.
# ---------------------------------------------------------------------------

.PHONY: test-fault
test-fault: fault-observer-jar ## T36: full IT - bring up HBase, run FaultMatrixIT (10 cases), tear down.
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
test-integration-read: filter-observer-jar ## T43: full IT - bring up HBase, run ReadPathFilterIT, tear down.
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
test-integration-batch: filter-observer-jar ## T44: full IT - bring up HBase, run BatchPartialBlockIT, tear down.
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
# Integration (T45): storage hooks (preFlush/postFlush, preCompactSelection,
# preCompact/postCompact) driven by admin.flush + admin.majorCompact.
# ---------------------------------------------------------------------------

.PHONY: test-integration-storage
test-integration-storage: filter-observer-jar ## T45: full IT - bring up HBase, run StorageHooksIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(FILTER_OBSERVER_DIR)/target/filter-observer.jar $(FILTER_COPROC_JAR_STAGED)
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=StorageHooksIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-storage.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T51): MasterObserver - preCreateTable policy rejection.
# The master coprocessor is registered cluster-wide (the entrypoint patches
# hbase-site.xml when HBASECOP_MASTER_COPROC_CLASS is exported), so the jar
# is staged before `up` and the env vars drive the injection.
# ---------------------------------------------------------------------------

MASTER_COPROC_JAR_STAGED := test/integration/coproc-jars/master-policy-observer.jar

.PHONY: test-integration-master
test-integration-master: master-policy-observer-jar ## T51: full IT - bring up HBase with master coproc, run MasterPolicyIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(MASTER_POLICY_DIR)/target/master-policy-observer.jar $(MASTER_COPROC_JAR_STAGED)
	@set +e; \
	  export HBASECOP_MASTER_COPROC_CLASS=com.virogg.hbasecop.examples.masterpolicy.PolicyMasterObserver; \
	  export HBASECOP_MASTER_COPROC_JAR=/coproc-jars/master-policy-observer.jar; \
	  export HBASECOP_POLICY_BLOCKED_PREFIX=forbidden-; \
	  $(HBASE_COMPOSE_CMD) up -d --build; \
	  ./test/integration/scripts/wait-master-status.sh; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=MasterPolicyIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-master.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T52): RegionServerObserver - preRollWALWriterRequest policy
# rejection. The region-server coprocessor is registered cluster-wide (the
# entrypoint patches hbase-site.xml when HBASECOP_RS_COPROC_CLASS is exported),
# so the jar is staged before `up` and the env vars drive the injection.
# ---------------------------------------------------------------------------

RS_COPROC_JAR_STAGED := test/integration/coproc-jars/rs-policy-observer.jar

.PHONY: test-integration-rs
test-integration-rs: rs-policy-observer-jar ## T52: full IT - bring up HBase with region-server coproc, run RegionServerPolicyIT, tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(RS_POLICY_DIR)/target/rs-policy-observer.jar $(RS_COPROC_JAR_STAGED)
	@set +e; \
	  export HBASECOP_RS_COPROC_CLASS=com.virogg.hbasecop.examples.rspolicy.RsPolicyRegionServerObserver; \
	  export HBASECOP_RS_COPROC_JAR=/coproc-jars/rs-policy-observer.jar; \
	  export HBASECOP_RS_POLICY_VETO_WAL_ROLL=true; \
	  $(HBASE_COMPOSE_CMD) up -d --build; \
	  ./test/integration/scripts/wait-master-status.sh; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=RegionServerPolicyIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-rs.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T72): post-hook audit on real HBase.
# ---------------------------------------------------------------------------

.PHONY: test-integration-audit
test-integration-audit: audit-observer-jar ## T72: full IT - bring up HBase, run AuditObserverIT (50 ops → 50 audit records), tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(AUDIT_OBSERVER_DIR)/target/audit-observer.jar test/integration/coproc-jars/audit-observer.jar
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=AuditObserverIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-audit.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Integration (T73): pre-hook TTL validation (strict mode) on real HBase.
# ---------------------------------------------------------------------------

.PHONY: test-integration-ttl
test-integration-ttl: ttl-validator-jar ## T73: full IT - bring up HBase, run TtlValidatorIT (valid → success, invalid → IOException), tear down.
	@mkdir -p test/integration/coproc-jars
	cp $(TTL_VALIDATOR_DIR)/target/ttl-validator.jar test/integration/coproc-jars/ttl-validator.jar
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=TtlValidatorIT -DfailIfNoTests=false; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-ttl.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Bench (T82): WAL write throughput, WALObserver on vs off, live HBase.
# ---------------------------------------------------------------------------

# Gate (plan T82): throughput with the WAL coprocessor registered must not
# regress more than BENCH_WAL_MAX_REGRESSION_PCT % vs the no-coproc baseline.
# Two full compose cycles: WAL coprocessors are cluster-wide, so on/off needs
# two cluster boots, A/B-measured by the same WalThroughputBenchIT.
BENCH_WAL_MAX_REGRESSION_PCT ?= 50
BENCH_WAL_OPS ?= 20000
WAL_BENCH_CLASS := com.virogg.hbasecop.examples.walbench.WalBenchWALCoprocessor

.PHONY: bench-wal
bench-wal: wal-observer-jar ## T82: WAL throughput A/B bench (observer on/off) + regression gate.
	@mkdir -p test/integration/coproc-jars
	cp $(WAL_OBSERVER_DIR)/target/wal-observer.jar test/integration/coproc-jars/wal-observer.jar
	@echo "== T82 cycle A: baseline (no WAL coprocessor) =="
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=WalThroughputBenchIT -DfailIfNoTests=false \
	    -Djacoco.skip=true -Dbench.wal.ops=$(BENCH_WAL_OPS) -Dbench.wal.expect.coproc=false \
	    2>&1 | tee test/integration/coproc-jars/wal-bench-baseline.log; \
	  status=$${PIPESTATUS[0]}; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-walbench-base.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status
	@echo "== T82 cycle B: WAL coprocessor registered =="
	HBASECOP_WAL_COPROC_CLASS=$(WAL_BENCH_CLASS) \
	HBASECOP_WAL_COPROC_JAR=/coproc-jars/wal-observer.jar \
	  $(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  $(MVN) $(MVN_FLAGS) test -Dtest=WalThroughputBenchIT -DfailIfNoTests=false \
	    -Djacoco.skip=true -Dbench.wal.ops=$(BENCH_WAL_OPS) -Dbench.wal.expect.coproc=true \
	    2>&1 | tee test/integration/coproc-jars/wal-bench-coproc.log; \
	  status=$${PIPESTATUS[0]}; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-walbench-coproc.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status
	@base=$$(grep -o 'ops_per_sec=[0-9.]*' test/integration/coproc-jars/wal-bench-baseline.log | head -1 | cut -d= -f2); \
	  cop=$$(grep -o 'ops_per_sec=[0-9.]*' test/integration/coproc-jars/wal-bench-coproc.log | head -1 | cut -d= -f2); \
	  [ -n "$$base" ] && [ -n "$$cop" ] || { echo "FAIL: WAL_BENCH_RESULT line missing from a cycle log" >&2; exit 1; }; \
	  awk "BEGIN { reg = (1 - $$cop / $$base) * 100; \
	    printf \"T82: baseline %.1f ops/s, coproc %.1f ops/s, regression %.1f%% (gate < $(BENCH_WAL_MAX_REGRESSION_PCT)%%)\n\", $$base, $$cop, reg; \
	    exit !(reg < $(BENCH_WAL_MAX_REGRESSION_PCT)) }" \
	  || { echo "FAIL: WAL throughput regression exceeds $(BENCH_WAL_MAX_REGRESSION_PCT)% (plan T82)"; exit 1; }

# ---------------------------------------------------------------------------
# Soak (T84): sustained load + kill -9 chaos + RSS sampling, live HBase.
# ---------------------------------------------------------------------------

# Defaults: 1h at 1000 ops/s with a kill every 120-300s (see soak.sh for all
# knobs). Smoke run: make soak SOAK_DURATION_S=120 SOAK_KILL_MIN_S=30 SOAK_KILL_MAX_S=60
SOAK_DURATION_S ?= 3600
SOAK_RATE ?= 1000
SOAK_KILL_MIN_S ?= 120
SOAK_KILL_MAX_S ?= 300

.PHONY: soak
soak: counter-observer-jar ## T84: soak/chaos run - load + kill -9 + RSS/zombie gates.
	@mkdir -p test/integration/coproc-jars
	cp $(COUNTER_OBSERVER_DIR)/target/counter-observer.jar test/integration/coproc-jars/counter-observer.jar
	$(HBASE_COMPOSE_CMD) up -d --build
	./test/integration/scripts/wait-master-status.sh
	@set +e; \
	  SOAK_DURATION_S=$(SOAK_DURATION_S) SOAK_RATE=$(SOAK_RATE) \
	  SOAK_KILL_MIN_S=$(SOAK_KILL_MIN_S) SOAK_KILL_MAX_S=$(SOAK_KILL_MAX_S) \
	    bash test/integration/scripts/soak.sh; \
	  status=$$?; \
	  $(HBASE_COMPOSE_CMD) logs hbase > test/integration/coproc-jars/hbase-soak.log 2>&1 || true; \
	  $(HBASE_COMPOSE_CMD) down; \
	  exit $$status

# ---------------------------------------------------------------------------
# Release (T85): assemble distributable artifacts into target/release/.
# ---------------------------------------------------------------------------

# Stamps the Maven build with VERSION (the repo pom stays SNAPSHOT - the
# stamp lives only in the produced artifact) and builds the release-mode
# hbasecop-build CLI for the supported platform. Tagging and publishing are
# the release workflow's job (.github/workflows/release.yml).
.PHONY: release
release: go-build-runtime ## T85: build release artifacts (make release VERSION=0.1.0).
	@[ -n "$(VERSION)" ] || { echo "usage: make release VERSION=0.1.0" >&2; exit 1; }
	@mkdir -p target/release
	$(MVN) $(MVN_FLAGS) versions:set -DnewVersion=$(VERSION) -DgenerateBackupPoms=false
	@set +e; \
	  $(MVN) $(MVN_FLAGS) package -DskipTests; \
	  status=$$?; \
	  $(MVN) $(MVN_FLAGS) versions:set -DnewVersion=0.0.1-SNAPSHOT -DgenerateBackupPoms=false; \
	  exit $$status
	cp target/hbasecop-bridge-$(VERSION).jar target/release/
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -ldflags "-s -w" \
		-o target/release/hbasecop-build-linux-amd64 ./cmd/hbasecop-build
	@echo "release artifacts in target/release/:"
	@ls -l target/release/

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
