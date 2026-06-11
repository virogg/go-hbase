# go-hbase — HBase Coprocessor Streaming for Go

## 1. Objective

Дать возможность писать HBase Observer-coprocessors на Go и исполнять их в контексте
RegionServer без fork-per-call overhead, как в Hadoop Streaming.

**Идея:** долгоживущий Go-процесс на каждом RegionServer общается с Java-coprocessor
через lock-free shared-memory ring buffer (см. `github.com/virogg/java-go-shmem`).
Ring buffer + protobuf вместо stdin/stdout + line-based, как в Hadoop Streaming.

**Целевые пользователи:**
- Go-инженеры, которым нужна in-DB логика (валидация, аудит, derived-данные, secondary index hooks),
  но писать на Java не хочется/нельзя.
- Команды с Go-доменной моделью, желающие переиспользовать её в coprocessors.

**Не цели:**
- Java SDK для clients (HBase native Java client уже есть).
- Replacement для HBase Endpoint coproc через REST/Thrift gateway.
- Поддержка не-JVM RegionServer (Phoenix/etc).

## 2. Scope (MVP)

### Покрытие хуков

Все Observer-типы HBase 2.5.x:
- `RegionObserver` (prePut/postPut, preGet/postGet, preDelete/postDelete, preScannerOpen, preFlush, preCompact, preBatchMutate, …)
- `RegionServerObserver` (preStop, preMerge, preReplicate, …)
- `MasterObserver` (preCreateTable, preDeleteTable, preModifyTable, preBalance, …)
- `WALObserver` (preWALWrite/postWALWrite)
- `BulkLoadObserver` (prePrepareBulkLoad, preCleanupBulkLoad)

Endpoint coproc — **out of scope MVP** и не в активном roadmap. Go API спроектирован так, чтобы добавить позже без breaking change, если будет запрос.

### Версия HBase / платформа
HBase **2.5.x LTS**, Java 11. Hadoop 3.x. **Linux x86-64 only** (POSIX shm + mmap; arm64 не таргетируем).

### Лицензия
**Apache 2.0** (как у HBase).

## 3. Architecture

### Компоненты
1. **`com.virogg.hbasecop.bridge`** — Java Observer adapter-jar.
   - Реализует все Observer-интерфейсы HBase 2.5.
   - При `start(CoprocessorEnvironment)` извлекает Go-бинарь из jar resources,
     стартует один процесс на RegionServer (shared, см. ниже), устанавливает shmem-канал.
   - На каждый hook сериализует контекст в protobuf, отправляет в ring,
     ждёт ответ (sync) или fire-and-forget (post-hooks).
2. **`pkg/hbasecop`** — Go SDK для пользователя.
   - Интерфейс `RegionObserver`, `MasterObserver`, … с методами `PrePut(ctx, env, mut) error` и т.п.
   - `hbasecop.Run(observers...)` — entrypoint, читает shmem-конфиг из env, запускает event loop.
3. **`internal/wire`** — generated protobuf для wire protocol (Go и Java, общие `.proto`).
4. **`internal/multiplex`** — мультиплексер frame'ов по (region_id, hook_id, request_id).
5. **`cmd/hbasecop-build`** — CLI: упаковывает Go-бинарь в coproc-jar пользователя.

### Process model

**Один Go-процесс на RegionServer (shared).** Java-side при старте первого Observer
на RS поднимает Go-процесс, регистрирует все последующие coproc-инстансы (региональные
обзёрверы) на тот же канал. Frame'ы мультиплексируются по `(region_id, observer_class, hook_id, req_id)`.
Refcount: процесс гасится при `stop()` последнего Observer на RS.

**Обоснование:** один shmem-сегмент на RS (минимум RAM/handles), но изоляция от user-кода
обеспечивается ring buffer-ом (Go crash → SIGCHLD → supervisor restart). Дороже чем «один
процесс на jar», но архитектурно интереснее: внутри Go-side работает scheduler с
горутинами на запрос, что естественно для Go.

### Failure mode

**Configurable per-hook + auto-restart Go side.**

- Конфиг coproc (`hbase-site.xml` или TableDescriptor):
  ```xml
  <property>
    <name>hbasecop.policy.prePut</name>
    <value>strict</value>  <!-- или: best-effort -->
  </property>
  ```
- `strict` (default для `pre*` validation hooks): Go упал/timeout → `IOException` к клиенту, операция aborts.
- `best-effort` (default для `post*` audit hooks): Go упал → log WARN, hook skip, операция продолжает.
- **Auto-restart:** Java supervisor перезапускает Go-side с exponential backoff (200ms → 5s, jitter).
  Inflight requests на момент crash → fail by policy. Новые — ждут до restart-deadline (configurable, default 3s),
  иначе fail by policy.
- **Watchdog:** heartbeat frame каждые 500ms; нет 3 подряд → kill -9 + restart.

### Wire protocol

Protobuf поверх ring-buffer. Frame:
```
| u32 length | u8 frame_type | u64 req_id | u32 region_id | u8 hook_id | <protobuf body> |
```
- `frame_type`: REQUEST | RESPONSE | HEARTBEAT | ERROR | SHUTDOWN | LOG.
- HBase-style messages: переиспользуем `Cell.proto`, `Mutation.proto`, `Get.proto`, `Result.proto` из HBase
  (через `hbase-protocol-shaded`).
- Custom: `HookContext`, `HookResponse`, `Error`.
- Big payloads (>ring-frame): chunk по `MAX_FRAME` (default 64KB), reassemble на consumer side.

## 4. Project Structure

```
go-hbase/
├── SPEC.md
├── README.md
├── go.mod                         # module github.com/virogg/go-hbase
├── pom.xml                        # Java side, Maven
├── proto/                         # canonical .proto (shared)
│   ├── wire.proto
│   ├── hooks.proto
│   └── hbase/                     # vendored HBase .proto (Cell, Mutation, …)
├── pkg/
│   └── hbasecop/                  # public Go SDK
│       ├── observer.go            # interfaces RegionObserver, MasterObserver, …
│       ├── env.go                 # CoprocessorEnvironment Go-side
│       ├── run.go                 # hbasecop.Run(...)
│       └── policy.go              # error/timeout policy types
├── internal/
│   ├── wire/                      # generated protobuf + framing
│   ├── multiplex/                 # request multiplexer
│   ├── cpruntime/                 # event loop, supervisor handshake (avoids stdlib `runtime` collision)
│   └── shmem/                     # thin wrapper over java-go-shmem pkg/ring
├── cmd/
│   └── hbasecop-build/            # packs user Go bin into Java jar
├── examples/
│   ├── audit-observer/            # post-hook audit example
│   └── ttl-validator/             # pre-hook validation example
├── java/
│   └── com/virogg/hbasecop/
│       ├── bridge/                # ObserverAdapter*, JNI loader
│       ├── supervisor/            # Go process lifecycle
│       └── multiplex/             # Java side of mux
├── test/
│   ├── java/                      # Java unit tests (JUnit 5)
│   ├── integration/               # docker-compose: HBase 2.5 mini-cluster + sample observer
│   └── e2e/
└── tools/
    └── proto-gen.sh
```

## 5. Commands

```bash
# Bootstrap
make deps                        # go mod download + mvn dependency:go-offline + protoc

# Build Go side (per OS/ARCH; Linux x86-64 default)
make go-build                    # → bin/hbasecop-runtime

# Build Java side
make java-build                  # → java/target/hbasecop-bridge-<ver>.jar

# Generate protobuf (Go + Java)
make proto

# Pack user observer into deployable coproc-jar
hbasecop-build \
  --go-bin ./examples/audit-observer/bin/observer \
  --observer-class com.virogg.hbasecop.examples.AuditObserver \
  --out ./audit-coproc.jar

# Tests
make test-go                     # go test ./...
make test-java                   # mvn -pl java test
make test-integration            # docker-compose up + e2e suite
make test-bench                  # latency/throughput benchmark vs Java-only

# Lint
make lint                        # golangci-lint + spotless/checkstyle

# Release
make release VERSION=0.1.0
```

## 6. Code Style

### Go
- Standard `gofmt` + `golangci-lint` (errcheck, gosimple, staticcheck, govet, revive).
- Public package: только `pkg/hbasecop`. Всё остальное — `internal/`.
- Errors: `fmt.Errorf("op X: %w", err)`. Без panic в библиотечном коде; panic в user-callback ловится recover'ом и конвертится в `Error` frame.
- Контекст: `context.Context` — первый аргумент в каждом hook-методе SDK (для cancel/timeout от Java side).
- Концурренция: один event-loop горутина per shmem-канал, по горутине на inflight request. Без mutex'ов в hot path; каналы.
- Логи: `slog`, JSON output, level из env `HBASECOP_LOG_LEVEL`.
- **Observability MVP — только логирование.** Метрик (JMX/Prometheus) нет. Структурированные log-поля: `hook`, `region`, `req_id`, `latency_ms`, `outcome` — достаточно для grep/log-aggregator анализа.

### Java
- Java 11. Maven. Google Java Style + Spotless.
- Минимум зависимостей сверх HBase 2.5 + `hbase-protocol-shaded`.
- Никакой рефлексии в hot path; всё JNI/shmem-вызовы — через generated bindings из `java-go-shmem`.
- Все Observer-методы делегируют в `Bridge.dispatch(hookId, …)` — никакой бизнес-логики на Java side.

### Protobuf
- `proto3`. `package virogg.hbasecop.v1`.
- Версионирование через major version в package; backward-compat в minor (только added fields).

## 7. Testing Strategy

### Уровни
1. **Unit (Go):** `internal/wire`, `internal/multiplex`, `internal/cpruntime` — table-driven, race-detector on.
2. **Unit (Java):** `bridge`, `supervisor`, `multiplex` — JUnit 5 + Mockito. Fake shmem channel.
3. **Contract:** общий golden corpus protobuf-сообщений; Java сериализует → Go десериализует (round-trip) и наоборот. Запускается в обоих CI-jobs.
4. **Integration:** docker-compose с HBase 2.5 standalone; реальный coproc-jar; sample observers; ассерты на side-effects (HBase scan/Get результаты).
5. **E2E fault-injection:** kill -9 Go-side под нагрузкой → проверяем strict/best-effort семантику, restart, отсутствие data loss/double-apply на post-hooks.
6. **Bench:** микробенч latency overhead per hook (Java-only baseline vs through-shmem); throughput на batch puts. Цель MVP: <100µs p50 overhead на pre/postPut.

### Coverage gate
- Go: ≥80% line, ≥70% branch.
- Java: ≥75% line.
- Contract suite: 100% .proto messages covered round-trip.

### CI
GitHub Actions: lint → unit → contract → integration (only on PR to main + nightly) → bench (nightly, regression alert на >20% degrade).

## 8. Boundaries

### Always do
- Любая правка proto/wire — bump minor version, golden corpus update в том же коммите.
- Любая новая Observer-method (HBase API расширилась) — синхронно в Java adapter, Go interface, .proto, golden tests.
- Все JNI/unsafe code — изолировано в `internal/shmem` Go и `bridge.jni` Java; снаружи — типизированные API.
- Каждый bug-fix → regression test первым коммитом.
- Все timeout'ы и буферы — configurable, default'ы документированы в README.

### Ask first
- Расширение scope (Endpoint coproc, не-Linux таргет, HBase 3.x, другая модель процесса).
- Breaking change в `pkg/hbasecop` public API после 0.1.0 release.
- Добавление JNI кроме того что предоставляет `java-go-shmem`.
- Зависимость на C-библиотеки сверх POSIX shm.
- Изменение wire-формата которое нарушает round-trip с предыдущим minor.

### Never do
- Fork-per-hook (это Hadoop Streaming, мы делаем именно лучше).
- Сериализация через JSON/XML в hot path.
- Глобальный mutex в Go event loop.
- Сетевой transport между Java и Go (shmem only — иначе теряется смысл).
- Скрытое игнорирование ошибок (`_ = err`); политика всегда явная (strict/best-effort).
- Логирование payload'ов с potentially-sensitive данными (row keys, values) на default log level.
- Изменения в HBase core jar / shaded зависимостях.

---

## Open questions

1. ~~Multi-tenant (>1 Go-coproc разных jar'ов на одном RS) — MVP или потом?~~ **Closed at
   CP-δ (2026-05-14): out of scope for MVP.** Каждый coproc-jar получает свой
   `CoprocessorRuntime` → свой `GoProcess` → свой shmem pair (через
   `Files.createTempDirectory`), но formal cross-coproc isolation не валидируется
   тестами в MVP. Defer to post-v0.1.0.
2. ~~Hot reload Go-binary без рестарта RS?~~ **Closed at CP-ε3 (2026-05-19):
   defer post-MVP.** Обновление = `disable/alterTable/enable` цикл (полный
   teardown coproc-jar). SIGHUP-flow рассмотрим post-v0.1.0 если будет запрос.
3. ~~Подпись/checksum Go-бинаря в jar — MVP или не сейчас?~~ **Closed at
   CP-ε3 (2026-05-19): SHA-256 checksum в MVP.** `hbasecop-build` (T71)
   записывает `SHA-256` ELF в manifest (`HbaseCop-Go-Bin-SHA256`); supervisor
   (`GoProcess.start`) валидирует digest при extract из jar resource — mismatch
   → fail-fast с ясным сообщением. Защита от corruption/wrong-arch, не от
   threat-actor (полноценный GPG signing — post-MVP).

   > **REOPENED then RE-CLOSED (review, fix/v0.1.0-blockers).** The CP-ε3
   > closure was inaccurate: `GoProcess.verifyChecksum` existed but
   > `CoprocessorRuntime.buildGoProcess()` never passed the expected digest, so
   > the check was **dead code** — every real launch skipped verification. Fixed
   > on `fix/v0.1.0-blockers`: the runtime now resolves
   > `HbaseCop-Go-Bin-SHA256` from the coproc-jar manifest and validates it
   > before exec. See [`tasks/RELEASE-BLOCKERS.md`](tasks/RELEASE-BLOCKERS.md).
