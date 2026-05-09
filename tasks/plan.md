# go-hbase — Implementation Plan

> Source of truth: `SPEC.md`. Этот файл декомпозирует SPEC в фазы/задачи с AC и
> verification. Слайсы **вертикальные** (каждый = тонкий end-to-end путь), не
> горизонтальные ("сначала весь Java, потом весь Go").

## 1. Dependency graph

```
                      ┌──────────────────┐
                      │  proto/*.proto   │  (canonical schema, single source)
                      └────────┬─────────┘
                       protoc-gen-go │ protoc-gen-java
                ┌──────────────┴──────────────┐
                ▼                             ▼
        ┌───────────────┐             ┌───────────────┐
        │ Go gen + frame│             │ Java gen+frame│
        │ internal/wire │             │ bridge.wire   │
        └───────┬───────┘             └───────┬───────┘
                │                             │
                ▼                             ▼
        ┌───────────────┐             ┌───────────────┐
        │ Go shmem wrap │             │ Java shmem wrap│
        │ internal/shmem│             │  bridge.shmem  │
        └───────┬───────┘             └───────┬───────┘
                │                             │
                ▼                             ▼
        ┌───────────────┐             ┌───────────────┐
        │ Go mux        │             │ Java mux      │
        │internal/mux   │             │bridge.multiplex│
        └───────┬───────┘             └───────┬───────┘
                │                             │
                ▼                             ▼
        ┌───────────────┐             ┌───────────────┐
        │ Go runtime    │             │ Java supervisor│
        │ event loop    │             │ + Observer     │
        │internal/cpruntime│            │   adapters     │
        └───────┬───────┘             └───────┬───────┘
                │                             │
                ▼                             ▼
        ┌───────────────┐             ┌───────────────┐
        │ Go SDK        │             │ HBase coproc   │
        │ pkg/hbasecop  │             │ (registered)   │
        └───────┬───────┘             └───────┬───────┘
                └─────────────┬───────────────┘
                              ▼
                     ┌──────────────────┐
                     │ user observer    │
                     │ jar + Go bin     │
                     └────────┬─────────┘
                              ▼
                     ┌──────────────────┐
                     │ HBase 2.5 cluster│
                     └──────────────────┘
```

External dep: `github.com/virogg/java-go-shmem` (Go pkg + Java jar). Lock версии в
`go.mod` и `pom.xml`. Любая несовместимость → fork в `third_party/` обсуждается.

## 2. Vertical slicing strategy

Каждая фаза = вертикальный slice сверху донизу через нужные компоненты.
Готовая фаза = можно собрать, запустить, продемонстрировать ценность.

| Фаза | Слайс                                 | Главная ценность                                         |
|------|---------------------------------------|----------------------------------------------------------|
| P0   | Foundation                            | `make all` собирается, CI зелёный, ничего не делает      |
| P1   | Java↔Go ping/pong (без HBase)         | Доказали что shmem+protobuf+supervisor работают          |
| P2   | prePut/postPut e2e на real HBase 2.5  | MVP-демо: Go-обзёрвер ловит Put на живом HBase           |
| P3   | Failure semantics + supervisor prod   | Strict/best-effort, kill -9 не теряет/не дублирует данные|
| P4   | Полный RegionObserver                 | Все RegionObserver-хуки работают                         |
| P5   | Master/RegionServer/WAL/BulkLoad      | Все Observer-типы из SPEC §2                             |
| P6   | Multi-region multiplexing             | Один Go-процесс на RS обслуживает N регионов параллельно |
| P7   | hbasecop-build CLI + примеры + docs   | Сторонний разработчик может собрать свой coproc          |
| P8   | Bench + harden + release v0.1.0       | Перф-цель достигнута, fault-injection пройден, тег       |

## 3. Phases & tasks

Формат: `Tnn — Title (slice)`
- **Deps:** id предыдущих задач
- **AC:** acceptance criteria (что значит «готово»)
- **Verify:** конкретный шаг проверки (команда/тест/демо)

### Phase 0 — Foundation

**T01 — Repo skeleton** *(P0)*
- Deps: —
- AC: дерево из SPEC §4 создано, пустые пакеты с `// Package ...` doc-комментарием, LICENSE (Apache 2.0), CODE_OF_CONDUCT, CONTRIBUTING заглушки, `.gitignore` (Go+Java+IDE+OS).
- Verify: `tree -L 3` совпадает с SPEC §4; `git ls-files` показывает все ожидаемые пути.

**T02 — Go build system** *(P0)*
- Deps: T01
- AC: `go.mod` (module `github.com/virogg/go-hbase`, go 1.22), `golangci-lint` config (revive, govet, staticcheck, errcheck, gosimple), `Makefile` цели `go-build`, `go-test`, `go-lint`. Tools/protoc-gen-go pinned via `tools/tools.go`.
- Verify: `make go-build go-test go-lint` зелёные на пустых пакетах.

**T03 — Java build system** *(P0)*
- Deps: T01
- AC: `pom.xml` (Java 11, HBase 2.5.x BOM, JUnit 5, Mockito, Spotless с Google Java Style, JaCoCo), Maven цели `package`, `test`, `verify`, `spotless:check`. Multi-module не нужен — один модуль.
- Verify: `mvn -B verify` зелёный.

**T04 — Make orchestration** *(P0)*
- Deps: T02, T03
- AC: верхний `Makefile` с `deps`, `proto`, `go-build`, `java-build`, `test-go`, `test-java`, `lint`, `clean`. `make all` = build обоих.
- Verify: `make all` собирает Go-бинарь и Java-jar.

**T05 — CI scaffold** *(P0)*
- Deps: T04
- AC: GitHub Actions workflow `.github/workflows/ci.yml` с job'ами: lint, go-test, java-test, contract (заглушка). Triggers: push, PR. Linux x86-64 only (`runs-on: ubuntu-22.04`).
- Verify: первый push → workflow зелёный.

**T06 — Apache 2.0 license headers tool** *(P0)*
- Deps: T05
- AC: `tools/license-header.sh`, header-check в CI, шаблоны для Go/Java/proto.
- Verify: `make license-check` проходит.

> **Checkpoint α:** `make all` зелёный, CI зелёный, лицензия чистая. **Демо:** ничего не делает, но
> компилируется. Решение пользователя продолжать.

### Phase 1 — Java↔Go IPC validated (no HBase)

**T11 — Wire .proto v1 minimal** *(P1)*
- Deps: T01
- AC: `proto/wire.proto` (Frame с oneof: Request/Response/Heartbeat/Error/Shutdown/Log; FrameHeader с req_id u64, region_id u32 (=0 в P1), hook_id u32, chunk_idx, chunk_total). `proto/hooks.proto` (HookContext, HookResponse — пустые сейчас, расширим в P2). Package `virogg.hbasecop.v1`.
- Verify: `protoc` без ошибок; `make proto` генерит Go и Java code; round-trip golden test (PB encode→decode==input) на 5+ примерах.

**T12 — Go wire framing + chunking** *(P1)*
- Deps: T11
- AC: `internal/wire`: `Encoder`/`Decoder` поверх `io.Writer`/`io.Reader` (для Test); фрейм `[u32 len][u8 type][u64 req_id][u32 region][u8 hook_id][u8 chunk_flags][u32 chunk_idx][u32 chunk_total][bytes pb]`. Chunking при > MAX_FRAME (default 64KB). Reassembly buffer per (req_id).
- Verify: `go test ./internal/wire -race` — таблица сценариев: пустой PB, ровно MAX_FRAME, multi-chunk, испорченный header (length too big), out-of-order chunks. Coverage ≥85%.

**T13 — Java wire framing + chunking** *(P1)*
- Deps: T11
- AC: пакет `com.virogg.hbasecop.bridge.wire` — то же самое API на Java (Encoder/Decoder поверх ByteBuffer). Идентичные фреймы биту-в-бит.
- Verify: JUnit таблица сценариев + cross-language test: Go encode → Java decode и наоборот через временный файл (загружаем golden в обоих CI job'ах).

**T14 — Java-Go-shmem dependency wiring** *(P1)*
- Deps: T01
- AC: `go.mod` use `github.com/virogg/java-go-shmem/pkg/...`, `pom.xml` зависимость `com.jgshmem:java-go-shmem` (если нет в Maven Central — install local или git submodule в `third_party/`). Документ `docs/dep-shmem.md`: версия, как пинить, как обновлять. Решение по vendoring/submodule/Maven repo фиксируем здесь.
- Verify: hello-world: на Go `import "github.com/virogg/java-go-shmem/pkg/ring"` компилируется; на Java `WaitingRingProducer p = new WaitingRingProducer(...)` компилируется.

**T15 — Go shmem wrapper** *(P1)*
- Deps: T14, T12
- AC: `internal/shmem` — тонкая обёртка над `java-go-shmem/pkg/ring`: типизированный `Channel` с `Send(Frame)`/`Recv() (Frame, error)`, lifecycle `Open(cfg)`/`Close()`. Конфиг: путь mmap-файла или posix-имя, размеры, role (producer/consumer). Никакой бизнес-логики.
- Verify: unit test через временный mmap файл: round-trip 1000 frames в одной горутине; race-detector clean.

**T16 — Java shmem wrapper** *(P1)*
- Deps: T14, T13
- AC: `bridge.shmem.Channel` — то же API на Java.
- Verify: JUnit round-trip в одном процессе.

**T17 — Go runtime: minimal event loop** *(P1)*
- Deps: T15
- AC: `internal/cpruntime/Loop`: spawn горутина-reader, на REQUEST → spawn handler-горутина, на REQUEST hook_id=`PING` → отвечает RESPONSE с эхом payload. Heartbeat sender каждые `cfg.HeartbeatPeriod` (default 500ms).
- Verify: integration test (но in-process, ровно 1 Go runtime + ровно 1 Go consumer-mock как «Java»): 10k ping/pong, p99 latency < 1ms на dev-машине, без race.

**T18 — Java supervisor: spawn Go process** *(P1)*
- Deps: T16, T17
- AC: `bridge.supervisor.GoProcess`: extracts ELF из jar resources в `tmpdir`, `exec`, передаёт shmem-конфиг через env (`HBASECOP_SHMEM_PATH`, `HBASECOP_RING_SIZE`, ...). API: `start()`, `stop()`, `isAlive()`, `pid()`. Logs из Go forward'ятся в SLF4J. Embedded ELF путь `src/main/resources/bin/linux-amd64/hbasecop-runtime`.
- Verify: JUnit: запускает stub-Go-binary (echo через shmem), посылает PING, получает PONG; `stop()` шлёт SHUTDOWN frame, ждёт graceful exit ≤ 1s. Тест с ELF из `make go-build` в `target/test-classes`.

**T19 — Cross-language ping/pong end-to-end** *(P1)*
- Deps: T18
- AC: интеграционный тест `test/e2e/ping/`: Java `main()` который через `GoProcess` стартует **реальный** Go-runtime бинарь и шлёт 10000 PING с разными payload size (0/1KB/64KB/1MB → проверяем chunking). Все RESPONSE приходят, latency p50/p99 в логах.
- Verify: `make test-e2e-ping` зелёный. Артефакт: лог latency-распределения.

> **Checkpoint β (CRITICAL):** доказали что архитектура IPC жизнеспособна. **Демо:** `make demo-ping`
> запускает Java↔Go ping/pong с метриками. Решение: продолжать к HBase или редизайнить wire.

### Phase 2 — One hook end-to-end on real HBase 2.5

**T21 — Vendored HBase .proto** *(P2)*
- Deps: T11
- AC: `proto/hbase/` — Cell, Mutation, Get, Result, ClientProtos.* из `hbase-protocol-shaded` (импортируем как git submodule + копия с правильным package alias). `proto/hooks.proto` extends: `PrePutRequest{ Mutation mutation; HookContext ctx; }`, `HookResponse{ оptional bool bypass; optional Error error; }`.
- Verify: round-trip golden test для 5+ HBase-сообщений.

**T22 — Go SDK skeleton: RegionObserver{PrePut,PostPut}** *(P2)*
- Deps: T17, T21
- AC: `pkg/hbasecop`: `type RegionObserver interface { PrePut(ctx, env, mut) (HookResult, error); PostPut(ctx, env, mut) error }`. `type HookResult struct{ Bypass bool }`. `func Run(observers ...RegionObserver) error`. `pkg/hbasecop/env.go`: `type ObserverEnv struct{ TableName, RegionName string }`. Internal: dispatch by hook_id → метод интерфейса.
- Verify: unit test SDK с in-process mock-channel: посылаем PrePut frame → handler вызван с ожидаемой Mutation → ответ корректно сериализован.

**T23 — Java RegionObserver adapter (Put only)** *(P2)*
- Deps: T18, T22, T21
- AC: `bridge.observer.RegionObserverAdapter implements org.apache.hadoop.hbase.coprocessor.RegionObserver`. Реализует только `prePut`/`postPut`. На вызове: serialize в protobuf, send через mux, ждёт RESPONSE с timeout. На bypass=true → `e.bypass()`. На error из Go → `IOException` (strict, default для P2).
- Verify: unit test Java с Mockito: подменяем Channel mock'ом, вызываем `prePut`, проверяем serialization+dispatch+ответ.

**T24 — Mux v0 (single region, single observer)** *(P2)*
- Deps: T17, T18
- AC: `internal/mux` (Go) и `bridge.multiplex` (Java) минимальные: 1 channel, по req_id матчим RESPONSE на REQUEST. Map `req_id → CompletableFuture` (Java) и `req_id → chan Frame` (Go). req_id монотонный uint64. При SHUTDOWN — все pending fail с ChannelClosed.
- Verify: unit test concurrency — 1000 параллельных REQUEST'ов, все RESPONSE сматчены.

**T25 — Coproc-jar packaging (manual, P2)** *(P2)*
- Deps: T18, T23
- AC: `examples/counter-observer/`: Java-класс `CounterRegionObserver extends RegionObserverAdapter` (или регистрируется через config-driven adapter, посмотрим), Go-программа `main.go` с реализацией `RegionObserver`. Maven shade plugin собирает jar с embedded Go ELF. **Без** CLI `hbasecop-build` — упаковка ручная, в pom.xml.
- Verify: `mvn -pl examples/counter-observer package` → артефакт `counter-observer.jar` с Go-бинарём в `bin/linux-amd64/`.

**T26 — HBase 2.5 docker-compose dev cluster** *(P2)*
- Deps: —
- AC: `test/integration/docker-compose.yml`: HBase 2.5.x standalone (zookeeper embedded), volume для coproc-jar'ов, expose 16010. `make hbase-up`/`hbase-down`. Образ: `harisekhon/hbase:2.5` или собственный slim build.
- Verify: `make hbase-up && curl localhost:16010/master-status` → 200.

**T27 — Integration test: Put → Go observer counter increments** *(P2)*
- Deps: T25, T26
- AC: тест `test/integration/PrePutCounterIT.java`: разворачивает HBase, копирует counter-observer.jar, регистрирует coproc на test table через `disable/alterTable/enable`, делает 100 Put, читает счётчик через специальный endpoint (или просто через лог + scan side-table которую observer пишет). Ассерт: 100 hooks вызвалось.
- Verify: `make test-integration` зелёный. Артефакт: лог из Go observer process.

> **Checkpoint γ (PUBLIC-DEMO READY):** MVP smoke-test работает на живом HBase. **Демо:** `make demo-counter` — пользователь видит, как Put на HBase триггерит Go-код. Решение: продолжать к hardening (P3) или сначала собрать feedback.

### Phase 3 — Failure semantics & supervisor production-grade

**T31 — Per-hook policy parsing** *(P3)*
- Deps: T23
- AC: `bridge.config.PolicyConfig` читает из `Configuration` (HBase передаёт): `hbasecop.policy.<hook>` ∈ {`strict`, `best-effort`}, defaults: pre*=strict, post*=best-effort. `hbasecop.timeout.<hook>` (Duration). API: `policy.forHook(hookId)`.
- Verify: JUnit с разными конфигами; default behavior table.

**T32 — Strict mode wiring** *(P3)*
- Deps: T31, T23
- AC: при `strict` + Go error/timeout/process-down → `IOException` к клиенту, операция aborts. При `best-effort` → log WARN, hook возвращает no-op, операция продолжает.
- Verify: integration test с Go-обзёрвером который всегда возвращает Error → strict-prePut → клиент HBase получает IOException; best-effort-postPut → put успешен, в логе WARN.

**T33 — Heartbeat watchdog** *(P3)*
- Deps: T18
- AC: Java-сторона ожидает HEARTBEAT каждые `cfg.HeartbeatPeriod` (500ms default). Если 3 подряд пропущены → пометить процесс как hung → `kill -9` → restart. Конфиг: `hbasecop.heartbeat.period`, `hbasecop.heartbeat.miss-threshold`.
- Verify: integration test: Go-процесс намеренно останавливает heartbeat-горутину на 2s → Java kills + restart; следующий request успешно проходит.

**T34 — Auto-restart with exponential backoff** *(P3)*
- Deps: T18, T33
- AC: supervisor: если процесс умер (либо watchdog, либо exit code) → restart с backoff 200ms→400→800→...→5s, jitter ±20%. После N последовательных fail (default 5) → деградация: пометить coproc как unhealthy, дальнейшие hook-вызовы fail by policy без попыток. Восстановление: каждые 30s probe restart.
- Verify: fault-injection test: Go-процесс падает после каждого 10го запроса; через 1000 запросов supervisor успешно держит SLO для best-effort и предсказуемо fail'ит для strict. Метрики из лога.

**T35 — Inflight handling on crash** *(P3)*
- Deps: T34
- AC: при crash все pending future в mux fail с `GoSideCrashed` исключением. Mux dropping policy: новые REQUEST'ы во время restart wait'ят до `restart-deadline` (default 3s) → fail by policy.
- Verify: stress test: 100 параллельных prePut, kill -9 в середине → все pending получают exception в течение `restart-deadline+ε`, никто не висит.

**T36 — Fault-injection test suite** *(P3)*
- Deps: T35, T26
- AC: `test/integration/fault/`: matrix (strict|best-effort) × (kill-9|hang|exit-1|protocol-error|oom). Каждый кейс: настроенный observer симулирует, тест проверяет (a) корректность семантики, (b) отсутствие data loss/double-apply (через post-state HBase scan), (c) supervisor recovery. ≥10 кейсов.
- Verify: `make test-fault` зелёный. Отчёт-таблица в логе.

> **Checkpoint δ:** production-grade семантика. SDK contract зафиксирован. Решение: open question
> #1 (multi-tenant) — нужно ли в MVP? Это блокатор для P6.

### Phase 4 — Full RegionObserver

**T41 — Hook dispatch table generated from .proto** *(P4)*
- Deps: T22, T23
- AC: для каждого RegionObserver-метода HBase 2.5 — запись в `proto/hooks.proto` (PrePutRequest, PreGetRequest, ..., ~30 шт), генерируется enum `HookId` и dispatch table. Java адаптер: каждый Observer-метод → `dispatch(HookId.PRE_GET, request)`. Go SDK: `RegionObserver` interface растёт до полного списка с дефолтным noop-embedding (`type UnimplementedRegionObserver struct{}`, чтобы пользователь имплементил только нужные).
- Verify: `proto/` обзор всех методов; unit test что каждый HBase-API метод имеет hook_id.

**T42 — Per-hook serialization mappers** *(P4)*
- Deps: T41
- AC: для каждого hook'а Java mapper: HBase native type → protobuf (`Cell` → `proto.Cell`, `Get` → `proto.Get`, etc). Go reverse mapper для аргументов back. Тесты: round-trip каждого типа.
- Verify: 100% .proto messages covered round-trip golden test.

**T43 — Read-path hooks (preGet, preScannerOpen, preScannerNext)** *(P4)*
- Deps: T42
- AC: интеграционный тест: observer модифицирует Get/Scan filter, ассерт что результат отличается.
- Verify: `make test-integration-read` зелёный.

**T44 — Write-path batch hooks (preBatchMutate, postBatchMutate)** *(P4)*
- Deps: T42
- AC: observer ловит batch операции, может частично blocked'ить отдельные мутации.
- Verify: integration test с `Table.batch(List<Put>)`.

**T45 — Storage hooks (preFlush, preCompact, postCompactSelection, ...)** *(P4)*
- Deps: T42
- AC: триггерим flush/compaction через admin API; observer фиксирует вызов.
- Verify: integration test с `admin.flush()` + `admin.majorCompact()`.

**T46 — RegionObserver coverage matrix doc** *(P4)*
- Deps: T43, T44, T45
- AC: `docs/coverage-region-observer.md` — таблица "HBase API method ↔ hook_id ↔ status" (covered/test).
- Verify: 100% строк status=covered; CI gate проверяет наличие тестов на каждый hook (по тегу).

> **Checkpoint ε1:** RegionObserver complete.

### Phase 5 — Other observer types

**T51 — MasterObserver adapter + tests** *(P5)*
- Deps: T41
- AC: все основные `Master*` хуки (preCreateTable, preDeleteTable, preModifyTable, preBalance, preEnableTable, preDisableTable, ~20 шт). Аналогично T41-T42.
- Verify: integration test: observer на `preCreateTable` отклоняет создание по политике → HBase admin получает IOException.

**T52 — RegionServerObserver adapter** *(P5)*
- Deps: T41
- AC: preStop, preRollWALWriterRequest, preReplicateLogEntries, etc.
- Verify: integration test: observer пишет в side-table при `preStop` (graceful shutdown).

**T53 — WALObserver adapter** *(P5)*
- Deps: T41
- AC: preWALWrite, postWALWrite. Замечание: WAL hot-path → особое внимание к latency overhead.
- Verify: bench: WAL write throughput с/без observer (regression < 50% — WAL критичен).

**T54 — BulkLoadObserver adapter** *(P5)*
- Deps: T41
- AC: prePrepareBulkLoad, preCleanupBulkLoad.
- Verify: integration test через `LoadIncrementalHFiles` tool.

> **Checkpoint ε2:** все Observer-типы из SPEC §2 готовы.

### Phase 6 — Multi-region multiplexing

**T61 — Region-scoped routing in mux** *(P6)*
- Deps: T24, T41
- AC: mux header использует `region_id`. Java-сторона ведёт `Map<RegionInfo, RegionId>` (id выдаётся при `start(env)` каждого RegionObserver-инстанса, освобождается на `stop`). Go-сторона: dispatch учитывает region_id (передаётся в `ObserverEnv`).
- Verify: integration test: создаём 4 региона на одной таблице, делаем Put в каждый → Go видит 4 разных region в env, обрабатывает параллельно.

**T62 — Concurrent inflight from N regions** *(P6)*
- Deps: T61
- AC: stress test — 100 регионов × 100 parallel Put → все hooks отрабатывают, без race, без head-of-line blocking (медленный observer на одном region не тормозит другие).
- Verify: bench отчёт: throughput с N регионами линейно масштабируется до core_count Go-side.

**T63 — Lifecycle refcount** *(P6)*
- Deps: T61
- AC: первый `RegionObserver.start()` на RS → spawn Go-процесс. Каждый следующий `start()` → register on existing channel. `stop()` декрементит refcount; на 0 → `SHUTDOWN` + `process.waitFor`.
- Verify: JUnit: 5×start/stop циклов, процесс корректно живёт/умирает; нет ELF-leak в `tmpdir`.

> **Checkpoint ε3:** один Go-процесс обслуживает все регионы. Решение: open question #2 (hot reload), #3 (binary signing). Нужно ли в MVP — gate перед P7.

### Phase 7 — DX: build CLI, examples, docs

**T71 — `hbasecop-build` CLI** *(P7)*
- Deps: T25
- AC: `cmd/hbasecop-build` — берёт `--go-bin`, `--observer-class`, `--coproc-id`, опционально `--policy-config`, выдаёт valid coproc-jar (shaded с bridge + embedded Go ELF + manifest registering observer класс). Заменяет ручную Maven-сборку из T25.
- Verify: `hbasecop-build --go-bin ./bin/audit --observer-class com.example.Audit --out audit.jar` → integration test с этим артефактом успешен.

**T72 — examples/audit-observer** *(P7)*
- Deps: T71
- AC: post-hook example: при каждой Put/Delete пишет audit-row в side table (JSON через `slog`). Code clean, README.
- Verify: integration test пишет 50 ops → 50 audit rows.

**T73 — examples/ttl-validator** *(P7)*
- Deps: T71
- AC: pre-hook example: блокирует Put если value не содержит TTL-aware structure. Демо strict mode.
- Verify: integration test: invalid Put → IOException; valid → success.

**T74 — README + getting started** *(P7)*
- Deps: T72, T73
- AC: top-level README: что делает, install, "Hello observer" за 5 минут (включая `hbasecop-build` команду), линки на examples и SPEC, FAQ (failure modes, troubleshooting). Достаточно сделать quick-start не читая SPEC.
- Verify: ревьюер проходит по README с нуля и собирает первый observer.

**T75 — Architecture doc** *(P7)*
- Deps: T74
- AC: `docs/architecture.md` — диаграмма + описание потока запроса (HBase RPC → Java adapter → mux → shmem → Go runtime → handler → ответ). Failure modes ссылаются на SPEC §3.
- Verify: ревью.

> **Checkpoint ε4:** release candidate ready.

### Phase 8 — Bench, harden, release

**T81 — Bench harness: latency overhead** *(P8)*
- Deps: T27, T62
- AC: `test/bench/`: vs Java-only baseline (no-op Java RegionObserver), vs go-hbase no-op observer. Измеряем p50/p95/p99 latency на prePut/postPut, batch size 1/100/1000. JMH или собственный harness. Цель MVP: <100µs p50 overhead на prePut.
- Verify: бенч-отчёт. Если не достигаем — анализ + iteration.

**T82 — Bench: throughput WAL/flush** *(P8)*
- Deps: T81
- AC: throughput WAL writes c WALObserver vs без — regression < 50%.
- Verify: бенч-отчёт.

**T83 — Fuzz wire codec** *(P8)*
- Deps: T12, T13
- AC: Go fuzz target на `wire.Decode`, Java jqf-fuzz target. 30 минут CPU. Найденные баги — фикс + regression test.
- Verify: фуз-репорт; регресс-тесты в codebase.

**T84 — Soak/long-run test** *(P8)*
- Deps: T36, T62
- AC: 1 час прогон стресс-нагрузки (1000 ops/s, mixed read/write) с периодическими kill -9 → отсутствие data loss, отсутствие memory leak (RSS ровный), отсутствие Go-supervisor-zombie.
- Verify: soak-отчёт + RSS plot.

**T85 — v0.1.0 release** *(P8)*
- Deps: T81-T84, T75
- AC: tag `v0.1.0`, GitHub release с changelog, артефакты `hbasecop-bridge-0.1.0.jar` + `hbasecop-build-linux-amd64`, поднятый README с install-from-release.
- Verify: clean clone → `make all` + manual test by user.

> **Checkpoint ζ:** v0.1.0 released. Курсовая защищена :)

## 4. Open question gating

| Open Q                              | Decide before    | Влияние                           |
|-------------------------------------|------------------|-----------------------------------|
| #1 Multi-tenant (>1 jar на RS)      | start of P6      | Меняет lifecycle/refcount design  |
| #2 Hot reload Go-binary             | start of P7      | Может потребовать SIGHUP-flow     |
| #3 Подпись/checksum Go-бинаря в jar | start of P7/P8   | Доп. шаг в `hbasecop-build`       |

## 5. Risks & mitigations

| Риск                                                     | Вероятность | Impact | Mitigation                                                                  |
|----------------------------------------------------------|-------------|--------|-----------------------------------------------------------------------------|
| `java-go-shmem` API ещё в движении / breaking changes    | средняя     | high   | T14 пинит точную версию; рассмотреть submodule для `third_party/`           |
| `hbase-protocol-shaded` package-путь breaks между patch  | низкая      | med    | Vendoring `.proto` копий с собственным package в P2/T21                     |
| Latency target <100µs не достижим из-за shmem busy-spin  | средняя     | med    | P8 даёт буфер; альтернатива — futex-based wait в форке shmem                |
| WALObserver overhead убивает throughput                  | высокая     | high   | T53 + T82: явный bench-gate; возможный fallback — sampling-only WAL hooks   |
| Go-process kill -9 при 1000 inflight теряет данные       | средняя     | high   | T34/T35 + T36 fault-injection как обязательный gate                         |
| HBase 2.5 patch versions меняют RegionObserver signature | низкая      | med    | Pin minor 2.5.x; CI matrix на 2.5.0 и latest 2.5                            |
| Embedded Go ELF в jar > 50MB → медленный coproc-load     | средняя     | low    | UPX или strip + `-trimpath -ldflags "-s -w"`; merit для shared-mode (1 ELF) |

## 6. Estimates (calendar, single dev, evenings/weekends)

| Phase | Tasks | Est.   |
|-------|-------|--------|
| P0    | 6     | 1 нед  |
| P1    | 9     | 2-3 нед |
| P2    | 7     | 2 нед  |
| P3    | 6     | 2 нед  |
| P4    | 6     | 2 нед  |
| P5    | 4     | 1-2 нед |
| P6    | 3     | 1 нед  |
| P7    | 5     | 1-2 нед |
| P8    | 5     | 2 нед  |
| **Total** | **51** | **~14-17 нед** |

Курсовая = ужать P5/P7/P8 если давит дедлайн; P3/P6 — нельзя срезать (это inkjet к "архитектурно интересному").

## 7. How to use this plan

- `tasks/todo.md` — flat checkbox список тех же T## для tracking. Вычеркиваем по мере готовности.
- Каждый Checkpoint = sync point с пользователем (демо + решение продолжать/корректировать).
- Любая правка `SPEC.md` → reflect здесь в течение того же коммита.
- Новые задачи (выявленные в ходе работы) → добавлять в фазу по смыслу, не в конец.
