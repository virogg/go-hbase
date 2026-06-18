# Tier 2 — Endpoint Coprocessors

> Отдельный пост-v0.1.0 workstream. НЕ входит в `tasks/plan.md` (мастер-план Phase 0–8).
> Нумерация задач намеренно в своём пространстве `TE##`, чтобы не сталкиваться с `T01–T85`.

## Context (зачем)

HBase-копроцессоры делятся на две семьи: **Observer** (event-перехватчики на путях
чтения/записи) и **Endpoint** (`CoprocessorService` / `getServices()` — клиент-инициируемый
серверный RPC, исполняемый *в контексте БД* с доступом к локальным данным региона). go-hbase
сегодня **observer-only**: тонкий Java-мост форвардит хуки через java-go-shmem SPSC-кольца в
out-of-process Go-рантайм; управление одностороннее (Java→Go запрос, Go→Java вердикт), обратного
канала «Go зовёт HBase» нет.

Цель — добавить **data-local endpoint'ы**: клиент зовёт серверную Go-логику, которая
**императивно** читает данные региона (scan/get, с data-dependence: прочитал A → решил прочитать B)
и опционально пишет. Это вторая модальность той же идеи «логика в контексте БД»; подменять её
сетевым клиентом нельзя (это ровно то, что и так возможно и от чего проект уходит).

Зафиксированные решения:
- **(A) CoralRing Non-Waiting-кольцо — вырезано.** Проверка кода: текущее кольцо уже даёт
  reliable + non-blocking-poll (`producer.NextToDispatch→ErrRingFull`,
  `consumer.Fetch→ErrNoData` — мгновенно, без блокировки), а funnel + goroutine-per-request уже
  реализованы (`internal/cpruntime/loop.go`). Non-Waiting нужен лишь для lossy-телеметрии, которая
  и так не блокирует. На MVP телеметрию не трогаем (остаётся на существующем lane). Оставляем только
  резерв поля `channel_id` для будущей многоканальности.
- **read-first:** запись (MUTATE) — за флагом, off по умолчанию (фаза E4).
- **Master-endpoint'ы** — отдельная меньшая вещь (нет региона), не тянут region-reverse-RPC.

## Целевая архитектура (сжато)

**Lanes (не 5, а 2–3 кольца/процесс):**
- E2 (stateless) едет на **существующей** паре колец — новые lanes не нужны.
- E3 (реверс-чтения) добавляет **второе J→G кольцо** под bulk `RpcResponse` (данные scan, до
  слота ≈ 1 MiB), чтобы объёмный ответ не вставал перед hook-инвоком (HoL-изоляция). G→J остаётся
  одним кольцом (`HOOK-RESP` + мелкие `RpcRequest` + heartbeat/log). Итого 3 кольца (2 J→G + 1 G→J).
- Резерв `channel_id` (TE01) позволяет адресовать кольца; телеметрия-lane отложена.

**Реверс-RPC:** Go-хендлер пишет обычный императивный код (`env.Scan`, `env.Get`), под капотом —
async поверх lanes. Корреляция по wire-header `req_id` (роутер НЕ PB-декодит); `call_id`
(id endpoint-вызова) — в payload только для группировки сканеров по вызову.

**Потоки (anti-deadlock — критично):**
- Java RS-handler пишет `EndpointInvoke`, блокируется на `call_id` до `EndpointResult`/таймаута.
- **Отдельный bounded** пул «RPC-servicing» исполняет реверс scan/get/mutate против `Region`
  (резолв по `region_id` через `RegionIdAllocator`); НИКОГДА не тот поток, что заблокирован на вызове.
  Пул **fail-closed**: не дождался слота за дедлайн → ошибка в Go, не безграничный блок.
- Go: goroutine-per-endpoint-call (уже есть модель) блокируется на per-`req_id` канале; единый
  out-writer (уже есть) — funnel.

**Жизненный цикл сканера:** реестр по `(call_id, scanner_id)`; закрытие всех сканеров вызова при
завершении/таймауте/краше Go (подписка на существующий `Multiplexer.pauseInflightFailing`);
idle-lease; bytes-primary батчинг (одно сообщение ≤ слот).

**Регистрация:** мост отдаёт ОДИН generic-Service `GoEndpointService{Call(method,bytes)→(bytes,err)}`
через `getServices()` (на `Generic{Region,Master}Coprocessor`). Клиент: тонкий helper над
`Table.coprocessorService` (region fan-out + reduce) и `Admin` (master).

## Code-grounded findings (учтены в плане)

- **F1/F2:** funnel + goroutine-per-request и non-blocking-poll **уже есть** → (A) урезан.
- **F3:** `Coprocessor.getServices()` возвращает **unshaded** `com.google.protobuf.Service`;
  проект уже тянет unshaded `protobuf-java` 3.25.5 → регистрация совместима by-construction
  (подтвердить в TE21).
- **F4 (ловушка):** есть ДВА `ProtobufUtil` (unshaded legacy ↔ shaded). Серверная конверсия
  vendored-pb (`proto/hbase`, unshaded) → native `Get/Scan/Mutation` должна идти через **shaded**
  `ProtobufUtil`, не legacy. Отдельная задача TE31.
- **F5:** `RegionIdAllocator` уже даёт стабильный `region_id` для маршрутизации реверса.
- **F6:** реальный лимит сообщения = слот кольца (`HBASECOP_RING_MAX_OBJECT_SIZE`, дефолт 1 MiB),
  одно сообщение/слот, кросс-слот reassembly не поддержан → pull-scan батчит bytes-primary.

## Dependency graph

```
E0  TE01 channel_id(@96) → TE02 multi-ring addressing
E1  TE11 wire v2 (types+oneof, lockstep Go+Java) → TE12 reader demux→stub (req_id correlation)
E2  TE21 getServices/GoEndpointService → TE22 Invoke/Result (на сущ. паре) → TE23 Go SDK Endpoint
                                                              └→ TE24 timeout/panic/crash
E3  TE31 2-е J→G кольцо + servicing-pool + shaded-конверсия  (РИСК)
        → TE32 GET (data-dependent) → TE33 pull-scan+lifecycle+leak-reaping → TE34 Go SDK EndpointEnv
E4  TE41 MUTATE(gated)+reentry · TE42 caps/admission · TE43 master endpoints
E5  TE51 region client helper · TE52 admin helper · TE53 packaging · TE54 IT/fault-matrix+docs
```
Критический путь: TE01→TE11/12→TE21/22→TE31→TE32→TE33→TE34→TE42→TE54.

## Задачи (вертикальные срезы)

Конвенция Verify: live-HBase IT через новый `make test-integration-endpoint*` (по образцу
`test-integration-master`: build coproc-jar → `compose up --build` → `wait-master-status.sh` →
IT → собрать логи → `compose down`). Новые ключи `hbasecop.endpoint.*` валидируются в
`ConfigPreflight` (неизвестные — WARN, известные — проверка значения).

### Phase E0 — примитив (минимум)
- **TE01: резерв `channel_id` (uint32 @offset 96)** *(E0)* — Deps: none. AC: Go (`ring.go`) и Java
  (`WaitingRing*`) читают/пишут `channel_id` байт-совместимо; существующие кольца по умолчанию
  `channel_id=0` и бит-идентичны текущим. Verify: `go test ./third_party/.../ring/...` + Java
  `WaitingRing*Test` + кросс-язык round-trip (Go записал → Java прочитал).
- **TE02: адресация нескольких колец/процесс** *(E0)* — Deps: TE01. AC: открытие 2 колец без
  алиасинга заголовков; математика capacity/slot не меняется. Verify: unit-тест 2 producer + 2
  consumer, отправка на оба, нет cross-talk.

> **Checkpoint E0:** два независимых кольца обмениваются фреймами в одном процессе Go↔Java,
> байт-совместимо. **Демо:** parity-тесты зелёные. Go/no-go: решение пользователя.

### Phase E1 — плумбинг (без изменения поведения, фичефлаг off)
- **TE11: wire v2 — новые type-байты + поля `Frame` oneof, в lockstep** *(E1)* — Deps: none.
  AC: `EndpointInvoke/EndpointResult/RpcRequest/RpcResponse` добавлены в `proto/wire.proto`,
  `internal/wire/frame.go` (`Type`,`Valid()`) и Java `FrameType`/`WireFormat` одним изменением;
  старые типы не тронуты; номера полей только дописаны, не переиспользованы. Verify: Go round-trip
  + Java Decoder/Encoder тесты + кросс-язык golden-frame.
- **TE12: демукс ридера на stub-реверс-хендлер** *(E1)* — Deps: TE11. AC: Java-ридер роутит
  `RpcRequest`→stub, Go-ридер `RpcResponse`→stub-waiter, корреляция по header `req_id`; PB не
  декодится в роутере; неизвестные типы — прежний WARN+skip. Verify: unit-тест инъекции каждого
  нового типа → сработал нужный stub.

> **Checkpoint E1:** весь транспорт-плумбинг на месте, endpoints OFF, **каждый существующий IT
> зелёный** (ноль изменений поведения). **Демо:** нет регрессии латентности на `make
> test-integration` и `LatencyBenchIT`. Go/no-go.

### Phase E2 — stateless endpoint end-to-end (вертикаль)
- **TE21: `GoEndpointService` через `getServices()` (unshaded protobuf)** *(E2)* — Deps: TE11.
  AC: generic `com.google.protobuf.Service` `Call(method,bytes)→(bytes,err)` зарегистрирован на
  `Generic{Region,Master}Coprocessor`; клиент через `Table.coprocessorService` инвокит `Call`,
  мост форвардит как `EndpointInvoke`. Verify: IT — клиент зовёт `Call` → мост логирует invoke
  (Go ещё нет) → round-trip method/payload.
- **TE22: round-trip `EndpointInvoke`/`EndpointResult` на СУЩЕСТВУЮЩЕЙ паре** *(E2)* — Deps: TE12,
  TE21. AC: handler-поток пишет invoke через `Multiplexer.call`, блокируется на future, Go отдаёт
  result; reuse `sendLock`+`Multiplexer`. Verify: `make test-integration-endpoint` — клиент шлёт X,
  Go возвращает f(X), ассерт.
- **TE23: Go SDK `Endpoint` + `RunEndpoint`/`RunAll`** *(E2)* — Deps: TE22. AC: `Endpoint{Call(ctx,
  EndpointEnv, method, payload)([]byte,error)}`; диспетч (по образцу `dispatch.go`, но single-handler
  — endpoint'ы не чейнятся как observer'ы); endpoint, зарегистрированный через `RunAll` рядом с
  observer'ами, вызывается. Verify: in-process harness + IT из TE22 уже через SDK.
- **TE24: ошибки — таймаут / panic→error / crash→strict-fail** *(E2)* — Deps: TE22. AC: новый
  `hbasecop.endpoint.timeout`; panic ловится (reuse `recoverInvoke`), отдаёт ошибку; SIGKILL
  in-flight → клиент падает быстро (reuse `pauseInflightFailing`). Verify: fault-IT варианты
  (паттерн `FaultMatrixIT` + `HBASECOP_FAULT_*`).

> **Checkpoint E2:** **stateless** endpoint (напр. хэш/трансформация payload'а) вызывается реальным
> клиентом на живом кластере; таймаут + panic + crash доказаны. **Демо:** первый user-visible
> endpoint. Go/no-go: если уже видно давление на handler-пул (A-1) — пересмотреть до E3.

### Phase E3 — реверс-чтения (сердце; в основном вертикаль)
- **TE31: 2-е J→G кольцо + servicing-pool + native-конверсия** *(E3)* — Deps: TE02, TE12. AC:
  bounded named-пул потребляет `RpcRequest`, резолвит `Region` по `region_id`, конвертит vendored-pb
  → native через **shaded** `ProtobufUtil` (F4), исполняет, отдаёт `RpcResponse` по 2-му J→G кольцу;
  пул **fail-closed** при насыщении (ошибка, не блок); servicing-поток ≠ заблокированный handler
  (A-1). Verify: IT — seed строка, реверс-GET, ассерт значения; unit-тест насыщения пула.
  **(Самая рискованная задача — нет аналога в текущем коде; спайк до начала E3.)**
- **TE32: `RpcRequest{op=GET}` round-trip (data-dependent)** *(E3)* — Deps: TE31. AC: `env.Get`
  шлёт `RpcRequest(GET)`, goroutine блокируется на header-`req_id`, in-pump будит; работает
  «прочитал A → прочитал B по ключу из A». Verify: IT с итеративным паттерном A→B.
- **TE33: pull-scan SCAN_OPEN/NEXT/CLOSE + реестр + reaping** *(E3)* — Deps: TE32. AC: lifecycle по
  `(call_id, scanner_id)`; bytes-primary батчинг с `has_more` (резюмируемо); одна ячейка > слота →
  чистая ошибка (не краш-цикл); **после SIGKILL посреди scan — ноль утёкших RegionScanner**
  (reaping через `pauseInflightFailing`). Verify: IT (нормальный scan) + fault-IT (crash mid-scan →
  счётчик сканеров вернулся к baseline по JMX/метрикам).
- **TE34: Go SDK `EndpointEnv.OpenScanner/Get`** *(E3)* — Deps: TE33. AC: эргономичный API; close/defer;
  агрегирующий endpoint сканирует + суммирует колонку. Verify: канонический IT «server-side SUM».

> **Checkpoint E3:** канонический endpoint — **серверная агрегация по локальным данным региона**,
> включая data-dependent путь, с доказанной leak-safety сканеров. **Демо:** ради чего всё затевалось.
> Go/no-go: fault-matrix чтений зелёный.

### Phase E4 — запись + лимиты + master
- **TE41: реверс `MUTATE` (gated off) + решение по re-entry** *(E4)* — Deps: TE33. AC: реверс-мутация
  (PUT/DELETE) через `Region`-уровень — `region.put`/`region.delete`, как штатный
  `MultiRowMutationEndpoint`. Обходит клиентский RPC-стек (RSRpcServices/ACL/quota/throttling), но
  **не** observer-pipeline: `Pre/PostPut`, `Pre/PostDelete`, `Pre/PostBatchMutate` стреляют штатно —
  в HBase 2.5 публичного API записи в обход обсерверов нет (подтверждено байткодом 2.5.10:
  `Region.put|delete|batchMutate|mutateRow|mutateRowsWithLocks` → `HRegion.doMiniBatchMutate` →
  `MutationBatchOperation` вызывает хуки; `MultiRowMutationEndpoint` пишет так же). Off без
  `hbasecop.endpoint.allow-mutate=true` (coproc-property или cluster-wide hbase-site). NB: флаг
  читается ОДИН раз на shared-runtime (по `coproc-id`, как весь `hbasecop.endpoint.*`), не per-table —
  задавать согласованно для всех таблиц одного jar на RS; per-table enforcement отложен в TE42.
  **Re-entry-решение (Option 1):** deadlock-safe by construction — servicing-pool-поток ≠
  заблокированный на `EndpointResult` handler-поток, Go goroutine-per-request обслуживает форвард
  prePut на отдельной горутине, fail-closed пул конвертит насыщение в ошибку; рекурсия
  `postPut→reverse-mutate→postPut` — ответственность автора endpoint'а (как в ванильном HBase, guard'а нет).
  Verify: IT оба состояния флага + reentry-стресс-IT.
- **TE42: лимиты + admission control + per-table gate + reaping-races** *(E4)* — Deps: TE33. AC:
  `hbasecop.endpoint.{max-concurrent-calls, max-scanners-per-call, max-bytes-per-resp,
  max-rows-per-next, scanner-idle-lease}`; превышение → определённая ошибка, не hang/OOM; admission
  отбивает лишние конкурентные вызовы (защита handler-пула RS, A-1). **+ per-table `allow-mutate`
  enforcement** (перенесено из TE41): servicer гейтит по конфигу таблицы вызывающего региона, не по
  baked-флагу — переиспользует per-call/per-region config-путь admission. **+ 3 crash-window
  scanner-гонки** (перенесено из CP-E3): getScanner-before-register, closeAll-vs-register,
  scanner-before-OPEN-reply — закрыть окна в `ScannerRegistry`/reaping-пути. Verify: IT трогает
  каждый лимит, нормальный трафик идёт; per-table gate IT (две таблицы, разный флаг); fault-IT на
  каждую гонку (crash в окне → ноль утёкших сканеров).
- **TE43: master-endpoint'ы (без региона)** *(E4)* — Deps: TE21. AC: `MasterCoprocessor.getServices()`
  + `Admin.coprocessorService`; scope = чтение master/meta-стейта, НЕ region-реверс (A-12). Verify:
  IT через `Admin.coprocessorService` (bring-up по образцу `MasterPolicyIT`).

> **Checkpoint E4:** ограниченные, безопасные endpoint'ы с опциональной записью и рабочий
> master-endpoint. **Демо:** cap/fault-matrix зелёный; граница ACL-bypass задокументирована и
> отревьюена. Go/no-go.
>
> **Closed 2026-06-18:** Phase E4 (TE41 reverse MUTATE · TE42 limits/admission/per-table-gate/
> reaping-races · TE43 master endpoints) shipped, все live IT зелёные (EndpointRoundTripIT 9/9,
> EndpointFaultIT 3/3, MasterEndpointIT 1/1 на HBase 2.5.11). **ACL-bypass граница задокументирована**
> в `docs/endpoint-security.md`: endpoint исполняется с authority RegionServer'а (не клиента);
> инвок гейтится EXEC-правом, запись — `allow-mutate` (off by default, per-table); reverse-операции
> обходят клиентский data-ACL и RPC-стек, но не observer-pipeline — та же модель, что у штатных
> `AggregateImplementation`/`MultiRowMutationEndpoint`. **Deferred → TE54** (E5 fault-matrix):
> confirmatory live ITs admission-stress / idle-lease-recovery / crash-vs-register (логика
> unit-доказана). Next: Phase E5.

### Phase E5 — клиент, упаковка, доки, fault-matrix
- **TE51: region client helper** *(E5)* — Deps: TE22/TE34. AC: один вызов агрегирует по всем регионам
  таблицы (fan-out + reduce). Verify: multi-region IT (пресплит как `SharedRegionProcessIT`).
- **TE52: admin master-endpoint helper** *(E5)* — Deps: TE43. AC: одновызовный master-endpoint. Verify: IT.
- **TE53: упаковка** *(E5)* — Deps: TE21. AC: `getServices()` вшит в стоковые `Generic*Coprocessor`;
  `hbasecop-build package/deploy` регистрирует Service; `ConfigPreflight` знает `hbasecop.endpoint.*`;
  ноль ручной Java для деплоя endpoint'а. Verify: smoke `hbasecop-build` + deploy-IT.
- **TE54: IT + fault-matrix + доки** *(E5)* — Deps: все. AC: консолидированные
  `make test-integration-endpoint*`; fault-кейсы: crash mid-invoke, crash mid-scan (leak),
  **долгий endpoint vs watchdog** (heartbeat не должен пропасть → ложный рестарт), cap-trips,
  mutate-reentry, oversized-cell; детерминированный исход у каждого; доки SPEC/README/ADR обновлены
  (ACL-bypass + CP-reentry + handler-pinning). Verify: полный fault-matrix IT зелёный в CI.

> **Checkpoint E5 (GO/NO-GO релиз):** каноническая агрегация по multi-region таблице через client
> helper, развёрнута без ручной Java, полный fault-matrix зелёный, доки по безопасности/эксплуатации
> на месте.

## Риски

- **TE31 (самый рискованный):** единственная задача без аналога в текущем коде; концентрирует
  deadlock-поверхность (A-1/A-2: размер servicing-пула, не-reentry handler-потока, обход
  CP-pipeline при mutate), ловушку двух `ProtobufUtil` (F4) и привязку leak-reaping к крах-пути.
  → спайк «vendored-pb → `Region.get(nativeGet)` на 2.5.x» + нагрузочный тест пула до E3.
- **TE33:** корректность reaping сканеров под крахом (read-points блокируют compaction cleanup).
- **Governance A-1/A-2:** жёсткий wall-clock таймаут endpoint'а + admission-cap обязательны, иначе
  endpoint'ы — DoS-вектор против handler-пула RegionServer.
- **Wire-skew (A-6):** новые type-байты должны приземляться lockstep Go+Java; Go-ELF version-locked
  с jar (manifest SHA) → нет skew на деплое, но есть на dev-этапе E1→E2.

## Verification (end-to-end)

- Каждая фаза заканчивается live-HBase IT (`make test-integration-endpoint*`), не только unit.
- Fault-matrix (TE54) — обязательное условие релиза.
- Прогон на обеих CI-версиях (HBase 2.5.0 / 2.5.11), как остальная интеграция.

## Критические файлы

- `java/.../bridge/CoprocessorRuntime.java` — `sendLock` funnel, демукс ридера, крах-путь;
  + 2-е кольцо + reverse-servicing-пул.
- `java/.../multiplex/Multiplexer.java` — корреляция `req_id` + `pauseInflightFailing`;
  + reverse-waiters + хук reaping сканеров.
- `internal/cpruntime/loop.go` — единый writer-funnel + goroutine-per-request; + reverse in-pump
  + 2-й consumer.
- `internal/wire/frame.go` (+ `proto/wire.proto`) — wire v2, lockstep Go+Java.
- `pkg/hbasecop/dispatch.go` (+ `env.go`, `hooktable.go`) — `Endpoint`, `EndpointEnv` реверс-операции,
  `RunEndpoint`/`RunAll`.
- `java/.../bridge/entrypoint/GenericRegionObserver.java` (+ `GenericCoprocessor.java`) — override
  `getServices()` с unshaded `GoEndpointService`.
- `third_party/java-go-shmem/.../ring/ring.go` + Java `WaitingRing*` — `channel_id` @offset 96.

## Чеклист

### Phase E0 — примитив
- [x] TE01 резерв `channel_id` (uint32 @offset 96)
- [x] TE02 адресация нескольких колец/процесс
- [ ] **CP-E0:** два независимых кольца Go↔Java, parity зелёный

### Phase E1 — плумбинг (поведение не меняется)
- [x] TE11 wire v2 (новые типы + oneof, lockstep Go+Java)
- [x] TE12 демукс ридера на stub-реверс-хендлер (корреляция `req_id`)
- [x] **CP-E1:** endpoints OFF, существующие IT зелёные (counter + master на живом HBase 2026-06-17), нет регрессии

### Phase E2 — stateless endpoint end-to-end
- [x] TE21 `GoEndpointService` через `getServices()` (unshaded protobuf) — core+unit; live IT в TE22
- [x] TE22 round-trip `EndpointInvoke`/`EndpointResult` — **live IT зелёный** (client→bridge→Go→`HELLO`,
      HBase 2.5.11 2026-06-17). Потребовало protobuf-реархитектуры (см. ниже).
- [x] TE23 Go SDK `Endpoint` + `RunAll`-интеграция (диспетч + panic-recover; покрыто unit + endpoint IT)
- [x] TE24 выделенный `hbasecop.endpoint.timeout` (30s default, preflight-валидируется) + fault-IT:
      panic→client error (процесс жив), crash mid-call→prompt error + восстановление. Зелёный на живом HBase.
- [x] **CP-E2:** stateless endpoint на живом кластере; round-trip + panic + crash доказаны (EndpointRoundTripIT + EndpointFaultIT)

> **Protobuf-реархитектура (TE22, потребовалась для endpoint-boundary):** внутренние wire/hooks/hbase
> protobuf переведены с `com.google.protobuf` (3.25.5) на HBase shaded `org.apache.hbase.thirdparty.com.google.protobuf`
> (server-provided, antrun-rewrite сгенерированного кода); endpoint Service генерится protoc 2.5.0 (proto2,
> `proto-endpoint/`) против server-provided `com.google.protobuf` 2.5.0. Coproc-jar **не бандлит protobuf**.
> Tier-1 регрессия (counter IT) зелёная. Корневая причина: HBase 2.5 CPEP-API жёстко привязан к unshaded
> protobuf 2.5.0, а jar-first CoprocessorClassLoader не делегирует `com.google.protobuf` родителю.

### Phase E3 — реверс-чтения
- [x] TE31 2-е J→G кольцо + servicing-pool + shaded-конверсия — **live reverse-GET IT зелёный**
      (EndpointRoundTripIT seed→reverse-GET→assert, HBase 2.5.11, 2026-06-18); F4 shaded-конверсия
      (byte-identity) + fail-closed пул насыщения unit-доказаны
- [x] TE32 `RpcRequest{GET}` round-trip (data-dependent A→B) — `EndpointEnv.Get` (планируемая форма
      `Call(ctx, env, method, payload)`); **live IT зелёный** (EndpointRoundTripIT "follow" A→B,
      HBase 2.5.11, 2026-06-18); env.Get/CellValue + data-dependent unit-доказаны
- [x] TE33 pull-scan SCAN_OPEN/NEXT/CLOSE + реестр + leak-reaping — wire scanner_id/has_more;
      `ScannerRegistry`(call_id,scanner_id) + bytes-primary resumable батчинг (ScannerContext
      BETWEEN_ROWS) + oversized-row чистая ошибка; reaping на крах-пути (`pauseInflightFailing`).
      **live IT зелёный** (scan 5 rows; crash-mid-scan → "reaped 1 orphaned scanner" → recovery,
      HBase 2.5.11, 2026-06-18). NB: leak-proof via unit-reap + наблюдаемый reap-лог (проект
      log-only, без JMX — отклонение от плана "JMX/метрики")
- [x] TE34 Go SDK `EndpointEnv.OpenScanner/Get` — эргономичный API (Get TE32 + OpenScanner/Scanner
      TE33); канонический server-side SUM endpoint (агрегация в контексте региона). **live IT
      зелёный** (EndpointRoundTripIT server-side SUM = 15, HBase 2.5.11, 2026-06-18)
- [x] **CP-E3:** серверная агрегация по данным региона + leak-safety сканеров — достигнут
      2026-06-18. GET/data-dependent A→B/pull-scan/server-side SUM на живом HBase 2.5.11;
      crash-mid-scan → reaped scanner → recovery доказан. Adversarial review (11 confirmed findings,
      2 HIGH) → must-fix исправлены (non-blocking reject off reader-thread, Go reverse-call deadline)
      + hardening (per-call scanner reaping, closeQuietly). Deferred → TE42: 3 narrow crash-window
      races (getScanner-before-register, closeAll-vs-register, scanner-before-OPEN-reply)

### Phase E4 — запись + лимиты + master
- [x] TE41 реверс `MUTATE` (gated off) + re-entry-решение (Option 1) — `EndpointEnv.Put/Delete` →
      `RpcRequest{MUTATE}` → `ReverseRpcServicer` конвертит vendored MutationProto → native через
      **shaded** `ProtobufUtil` (`ReverseGetConverter.toNativeMutation`, PUT/DELETE) и пишет
      `region.put/delete`. Обсерверы стреляют (как штатный `MultiRowMutationEndpoint`; в обход только
      клиентского RPC-стека — в HBase 2.5 публичного API записи без обсерверов нет, подтверждено
      байткодом 2.5.10). Gate `hbasecop.endpoint.allow-mutate` (off; coproc-property или
      hbase-site; `ConfigPreflight` валидирует). NB: читается раз на shared-runtime (per `coproc-id`),
      не per-table — задавать согласованно; per-table enforcement → TE42. Re-entry: servicing-pool ≠
      заблокированный handler + goroutine-per-request → no self-deadlock; рекурсия `postPut→mutate` — на авторе.
      **live IT зелёный** (EndpointRoundTripIT 8/8, HBase 2.5.11, 2026-06-18): rejected-when-off
      (row unwritten) / writes-when-on / reentry-stress (20 concurrent read-then-write на один регион).
      Unit: converter PUT/DELETE/unsupported, servicer gate+apply+unknown-region, env.Put/Delete,
      preflight true|false|garbage, buildConfig default-off+override.
- [x] TE42 лимиты + admission + per-table gate + reaping-races — 5 ключей
      `hbasecop.endpoint.{max-concurrent-calls=8, max-scanners-per-call=16, max-bytes-per-resp=1MiB,
      max-rows-per-next=1000, scanner-idle-lease=2m}` (Config+preflight). **Admission**: Semaphore в
      `invokeEndpoint` (tryAcquire→fail-fast, защита handler-пула RS, A-1). **Scanner-лимиты**:
      per-call cap (AT_CAPACITY) + idle-lease (injected clock, `evictIdle` на scheduler-tick).
      **Scan-reply**: byte-ceiling=min(slot,max-bytes) + row-cap через per-row `nextRaw`-loop.
      **Per-table allow-mutate**: `serviceMutate` гейтит по coproc-property таблицы вызывающего
      региона (override) → baked cluster-default — фикс F2 shared-runtime leak. **Reaping-гонки**
      (CP-E3 deferral): `ScannerRegistry` переписан на per-call `closing`-tombstone + `compute`-атомарность —
      register проигравший гонку с reap-свипом возвращает REJECTED и закрывает свой scanner
      (закрыты #2 closeAll-vs-register, #3 scanner-before-OPEN-reply; #1 невозможна intra-call).
      Unit: registry concurrency-stress/cap/idle-lease, servicer cap/byte/row/per-table-gate, config.
      **live IT зелёный** (EndpointRoundTripIT 9/9 + per-table-gate две таблицы/один runtime,
      EndpointFaultIT 3/3, HBase 2.5.11, 2026-06-18). Deferred → TE54 fault-matrix: confirmatory
      live ITs для admission-stress / idle-lease-recovery / crash-vs-register (логика unit-доказана).
- [x] TE43 master-endpoint'ы (без региона) — `GenericMasterObserver.getServices()` отдаёт generic
      `GoEndpointService` (region_id 0); клиент зовёт через `Admin.coprocessorService()` поверх
      master `CoprocessorRpcChannel` (unshaded). endpoint-observer регистрирует ещё и
      `UnimplementedMasterObserver` → бинарь деплоится как master-coproc без abort'а master-init.
      Scope (A-12): чтение master/meta-стейта, region-реверс недоступен (region_id 0). **live IT
      зелёный** (MasterEndpointIT 1/1 → "MASTER-HELLO", HBase 2.5.11, 2026-06-18); make
      `test-integration-master-endpoint`.
- [x] **CP-E4** достигнут 2026-06-18 — E4 (TE41/TE42/TE43) shipped, live IT зелёные;
      ACL-bypass граница в `docs/endpoint-security.md` (endpoint = authority RS, инвок гейтится EXEC,
      запись — allow-mutate off-by-default). Deferred → TE54: admission/idle-lease/crash live fault-ITs.

### Phase E5 — клиент, упаковка, доки
- [x] TE51 region client helper (fan-out + reduce) — `com.virogg.hbasecop.client.EndpointClient`:
      `callAllRegions`/`callRegions` fan `GoEndpointService.Call(method,payload)` out over a table's
      regions via `Table.coprocessorService` (unshaded controller+callback, extracted from the
      inline IT path); `callAndReduce` folds the per-region payloads into one aggregate. **live IT
      зелёный** (EndpointMultiRegionIT 1/1 — 4-way pre-split, "sum" partials {3,7,11,15} reduce to
      table total 36, HBase 2.5.11, 2026-06-18); make `test-integration-endpoint-multiregion`.
      Unit: fold/raw-map/empty-payload/error-surfacing/null-response/controller-failure (Mockito).
- [ ] TE52 admin master-endpoint helper
- [ ] TE53 упаковка (getServices в Generic*, hbasecop-build, preflight)
- [ ] TE54 IT + fault-matrix + доки
- [ ] **CP-E5 (GO/NO-GO релиз):** агрегация по multi-region таблице, zero-Java деплой, fault-matrix зелёный

## Открытые вопросы

- MUTATE — оставляем в E4 (gated off), или полностью выносим за рамки этого плана (только чтение)?
- Дублировать ли чекбоксы TE## в `tasks/todo.md`, или этот файл — единственный источник истины для Tier 2?
