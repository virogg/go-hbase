# go-hbase — выполнение пользовательской логики внутри HBase на языке Go

> **Курсовая работа · отчёт-презентация**
> **Тема:** интеграция доменной логики на Go в путь чтения/записи Apache HBase
> через копроцессоры, минуя ограничение «копроцессоры только на Java».

Долгоживущий **Go-процесс** работает рядом с каждым RegionServer и общается с
тонким **Java-мостом** через lock-free **shared-memory ring buffer**
(protobuf-фреймы, без сокетов, без fork-per-call как в Hadoop Streaming). Доменная
логика исполняется *внутри базы данных*, на каждом пути записи/чтения, но пишется
на Go.

```
HBase client ──RPC──▶ RegionServer
                        │  Java-мост (этот репозиторий): сериализация хука → shmem ring
                        ▼
                      Go-процесс (ваш observer, SDK pkg/hbasecop)
```

> **Статус:** pre-release. Платформа — HBase **2.5.x** (мин. **2.5.6**), Java 11,
> Linux x86-64.

---

## 1. Постановка задачи

Apache HBase позволяет встраивать пользовательскую логику в сам сервер через
**копроцессоры** (observers и endpoints), но **только на Java/JVM**: код
выполняется внутри процесса RegionServer. Это даёт два ограничения:

- **язык** — доменную логику нельзя написать на Go (или другом не-JVM языке);
- **изоляция** — ошибка/паника/утечка пользовательского кода рушит RegionServer.

**Цель работы:** дать возможность писать копроцессоры HBase на Go, исполняя их в
отдельном долгоживущем процессе с изоляцией от RegionServer, и доказать, что такое
решение:\
(a) функционально эквивалентно нативному копроцессору\
(b) приемлемо по накладным расходам.

### Задачи

1. Спроектировать IPC-канал RegionServer $\leftrightarrow$ Go без сокетов и без fork-per-call.
2. Реализовать Go-SDK для всех observer-поверхностей HBase и endpoint-копроцессоров.
3. Реализовать Java-мост: диспетчеризация хуков, supervisor (heartbeat, рестарт,
   политики отказов).
4. Сделать упаковку и развёртывание копроцессора одной командой.
5. **Экспериментально сравнить** Go-копроцессоры с нативными Java по
   корректности и производительности.
6. Покрыть решение тестами (unit, cross-language golden, интеграционные на живом
   кластере, fuzzing, fault-injection).

---

## 2. Архитектура решения

| Слой                                                                                | Ответственность                                                                     |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| **Go-SDK** (`pkg/hbasecop`)                                                         | пользователь реализует интерфейс observer/endpoint и вызывает `hbasecop.Run(...)`   |
| **Go-runtime** (`internal/*`)                                                       | wire-кодек, обёртка shmem, мультиплексор, event-loop                                |
| **shared-memory ring** ([`java-go-shmem`](https://github.com/virogg/java-go-shmem)) | lock-free кольцевой буфер, два кольца (Java→Go, Go→Java), framing с префиксом длины |
| **Java-мост** (`java/com/virogg/…`)                                                 | адаптеры хуков → proto → wire, мультиплексор, supervisor процесса                   |
| **CLI** (`cmd/hbasecop-build`)                                                      | упаковка coproc-jar (кросс-компиляция ELF + shade моста), деплой через Admin API    |

**Поток одного хука (напр. `prePut`):** RegionServer вызывает адаптер моста →
мутация сериализуется в protobuf и кладётся в shmem-кольцо → Go-процесс будится,
декодирует, исполняет пользовательский хук, кладёт результат в обратное кольцо →
мост возвращает вердикт в RegionServer. 

Всё пересечение границы — это **lock-free запись в разделяемую память + пробуждение потока**, без syscall-сокетов.

Подробно: [`docs/architecture.md`](docs/architecture.md)

---

## 3. Что реализовано

- **Все пять observer-поверхностей**: Region, RegionServer, Master, WAL, BulkLoad
  — 103 хука , диспетчеризуемые через protobuf wire-протокол `virogg.hbasecop.v1`.
- **Go-SDK**: реализуйте `RegionObserver` (или другой интерфейс), вызовите
  `hbasecop.Run`; встраивания `Unimplemented*` держат observers forward-совместимыми.
  Паника в хуке перехватывается и возвращается как ошибка хука — процесс не падает.
- **Supervisor**: детекция краха, heartbeat-watchdog (зависание $\rightarrow$ SIGKILL),
  экспоненциальный backoff рестарта, per-hook политики отказов (`strict` /
  `best-effort`).
- **Endpoint-копроцессоры**: серверные endpoint'ы (агрегация, обратное
  чтение/запись *внутри* БД), один Go-процесс на coproc-jar (`SharedRuntime`,
  refcount по регионам/таблицам RegionServer'а).
- **Упаковка и деплой одной командой**: `hbasecop-build package` встраивает ELF +
  стоковый Java-делегат (Java писать не нужно), `hbasecop-build admin deploy`
  регистрирует копроцессор через HBase Admin API.

**Технологии:** Go ≥ 1.24, Java 11, Protocol Buffers (shaded 3.25 + endpoint
2.5.0), Maven, Docker (dev-кластер HBase 2.5).

---

## 4. Результаты экспериментов

### 4.1. Нативный паритет: Go-копроцессор vs нативный Java

Для четырёх типов копроцессоров написан **нативный Java-двойник с идентичной
логикой**; `make compare-all` гоняет оба плеча на одном живом кластере (на
co-resident таблицах), сверяет результаты на эквивалентность и измеряет накладные
расходы Go-стороны. Прогон на dev-хосте, HBase 2.5.11:

| Сравнение  | Тип / хук                  | Go / native | Δ               |
|------------|----------------------------|-------------|-----------------|
| **sum**    | endpoint, агрегация (read) | **1.56x**   | +2.8 ms / вызов |
| **filter** | preGetOp bypass (read)     | **1.59x**   | +1.4 ms / 2 Get |
| **audit**  | postPut SHA-256 (write)    | **5.70x**   | +108 µs / put   |
| **ttl**    | prePut валидация (write)   | **8.28x**   | +112 µs / put   |

**Выводы:**

- **Корректность совпадает во всех случаях** — мост доставляет ту же семантику,
  что и нативный копроцессор (та же агрегация, те же accept/reject решения, те же
  дайджесты, тот же read-bypass).
- **Read-/endpoint-путь: ~1.5× нативного.** Одно пересечение границы
  амортизируется на весь scan / большой Get.
- **Per-op write-путь: дороже.** Устойчивая величина — не отношение, а **абсолютная
  дельта ~110 µs/op** (один синхронный межпроцессный round-trip; одинакова для ttl и
  audit). Отношение шумит, т.к. знаменатель — нативное время записи (memstore/WAL).
- Овэрхед — это **стоимость процессной границы на синхронном per-операционном
  пути**.

Методология и полные числа: [`docs/bench/compare.md`](docs/compare.md).

### 4.2. Накладные расходы и масштабирование (микробенчи)

| Бенч                | Метрика                              | Результат              |
|---------------------|--------------------------------------|------------------------|
| Latency на хук      | p50-overhead `prePut` vs Java-only   | ~75 µs (цель < 100 µs) |
| WAL throughput      | регрессия при включённом WALObserver | ~15 % (порог < 50 %)   |
| Fan-out по регионам | масштабирование диспетчеризации      | ~7× на 8 ядрах         |

### 4.3. Отказоустойчивость (fault-injection matrix)

`make test-fault` — матрица 2 политики × 5 режимов отказа (kill-9, hang, exit-1,
protocol-error, OOM), **10 кейсов** на живом кластере. Проверяется: отсутствие
потери данных и двойного применения, корректная семантика политики, восстановление
supervisor'ом за конечное время.

| Политика                    | put #1      | put #2      | строк | итог |
|-----------------------------|-------------|-------------|-------|------|
| `strict` (любой отказ)      | IOException | IOException | 0     | ✅    |
| `best-effort` (любой отказ) | ok          | ok          | 2     | ✅    |

### 4.4. Тестирование

- **Кросс-языковой golden-corpus** — байт-в-байт паритет encode↔decode Go ↔ Java.
- **Интеграционные тесты** на dockerized HBase 2.5, матрица CI **2.5.6 + 2.5.11**.
- **Fuzzing** wire-кодека; **покрытие**: Go ≥ 80 %, Java ≥ 75 % (JaCoCo).

---

## 5. Апробация: сборка и запуск демо

Требования: Go ≥ 1.24, JDK 11, Maven, Docker, Linux x86-64.

```bash
git clone --recursive https://github.com/virogg/go-hbase && cd go-hbase
make deps          # go mod download + установка java-go-shmem в ~/.m2
make all           # lint + build + test (оба языка)

make hbase-up            # dockerized HBase 2.5 на localhost:16010
make test-integration    # демо counter: 100 Put → 100 вызовов Go-хука
make compare-all         # нативный паритет: эквивалентность + overhead
make test-fault          # fault-injection матрица (10 кейсов)
```

Минимальный observer на Go (переопределяется только нужный хук):

```go
type myObserver struct{ hbasecop.UnimplementedRegionObserver }

func (myObserver) PrePut(_ context.Context, env hbasecop.ObserverEnv,
    mut *hbasecop.MutationProto) (hbasecop.HookResult, error) {
    slog.Info("prePut", "table", env.TableName)
    return hbasecop.HookResult{}, nil // err → strict-политика прервёт запись клиента
}

func main() { _ = hbasecop.Run(myObserver{}) }
```

Упаковка и деплой — две команды, без написания Java:

```bash
go run ./cmd/hbasecop-build package --src ./my-observer --surface region --out my-observer.jar
go run ./cmd/hbasecop-build admin deploy --table my-table \
  --jar file:///coproc-jars/my-observer.jar \
  --class com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver
```

### Примеры (каждый — runnable + интеграционный тест)

| Пример                                                      | Хуки                              | Демонстрирует                               | Запуск                               |
|-------------------------------------------------------------|-----------------------------------|---------------------------------------------|--------------------------------------|
| [`counter-observer`](examples/counter-observer)             | PrePut                            | минимальный observer                        | `make test-integration`              |
| [`audit-observer`](examples/audit-observer)                 | PostPut/PostDelete                | best-effort аудит, приватность payload      | `make test-integration-audit`        |
| [`ttl-validator`](examples/ttl-validator)                   | PrePut                            | strict-валидация → client IOException       | `make test-integration-ttl`          |
| [`filter-observer`](examples/filter-observer)               | PreGetOp/Scan/Batch/Flush/Compact | bypass чтения, хуки хранилища               | `make test-integration-read`         |
| [`fault-observer`](examples/fault-observer)                 | PrePut/PostPut                    | инъекция crash/hang/OOM                     | `make test-fault`                    |
| [`master-policy-observer`](examples/master-policy-observer) | PreCreateTable                    | veto MasterObserver                         | `make test-integration-master`       |
| [`rs-policy-observer`](examples/rs-policy-observer)         | PreRollWALWriterRequest           | RegionServerObserver                        | `make test-integration-rs`           |
| [`wal-observer`](examples/wal-observer)                     | PreWALWrite                       | WALObserver (бенч throughput)               | `make bench-wal`                     |
| [`endpoint-observer`](examples/endpoint-observer)           | Endpoint `sum`/`get`/`scan`       | агрегация/обратное чтение внутри БД         | `make test-integration-endpoint-all` |
| [`native-coproc`](examples/native-coproc)                   | —                                 | нативные Java-двойники для сравнения (§4.1) | `make compare-all`                   |

---

### Ограничения и направления развития

- Per-op write-overhead: снизить через диспетчеризацию на `preBatchMutate` (одно
  пересечение на батч) и асинхронные best-effort post-хуки.
- Платформа — только Linux x86-64, HBase 2.5.x; расширение матрицы версий.
- Полное byte-parity payload-сообщений хуков (сейчас — на уровне wire-framing).

---

## Структура репозитория

```
pkg/hbasecop/        публичный Go-SDK (единственный публичный Go-пакет)
internal/            wire-кодек, обёртка shmem, мультиплексор, event-loop
java/com/virogg/     Java-мост: адаптеры, supervisor, mux, shmem
cmd/hbasecop-build/  CLI упаковки/деплоя coproc-jar
proto/               .proto (wire, hooks, vendored HBase)
proto-endpoint/      .proto endpoint-сервиса (unshaded protobuf 2.5.0)
examples/            runnable observers + native-coproc (база для сравнения)
test/integration/    dockerized HBase 2.5 + *IT тесты
docs/                архитектура, бенчи, матрица покрытия
```

## Лицензия

Apache 2.0. См. [`LICENSE`](LICENSE).
