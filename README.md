# go-hbase

Пишите HBase Observer-coprocessor'ы на **Go**.

Долгоживущий Go-процесс работает рядом с каждым RegionServer и общается с
тонким Java-мостом через lock-free **shared-memory ring buffer** (protobuf-фреймы,
без сокетов, без fork-per-call как в Hadoop Streaming). Ваша доменная логика
выполняется *внутри базы данных*, на каждом пути записи/чтения, на Go.

```
HBase client ──RPC──▶ RegionServer
                        │  Java bridge (this repo): serialize hook → shmem ring
                        ▼
                      Go process (your observer, pkg/hbasecop SDK)
```

> **Статус: pre-release.** Реализовано по Phase 7 включительно из
> [`tasks/plan.md`](tasks/plan.md); v0.1.0 заблокирован
> [`tasks/RELEASE-BLOCKERS.md`](tasks/RELEASE-BLOCKERS.md).
> Целевая платформа — HBase **2.5.x**, Java 11, только Linux x86-64.

- Полная спецификация: [`SPEC.md`](SPEC.md) · архитектура: [`docs/architecture.md`](docs/architecture.md)
- IPC-примитив: [`virogg/java-go-shmem`](https://github.com/virogg/java-go-shmem)

## Установка из релиза

Каждый [GitHub release](https://github.com/virogg/go-hbase/releases) поставляет
два артефакта:

- `hbasecop-bridge-<version>.jar`: Java-мост, который ваш coproc-jar shade'ит в себя
  (установите в свой Maven-репозиторий: `mvn install:install-file
  -Dfile=hbasecop-bridge-<version>.jar -DgroupId=com.virogg
  -DartifactId=hbasecop-bridge -Dversion=<version> -Dpackaging=jar`).
- `hbasecop-build-linux-amd64`: CLI для упаковки; сделайте `chmod +x` и используйте
  как `hbasecop-build` в шаге 3 ниже.

Go SDK подтягивается как обычный модуль:
`go get github.com/virogg/go-hbase/pkg/hbasecop@v<version>`. Чтобы собрать
всё из исходников, следуйте quick start.

## Quick start: ваш первый observer за ~5 минут

Требования: Go >= 1.24, JDK 11, Maven, Docker (для dev-кластера), Linux x86-64.

**1. Clone + bootstrap** (зависимость shmem — это git-submodule):

```bash
git clone --recursive https://github.com/virogg/go-hbase
cd go-hbase
make deps          # go mod download + installs java-go-shmem into ~/.m2
make all           # lint + build + test, both languages
```

**2. Напишите Go-observer.** Встройте `UnimplementedRegionObserver`, переопределите
только нужные вам хуки:

```go
package main

import (
    "context"
    "log/slog"
    "os"

    "github.com/virogg/go-hbase/pkg/hbasecop"
)

type myObserver struct {
    hbasecop.UnimplementedRegionObserver
}

func (myObserver) PrePut(
    _ context.Context,
    env hbasecop.ObserverEnv,
    mut *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
    slog.Info("prePut", "table", env.TableName)
    // return an error → strict policy aborts the client's write;
    // return HookResult{Bypass: true} → HBase skips its own implementation.
    return hbasecop.HookResult{}, nil
}

func main() {
    if err := hbasecop.Run(myObserver{}); err != nil {
        slog.Error("fatal", "err", err)
        os.Exit(1)
    }
}
```

`hbasecop.Run` блокируется на всё время жизни coprocessor'а; конфигурация
приходит из переменных окружения, выставленных Java-supervisor'ом. Поверхности
Master / RegionServer / WAL / BulkLoad используют `RunMaster` / `RunRegionServer` /
`RunWAL` / `RunBulkLoad`.

**3. Соберите ELF и упакуйте coproc-jar** с помощью CLI `hbasecop-build`:

```bash
GOOS=linux GOARCH=amd64 go build -o myobserver ./path/to/your/observer
mvn install -DskipTests   # publishes the bridge jar into ~/.m2

go run ./cmd/hbasecop-build \
  --go-bin         ./myobserver \
  --bridge-jar     ~/.m2/repository/com/virogg/hbasecop-bridge/0.0.1-SNAPSHOT/hbasecop-bridge-0.0.1-SNAPSHOT.jar \
  --observer-class com.virogg.hbasecop.examples.counter.CounterRegionObserver \
  --coproc-id      my-observer \
  --out            my-observer.jar
```

CLI shade'ит мост, встраивает ваш ELF по пути
`bin/linux-amd64/hbasecop-runtime` и записывает его SHA-256 в манифест
(`HbaseCop-Go-Bin-SHA256`); supervisor проверяет дайджест перед exec
и отказывается запускать повреждённый бинарь или бинарь не той архитектуры.

`--observer-class` — это Java-класс `RegionCoprocessor`, который инстанцирует HBase.
Каждый пример поставляет небольшой делегирующий класс (~30 строк boilerplate; см.
[`examples/counter-observer`](examples/counter-observer)); переиспользуйте один
из них или скопируйте под своим именем пакета в jar.

**4. Разверните на таблице:**

```bash
make hbase-up      # dockerized HBase 2.5 standalone with a /coproc-jars bind-mount
cp my-observer.jar test/integration/coproc-jars/
```

```java
admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.your.pkg.MyObserver")
        .setJarPath("file:///coproc-jars/my-observer.jar")
        .build())
    .build());
```

Теперь каждый Put по этой таблице проходит через ваш Go-код. Чтобы сразу увидеть
работу end-to-end: `make test-integration` (пример counter, 100 Put'ов →
100 вызовов хука на Go-стороне, с проверкой).

## Примеры

| Пример | Хуки | Демонстрирует | IT |
|---|---|---|---|
| [`counter-observer`](examples/counter-observer) | PrePut | минимальный observer, проверка по логам | `make test-integration` |
| [`audit-observer`](examples/audit-observer) | PostPut/PostDelete | best-effort post-хук аудита, приватность payload | `make test-integration-audit` |
| [`ttl-validator`](examples/ttl-validator) | PrePut | strict-валидация → client IOException | `make test-integration-ttl` |
| [`filter-observer`](examples/filter-observer) | PreGetOp/Scan/Batch/Flush/Compact | bypass на пути чтения, хуки хранилища | `make test-integration-read` |
| [`fault-observer`](examples/fault-observer) | PrePut/PostPut | инъекция crash/hang/OOM (fault matrix) | `make test-fault` |
| [`master-policy-observer`](examples/master-policy-observer) | PreCreateTable | veto политики MasterObserver | `make test-integration-master` |
| [`rs-policy-observer`](examples/rs-policy-observer) | PreRollWALWriterRequest | RegionServerObserver | `make test-integration-rs` |

## Справочник по конфигурации

Все ключи читаются из HBase `Configuration` (`hbase-site.xml` или дескриптор
таблицы). Значения по умолчанию — это то, что поставляется; каждый таймаут/буфер
настраивается (SPEC §8).

### Политика отказов (per-hook)

| Ключ | Значения / по умолчанию |
|---|---|
| `hbasecop.policy.<hook>` (напр. `hbasecop.policy.prePut`) | `strict` \| `best-effort`. По умолчанию: `pre*` → **strict**, `post*` → **best-effort**, всё остальное → strict |
| `hbasecop.timeout.<hook>` | Hadoop duration (**указывайте единицу**: `500ms`, `2s`). Per-hook ожидание ответа от Go |
| `hbasecop.timeout.default` | Резервный таймаут. По умолчанию **5s** |

**strict**: ошибка Go / таймаут / процесс упал → `IOException` клиенту,
операция прерывается. **best-effort**: WARN в логе RegionServer, хук становится
no-op, операция продолжается. Замечание: хуки, чья сигнатура в HBase не может
бросать исключение (`postOpen`, `postClose`, `postCompactSelection`), фактически
работают как best-effort независимо от конфигурации.

### Supervisor: heartbeat, рестарт

| Ключ | По умолчанию | Значение |
|---|---|---|
| `hbasecop.heartbeat.period` | `500ms` | интервал heartbeat Go→Java |
| `hbasecop.heartbeat.miss-threshold` | `3` | подряд пропущенных → SIGKILL + рестарт |
| `hbasecop.restart.initial-delay` | `200ms` | backoff первого рестарта (удваивается на каждом сбое) |
| `hbasecop.restart.max-delay` | `5s` | потолок backoff (jitter ±20%) |
| `hbasecop.restart.max-fails` | `5` | сбоев подряд → пометить unhealthy |
| `hbasecop.restart.probe-interval` | `30s` | частота probe рестарта в состоянии unhealthy |
| `hbasecop.restart.deadline` | `3s` | сколько вызовы, отправленные во время краша, ждут рестарта, прежде чем упасть по политике |

Детекция краша и авто-рестарт работают даже когда heartbeat'ы отключены.

### Окружение Go-процесса (выставляется supervisor'ом)

`HBASECOP_SHMEM_IN_PATH`, `HBASECOP_SHMEM_OUT_PATH` (mmap-файлы ring),
`HBASECOP_RING_CAPACITY` (слоты, по умолчанию 16), `HBASECOP_RING_MAX_OBJECT_SIZE`
(байт/слот, по умолчанию 1 MiB), `HBASECOP_HEARTBEAT_MS`. Go SDK логирует JSON
через `slog` в stderr; мост пробрасывает каждую строку в лог RegionServer.

## FAQ / troubleshooting

**Что происходит, когда мой Go-observer падает или зависает?**
Supervisor немедленно обнаруживает выход (а зависание — через пропущенные
heartbeat'ы → SIGKILL), перезапускает с экспоненциальным backoff и заваливает
in-flight хуки по политике: strict-вызывающие получают `IOException`;
best-effort-вызывающие продолжают. Новые вызовы в окне рестарта ждут до
`hbasecop.restart.deadline`. После `max-fails` неудачных рестартов подряд
coprocessor помечается как unhealthy и пробится каждые `probe-interval`. Матрица
fault-injection (`make test-fault`) проверяет отсутствие потери данных и
двойного применения при kill -9 / зависании / выходе / OOM в обеих политиках.

**Panic в моём Go-колбэке?**
Перехватывается SDK и возвращается как ошибка хука (применяется политика). Он
никогда не убивает общий Go-процесс.

**`ELF SHA-256 mismatch` при старте?**
Встроенный в jar Go-бинарь не совпадает с дайджестом в манифесте: повреждённый
jar или устаревшая/смешанная сборка. Пересоберите через `hbasecop-build` (он
пишет дайджест) и переразверните. Эта проверка защищает от повреждения/неверной
архитектуры, это не схема подписи.

**`classpath resource not found: bin/linux-amd64/hbasecop-runtime`?**
В coproc-jar нет встроенного ELF: упакуйте через `hbasecop-build` (или пример
Maven-настройки) и соберите ELF с `GOOS=linux GOARCH=amd64`.

**Мой Put падает с `RetriesExhaustedWithDetailsException`.**
Это работает strict-политика: pre-хук вернул ошибку (причину со стороны Go см.
в логе RegionServer). Сбои валидации детерминированы; ретраи клиента не изменят
исход.

**Какие хуки могут делать `Bypass` / подменять результат?**
`HookResult{Bypass: true}` отображается в `ObserverContext.bypass()` там, где
HBase 2.5 это допускает (где нет — логируется WARN). `PreAppend`/`PreIncrement`
(и `*AfterRowLock`) вместо этого подменяют видимый клиенту `Result` из
`HookResult.ResultCells`. `PreScannerOpen` эмулирует bypass, ограничивая scan
пустым диапазоном. Batch-хуки используют `HookResult.BlockedIndices`, чтобы
завалить отдельные мутации.

**Чувствительные данные в логах?**
Фреймворк никогда не логирует row key'и или значения ячеек на уровне по умолчанию
(SPEC §8) и пробрасывает stdout/stderr вашего observer'а в лог RegionServer на
уровне INFO; не печатайте payload'ы. См. `examples/audit-observer` для паттерна
«дайджест вместо ключа».

**Сколько Go-процессов на один RegionServer?**
По одному на coproc-jar (`SharedRuntime` ведёт refcount по всем регионам/таблицам,
использующим этот jar на RS). Каждый отдельный jar получает собственный процесс
и пару ring'ов.

## Структура репозитория

```
pkg/hbasecop/        public Go SDK (the only public Go package)
internal/            wire codec, shmem wrapper, multiplexer, event loop
java/com/virogg/     Java bridge: adapters, supervisor, mux, shmem
cmd/hbasecop-build/  coproc-jar packer CLI
proto/               canonical .proto (wire, hooks, vendored HBase)
examples/            seven runnable observers (see table above)
test/integration/    dockerized HBase 2.5 + *IT tests
docs/                architecture, benches, coverage matrix
```

## Разработка

```bash
make help                # every target, annotated
make all                 # lint + build + test (both languages)
make go-cover            # Go coverage gate
make fuzz FUZZTIME=30s   # wire-codec fuzzer
make hbase-up            # dev cluster on localhost:16010
make test-integration    # counter example end-to-end
```

CI прогоняет проверки структуры/лицензий, Go (lint+race+coverage+fuzz), Java
(spotless+tests+JaCoCo), кросс-языковой contract-job на golden-corpus и
(на main/nightly) полную матрицу интеграции на HBase 2.5.0 и 2.5.11.

## Лицензия

Apache 2.0. См. [`LICENSE`](LICENSE).
