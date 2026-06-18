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
> Целевая платформа — HBase **2.5.x** (минимум **2.5.6** — shaded-protobuf 3.25 ABI floor, см. SPEC §1), Java 11, только Linux x86-64.

- Полная спецификация: [`SPEC.md`](SPEC.md) · архитектура: [`docs/architecture.md`](docs/architecture.md)
- Tier 2 endpoint-копроцессоры (пост-v0.1.0): [`tasks/tier2-endpoints.md`](tasks/tier2-endpoints.md) · модель безопасности: [`docs/endpoint-security.md`](docs/endpoint-security.md)
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

Не хотите писать с нуля — `go run ./cmd/hbasecop-build init my-observer`
генерирует готовый к сборке скелет (`--surface region|master|regionserver|wal|bulkload|endpoint`;
`endpoint` скаффолдит серверный endpoint-копроцессор и пакуется как `--surface region`).

**3. Упакуйте coproc-jar — одна команда.** `package` кросс-компилирует ELF,
встраивает стоковый Java-делегат (Java писать не нужно) и шейдит мост:

```bash
mvn install -DskipTests   # один раз: публикует uber-мост (…-all.jar) в ~/.m2
go run ./cmd/hbasecop-build package \
  --src ./path/to/your/observer --surface region --out my-observer.jar
```

В jar встроены ELF (по пути `bin/linux-amd64/hbasecop-runtime`), его SHA-256 в
манифесте и стоковый класс `com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver`
(+ `Master`/`RegionServer`/`WAL` по `--surface`); supervisor проверяет дайджест
перед exec. Низкоуровневый режим (`--go-bin/--bridge-jar/--observer-class`) для
своего Java-делегата сохранён.

**4. Разверните на таблице — одна команда.** `deploy` регистрирует копроцессор
через HBase Admin API (disable → modify → enable):

```bash
make hbase-up      # dockerized HBase 2.5 с bind-mount /coproc-jars
cp my-observer.jar test/integration/coproc-jars/
go run ./cmd/hbasecop-build admin deploy \
  --table my-table --jar file:///coproc-jars/my-observer.jar \
  --class com.virogg.hbasecop.bridge.entrypoint.GenericRegionObserver
# admin list | admin remove — то же; нужен `hbase` в PATH
```

Теперь каждый Put по таблице проходит через ваш Go-код. End-to-end сразу:
`make test-integration` (пример counter, 100 Put'ов → 100 вызовов хука, с проверкой).
Проверить конфиг до деплоя: `hbasecop-build config --check hbase-site.xml`.

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
| [`wal-observer`](examples/wal-observer) | PreWALWrite | WALObserver (бенч throughput) | `make bench-wal` |

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

**Сборка падает: shmem-сабмодуль не найден / `mvn` не находит `hbasecop-bridge`?**
Клонируйте с `--recursive` (или `git submodule update --init --recursive`), затем
`make deps` (ставит java-go-shmem в `~/.m2`). Bridge нужен в `~/.m2` до `package` —
`mvn install -DskipTests` один раз.

**`NoClassDefFoundError: com/jgshmem/...` при открытии региона?**
В coproc-jar нет runtime-зависимостей моста: используйте `hbasecop-build package`
(шейдит uber-мост `…-all.jar`), а не «тонкий» модульный jar.

**`class file version` mismatch при сборке Java?**
Нужен JDK 11.

**Как протестировать observer без кластера?**
`pkg/hbasecop/hbasecoptest`: гоняет ваш observer через реальный диспетчер
in-process (`go test`, без Docker).

## Структура репозитория

```
pkg/hbasecop/        public Go SDK (the only public Go package)
internal/            wire codec, shmem wrapper, multiplexer, event loop
java/com/virogg/     Java bridge: adapters, supervisor, mux, shmem
cmd/hbasecop-build/  coproc-jar packer CLI
proto/               canonical .proto (wire, hooks, vendored HBase)
examples/            eight runnable observers (see table above)
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
(на main/nightly) полную матрицу интеграции на HBase 2.5.6 и 2.5.11.

## Лицензия

Apache 2.0. См. [`LICENSE`](LICENSE).
