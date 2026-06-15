# T83 - Отчёт о fuzzing'е wire-кодека

**Проверяет:** задачу плана T83 "Go fuzz target на `wire.Decode`, Java fuzz
target. 30 минут CPU. Найденные баги: фикс + regression test."

Декодер фрейминга читает с обеих сторон bridge кадры с префиксом длины, нарезанные на чанки,
из недоверенного shmem-кольца: это основная для проекта
поверхность для атак через вход. Инвариант под fuzz: decode никогда не паникует/не бросает
непроверенных исключений и никогда не делает неограниченную аллокацию; каждый
некорректный вход всплывает как типизированная ошибка декодирования.

## Targets

| Сторона | Target | Framework | Точка входа |
|------|--------|-----------|-------|
| Go   | `internal/wire.FuzzDecode` | нативный `go test -fuzz` | `make fuzz FUZZTIME=...` |
| Java | `bridge.wire.DecoderFuzzTest` | jazzer-junit (`@FuzzTest`, JUnit 5) | `make java-fuzz` |

Java-target заменяет предложенный планом jqf: jqf основан на JUnit4-runner'е
и затащил бы junit4 + vintage-engine в сборку, рассчитанную только на Jupiter;
jazzer-junit нативен для JUnit5, проигрывает свой seed-corpus как обычный
тест в каждом `mvn verify` (regression-режим) и по-настоящему фаззит под
`JAZZER_FUZZ=1`. Seed'ы: золотой wire-corpus (`test/golden/wire/v1`),
смапленный на test classpath корневым pom.

## Кампания (2026-06-10): 30 мин CPU суммарно

| Сторона | Длительность | Исполнений | Throughput | Покрытие | Находки |
|------|---------:|-----------:|-----------:|---------:|----------|
| Go   | 20m      | 245.0M     | ~187k/s    | 28 interesting inputs | нет |
| Java | 10m      | 12.7M      | ~21k/s     | 104 edges, 348 features | нет |

## Баг, вокруг которого строилась кампания

При подготовке Java-target всплыло, что **границы аллокаций H2/H4 отсутствовали
в Java-декодере**: `RELEASE-BLOCKERS.md` заявлял "both
decoders", но только `internal/wire/decoder.go` обеспечивал `MaxChunks` /
`MaxPendingReassemblies`. Враждебный 27-байтовый фрейм, объявляющий
`chunk_total=2^31−1`, заставлял `Decoder.java` аллоцировать ~16GiB-массив ссылок
(мгновенный OOM; jazzer находит это за секунды на неисправленном декодере), а
брошенные многочанковые req_id'ы наращивали pending-map без границы.

Фикс: `WireFormat.MAX_CHUNKS=1024` / `MAX_PENDING_REASSEMBLIES=4096`,
проверяются в `Decoder.readChunk` до любой аллокации, с
`TooManyChunksException` / `TooManyPendingException`, зеркалящими Go-ошибки.
Регрессии: `DecoderBoundsTest` (портирует `bounds_test.go`, плюс кейс
cap-doesn't-block-existing-reassembly). С установленным фиксом
10-минутная jazzer-кампания отработала чисто на ровном ~1.5GB RSS.

Последующий adversarial review фикса нашёл в нём пробел второго порядка
**на обеих сторонах**: cap по числу записей ограничивает, сколько reassembly существует,
а не сколько байт они удерживают. Каждый брошенный почти-завершённый reassembly может
держать (MAX_CHUNKS-1) × MAX_PAYLOAD_BYTES ≈ 67 MB, поэтому 4096 записей всё ещё
допускали ~256 GiB удержанного heap: OOM на ~60 брошенных записях на
4 GiB heap, далеко ниже cap по числу. Закрыто кумулятивной границей удержанных байт
(`MaxPendingBytes` / `WireFormat.MAX_PENDING_BYTES` = 96 MiB,
декрементируется при завершении) в обоих декодерах; `ErrTooManyPendingBytes` /
`TooManyPendingBytesException`. Регрессии используют payload'ы максимального размера и
утверждают, что byte-cap срабатывает заметно раньше entry-cap'а:
`TestDecodeCapsPendingBytes` (Go), `capsPendingBytesBeforeEntryCap` (Java).

## CI

- Каждый CI-прогон: 30s Go fuzz smoke (`go` job) + jazzer seed-replay (едет в
  `mvn verify`).
- Nightly: 15m Go + 10m Java (`fuzz-nightly` job).
