# golden corpus wire/v1

Межъязыковые байтовые фикстуры для wire-формата go-hbase
(`internal/wire` ↔ `com.virogg.hbasecop.bridge.wire`).

`fixtures.tsv` описывает каждую фикстуру; `<name>.bin` — закодированные wire-байты,
которые **оба** кодировщика, Go и Java, обязаны выдавать байт в байт для
соответствующего логического Message.

## Перегенерация

```sh
go run ./cmd/wire-golden -out test/golden/wire/v1
```

Генератор записывает только файлы `<name>.bin`; `fixtures.tsv` правится вручную.
Перезапускайте после любого изменения wire-layout (T11/T12/T13 владеют этим форматом; T31+ может
расширять семантику payload без перезаписи байтов).

## Потребляется

- Go: `internal/wire/wire_golden_test.go`
- Java: `test/java/com/virogg/hbasecop/bridge/wire/WireGoldenTest.java`

Оба теста утверждают, что:

1. Кодирование логического Message выдаёт точно те байты, что в `<name>.bin`.
2. Декодирование `<name>.bin` выдаёт Message, равный логическому Message.

Эта двунаправленная проверка и доказывает, что Go encode ↔ Java decode (и
наоборот) совпадают без запуска живого межпроцессного набора.
