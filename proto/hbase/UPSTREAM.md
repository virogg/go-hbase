# Вендорённое подмножество proto-файлов HBase

Файлы `.proto` в этом каталоге — это **урезанная копия**
`hbase-protocol-shaded/src/main/protobuf/` из upstream
[apache/hbase][hbase], переписанная под наш собственный алиас пакета, чтобы
сгенерированный код линковался в этот проект, не затягивая runtime-jar'ы HBase.

[hbase]: https://github.com/apache/hbase

## Pin

| Поле       | Значение                                    |
|------------|---------------------------------------------|
| Tag        | `rel/2.5.10`                                |
| SHA        | `1a89f98d268eab842ee376563b6961b030fee916`  |
| Subtree    | `hbase-protocol-shaded/src/main/protobuf/`  |
| Получено   | 2026-05-13                                  |

## Файлы в этом вендоре

| Файл           | Upstream                | Заметки по урезанию                                                         |
|----------------|-------------------------|----------------------------------------------------------------------------|
| `Cell.proto`   | `Cell.proto`            | Тела сообщений дословно (`Cell`, `KeyValue`, `CellType`).                  |
| `HBase.proto`  | `HBase.proto`           | Урезано: `TableName`, `TimeRange`, `ColumnFamilyTimeRange`, `NameBytesPair`, `RegionSpecifier`, `ServerName`, `RegionInfo`. Остальные сообщения верхнего уровня выброшены; добавляйте обратно по мере необходимости в будущих задачах. |
| `Client.proto` | `Client.proto`          | Урезано: только `MutationProto` (с вложенными `ColumnValue`, `QualifierValue`, `Durability`, `MutationType`, `DeleteType`). `Get`, `Result`, `Scan`, `MutateRequest`/`Response` и т. д. откладываются до T43+. |

## Правки, применённые к каждому файлу

- `package hbase.pb;` становится `package virogg.hbasecop.hbase.v1;`
- `option java_package = "org.apache.hadoop.hbase.shaded.protobuf.generated";`
  становится `option java_package = "com.virogg.hbasecop.hbase.v1";`
- Добавлен `option go_package = "github.com/virogg/go-hbase/internal/wire/hbasepb";`
- Добавлен SPDX-заголовок (Apache-2.0; copyright upstream сохранён дословно).
- `import "X.proto";` становится `import "hbase/X.proto";`, чтобы один
  `--proto_path=proto` разрешал и вендорённые, и проектные файлы.

Номера полей и тела сообщений сохранены побайтово идентичными upstream, чтобы
PB-кодирование на проводе оставалось совместимым с самим HBase; это позволяет
RegionObserver передать `Put` прямо на сторону Go без трансляции.

## Как поднять версию

1. Выберите новый тег HBase (например, `rel/2.5.11`).
2. Сравните upstream-subtree с этим каталогом; заново примените правки
   выше. Большинство полей стабильны между patch-релизами; держите урезанное
   подмножество урезанным.
3. Обновите таблицу выше новым тегом/SHA.
4. `make proto && make all`; golden-тесты отвергают молчаливые изменения на проводе.
