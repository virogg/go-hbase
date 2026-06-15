# audit-observer

Пример post-hook audit (T72): Go-observer, который эмитит одну структурированную
JSON-запись audit на каждый завершённый `Put` и `Delete` в таблице, к которой
он подключён.

## Что демонстрирует

- **Post-hooks** (`PostPut`, `PostDelete`), работающие под политикой
  **best-effort** по умолчанию: audit никогда не может заблокировать или
  завалить запись клиента — если Go-сторона недоступна, операция продолжается, а
  bridge логирует WARN.
- **Log-based observability** (SPEC §6): записи проходят через `slog` как JSON
  на stderr; bridge пересылает их в лог RegionServer, готовые для любого
  log-агрегатора.
- **Приватность payload** (SPEC §8): записи audit несут короткий SHA-256 digest
  от row key (`row_digest`), никогда — сырой ключ или cell values. Digest
  стабилен для каждой строки, так что повторные операции над одной и той же
  строкой коррелируют, но ключ восстановить нельзя.

Одна запись audit выглядит так:

```json
{"level":"INFO","msg":"audit-observer: audit","op":"put","table":"ns:users",
 "region":"d5a1...","row_digest":"9f86d081884c7d65","cells":1,"seq":42}
```

## Сборка

```bash
make audit-observer-jar
# → examples/audit-observer/target/audit-observer.jar
```

## Запуск integration-теста

Поднимает dockerized HBase 2.5 standalone, подключает coproc к тестовой
таблице, выполняет 25 Puts + 25 Deletes и утверждает ровно 50 записей audit в
логе RegionServer:

```bash
make test-integration-audit
```

## Подключение к своей таблице

```java
admin.modifyTable(TableDescriptorBuilder.newBuilder(existing)
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.virogg.hbasecop.examples.audit.AuditRegionObserver")
        .setJarPath("file:///path/to/audit-observer.jar")
        .build())
    .build());
```
