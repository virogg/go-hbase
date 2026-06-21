# ttl-validator

Пример pre-hook валидации: Go-observer, который отклоняет любой `Put`,
чьи cell values не объявляют TTL-конверт. Демонстрирует политику отказа
**strict**, при которой отклонение со стороны Go прерывает запись клиента с
`IOException`.

## Что демонстрирует

- **Pre-hooks** (`PrePut`) под политикой **strict** по умолчанию: возврат
  ошибки из Go отображается в `IOException` на стороне HBase-клиента; запись
  никогда не попадает в хранилище.
- **In-database валидация на Go**: доменные правила выполняются внутри
  RegionServer, на каждом пути записи (API, MR jobs, другие клиенты), а не
  только в коде приложения.
- **Приватность payload**: причины отклонения называют координаты
  колонки (family/qualifier: схема, не данные), но никогда не отражают cell
  values.

## Правило валидации

Каждое cell value должно начинаться с текстового TTL-конверта:

```
ttl=<seconds>;<payload>      e.g.  ttl=3600;{"name":"alice"}
```

`<seconds>` — это 1-9 цифр, > 0. Всё остальное (отсутствующий префикс, нулевой
TTL, отсутствие терминатора `;`) отклоняет весь `Put`:

```
org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException:
  ... ttl-validator: cf:q - value lacks the "ttl=" TTL envelope ...
```

## Сборка

```bash
make ttl-validator-jar
# → examples/ttl-validator/target/ttl-validator.jar
```

## Запуск integration-теста

Поднимает dockerized HBase 2.5 standalone, подключает coproc и утверждает:
валидный Put проходит и доступен на чтение; невалидный Put бросает
`IOException` и не оставляет за собой строки:

```bash
make test-integration-ttl
```

## Подключение к своей таблице

```java
admin.modifyTable(TableDescriptorBuilder.newBuilder(existing)
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.virogg.hbasecop.examples.ttl.TtlValidatorRegionObserver")
        .setJarPath("file:///path/to/ttl-validator.jar")
        .build())
    .build());
```
