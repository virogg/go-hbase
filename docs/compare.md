# Native vs Go: сравнение копроцессоров (эквивалентность + perf)

**Проверяет:** что копроцессор с одной и той же логикой, написанный (а) нативно
на Java и (б) на Go поверх bridge'а, даёт **идентичные результаты**, и измеряет
**накладные расходы** Go-плеча относительно нативного — на четырёх типах
копроцессоров и действий.

## Что сравнивается

Каждая пара - это нативный класс из `examples/native-coproc` против Go-примера, оба
зарегистрированы per-table на двух co-resident таблицах (`*_native` / `*_go`) в
одном кластере; один и тот же клиентский путь, один и тот же workload.

| Сравнение  | Тип / хук                            | Логика                                     | Go-плечо                                       | Нативное плечо         |
|------------|--------------------------------------|--------------------------------------------|------------------------------------------------|------------------------|
| **sum**    | endpoint (read, агрегация)           | scan региона, сумма `cf:n` как int64       | `endpoint-observer` (`GenericRegionObserver`)  | `NativeSumEndpoint`    |
| **ttl**    | observer `prePut` (write, валидация) | reject Put без `ttl=<sec>;` конверта       | `ttl-validator` (`TtlValidatorRegionObserver`) | `NativeTtlObserver`    |
| **audit**  | observer `postPut` (write, CPU)      | SHA-256-дайджест строки + счёт ячеек в лог | `audit-observer` (`AuditRegionObserver`)       | `NativeAuditObserver`  |
| **filter** | observer `preGetOp` (read, bypass)   | bypass Get'а по префиксу строки `block-`   | `filter-observer` (`FilterRegionObserver`)     | `NativeFilterObserver` |

Нативный endpoint переиспользует **тот же** `GoEndpointService`-surface, что и
Go-плечо, поэтому клиентский путь (`EndpointClient.callAndReduce`) байт-в-байт
одинаков для обоих — различается лишь серверная реализация Service'а
(нативный Java против форвардинга в Go через shmem).

## Как воспроизвести

```
make compare-all
```
по отдельности:
```
make compare-sum
make compare-ttl
make compare-audit
make compare-filter
```

## Методология

- **Эквивалентность (жёсткий pass/fail).** Оба плеча прогоняются на идентичных
  данных, результат сверяется `assertEquals`:
  - *sum*: `nativeSum == goSum == ` арифметическая сумма засеянной `cf:n`;
  - *ttl*: для корпуса валидных/невалидных значений оба плеча принимают
    решение accept/reject **идентично** (и состояние таблицы совпадает);
  - *audit*: оба плеча эмитят одинаковый `row_digest` для каждой строки (и он
    совпадает с независимым Java-оракулом SHA-256); сверка через скрейп
    `docker logs`;
  - *filter*: blocked-Get пуст, allowed-Get присутствует — одинаково на обоих.
- **Производительность.** Чередующиеся A/B-повторы (native-first / go-first попеременно, чтобы погасить дрейф), медиана по
  раундам.\
  Печатаются машиночитаемые строки `COMPARE_RESULT` (per-arm) и `COMPARE_SUMMARY` (native/go медианы + отношение `go_over_native`).

## Результат (`make compare-all`, один boot, dev-хост, HBase 2.5.11)

| Сравнение                          | native median | go median | `go_over_native` | Δ (go−native)   |
|------------------------------------|---------------|-----------|------------------|-----------------|
| sum (агрегация, 2000 строк)        | 4.9 ms        | 7.7 ms    | **1.56x**        | +2.8 ms / вызов |
| filter (1 blocked + 1 allowed Get) | 2.4 ms        | 3.8 ms    | **1.59x**        | +1.4 ms / 2 Get |
| audit (батч 500 postPut)           | 11.4 ms       | 65.2 ms   | **5.70x**        | +108 µs / put   |
| ttl (батч 500 prePut)              | 7.7 ms        | 63.8 ms   | **8.28x**        | +112 µs / put   |
