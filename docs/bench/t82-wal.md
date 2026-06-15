# T82 — WAL write throughput: WALObserver вкл. vs выкл.

**Проверяет:** задачу плана T82 «throughput WAL writes с WALObserver vs без,
regression < 50%».

A/B-сравнение на живом dockerized HBase 2.5 standalone-кластере. Цикл A
поднимает кластер «голым»; цикл B поднимает его с зарегистрированным
cluster-wide coproc-jar `wal-observer` (`hbase.coprocessor.wal.classes`; WAL-
копроцессоры нельзя подключить per-table), чья Go-сторона — **no-op
WALObserver** (`examples/wal-observer`), так что дельта — это чистая стоимость
bridge-dispatch на пути WAL append (preWALWrite/postWALWrite, hooks 220/221).
Оба цикла гоняют один и тот же `WalThroughputBenchIT`: 20 000 puts батчами по
100 против одной таблицы, замер по wall-clock со стороны клиента. IT проверяет,
что присутствие coproc (`pgrep -f hbasecop-runtime` в контейнере) соответствует
плечу, так что устаревший кластер не может загрязнить baseline.

## Как воспроизвести

```
make bench-wal                       # both cycles + gate
# knobs:
make bench-wal BENCH_WAL_OPS=20000 BENCH_WAL_MAX_REGRESSION_PCT=50
```

## Результат (2026-06-10)

Железо: AMD Ryzen 7 5800H, WSL2 + Docker, HBase 2.5.11 standalone.

| Плечо | Throughput |
|-----|-----------:|
| A: без WAL-копроцессора | 16 170 ops/s |
| B: go-hbase no-op WALObserver | 13 685 ops/s |

**Regression: 15.4%, PASS (< 50% gate).**

## Как читать

- Путь WAL хорошо амортизируется: один батч из 100 puts даёт примерно один
  WAL append на регион, так что стоимость hook на каждый append (~2× dispatch
  round trip по ~70µs каждый) размазывается по батчу. Нагрузки из небатчованных
  одиночных puts увидели бы более высокий относительный удар; таблица рисков в
  плане отмечает sampling-only WAL hooks как fallback, если реальная нагрузка
  этого потребует.
- Измеряется end-to-end client put throughput, а не изолированная скорость
  WAL append: это та regression, которую пользователь реально ощущает.

## Оговорки

- Standalone single-JVM HBase под WSL2/Docker; абсолютные числа не
  репрезентативны для production-кластера, но метрикой является A/B-отношение,
  а оба плеча делят одно и то же окружение.
- Разброс от запуска к запуску ±10%; у 50% gate достаточный запас.
