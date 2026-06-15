# T81: Latency overhead на один hook против Java-only observer

**Проверяет:** задачу плана T81 "latency overhead per hook (Java-only baseline vs
through-shmem), p50/p95/p99 на prePut/postPut, batch 1/100/1000" и
цель MVP из SPEC §7.6 **<100µs p50 overhead на prePut**.

Bench гонит продакшн-путь Java от начала до конца: реальный
`RegionObserverAdapter` (включая proto-конверсию) → `Multiplexer` → wire
encode → shmem ring → реальный порождённый Go-процесс с **тихим no-op
observer** (`test/bench/noop-observer`) → путь ответа обратно к вызывающему.
Baseline-плечо делает идентичные вызовы против Java `RegionObserver` с
no-op методами по умолчанию; overhead = p50(bridge) − p50(java-only).

## Как воспроизвести

```
make bench-latency            # builds the no-op ELF, runs the gate
# knobs:
make bench-latency BENCH_P50_MAX_US=100
mvn test -Dtest=LatencyBenchIT -Djacoco.skip=true -Dbench.ops=10000
```

JaCoCo пропускается для прогона bench; latency измеряется без инструментации.

## Методология

- **batch=1 (gated)**: непрерывные последовательные round trip, та же
  методология, что и у baseline ping-pong из T19. Это режим, описываемый
  целью SPEC: под нагрузкой кольца остаются прогретыми.
- **batch=100/1000**: всплески из N вызовов подряд, разделённые 1ms паузой
  простоя, как клиентский batch из N мутаций достигает prePut на одной
  handler-нити.
- **sparse (отчёт, не gated)**: одиночные вызовы, каждому предшествует 1ms
  простоя, стоимость холодного возобновления после того, как spin-wait
  reader-нити были вытеснены планировщиком. Доминируется планировщиком и
  шумна на WSL2/общих CI-раннерах.
- 10 000 хронометрируемых операций на плечо после прогрева в 2 000 операций;
  перцентили через сортировку и индексирование.


## Результат (2026-06-10)

Железо: AMD Ryzen 7 5800H, WSL2, Linux x86-64. Продакшн-конфигурация колец
по умолчанию (capacity 16, maxObjectSize 1MiB, heartbeats включены).

| Leg                      | p50    | p95    | p99    |
|--------------------------|-------:|-------:|-------:|
| prePut bridge, batch=1   | ~75µs  | ~190µs | ~300µs |
| prePut java-only, batch=1| ~0.9µs | ~2µs   | ~4µs   |
| prePut bridge, batch=100 | ~64µs  | ~150µs | ~280µs |
| prePut bridge, batch=1000| ~62µs  | ~200µs | ~400µs |
| postPut bridge, batch=1  | ~75µs  | ~190µs | ~300µs |
| sparse (not gated)       | ~90-135µs | ~250µs | ~390µs |

**Gate: prePut p50 overhead = 72-78µs за три прогона: PASS (<100µs).**

## Итерация, которая загнала результат под цель

Первый прогон harness намерил p50 в 128-135µs, выше цели. Два исправления:

1. **Измерять без инструментации.** Агент JaCoCo (включён по умолчанию под
   `mvn test`) инструментирует горячий путь bridge; его отключение сэкономило ~7µs.
2. **Spin-before-park в `MuxHookDispatcher`** (продакшн-изменение). Вызывающая
   нить раньше сразу парковалась (park) в `CompletableFuture.get()`,
   платя по два context switch на hook, что больше остаточного ожидания,
   поскольку весь round trip завершается за ~100µs. Теперь dispatcher
   spin-поллит future до 150µs (`Thread.onSpinWait`), прежде чем откатиться
   к блокирующему ожиданию. Установившийся p50 упал 128µs → ~75µs
   (batch=1000: 95µs → ~62µs). Медленные или падающие hook проваливаются
   на блокирующий путь не позже исчерпания spin-бюджета, поэтому семантика
   timeout не меняется; цена — ≤150µs занятого CPU на RPC handler-нити для
   hook, которые уже в полёте.

## Оговорки

- Остаточный пол ~60µs распределён между сборкой/парсингом proto, двумя
  ring-хопами и Go-side передачами reader→handler→writer между горутинами;
  здесь применим анализ single-writer/single-reader shim из отчёта T62.
- Плечо sparse показывает, что нагрузка, простаивающая >1ms между hook,
  платит примерно двойной p50; это OS-планирование spin-wait reader, а не
  стоимость протокола, и оно балансирует на грани 100µs на WSL2. На
  bare-metal Linux ожидайте, что разрыв сократится.
- Числа на WSL2 варьируются на ±10% от прогона к прогону; на общих
  CI-раннерах больше. CI-gate использует абсолютную цель SPEC; промах на CI
  оправдывает локальный повторный прогон перед действиями.
