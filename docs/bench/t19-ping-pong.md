# T19 - Latency ping/pong Java↔Go

Артефакт для **Phase 1 / Checkpoint β**. Числа ниже получены из единственного
прогона `make test-e2e-ping` на хосте разработки; воспроизводится локально той
же командой. Харнесс — `test/java/com/virogg/hbasecop/e2e/PingPongE2ETest`
- 10 000 последовательных PING/PONG round-trip'ов через порождённый процесс `hbasecop-runtime`,
на четырёх размерах payload'а, нагружающих wire-чанкер:

| Payload | Закодировано чанков | Примечания                              |
|---------|----------------|----------------------------------------|
| 0       | 1              | фрейм только из заголовка               |
| 1 KiB   | 1              | single-chunk fast path                 |
| 64 KiB  | 2              | переходит границу `MAX_PAYLOAD_BYTES = 65 509` |
| 1 MiB   | 17             | полная multi-chunk reassembly          |

Shmem-кольца сконфигурированы с `capacity=8`, `maxObjectSize=2 MiB`, так что каждый
слот вмещает одно полностью закодированное сообщение (все чанки впритык). Heartbeat'ы
отключены, чтобы поток замеров latency был чистым.

## Распределение latency (самый свежий локальный прогон)

```
T19 ping/pong: N=10000 wall=13846ms throughput=722 msg/s
  overall:        min=4.5us  p50=111.3us  p99=6247.7us  p999=9548.9us  max=27.94ms
  payload=0       n=2500     p50=26.2us   p99=166.1us   max=708.6us
  payload=1024    n=2500     p50=24.4us   p99=78.0us    max=559.0us
  payload=65536   n=2500     p50=140.7us  p99=426.6us   max=15.55ms
  payload=1048576 n=2500     p50=3628us   p99=7868us    max=27.94ms
```

Throughput определяется бакетом 1 MiB (≈2.5 GiB переслано в каждую сторону
через shmem за прогон). Субмиллисекундный p99 держится для всего вплоть
до 2-чанкового бакета 64 KiB включительно; хвост 1 MiB отражает
17-чанковый encode + memcpy round-trip, а не какой-либо overhead IPC.

## Хост

Числа выше собраны на:

- WSL2 Linux 6.6.114.1-microsoft-standard, Ubuntu 24.04
- OpenJDK 21.0.10, Apache Maven 3.8.7
- Go 1.22 (Linux x86-64 ELF, встроенный в bridge jar)

CI гоняет тот же харнесс на `ubuntu-22.04` (см. `.github/workflows/ci.yml`)
и отбросит регрессии, где тест перестаёт завершаться в пределах
surefire wall clock.

## Воспроизведение

```sh
git submodule update --init --recursive
make deps-shmem      # one-time install of the patched java-go-shmem
make test-e2e-ping   # builds the Go runtime ELF, runs 10k PING/PONG
```

Строки latency выше выводятся в stderr в конце теста.
