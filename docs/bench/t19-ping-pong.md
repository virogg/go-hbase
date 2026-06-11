# T19 — Java↔Go ping/pong latency

Artefact for **Phase 1 / Checkpoint β**. The numbers below come from a single
run of `make test-e2e-ping` on the development host; reproduce locally with
the same command. The harness is `test/java/com/virogg/hbasecop/e2e/PingPongE2ETest`
— 10 000 serial PING/PONG roundtrips through the spawned `hbasecop-runtime`
process, across four payload sizes that exercise the wire chunker:

| Payload | Encoded chunks | Notes                                  |
|---------|----------------|----------------------------------------|
| 0       | 1              | header-only frame                      |
| 1 KiB   | 1              | single-chunk fast path                 |
| 64 KiB  | 2              | straddles `MAX_PAYLOAD_BYTES = 65 509` |
| 1 MiB   | 17             | full multi-chunk reassembly            |

Shmem rings are configured with `capacity=8`, `maxObjectSize=2 MiB` so each
slot fits one fully-encoded message (all chunks back-to-back). Heartbeats
are disabled to keep the latency stream clean.

## Latency distribution (most recent local run)

```
T19 ping/pong: N=10000 wall=13846ms throughput=722 msg/s
  overall:        min=4.5us  p50=111.3us  p99=6247.7us  p999=9548.9us  max=27.94ms
  payload=0       n=2500     p50=26.2us   p99=166.1us   max=708.6us
  payload=1024    n=2500     p50=24.4us   p99=78.0us    max=559.0us
  payload=65536   n=2500     p50=140.7us  p99=426.6us   max=15.55ms
  payload=1048576 n=2500     p50=3628us   p99=7868us    max=27.94ms
```

Throughput is dominated by the 1 MiB bucket (≈2.5 GiB shipped each way
through shmem during the run). Sub-millisecond p99 holds for everything up
to and including the 2-chunk 64 KiB bucket; the 1 MiB tail reflects the
17-chunk encode + memcpy round-trip rather than any IPC overhead.

## Host

The numbers above were collected on:

- WSL2 Linux 6.6.114.1-microsoft-standard, Ubuntu 24.04
- OpenJDK 21.0.10, Apache Maven 3.8.7
- Go 1.22 (Linux x86-64 ELF embedded in the bridge jar)

CI runs the same harness on `ubuntu-22.04` (see `.github/workflows/ci.yml`)
and will reject regressions where the test no longer terminates within the
surefire wall clock.

## Reproducing

```sh
git submodule update --init --recursive
make deps-shmem      # one-time install of the patched java-go-shmem
make test-e2e-ping   # builds the Go runtime ELF, runs 10k PING/PONG
```

The latency lines above are emitted to stderr at the end of the test.
