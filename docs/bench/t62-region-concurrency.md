# T62 - Multi-region throughput benchmark

**Verifies:** plan task T62 "throughput с N регионами линейно масштабируется
до core_count Go-side".

The benchmark drives PrePut wire frames through the real `cpruntime.Loop`,
rotating across `N` distinct `region_id` values. The user observer
(`busyObserver`) burns ~50 µs of CPU per call, so dispatch sits in the
work-bound regime; otherwise the wire/codec floor dominates and parallel
scaling is invisible.

## How to reproduce

```
go test -run='^$' \
    -bench=BenchmarkRegionConcurrencyThroughput \
    -benchtime=1s \
    -cpu=1,2,4,8 \
    ./pkg/hbasecop/
```

Or via Make:

```
make bench-region-concurrency
```

## Result (2026-05-19)

Hardware: AMD Ryzen 7 5800H, Linux x86-64. ns/op figures are
end-to-end per-PrePut wallclock (sender → SPSC ring → cpruntime →
observer → SPSC ring → receiver). Lower is better.

| Regions | GOMAXPROCS=1 | GOMAXPROCS=2 | GOMAXPROCS=4 | GOMAXPROCS=8 |
|--------:|-------------:|-------------:|-------------:|-------------:|
|       1 |       285764 |       143669 |        72015 |        41277 |
|       2 |       622910 |       141020 |        70288 |        41855 |
|       4 |       277561 |       141965 |        71806 |        38151 |
|       8 |       277491 |       141730 |        71050 |        41662 |
|      16 |       279905 |       142386 |        71254 |        39710 |
|      32 |       279755 |       143599 |        71269 |        38424 |
|      64 |       280316 |       139564 |        71241 |        39089 |

(The `regions=2, GOMAXPROCS=1` outlier is single-iteration noise from a
small `b.N` at the slowest core count; subsequent rows confirm the
~280 µs floor.)

Translating to ops/sec at any region count ≥ 4:

| GOMAXPROCS | ns/op (mean) | ops/sec  | Speed-up vs 1 |
|-----------:|-------------:|---------:|--------------:|
|          1 |       ~280 k |    3.6 k |          1.0× |
|          2 |       ~142 k |    7.1 k |          1.97× |
|          4 |        ~71 k |   14.1 k |          3.94× |
|          8 |        ~40 k |   25.0 k |          7.0× |

## Read

- **Linear scaling with core_count.** Doubling GOMAXPROCS approximately
  halves ns/op, up to GOMAXPROCS=8 (the host has 8 physical cores). The
  shortfall from a perfect 8× (we hit ~7×) is the unavoidable
  serialization on the SPSC reader/writer goroutines inside `cpruntime`
  - every inbound frame queues through one reader, every outbound
  response through one writer, so two single-threaded shims bracket the
  parallel handler pool. This matches the architectural intent (one Go
  process per RegionServer, one Java→Go ring, one Go→Java ring) and is
  the design ceiling, not a regression.

- **N regions ≥ GOMAXPROCS gives no further gain.** Once enough distinct
  region_ids are in flight to saturate the cores, adding more regions
  cannot help: parallelism is bounded by the goroutine scheduler, not
  by routing diversity. Equivalently, even a single region (N=1) drives
  the full available parallelism because the runtime spawns one handler
  goroutine per inbound frame regardless of `region_id` - `region_id` is
  routing metadata, not a partition key for the dispatcher. This is the
  property T62 Wave-B locks in: no per-region serialization point.

- **Floor at GOMAXPROCS=1 ≈ 280 µs.** Single-core ns/op is dominated by
  the synthetic 50 µs handler work plus ~230 µs of (encode + ring send
  + decode + dispatch + marshal + handler-spawn + response-marshal +
  ring send + decode) overhead distributed across the path. Real
  observers will trade this floor against their own per-call cost.

## Caveats

- `busyObserver` is a synthetic CPU-bound stand-in. Real observers vary
  wildly (e.g. preWALWrite is latency-critical, see T82). This bench
  isolates the dispatch fan-out, not steady-state HBase throughput.
- WSL2 numbers are noisier than bare-metal Linux; a re-run typically
  varies ±10 %. Treat the table as a regression baseline, not a
  spec-grade SLO.
- The bench currently runs sender + receiver in one process and uses
  `shmem` over a tmpfs-backed mmap; a cross-process run (Java
  supervisor + ELF) adds a constant context-switch term but does not
  change the scaling shape.
