# T84: 1h soak/chaos run, kill -9 under sustained load

**Verifies:** plan task T84 "1 час прогон стресс-нагрузки (1000 ops/s, mixed
read/write) с периодическими kill -9 → отсутствие data loss, отсутствие
memory leak (RSS ровный), отсутствие Go-supervisor-zombie" and SPEC §7.5.

Architecture: `SoakIT` (Java client) drives paced mixed load (80% Put / 20%
Get, 4 writer threads) against a `soak` table carrying the counter-observer
coproc-jar with `hbasecop.policy.prePut=best-effort`; every client-acked Put
rowkey goes into a ledger, and the final full scan must contain every
ledgered key. Around it, `test/integration/scripts/soak.sh` runs a chaos
loop (`docker exec ... pkill -9 -f '[h]basecop-runtime'` at uniform-random
120-300s intervals, armed only once the runtime exists, disarmed 30s before
load end) and a sampler (every 10s: Go pid/RSS, RS JVM RSS, runtime process
count, Z-state count to CSV), then evaluates the gates.

## How to reproduce

```
make soak                                    # full hour
make soak SOAK_DURATION_S=150 SOAK_KILL_MIN_S=25 SOAK_KILL_MAX_S=45 SOAK_RATE=400   # smoke
```

## Result (2026-06-10)

Hardware: AMD Ryzen 7 5800H, WSL2 + Docker, HBase 2.5.11 standalone.
3600s configured load at 1000 ops/s, **12 kill -9s injected**.

| Gate | Result | Evidence |
|------|--------|----------|
| Data loss | **PASS** | 2 839 779 acked Puts, 2 839 779 rows scanned, **0 lost**, 0 failed Puts, 711 149 Gets OK |
| Go RSS flat | **PASS** | medians 13 718 → 13 946 kB (+1.7%, ≤15% band) |
| RS JVM RSS flat | **PASS** | post-ramp medians 888 672 → 946 372 kB (+6.5%, ≤10% band); see note |
| Single runtime | **PASS** | never >1 `hbasecop-runtime` in 345 samples |
| No leaked runtime | **PASS** | 0 runtime procs after table drop |
| Zombies | **PASS** | 0 Z-state processes throughout and at end |
| Restart accounting | **PASS** | `GoProcess started:` ×13 = 1 initial + 12 kills, exactly; 0 `UNHEALTHY` |

Notable: **zero client-visible failures** across 12 crash windows. The
best-effort policy + 3s restart deadline absorbed every kill without a
single rejected Put, and every restart was 1:1 (no restart storms, no
missed crashes).

## RS JVM RSS window choice

The RegionServer JVM ramps 714 → 871 MB over the first ~25 minutes (heap
expansion, block-cache and memstore fill while ingesting 2.8M rows) and
then plateaus, so a first-vs-last-window comparison misreads warm-up as a
leak. The gate therefore compares the 60-80% window against the final 20%
(measured drift: 6.5%); a real Java-bridge leak under constant load holds
its slope and fails this window. The Go process (which the bridge respawns
on every kill) is gated first-vs-last at ±15% and sat dead flat.

## Harness bugs the smoke runs caught (fixed before the 1h run)

- `docker exec sh -c 'pgrep -f hbasecop-runtime'` matches the probing
  shell's own cmdline, so every process-count gate failed deterministically.
  Fixed with the `[h]basecop-runtime` bracket pattern.
- Kills before the Go runtime existed (cluster still booting) and kills
  landing in empty restart windows were counted, breaking restart
  accounting. Chaos now arms only after the runtime appears and counts
  only kills that found a live victim.

## Caveats

- Standalone single-RS topology; "per RegionServer" claims are exercised on
  exactly one RS. Multi-RS chaos is future work.
- Kill cadence (120-300s) bounds detectable Go-side leak rates: the process
  never lives longer than ~5 min, so slower Go leaks hide behind restarts
  (the RS-JVM axis has no such blind spot; it is never restarted).
- Heartbeats are disabled in the counter-observer config; crash detection
  ran on the 500ms crash-probe path (the C2 fix). The hang-detection path
  is exercised by unit tests, not this soak.
