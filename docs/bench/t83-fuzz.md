# T83 - Wire-codec fuzzing report

**Verifies:** plan task T83 "Go fuzz target на `wire.Decode`, Java fuzz
target. 30 минут CPU. Найденные баги: фикс + regression test."

The framing decoder reads length-prefixed, chunked frames off an untrusted
shmem ring on both sides of the bridge: the project's primary
adversarial-input surface. Invariant under fuzz: decode never panics/throws
unchecked and never makes an unbounded allocation; every malformed input
surfaces as a typed decode error.

## Targets

| Side | Target | Framework | Entry |
|------|--------|-----------|-------|
| Go   | `internal/wire.FuzzDecode` | native `go test -fuzz` | `make fuzz FUZZTIME=...` |
| Java | `bridge.wire.DecoderFuzzTest` | jazzer-junit (`@FuzzTest`, JUnit 5) | `make java-fuzz` |

The Java target replaces the plan's jqf suggestion: jqf is JUnit4-runner
based and would have dragged junit4 + vintage-engine into the Jupiter-only
build; jazzer-junit is JUnit5-native, replays its seed corpus as a plain
test in every `mvn verify` (regression mode), and fuzzes for real under
`JAZZER_FUZZ=1`. Seeds: the golden wire corpus (`test/golden/wire/v1`),
mapped onto the test classpath by the root pom.

## Campaign (2026-06-10): 30 min CPU total

| Side | Duration | Executions | Throughput | Coverage | Findings |
|------|---------:|-----------:|-----------:|---------:|----------|
| Go   | 20m      | 245.0M     | ~187k/s    | 28 interesting inputs | none |
| Java | 10m      | 12.7M      | ~21k/s     | 104 edges, 348 features | none |

## The bug the campaign was built around

Preparing the Java target surfaced that the **H2/H4 allocation bounds were
missing from the Java decoder**: `RELEASE-BLOCKERS.md` claimed "both
decoders", but only `internal/wire/decoder.go` enforced `MaxChunks` /
`MaxPendingReassemblies`. A hostile 27-byte frame declaring
`chunk_total=2^31−1` made `Decoder.java` allocate a ~16GiB reference array
(instant OOM; jazzer finds it in seconds on the unfixed decoder), and
abandoned multi-chunk req_ids grew the pending map without bound.

Fix: `WireFormat.MAX_CHUNKS=1024` / `MAX_PENDING_REASSEMBLIES=4096`,
enforced in `Decoder.readChunk` before any allocation, with
`TooManyChunksException` / `TooManyPendingException` mirroring the Go
errors. Regressions: `DecoderBoundsTest` (ports `bounds_test.go`, plus a
cap-doesn't-block-existing-reassembly case). With the fix in place the
10-minute jazzer campaign ran clean at a flat ~1.5GB RSS.

A follow-up adversarial review of the fix found a second-order gap in it,
**on both sides**: the entry-count cap bounds how many reassemblies exist,
not how many bytes they retain. Each abandoned near-complete reassembly may
hold (MAX_CHUNKS-1) × MAX_PAYLOAD_BYTES ≈ 67 MB, so 4096 entries still
permitted ~256 GiB of retained heap: an OOM at ~60 abandoned entries on a
4 GiB heap, far below the count cap. Closed with a cumulative retained-byte
bound (`MaxPendingBytes` / `WireFormat.MAX_PENDING_BYTES` = 96 MiB,
decremented on completion) in both decoders; `ErrTooManyPendingBytes` /
`TooManyPendingBytesException`. Regressions use max-size payloads and
assert the byte cap fires well before the entry cap:
`TestDecodeCapsPendingBytes` (Go), `capsPendingBytesBeforeEntryCap` (Java).

## CI

- Every CI run: 30s Go fuzz smoke (`go` job) + jazzer seed-replay (rides in
  `mvn verify`).
- Nightly: 15m Go + 10m Java (`fuzz-nightly` job).
