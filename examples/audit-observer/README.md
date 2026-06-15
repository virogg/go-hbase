# audit-observer

Post-hook audit example (T72): a Go observer that emits one structured JSON
audit record for every completed `Put` and `Delete` on the table it is
attached to.

## What it demonstrates

- **Post-hooks** (`PostPut`, `PostDelete`) running under the default
  **best-effort** policy: auditing can never block or fail the client's
  write - if the Go side is down, the operation proceeds and the bridge
  logs a WARN.
- **Log-based observability** (SPEC §6): records go through `slog` as JSON
  on stderr; the bridge forwards them into the RegionServer log, ready for
  any log aggregator.
- **Payload privacy** (SPEC §8): audit records carry a short SHA-256 digest
  of the row key (`row_digest`), never the raw key or cell values. The
  digest is stable per row, so repeated operations on the same row
  correlate, but the key cannot be recovered.

One audit record looks like:

```json
{"level":"INFO","msg":"audit-observer: audit","op":"put","table":"ns:users",
 "region":"d5a1...","row_digest":"9f86d081884c7d65","cells":1,"seq":42}
```

## Build

```bash
make audit-observer-jar
# → examples/audit-observer/target/audit-observer.jar
```

## Run the integration test

Brings up the dockerized HBase 2.5 standalone, attaches the coproc to a test
table, performs 25 Puts + 25 Deletes, and asserts exactly 50 audit records
in the RegionServer log:

```bash
make test-integration-audit
```

## Attach to your own table

```java
admin.modifyTable(TableDescriptorBuilder.newBuilder(existing)
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.virogg.hbasecop.examples.audit.AuditRegionObserver")
        .setJarPath("file:///path/to/audit-observer.jar")
        .build())
    .build());
```
