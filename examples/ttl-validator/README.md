# ttl-validator

Pre-hook validation example (T73): a Go observer that rejects any `Put`
whose cell values do not declare a TTL envelope. Demonstrates the
**strict** failure policy, where a Go-side rejection aborts the client's
write with an `IOException`.

## What it demonstrates

- **Pre-hooks** (`PrePut`) under the default **strict** policy: returning an
  error from Go maps to an `IOException` at the HBase client; the write
  never lands.
- **In-database validation in Go**: domain rules run inside the
  RegionServer, on every write path (API, MR jobs, other clients), not just
  in application code.
- **Payload privacy** (SPEC §8): rejection reasons name column coordinates
  (family/qualifier: schema, not data) but never echo cell values.

## The validation rule

Every cell value must start with a textual TTL envelope:

```
ttl=<seconds>;<payload>      e.g.  ttl=3600;{"name":"alice"}
```

`<seconds>` is 1-9 digits, > 0. Anything else (missing prefix, zero TTL,
no `;` terminator) rejects the whole `Put`:

```
org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException:
  ... ttl-validator: cf:q - value lacks the "ttl=" TTL envelope ...
```

## Build

```bash
make ttl-validator-jar
# → examples/ttl-validator/target/ttl-validator.jar
```

## Run the integration test

Brings up the dockerized HBase 2.5 standalone, attaches the coproc, and
asserts: valid Put succeeds and is readable; invalid Put throws
`IOException` and leaves no row behind:

```bash
make test-integration-ttl
```

## Attach to your own table

```java
admin.modifyTable(TableDescriptorBuilder.newBuilder(existing)
    .setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("com.virogg.hbasecop.examples.ttl.TtlValidatorRegionObserver")
        .setJarPath("file:///path/to/ttl-validator.jar")
        .build())
    .build());
```
