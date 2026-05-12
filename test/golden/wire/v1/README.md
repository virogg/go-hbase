# wire/v1 golden corpus

Cross-language byte-level fixtures for the go-hbase wire format
(`internal/wire` ↔ `com.virogg.hbasecop.bridge.wire`).

`fixtures.tsv` defines each fixture; `<name>.bin` is the encoded wire bytes
that **both** the Go and Java encoders must produce, byte-for-byte, for the
corresponding logical Message.

## Regenerating

```sh
go run ./cmd/wire-golden -out test/golden/wire/v1
```

The generator only writes `<name>.bin` files; `fixtures.tsv` is hand-edited.
Re-run after any wire-layout change (T11/T12/T13 own this format; T31+ may
extend payload semantics without rewriting bytes).

## Consumed by

- Go: `internal/wire/wire_golden_test.go`
- Java: `test/java/com/virogg/hbasecop/bridge/wire/WireGoldenTest.java`

Both tests assert that:

1. Encoding the logical Message produces the exact bytes in `<name>.bin`.
2. Decoding `<name>.bin` produces a Message equal to the logical Message.

That double-direction check is what proves Go encode ↔ Java decode (and the
reverse) match without running a live cross-process suite.
