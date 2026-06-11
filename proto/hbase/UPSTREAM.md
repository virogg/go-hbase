# Vendored HBase proto subset

The `.proto` files in this directory are a **trimmed copy** of
`hbase-protocol-shaded/src/main/protobuf/` from upstream
[apache/hbase][hbase], rewritten to live under our own package alias so
generated code can be linked into this project without dragging in HBase's
runtime jars.

[hbase]: https://github.com/apache/hbase

## Pin

| Field      | Value                                       |
|------------|---------------------------------------------|
| Tag        | `rel/2.5.10`                                |
| SHA        | `1a89f98d268eab842ee376563b6961b030fee916`  |
| Subtree    | `hbase-protocol-shaded/src/main/protobuf/`  |
| Fetched on | 2026-05-13                                  |

## Files in this vendor

| File           | Upstream                | Trim notes                                                                 |
|----------------|-------------------------|----------------------------------------------------------------------------|
| `Cell.proto`   | `Cell.proto`            | Verbatim message bodies (`Cell`, `KeyValue`, `CellType`).                  |
| `HBase.proto`  | `HBase.proto`           | Slim: `TableName`, `TimeRange`, `ColumnFamilyTimeRange`, `NameBytesPair`, `RegionSpecifier`, `ServerName`, `RegionInfo`. Other top-level messages dropped — re-add as later tasks need them. |
| `Client.proto` | `Client.proto`          | Slim: `MutationProto` (with nested `ColumnValue`, `QualifierValue`, `Durability`, `MutationType`, `DeleteType`) only. `Get`, `Result`, `Scan`, `MutateRequest`/`Response`, etc. defer to T43+. |

## Rewrites applied to every file

- `package hbase.pb;` → `package virogg.hbasecop.hbase.v1;`
- `option java_package = "org.apache.hadoop.hbase.shaded.protobuf.generated";`
  → `option java_package = "com.virogg.hbasecop.hbase.v1";`
- Added `option go_package = "github.com/virogg/go-hbase/internal/wire/hbasepb";`
- Added SPDX header (Apache-2.0; upstream copyright preserved verbatim).
- `import "X.proto";` → `import "hbase/X.proto";` so a single
  `--proto_path=proto` resolves both vendored and project-level files.

Field numbers and message bodies are kept byte-identical to upstream so PB
wire encoding stays compatible with HBase itself — this is what lets a
RegionObserver hand a `Put` straight to the Go side without translation.

## How to bump

1. Pick a new HBase tag (e.g. `rel/2.5.11`).
2. Diff the upstream subtree against this directory; re-apply the rewrites
   above. Most fields are stable across patch releases — keep the slim
   subset slim.
3. Update the table above with the new tag/SHA.
4. `make proto && make all` — golden tests reject silent wire changes.
