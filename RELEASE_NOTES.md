# What's New

## CSharpDB 4.6.6

CSharpDB 4.6.6 improves durable write throughput, prepared queries, and bounded
primary-key reads while strengthening reader snapshot, buffered-row, and concurrent
WAL-read correctness.
These notes cover changes since v4.6.5.

### Query and write improvements

- Reuse prepared SELECT plans across parameter values and support bounded integer
  primary-key ranges through pooled readers and ordinary queries.
- Preserve reader catalog snapshots across commits and checkpoints, including
  schema metadata used by existing readers.
- Preserve committed row-count metadata during writes and restore it after
  publication, avoiding unnecessary full-table scans for reader `COUNT(*)` queries.
- Reduce table-scan, LIMIT, snapshot-pager, and leaf-prefetch allocations. Forward
  intact row batches while preserving ownership and mixed row/batch traversal.
- Streamline primary-key mutations and INSERT execution by reusing column ordinals,
  avoiding per-row observer allocations, and skipping redundant canonical value
  conversions.
- Reduce durable-write checkpoint stalls and improve WAL/checkpoint lifecycle
  handling and storage-page ownership.

### Storage correctness

- Prevent checkpoint reset or retained-tail compaction from invalidating WAL
  offsets used during write-conflict resolution. This fixes a concurrency race
  that could cause short WAL reads or return incorrect page contents.
- Defer checkpoint finalization while durable commits still have unpublished WAL
  offsets, preserving correct reads after those commits complete.
- Add deterministic regression coverage for reset and compaction, including stale
  offsets that remain within file bounds, cancellation, lock release, and forward
  progress. The database and WAL formats are unchanged.

### Dependencies and maintenance

- Update gRPC/Protobuf, SQL Server migration dependencies, WebView2, OpenTelemetry,
  Scalar, and test tooling.
- Retain Microsoft.OpenApi 2.12.0 for compatibility with
  Microsoft.AspNetCore.OpenApi 10.0.12 on .NET 10.

### Performance guardrails

- Validate performance evidence integrity and reject incomplete or inconsistent
  comparison evidence.
