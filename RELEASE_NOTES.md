# What's New

## CSharpDB 4.6.6

CSharpDB 4.6.6 improves durable write throughput, prepared queries, and bounded
primary-key reads while strengthening reader snapshot and buffered-row correctness.
These notes cover changes since v4.6.5.

### Query and write improvements

- Reuse prepared SELECT plans across parameter values and support bounded integer
  primary-key ranges through pooled readers and ordinary queries.
- Preserve reader catalog snapshots across commits and checkpoints, including
  schema metadata used by existing readers.
- Reduce table-scan, LIMIT, snapshot-pager, and leaf-prefetch allocations. Forward
  intact row batches while preserving ownership and mixed row/batch traversal.
- Streamline primary-key mutations and INSERT execution by reusing column ordinals,
  avoiding per-row observer allocations, and skipping redundant canonical value
  conversions.
- Reduce durable-write checkpoint stalls and improve WAL/checkpoint lifecycle
  handling and storage-page ownership.

### Performance guardrails

- Validate performance evidence integrity and reject incomplete or inconsistent
  comparison evidence.
