# Compression SDK Sample

This sample keeps compression at the application/SDK layer. It shows how an app can compress large text, JSON, or blob payloads before storing them as ordinary CSharpDB `BLOB` values, and includes benchmarks so users can decide whether the tradeoff is useful for their own data.

Compression is not part of the current CSharpDB storage format. There is no database-wide compression flag here, no page/WAL format change, and no transparent SQL or `Collection<T>` behavior change.

Run the SDK-style demo:

```powershell
dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --demo
```

The demo stores a large JSON-like document with codec metadata columns:

- `codec`
- `original_bytes`
- `payload`

The helper API lives in:

```text
samples/compression-sdk/PayloadCompression.cs
```

Example:

```csharp
CompressedPayload payload = PayloadCompression.CompressText(json, CompressionCodec.GZip, minimumBytes: 1024);
string jsonAgain = PayloadCompression.DecompressText(payload);
```

## Goals

- Reduce database and WAL bytes for text-heavy rows, JSON collection payloads, and larger blob-like values.
- Avoid slowing hot point reads or writes for ordinary small records.
- Keep crash recovery, checkpointing, backup/restore, and older database compatibility explicit.
- Keep compression optional, measurable, and outside the engine unless future benchmark evidence justifies deeper storage work.

## Candidate 1: SQL Payload Compression SDK

Compress selected application payload columns before storing them as normal `BLOB` values. Store codec metadata in ordinary columns next to the payload.

Pros:

- No durable-format change.
- Localized to application DTO/mapper code.
- Can mix compressed and uncompressed rows in one table.

Risks:

- Per-record overhead can dominate small rows.
- SQL predicates, ordering, full-text search, and indexes cannot inspect compressed payload bytes.
- Apps should keep searchable fields in separate uncompressed columns.

Entry criteria:

- Compression metadata is versioned in application-visible columns.
- Uncompressed small-payload path stays the default.
- Focused benchmarks show no more than 10% regression for hot point reads on small rows.

## Candidate 2: Collection Payload Compression SDK

Compress a selected field inside a collection document while leaving keys and queryable metadata uncompressed.

Pros:

- Targets the most compressible CSharpDB payload shape first.
- Avoids changing collection storage internals.
- Compatible with collection-specific thresholds and future generated app codecs.

Risks:

- Path indexes can only use uncompressed sidecar fields.
- Compression may hurt small document updates and point reads.
- Mixed legacy JSON payloads and compressed payload fields need clean app-level detection.

Entry criteria:

- Existing application documents remain readable.
- Path-index usage keeps query fields uncompressed.
- Benchmarks show meaningful file-size reduction on text-heavy JSON collections with bounded read overhead.

## Candidate 3: True Page-Level Compression

Compress whole data pages before they are written to the main file and WAL.

Pros:

- Best chance to reduce total file and WAL bytes.
- Works below SQL and collection layers.
- Can improve cold reads if I/O reduction beats CPU overhead.

Risks:

- Largest storage-format change.
- Page IDs, free-list accounting, page checks, WAL frames, snapshots, and checkpointing all need a new compressed-page contract.
- Random in-place page overwrite is harder when compressed sizes vary.
- Memory-mapped clean-page reads become more complicated.

Entry criteria:

- New format version and migration/export path are defined.
- WAL recovery can replay compressed and uncompressed page frames.
- Storage inspector can identify compressed pages without needing application schema.
- Crash-recovery tests cover interrupted compressed-page commits and checkpoints.

## Benchmark Harness

The exploratory codec-level BenchmarkDotNet harness lives in:

```text
samples/compression-sdk/Benchmarks/CompressionCandidateBenchmarks.cs
```

Run it with:

```powershell
dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --micro --filter *CompressionCandidateBenchmarks*
```

The current harness measures BCL GZip and Brotli candidates over synthetic record-payload, collection-payload, and page-like byte shapes. It is intentionally not a release gate and does not imply a chosen implementation.

The end-to-end benchmark lives in:

```text
samples/compression-sdk/Benchmarks/CompressionEndToEndBenchmark.cs
```

Run the full profile with:

```powershell
dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --e2e --repeat 3
```

Run the smoke profile with:

```powershell
dotnet run -c Release --project samples/compression-sdk/CompressionSdkSample.csproj -- --e2e-quick
```

The end-to-end benchmark creates real CSharpDB database files, writes uncompressed and compressed payload variants, checkpoints the WAL, runs hot point reads, and emits CSV rows with:

- original payload bytes
- stored payload bytes
- pre-checkpoint `.db + .wal` bytes
- post-checkpoint `.db + .wal` bytes
- final size ratio versus the uncompressed baseline for the same surface
- read/write throughput regression versus the uncompressed baseline

Current end-to-end scenarios are payload-level candidates:

- SQL text record payload: uncompressed `TEXT` versus compressed `BLOB`
- SQL page-like blob payload: uncompressed `BLOB` versus compressed `BLOB`
- collection JSON document payload: uncompressed string document versus compressed byte-array document through `Collection<T>`

These scenarios are useful before/after evidence for payload-level compression candidates. They are not transparent page-level storage compression and should not be described as such.

## Acceptance Criteria For Future Engine Compression

Before enabling a durable compression format:

- Final `.db` plus `.wal` size drops at least 15% on text-heavy SQL and collection workloads.
- Hot point-read regression is no more than 10% on small-row and small-document workloads.
- Cold-read benchmarks are neutral or better on cache-pressured file-backed workloads.
- Write benchmarks include single-row auto-commit, batch writes, and concurrent durable writes.
- Backup/restore, WAL recovery, storage inspector, vacuum, reindex, and hybrid mode all pass compressed-format tests.
- Existing uncompressed databases remain readable without migration.
- Format metadata allows future compression codecs without guessing from payload bytes.

Engine-level compression should move from research into implementation only after the benchmark harness shows where the CPU/I/O tradeoff is actually favorable.
