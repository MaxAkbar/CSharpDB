# Retained CSV Large-Stream Qualification

Measured July 22, 2026 on branch `version4.3.0` at base commit `57a76e4`
with the Phase 4A qualification working tree applied.

## Scope

The qualification exercises the real strict retained-CSV path:

- streamed generation of RFC 4180 records whose fixed 64-byte TEXT value is
  quoted, multiline, and contains an escaped quote;
- immutable snapshot creation, bounded format inspection, a 1,000-row schema
  profile, catalog/plan creation, and atomic package publication;
- deletion of the raw CSV followed by digest-pinned package reopen;
- complete source replay with cursor-chain and payload checks;
- prepared staged-target apply with transactional receipts;
- a fresh package/target session that resumes without duplicate writes; and
- bounded source/target canonical checksum validation.

The 50,000-row `CsvLargeStreamTests` fixture runs in CI without timing or
process-memory assertions. It independently checks row- and byte-triggered
batches, exact cursor/order semantics, raw-source deletion, package reopen,
and workspace/package-temporary cleanup.

The adapter now also treats its defaults as absolute safety ceilings: 16 MiB
per decoded field, 64 MiB per logical record, 16,384 fields, 1,000,000 profile
records, 64 MiB of profile characters, and 16,384 ordinal overrides. Package
manifest text has a 1 MiB serialization/reopen budget enforced before
canonicalization or raw copying. A valid re-signed package cannot raise these
limits or trigger raw copying before the policy is rejected.

## Environment

- Windows `10.0.26200`, x64
- Intel64 Family 6 Model 167, 16 logical processors
- .NET SDK `10.0.203`, Release configuration
- reproducible benchmark mode: high priority, pinned to 8 logical processors

## Reproduction

```powershell
dotnet test tests/CSharpDB.Migration.Files.Tests/CSharpDB.Migration.Files.Tests.csproj -c Release --no-restore --filter FullyQualifiedName~CsvLargeStreamTests
dotnet build tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -c Release --no-restore
dotnet run -c Release --no-build --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --csv-retained-migration-scenario Rows100K_Batch1000_Text64 --repro
dotnet run -c Release --no-build --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --csv-retained-migration-scenario Rows1M_Batch1000_Text64 --repro
```

The two measured sizes run in separate processes so peak working-set values
are not cumulative.

## Results

| Phase | 100K elapsed | 100K rows/s | 1M elapsed | 1M rows/s |
| --- | ---: | ---: | ---: | ---: |
| Inspect and package | 403.8 ms | 247,660 | 862.6 ms | 1,159,274 |
| Digest-pinned package open | 76.7 ms | 1,303,745 | 310.6 ms | 3,219,352 |
| Full retained replay | 478.3 ms | 209,072 | 2,792.0 ms | 358,171 |
| Prepared staged apply | 1,782.1 ms | 56,112 | 8,987.5 ms | 111,265 |
| Fresh-session resume | 581.0 ms | 172,112 | 2,787.8 ms | 358,701 |
| Canonical checksum validation | 2,192.9 ms | 45,602 | 10,646.8 ms | 93,925 |

| Rows | Raw CSV bytes | Package bytes | Maximum process peak working set | Maximum managed heap after a phase | Peak live apply batch | Peak checksum spill |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 6,900,007 | 6,901,847 | 110,198,784 | 24,184,928 | 1,000 rows / 69,000 bytes | 25,600,160 bytes |
| 1,000,000 | 69,000,007 | 69,001,849 | 186,331,136 | 37,235,272 | 1,000 rows / 69,000 bytes | 256,000,608 bytes |

The 10x source-size increase retained the exact same live row/byte bound. The
maximum observed process peak grew by 1.69x and the maximum post-phase managed
heap by 1.54x, rather than in proportion to source size. Total allocations do
scale with rows because decoded fields, normalized values, canonical hashes,
and target rows are short-lived; they are not retained as the source grows.

Checksum spill grew by exactly 10x, as expected for bounded external sorting.
The validator reports that disk explicitly and deletes it after the run. A
package-open session owns exactly one private raw snapshot of the retained
source length. The harness verifies that snapshot workspaces, package temporary
files, WAL files, and migration locks are gone after their owners close.

## Correctness Gate

Both scenarios passed all deterministic gates:

- package content length, manifest pin, catalog digest, source identity, and
  package bytes remained unchanged;
- replay produced exactly 100/1,000 contiguous batches and 100K/1M rows;
- apply wrote every row once with 100/1,000 matching receipts;
- fresh-session resume wrote zero rows and skipped every committed batch;
- target counts were exact; and
- source and target canonical checksums matched
  (`59eefb6b...b69f5` for 100K and `f26e43e8...075d3` for 1M).

These measurements qualify the current implementation on this machine. They
are diagnostic evidence, not a portable throughput or memory SLA.
