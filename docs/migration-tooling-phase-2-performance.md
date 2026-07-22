# Migration Tooling Phase 2 Performance Qualification

Measured July 22, 2026 on branch `version4.3.0` at base commit `4462546`
with the Phase 1/2 working tree applied.

## Scope

The qualification exercises the real file-backed Phase 2 target path:

- canonical batch digest construction;
- prepared target inserts;
- one atomic batch receipt per transaction;
- the 2,048-page bounded target cache and write-optimized checkpoint policy;
- all post-load schema stages; and
- a coherent target row count after the load.

It intentionally starts from prepared `DbValue` rows. Source decoding and
provider-specific conversion costs belong in each connector's later benchmark.

## Environment

- Windows `10.0.26200`, x64
- Intel64 Family 6 Model 167, 16 logical processors
- .NET SDK `10.0.203`, Release configuration
- reproducible benchmark mode: high priority, pinned to 8 logical processors

## Reproduction

```powershell
dotnet build tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -c Release --no-restore
dotnet run -c Release --no-build --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --migration-target-scenario Rows100K_Batch1000_Text64 --repro
dotnet run -c Release --no-build --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --migration-target-scenario Rows1M_Batch1000_Text64 --repro
```

## Results

Both scenarios use 1,000-row batches containing one 64-character TEXT value.
The canonical live batch bound is 69,000 bytes in both runs.

| Rows | Batches | Throughput | Batch P50 | Batch P99 | Peak batch | Managed heap after | Process peak working set |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 100 | 55,964 rows/s | 13.957 ms | 45.246 ms | 1,000 rows / 69,000 bytes | 10,426,384 bytes | 86,122,496 bytes |
| 1,000,000 | 1,000 | 85,805 rows/s | 8.004 ms | 29.186 ms | 1,000 rows / 69,000 bytes | 21,685,304 bytes | 106,500,096 bytes |

The 10x row-count increase retained the exact same live migration batch bound.
The process peak increased by about 1.24x rather than with source size. Total
managed allocations still scale with rows because converted row and digest
objects are short-lived; they are not retained as the source grows.

Each run also verifies that receipt count and receipt row totals are exact and
that the staged target contains the requested row count after every schema
stage. These figures are qualification evidence for the current implementation,
not a cross-machine performance commitment.
