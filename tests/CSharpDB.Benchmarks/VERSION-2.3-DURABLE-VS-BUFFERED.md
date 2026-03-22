# CSharpDB Version 2.3 Durable vs Buffered Benchmarks

Captured on March 21, 2026 from `C:\Users\maxim\source\Code\CSharpDB`.

This report compares the new explicit storage durability modes added in `2.3`:

- `Durable`: WAL commit forces an OS-buffer flush before success is reported.
- `Buffered`: WAL commit flushes managed buffers to the OS but does not force an OS-buffer flush per commit.

The benchmark harness was updated to read `CSHARPDB_BENCH_DURABILITY` so the same benchmark code path can be run in both modes.

## Commands

```powershell
$env:CSHARPDB_BENCH_DURABILITY='Durable'
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --macro --repeat 3 --repro
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *ColdLookupBenchmarks*
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *InMemorySqlBenchmarks*
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *InMemoryCollectionBenchmarks*

$env:CSHARPDB_BENCH_DURABILITY='Buffered'
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --macro --repeat 3 --repro
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *ColdLookupBenchmarks*
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *InMemorySqlBenchmarks*
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *InMemoryCollectionBenchmarks*
```

Captured result files:

- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\macro-durable-median-of-3.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\macro-buffered-median-of-3.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\ColdLookupBenchmarks-durable.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\ColdLookupBenchmarks-buffered.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\InMemorySqlBenchmarks-durable.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\InMemorySqlBenchmarks-buffered.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\InMemoryCollectionBenchmarks-durable.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\InMemoryCollectionBenchmarks-buffered.csv`

## Summary

`Buffered` is dramatically faster for commit-heavy file-backed writes, which is exactly what the durability fix predicts. `Durable` pays the real OS-flush cost per commit, while `Buffered` avoids it.

For this benchmark set:

- File-backed single-row writes improved by about `31x-32x` in `Buffered`.
- File-backed batched writes improved by about `6x-9x` in `Buffered`.
- File-backed reads also improved, but much more modestly, mostly around `1.4x-1.9x`.
- Mixed reader throughput was not harmed by `Durable`; in this run it was slightly better than `Buffered`, likely because reader scaling is dominated by scheduler/cache effects rather than commit cost.

## Side-by-Side

### Macro

| Benchmark | Durable | Buffered | Buffered vs Durable |
|---|---:|---:|---:|
| SQL sustained single insert | `371.2 ops/sec` | `11,583.3 ops/sec` | `31.2x` |
| SQL sustained batch insert (100/tx) | `351.9 tx/sec` / `35.2K rows/sec` | `3,080.9 tx/sec` / `308.1K rows/sec` | `8.8x` |
| Collection sustained single put | `362.0 ops/sec` | `11,598.2 ops/sec` | `32.0x` |
| Collection sustained batch put (100/tx) | `320.0 tx/sec` / `32.0K docs/sec` | `1,928.2 tx/sec` / `192.8K docs/sec` | `6.0x` |
| 8-reader scaling reads | `303,752.6 ops/sec` | `234,433.1 ops/sec` | `0.77x` |
| 8-reader burst scaling reads | `5,653,145.5 ops/sec` | `4,854,377.7 ops/sec` | `0.86x` |
| SQL point lookup comparison | `794,174.3 ops/sec` | `770,327.1 ops/sec` | `0.97x` |
| Collection point lookup comparison | `1,164,466.7 ops/sec` | `1,157,436.7 ops/sec` | `0.99x` |

### Micro

| Benchmark | Durable | Buffered | Buffered vs Durable |
|---|---:|---:|---:|
| SQL cold lookup (file-backed) | `94.801 us` / `10.5K ops/sec` | `66.883 us` / `15.0K ops/sec` | `1.42x` |
| Collection cold get (file-backed) | `96.965 us` / `10.3K ops/sec` | `65.364 us` / `15.3K ops/sec` | `1.48x` |
| SQL point lookup (file-backed) | `803.4 ns` / `1.24M ops/sec` | `428.4 ns` / `2.33M ops/sec` | `1.88x` |
| Collection get (file-backed) | `314.6 ns` / `3.18M ops/sec` | `224.0 ns` / `4.46M ops/sec` | `1.40x` |
| SQL insert (file-backed) | `2,768,207.3 ns` / `361.2 ops/sec` | `95,839.2 ns` / `10.4K ops/sec` | `28.9x` |
| Collection put (file-backed) | `2,757,432.9 ns` / `362.7 ops/sec` | `101,569.2 ns` / `9.8K ops/sec` | `27.1x` |

## Interpretation

The practical product split is now clear:

- `Durable` is the correctness mode. It behaves like a real crash-safe commit path and should be the default.
- `Buffered` is the high-throughput mode. It is materially faster for commit-heavy workloads, but the database can acknowledge a commit that is still sitting in OS buffers.

The data also shows that the old `2.2` numbers were effectively closer to `Buffered` semantics than true durable semantics. Version `2.3` now makes that tradeoff explicit instead of accidental.
