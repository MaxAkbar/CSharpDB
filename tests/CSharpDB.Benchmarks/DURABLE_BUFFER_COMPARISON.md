# CSharpDB Durable Buffer Comparison

Captured on March 22, 2026 from `C:\Users\maxim\source\Code\CSharpDB`.

This report reruns the macro benchmark suite on the current tree after removing the fixed production batch window from durable group commit. It compares:

- `Durable`: WAL commit drains managed buffers and forces an OS-buffer flush before success.
- `Buffered`: WAL commit flushes managed buffers but does not force an OS-buffer flush per commit.

## Commands

```powershell
$env:CSHARPDB_BENCH_DURABILITY='Durable'
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --macro --repeat 3 --repro

$env:CSHARPDB_BENCH_DURABILITY='Buffered'
dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --macro --repeat 3 --repro
```

## Captured Files

- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\macro-durable-group-commit-median-of-3.csv`
- `tests\CSharpDB.Benchmarks\bin\Release\net10.0\results\macro-buffered-group-commit-median-of-3.csv`
- historical comparison: `tests\CSharpDB.Benchmarks\VERSION-2.3-DURABLE-VS-BUFFERED.md`

## Headline

Removing the fixed durable batch delay recovered the durable regression from the earlier group-commit run.

The current durable path is now faster than the earlier `2.3` durable baseline on the main macro write benchmarks, while `Buffered` remains the throughput mode by a very large margin.

## Current Side-by-Side

| Benchmark | Durable | Buffered | Buffered vs Durable |
|---|---:|---:|---:|
| SQL sustained single insert | `452.0 ops/sec` | `28,221.5 ops/sec` | `62.4x` |
| SQL sustained batch insert (100/tx) | `441.3 tx/sec` / `44.1K rows/sec` | `6,902.2 tx/sec` / `690.2K rows/sec` | `15.6x` |
| SQL mixed workload reads | `1,673.4 ops/sec` | `61,914.8 ops/sec` | `37.0x` |
| SQL mixed workload writes | `435.0 ops/sec` | `15,478.6 ops/sec` | `35.6x` |
| Collection single put | `419.2 ops/sec` | `26,862.6 ops/sec` | `64.1x` |
| Collection batch put (100/tx) | `395.0 tx/sec` / `39.5K docs/sec` | `4,399.9 tx/sec` / `440.0K docs/sec` | `11.1x` |
| SQL point lookup comparison | `1,465,426.1 ops/sec` | `1,461,942.6 ops/sec` | `1.00x` |
| Collection point lookup comparison | `1,997,662.6 ops/sec` | `1,984,073.3 ops/sec` | `0.99x` |
| Checkpoint 500-frame time | `5.603 ms` | `2.823 ms` | `1.98x` |
| Checkpoint 1000-frame time | `4.612 ms` | `2.294 ms` | `2.01x` |

## Compared To Version 2.3

Compared with `VERSION-2.3-DURABLE-VS-BUFFERED.md`, the current macro medians moved as follows:

| Benchmark | 2.3 Durable | Current Durable | Change | 2.3 Buffered | Current Buffered | Change |
|---|---:|---:|---:|---:|---:|---:|
| SQL sustained single insert | `371.2` | `452.0` | `+21.8%` | `11,583.3` | `28,221.5` | `+143.6%` |
| SQL sustained batch insert (100/tx) | `351.9` | `441.3` | `+25.4%` | `3,080.9` | `6,902.2` | `+124.0%` |
| Collection single put | `362.0` | `419.2` | `+15.8%` | `11,598.2` | `26,862.6` | `+131.6%` |
| Collection batch put (100/tx) | `320.0` | `395.0` | `+23.4%` | `1,928.2` | `4,399.9` | `+128.2%` |

Those are observed benchmark deltas, not proof that group commit alone caused every improvement. The durable result is the key signal here: the current production path no longer shows the earlier durable collapse.

## Compared To The Earlier Broken Group-Commit Run

The stale fixed-window report in the previous `DURABLE_BUFFER_COMPARISON.md` had durable writes around `64 ops/sec` / `64 tx/sec`. Relative to that run, the current durable macro medians recovered to:

- SQL sustained single insert: `64.0 -> 452.0 ops/sec` (`7.1x`)
- SQL sustained batch insert: `64.0 -> 441.3 tx/sec` (`6.9x`)
- Collection single put: `64.0 -> 419.2 ops/sec` (`6.6x`)
- Collection batch put: `64.0 -> 395.0 tx/sec` (`6.2x`)

That confirms the fixed batch delay, not the batching architecture itself, was the main cause of the prior regression.

## Interpretation

- `Durable` is still much slower than `Buffered` for commit-heavy file-backed writes because it pays the real OS flush cost.
- The current durable group-commit path is no longer an obvious regression against the earlier `2.3` durable baseline.
- Read-heavy lookup benchmarks remain broadly similar across modes; the biggest separation is still commit-heavy writes and checkpoint timing.
- The benchmark harness still does not prove how much overlap-based batching is happening in these macro runs. It does show that removing the fixed wait restored durable throughput to a defensible range.
