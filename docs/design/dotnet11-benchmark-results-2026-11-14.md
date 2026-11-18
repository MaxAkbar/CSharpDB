# First .NET 10 / .NET 11 core-engine comparison

Date: 2026-11-14. Branch: `max/dotnet11`, with uncommitted migration changes.

The clearest result is a **7.1% increase in median cached point-read throughput**.
Full scans improved 4.2% and in-memory inserts 3.1% in this workload. These three
workloads improved in each pair. Sorting changed little. Parsing and durable
writes were too variable to establish a repeatable improvement or regression.

## Main comparison

Numbers are medians of three fresh-process runs per runtime. Change is the ratio
of the two medians; the paired range separately shows each matched run's change.
Positive throughput changes favor .NET 11. Scan and sort rates are whole queries
over 10,000 rows, not rows/second. Batch rates are inserted rows/second.

| Workload | .NET 10 / second | .NET 11 / second | Median change | Changes across the three pairs |
| --- | ---: | ---: | ---: | --- |
| Parse SELECT | 457,288 | 482,277 | +5.5% | -3.7% to +8.5%; mixed |
| Cached primary-key query, 10K rows | 1,760,136 | 1,885,908 | **+7.1%** | +5.1% to +9.9% |
| Full scan and materialization, 10K rows | 658.2 | 686.0 | **+4.2%** | +0.9% to +6.1% |
| ORDER BY and materialization, 10K rows | 317.4 | 319.9 | +0.8% | +0.3% to +5.5%; small median change |
| In-memory single-row INSERT | 269,247 | 277,569 | **+3.1%** | +1.3% to +3.5% |
| Durable single-row INSERT | 283.8 | 273.0 | -3.8% | -5.8% to +5.0%; mixed |
| Durable prepared batch, 100 rows/commit | 26,762 | 27,213 | +1.7% | -4.7% to +4.4%; mixed |

Cached point reads were the most stable comparison: throughput relative standard
deviation was 1.55% on .NET 10 and 1.21% on .NET 11, with non-overlapping observed
throughput ranges. Full scans and memory inserts showed smaller gains; three
pairs are an initial indication, not a statistical guarantee for other workloads.
Parsing varied substantially on .NET 11 (5.52% relative standard deviation), so
its positive median must not be presented as a reliable parser speedup.

### Allocations

| Workload | .NET 10 bytes/unit | .NET 11 bytes/unit | Change |
| --- | ---: | ---: | ---: |
| In-memory INSERT | 1,938.7 | 1,875.7 | **-3.25%** |
| Cached point query | 631.9 | 631.9 | Essentially unchanged |
| Full scan | 2,889,041 | 2,889,041 | Essentially unchanged |
| Parse SELECT | 5,968.2 | 5,968.2 | Essentially unchanged |

The allocation counters cover the entire process, including async/background
work. They are sampled at measurement boundaries, not for every database call.
Write tables grow during the interval; total row counts are retained in each
run's correctness evidence. The memory-insert allocation result is therefore
specific to these durations and table sizes.

## Focused durable-write confirmation

The initial suite reported higher process CPU per durable insert on .NET 11
(223.7 to 372.8 microseconds at the medians). This warranted a separate check.
Six additional fresh processes ran only durable single-row inserts, with a
4-second warmup and 20-second measurement, again using three alternating pairs.

| Metric | .NET 10 | .NET 11 | Change |
| --- | ---: | ---: | ---: |
| Median inserts/second | 283.6 | 290.8 | +2.5% |
| Median process CPU microseconds/insert | 275.4 | 255.0 | -7.4% |
| Median allocated bytes/insert | 3,389.4 | 3,334.1 | -1.63% |

The throughput changes by pair were +0.2%, -1.2%, and +5.3%. CPU changes also
changed direction. The initial CPU increase and throughput decrease did **not**
reproduce consistently. Neither a durable-write speedup nor a durable-write CPU
regression is established. The two experiments use different durations and are
reported separately rather than pooled into a favorable aggregate.

## Method and scope

- Windows 11 Pro x64, Intel Core i9-11900K, 8 physical / 16 logical processors.
  The repository's process tuner selected high priority and affinity `0xFF`
  (8 logical processors). This is a single-client benchmark.
- Actual loaded runtimes: **.NET 10.0.15** and **.NET 11.0.0**. Both targets were
  compiled in Release with SDK **11.0.100**.
- Eight core-library projects were copied from the migrated working tree and
  built for both `net10.0` and `net11.0`. Their C# source is identical across
  conditions. Source hashes, the base commit, migration patch, and executed
  assembly hashes are retained. Both builds passed without warnings or errors.
- This isolates runtime/target-framework effects for the selected core-engine
  operations. It is not a complete `main` versus branch comparison and does not
  cover the client, API, EF provider, gRPC, concurrent clients, or NativeAOT.
- The harness reuses `BenchmarkDatabase`, deterministic `DataGenerator`,
  `BenchmarkDurability`, and `BenchmarkProcessTuner` from the existing suite.
  It is a bounded custom harness, not a BenchmarkDotNet or release-qualification run.
- Main measurements: 2-second warmup, forced GC outside the timed interval, then
  8 seconds per workload. Three pairs ran in order **10, 11, 11, 10, 10, 11**.
  Parsing uses 64 parses per harness operation to amortize harness timing costs.
- Latency sampling is every 64th cached lookup, every 8th parser batch/memory
  insert, and every operation for scans, sorts, and durable writes. Each retained
  at least 100 observations; raw operation latency percentiles are in the JSON.
  Parser operation percentiles describe a batch of 64 parses, not one parse.
- Read databases contain 10,000 deterministic rows and are warmed. Query result
  counts and point-read IDs are checked; sort ordering is checked before timing.
- Writes retain **Durable** mode with default storage settings and no group
  commit or relaxed flush policy. Row counts and value sums are checked after
  every write workload and again after closing/reopening file-backed databases.
- No application runtime-async switch or other performance optimization was
  enabled. No builds, tests, or parallel benchmark jobs ran during timing.
  Ordinary OS/application activity and dynamic CPU frequency were not isolated.

All **12 measured process runs** passed correctness and durable reopen checks.
All **48 workload measurements** retained the required duration/sample floors.
Executed artifact hashes and original C# source hashes were checked again after
both experiments. A preliminary 3-second .NET 11 pilot is excluded from these
tables. These are exploratory local results, not production or release guarantees.

## Evidence and next experiment

The [main summary CSV](benchmarks/dotnet11-2026-11-14/summary.csv),
[focused summary CSV](benchmarks/dotnet11-2026-11-14/focused-summary.csv),
[main run manifest](benchmarks/dotnet11-2026-11-14/run-manifest.json), and
[focused manifest](benchmarks/dotnet11-2026-11-14/focused-manifest.json) accompany
the report. Individual JSON reports are under `measured/` and `focused/` in that
same evidence directory. Harness source text, analysis scripts, the migration
source patch, and source-file hashes are also retained there.

The executable isolated workspace is `.tmp/dotnet11-benchmark-20261114`.
Its `Run-Comparison.ps1` and `Run-Focused.ps1` expect new output paths; preserve
these results and use a fresh workspace/output directory when rerunning.

The next useful experiment is an explicit runtime-async variant against the
current .NET 11 build, using the same workloads and correctness checks. It should
also include longer isolated write measurements before changing any defaults.
The [migration plan](dotnet11-migration-and-performance.md) contains the remaining
source-level candidates and acceptance criteria.
