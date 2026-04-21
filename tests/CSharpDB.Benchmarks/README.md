# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current top-level snapshot in this README still centers on the April 7, 2026 broad validation refresh, but it now also includes the April 13, 2026 focused `hybrid-storage-mode` and durable `master-table` median-of-3 reruns after the burst concurrent-read benchmark harness fix. The dedicated multi-writer sections in this README also include the April 10, 2026 Phase 3 closeout reruns for explicit `WriteTransaction`, concurrent durable auto-commit, and `--stress`, plus the April 11, 2026 focused checkpoint-retention, commit-fan-in, and insert-fan-in reruns for the split checkpoint/fan-in phase-4 work, including the later same-day insert-side reservation follow-up.

## April 17, 2026 Regression Investigation

An additional full `--all --repro` rerun completed on April 17, 2026 at 8:34:11 PM PT. That rerun is being kept as an active regression-investigation snapshot and does not replace the published April 7/10/11/13 validation set below.

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --all --repro
pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Compare-Baseline.ps1 `
  -ThresholdsPath .\tests\CSharpDB.Benchmarks\perf-thresholds.json `
  -ReportPath .\tests\CSharpDB.Benchmarks\results\perf-guardrails-20260417-rerun2.md
```

| Item | Result |
|------|--------|
| Full rerun status | `tests/CSharpDB.Benchmarks/results/codex-bench-20260417-180306/monitor.status.json` |
| Full rerun completion | `Friday, April 17, 2026 8:34:11 PM PT` |
| Compare report | `tests/CSharpDB.Benchmarks/results/perf-guardrails-20260417-rerun2.md` |
| Compare result | `Compared 185 rows against baseline. PASS=33, WARN=0, SKIP=152, FAIL=0` |
| Compare note | `The 152 skipped rows were skipped because the current machine fingerprint differed materially from the baseline fingerprint; skip does not mean the suite failed to run.` |
| Broad rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260418-031350.csv` |
| Direct transport rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/direct-file-cache-transport-20260418-031948.csv` |
| Concurrent durable write rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260418-032123.csv` |
| Durable SQL batching rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260418-032300.csv` |
| Explicit `WriteTransaction` rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-transaction-diagnostics-20260418-032413.csv` |
| Commit fan-in rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/commit-fan-in-diagnostics-20260418-032652.csv` |
| Insert fan-in rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260418-032721.csv` |
| Checkpoint-retention rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/checkpoint-retention-diagnostics-20260418-032901.csv` |
| Hybrid storage rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260418-032907.csv` |
| Hybrid hot-set rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260418-033207.csv` |
| Hybrid post-checkpoint rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260418-033218.csv` |
| Stress rerun artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/stress-20260418-033319.csv` |

The biggest deltas in the April 17 rerun versus the currently published README snapshot are:

- `concurrent-write-diagnostics`: down roughly `37%` to `49%`; for example, `W8_Batch250us_10s` moved from `467.3` to `276.9 commits/sec` and `W8_Batch0_10s` moved from `466.7` to `238.4 commits/sec`.
- `commit-fan-in-diagnostics`: down roughly `29%` to `33%`; `AutoCommit_DisjointUpdate_W8_Batch250us_5s` moved from `743` to `530.6 commits/sec` and `ExplicitTx_DisjointUpdate_W8_Batch250us_5s` moved from `765` to `522.0 commits/sec`.
- `insert-fan-in-diagnostics`: down roughly `39%` to `46%`; `duplicateKeys` remained `0`, so this rerun did not reopen the earlier explicit auto-id correctness issue.
- `write-transaction-diagnostics`: the published April 10 artifact still shows many headline hot-insert rows down roughly `20%` to `36%`, while `UpdateDisjoint_W8_Rows10_Batch250us_Prealloc1MiB_10s` was slightly up (`86.2` to `92.0 commits/sec`).
- `checkpoint-retention-diagnostics`: down roughly `34%` to `42%`; `W8_Blocker3s_Batch250us` moved from `391.8` to `257.2 commits/sec`, and the post-release manual checkpoint tail moved from `5.425 ms` to `40.231 ms`.

The explicit `WriteTransaction` deltas need extra caution because the published April 10 snapshot is not fully reproducible on the current runner anymore. A detached clean-worktree replay of the April 11 `8be6912` benchmark/storage snapshot on this same machine measured `W1_Rows1_Batch0` at about `239.8 tx/sec` and `W8_Rows10_Batch250us_Prealloc1MiB` at about `249.7 tx/sec`, versus the April 17 current-tree rerun at `209.4` and `241.0 tx/sec` respectively. That same-machine replay shows the large README-level gap is mostly baseline drift for the explicit suite, while the remaining current-code miss is much smaller and concentrated in a subset of zero-window rows.

The shared checkpoint-path investigation completed on April 18, 2026 for the pathological retention row. Focused same-machine reruns plus code inspection showed that the real current-code checkpoint regression came from a blanket `HasActiveExplicitTransactions` guard in `Pager.TryFinalizeCheckpointAsync`. In the `W8_Blocker3s_Batch250us` scenario, the explicit blocker transaction opens its snapshot read before any WAL frames exist, so it does not actually retain a WAL floor. The blanket guard therefore deferred checkpoint finalization unnecessarily and let the broken April 17 current-tree run swell to `backgroundCheckpointStarts=759`, `walBeforeCheckpointKiB=9499.4`, and `postReleaseCheckpointMs=40.231`.

After removing that blanket finalization guard and leaving finalization governed by the actual retained-offset checks, the April 18, 2026 focused reruns landed back in the expected band: `checkpoint-retention-scenario-W8_Blocker3s_Batch250us-20260418-055755.csv` measured `209 commits/sec`, `backgroundCheckpointStarts=29`, `walBeforeCheckpointKiB=96.6`, and `postReleaseCheckpointMs=12.954`, while `checkpoint-retention-scenario-W8_NoBlocker_Batch250us-20260418-055853.csv` measured `237 commits/sec`, `backgroundCheckpointStarts=33`, `walBeforeCheckpointKiB=60.4`, and `postReleaseCheckpointMs=9.307`. Later April 18 spot checks stayed in the same order-of-magnitude band even though absolute throughput continued to drift by runner state.

A separate April 18, 2026 same-session A/B also confirmed that the broader inline pending-commit fast path in `WriteAheadLog` should remain for zero-window commits. With that path enabled, `W1_Rows1_Batch0` measured about `219 tx/sec` versus `171 tx/sec` without it, and `ConcurrentDurableWrite_W8_Batch0` measured about `270-272 commits/sec` versus `178 commits/sec` without it. That keeps the remaining zero-window tuning discussion separate from the checkpoint-retention fix rather than incorrectly blaming the WAL inline path for the retained-WAL regression.

An additional April 18, 2026 same-machine scalar-lookup A/B also confirmed that the March 30 focused `ScalarAggregateLookupBenchmarks` timing CSV is no longer directly reproducible on the current runner, even when replayed from a detached clean `b59a8d1` worktree. Both the detached baseline replay and the current-tree rerun now land around `~2.1 us` for the PK rows, so the historical `~0.9 us` March 30 timing snapshot should be treated as runner drift rather than a fresh current-code regression. The same-runner comparison is still useful, and after changing `TryBuildSimpleLookupScalarAggregateColumnQuery` to consume a lightweight lookup plan instead of first materializing and discarding a full lookup operator, the current tree measured the indexed rows materially faster with much lower allocation: `Index lookup scalar COUNT(text_col)` improved by about `28%` to `41%` and `Index lookup scalar SUM(id)` improved by about `20%` to `33%`, while both indexed rows dropped by roughly `8.15 KB` per operation (`18.42-18.54 KB` down to `10.27-10.39 KB`). The PK rows stayed within a small same-runner band and still shed about `0.04 KB` per operation.

The benchmark harness supports scenario-level isolation for the write-path suites used by this investigation:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-write-scenario W8_Batch250us_Prealloc1MiB --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --commit-fan-in-scenario ExplicitTx_DisjointUpdate_W8_Batch250us --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --insert-fan-in-scenario ExplicitTx_AutoId_W8_Batch250us --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-transaction-scenario W8_Rows10_Batch250us_Prealloc1MiB --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --checkpoint-retention-scenario W8_Blocker3s_Batch250us --repro
```

## Latest Validation Snapshot

The latest release guardrail rerun completed on April 7, 2026 with a clean compare report:

```powershell
pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Compare-Baseline.ps1 `
  -ThresholdsPath .\tests\CSharpDB.Benchmarks\perf-thresholds.json `
  -ReportPath .\tests\CSharpDB.Benchmarks\results\perf-guardrails-last.md
```

| Item | Result |
|------|--------|
| Release guardrail report | `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md` |
| Release guardrail result | `Compared 175 rows against baseline. PASS=175, WARN=0, SKIP=0, FAIL=0` |
| Baseline used by release rerun | `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` |
| Release rerun note | `BenchmarkDotNet class refreshes were run sequentially before compare to avoid shared job-directory collisions between concurrent filtered runs.` |

As of April 12, 2026, the release guardrail surface has been updated to match the current phase-4 write contract. The release threshold file no longer treats the older hot-insert `concurrent-write-diagnostics` rows as release blockers. It now gates the shared non-insert queue shape with `commit-fan-in-diagnostics` and the remaining insert-side boundary with `insert-fan-in-diagnostics`, which matches the current storage guidance and the focused April 11, 2026 fan-in reruns.

As of April 13, 2026, the release threshold file also points the `write-transaction-diagnostics` check at `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260413-033723`. That refresh only updates the required `WriteTransactionDiagnostics_W1_Rows100_Batch0_10s` row after a same-runner comparison showed the older March 30 row was no longer reproducible locally, even when replayed against pre-`90615fa` WAL code.

As of April 13, 2026, the release threshold file also points the `InsertBenchmarks` and `CollectionIndexBenchmarks` checks at `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260413-050945`. That refresh carries full clean-HEAD reruns for those two micro benchmark classes after same-runner replays showed the older March 30 rows were no longer reproducible locally across more than the initially suspected rows.

As of April 18, 2026, the release threshold file also points the `ScalarAggregateLookupBenchmarks` check at `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260418-185724`. That refresh is intentionally narrow and captures the same-runner rerun taken after the lookup-plan allocation fix, because the older March 30 scalar-lookup timing snapshot no longer reproduces on this runner even from a detached replay of the old code.

A focused concurrent-read rerun was also completed on April 13, 2026 after the benchmark harness stopped retaining every reused-session burst latency sample:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --hybrid-storage-mode --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --master-table --repeat 3 --repro
```

| Item | Result |
|------|--------|
| Focused storage-mode artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260413-124730-median-of-3.csv` |
| Durable master comparison artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260413-125827-median-of-3.csv` |
| Concurrent-read note | `Reused-session burst rows now retain latency samples at 1/128 in the benchmark harness. Throughput remains exact; latency columns stay sampled for those rows.` |
| Concurrent-read closeout | `The durable master-table burst rows moved back above the earlier April 7 published snapshot while the per-query reader rows stayed well above the same baseline.` |

A separate multi-writer closeout rerun was completed on April 10, 2026 after the retained-WAL compaction fix used by incremental checkpoint finalization:

```powershell
dotnet test .\tests\CSharpDB.Tests\CSharpDB.Tests.csproj -nologo -m:1 --filter "FullyQualifiedName!~SampleSmokeTests"
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-transaction-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-write-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --stress --repeat 3 --repro
```

| Item | Result |
|------|--------|
| Non-sample validation result | `1081/1081 passing` |
| Explicit `WriteTransaction` artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-transaction-diagnostics-20260410-134444-median-of-3.csv` |
| Concurrent durable write artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260410-135529-median-of-3.csv` |
| Stress artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/stress-20260410-140205-median-of-3.csv` |
| Closeout note | `The repeated April 10 rerun surfaced and then revalidated the retained-WAL compaction fix under preallocation/background checkpointing.` |

A focused checkpoint-retention rerun was completed on April 11, 2026 after the checkpoint lock-order fix that allows checkpoint copy to keep progressing while explicit write transactions defer only finalization:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --checkpoint-retention-diagnostics --repeat 3 --repro
```

| Item | Result |
|------|--------|
| Checkpoint-retention artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/checkpoint-retention-diagnostics-20260411-082100-median-of-3.csv` |
| `W8_NoBlocker_Batch250us` | `410.6 commits/sec`, `5.388 ms` post-run manual checkpoint |
| `W8_Blocker3s_Batch250us` | `391.8 commits/sec`, `5.425 ms` post-release manual checkpoint |
| Retention note | `Background checkpoint starts still occurred while the explicit write transaction was open, and the post-release checkpoint stayed in the same low-single-digit millisecond band as the no-blocker control.` |

A focused commit-fan-in rerun was also completed on April 11, 2026 after the shared auto-commit non-insert path moved onto isolated `WriteTransaction` state:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --commit-fan-in-diagnostics --repro
```

| Item | Result |
|------|--------|
| Commit fan-in artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/commit-fan-in-diagnostics-20260411-141949.csv` |
| Auto-commit `W4` disjoint update | `525 commits/sec`, `commitsPerFlush = 1.99`, `maxPendingCommits = 4` |
| Auto-commit `W8` disjoint update | `743 commits/sec`, `commitsPerFlush = 3.37`, `maxPendingCommits = 8` |
| Explicit `W8` disjoint update | `765 commits/sec`, `commitsPerFlush = 3.38`, `maxPendingCommits = 8` |
| Fan-in note | `Shared non-insert auto-commit now reaches the pending WAL commit queue on this runner; the gain is intentionally narrower than hot auto-commit insert loops.` |

A focused insert-fan-in rerun was also completed on April 11, 2026 to answer the remaining hot-insert question directly:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --insert-fan-in-diagnostics --repro
```

| Item | Result |
|------|--------|
| Insert fan-in artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260411-165557.csv` |
| Auto-commit explicit-id `W8` | `458 commits/sec`, `commitsPerFlush = 1.00`, `maxPendingCommits = 1` |
| Auto-commit auto-id `W8` | `449 commits/sec`, `commitsPerFlush = 1.00`, `maxPendingCommits = 1` |
| Explicit `WriteTransaction` explicit-id `W8` | `438 commits/sec`, `commitsPerFlush = 1.00`, `maxPendingCommits = 2` |
| Explicit `WriteTransaction` auto-id `W8` | `413 commits/sec`, `commitsPerFlush = 1.00`, `duplicateKeys = 0` |
| Insert note | `The shared row-id reservation follow-up removed the earlier duplicate-key failures from the explicit auto-id rows, but the insert hot path still stayed structural across both shared auto-commit and explicit WriteTransaction variants.` |

A rebuilt April 12, 2026 spot-check of the previously lagging explicit auto-id scenario also came back clean on the current binaries:

| Item | Result |
|------|--------|
| Spot-check artifacts | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-scenario-ExplicitTx_AutoId_W8_Batch250us-20260412-034728.csv`, `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-scenario-ExplicitTx_AutoId_W8_Batch250us-20260412-034745.csv` |
| Explicit `WriteTransaction` auto-id `W8` spot-check | `441-445 commits/sec`, `extraAttempts = 0`, `dirtyParentRecoveries = 0` |
| Spot-check note | `The earlier retry-tail signal did not reproduce after rebuilding the benchmark binaries and rerunning the targeted scenario on the current storage path.` |

A focused concurrent-insert rerun was also completed on April 19, 2026 to close out the wider Plan 3 disjoint-key matrix:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --insert-fan-in-diagnostics --repro
```

| Item | Result |
|------|--------|
| Concurrent insert artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260419-140845.csv` |
| Zero-window disjoint explicit-key control | All `Batch0` disjoint explicit-key rows stayed in a flat `244-273 commits/sec` band with `commitsPerFlush = 1.00` |
| Disjoint explicit-key `W4` crossover | Serialized auto-commit `255.2 commits/sec`; shared concurrent auto-commit `505.8`; explicit `WriteTransaction` `494.3`; `commitsPerFlush` moved from `1.00` to `1.99` / `2.00` |
| Disjoint explicit-key `W8` crossover | Serialized auto-commit `258.3 commits/sec`; shared concurrent auto-commit `834.5`; explicit `WriteTransaction` `883.6`; `commitsPerFlush` moved from `1.00` to `3.28` / `3.41` |
| Hot right-edge boundary | `W8 Batch250us` shared concurrent and explicit explicit-id rows stayed at `255.6` / `257.0 commits/sec` with `commitsPerFlush = 1.00` |
| Auto-id validation | `W8 Batch250us` shared concurrent and explicit auto-id rows stayed at `252.1` / `261.5 commits/sec`; `duplicateKeys = 0` in both cases |
| Closeout note | `Concurrent inserts now show real fan-in on this runner, but only for disjoint explicit key ranges and only once writer count is high enough to fill the 250us durable batch window.` |

A focused composite-index rerun was also completed on April 6, 2026 after the covered composite lookup fix:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *CompositeIndexBenchmarks*
```

| Item | Result |
|------|--------|
| Focused composite rerun artifact | `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CompositeIndexBenchmarks-report.csv` |
| Covered composite 100K row | `2.386 us, 3.04 KB allocated` |
| Unique covered composite 100K row | `2.392 us, 3.04 KB allocated` |
| March 30 baseline reference | `3.108 us / 3.159 us, both at 10.63 KB allocated` |

The active published snapshot combines the checked-in March 30, 2026 focused baseline with the latest April 7, 2026 broad reruns, the April 10, 2026 multi-writer closeout reruns, the April 11, 2026 checkpoint-retention, commit-fan-in, and insert-fan-in reruns, and the April 13, 2026 focused storage-mode plus durable master-table reruns:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --all
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-transaction-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --checkpoint-retention-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --commit-fan-in-diagnostics --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --insert-fan-in-diagnostics --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-write-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --stress --repeat 3 --repro
```

| Item | Result |
|------|--------|
| Baseline snapshot | `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` |
| PR guardrail baseline snapshot | `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` |
| Broad main sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260407-070125.csv` |
| Direct transport sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/direct-file-cache-transport-20260407-070730.csv` |
| Hybrid storage sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260407-071155.csv` |
| Focused storage-mode median-of-3 rerun | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260413-124730-median-of-3.csv` |
| Hybrid cold-open sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260407-071440.csv` |
| Hybrid hot-set sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260407-071453.csv` |
| Hybrid post-checkpoint sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260407-071502.csv` |
| Durable batching artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260407-071043.csv` |
| Durable write artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-diagnostics-20260330-121058-median-of-3.csv` |
| Explicit `WriteTransaction` artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-transaction-diagnostics-20260410-134444-median-of-3.csv` |
| Checkpoint-retention artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/checkpoint-retention-diagnostics-20260411-082100-median-of-3.csv` |
| Commit fan-in artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/commit-fan-in-diagnostics-20260411-141949.csv` |
| Insert fan-in artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260411-165557.csv` |
| Concurrent durable write artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260410-135529-median-of-3.csv` |
| Stress artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/stress-20260410-140205-median-of-3.csv` |
| Durable master comparison | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260413-125827-median-of-3.csv` |
| Buffered master comparison | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260407-204058-median-of-3.csv` |
| Broad micro sweep | `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.*-report.csv` |

High-level takeaways from the current refresh:

- The focused validation baseline still points to the checked-in March 30 snapshot built on `main`.
- The top-level SQL and collection API tables below now use the April 7 broad durable sweep.
- The hot steady-state storage-mode tables below now use the April 13 focused `hybrid-storage-mode-20260413-124730-median-of-3.csv` rerun.
- The explicit multi-writer, concurrent durable write, and stress summaries below now use the April 10 `median-of-3` closeout artifacts, and the checkpoint-retention, commit-fan-in, and insert-fan-in summaries use the April 11 targeted reruns.
- The durable master comparison table now uses the April 13 `master-table-20260413-125827-median-of-3.csv` rerun, while the buffered table remains on the April 7 snapshot.
- The current in-memory, cold-lookup, payload, schema-breadth, and collection-index micro spot checks now use April 7 BenchmarkDotNet artifacts.
- The April 13 focused concurrent-read rerun also switched reused-session burst rows to `latency-sampling=1/128`, which keeps the throughput denominator exact without retaining tens of millions of per-op latency samples.
- The April 10 closeout also revalidated the runnable non-sample test suite at `1081/1081` after the retained-WAL compaction fix.
- The April 11 checkpoint-retention rerun showed that holding an explicit write transaction open no longer leaves a large manual checkpoint tail once the blocker is released.
- The April 11 commit-fan-in rerun showed that shared non-insert auto-commit can now reach the pending WAL commit queue on the measured runner.
- The April 11 insert-fan-in rerun showed that the remaining insert hot path is still structural even when the commit path changes.

- `Focused validation baseline refresh on March 30, 2026: tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`
- `Broad main sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260407-070125.csv`
- `Direct client transport sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/direct-file-cache-transport-20260407-070730.csv`
- `Focused storage-mode median-of-3 sweep on April 13, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260413-124730-median-of-3.csv`
- `Durable master comparison median-of-3 sweep on April 13, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260413-125827-median-of-3.csv`
- `Buffered master comparison median-of-3 sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260407-204058-median-of-3.csv`
- `Hybrid storage-mode sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260407-071155.csv`
- `Hybrid cold-open sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260407-071440.csv`
- `Hybrid hot-set sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260407-071453.csv`
- `Hybrid post-checkpoint sweep on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260407-071502.csv`
- `Durable SQL batching capture on April 7, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260407-071043.csv`
- `Durable write median-of-3 capture on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-diagnostics-20260330-121058-median-of-3.csv`
- `Explicit WriteTransaction median-of-3 capture on April 10, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-transaction-diagnostics-20260410-134444-median-of-3.csv`
- `Checkpoint retention median-of-3 capture on April 11, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/checkpoint-retention-diagnostics-20260411-082100-median-of-3.csv`
- `Commit fan-in capture on April 11, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/commit-fan-in-diagnostics-20260411-141949.csv`
- `Insert fan-in capture on April 11, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260411-165557.csv`
- `Concurrent durable write capture on April 10, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260410-135529-median-of-3.csv`
- `Stress median-of-3 capture on April 10, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/stress-20260410-140205-median-of-3.csv`
- `Broad micro sweep on April 7, 2026: BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.*-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ReaderSessionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.MemoryMappedReadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.WalReadCacheBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.BTreeCursorBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanProjectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ColdLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionFieldExtractionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionAccessBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemorySqlBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryCollectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryAdoNetBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryPersistenceBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionPayloadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionSchemaBreadthBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.StorageTuningBenchmarks-report.csv`

## Test Environment

| Component | Details |
|-----------|---------|
| CPU | Intel Core i9-11900K @ 3.50GHz, 8 cores / 16 threads |
| OS | Windows 11 (10.0.26300) |
| Runtime | .NET 10.0.5, X64 RyuJIT AVX-512F |
| Disk | NVMe SSD |
| Page Size | 4,096 bytes |
| WAL Mode | Enabled (redo-log with auto-checkpoint at 1,000 frames) |
| Page Cache | LRU page cache (in-memory) |
| WAL Index | Hash map (O(1) page lookup) |
| Benchmark Mode | Broad API/storage tables use the April 7 `--all` and focused reruns against the checked-in March 30 baseline; explicit multi-writer, concurrent durable write, and stress tables use the April 10 median-of-3 closeout artifacts; checkpoint-retention rows use the April 11 targeted rerun |

## Running Benchmarks

```bash
# Fast PR guardrail pass (dedicated guardrail classes only)
dotnet run -c Release -- --pr

# Focused release/guardrail pass (recommended for normal validation)
dotnet run -c Release -- --release

# Micro-benchmarks (BenchmarkDotNet, original full micro surface)
dotnet run -c Release -- --micro

# Filter to a specific micro suite
dotnet run -c Release -- --micro --filter *InMemory*
dotnet run -c Release -- --micro --filter *InsertBenchmarks*
dotnet run -c Release -- --micro --filter *PointLookupBenchmarks*
dotnet run -c Release -- --micro --filter *ReaderSessionBenchmarks*
dotnet run -c Release -- --micro --filter *SqlMaterializationBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionAccessBenchmarks*
dotnet run -c Release -- --micro --filter *MemoryMappedReadBenchmarks*
dotnet run -c Release -- --micro --filter *WalReadCacheBenchmarks*
dotnet run -c Release -- --micro --filter *BTreeCursorBenchmarks*
dotnet run -c Release -- --micro --filter *CoveringIndexBenchmarks*
dotnet run -c Release -- --micro --filter *CompositeIndexBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionFieldExtractionBenchmarks*
dotnet run -c Release -- --micro --filter *IndexProjectionBenchmarks*
dotnet run -c Release -- --micro --filter *OrderByIndexBenchmarks*
dotnet run -c Release -- --micro --filter *CollationIndexBenchmarks*
dotnet run -c Release -- --micro --filter *IndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *PrimaryKeyAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *DistinctAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *GroupedIndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *CompositeGroupedIndexBenchmarks*
dotnet run -c Release -- --micro --filter *PredicatePushdownBenchmarks*
dotnet run -c Release -- --micro --filter *ScanBenchmarks*
dotnet run -c Release -- --micro --filter *ScanProjectionBenchmarks*
dotnet run -c Release -- --micro --filter *ScalarAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *DistinctBenchmarks*
dotnet run -c Release -- --micro --filter *JoinBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionLookupFallbackBenchmarks*

# Run the focused phase-1 baseline set
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Phase1-Baselines.ps1

# Stable macro snapshot (median-of-3, reproducible mode)
dotnet run -c Release -- --macro --repeat 3 --repro

# Durable write variance diagnostics
dotnet run -c Release -- --write-diagnostics --repeat 3 --repro

# Durable SQL batching diagnostics
dotnet run -c Release -- --durable-sql-batching --repeat 3 --repro

# Explicit WriteTransaction diagnostics
dotnet run -c Release -- --write-transaction-diagnostics --repeat 3 --repro
dotnet run -c Release -- --write-transaction-scenario UpdateDisjoint_W8_Rows1_Batch250us_Prealloc1MiB --repro

# Checkpoint-retention diagnostics
dotnet run -c Release -- --checkpoint-retention-diagnostics --repeat 3 --repro
dotnet run -c Release -- --checkpoint-retention-scenario W8_Blocker3s_Batch250us --repro

# Concurrent durable write diagnostics
dotnet run -c Release -- --concurrent-write-diagnostics --repeat 3 --repro
dotnet run -c Release -- --concurrent-write-scenario W8_Batch250us_Prealloc1MiB --repro

# Multi-writer stress closeout
dotnet run -c Release -- --stress --repeat 3 --repro

# Focused direct hybrid transport comparison (no gRPC)
dotnet run -c Release -- --direct-file-cache-transport --repeat 3 --repro

# Focused hot steady-state comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-storage-mode --repeat 3 --repro

# Focused master comparison refresh for only the CSharpDB rows used in the README table
dotnet run -c Release -- --master-table --repeat 3 --repro

# Focused master comparison refresh in buffered mode (less durable; analogous to SQLite WAL NORMAL)
$env:CSHARPDB_BENCH_DURABILITY='Buffered'
dotnet run -c Release -- --master-table --repeat 3 --repro
Remove-Item Env:CSHARPDB_BENCH_DURABILITY

# Standalone local SQLite apples-to-apples SQL comparison (WAL + FULL only)
dotnet run -c Release -- --sqlite-compare --repeat 3 --repro

# Strict ADO.NET insert apples-to-apples comparison for CSharpDB vs SQLite
dotnet run -c Release -- --strict-insert-compare --repeat 3 --repro

# Direct engine vs SQLite C API concurrent comparison on one durable file-backed database per run
dotnet run -c Release -- --concurrent-sqlite-capi-compare --repeat 3 --repro
dotnet run -c Release -- --concurrent-sqlite-capi-compare-scenario CSharpDB_InsertBatch_DisjointConcurrent_W8_Batch250us --repro

# Concurrent ADO.NET provider comparison on shared-memory, non-durable targets
dotnet run -c Release -- --concurrent-adonet-compare --repeat 3 --repro
dotnet run -c Release -- --concurrent-adonet-compare-scenario SQLite_AdoNet_Disjoint_W8 --repro

# Publish the NativeAOT shared library for the current RID before running the native mix
dotnet publish .\src\CSharpDB.Native\CSharpDB.Native.csproj -c Release -r win-x64

# Raw insert comparison including CSharpDB NativeAOT FFI
dotnet run -c Release -- --native-aot-insert-compare --repeat 3 --repro

# EF Core insert comparison for CSharpDB vs SQLite
dotnet run -c Release -- --efcore-compare --repeat 3 --repro

# EF Core insert comparison for CSharpDB vs SQLite with short-lived DbContexts over one shared open connection
dotnet run -c Release -- --efcore-compare-hybrid-shared-connection --repeat 3 --repro

# EF Core insert comparison for CSharpDB vs SQLite with EF-managed auto open/close per SaveChanges
dotnet run -c Release -- --efcore-compare-auto-open-close --repeat 3 --repro

# Focused engine-cold open + first read comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-cold-open --repeat 3 --repro

# Focused post-open hot-set read comparison including hybrid warm-set mode
dotnet run -c Release -- --hybrid-hot-set-read --repeat 3 --repro

# Focused post-checkpoint hot reread comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-post-checkpoint --repeat 3 --repro

# In-memory rotating batch throughput
dotnet run -c Release -- --macro-batch-memory

# Stress and scaling suites
dotnet run -c Release -- --stress
dotnet run -c Release -- --scaling

# Exhaustive full sweep (includes the original full micro suite set; very slow)
dotnet run -c Release -- --all
```

Results are written to `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/` and `BenchmarkDotNet.Artifacts/results/`.

The standalone SQLite comparison suite writes `sqlite-compare-*.csv` with only local SQLite `WAL + FULL` SQL rows. It does not feed the CSharpDB `--master-table` refresh path.

Unless `CSHARPDB_BENCH_DURABILITY=Buffered` is set, the macro and master-table harnesses run in durable mode. `Buffered` is the less-durable CSharpDB-only mode, analogous to SQLite WAL `synchronous=NORMAL`.

### Cross-Provider Comparison Guide

The benchmark suite now has several distinct comparison harnesses. They are not interchangeable. Each one isolates a different layer of the stack, and the README tables should be read in that context.

| Command | Surface being compared | Storage and durability | Writer shape | Primary use |
|---------|------------------------|------------------------|--------------|-------------|
| `--sqlite-compare` | Local SQLite only through `Microsoft.Data.Sqlite` | File-backed, `journal_mode=WAL`, `synchronous=FULL` | Single writer for insert rows, `8` readers for read rows | Durable local SQLite reference rows used alongside the CSharpDB single-writer write tables |
| `--strict-insert-compare` | CSharpDB ADO.NET vs SQLite ADO.NET | File-backed, CSharpDB `WriteOptimized` durable direct mode; SQLite `WAL + FULL` | Single writer | Apples-to-apples provider comparison for raw SQL vs prepared SQL and single-row vs `Batch100` |
| `--concurrent-sqlite-capi-compare` | CSharpDB engine API vs direct SQLite C API | File-backed, durable; CSharpDB default durable, SQLite `WAL + FULL` | `4` or `8` concurrent writer tasks | Closest direct engine-vs-SQLite write-contention comparison |
| `--concurrent-adonet-compare` | CSharpDB ADO.NET vs SQLite ADO.NET | Shared-memory, non-durable | `4` or `8` concurrent writer tasks | Provider-host overhead comparison without file durability costs |
| `--efcore-compare` | CSharpDB EF Core vs SQLite EF Core | File-backed, CSharpDB `WriteOptimized` durable direct mode; SQLite `WAL + FULL` | Single writer | Steady-state ORM-layer insert comparison with one open connection per timed run |
| `--efcore-compare-hybrid-shared-connection` | CSharpDB EF Core vs SQLite EF Core | File-backed, CSharpDB `WriteOptimized` durable direct mode; SQLite `WAL + FULL` | Single writer | Short-lived `DbContext` comparison with one externally-owned open connection reused for the timed run |
| `--efcore-compare-auto-open-close` | CSharpDB EF Core vs SQLite EF Core | File-backed, CSharpDB `WriteOptimized` durable direct mode; SQLite `WAL + FULL` | Single writer | EF-managed connection-lifecycle comparison where each `SaveChangesAsync` can auto-open/close |
| `--native-aot-insert-compare` | CSharpDB ADO.NET vs CSharpDB NativeAOT FFI vs SQLite ADO.NET | File-backed, durable | Single writer | Isolates managed-provider overhead versus direct native FFI on the CSharpDB side |

### Single Writer vs Multi Writer

- `Single writer` means one active write loop for the timed phase.
- `Multi writer` means the harness launches an array of `Task`s and releases them together through a start gate.
- In the CSharpDB engine benchmarks, multi-writer rows usually mean multiple concurrent callers sharing one `Database` instance.
- In the SQLite C API and ADO.NET benchmarks, multi-writer rows mean one connection per writer task. The connections target the same database identity for that benchmark run.
- Concurrent throughput rows report total successful commits across all writers combined, not per-writer throughput.
- Batch rows report rows/sec, not transactions/sec. Single-row auto-commit rows report commits/sec or ops/sec depending on the harness.

### SQLite Concurrency Interpretation

- The direct SQLite C API benchmark follows SQLite's documented multi-thread usage model: one connection and one prepared statement per writer task, with no sharing of a `sqlite3*` or derived statement object across threads.
- The C API benchmark opens each SQLite writer with `SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE`, applies `busy_timeout`, and runs file-backed `journal_mode=WAL` plus `synchronous=FULL`.
- That means the SQLite C API benchmark is measuring multiple independent writer connections contending for SQLite's WAL writer slot. It is not a "parallel writers all committing at once" benchmark because SQLite WAL still allows only one active writer at a time.
- The concurrent ADO.NET comparison is intentionally different. It uses shared-memory targets for both providers and configures SQLite with `journal_mode=MEMORY`, `cache=shared`, and `busy_timeout`. Those rows are useful for provider-layer concurrency overhead, but they are not durable WAL comparisons and they do not exercise the new CSharpDB embedded tuning surface.
- The EF Core comparison is currently single-writer only. It is a `SaveChangesAsync` insert benchmark, not a concurrent writer benchmark.
- The EF Core harness opens one connection per provider/context for the timed run so it measures steady-state `SaveChangesAsync` cost instead of repeated per-save connection open/close cost.
- The separate `--efcore-compare-hybrid-shared-connection` harness keeps one externally-owned open connection alive for the timed run while creating a fresh `DbContext` for each save. That isolates `DbContext` lifetime cost from physical connection open cost.
- The separate `--efcore-compare-auto-open-close` harness keeps the same single-writer durable workload but leaves connection lifetime under EF's default management so repeated auto-open/close cost stays in the measurement.

### CSharpDB Engine Semantics In These Comparisons

- The CSharpDB engine does not carry an explicit writer-count mode on the `Database` handle. The harness creates the single-writer or multi-writer shape.
- For implicit SQL insert execution, the key engine switch is `DatabaseOptions.ImplicitInsertExecutionMode`.
- `Serialized` keeps implicit inserts on the shared write-gate path and behaves like a single effective writer even if multiple callers arrive.
- `ConcurrentWriteTransactions` routes implicit inserts through isolated write transactions so multiple callers can overlap, conflict, and retry under the shared engine.
- The `concurrent-sqlite-capi-compare` harness uses `ConcurrentWriteTransactions` for the CSharpDB concurrent rows because that is the engine mode intended for disjoint-key insert fan-in.
- The `InsertBatch` rows inside that same harness are still one row per commit. The batch object is reused to avoid parse/object churn, but the benchmark is not converting the concurrent test into a multi-row transaction benchmark.

### What Each Comparison Proves

- `--sqlite-compare` answers "what does matched local SQLite WAL+FULL look like on this runner?" for single-writer durable SQL.
- `--strict-insert-compare` answers "how much of the gap is provider and SQL preparation overhead?" for file-backed ADO.NET.
- `--concurrent-sqlite-capi-compare` answers "how does shared-engine CSharpDB auto-commit compare to direct SQLite C API under actual durable writer contention?"
- `--concurrent-adonet-compare` answers "how much concurrency overhead comes from the provider layer itself when durability is removed?"
- `--efcore-compare` answers "what is the steady-state ORM-layer delta for `SaveChangesAsync` inserts on the two providers when the connection is already open?"
- `--efcore-compare-hybrid-shared-connection` answers "what is the ORM-layer delta when each unit of work gets a short-lived `DbContext`, but the underlying connection stays open?"
- `--efcore-compare-auto-open-close` answers "what is the ORM-layer delta when EF owns the connection lifetime and each save may pay open/close cost?"
- `--native-aot-insert-compare` answers "how much managed ADO.NET overhead remains versus a native CSharpDB call surface?"

### Validation Coverage

The comparison benchmarks are backed by smoke tests, but not every benchmark layer has a dedicated unit-test twin.

- `tests/CSharpDB.Data.Tests/ComparativeAdoNetSmokeTests.cs` validates prepared insert/query round-trip for both providers on file-backed databases and a `4`-writer disjoint-key shared-memory row-count check for both providers.
- `tests/CSharpDB.EntityFrameworkCore.Tests/ComparativeEfCoreSmokeTests.cs` validates `EnsureCreated`, CRUD round-trip, and batch insert counts for both EF Core providers on file-backed databases.
- There is currently no separate xUnit project dedicated to the raw SQLite C API benchmark harness. That surface is covered by the benchmark implementation itself plus the broader SQLite ADO.NET and EF Core smoke coverage.

Recommended validation commands:

```powershell
dotnet test .\tests\CSharpDB.Data.Tests\CSharpDB.Data.Tests.csproj -c Release --filter ComparativeAdoNetSmokeTests
dotnet test .\tests\CSharpDB.EntityFrameworkCore.Tests\CSharpDB.EntityFrameworkCore.Tests.csproj -c Release --filter ComparativeEfCoreSmokeTests
```

### Reading Results Correctly

- Do not compare `--concurrent-adonet-compare` directly against `--concurrent-sqlite-capi-compare` and treat the numbers as equivalent. One is shared-memory and non-durable; the other is file-backed and durable.
- Do not compare the shared multi-writer rows directly to the single-writer durable bulk tables and call that "insert throughput" without checking the unit. Shared multi-writer rows are usually total commits/sec across multiple tasks.
- For SQLite, interpret concurrent writer rows as contention among independent writer connections inside SQLite's one-writer WAL model.
- For CSharpDB, interpret concurrent writer rows as contention and possible fan-in through one shared engine instance, subject to key shape and the selected implicit insert execution mode.

### Concurrent Durable Write Methodology

- `ConcurrentDurableWriteBenchmark` is an in-process contention benchmark, not a multi-process transport benchmark.
- Each scenario launches `4` or `8` parallel writer `Task`s against one shared `Database` instance and one shared WAL/pager.
- All writers are released together through a start gate, then each task loops on auto-commit `INSERT` statements for the timed phase.
- `Ops/sec` in the concurrent durability CSVs is total successful commits per second across all writers combined, not per-writer throughput.
- The high shared-engine throughput rows therefore belong to the `8`-writer scenarios in `concurrent-write-diagnostics-*.csv`, while the much lower single-writer rows belong to the separate `write-diagnostics-*.csv` and `durable-sql-batching-*.csv` harnesses.

### Explicit WriteTransaction Methodology

- `WriteTransactionDiagnosticsBenchmark` exercises the phase-1 `Database.RunWriteTransactionAsync(...)` API directly rather than the legacy auto-commit or `BeginTransactionAsync/CommitAsync` paths.
- Each logical transaction precomputes its row ids before entering the retry loop so retries re-run the same transaction body instead of inventing new keys.
- The suite reports committed transactions as `Ops/sec`, puts `rowsPerSec`, retry counts, and exhausted conflict counts into `ExtraInfo`, and now includes two workload shapes:
- `HotInsert`: concurrent explicit inserts into one table, which intentionally exposes leaf-page conflict pressure and retry behavior.
- `DisjointUpdate`: concurrent explicit updates against far-apart preseeded rows, which minimizes page conflicts so commit-window coalescing and WAL batching can be observed directly.
- The tuned rows keep the same batch-window and WAL-preallocation knobs as the existing concurrent auto-commit suite so phase-1 transaction behavior can be compared against the older shared-engine write benchmark.

### Explicit WriteTransaction Snapshot (April 10, 2026 median-of-3)

The current explicit multi-writer snapshot uses `write-transaction-diagnostics-20260410-134444-median-of-3.csv`.

| Scenario | Durable Tx/sec | Rows/sec | P99 | Notes |
|----------|----------------|----------|-----|-------|
| `W1 Rows1 Batch0` | `275.4` | `275.4` | `5.54 ms` | Single-writer durable baseline on the phase-1 API |
| `W8 Rows1 Batch0` | `278.7` | `278.7` | `53.64 ms` | Hot unseeded right-edge inserts, `0.19` retries/commit |
| `W8 Rows1 Batch0 Seed16K` | `341.8` | `341.8` | `49.34 ms` | Same workload after pre-seeding the tree to steady state |
| `W8 Rows10 Batch0 Seed16K` | `377.7` | `3778.0` | `72.80 ms` | Best hot steady-state insert row in the closeout artifact |
| `W8 Rows10 Batch250us Prealloc1MiB` | `342.9` | `3458.6` | `74.66 ms` | Tuned hot-insert row after the late Phase 2 / Phase 3 storage work |
| `UpdateDisjoint W4 Rows1 Batch250us` | `449.8` | `449.8` | `21.78 ms` | Commit coalescing becomes visible once page conflicts are low |
| `UpdateDisjoint W8 Rows1 Batch250us Prealloc1MiB` | `590.4` | `590.4` | `30.65 ms` | Best disjoint update row, `3.37` commits/flush |

Targeted rerun: `write-transaction-scenario-UpdateDisjoint_W8_Rows1_Batch250us_Prealloc1MiB-20260411-052431-median-of-3.csv`

| Scenario | Durable Tx/sec | Rows/sec | P99 | Notes |
|----------|----------------|----------|-----|-------|
| `UpdateDisjoint W8 Rows1 Batch250us Prealloc1MiB` | `727.0` | `727.0` | `27.52 ms` | WAL pending-commit writes coalesced into one combined write per contiguous flush batch; median rerun also dropped `avgPendingCommitWriteMs` to about `0.09-0.10 ms` while `avgDurableFlushMs` stayed around `2.00 ms`, so the remaining ceiling is still the durable flush itself rather than pager-side serialization |

### Multi-Writer Stress Snapshot (April 10, 2026 median-of-3)

The current stress closeout uses `stress-20260410-140205-median-of-3.csv`.

| Scenario | Ops/sec | P99 | Notes |
|----------|---------|-----|-------|
| `CrashRecovery_50cycles` | `58.6` | `22.77 ms` | Closeout rerun after the retained-WAL compaction fix |
| `LogicalConflictRange_Overlap_ReadTx_5s` | `96.5` | `16.88 ms` | `63.1%` read conflict rate under overlapping ranges |
| `LogicalConflictRange_Overlap_WriteTx_5s` | `154.3` | `34.28 ms` | Writers stay mostly successful; reader side absorbs the conflict load |
| `LogicalConflictRange_Disjoint_ReadTx_5s` | `261.5` | `18.89 ms` | `0.0%` read conflict rate for disjoint ranges |
| `LogicalConflictRange_Disjoint_WriteTx_5s` | `156.0` | `34.14 ms` | `1.5%` writer conflict rate while disjoint readers stay clean |

### New In-Memory Suites

- `InMemorySqlBenchmarks`: file-backed vs in-memory SQL point lookups and inserts
- `InMemoryCollectionBenchmarks`: file-backed vs in-memory collection `GetAsync` and `PutAsync`
- `InMemoryAdoNetBenchmarks`: ADO.NET private `:memory:` vs named shared `:memory:name`
- `InMemoryPersistenceBenchmarks`: `LoadIntoMemoryAsync` and `SaveToFileAsync`
- `ColdLookupBenchmarks`: file-backed vs in-memory point lookups under cache pressure
- `MemoryMappedReadBenchmarks`: file-backed SQL and collection cold lookups with `UseMemoryMappedReads` toggled on and off
- `WalReadCacheBenchmarks`: file-backed SQL cold lookups where the latest table pages stay in WAL and `MaxCachedWalReadPages` is toggled
- `BTreeCursorBenchmarks`: raw storage-layer sequential B+tree cursor scans and seek+window traversals without SQL executor overhead
- `CollectionIndexBenchmarks`: focused collection secondary-index lookup and indexed write-maintenance costs
- `StorageTuningBenchmarks`: cache-size, index-provider, and reader-session matrix for file-backed indexed lookups
- `DurableWriteDiagnosticsBenchmark`: file-backed single-row write diagnostics across frame-count and WAL-size checkpoint policies, including background sliced auto-checkpoint scheduling
- `DurableSqlBatchingBenchmark`: file-backed durable SQL ingest sweep covering auto-commit single-row inserts, analyzed-table single-row inserts, and explicit transaction batches of `10`, `100`, and `1000` rows with commit-path stage diagnostics
- `WriteTransactionDiagnosticsBenchmark`: file-backed phase-1 explicit `WriteTransaction` sweep covering both hot-conflict inserts and disjoint-page updates so retry pressure and commit coalescing can be measured separately on the new transaction API
- `CommitFanInDiagnosticsBenchmark`: focused phase-4 comparison between shared auto-commit and explicit `WriteTransaction` disjoint-update workloads so `commitsPerFlush`, queue depth, and tail latency can be compared directly on one shared `Database`
- `InsertFanInDiagnosticsBenchmark`: focused phase-4 insert comparison between shared auto-commit and explicit `WriteTransaction`, split by explicit-id versus auto-generated-id inserts, so remaining insert-path structural limits can be measured directly
- `ConcurrentDurableWriteBenchmark`: shared-database multi-writer auto-commit sweep where `4` or `8` in-process writer tasks hit one shared `Database` instance for durable batch-window tuning under actual writer contention
- `InMemoryBatchBenchmark`: rotating x100 batch throughput for in-memory SQL and collections
- `InMemoryWorkloadBenchmark`: macro mixed workloads for SQL and collections in memory vs file-backed
- `SharedMemoryAdoNetBenchmark`: named shared-memory reader/writer contention through the provider host layer
- `InMemoryPersistenceBenchmark`: macro load/save latency and output-size reporting
- `DirectFileCacheTransportBenchmark`: macro-style direct-client comparison on the same larger file-backed dataset so direct pager/file-cache tuning can be measured without gRPC transport overhead
- `HybridStorageModeBenchmark`: macro-style hot steady-state comparison between file-backed, pure in-memory, and the lazy-resident hybrid mode with incremental durable commits
- `HybridColdOpenBenchmark`: engine-cold open + first SQL lookup / collection get comparison across file-backed, pure in-memory load, and lazy-resident incremental-durable hybrid on a larger seeded database
- `HybridHotSetReadBenchmark`: post-open hot-burst SQL lookups and collection gets that isolate the hybrid warm-set hint from generic lazy hybrid behavior while excluding open cost from the throughput denominator
- `HybridPostCheckpointBenchmark`: identical post-checkpoint hot reread comparison across file-backed, pure in-memory, and lazy-resident hybrid after one auto-checkpointed write per burst

### Phase 1 Baseline Suites

- `SqlMaterializationBenchmarks`: isolates full-row decode, selected-column decode, single-column access, and payload-level text/numeric checks
- `CollectionAccessBenchmarks`: isolates full document hydration, key-only access, key matching, and indexed-field reads over collection payloads
- `MemoryMappedReadBenchmarks`: isolates file-backed SQL and collection cold lookups so the opt-in `mmap` read path can be compared directly against copy-based reads
- `WalReadCacheBenchmarks`: isolates file-backed SQL cold lookups where the latest table pages remain WAL-backed so the dedicated WAL read cache can be compared directly against fresh WAL stream reads
- `BTreeCursorBenchmarks`: isolates raw forward cursor traversal and seek+window scans over a B+tree so sequential leaf-scan changes can be measured without the SQL executor on top
- `CoveringIndexBenchmarks`: isolates unique-index lookup shapes that could become index-only from shapes that still need the wide base-row payload
- `IndexProjectionBenchmarks`: isolates non-unique secondary-index lookups where `SELECT id` or `SELECT indexed_col` can now avoid base-row fetches
- `OrderByIndexBenchmarks`: isolates indexed `ORDER BY`, covered integer range scans, and compact non-covered range projection shapes, including residual-filter batch-plan variants where indexed filtering still avoids full row materialization
- `CollationIndexBenchmarks`: isolates ordered SQL text-index equality lookup, range scan, top-N `ORDER BY`, and indexed write-maintenance cost under `BINARY`, `NOCASE`, `NOCASE_AI`, and `ICU:<locale>`
- `ScanProjectionBenchmarks`: isolates compact table scans and LIMIT-forced generic scans where scan-heavy filter/projection shapes now stay on the internal row-batch transport path
- `IndexAggregateBenchmarks`: isolates scalar `SUM` / `COUNT` / `MIN` / `MAX` queries and range aggregates that can now execute directly from integer index keys
- `PrimaryKeyAggregateBenchmarks`: isolates scalar and ranged aggregates that can now execute directly from the `INTEGER PRIMARY KEY` table B-tree key stream
- `GroupedIndexAggregateBenchmarks`: isolates `GROUP BY` on a duplicate-heavy integer key so grouped aggregates can be compared against the new direct index-grouped fast path
- `CompositeGroupedIndexBenchmarks`: isolates `GROUP BY` on composite indexed keys and leftmost-prefix grouped scans so composite grouped fast paths can be compared against generic grouped scans
- `CollectionFieldExtractionBenchmarks`: isolates early/middle/late extraction cost, nested-path access, miss cost, and full document hydration comparison for collection payload scans
- `CollectionLookupFallbackBenchmarks`: isolates collection equality lookups on unindexed fields to measure the direct-payload compare fallback before full document hydration
- `Run-Phase1-Baselines.ps1`: runs the focused phase-1 micro set plus the repeat-3 explicit `WriteTransaction` diagnostics without the larger macro, stress, or scaling suites
- `CollectionFieldExtractionBenchmarks`, `CollectionPayloadBenchmarks`, and `CollectionAccessBenchmarks` use an in-process BenchmarkDotNet toolchain on this machine because that was the smallest stable fix for local child-job generation/runtime issues

### Baselines and Guardrails

```bash
# Capture a fresh baseline snapshot
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1

# Run the fast PR guardrails
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 -Mode pr

# Run the fuller release guardrails
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 -Mode release
```

Defaults:

- Baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`
- PR guardrail baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`
- Threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- PR threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds-pr.json`
- Last guardrail report: `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md`
- `Capture-Baseline.ps1` runs non-micro suites in reproducible mode by default and captures macro results as `--macro --repeat 3 --repro`.
- The focused release guardrail set now stages stable durability CSVs from `--write-diagnostics --repeat 3 --repro`, `--durable-sql-batching --repeat 3 --repro`, and `--write-transaction-diagnostics --repeat 3 --repro`, plus the focused phase-4 fan-in captures from `--commit-fan-in-diagnostics --repro` and `--insert-fan-in-diagnostics --repro`, into `macro-stress-scaling/write-diagnostics-median-of-3.csv`, `macro-stress-scaling/durable-sql-batching-median-of-3.csv`, `macro-stress-scaling/write-transaction-diagnostics-median-of-3.csv`, `macro-stress-scaling/commit-fan-in-diagnostics.csv`, and `macro-stress-scaling/insert-fan-in-diagnostics.csv`.
- `--pr` reads `perf-thresholds-pr.json` and runs only the dedicated PR guardrail classes. It skips the repeat-3 durability suites and the specialized read suites that stay release-only.
- `--release` reads `perf-thresholds.json` and runs the existing tracked micro filters plus the tracked non-micro guardrail suites, sequentially.
- `--micro` and `--all` keep the original full micro benchmark surface; they do not automatically add the PR guardrail duplicates unless you explicitly filter for `*GuardrailBenchmarks*`.
- The focused validation baseline snapshot under `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` is checked in and now carries the tracked micro guardrail CSVs plus the staged durable median-of-3 CSVs, so fresh clones can run the current guardrail set without first rebuilding older focused baseline snapshots.
- That checked-in focused snapshot also now carries the April 11, 2026 `commit-fan-in-diagnostics` and `insert-fan-in-diagnostics` captures used by the release guardrails, so the checked-in baseline matches the current phase-4 release surface.
- The checked-in focused snapshot under `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` now carries the dedicated PR guardrail CSVs as well, so both release and PR validation use the same baseline snapshot.
- That focused validation snapshot also tracks `CSharpDB.Benchmarks.Micro.CollationIndexBenchmarks-report.csv` so ordered-text collation regressions can be checked alongside the existing micro suites.
- Baseline snapshots now include a `machine.json` fingerprint sidecar. `Run-Perf-Guardrails.ps1` stays strict on a matching perf runner or same-machine fingerprint, downgrades regressions to warnings on compatible hardware/runtime, and skips regression enforcement on materially different machines.
- Set `CSHARPDB_PERF_RUNNER_ID` on the canonical perf runner before capturing a baseline if you want strict regression failures to be limited to that designated machine.
- The focused durability checks compare `Mean` against the stable median-of-3 durable rows only. Allocation comparison is intentionally skipped for those checks because the diagnostics CSVs emit raw millisecond values, not BenchmarkDotNet `Allocated` columns.

## Current Performance Snapshot

The API snapshot tables below use the April 7, 2026 broad durable sweep from `macro-20260407-070125.csv`. The hot steady-state storage-mode tables use the April 13, 2026 focused `hybrid-storage-mode-20260413-124730-median-of-3.csv` rerun. The durable master comparison table uses the April 13, 2026 `master-table-20260413-125827-median-of-3.csv` snapshot, while the buffered master comparison table still uses the April 7 buffered rerun. Reused-session burst concurrent-read rows in the April 13 artifacts retain latency samples at `1/128` so throughput is not throttled by benchmark-side sample retention. CSharpDB values are durable-only unless a note says otherwise.

### SQL API (April 7, 2026 broad sweep)

| Metric | Value | Notes |
|--------|-------|-------|
| Single INSERT | 291.0 ops/sec | Auto-commit write |
| Batch 100 rows/tx | ~26.81K rows/sec | 268.1 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.76M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 1,126.2 ops/sec | 80/20 read/write mix |
| Mixed workload writes | 288.9 ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers, per-query sessions) | 690.88K ops/sec | Total `COUNT(*)` queries/sec across 8 readers |
| Reader throughput (8 readers, reused snapshots x32) | 11.43M ops/sec | `ReaderScalingBurst32_8readers_Readers` |
| Writer throughput under 8 readers | 291.6 ops/sec | `ReaderScaling_8readers_Writer` |
| Checkpoint time (1,000 WAL frames) | 7.59 ms | `CheckpointLoad_1000frames_CheckpointTime` |

### Collection API (April 7, 2026 broad sweep)

| Metric | Value | Notes |
|--------|-------|-------|
| Single Put | 285.8 ops/sec | Auto-commit document write |
| Batch 100 docs/tx | ~25.69K docs/sec | 256.9 tx/sec x 100 docs |
| Point Get (10K docs) | 2.36M ops/sec | Direct collection lookup |
| Mixed workload reads | 1,143.1 ops/sec | 80/20 read/write mix |
| Mixed workload writes | 293.6 ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 5,067.0 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 4,954.4 scans/sec | Predicate evaluation path |
| Indexed equality lookup (10K docs) | 846.31K ops/sec | `Collection_FindByIndex_Value_10k_15s` |
| Single Put (with 1 secondary index) | 289.6 ops/sec | `Collection_Put_Single_WithIndex_15s` |

### Collection Path Micro Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection encode (direct payload) | 170.6 ns | 552 B | Current versioned binary direct-payload path |
| Collection encode (legacy row format) | 256.4 ns | 304 B | Prior `DbValue[]` + record serializer path |
| Collection decode (direct payload) | 150.3 ns | 328 B | Current binary direct-payload path with direct header reuse |
| Collection decode (legacy row format) | 392.4 ns | 600 B | Prior `DbValue[]` + record serializer path |
| Collection put (minimal schema, in-memory) | 2.825 us | 2.00 KB | Auto-commit write with only the target collection loaded |
| Collection put (48 extra tables + 48 extra collections, in-memory) | 2.897 us | 1.95 KB | Unrelated schema breadth still does not add measurable write tax |

### WAL Core Micro Spot Checks (March 26, 2026)

Source: `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.WalCoreBenchmarks-report.csv`

| Metric | 100 frames before checkpoint | 500 frames before checkpoint | 1000 frames before checkpoint | Notes |
|--------|-----------------------------:|-----------------------------:|------------------------------:|-------|
| WAL core: 100-frame batch commit | 10.803 ms | 10.301 ms | 10.613 ms | Direct `AppendFramesAndCommitAsync(...)` path |
| WAL core: 100-frame staged commit | 10.584 ms | 10.717 ms | 10.477 ms | Repeated `AppendFrameAsync(...)` calls staged and emitted at `CommitAsync(...)` |
| WAL core: single-frame commit | 4.202 ms | 4.297 ms | 4.450 ms | One page + one durable commit |
| WAL core: manual checkpoint after N frames | 20.449 ms | 44.536 ms | 85.726 ms | Commit N single-page appends, then checkpoint |

The important result for async I/O batching is still the staged-vs-direct comparison: the repeated-`AppendFrameAsync(...)` path stays close to the direct batched WAL path instead of collapsing into one durable write per page before commit, while manual checkpoint cost rises sharply as the frame threshold grows.

### Collection Path Index Spot Checks (April 7, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` nested path equality (`$.address.city`) | 995.823 us | 820.75 KB | Public string-path index lookup over a nested scalar path with many matches |
| Collection `FindByPath` nested path equality (`$.address.city`) | 979.174 us | 819.81 KB | Query-facing path API running on the same nested scalar index |
| Collection `FindByIndex` array path equality (`$.tags[]`) | 1,154.611 us | 1254.28 KB | Multi-value array element lookup over a public string-path collection index |
| Collection `FindByPath` array path equality (`$.tags[]`) | 1,178.161 us | 1254.22 KB | Query-facing path API running on the same array index |
| Collection `FindByPath` nested array path equality (`$.orders[].sku`) | 1,813.161 us | 1742.21 KB | Query-facing path API over an index on scalar fields inside array elements |
| Collection `FindByPath` integer range (`Value`, 1024 matches) | 487.011 us | 307.27 KB | Ordered integer path range over the collection index path/query surface |
| Collection `FindByPath` text range (`Tag`, 1000 matches) | 524.149 us | 499.56 KB | Ordered text path range over prefix-bucket text indexes with exact in-bucket filtering |
| Collection `FindByPath` Guid equality (`SessionId`) | 3.566 us | 15.54 KB | Canonical `Guid` path lookup over the ordered text collection index path/query surface |
| Collection `FindByPath` DateOnly range (`EventDate`, 1000 matches) | 954.280 us | 837.33 KB | Canonical `DateOnly` range over ordered text collection indexes using fixed-width ISO keys |

### Collection Extraction Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection field read (early field) | 65.40 ns | 112 B | Direct binary payload scan near the front of the document |
| Collection field read (middle field) | 76.72 ns | 64 B | Unbound middle-field integer read |
| Collection field read (late field) | 121.01 ns | 64 B | Unbound late-field integer read |
| Collection field compare (late text field, bound accessor) | 127.38 ns | 0 B | Bound accessor text compare stays allocation-free |
| Collection field read (middle field, bound accessor) | 56.72 ns | 0 B | Bound accessor integer extraction on the binary payload path |
| Collection field read (nested path, bound accessor) | 214.10 ns | 40 B | Nested document walk without hydrating `T` |
| Collection hydrate document (comparison) | 517.23 ns | 912 B | Full typed hydration on the new binary direct-payload path |

### Query Micro Spot Checks

| Metric | Mean | Allocated |
|--------|------|-----------|
| SQL PK lookup (10K rows) | 548.0 ns | 768 B |
| SQL PK lookup (100K rows) | 802.9 ns | 768 B |
| SQL indexed lookup (100K rows) | 2,450.7 ns | 17185 B |
| SQL point miss (100K rows) | 366.3 ns | 464 B |

### Reader Session Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(*)` with per-query reader sessions | 1,035.5 ns | 7528 B | Full reader-session create/execute/dispose path |
| `COUNT(*)` with reused reader session | 105.7 ns | 321 B | Same query with a reused snapshot session |
| Point lookup with per-query reader sessions | 1,703.7 ns | 7799 B | Reader-session setup is materially higher than direct execute on the current path |
| Point lookup with reused reader session | 680.9 ns | 592 B | Small remaining gap versus direct execution |
| Point lookup with direct `ExecuteAsync` | 663.6 ns | 640 B | Lower bound for the same simple PK read path |

### Memory-Mapped Read Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, copy-based read path | 28.704 us | 9707 B | File-backed cache-pressured lookup with `UseMemoryMappedReads = false` |
| SQL cold lookup, mmap read path | 1.608 us | 784 B | Same workload with clean main-file pages served from mapped read views |
| Collection cold get, copy-based read path | 29.286 us | 9373 B | File-backed cache-pressured collection lookup with `UseMemoryMappedReads = false` |
| Collection cold get, mmap read path | 1.252 us | 409 B | Same workload with mapped main-file reads and copy-on-write only on mutable access |

### WAL Read Cache Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, WAL-backed, no WAL cache | 28.74 us | 8.68 KB | File-backed cache-pressured lookup where the latest table pages are still read from WAL frames |
| SQL cold lookup, WAL-backed, 128-page WAL cache | 19.83 us | 6.27 KB | Same workload with `MaxCachedWalReadPages = 128` so immutable WAL frame images can be reused between reads |

### B-Tree Cursor Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| B-tree cursor full scan (10K rows, read-ahead off) | 9.25 ms | 3.34 MB | File-backed raw forward scan with `EnableSequentialLeafReadAhead = false` |
| B-tree cursor full scan (10K rows, read-ahead on) | 8.51 ms | 3.26 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead off) | 919.2 us | 359.98 KB | Mid-tree seek followed by sequential leaf traversal |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead on) | 822.2 us | 346.56 KB | Same seek-window path with speculative next-leaf reads |
| B-tree cursor full scan (100K rows, read-ahead off) | 92.74 ms | 33.46 MB | File-backed forward scan across a deeper leaf chain |
| B-tree cursor full scan (100K rows, read-ahead on) | 87.41 ms | 32.69 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead off) | 945.7 us | 359.98 KB | Mid-tree seek followed by a bounded sequential window |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead on) | 824.7 us | 346.54 KB | Seek-window path with speculative next-leaf reads; latency stays roughly flat as the tree grows |

### SQL Covered Read-Path Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Unique index lookup `SELECT *` (100K rows) | 5.963 us | 19.47 KB | Baseline unique secondary-index lookup |
| Unique index lookup `SELECT id` (100K rows) | 2.374 us | 9.92 KB | Covered projection from index payload |
| Unique index lookup `SELECT lookup_key` (100K rows) | 2.870 us | 9.88 KB | Covered projection from index payload |
| Non-unique index lookup `SELECT *` (100K rows) | 148.913 us | 83.00 KB | Baseline duplicate-key secondary-index lookup |
| Non-unique index lookup `SELECT id` (100K rows) | 19.163 us | 34.61 KB | Covered projection drops most row materialization cost |
| `ORDER BY value` no index (100K rows) | 160.914 ms | 63.53 MB | Full sort baseline from the latest indexed-order rerun |
| `ORDER BY value` covered index-order scan (100K rows) | 33.637 ms | 14.58 MB | `SELECT id, value` stays on index data |
| `ORDER BY value LIMIT 100` index-order scan (100K rows) | 63.45 us | 83.22 KB | Index order avoids sort, still fetches base rows |
| `ORDER BY value LIMIT 100` covered index-order scan (100K rows) | 21.78 us | 40.75 KB | Index-only top-N path |
| `WHERE value BETWEEN ...` row fetch (100K rows) | 52.728 ms | 16.45 MB | Integer range scan with base-row fetch |
| `WHERE value BETWEEN ...` covered projection (100K rows) | 16.619 ms | 7.30 MB | Integer range scan that stays on index data |
| `WHERE value BETWEEN ... SELECT id, category` compact projection (100K rows) | 38.913 ms | 7.30 MB | Non-covered indexed range scan decodes only projected payload columns instead of wide rows |
| `WHERE value BETWEEN ... SELECT id, value + id` compact expression projection (100K rows) | 39.049 ms | 7.30 MB | Indexed range scan keeps the compact payload decode path even when projection includes an expression |

### SQL Composite Equality Lookup Spot Checks (April 6, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE a = ... AND b = ...` no index (100K rows) | 9.624 ms | 800.74 KB | Full scan over wide rows |
| `WHERE a = ... AND b = ...` single-column index (100K rows) | 21.650 us | 20.59 KB | Uses `a` only, then filters `b` after row fetch |
| `WHERE a = ... AND b = ... SELECT *` composite index (100K rows) | 3.440 us | 11.46 KB | Direct composite equality lookup over the hashed secondary index |
| `WHERE a = ... AND b = ... SELECT id, a, b` composite covered projection (100K rows) | 2.386 us | 3.04 KB | Cache-hot covered projection short-circuits directly from cached composite index payload without base-row materialization |
| `WHERE a = ... AND b = ... SELECT id, a, b` unique composite covered projection (100K rows) | 2.392 us | 3.04 KB | Same cache-hot covered path on a unique composite index |

### SQL Indexed Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SUM(value)` no index (100K rows) | 56.782 ms | 20.79 MB | Full table aggregate over decoded rows |
| `SUM(value)` direct index aggregate (100K rows) | 14.491 ms | 4.46 MB | Walks integer index keys without base-row fetch |
| `COUNT(value)` no index (100K rows) | 56.479 ms | 20.79 MB | Full table aggregate |
| `COUNT(value)` direct index aggregate (100K rows) | 14.519 ms | 4.47 MB | Counts row-id payloads per integer key |
| `MIN(value)` no index (100K rows) | 57.152 ms | 20.78 MB | Full scan baseline |
| `MIN(value)` direct index aggregate (100K rows) | 7.608 us | 5674 B | First-key fast path on ordered integer index |
| `MAX(value)` no index (100K rows) | 57.626 ms | 20.78 MB | Full scan baseline |
| `MAX(value)` direct index aggregate (100K rows) | 519.6 ns | 824 B | Rightmost-key fast path on ordered integer index |
| `COUNT(*) WHERE value BETWEEN ...` no index (100K rows) | 56.392 ms | 20.79 MB | Scan + predicate baseline |
| `COUNT(*) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 7.304 ms | 2.20 MB | Range aggregate from integer index keys |
| `SUM(value) WHERE value BETWEEN ...` no index (100K rows) | 57.880 ms | 20.79 MB | Scan + predicate baseline |
| `SUM(value) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 7.284 ms | 2.20 MB | Range aggregate stays on index data |

### SQL Primary-Key Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `MIN(id)` via table key aggregate (100K rows) | 7.052 us | 5526 B | First-row fast path on the table B-tree key |
| `MAX(id)` via table key aggregate (100K rows) | 409.1 ns | 728 B | Rightmost-key fast path on the table B-tree key |
| `COUNT(id)` via table key aggregate (100K rows) | 278.5 ns | 520 B | Reuses cached table row count; same semantics as `COUNT(*)` on integer PK |
| `SUM(id)` via table key aggregate (100K rows) | 55.462 ms | 20.79 MB | Sums row keys without row payload decode |
| `COUNT(*) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 27.535 ms | 10.67 MB | Range aggregate stays on the table key stream |
| `SUM(id) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 27.166 ms | 10.67 MB | PK range aggregate without row fetch |

### SQL DISTINCT Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(DISTINCT value)` no index (100K rows) | 29.054 ms | 10.95 MB | Duplicate-heavy integer column with 1,024 distinct keys |
| `COUNT(DISTINCT value)` direct index aggregate (100K rows) | 81.74 us | 904 B | Counts unique integer index keys without row decode |
| `SUM(DISTINCT value)` no index (100K rows) | 29.739 ms | 10.94 MB | Full table distinct-set baseline |
| `SUM(DISTINCT value)` direct index aggregate (100K rows) | 83.80 us | 904 B | Sums unique integer index keys directly |
| `AVG(DISTINCT value)` no index (100K rows) | 29.635 ms | 10.95 MB | Full table distinct-set baseline |
| `AVG(DISTINCT value)` direct index aggregate (100K rows) | 83.26 us | 904 B | Computes distinct sum/count from index keys only |

### SQL Grouped Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY group_id SELECT group_id, COUNT(*)` no index (100K rows) | 34.742 ms | 11.51 MB | Generic grouped hash aggregate over a duplicate-heavy integer key |
| `GROUP BY group_id SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 153.156 us | 103.10 KB | Streams distinct integer index keys and row-id payload counts without row decode |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` no index (100K rows) | 37.018 ms | 11.93 MB | Generic grouped hash aggregate with multiple scalar states per group |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` direct index aggregate (100K rows) | 170.621 us | 165.96 KB | Same grouped result computed directly from ordered index keys |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` no index (100K rows) | 35.477 ms | 11.24 MB | Generic grouped aggregate still scans and groups the filtered input |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 87.671 us | 52.30 KB | Range-restricted grouped aggregate stays on the ordered integer index |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` no index (100K rows) | 35.231 ms | 11.52 MB | Generic grouped aggregate still materializes, sorts, and then trims |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 17.615 us | 18.55 KB | Natural key order from the index lets the grouped path stop after the first 100 groups |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` no index (100K rows) | 32.516 ms | 10.76 MB | Equality filter still scans the table, groups one key, and applies HAVING in the generic path |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 1.155 us | 1.53 KB | Equality-restricted grouped fast path now applies `HAVING COUNT(*)` directly from the index payload count |

### SQL Composite Grouped Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY a, b SELECT a, b, COUNT(*)` no index (100K rows) | 44.156 ms | 22.57 MB | Generic grouped aggregate over the full composite key builds hash state from decoded rows |
| `GROUP BY a, b SELECT a, b, COUNT(*)` composite index aggregate (100K rows) | 3.571 ms | 3.66 MB | Streams hashed composite index payloads and aggregates directly from grouped key buckets |
| `GROUP BY a SELECT a, COUNT(*)` no index (100K rows) | 35.172 ms | 11.41 MB | Generic grouped aggregate over the leftmost composite key prefix |
| `GROUP BY a SELECT a, COUNT(*)` composite index prefix aggregate (100K rows) | 927.8 us | 2.09 MB | Leftmost-prefix grouping stays on the `(a, b)` index and avoids base-row decode |

### SQL Predicate Pushdown Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE value < 200000` (100K rows) | 67.312 ms | 26.89 MB | Single simple pre-decode predicate with about 20% selectivity |
| `WHERE value >= 10000 AND value < 20000` (100K rows) | 54.542 ms | 21.84 MB | Compound same-column range now pushes both bounds into pre-decode filtering |
| `WHERE category = 'Alpha' AND value < 200000` (100K rows) | 56.748 ms | 22.61 MB | Compound mixed text + integer predicate now pushes both conjuncts before row decode |

### SQL Scan Projection Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Compact scan batch plan: residual column projection (10K rows) | 1.172 ms | 780.69 KB | Compact scan path keeps residual filtering and column projection on the internal row-batch transport |
| Compact scan batch plan: expression projection (10K rows, 20% selectivity) | 856.8 us | 266.12 KB | Compact scan path keeps expression projection batch-backed instead of dropping to row transport immediately |
| Compact scan batch plan: residual column projection (100K rows) | 71.956 ms | 28.64 MB | Large filtered compact scan stays on the internal row-batch path end to end |
| Generic scan batch plan: expression projection + LIMIT (100K rows, 20% selectivity) | 56.532 ms | 23.41 MB | Generic scan path still reaches the projection boundary without losing the batch transport |

### SQL Batched Scan Root Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SELECT *` full scan (100K rows) | 123.052 ms | 48.32 MB | Plain `TableScanOperator` now feeds `QueryResult` through row batches instead of only row-by-row materialization |
| `SELECT * WHERE value < 200000` (100K rows) | 68.517 ms | 26.89 MB | Plain filtered scan stays batch-backed through `FilterOperator` on the simple scan path |
| `SELECT * WHERE value < 10000` (100K rows) | 54.625 ms | 21.83 MB | Same batched scan root with a low-selectivity predicate |
| `SELECT * LIMIT 100` (100K rows) | 110.620 us | 98.49 KB | `LimitOperator` preserves the scan batch root instead of forcing row-by-row materialization at the result boundary |

### SQL Batched Sort / Distinct Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SELECT DISTINCT value` (10K rows) | 2.004 ms | 2.21 MB | `DistinctOperator` ingests batch sources directly and can act as a batch-backed root rather than pulling one row at a time through the result boundary |
| `SELECT DISTINCT value ORDER BY value LIMIT 100` (10K rows) | 2.660 ms | 974.79 KB | `Distinct` feeding `Sort` stays batch-aware on both operators before final row materialization |
| `SELECT * ORDER BY value` (100K rows) | 146.535 ms | 63.56 MB | `SortOperator` materializes input from batch sources in `OpenAsync` and can emit sorted output in row batches |
| `SELECT * ORDER BY value + id` (100K rows) | 161.375 ms | 63.56 MB | Same full-sort path with an expression key; batch-aware input still helps when sort keys are computed |
| `SELECT * ORDER BY value LIMIT 100` (100K rows) | 52.767 ms | 20.89 MB | Top-N sort root stays batch-backed at the output boundary too |
| `SELECT * ORDER BY value + id LIMIT 100` (100K rows) | 52.864 ms | 20.93 MB | Expression top-N path over the batch-aware sort root |

### SQL Batched Aggregate Consumer Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `Scalar SUM(value)` (100K rows) | 53.467 ms | 20.79 MB | `ScalarAggregateOperator` ingests batch sources directly, removing row-by-row executor calls on the generic scan-fed aggregate path |
| `Scalar COUNT(value)` (100K rows) | 53.435 ms | 20.79 MB | Same generic scalar aggregate consumer path with batch-fed row accumulation |
| `Scalar MIN(value)` (100K rows) | 53.660 ms | 20.78 MB | Generic aggregate consumer stays on batched scan input even when the aggregate itself is non-additive |
| `Hash SUM(value) via GROUP BY 1` (100K rows) | 53.400 ms | 20.79 MB | `HashAggregateOperator` reads batches directly before materializing grouped output rows |
| `GROUP BY with COUNT + AVG` (100K rows) | 57.704 ms | 20.98 MB | Generic grouped aggregate path over the batch-fed scan root; specialized index-grouped paths remain much faster when they apply |

### SQL Join Spot Checks (March 26, 2026)

Source: `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv`

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `INNER JOIN 5Kx200x10` reorder chain with outer mixed union filter | 2.071 ms | 792.46 KB | Focused planner/executor stress case for reordered multi-join pipelines under an outer mixed union filter |

The latest targeted join rerun is narrower than the older March 25 projection matrix: this refresh is a focused reorder-chain validation case rather than a full hash-vs-nested-loop sweep.

### Focused Query-Engine Validation (March 12, 2026)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Parser simple SELECT | 120.4 ns | `ParserBenchmarks` |
| Parser complex SELECT | 2.50 us | JOIN + GROUP BY + HAVING + ORDER BY |
| Parser CTE (WITH clause) | 1.09 us | `ParserBenchmarks` |
| Query-plan cache stable SQL | 1.013 ms | Statement + plan cache hits |
| Query-plan cache pre-parsed | 1.003 ms | Plan cache hit |
| Query-plan cache varying SQL | 974 us | Limited plan reuse |
| Correlated scalar subquery filter (1K / 10K) | 3.061 ms / 29.292 ms | `COUNT(*)` guardrail |
| Correlated IN subquery filter (1K / 10K) | 1.440 ms / 16.344 ms | `COUNT(*)` guardrail, simple bound-probe fast path |
| Correlated NOT IN subquery filter (1K / 10K) | 1.515 ms / 17.848 ms | `COUNT(*)` guardrail, null-aware anti-semi bound-probe fast path |
| Correlated EXISTS subquery filter (1K / 10K) | 1.693 ms / 19.511 ms | `COUNT(*)` guardrail, simple bound-probe fast path |

### In-Memory Spot Checks

The single-op, ADO.NET, and persistence rows below were refreshed on April 7, 2026 from the latest `InMemory*Benchmarks` BenchmarkDotNet artifacts. The rotating batch rows below use the April 13, 2026 focused `hybrid-storage-mode-20260413-124730-median-of-3.csv` rerun.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL insert (private engine in-memory) | 4.267 us | `InMemorySqlBenchmarks` |
| Collection put (private engine in-memory) | 4.234 us | `InMemoryCollectionBenchmarks` on the binary payload path |
| SQL batch insert x100 (rotating in-memory) | ~781.25K rows/sec | Focused April 13 `hybrid-storage-mode-20260413-124730-median-of-3.csv` rerun |
| Collection batch put x100 (rotating in-memory) | ~947.97K docs/sec | Focused April 13 `hybrid-storage-mode-20260413-124730-median-of-3.csv` rerun |
| ADO.NET ExecuteScalar (`:memory:`) | 213.1 ns | Private connection-local in-memory DB |
| ADO.NET ExecuteScalar (`:memory:name`) | 301.2 ns | Named shared in-memory DB |
| ADO.NET insert (`:memory:`) | 2.815 us | Private connection-local in-memory DB |
| ADO.NET insert (`:memory:name`) | 2.824 us | Named shared in-memory DB |
| Load SQL DB + WAL into memory | 565.9 us | `Database.LoadIntoMemoryAsync` |
| Load collection DB + WAL into memory | 757.6 us | `Database.LoadIntoMemoryAsync` |
| Save in-memory SQL snapshot to disk | 1.554 ms | `Database.SaveToFileAsync` |
| Save in-memory collection snapshot to disk | 1.801 ms | `Database.SaveToFileAsync` |

### Cold / Cache-Pressured Lookup Spot Checks

These runs use a 200K-row working set with `MaxCachedPages = 16` and randomized lookup probes so the storage path is exercised instead of a single warmed page.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL cold lookup (file-backed) | 16.168 us | Cache-pressured primary-key lookup |
| SQL cold lookup (in-memory) | 1.452 us | Same workload after `LoadIntoMemoryAsync` |
| Collection cold get (file-backed) | 15.303 us | Cache-pressured direct collection lookup |
| Collection cold get (in-memory) | 1.172 us | Same workload after `LoadIntoMemoryAsync` |

### Indexed Lookup / Tuning Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` int equality (1 match) | 1.072 us | 1.53 KB | Direct integer-key index probe |
| Collection `FindByIndex` text equality (1 match) | 1.808 us | 4.25 KB | Ordered text bucket probe with exact string match before document materialization |
| Collection `FindByIndex` nested path equality | 995.823 us | 820.75 KB | Indexed nested scalar path probe over a many-match workload |
| Collection `FindByPath` nested path equality | 979.174 us | 819.81 KB | Query-facing path API on the same indexed nested path |
| Collection `FindByIndex` array path equality | 1,154.611 us | 1254.28 KB | Indexed terminal-array contains lookup |
| Collection `FindByPath` array path equality | 1,178.161 us | 1254.22 KB | Query-facing path API on the same indexed array path |
| Collection `FindByPath` nested array path equality | 1,813.161 us | 1742.21 KB | Indexed scalar lookup through array-of-object elements |
| Collection `FindByPath` integer range (1024 matches) | 487.011 us | 307.27 KB | Ordered integer path range query over the public collection path surface |
| Collection `FindByPath` text range (1000 matches) | 524.149 us | 499.56 KB | Ordered text path range query over prefix-bucket collection indexes |
| Collection `PutAsync` with secondary indexes (insert) | 10.099 us | 45.36 KB | Transaction + rollback micro for write maintenance |
| Collection `PutAsync` with secondary indexes (update) | 38.830 us | 220.83 KB | Includes old-entry removal plus reinsert |
| Collection `DeleteAsync` with secondary indexes | 33.565 us | 211.78 KB | Transaction + rollback micro for delete-side cleanup |

### File-Backed Lookup Tuning Takeaways

- `MaxCachedPages = 2048` was still the best collection setting in the current tuning matrix: indexed collection lookup fell from `54.15 us` at 16 pages to `20.57 us` at 2048 pages with `UseCachingIndexes = false`.
- `UseCachingIndexes` stayed neutral-to-negative on these lookup workloads. At `256` pages, SQL indexed lookup worsened from `91.55 us` to `273.44 us`, and the reused reader-session path worsened from `41.36 us` to `212.94 us`; collection lookup was roughly flat at `32.35 us` vs `34.06 us`.
- The reader-session setup penalty is visible again in the latest dedicated `ReaderSessionBenchmarks`: simple point lookups measured `1,703.7 ns` with per-query sessions, `680.9 ns` with a reused session, and `663.6 ns` with direct `ExecuteAsync`.
- The April 6, 2026 focused `CompositeIndexBenchmarks` rerun confirmed that cache-hot covered composite equality lookups are back ahead of the March 30 baseline: the 100K covered rows landed at `2.386 us` and `2.392 us` with `3.04 KB` allocated, down from the March 30 baseline's `3.108 us` and `3.159 us` at `10.63 KB`.
- A small dedicated WAL read cache still helps once the hottest pages live in the WAL instead of the main file: the WAL-backed SQL cold-lookup micro dropped from `28.74 us` to `19.83 us` when `MaxCachedWalReadPages = 128`.
- Raw storage traversal still benefits from speculative next-leaf reads: on the direct file-backed `BTreeCursor` micro, full scans improved from `9.25 ms` to `8.51 ms` at `10K` rows and from `92.74 ms` to `87.41 ms` at `100K`, while the `1024`-row seek window improved from `919.2 us` to `822.2 us` at `10K` and from `945.7 us` to `824.7 us` at `100K`.
- Recommended direct file-backed read preset for hot local workloads: `builder.UseDirectLookupOptimizedPreset()` keeps the existing page-cache shape and read path.
- Recommended direct cold-file preset for cache-pressured local reads: `builder.UseDirectColdFileLookupPreset()` keeps the existing cache shape but enables memory-mapped reads for clean main-file pages.

### File-Backed Durable Write Tuning Takeaways

#### Plan 1: Single-Writer Bulk Path Closeout

The current single-writer bulk-insert recommendation now comes from the April 19, 2026 focused Plan 1 scenario reruns under `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/`, plus the matched local SQLite comparison in `sqlite-compare-20260419-053217.csv`.

| Question | Result | Interpretation |
|----------|--------|----------------|
| Fastest embedded bulk API at `B1000` | `InsertBatch`: `184,782 rows/sec`; multi-row SQL: `175,084`; per-row SQL in one explicit transaction: `157,768` | `Database.PrepareInsertBatch(...)` remains the recommended embedded bulk path. |
| Best starting batch band | `B1`: `271 rows/sec`; `B10`: `2,599`; `B100`: `24,813`; `B1000`: `185,524`; `B10000`: `492,859` | Start at `B1000`. `B10000` is a valid pure-throughput mode, but it materially increases commit latency and tail risk (`P50 13.167 ms`, `P99 218.139 ms`). |
| Row-width sensitivity at `InsertBatch B1000` | Baseline: `182,692 rows/sec`; medium: `156,056`; wide: `75,209` | Row encoding and payload width are real tuning levers once batching is already in place. |
| Secondary-index sensitivity at `InsertBatch B1000` | PK-only: `185,524 rows/sec`; `+1` secondary: `130,999`; `+2`: `102,686`; `+4`: `68,567` | Secondary-index maintenance is the main remaining durable single-writer cost after batching. |
| Key-pattern sensitivity at `InsertBatch B1000` | Monotonic key: `183,191 rows/sec`; random key: `16,816` | Right-edge append behavior dominates random-key split-heavy behavior on this storage path. |
| SQLite parity check | SQLite matched durable SQL baseline: `206,936 rows/sec` at `B1000`, `580,289` at `B10000` | Current CSharpDB is now ahead on the same branch at `218,029 rows/sec` and `820,459 rows/sec`, about `105.4%` and `141.4%` of the matched SQLite rows on this runner. |

- Recommended embedded single-writer bulk path: `Database.PrepareInsertBatch(...)` with an explicit transaction and `builder.UseWriteOptimizedPreset()`.
- Recommended default batch size: `1000` rows per commit. Use `10000` for dedicated ingest jobs that can absorb larger commit latency and larger checkpoint/WAL bursts.
- Treat `INSERT ... VALUES (...), (...)` as a parity-check path, not the main recommendation. It is close enough to be useful, but it was still slower than `InsertBatch` on the same durable file-backed setup.
- Treat batch size, row width, secondary indexes, and key locality as the real tuning levers. API shape matters less than those once the path is already on `InsertBatch`.
- The April 19, 2026 duplicate-bucket follow-up also closed the previous secondary-index correctness hole: duplicate-heavy SQL index rows no longer crash the `Idx2` / `Idx4` scenarios, and those rows recovered to `102.7K` / `68.6K rows/sec` instead of failing outright.

### April 19, 2026 Plan 5 Refresh

The earlier table above remains the historical Plan 1 closeout snapshot. A
separate same-runner Plan 5 follow-up was captured on April 19, 2026 after
removing unnecessary logical conflict-key generation from the single-writer bulk
path when no explicit conflict-tracked transaction is active.

Artifacts:

- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic-20260419-154730-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic-20260419-154824-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260419-154948-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx0_Monotonic-20260419-164915-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx1_Monotonic-20260419-165524-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx2_Monotonic-20260419-165623-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx4_Monotonic-20260419-165721-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx0_Monotonic-20260419-171446-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx1_Monotonic-20260419-171147-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx2_Monotonic-20260419-171248-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx4_Monotonic-20260419-171347-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic-20260419-190840-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic-20260419-190839-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260419-191101-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic-20260419-193041-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic-20260419-193138-median-of-3.csv`

| Shape | CSharpDB | SQLite | Interpretation |
|-------|----------|--------|----------------|
| Matched durable bulk `B1000` | `218,029 rows/sec` | `206,936 rows/sec` | CSharpDB is now about `105.4%` of SQLite on the same runner. |
| Matched durable bulk `B10000` | `820,459 rows/sec` | `580,289 rows/sec` | CSharpDB is now about `141.4%` of SQLite on the same runner. |

- A later row-table amortization follow-up reused one encoded-record buffer per
  multi-row insert statement instead of allocating a fresh encoded payload for
  every inserted row. The April 19, 2026 reproducible rerun on that version
  landed at:
  `B1000 = 185,289 rows/sec`,
  `B10000 = 707,265 rows/sec`,
  matched SQLite `B1000 = 206,936 rows/sec`,
  matched SQLite `B10000 = 580,289 rows/sec`.
- That moved the parity story again:
  on the latest same-session rerun CSharpDB is about `89.5%` of SQLite at
  `B1000`, but about `121.9%` at `B10000`.
- The interpretation matters more than the headline:
  reusing the encoded-row buffer is a real primary-row win, especially once the
  durable flush is amortized, but it did not remove the remaining small-batch
  SQLite miss. That makes the next likely Plan 5 win a deeper row-table
  right-edge leaf fast path rather than more duplicate-bucket work.
- A later same-day row-table follow-up then removed one more piece of duplicate
  encode work from the same matched bulk path: the planner now passes the known
  encoded row length through to the serializer instead of having the encoder
  recompute it. That version also kept the right-edge leaf-split shortcut in
  place on the monotonic PK row. The reproducible rerun landed at
  `B1000 = 218,029 rows/sec` and `B10000 = 820,459 rows/sec`, while the latest
  matched SQLite rerun on the same branch remained `206,936` and `580,289`.
- That changed the headline answer again:
  CSharpDB is now about `105.4%` of SQLite at `B1000` and about `141.4%` at
  `B10000` on the matched durable monotonic bulk row.

- The refreshed post-fix attribution rows on the same branch landed at:
  `RowWidth baseline/medium/wide = 180.8K / 156.6K / 72.6K rows/sec`,
  `KeySweep monotonic/random = 179.6K / 15.8K rows/sec`.
- A later same-runner indexed-insert follow-up, after caching resolved insert
  plans and reusing key-component buffers, moved the index rows to:
  `Idx1 = 142.6K`, `Idx2 = 125.4K`, `Idx4 = 74.1K rows/sec`.
- A later reproducible duplicate-bucket follow-up added a validated append
  context for repeated same-key appendable hashed-index updates. That landed at
  `Idx0 = 184.1K`, `Idx1 = 139.7K`, `Idx2 = 126.4K`, `Idx4 = 74.8K rows/sec`.
- A deeper duplicate-bucket follow-up then changed the storage shape for new
  appendable hashed buckets so mutable append state lives in the overflow
  chain's first page instead of the B-tree payload. The latest reproducible
  rerun landed at `Idx0 = 213.1K`, `Idx1 = 163.0K`, `Idx2 = 149.1K`,
  `Idx4 = 83.2K rows/sec`.
- A later statement-level append-header amortization pass then staged repeated
  external-chain appends across each safe multi-row insert statement and
  flushed once at statement end. The reproducible rerun landed at
  `Idx0 = 211.1K`, `Idx1 = 158.2K`, `Idx2 = 148.2K`, `Idx4 = 86.4K rows/sec`.
- A deeper duplicate-bucket storage follow-up then delta-packed monotonic row
  ids inside the external duplicate-bucket chains instead of storing fixed
  `8`-byte row ids. The reproducible rerun landed at `Idx0 = 215.9K`,
  `Idx1 = 157.6K`, `Idx2 = 158.3K`, `Idx4 = 96.2K rows/sec`.
- A later row-table amortization follow-up then reused one caller-owned
  encoded-record buffer across each multi-row insert statement instead of
  allocating a fresh encoded row payload on every insert. The reproducible
  rerun landed at `B1000 = 185.3K`, `B10000 = 707.3K rows/sec`, while the
  matched SQLite rerun on the same branch landed at `206.9K` and `580.3K`.
- The immediate Plan 5 conclusion changed again after that later row-table
  follow-up: the matched durable monotonic bulk row is now ahead of SQLite on
  this runner at both `B1000` and `B10000`.
- The remaining Plan 5 priority is no longer durable flush amortization on the
  matched monotonic row. The external-chain-state follow-up removes a real
  duplicate-bucket B-tree rewrite, but the same-session `Idx0` control also
  moved up, so the indexed slope is still substantial. The later
  append-header-amortization rerun was basically a wash on this matrix, which
  is useful: repeated header rewrites were not the main blocker. The next
  insert-side win did come from a deeper secondary-index write-amplification
  cut: the unique-only `Idx1` row stayed flat while the duplicate-heavy
  `Idx2` / `Idx4` rows improved and their WAL bytes per flush dropped. Wide
  rows still matter. The later reusable-encode and known-length-encode
  follow-ups showed that row-table work still mattered on the matched baseline,
  but that path is no longer behind SQLite on this runner. The larger remaining
  Plan 5 slopes are now indexed maintenance on realistic schemas and the
  random-key locality cliff rather than primary monotonic-row parity.

#### Plan 2: Durability And Residency Trade-Offs

The current storage-semantics insert recommendation now comes from the focused Plan 2 rerun in `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260419-063026.csv`. These rows reuse the Plan 1 winner instead of the older per-row SQL path: one writer, `Database.PrepareInsertBatch("bench", 1000)`, one explicit transaction per commit, schema `bench(id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)`, monotonic keys, and a shared `20K`-row seeded starting state.

| Mode | Commits/sec | Rows/sec | P50 | P95 | P99 | Durability / Residency Semantics | Use When |
|------|-------------|----------|-----|-----|-----|----------------------------------|----------|
| File-backed durable + `UseWriteOptimizedPreset()` | `182.1` | `182,075` | `4.431 ms` | `5.052 ms` | `7.804 ms` | Fully durable file-backed commits; acknowledged commits force backing-file visibility and pages remain file-backed with normal caching. | Default durable embedded ingest baseline. |
| File-backed durable + `UseLowLatencyDurableWritePreset()` | `181.3` | `181,326` | `4.421 ms` | `5.093 ms` | `8.625 ms` | Same durability as the baseline, but planner-stat persistence is deferred. | Measure-first only; it was effectively flat-to-slightly-worse here, not a new default. |
| File-backed buffered + `UseWriteOptimizedPreset()` | `558.6` | `558,600` | `0.801 ms` | `1.285 ms` | `3.789 ms` | Buffered file-backed commits; managed buffers are flushed but recent commits remain more exposed on OS crash or power loss. | High-throughput ingest when that durability trade-off is intentional. |
| `OpenInMemoryAsync(...)` | `1,314.1` | `1,314,099` | `0.672 ms` | `1.170 ms` | `2.071 ms` | No crash durability; the database exists only in private process memory. | Ephemeral ingest, cache-like workloads, or explicit later snapshotting. |
| `LoadIntoMemoryAsync(...)` | `1,314.2` | `1,314,198` | `0.666 ms` | `1.165 ms` | `2.199 ms` | Existing file + committed WAL state are loaded once, then subsequent inserts run entirely in memory until an explicit save. | "Load once, then ingest in RAM" workflows on an existing durable dataset. |
| `OpenHybridAsync(...)` + `IncrementalDurable` | `179.7` | `179,737` | `4.452 ms` | `5.185 ms` | `11.231 ms` | Fully durable hybrid WAL/checkpoint path; the backing file stays authoritative while touched pages remain resident by cache policy. | Residency/layout choice when file-backed durability must remain, not as an ingest-speed shortcut. |

- Buffered file-backed mode is the major write-throughput lever inside the durable/file-backed family on this runner: about `3.1x` the fully durable baseline at the same `InsertBatch B1000` path.
- Fresh in-memory and load-into-memory were effectively identical on steady-state insert throughput here: both landed at about `1.31M rows/sec`, or about `7.2x` the durable file-backed baseline.
- Hybrid incremental-durable was effectively the same as ordinary durable file-backed ingest on this path. Treat it as a residency/design choice, not a generic insert-speed substitute for in-memory mode.

#### Plan 3: Concurrent Disjoint-Key Writers Closeout

The current concurrent-insert recommendation now comes from `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/insert-fan-in-diagnostics-20260419-140845.csv`. These rows keep one shared durable file-backed `Database`, one row per commit, and compare serialized auto-commit, shared auto-commit concurrent insert execution, and explicit `WriteTransaction` ownership under disjoint explicit-key, hot right-edge, and auto-generated-id shapes.

Disjoint explicit-key crossover at `250us` durable group commit (`commits/sec / commitsPerFlush`):

| Path | `W1` | `W2` | `W4` | `W8` | Interpretation |
|------|------|------|------|------|----------------|
| Serialized auto-commit control | `277.4 / 1.00` | `270.5 / 1.00` | `255.2 / 1.00` | `258.3 / 1.00` | Control path stays flat; inserts do not fan in by themselves |
| Shared auto-commit concurrent | `250.2 / 1.00` | `254.7 / 1.00` | `505.8 / 1.99` | `834.5 / 3.28` | Real crossover starts at `W4` and becomes large at `W8` |
| Explicit `WriteTransaction` | `247.1 / 1.00` | `249.8 / 1.00` | `494.3 / 2.00` | `883.6 / 3.41` | Same band as shared concurrent; only modestly ahead at the winning `W8` row |

Failure-boundary rows at `W8`, `250us`:

| Shape | Commits/sec | `P50` | `P95` | `P99` | `commitsPerFlush` | Note |
|-------|-------------|-------|-------|-------|-------------------|------|
| Shared auto-commit concurrent, hot right-edge explicit ID | `255.6` | `30.173 ms` | `57.348 ms` | `84.233 ms` | `1.00` | Still the primary insert-side failure boundary |
| Explicit `WriteTransaction`, hot right-edge explicit ID | `257.0` | `27.238 ms` | `73.117 ms` | `99.365 ms` | `1.00` | Explicit transaction ownership does not unlock fan-in here |
| Shared auto-commit concurrent, auto-generated ID | `252.1` | `30.830 ms` | `61.916 ms` | `86.468 ms` | `1.00` | Correctness clean; `duplicateKeys = 0` |
| Explicit `WriteTransaction`, auto-generated ID | `261.5` | `26.529 ms` | `74.863 ms` | `105.511 ms` | `1.00` | Correctness clean; `duplicateKeys = 0`, `extraAttempts = 84` |

- With `Batch0`, all disjoint explicit-key rows stayed in the same `244-273 commits/sec` band and `commitsPerFlush = 1.00`. The queue only helps once commits overlap inside the batch window.
- The crossover on this runner is `W4/W8` plus `250us`, not `W1/W2`.
- The gain requires explicit disjoint key ranges. Hot right-edge inserts and auto-generated IDs remain separate shapes and should not be used as the best-case concurrent insert story.
- Recommended concurrent-insert guidance: if the workload naturally shards keys across writers, benchmark shared auto-commit concurrent inserts first with a small batch window. If the workload is hot right-edge or auto-id dominated, keep application-level batching as the first lever and treat concurrency as secondary.

- Do not compare the durable write harnesses directly. `durable-sql-batching` answers explicit-transaction and analyzed-table SQL questions, `write-diagnostics` answers single-row checkpoint/WAL policy questions, and `concurrent-write-diagnostics` answers shared-engine in-process writer contention questions.
- The April 7, 2026 `durable-sql-batching-20260407-071043.csv` run again showed that application-level batching is the largest lever. Auto-commit single-row SQL stayed around `281.6 ops/sec`, but explicit transactions scaled to about `2906 rows/sec` at `10` rows/commit, `27911 rows/sec` at `100` rows/commit, and `220172 rows/sec` at `1000` rows/commit.
- The same batching run showed that analyzed `UseLowLatencyDurableWritePreset()` was clearly ahead of the analyzed `UseWriteOptimizedPreset()` row on this runner: `295.3 ops/sec` vs `281.2 ops/sec`. Treat the low-latency preset as measure-first rather than a blanket default change.
- Commit-path diagnostics from the batching run still pointed at the durable flush as the dominant fixed cost for single-row durable SQL. WAL append, publish-batch, finalize, and checkpoint-decision stages remained comparatively small.
- The latest dedicated single-writer `write-diagnostics` reference point is still the March 30, 2026 `write-diagnostics-20260330-121058-median-of-3.csv` rerun, which stayed in a tight `270-280 ops/sec` band. The April 10 closeout targeted the newer explicit `WriteTransaction`, concurrent durable auto-commit, and stress suites instead of rerunning that older single-writer matrix.
- The April 10 explicit `WriteTransaction` closeout in `write-transaction-diagnostics-20260410-134444-median-of-3.csv` shows the current shape clearly: hot unseeded `W8 Rows1 Batch0` landed at `278.7 tx/sec`, the same workload improved to `341.8 tx/sec` after seeding `16K` rows, the best hot steady-state insert row reached `377.7 tx/sec` / `3778.0 rows/sec`, and low-conflict disjoint updates reached `590.4 tx/sec` at `W8`.
- The April 11 checkpoint-retention rerun in `checkpoint-retention-diagnostics-20260411-082100-median-of-3.csv` gives the clearest read on the split checkpoint path: the `W8_Blocker3s_Batch250us` row still recorded `55` background checkpoint starts while the explicit write transaction stayed open, and the forced checkpoint immediately after release was only `5.425 ms`, effectively the same as the `5.388 ms` no-blocker control.
- The April 10 concurrent durable auto-commit closeout in `concurrent-write-diagnostics-20260410-135529-median-of-3.csv` is now tightly clustered around `463-468 commits/sec` without preallocation. `W8_Batch250us` was the best `8`-writer no-preallocation row at `467.3`, but it was effectively tied with `W8_Batch0` at `466.7`. `W4_Batch500us` led the `4`-writer rows at `468.3`, also effectively tied with `W4_Batch0` at `468.1`.
- The same April 10 concurrent closeout showed no win from `1 MiB` WAL preallocation on this runner: `W8_Batch0_Prealloc1MiB` dropped to `420.0 commits/sec` and `W8_Batch250us_Prealloc1MiB` landed at `420.9`.
- A fresh April 11, 2026 focused fan-in rerun in `commit-fan-in-diagnostics-20260411-141949.csv` now shows shared auto-commit disjoint updates actually coalescing commits on one shared engine: the `W4` row reached `525 commits/sec` with `commitsPerFlush = 1.99`, and the `W8` row reached `743 commits/sec` with `commitsPerFlush = 3.37`. On the same run, the explicit `WriteTransaction` `W8` disjoint-update row landed at `765 commits/sec` with `commitsPerFlush = 3.38`, so the shared non-insert auto-commit path is now effectively in the same commit-fan-in band on this runner.
- That April 11 phase-4 result is intentionally narrower than "all auto-commit writes now coalesce." The targeted hot-insert sanity rerun in `concurrent-write-scenario-W8_Batch250us-20260411-142023.csv` still measured `428 commits/sec` with `commitsPerFlush = 1.00`, so hot single-row auto-commit inserts should still be treated as insert-specific contention rather than assumed to benefit from the new shared non-insert path.
- The expanded April 19, 2026 insert fan-in rerun in `insert-fan-in-diagnostics-20260419-140845.csv` gives the cleaner insert-side rule. Disjoint explicit-key inserts still stayed flat at `W1/W2` and with `Batch0`, but they crossed over sharply at `W4/W8` with `250us`: shared auto-commit concurrent reached `505.8` / `834.5 commits/sec`, and explicit `WriteTransaction` reached `494.3` / `883.6`, both with `commitsPerFlush` near `2.00` and `3.3-3.4`.
- The April 10 stress closeout in `stress-20260410-140205-median-of-3.csv` still shows the intended logical-conflict separation: overlapping read transactions fell to `96.5 ops/sec` with a `63.1%` conflict rate, while disjoint read transactions reached `261.5 ops/sec` with `0.0%` conflicts. Disjoint write transactions held `156.0 ops/sec` with only `1.5%` conflicts.
- Commit-path diagnostics across the April 10 through April 19 multi-writer reruns still point at the durable flush as the dominant fixed cost once the queue is available. The insert-side update to the story is narrower than a blanket "inserts now fan in": only disjoint explicit-key inserts reached the queue effectively, while hot right-edge and auto-generated-id shapes stayed pinned near one commit per flush.
- Recommended interpretation after the April 19 rerun: keep defaults at `0`, start with `builder.UseWriteOptimizedPreset()`, benchmark application-level batching first, then benchmark `UseDurableGroupCommit(...)` on your actual conflict shape. If your shared workload naturally shards explicit keys across writers, shared auto-commit concurrent inserts are now worth measuring directly. If the workload is a hot single-row insert loop or depends on auto-generated IDs, keep batching as the first lever and treat concurrency as secondary.
- Recommended file-backed write-heavy preset: `builder.UseWriteOptimizedPreset()`. This is opt-in and does not change the engine default checkpoint policy.

## CSharpDB Storage Mode Comparison

These tables isolate the embedded CSharpDB storage modes relevant to the current hybrid design:

- file-backed
- hybrid incremental-durable
- hybrid hot-set incremental-durable
- in-memory

The tables below come from different focused harnesses and should not be mixed:

- resident hot-set read source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260407-071453.csv`
- post-checkpoint hot reread source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260407-071502.csv`
- hot steady-state source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260413-124730-median-of-3.csv`
- cold open source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260407-071440.csv`

Use the resident hot-set table as the canonical "pinned/resident hot object"
comparison. That harness opens fresh instances and times only the immediate hot
read burst. Use the cold-open tables to see the up-front cost of that preload,
and use the post-checkpoint table to see the baseline lazy-hybrid checkpoint
residency behavior.

### Canonical Resident Hot-Set Comparison

Each iteration opens a fresh database instance, then measures the same `256`-item
SQL or collection hot burst. Open cost is excluded from throughput here so the
table shows read speed after the mode is ready to serve.

| Mode | SQL Hot Burst | SQL P50 | Collection Hot Burst | Collection P50 |
|------|---------------|---------|----------------------|----------------|
| **File-backed** | **42.43K ops/sec** | **0.0238 ms** | **41.94K ops/sec** | **0.0220 ms** |
| **Hybrid incremental-durable** | **46.56K ops/sec** | **0.0200 ms** | **44.22K ops/sec** | **0.0218 ms** |
| **Hybrid hot-set incremental-durable** | **628.12K ops/sec** | **0.0011 ms** | **850.22K ops/sec** | **0.0011 ms** |
| **In-memory** | **451.53K ops/sec** | **0.0017 ms** | **442.29K ops/sec** | **0.0015 ms** |

### Hybrid Open Cost

This is the cost you pay to get the resident hot-set behavior above.

| Mode | SQL Open Only | SQL P50 | Collection Open Only | Collection P50 |
|------|---------------|---------|----------------------|----------------|
| **File-backed** | **83.3 ops/sec** | **11.8953 ms** | **81.1 ops/sec** | **12.2567 ms** |
| **Hybrid incremental-durable** | **80.6 ops/sec** | **12.2386 ms** | **81.8 ops/sec** | **12.2379 ms** |
| **Hybrid hot-set incremental-durable** | **12.4 ops/sec** | **79.2667 ms** | **7.7 ops/sec** | **119.3234 ms** |
| **In-memory** | **74.4 ops/sec** | **12.2932 ms** | **54.9 ops/sec** | **17.6865 ms** |

### Cold Open + First Read

| Mode | SQL Open + First Lookup | SQL P50 | Collection Open + First Get | Collection P50 |
|------|--------------------------|---------|------------------------------|----------------|
| **File-backed** | **80.5 ops/sec** | **12.4610 ms** | **79.9 ops/sec** | **12.5342 ms** |
| **Hybrid incremental-durable** | **78.9 ops/sec** | **12.4234 ms** | **76.9 ops/sec** | **12.4167 ms** |
| **Hybrid hot-set incremental-durable** | **12.0 ops/sec** | **82.2847 ms** | **7.8 ops/sec** | **115.9533 ms** |
| **In-memory** | **80.8 ops/sec** | **12.3731 ms** | **50.5 ops/sec** | **19.8949 ms** |

### Post-Checkpoint Hot Reread

Each measured burst forces one auto-checkpointed write and then rereads the same
`256`-item hot set. This is the baseline lazy-hybrid checkpoint-residency
harness.

| Mode | SQL Rereads/sec | SQL P50 | Collection Rereads/sec | Collection P50 |
|------|-----------------|---------|------------------------|----------------|
| **File-backed** | **24.09K ops/sec** | **0.0007 ms** | **24.29K ops/sec** | **0.0005 ms** |
| **Hybrid incremental-durable** | **24.29K ops/sec** | **0.0007 ms** | **24.09K ops/sec** | **0.0006 ms** |
| **Hybrid hot-set incremental-durable** | **24.17K ops/sec** | **0.0007 ms** | **24.08K ops/sec** | **0.0006 ms** |
| **In-memory** | **2.03M ops/sec** | **0.0004 ms** | **2.88M ops/sec** | **0.0003 ms** |

### Hot Steady-State SQL

| Mode | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|------|---------------|----------------|--------------|------------------|
| **File-backed** | **283.6 ops/sec** | **~26.89K rows/sec** | **~1.46M ops/sec** | **~1.01M / ~11.34M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **Hybrid incremental-durable** | **289.2 ops/sec** | **~27.43K rows/sec** | **~1.45M ops/sec** | **~1.02M / ~11.26M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **In-memory** | **~237.30K ops/sec** | **~781.25K rows/sec** | **~1.45M ops/sec** | **~1.02M / ~11.34M COUNT(*) ops/sec (8r, per-query / reused x32)** |

`Hybrid hot-set incremental-durable` is intentionally not shown in the generic
steady-state table. Once the workload itself has already touched the hot pages,
the dedicated warm-set hint stops being the differentiator. Use the resident
hot-set table above for that feature.

### Hot Steady-State Collection

| Mode | Single Put | Batched Put | Point Get |
|------|------------|-------------|-----------|
| **File-backed** | **277.7 ops/sec** | **~25.89K docs/sec** | **~1.93M ops/sec** |
| **Hybrid incremental-durable** | **282.2 ops/sec** | **~26.71K docs/sec** | **~1.92M ops/sec** |
| **In-memory** | **~245.48K ops/sec** | **~947.97K docs/sec** | **~1.91M ops/sec** |

## Competitor Comparison

The master table below separates embedded engine runs from client/hosted runs so the interface cost is visible.

- CSharpDB rows in this section were refreshed on April 13, 2026 from `master-table-20260413-125827-median-of-3.csv` using the dedicated `--master-table --repeat 3 --repro` harness.
- CSharpDB SQL concurrent reads are shown as `per-query sessions / reused reader sessions (x32 reads per snapshot)` because those patterns measure materially different setup costs. The direct local SQL client row still reports only the per-query reader pattern.
- The April 13 reused-session burst rows also carry `latency-sampling=1/128` in `ExtraInfo`. Throughput stays exact; only the latency columns are sampled for those high-rate rows.
- A dedicated `--master-table` suite now emits only the CSharpDB rows used by this section into `master-table-*.csv`, with stable `MasterComparison_*` row names and batch throughput normalized to rows/sec or docs/sec.
- The concurrent durable write companion table below stays separate because its `ops/sec` is total successful commits/sec across `4` or `8` simultaneous writers sharing one engine and WAL, not single-writer insert throughput.
- The top SQL/collection API snapshot tables above use the macro harness in durable mode (`macro-20260407-070125.csv`).
- Cold / cache-pressured lookup numbers were also refreshed on April 7, 2026 from `ColdLookupBenchmarks-report.csv`, but they stay in the dedicated spot-check section rather than the master table.
- Ordered/range covered-scan numbers were refreshed on March 14, 2026 from `OrderByIndexBenchmarks`, but they stay in the micro sections because the master table tracks durable writes, cold point lookups, and concurrent-read throughput rather than scan-shape throughput.
- Indexed aggregate numbers were refreshed on March 14, 2026 from `IndexAggregateBenchmarks`, but they stay in the micro sections because the master table does not currently have an aggregate column.
- Primary-key aggregate numbers were refreshed on March 14, 2026 from `PrimaryKeyAggregateBenchmarks`, and they also stay in the micro sections for the same reason.
- Embedded engine rows and client rows are different surfaces: the direct client row includes public client API overhead on top of the embedded engine itself.
- Competitor figures are still approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

CSharpDB rows below are durable April 13, 2026 median-of-3 captures from `master-table-20260413-125827-median-of-3.csv`. Competitor rows remain single published directional ranges.

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB SQL (embedded engine, file-backed)** | **C#** | **Relational SQL** | **281.7 ops/sec** | **~26.07K rows/sec** | **~1.45M ops/sec** | **~956.51K / ~11.56M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (embedded engine, incremental-durable hybrid)** | **C#** | **Relational SQL** | **279.9 ops/sec** | **~26.93K rows/sec** | **~1.45M ops/sec** | **~997.22K / ~11.37M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (direct client, local process)** | **C#** | **Relational SQL** | **285.6 ops/sec** | **~3.14K rows/sec** | **~653.98K ops/sec** | **~467.82K COUNT(*) ops/sec (8r)** |
| **CSharpDB SQL (embedded engine, in-memory)** | **C#** | **Relational SQL** | **~243.77K ops/sec** | **~798.91K rows/sec** | **~1.46M ops/sec** | **~990.44K / ~11.58M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB Collection (embedded engine, file-backed)** | **C#** | **Document (NoSQL)** | **281.8 ops/sec** | **~26.93K docs/sec** | **~1.90M ops/sec** | **-** |
| **CSharpDB Collection (embedded engine, incremental-durable hybrid)** | **C#** | **Document (NoSQL)** | **282.7 ops/sec** | **~26.89K docs/sec** | **~1.91M ops/sec** | **-** |
| **CSharpDB Collection (embedded engine, in-memory)** | **C#** | **Document (NoSQL)** | **~247.20K ops/sec** | **~943.20K docs/sec** | **~1.93M ops/sec** | **-** |
| SQLite | C | Relational SQL | ~1-4K ops/sec | ~80-114K rows/sec | N/A | WAL lock limited |
| LiteDB | C# | Document (NoSQL) | ~1K ops/sec | ~16-21K rows/sec | N/A | N/A |
| Realm | C++ | Object DB | ~9-76K obj/sec | N/A | N/A | Multi-reader |
| UnQLite | C | Doc + KV store | ~41K (KV) / ~28K (doc) | N/A | N/A | N/A |
| H2 | Java | Relational SQL | ~3-8K ops/sec | ~2-6.5K rows/sec | N/A | Multi-threaded |
| NeDB | JavaScript | Document (JSON) | ~325 (persistent) | N/A | N/A | N/A |
| LowDB | JavaScript | JSON file | ~5-50 ops/sec | N/A | N/A | N/A |
| RocksDB | C++ | KV (LSM-tree) | ~17K ops/sec | ~1M+ rows/sec | N/A | ~713K (32 threads) |
| DuckDB | C++ | Columnar SQL (OLAP) | ~1-8K ops/sec | ~163K-1.2M rows/sec | N/A | N/A (OLAP) |
| PouchDB | JavaScript | Document + sync | ~4-6K (bulk) | ~4-6K docs/sec | N/A | N/A |
| TinyDB | Python | Document (JSON) | ~1-5K ops/sec | ~26K batch | N/A | N/A |

### Buffered Durability Snapshot (CSharpDB Only)

The durable master table above stays the default published comparison. The table below is a separate April 7, 2026 CSharpDB-only snapshot from `master-table-20260407-204058-median-of-3.csv`, produced by rerunning `--master-table --repeat 3 --repro` with `CSHARPDB_BENCH_DURABILITY=Buffered`.

`Buffered` is the less-durable write mode, analogous to SQLite WAL `synchronous=NORMAL`: commit records are flushed to the OS, but CSharpDB does not force an OS-buffer flush before acknowledging every commit. That can materially improve write throughput, but it also means recently acknowledged commits are more exposed to loss on OS crash or power loss than in the durable table above.

This section stays write-focused because durability mode mostly changes commit costs, not point-lookups or concurrent-read setup costs.

| Surface | Single Op (Durable) | Single Op (Buffered) | Batch x100 (Durable) | Batch x100 (Buffered) | Notes |
|---------|----------------------|----------------------|----------------------|-----------------------|-------|
| CSharpDB SQL (embedded engine, file-backed) | `281.7 ops/sec` | `~21.17K ops/sec` | `~26.07K rows/sec` | `~456.63K rows/sec` | Biggest write uplift in the embedded SQL path |
| CSharpDB SQL (embedded engine, incremental-durable hybrid) | `279.9 ops/sec` | `~19.65K ops/sec` | `~26.93K rows/sec` | `~431.07K rows/sec` | Similar buffered uplift with the hybrid pager layout |
| CSharpDB SQL (direct client, local process) | `285.6 ops/sec` | `~20.03K ops/sec` | `~3.14K rows/sec` | `~5.79K rows/sec` | Single inserts jump sharply; batch remains transport-heavy |
| CSharpDB Collection (embedded engine, file-backed) | `281.8 ops/sec` | `~19.30K ops/sec` | `~26.93K docs/sec` | `~399.76K docs/sec` | File-backed document path benefits strongly from buffered commits |
| CSharpDB Collection (embedded engine, incremental-durable hybrid) | `282.7 ops/sec` | `~20.55K ops/sec` | `~26.89K docs/sec` | `~391.35K docs/sec` | Buffered mode is similarly favorable on the hybrid document path |

### Concurrent Durable Writes (Single-Row Auto-Commit, CSharpDB Only)

The table below uses the April 10, 2026 shared-engine artifact `concurrent-write-diagnostics-20260410-135529-median-of-3.csv`.

Each logical writer issues single-row `INSERT` statements with auto-commit against one shared `Database` instance. This is not an application-level batch-insert benchmark. `Durable Commits/sec` is the total successful commit rate across all writers combined. `Commit Coalescing Window` is the engine-side durable commit window; it may group overlapping commits at flush time, but it does not turn each writer into a multi-row transaction benchmark.

| Scenario | Writers | Commit Coalescing Window | WAL Prealloc | Durable Commits/sec | Notes |
|----------|---------|--------------------------|--------------|---------------------|-------|
| CSharpDB SQL shared engine | 4 | `0` | `0` | `468.1` | Essentially tied for the best 4-writer row |
| CSharpDB SQL shared engine | 4 | `250us` | `0` | `461.0` | Slightly below the `0` / `500us` rows |
| CSharpDB SQL shared engine | 4 | `500us` | `0` | `468.3` | Best 4-writer row, but only by noise-level margin |
| CSharpDB SQL shared engine | 8 | `0` | `0` | `466.7` | Baseline 8-writer shared-engine throughput in the closeout sweep |
| CSharpDB SQL shared engine | 8 | `250us` | `0` | `467.3` | Best 8-writer no-preallocation row, effectively tied with `0` |
| CSharpDB SQL shared engine | 8 | `500us` | `0` | `463.5` | Slightly behind the other no-preallocation rows |
| CSharpDB SQL shared engine | 8 | `0` | `1 MiB` | `420.0` | Preallocation regressed throughput on this runner |
| CSharpDB SQL shared engine | 8 | `250us` | `1 MiB` | `420.9` | Slightly above `Batch0`, still below the no-preallocation rows |

Hot-cache point-lookup reference for CSharpDB:

| Database | Point Lookup (hot cache) |
|----------|---------------------------|
| CSharpDB SQL (file-backed) | ~4.16M ops/sec |
| CSharpDB SQL (in-memory) | ~4.21M ops/sec |
| CSharpDB Collection (file-backed) | ~2.63M ops/sec |
| CSharpDB Collection (in-memory) | ~2.79M ops/sec |

### Sources for Competitor Numbers

| Database | Sources |
|----------|---------|
| **SQLite** | [SQLite Optimizations for Ultra High-Performance (PowerSync)](https://www.powersync.com/blog/sqlite-optimizations-for-ultra-high-performance) · [Evaluating SQLite Performance by Testing All Parameters (Eric Draken)](https://ericdraken.com/sqlite-performance-testing/) · [SQLite Performance Tuning (phiresky)](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/) · [How fast is SQLite? (marending.dev)](https://marending.dev/notes/sqlite-benchmarks/) |
| **LiteDB** | [LiteDB-Benchmark (official)](https://github.com/mbdavid/LiteDB-Benchmark) · [LiteDB-Perf: INSERT/BULK compare (official)](https://github.com/mbdavid/LiteDB-Perf) · [SoloDB vs LiteDB Deep Dive (unconcurrent.com)](https://unconcurrent.com/articles/SoloDBvsLiteDB.html) |
| **Realm** | [Realm API Optimized for Performance (Realm Academy)](https://academy.realm.io/posts/realm-api-optimized-for-performance-and-low-memory-use/) · [realm-java-benchmarks (official)](https://github.com/realm/realm-java-benchmarks) · [SwiftData vs Realm Performance (Emerge Tools)](https://www.emergetools.com/blog/posts/swiftdata-vs-realm-performance-comparison) |
| **UnQLite** | [Un-scientific Benchmarks of Embedded Databases (Charles Leifer)](https://charlesleifer.com/blog/completely-un-scientific-benchmarks-of-some-embedded-databases-with-python/) · [ioarena: Embedded Storage Benchmarking Tool](https://github.com/pmwkaa/ioarena) |
| **H2** | [H2 Database Performance (official)](http://www.h2database.com/html/performance.html) · [H2 JPA Benchmark (jpab.org)](https://www.jpab.org/H2.html) · [Evaluating H2 as a Production Database (Baeldung)](https://www.baeldung.com/h2-production-database-features-limitations) |
| **RocksDB** | [Performance Benchmarks (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks) · [RocksDB Benchmarks 2025 (Small Datum)](https://smalldatum.blogspot.com/2025/12/performance-for-rocksdb-98-through-1010.html) · [Benchmarking Tools - db_bench (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools/) |
| **DuckDB** | [Benchmarks (DuckDB Docs)](https://duckdb.org/docs/stable/guides/performance/benchmarks) · [DuckDB v1.4 LTS Benchmark Results](https://duckdb.org/2025/10/09/benchmark-results-14-lts) · [Database-like Ops Benchmark (DuckDB Labs)](https://duckdblabs.github.io/db-benchmark/) |
| **NeDB / LowDB / PouchDB / TinyDB** | [PouchDB vs NeDB comparison (GitHub)](https://github.com/pouchdb/pouchdb/issues/4031) · [npm trends: lowdb vs nedb vs pouchdb](https://npmtrends.com/lowdb-vs-nedb-vs-pouchdb) · [Flat File Database roundup (medevel.com)](https://medevel.com/flatfile-database-1721/) |

> Note: competitor figures are directional, not lab-identical. If the decision matters, run the same workload on your own hardware.

## See Also

- [Project README](../../README.md)
- [Architecture Guide](../../docs/architecture.md)
- [Roadmap](../../docs/roadmap.md)
