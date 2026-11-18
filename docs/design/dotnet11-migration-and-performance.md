# .NET 11 migration and performance work

Date: 2026-11-13. Branch: `max/dotnet11`.

## Migration scope

- Pin `global.json` to SDK 11.0.100, retaining patch roll-forward and the
  Microsoft.Testing.Platform runner.
- Target .NET 11 in the runtime libraries, applications, tests, benchmarks,
  package smoke projects, and samples. Studio keeps its existing Windows minimum.
- Keep `CSharpDB.Generators` on `netstandard2.0`: it is loaded by the compiler,
  rather than run as a .NET application. Its Roslyn 5.9 references are compatible
  with the SDK's Roslyn 5.12 compiler.
- Align direct ASP.NET Core, Microsoft.Extensions, OleDb, and PKCS references
  with 11.0.0. ASP.NET Core OpenAPI 11 requires the OpenAPI 3 document model.
- Update child-process paths, desktop hosting, EF migration-tool project
  validation, release/install scripts, worker dependency checks, and CI paths.
- CI performance guardrails use `-UseHarnessSdk` to build both disposable
  product checkouts with the candidate harness SDK. The original SDK manifests
  and effective build-input hashes are retained. Product TFMs are unchanged.
  This compares product source under a common benchmark runtime; a separate
  runtime-only experiment is still required to quantify the .NET upgrade.
- Retain qualified EF Core **10.0.14**, including `dotnet-ef` and the provider's
  package graph. Targeting .NET 11 does not require migrating the provider to EF
  Core 11. That upgrade needs its own SQL translation and migration review.
  The local EF tool allows runtime roll-forward so .NET 11-only hosts can run it.
- Preserve published benchmark manifests, archived results, and historical
  design/release documents. Their .NET 10 identifiers describe past runs.

Consumers of the new binaries/packages need .NET 11. Existing published packages
and releases are unaffected. No database format or durability setting changes.

### Numeric compatibility

.NET 11 changes [floating-point/decimal conversions](https://learn.microsoft.com/en-us/dotnet/core/compatibility/core-libraries/11/decimal-biginteger-floating-point-conversions)
and [fractional-digit floating-point rounding](https://learn.microsoft.com/en-us/dotnet/core/compatibility/core-libraries/11/math-round-digits).
Initial migration tests exposed binary tails in DECIMAL assignments, generated
values, and EF updates. `CSharpDbDecimalCodec.FromDouble` and `FromSingle` now
define 15- and 7-significant-digit conversion explicitly, using stack formatting
and decimal parsing. Exact decimal inputs remain exact; declared scale and
precision validation still reject values that cannot be stored. The Access-style
formula `Round` retains its previous scale/round/unscale algorithm.

## What to investigate next

The source is Microsoft's [Performance Improvements in .NET 11](https://devblogs.microsoft.com/dotnet/performance-improvements-in-net-11/).
Its relevant changes include JIT devirtualization and escape analysis, opt-in
runtime async, vectorized Unicode whitespace searches, sorting improvements,
collection operations, and JSON processing. These are leads for measurement,
not measured CSharpDB improvements. In particular, its Windows synchronous
`RandomAccess.Read` result does not describe our asynchronous read path.

The ranking below comes from inspecting the current CSharpDB implementation.

| Priority | Candidate and owning code | Experiment and acceptance criteria |
| --- | --- | --- |
| 1 | Measure the runtime upgrade alone. B-tree cursors, serializers, query operators, page caches, and WAL code all execute on the upgraded runtime. | Compare unchanged database source built for .NET 10 and 11. Run point lookup, scan, insert/batch, ORDER BY, and durable commit cases; retain latency distributions, allocations, throughput, and exact runtime metadata. This separates runtime gains from later source changes. |
| 2 | Runtime async across `Database`, `BTree`, `Pager`, `WriteAheadLog`, and `FileStorageDevice`. These layers contain many directly awaited `Task`/`ValueTask` calls. | Build a separate variant with `<Features>$(Features);runtime-async=on</Features>` for the runtime projects. Compare cached reads, real file I/O, durable writes, cancellation, exceptions, and disposal. Keep the default unchanged until correctness and repeated performance results pass; qualify NativeAOT independently. |
| 3 | SQL whitespace scanning in `Tokenizer.SkipWhitespace`, `SqlQueryFingerprintProvider`, and raw-text scanners in `Parser`. | Compare scalar scanning with [`ReadOnlySpan<char>.IndexOfAnyExceptWhiteSpace`](https://github.com/dotnet/runtime/pull/111439). Benchmark no whitespace, ordinary one-space SQL, long indentation, all-whitespace input, and Unicode whitespace. Preserve token offsets, fingerprint output, EOF behavior, and cancellation. Reject a change that helps long runs but regresses ordinary queries. |
| 4 | Sort comparison dispatch in `Execution/Operators.cs` and checkpoint page ordering in both WAL implementations. | Compare current delegate/interface comparers with concrete struct comparers and generic span sorting. Use `SortStabilityBenchmarks` plus checkpoint workloads. Preserve SQL null ordering, collations, tie behavior, and the valid range of rented arrays. The existing WAL comparer is cached, so a per-sort delegate-allocation saving must not be assumed. |
| 5 | Collection construction, removal, and JSON in caches, collection indexes, and client/API serialization. | Profile first, then use `StorageHotPathBenchmarks`, collection benchmarks, and transport benchmarks to isolate actual allocation/CPU costs. Existing tokenizer keyword lookup already uses span alternate lookup; avoid duplicating that optimization. |

## Measurement sequence

1. Finish migration correctness and packaging validation before collecting timings.
2. Build isolated .NET 10 and .NET 11 baselines from the same source; do not use
   old published benchmark numbers as the control. Record SDK, runtime, commit,
   architecture, durability, storage preset, and dependency versions. Separate
   any dependency update effects from a runtime-only comparison.
3. Run benchmarks without concurrent builds or tests. Use a pilot to select run
   duration, then repeat in alternating A/B order. Retain raw output and report
   dispersion; an unstable comparison is inconclusive.
4. Evaluate one source change at a time against the migrated baseline. Require
   correct results, no meaningful regressions on ordinary workloads, and a
   repeatable improvement on the intended workload.
5. Confirm file-backed durable behavior and reopen/crash recovery after any
   async, I/O, WAL, or checkpoint change. Do not trade away flush guarantees to
   improve a timing result.

Example existing microbenchmark entry points, run separately:

```powershell
dotnet run --project tests/CSharpDB.Benchmarks -c Release -- --micro --filter '*ParserBenchmarks*'
dotnet run --project tests/CSharpDB.Benchmarks -c Release -- --micro --filter '*PointLookupBenchmarks*'
dotnet run --project tests/CSharpDB.Benchmarks -c Release -- --micro --filter '*SortStabilityBenchmarks*'
dotnet run --project tests/CSharpDB.Benchmarks -c Release -- --micro --filter '*StorageHotPathBenchmarks*'
```

The migration itself did not establish a speedup. The
[first controlled core-engine comparison](dotnet11-benchmark-results-2026-11-14.md)
now records the measured .NET 10/11 differences and their repeatability limits.
Source-level performance changes should use that migrated baseline and carry
their own results.

## Local validation

Validated on Windows x64 with SDK 11.0.100 and runtime 11.0.0:

- Release solution build and standalone sample/project builds passed. The
  solution build reported 39 analyzer warnings and no errors.
- Latest results across 27 solution test suites: **9,035 passed, 0 failed,
  4 skipped**. The skips require opt-in Access, SQL Server, or MySQL fixtures.
  This combines the full run with successful daemon and API reruns. The daemon
  rerun used a shortened process PATH because the machine's inherited PATH
  exceeded Windows batch-file limits. One health test observed an independent
  background refresh under load; all 191 API tests passed on the isolated rerun.
- Separate WAL prototype suite: **179 passed**.
- All **19 release NuGet packages** passed graph validation. Package-only
  observability consumers and EF CRUD, migration generation, SQL generation,
  database update, and reopen checks passed with an isolated local feed.
- SQL Server, MySQL, and Access worker bundle/isolation checks passed. The EF
  migration-tool package, scratch proof, and isolation checks passed in a
  separate run after NativeAOT compilation finished.
- Generated-collection trimmed publish and execution passed. NativeAOT shared
  library publish and native archive SQL smoke passed, including scan, indexed
  lookup, declared types, and reopen.
- All **42 performance-guardrail contract tests** passed, including common-SDK
  provenance, balanced execution order, and rejection of zero benchmark execution.
  A fresh export of `main` also built successfully with the candidate .NET 11
  harness and SDK while its product projects retained their .NET 10 TFMs.
- Documentation, EF package consistency, package closure, PowerShell parsing,
  and whitespace/diff checks passed.

The local ignored `CheckpointControlBenchmark.cs` experiment did not compile
against `main`'s benchmark API. It was preserved at
`.tmp/dotnet11/preserved-scratch/CheckpointControlBenchmark.cs` so the SDK would
not include it in the product build. Other local investigations were retained.

Linux/macOS execution and the complete hosted performance matrix have not been
run locally. The checks above establish migration correctness and packaging;
they do not establish a throughput or latency improvement.
