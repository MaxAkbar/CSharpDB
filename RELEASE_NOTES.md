# What's New

## v3.5.0

v3.5.0 focuses on the collection binary payload fast path, generated
collection codec performance, targeted UTF-8 span plumbing, and refreshed
release benchmark publishing. It also includes the confirmed CSharpDB Studio
admin UI access-parity notes and static mockups that are part of this branch.

### Collection Binary Payload Fast Path

- Added the opt-in source-generated collection fast path for fixed generated
  field order, compact type/null metadata, and raw value payloads.
- Existing non-generated collection paths continue to use their current JSON
  and binary document behavior.
- Generated collection models can now use direct binary record encode/decode
  instead of routing through the slower document-shaped path.
- `CollectionDocumentCodec<T>` now parses direct binary payload headers once
  and decodes the key/document from that parsed header.
- `CollectionBinaryDocumentCodec` now has single-segment
  `ReadOnlySpan<byte>` lookup overloads for top-level binary document fields.
- `CollectionPayloadCodec` fast header parsing now favors the common binary
  payload marker path.
- Collection field and index bindings now expose generated accessors to faster
  direct field-reader paths where available.

### UTF-8 Text Index And Compare Paths

- Added targeted UTF-8 span plumbing for collection text index/read/compare
  paths.
- Reduced transient allocations in top-level string property reads by avoiding
  per-call `byte[]` and `byte[][]` path materialization where the
  single-segment path applies.
- Updated ordered text index key comparison coverage.

### Tests And Benchmarks

- Added `GeneratedCollectionCodecBenchmarks`.
- Expanded generated collection model tests around binary payload support.
- Expanded binary document codec tests for direct field access.
- Added ordered text index key codec coverage.
- Refreshed `tests/CSharpDB.Benchmarks/README.md` and
  `release-core-manifest.json` from the April 26, 2026 release-core artifacts.
- Recorded the collection binary payload investigation, noisy initial guardrail
  compare, focused retry, and final passing release guardrail in
  `tests/CSharpDB.Benchmarks/HISTORY.md`.

Focused collection investigation:

| Metric | Result |
|--------|-------:|
| Matched rows vs same-machine HEAD baseline | `60` |
| Faster matched rows | `50` |
| Slower matched rows | `10` |
| Median matched speedup | `+4.1%` |
| Mean matched speedup | `+4.8%` |

Focused recovery highlights:

| Benchmark | Before | After | Change |
|-----------|-------:|------:|-------:|
| Collection field read, missing field | `223.22 ns` | `136.73 ns` | `+38.7%` |
| Collection decode, direct payload | `333.80 ns` | `155.20 ns` | `+53.5%` |
| Collection field read, early field | `108.60 ns` | `49.77 ns` | `+54.2%` |
| Collection field compare, late text field | `159.55 ns` | `97.60 ns` | `+38.8%` |
| Collection field compare, bound accessor | `128.03 ns` | `92.83 ns` | `+27.5%` |

Path-index follow-up highlights:

| Benchmark | Final vs same-machine HEAD baseline |
|-----------|------------------------------------:|
| Nested path equality via `FindByIndex` | `+53.6%` |
| Nested path equality via `FindByPath` | `+55.9%` |
| Array path equality via `FindByIndex` | `+52.1%` |
| Text range path lookup | `+42.7%` |
| Guid equality path lookup | `+59.4%` |

Published scorecard examples from the refreshed benchmark README:

| Area | Result |
|------|-------:|
| SQL file-backed single insert | `450.4 ops/sec` |
| SQL file-backed batch x100 | `41.88K rows/sec` |
| Collection file-backed put | `447.3 ops/sec` |
| Collection file-backed batch x100 | `42.28K docs/sec` |
| Collection hot point get | `1.60M ops/sec` |
| CSharpDB InsertBatch B1000 | `233.06K rows/sec` |

### CSharpDB Studio Admin UI Notes

- Added admin forms access-parity notes.
- Added admin reports access-parity notes.
- Added static CSharpDB Studio admin UI mockups under `www/admin-ui-mockups`.
- Included dashboard, data, query, heavy operations, reports designer, mobile
  forms/reports, command palette, sidebar, and shared styling mockups.

### Validation

- `dotnet build CSharpDB.slnx -c Release --no-restore`
- `dotnet test CSharpDB.slnx -c Release --no-build -m:1 -- RunConfiguration.DisableParallelization=true`
- Non-parallel unit test run passed with `1,652` tests.
- `dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --release-core --repeat 3 --repro`
- `pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Run-Perf-Guardrails.ps1 -Mode release`
- `pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Compare-Baseline.ps1 -ThresholdsPath .\tests\CSharpDB.Benchmarks\perf-thresholds.json -CurrentMicroResultsDir .\tests\CSharpDB.Benchmarks\results\.tmp-current-micro-run -ReportPath .\tests\CSharpDB.Benchmarks\results\perf-guardrails-last.md`
- `pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Update-BenchmarkReadme.ps1 -RunManifest .\tests\CSharpDB.Benchmarks\release-core-manifest.json`
- `Get-Content -Raw .\tests\CSharpDB.Benchmarks\release-core-manifest.json | ConvertFrom-Json`
- `pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Update-BenchmarkReadme.ps1 -RunManifest .\tests\CSharpDB.Benchmarks\release-core-manifest.json -DryRun`
- `git diff --check -- tests\CSharpDB.Benchmarks\README.md tests\CSharpDB.Benchmarks\HISTORY.md tests\CSharpDB.Benchmarks\release-core-manifest.json`

Release benchmark guardrail result:

```text
Compared 185 rows against baseline. PASS=185, WARN=0, SKIP=0, FAIL=0
```

### Review Notes

- The highest-risk runtime changes are in the generated collection model and
  collection payload codec paths: `CollectionModelGenerator`,
  `CollectionPayloadCodec`, `CollectionBinaryDocumentCodec`,
  `CollectionDocumentCodec<T>`, `CollectionIndexedFieldReader`, and collection
  index binding.
- The generator diff is intentionally large because generated code now owns the
  binary record encode/decode shape for opt-in generated models.
- The benchmark README generated region should be edited through
  `release-core-manifest.json` plus `scripts/Update-BenchmarkReadme.ps1`, not
  manually.
