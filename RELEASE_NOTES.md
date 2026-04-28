# What's New

## v3.6.0

v3.6.0 adds trusted, in-process C# scalar functions across CSharpDB's
user-facing expression surfaces. Host applications can now register C#
delegates when opening or hosting a database, then call those functions from
SQL, SQL-backed triggers and procedures, Admin Forms formulas, Admin Reports
calculated text, and pipeline filter/derive expressions.

### Trusted C# Scalar Functions

- Added the shared `DbFunctionRegistry`, `DbFunctionRegistryBuilder`,
  `DbScalarFunctionDelegate`, and `DbScalarFunctionOptions` public model in
  `CSharpDB.Primitives`.
- Added `DatabaseOptions.Functions` plus `ConfigureFunctions(...)` so embedded
  hosts can register scalar functions when opening file-backed, in-memory, or
  hybrid databases.
- SQL expression evaluation now resolves registered scalar functions in
  projections, filters, ordering expressions, `INSERT`/`UPDATE` expressions,
  trigger bodies, and stored SQL procedure bodies.
- Direct clients can pass trusted functions through `DirectDatabaseOptions`;
  HTTP and gRPC clients still do not serialize delegates and can only call
  functions registered inside the remote host process.
- Admin Forms formulas and Admin Reports calculated expressions can use the
  same registry while preserving existing arithmetic and aggregate behavior.
- Pipeline filter and derived-column expressions can call registered functions;
  package definitions store expressions plus generated automation metadata, but
  never C# function bodies.
- Added the usage guide at `docs/trusted-csharp-functions/README.md`.

### Trusted Commands And Form Events

- Added the shared `DbCommandRegistry`, `DbCommandRegistryBuilder`,
  `DbCommandDelegate`, `DbCommandContext`, `DbCommandResult`, and
  `DbCommandOptions` public model in `CSharpDB.Primitives`.
- Admin Forms can now store form-level event bindings that reference trusted
  command names instead of storing C# source.
- The Forms data-entry runtime dispatches `OnOpen`, `OnLoad`, `BeforeInsert`,
  `AfterInsert`, `BeforeUpdate`, `AfterUpdate`, `BeforeDelete`, and
  `AfterDelete`.
- `BeforeInsert`, `BeforeUpdate`, and `BeforeDelete` can cancel the requested
  write by returning `DbCommandResult.Failure(...)`; after-events report errors
  without attempting to roll back a completed write.
- Command context arguments include current record fields converted to
  `DbValue`; metadata includes the Forms surface, form id/name, table name, and
  event name.
- `AddCSharpDbAdminForms(...)` now has a command-registration overload for
  trusted host applications.
- The Admin Forms designer preserves and edits form-level event bindings
  instead of dropping automation metadata during save.
- Added a command button control that invokes a trusted host command on click,
  passing current record fields, optional configured arguments, and form
  metadata to the command callback.
- Added control-level Admin Forms event bindings for `OnClick`, `OnChange`,
  `OnGotFocus`, and `OnLostFocus`, so ordinary controls can invoke trusted
  host commands without being command buttons.
- The Forms property inspector now edits selected-control event bindings using
  the same registered-command picker and JSON argument editor as form-level
  events.
- Added shared declarative action sequence metadata with `RunCommand`,
  `SetFieldValue`, `ShowMessage`, and `Stop` steps for Admin Forms automation.
  Form and control event bindings can now be command-only,
  action-sequence-only, or a command followed by an action sequence.
- The form-event and selected-control event editors now include a visual
  action-sequence editor for adding, ordering, removing, and configuring
  `RunCommand`, `SetFieldValue`, `ShowMessage`, and `Stop` steps.
- The action-sequence editor uses registered-command pickers when commands are
  available, preserves missing command names for portable form metadata, and
  keeps JSON editing limited to optional argument payloads.
- Action sequences store names, arguments, field targets, and literal values
  only. They do not store C# source, serialize delegates, or run untrusted code.
- Added shared command argument conversion helpers so Forms, Reports, and
  Pipelines pass host command arguments with the same `DbValue` conversion
  rules.
- Admin Reports can now bind `OnOpen`, `BeforeRender`, and `AfterRender`
  preview lifecycle events to trusted commands. The preview service passes
  report/source metadata plus row, truncation, page, and schema-drift metrics.
- `AddCSharpDbAdminReports(...)` now has a command-registration overload for
  trusted host applications.
- Pipeline packages can now include trusted command hooks for `OnRunStarted`,
  `OnBatchCompleted`, `OnRunSucceeded`, and `OnRunFailed`. Package JSON stores
  hook names, arguments, and generated automation metadata only; command bodies
  remain host-registered code.
- Pipeline hook failures fail the run through `PipelineRunResult`; failure-hook
  errors are appended to the failed run summary instead of recursively
  dispatching more failure hooks.

### Stored Automation Metadata

- Added shared `DbAutomationMetadata`, command references, and scalar-function
  references so portable definitions can declare the trusted host callbacks
  they expect without storing C# code.
- Admin Forms, Admin Reports, and pipeline packages now regenerate automation
  metadata during repository save/load or package serialization/deserialization.
  Older JSON without automation metadata is backfilled on read.
- Form metadata captures trusted form events, command buttons, selected-control
  events, action-sequence `RunCommand` steps, and computed-formula scalar
  functions.
- Report metadata captures preview lifecycle command bindings and calculated
  text scalar functions.
- Pipeline package metadata captures command hooks and scalar functions used by
  filter and derived-column expressions; package validation reports stale
  automation manifests so packages can be re-exported.

### Developer Experience

- Added `samples/trusted-csharp-host`, a VS Code-ready C# host project for
  writing and debugging trusted C# callbacks in ordinary application code.
- The sample registers a trusted scalar function, calls it from SQL, registers
  a trusted command, and runs an Admin Forms action sequence that sets a field
  before invoking that command.
- The sample includes local `.vscode` launch/tasks files so developers can open
  the sample folder, press `F5`, and set breakpoints inside callback code.

### Behavior And Safety

- Function names are case-insensitive SQL identifiers, and registration rejects
  duplicate user names or collisions with reserved built-ins such as `TEXT`,
  `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- Arity is validated before invocation, missing SQL functions fail with the
  existing unknown scalar function path, and thrown delegate exceptions are
  wrapped with the function name before normal statement/transaction rollback.
- `NullPropagating = true` returns `NULL` without invoking the delegate when
  any argument is `NULL`; otherwise `DbValue.Null` is passed explicitly.
- V1 remains scalar-only, synchronous, trusted, and in-process. It does not
  persist C# source, sandbox code, load database-owned plugin assemblies,
  marshal delegates over HTTP/gRPC, or add aggregate/table-valued/procedure
  UDFs.
- Query planning keeps custom functions on the residual expression path in V1:
  no index pushdown, generated columns, constant folding, or cost assumptions
  are inferred from user functions.

### Tests And Benchmarks

- Added registry and SQL coverage for case-insensitive lookup, duplicate and
  built-in collision rejection, null propagation, deterministic metadata,
  missing functions, thrown functions, rollback behavior, triggers, and stored
  SQL procedures.
- Added direct-client, Admin Forms, Admin Reports, pipeline validation, and
  pipeline runtime tests for registered scalar functions.
- Added command-registry, form-event dispatcher, event JSON round-trip, and
  Forms data-entry tests for create/update/delete event dispatch and
  before-event cancellation.
- Added designer-state tests for action sequences, plus command-button and
  control-event tests covering event binding preservation and registered
  command invocation from rendered forms.
- Added Forms action-sequence tests for event dispatch, mutable record updates,
  command button action-only clicks, and JSON round-tripping.
- Added report-event dispatcher and preview lifecycle tests, pipeline hook
  serialization/validation/orchestrator tests, and shared command argument
  conversion tests.
- Added automation metadata tests covering manifest extraction, JSON
  round-tripping, repository persistence/backfill, pipeline package
  import/export, and stale package metadata validation.
- Same-machine affected benchmark comparison against the pre-feature HEAD
  baseline showed no material regression in the main write/query guardrails:

| Suite | Worst current change | Best current change |
|-------|---------------------:|--------------------:|
| Insert | `+3.76%` | `-3.38%` |
| Join | `+6.65%` | `-6.93%` |
| PointLookup | `+5.15%` | `-9.25%` |
| QueryPlanCache | `+1.62%` | `-4.45%` |
| ScanProjection | `+0.20%` | `-18.12%` |
| TriggerDispatch | `+0.77%` | `-4.52%` |
| BatchEvaluation | `+10.53%` | `-10.36%` |

The one notable row was the synthetic BatchEvaluation delegate
filter/projection case at `+10.53%`; its paired specialized path improved by
`-10.36%`, allocations were unchanged, and the affected guardrail suites were
otherwise neutral to improved.

### Validation

- `git status --short --branch`
- `dotnet restore CSharpDB.slnx`
- `.\scripts\Test-NoLegacyCoreReferences.ps1`
  - Passed through the script's PowerShell fallback after the local packaged
    `rg.exe` could not be launched normally in this desktop environment.
- `dotnet build CSharpDB.slnx -c Release --no-restore`
  - Passed with `0` warnings and `0` errors.
- `dotnet test CSharpDB.slnx -c Release --no-build -m:1 -- RunConfiguration.DisableParallelization=true`
  - Non-parallel unit test run passed with `1,663` tests.
- Phase 5 local validation used `dotnet build CSharpDB.slnx --no-restore -m:1`
  and `dotnet test CSharpDB.slnx --no-build -m:1 -- RunConfiguration.DisableParallelization=true`
  - Debug non-parallel unit test run passed with `1,703` tests after adding
    automation metadata coverage.
- `dotnet pack` smoke for the release workflow packages with
  `-p:Version=3.6.0`
  - Produced `11` local packages:
    `CSharpDB`, `CSharpDB.Client`, `CSharpDB.Data`, `CSharpDB.Engine`,
    `CSharpDB.EntityFrameworkCore`, `CSharpDB.Execution`,
    `CSharpDB.Pipelines`, `CSharpDB.Primitives`, `CSharpDB.Sql`,
    `CSharpDB.Storage`, and `CSharpDB.Storage.Diagnostics`.
- `.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.6.0 -Runtime win-x64 -OutputRoot artifacts\daemon-release-local`
  - Produced `csharpdb-daemon-v3.6.0-win-x64.zip` and `SHA256SUMS.txt`.

### Review Notes

- The highest-risk runtime changes are in expression evaluation and planner
  plumbing: custom functions are intentionally kept off the index-pushdown and
  batch-fast-path planning assumptions in V1.
- Remote hosts must register functions in the daemon/API host process; direct
  clients can register functions locally through `DirectDatabaseOptions`, but
  callback delegates are never serialized over HTTP or gRPC.
- Admin Forms and Reports use the shared registries, but their formula and
  automation surfaces remain narrower than SQL or stored macro systems:
  formulas stay expression-focused, command hooks invoke host-owned code by
  name, and declarative action sequences store only limited action metadata
  rather than executable scripts in database metadata.

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
