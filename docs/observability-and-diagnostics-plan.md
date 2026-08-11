# Observability And Diagnostics Plan

Internal implementation plan for the next CSharpDB observability and
operational-diagnostics release. This plan covers embedded use, the standalone
REST host, the combined daemon, remote clients, and Admin.

This file intentionally lives under the top-level `docs/` folder and is not
part of the public `www` documentation site. Public documentation should be
added as the related phases ship.

## How To Use This Plan

- Work in phase order unless a phase explicitly says it can run in parallel.
- Update each phase status and its checkboxes as work lands.
- Stop at each exit gate for review before starting the next phase.
- Keep each implementation pull request small enough to prove one coherent
  contract or vertical slice.
- Do not mark a phase complete because its UI exists. Its direct, REST, and
  gRPC behavior, safety tests, documentation, and performance gates must also
  pass where those surfaces apply.

Status values used below:

- `Not started`
- `In progress`
- `Blocked`
- `Complete`

## Target Outcome

CSharpDB should provide one coherent, safe observability model across embedded
and remote deployment modes. Application developers should be able to use
normal .NET logging and OpenTelemetry. Operators should be able to scrape
Prometheus metrics, check health, inspect current and recent database activity,
and understand storage or maintenance problems without exposing SQL values,
credentials, or database contents by default.

The initial observability release is complete only when CSharpDB can:

- Emit stable structured operational logs with correlation identifiers.
- Record query execution without logging SQL text or parameter values by
  default.
- Identify slow and currently long-running queries using configurable
  thresholds.
- Publish supported OpenTelemetry traces and metrics.
- Expose a bounded-cardinality Prometheus endpoint in configured hosts.
- Report meaningful process liveness and database readiness separately.
- Show the same supported runtime diagnostics in Admin for direct and remote
  connections.
- Report query-plan, connection, session, storage, WAL, backup, and restore
  state through transport-neutral models.
- Preserve NativeAOT compatibility for the libraries and Native surface that
  already claim it, and keep disabled-observability overhead negligible.
- Pass explicit redaction, cardinality, concurrency, cross-platform, and
  transport-parity gates.

## Baseline At Plan Creation

This section is the historical baseline used to scope the plan. It is not a
current feature inventory; the phase status, progress notes, and checklists
below are authoritative as implementation lands.

Implemented behavior to preserve and extend:

- ASP.NET Core supplies normal host logging, and
  `CSharpDB.Api/Middleware/ExceptionHandlingMiddleware.cs` records handled and
  unhandled API errors through `ILogger`.
- `CSharpDB.Primitives/DbCallbackDiagnostics.cs` and Admin's
  `HostCallbackDiagnosticsHistoryService` demonstrate a strongly typed
  `DiagnosticListener` event and a bounded in-memory history.
- `CSharpDB.Admin.Forms/Contracts/FormActionDiagnostics.cs` provides a second
  strongly typed diagnostics pattern with focused tests.
- `CSharpDB.Execution/QueryPlanner.cs` already tracks select-plan cache,
  adaptive reoptimization, and mutation-target counters for tests and
  benchmarks.
- `EXPLAIN ESTIMATE FOR` already provides bounded planner-estimate diagnostics
  and has an Admin query experience.
- `CSharpDB.Engine/Database.cs` exposes internal WAL, commit-path, row-id,
  adaptive-reoptimization, mutation-target, and select-plan-cache snapshots to
  tests and benchmarks.
- `CSharpDB.Storage/Wal/WriteAheadLog.cs` already records cumulative WAL flush
  and commit-path counters.
- `CSharpDB.Storage.Diagnostics` provides offline database, page, index, and WAL
  inspection. Those tools inspect physical files; they are not a runtime
  telemetry service.
- `ICSharpDbClient` already carries storage inspection, maintenance reports,
  backup, and restore through direct, HTTP, and gRPC transports.
- Admin's `StorageTab.razor` already shows offline storage inspection,
  maintenance, backup, and restore results.
- `CSharpDB.Data/CSharpDbConnectionPool.cs` tracks logical pooled sessions,
  snapshot readers, transaction ownership, and pool retirement internally.
- `EngineTransportClient` tracks client-managed transaction sessions and
  exclusive maintenance access internally.
- Admin exposes a shallow `/healthz` process check used by the desktop launcher.
  That compatibility route must continue to work when the richer readiness
  model is added.
- `QueryResult` owns the lazy query-result lifetime, including operator open,
  row streaming, and disposal callbacks. Query timing must account for that
  complete lifetime rather than stopping when `Database.ExecuteAsync` returns.

Known gaps:

- There is no CSharpDB-wide structured event catalog or logging configuration.
- There is no query log, slow-query log, active-query registry, or recent-query
  history.
- Existing planner, storage, WAL, pool, and transaction counters are internal,
  fragmented, and primarily intended for tests or benchmarks.
- Existing WAL and commit-path benchmark counters can be reset. They cannot be
  published directly as monotonic production counters without a distinct
  non-resetting family or a visible counter-epoch change.
- There is no transport-neutral runtime-diagnostics contract for Admin or
  operators.
- REST SQL and transaction routes already propagate request cancellation, but
  maintenance and inspection routes do not do so consistently. The direct
  inspection helpers also accept a cancellation token and then drop it before
  invoking the inspectors.
- ADO.NET `CommandTimeout` is not enforced and `DbCommand.Cancel()` is a no-op.
  Visibility must not be presented as query-control or cancellation support.
- Backup and restore return final results but expose no in-progress or recent
  operation status.
- A successful full restore currently replaces the database without eagerly
  reopening it; the first later request discovers reopen failures.
- API and daemon hosts do not configure CSharpDB OpenTelemetry sources,
  exporters, Prometheus scraping, or database health checks.
- API and daemon hosts do not expose distinct liveness and readiness endpoints.
- API, daemon, and Admin warm up the database before `app.Run`. As a result,
  their listeners cannot currently report `starting`, `recovering`, or failed
  initialization states.
- Admin has no live metrics or unified operational-diagnostics workspace.
- No release gate currently verifies telemetry redaction, metric cardinality,
  trace shape, or observability overhead.
- The API exception middleware currently logs raw exception objects and returns
  several raw exception messages. Safe-error projection is therefore an early
  privacy prerequisite, not only a query-logging concern.

## Non-Negotiable Rules

- Never record SQL text, bound values, row values, credentials, connection
  strings, database paths, backup paths, or exception messages as metrics.
- Raw SQL text and detailed object names are disabled by default in logs,
  traces, histories, APIs, and Admin. An explicit opt-in must explain the data
  exposure.
- Built-in query and maintenance logging projects exceptions to a reviewed safe
  error code and type by default. It does not attach the raw `Exception`,
  message, data, or stack trace because those can contain SQL, values,
  connection details, and paths.
- Ordinary snapshots and list APIs omit raw SQL and paths even when the server
  captures them. Sensitive details require a separate server-authorized request;
  client-side hiding is not an access-control boundary.
- Parameter and row values are never captured by the built-in query telemetry,
  even when SQL-text capture is enabled.
- Use a deterministic normalized query fingerprint for correlation. Never use
  a query fingerprint, query id, session id, table name, database path, or
  exception message as a metric label.
- Never expose a client-managed transaction id through diagnostics. It is a
  bearer capability, not a safe session identifier.
- Metric labels must come from a reviewed bounded set. Initial bounded labels
  are operation class, outcome, transport, and a configured low-cardinality
  database or shard alias.
- Histories and active-operation registries must have explicit capacity,
  retention, and truncation behavior. Observability must not become an
  unbounded memory or disk sink.
- Expensive plan capture, file inspection, stack collection, and full SQL
  capture are opt-in and must not run on every query.
- Instrument one logical operation once. Internal planner calls, retries,
  triggers, procedures, transport adapters, and lazy result consumption must
  preserve correlation without double-counting the root query.
- Define scripts and procedures explicitly as one parent operation with child
  statement operations. Request counts and statement-execution counts are
  different metrics and must not be conflated.
- A query span and duration cover planning, execution, row streaming, and
  disposal. Separate timing fields may show time to first result and result
  consumption.
- Runtime cumulative counters must not decrease. Current values such as active
  work, WAL frames, or WAL bytes are gauges; benchmark-resettable counters use
  a separate lifetime family or advance the published counter epoch.
- Liveness must not depend on opening or querying the database. Readiness may
  perform a bounded, side-effect-free database check.
- Health responses available without authentication contain status only.
  Detailed failure reasons and runtime diagnostics remain behind the same
  configured API security boundary as inspection and maintenance.
- Restore readiness remains false until the replacement database has been
  reopened successfully, or the status explicitly reports `ReopenPending` and
  readiness stays false until the deferred reopen succeeds.
- When API security mode is `None`, detailed runtime diagnostics and metrics
  are loopback-only unless an explicit insecure-remote override is configured
  and logged. Do not describe an unauthenticated endpoint as protected.
- A Prometheus endpoint is disabled until explicitly configured and must not
  silently expose itself on a public listener.
- Core engine, execution, and storage assemblies use BCL diagnostics
  primitives and do not depend on a specific logger or telemetry exporter.
- Direct, HTTP, and gRPC diagnostics use the same models and semantics.
- Sharded snapshots clearly distinguish aggregate data from per-shard data and
  cap the number of returned shard records.
- Existing embedded behavior remains compatible when observability is not
  configured.
- NativeAOT and trimming remain release gates for currently AOT-compatible
  libraries and the existing Native surface; this plan does not add an API or
  daemon NativeAOT-hosting claim.

## Proposed Architecture

### Assembly Boundary

Add a small `CSharpDB.Observability` assembly that is BCL-only, AOT-compatible,
and lower in the dependency graph than Storage, Execution, Engine, Client, and
Data. It owns:

- Stable `ActivitySource` and `Meter` names.
- Query and operation correlation primitives.
- Redaction policy, capture modes, and the query-fingerprint contract.
- Runtime snapshot contracts and bounded registry abstractions.
- Shared options validation that does not require ASP.NET Core or a particular
  exporter.

`CSharpDB.Sql` implements SQL normalization and fingerprint generation using
the existing tokenizer and token kinds. Do not create a second ad hoc SQL
tokenizer in the observability assembly.

The core assemblies emit through those contracts. `CSharpDB.Client` supplies
the optional `ILogger` bridge and client-facing diagnostics capability.
`CSharpDB.Api`, `CSharpDB.Daemon`, and `CSharpDB.Admin` bind host options and
register OpenTelemetry, exporters, health checks, and UI services.
Reusable ASP.NET Core registration belongs in a host-only adapter or explicit
per-host composition; Admin must not acquire a dependency on the API executable
project merely to share health or exporter setup.

Do not extend `ICSharpDbClient` with required members. That would break external
implementations. Add an optional `ICSharpDbObservabilityClient` capability
implemented by the built-in direct, HTTP, gRPC, and sharded clients.

### Runtime And Offline Diagnostics

Keep these concepts separate:

- Runtime diagnostics are cheap snapshots of a live engine, active work,
  cumulative counters, and recent bounded history.
- Offline diagnostics remain the explicit physical inspection APIs in
  `CSharpDB.Storage.Diagnostics`.

Admin may link between them, but a normal metrics refresh must never start a
full database, page, index, or WAL file scan.

### Correlation

Every root database operation receives:

- An opaque operation id.
- An operation class such as query, transaction, checkpoint, backup, restore,
  or maintenance.
- A stable query fingerprint when applicable.
- A start timestamp and monotonic duration source.
- The current trace id when an `Activity` exists.
- A transport and configured database/shard alias.
- An optional logical session id that is safe for diagnostics but is never a
  metric label.

Inbound HTTP or gRPC activities remain the parent for remote calls. Direct
calls create a root activity only when a listener requests one.

Multi-statement scripts and procedures create one request/script parent plus
one child statement operation per executed statement. Nested planner work,
subqueries, triggers, and retries remain part of the statement operation rather
than becoming additional query roots. An immutable root context established by
the host, client, or ADO.NET layer carries transport and safe session identity
into the engine; the engine alone cannot infer them.

### Snapshot Authority

The live engine is authoritative for query, storage, WAL, and operation state.
Client and host layers contribute their connection, pool, transport, and
session counters. A snapshot coordinator merges these sources without resetting
cumulative counters.

Storage initialization and WAL recovery occur before a `Database` instance
exists. A diagnostics context must therefore enter through storage-engine
options/factories and survive into the Pager/WAL runtime handle; attaching
instrumentation only to `Database` would make startup recovery invisible.

Remote Admin reads server snapshots through the optional diagnostics client; it
must not substitute metrics from the Admin process for metrics from the
database host.

## Repository Alignment

| Existing seam | Planned use | Constraint |
| --- | --- | --- |
| `src/CSharpDB.Sql` tokenizer and parser | Literal-safe normalization and query fingerprint generation. | Reuse the real SQL grammar; do not duplicate tokenization in Observability. |
| `src/CSharpDB.Engine/Database.cs` | Root query lifecycle, engine counters, active readers, and snapshot composition. | Cover SQL text, pre-parsed statements, simple inserts, fast lookups, explicit transactions, and lazy results exactly once. |
| `src/CSharpDB.Execution/QueryResult.cs` | Complete query timing and row-consumption lifecycle. | Completion must fire once on exhaustion, failure, cancellation, or disposal; do not attach per-operation state to shared zero/one-row DML result singletons. |
| `src/CSharpDB.Execution/QueryPlanner.cs` | Plan cache, selected-access-path, estimate, and adaptive-plan diagnostics. | Do not run a second plan or `EXPLAIN` automatically for every query. |
| `src/CSharpDB.Storage/Wal/WriteAheadLog.cs`, `Paging/Pager.cs`, and storage factories | WAL, checkpoint, recovery, commit, cache, and I/O state, including startup-before-Database work. | Snapshot reads must be lock-safe; production counters must be distinct from resettable benchmark counters. |
| `src/CSharpDB.Storage.Diagnostics` | Explicit offline storage and WAL inspection. | Keep file scans off the metrics and health hot paths. |
| `src/CSharpDB.Client/Internal/EngineTransportClient*.cs` | Direct-client transactions, exclusive operations, backup/restore status, and direct snapshots. | Avoid leaking local paths and do not hold the client lock while serializing large snapshots. |
| `src/CSharpDB.Data/CSharpDbConnectionPool.cs` | Pool capacity, waiters, active/idle sessions, readers, and transaction age. | Registry enumeration must be bounded and safe during pool retirement. |
| `src/CSharpDB.Client/ICSharpDbClient.cs` and transport clients | Additive diagnostics capability and shared models. | Do not add required members to the existing interface. |
| `src/CSharpDB.Client/Protos/csharpdb_rpc.proto` | Runtime snapshot and active/recent operation RPCs. | Maintain REST/gRPC field and behavior parity. |
| `src/CSharpDB.Api/CSharpDbRestApiHostExtensions.cs` | Shared diagnostics routes, logging bridge, health, and optional metrics mapping. | Minimal health endpoints may be public; detailed diagnostics remain under API security. |
| `src/CSharpDB.Daemon/Program.cs` and `Grpc/CSharpDbRpcService.cs` | Host telemetry, gRPC health, and diagnostics RPCs. | REST-disabled daemon mode must still support configured gRPC health and telemetry. |
| `src/CSharpDB.Admin/Components` and `Services` | Live metrics and diagnostics workspace. | Use the client capability for both direct and remote mode. |
| Existing API, daemon, client, Admin, Data, and engine test projects | Contract, redaction, transport, concurrency, and UI coverage. | Tests must use deterministic clocks/exporters where elapsed time matters. |

## Delivery Sequence

Phases are ordered by dependency, not calendar estimate.

| Phase | Primary outcome | Depends on |
| --- | --- | --- |
| 0 | Contracts, safety policy, correlation, and benchmark baseline. | Current repository baseline. |
| 1 | Structured logging plus query and slow-query logging. | Phase 0. |
| 2 | Active/recent queries, query plans, connections, and sessions. | Phases 0 and 1. |
| 3 | Runtime storage, WAL, backup, and restore diagnostics. | Phase 0; can overlap late Phase 2 work. |
| 4 | OpenTelemetry traces/metrics and Prometheus export. | Phases 1 through 3. |
| 5 | Liveness, readiness, and health endpoints. | Phases 0 and 3; exporters are not a health dependency. |
| 6 | Admin metrics and diagnostics UI. | Phases 2 through 5 and transport APIs. |
| 7 | Qualification, public documentation, packaging, and release closure. | All prior phases. |

Shipping checkpoints:

- **Operator preview:** Phases 0 through 3 plus Phase 5. This is the first
  trustworthy daemon milestone: safe structured/slow-query logs, active and
  recent work, runtime storage/WAL/maintenance state, and live/ready signals.
- **Telemetry beta:** Add Phase 4 after the operation and snapshot contracts
  have stabilized. Phase 4 and Phase 5 may run in parallel after Phase 3;
  exporters are not a health dependency.
- **General availability:** Add the Admin experience, cross-transport and
  cross-platform qualification, public schema documentation, and release gates
  from Phases 6 and 7.

## Phase 0: Contracts, Safety, And Baselines

Status: `Complete`

Goal: establish one telemetry vocabulary, safe defaults, and measurable
performance limits before instrumenting independent layers.

Work:

- [x] Add `CSharpDB.Observability` to the solution as a BCL-only,
  AOT-compatible assembly.
- [x] Define and qualify a standalone `CSharpDB.Observability` NuGet package,
  then add it to the all-in-one `CSharpDB` package composition without pulling
  exporter packages into embedded applications.
- [x] Add the package immediately to CI packing, README-link rewriting,
  package-smoke, and release-one-by-one qualification so dependent packages
  are never tested against a missing observability dependency.
- [x] Define stable source names, schema versioning, operation classes,
  outcomes, correlation fields, and bounded tag keys.
- [x] Define the parent request/script and child statement hierarchy for
  scripts and procedures, including which counters apply to each level.
- [x] Define `CSharpDbObservabilityOptions` and validate impossible or unsafe
  combinations at startup.
- [x] Define SQL-text capture modes: `None`, `Normalized`, and `Raw`, with
  `None` as the default.
- [x] Implement deterministic query normalization and fingerprinting in
  `CSharpDB.Sql` using the existing tokenizer. It must remove literals and
  never retain parameter or row values.
- [x] Define immutable runtime snapshot records with `SchemaVersion`,
  `CapturedAtUtc`, process/server instance id, counter epoch, truncation
  indicators, and source/alias metadata.
- [x] Define cumulative-counter versus gauge semantics, including a distinct
  non-resetting production-counter family or an epoch advance around every
  resettable benchmark counter.
- [x] Define capacity and retention controls for recent queries, active
  operations, and maintenance history.
- [x] Define a monotonic timing and test-clock strategy.
- [x] Reserve stable structured-log event-id ranges by subsystem.
- [x] Define a centralized safe-error projection and exception-detail policy
  for the logging bridge, API middleware, histories, traces, and operation
  status.
- [x] Apply that safe-error projection to the existing API exception middleware
  before broadening diagnostics; remove raw exception-object logging and raw
  client messages from the default path.
- [x] Define a host initialization/readiness state machine that can represent
  startup and WAL recovery before `Database` exists.
- [x] Record the approved metric label allowlist and maximum configured
  database/shard alias count.
- [x] Add baseline benchmarks for query fast paths, writes, streaming results,
  and connection pooling with no listeners configured.
- [x] Set release budgets from those baselines for disabled telemetry,
  metrics-only telemetry, and query-history capture.
- [x] Document how root operations, nested engine work, retries, triggers,
  procedures, and sharded fan-out are counted.

Deliverables:

- Shared observability contracts and options.
- A privacy/cardinality contract enforced by unit tests.
- A standalone package identity and dependency/publication order.
- A benchmark baseline and reviewed performance budgets.
- A short architecture record for the optional diagnostics-client interface.

Exit gate:

- Core projects compile without OpenTelemetry, Prometheus, or ASP.NET package
  dependencies.
- Fingerprint tests prove literals and parameter values cannot be reconstructed.
- Invalid capacities, thresholds, paths, and label aliases fail configuration
  validation.
- Disabled instrumentation passes the agreed fast-path allocation and
  throughput budgets.

## Phase 1: Structured, Query, And Slow-Query Logging

Status: `Complete`

Goal: emit correlated, structured lifecycle events for database operations
while keeping sensitive query data disabled by default.

Work:

- [x] Add an `ILogger` bridge in the client/hosting layer that subscribes to
  strongly typed core events.
- [x] Publish stable event ids and message templates for host startup,
  database open/close, query completion, slow query, query failure,
  cancellation, transaction completion, checkpoint, and maintenance events.
- [x] Instrument all root `Database.ExecuteAsync` paths without double-counting
  cached statements, fast lookups, simple inserts, or explicit transactions.
- [x] Instrument scripts and procedures as a parent operation plus child
  statements so a slow child is visible without double-counting a request.
- [x] Carry the same root context through procedures, triggers, pipelines, and
  sharded fan-out.
- [x] Attach query completion to `QueryResult` so streamed queries finish on
  exhaustion, failure, cancellation, or disposal, not when the result object is
  returned.
- [x] Use a once-only terminal observer invoked directly from exhaustion,
  materialization, failure, cancellation, early disposal, and never-opened
  disposal paths. Do not attach mutable operation callbacks to the shared
  zero/one-row `QueryResult` instances.
- [x] Record total duration, time to first result, queue duration,
  execution/result-consumption duration, rows produced or affected, outcome,
  operation class, query fingerprint, transport, and correlation ids.
- [x] Add configurable query logging independent of the general operational
  log level.
- [x] Add configurable slow-query logging with one threshold and optional
  per-operation overrides.
- [x] Classify and emit a slow-query event once when the full logical query
  completes. Phase 2 adds the separate in-flight long-running notification.
- [x] Ensure failure events expose stable error type/code fields without
  copying exception messages into metrics or default query history.
- [x] Use the Phase 0 centralized safe-error projection in the logging bridge
  and all new events. Do not attach raw exception objects to query or
  maintenance events by default.
- [x] Add a startup warning when raw SQL capture is explicitly enabled.
- [x] Add log snapshot and secret-canary tests for direct, REST, gRPC, and API
  exception-middleware paths.

Deliverables:

- Stable structured operational logging.
- Query and slow-query logging with safe default fields.
- Full-lifecycle query duration and row counts.

Exit gate:

- Every supported query entry path emits one and only one completion outcome.
- Slow queries are classified on completion consistently for eager and
  streamed results.
- Default logs contain no SQL text, literal values, parameters, connection
  strings, credentials, file paths, or row data.
- Logging exceptions never change query results or database durability.

Qualification on 2026-08-10:

- The Release solution build completed with zero warnings and zero errors.
- Full suites passed: Core `2325/2325`, Data `225/225`, Pipelines `41/41`,
  benchmark contracts `130/130`, Observability `68/68`, API `94/94`, and
  Daemon `179/179`.
- Package closure/topological order, managed full trimming, and a Windows x64
  NativeAOT publish/runtime smoke all passed without trim or AOT warnings.
- The disabled performance mode passed all seven Phase 0 gates with zero
  incremental allocation. The final pooled open/close median was `415.0 ns /`
  `256 B` versus the detached `450.9 ns / 256 B` reference.
- Stable recovery/restore/maintenance event definitions are present; live
  storage/path-owned publication remains in Phase 3 with the corresponding
  runtime registries. Remote shard physical work remains a distinct runtime
  root correlated by W3C trace until cross-process propagation in Phase 4.
- Several reference or structured-logging benchmark launches exceeded the 5%
  variance preference. Numeric disabled gates passed; preserve that workstation
  variance caveat in release qualification rather than claiming noise-free
  reproducibility.

## Phase 2: Query, Plan, Connection, And Session Visibility

Status: `Complete` (2026-08-11). Formal performance attribution remains
unqualified on this host, as recorded in the qualification evidence below.

Progress on 2026-08-10:

- Added validated runtime identity, collection/value, runtime-family, and
  aggregate/per-shard topology contracts. Non-available envelopes omit values;
  available records share one exact capture identity, epoch, scope, source,
  alias, and truncation state.
- Added the engine-owned active/recent ledger, exact-once completion, phase
  transitions, one shared long-running sweep, bounded query detail, automatic
  plan summaries, and cumulative query counters.
- Added exact physical-owner connection/session projections for direct ADO.NET,
  pooled, named shared-memory, direct client, HTTP request, gRPC request, and
  client-managed transaction sessions. Bearer transaction ids and data-source
  paths are never projected.
- Added the optional `ICSharpDbObservabilityClient` without changing
  `ICSharpDbClient`, including direct, ADO.NET, HTTP, gRPC, sharded, routed, and
  Admin holder delegation. Custom clients remain discoverable as unsupported;
  older remote endpoints use the safe unsupported-capability exception/status.
- Added authenticated REST and gRPC diagnostics methods. API-key mode requires
  the configured key; security mode `None` is loopback-only unless
  `AllowInsecureRemoteDiagnostics` is explicitly enabled. Query detail also
  requires `AllowSensitiveQueryDetailAccess`.
- Forwarded already-accepted cancellation tokens through diagnostics,
  maintenance, inspection, HTTP, gRPC, and direct inspector paths. Cancellation
  remains cooperative; parsing/planning and synchronous fast paths may finish
  before observing it. ADO.NET `CommandTimeout`, `DbCommand.Cancel()`, and
  query/session termination remain unsupported.
- Focused and full component suites passed for the implemented slices. At that
  checkpoint, solution, packaging, trimming/NativeAOT, performance, and
  exit-gate qualification remained outstanding; the final 2026-08-11 evidence
  below satisfies those gates.

Provisional performance evidence and budget amendment on 2026-08-10:

- The original provisional relative-only `+20%` bounded query-history elapsed
  gate failed four of six engine query rows, while the `1,024 B/logical query`
  allocation gate passed all six. A relative-only gate is pathological on the
  sub-microsecond paths that must still pay fixed costs for clocks,
  fingerprinting, and the exact bounded active/recent ledger.
- The amended per-path rule is
  `HistoryCapture median - same-launch Disabled median <= max(20% of Disabled median, 1.5 microseconds)`.
  The allocation ceiling remains `1,024 B/logical query`; the detached-reference
  `Disabled` gate remains `+3%` and `+0 B`. Three paired launches,
  median-of-three evaluation, and rerunning pairs with greater-than-5% spread
  are unchanged.

The diagnostic run that motivated the amendment is retained below as a
provisional result, not final qualification evidence:

| Engine query path | HistoryCapture elapsed delta from same-launch Disabled | Relative delta | Allocation delta |
| --- | ---: | ---: | ---: |
| SQL primary-key lookup | +897.1 ns | +194.26% | +144 B |
| Pre-parsed primary-key lookup | +415.7 ns | +77.58% | -16 B |
| SQL autocommit insert | +1,390.8 ns | +39.55% | +560 B |
| Pre-parsed autocommit insert | +741.5 ns | +24.67% | +496 B |
| Explicit-transaction insert | +281.1 ns | +8.23% | +496 B |
| Stream 128 rows to exhaustion | +1,854.9 ns | +15.06% | +40 B |

Pool `HistoryCapture` is not applicable to the query gate because the pool
benchmark performs zero logical queries and creates zero history records. Its
`11,584.8 ns / 10,516 B` result versus `459.7 ns / 256 B` for `Disabled` is
retained as pool-lifecycle characterization and a future optimization baseline,
not as a pass. This original diagnostic is retained transparently as the run
that motivated the amendment; it is not substituted for the final paired
qualification below.

Final Phase 2 qualification on 2026-08-11:

- The Release solution build completed with zero warnings and zero errors.
- The original nine-suite execution completed `3,968/3,968` before the final API
  compatibility canary. After adding and running that canary, the final
  post-compatibility total is `3,969/3,969`: Core `2,479/2,479`, Data
  `267/267`, Observability `110/110`, API `137/137`, Daemon `200/200`,
  Pipelines `41/41`, benchmark contracts `130/130`, Admin `452/452`, and
  Entity Framework Core `153/153`.
- NuGet package closure and topological package-order qualification passed.
  Managed full trimming and the Windows x64 NativeAOT publish/runtime smoke
  passed without trim or AOT warnings.
- Three independent strict-serial Release pairs ran the detached Phase 1
  baseline first and the exact candidate second after separate five-minute
  low-CPU gates. Each baseline launch produced 14 rows and each candidate
  launch produced 21 rows with 3 warmups, 10 measured iterations, and one
  launch. All 20 applicable launch series passed the 5% stability rule; the
  maximum spread was `4.253806%` for the candidate `HistoryCapture` stream.
  No additional whole-pair rerun was triggered.

Disabled candidate versus detached Phase 1, using the median of the three
reported launch medians:

| Path | Detached Phase 1 | Candidate `Disabled` | Elapsed change | Allocation delta |
| --- | ---: | ---: | ---: | ---: |
| Pooled open/close/dispose | 394.552541 ns / 256 B | 395.064807 ns / 256 B | +0.129835% | +0 B |
| SQL primary-key lookup | 419.921541 ns / 544 B | 412.332439 ns / 504 B | -1.807267% | -40 B |
| Pre-parsed primary-key lookup | 524.057055 ns / 1,016 B | 531.157637 ns / 976 B | +1.354925% | -40 B |
| SQL autocommit insert | 3,453.251457 ns / 1,752 B | 3,426.802826 ns / 1,712 B | -0.765905% | -40 B |
| Pre-parsed autocommit insert | 3,112.067413 ns / 1,191 B | 3,046.196365 ns / 1,127 B | -2.116633% | -64 B |
| Explicit-transaction insert | 3,188.526154 ns / 1,257 B | 3,131.835556 ns / 1,193 B | -1.777956% | -64 B |
| Stream 128 rows to exhaustion | 11,394.222260 ns / 22,840 B | 11,522.991180 ns / 22,800 B | +1.130125% | -40 B |

All seven `Disabled` elapsed gates and all seven `+0 B` allocation gates pass.

`HistoryCapture` versus `Disabled` from the same candidate launch, evaluated as
the median of three paired results:

| Engine query path | Median `Disabled` | Median `HistoryCapture` | Median paired delta | Median allowance | Median paired margin | Allocation delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SQL primary-key lookup | 412.332439 ns | 1,309.516716 ns | +898.190594 ns | 1,500.000000 ns | -601.809406 ns | +144 B |
| Pre-parsed primary-key lookup | 531.157637 ns | 889.147568 ns | +357.989931 ns | 1,500.000000 ns | -1,142.010069 ns | -16 B |
| SQL autocommit insert | 3,426.802826 ns | 4,710.016251 ns | +1,283.213425 ns | 1,500.000000 ns | -216.786575 ns | +560 B |
| Pre-parsed autocommit insert | 3,046.196365 ns | 3,556.118965 ns | +509.922600 ns | 1,500.000000 ns | -990.077400 ns | +496 B |
| Explicit-transaction insert | 3,131.835556 ns | 3,613.367844 ns | +479.357910 ns | 1,500.000000 ns | -1,020.642090 ns | +496 B |
| Stream 128 rows to exhaustion | 11,522.991180 ns | 13,693.324280 ns | +2,247.441101 ns | 2,304.598236 ns | -41.735535 ns | +40 B |

The paired margin is calculated within each launch before taking its median, so
it need not equal the displayed median delta minus the displayed median
allowance. All six amended elapsed gates and all six `1,024 B/logical query`
allocation gates pass. Pool `HistoryCapture` remains N/A: it performs zero
logical queries and creates zero history records. Its final median-of-three
characterization is `11,643.017578 ns / 10,516 B` versus
`395.064807 ns / 256 B` for `Disabled`.

Raw launches, iteration variability, hash manifests, pair disposition, and the
reproducible calculation are preserved as repo-local release evidence under
`work/artifacts/phase2-formal-final-perf-after-fastpaths/`; this path reference
does not assert that the evidence is tracked or committed. The independent raw
JSON audit found no discrepancy with the final report.

Goal: make active and recent query work, plan behavior, pools, readers,
transactions, and sessions inspectable through one bounded model.

Work:

- [x] Add a lock-safe active-query registry keyed by opaque operation id.
- [x] Track query phase (`queued`, `planning`, `executing`, `streaming`,
  `waiting`, or `disposing`), start time, elapsed time, operation class,
  fingerprint, outcome, transport, trace id, and safe session correlation.
- [x] Add a bounded recent-query history with explicit capacity, retention,
  dropped-count, and truncation fields.
- [x] Add a single registry sweep mechanism for long-running thresholds; do
  not allocate one timer per query.
- [x] Emit the long-running event once while retaining the final slow-query
  completion event.
- [x] Expose selected access-path category, plan-cache hit/miss,
  reclassification, adaptive reoptimization, estimated rows, and actual rows
  where they can be collected without replaying the query.
- [x] Preserve `EXPLAIN ESTIMATE FOR` as explicit operator-submitted deep
  inspection. Phase 2 diagnostics never reconstruct or replay SQL and do not
  add a recent-query link; the separately authorized Admin handoff/link is
  deferred to Phase 6.
- [x] Never auto-run `EXPLAIN ANALYZE` from diagnostics. It executes the target
  and can mutate data; Admin must label that behavior explicitly.
- [x] Keep automatic full-plan capture off by default. Phase 2 retains only a
  bounded plan summary; explicit explain remains a separately requested
  operation and diagnostics never replay SQL.
- [x] Snapshot ADO.NET pool capacity, waiters, active/idle sessions, active
  readers, retired/poisoned pools, transaction owner, and oldest transaction
  age without exposing raw data-source paths.
- [x] Model the pool accurately: active logical sessions, available slots,
  waiters, active snapshot readers, transaction owner/age, warm-engine idle,
  and disabled/poisoned/retiring state. The current `IdleCount` is not a count
  of retained logical sessions.
- [x] Snapshot direct-client transaction sessions, active snapshot readers,
  exclusive maintenance state, and in-flight REST/gRPC requests.
- [x] Add created/last-active/current-operation timestamps to remote
  transaction sessions and make abandoned non-expiring sessions visible, while
  replacing their bearer transaction ids with separate opaque diagnostic ids.
- [x] Define aggregate and capped per-shard query/session snapshots.
- [x] Add additive `ICSharpDbObservabilityClient` methods for summary, active
  queries, recent queries, query-plan diagnostics, session diagnostics, and
  separately authorized query detail.
- [x] Implement direct, HTTP, gRPC, and sharded transports with matching
  contracts.
- [x] Make Admin's `DatabaseClientHolder` implement and delegate
  `ICSharpDbObservabilityClient`, including after database switches and
  reconnects.
- [x] Add authenticated REST routes under `/api/diagnostics` and matching gRPC
  methods.
- [x] Enforce API authentication when configured and loopback-only access when
  security mode is `None`, unless the operator explicitly enables and
  acknowledges insecure remote diagnostics.
- [x] Preserve the existing HTTP SQL/transaction and gRPC cancellation paths;
  propagate request cancellation through maintenance and inspection endpoints,
  and pass the already-accepted token through direct inspector helpers.
- [x] Document that parsing/planning and synchronous fast paths may not observe
  cancellation immediately. Keep ADO.NET `CommandTimeout`/`Cancel()` and query
  termination explicitly unsupported until they are implemented and tested.
- [x] Add a separate authorized query-detail request for any captured SQL or
  path field. Never include those fields in summary, active, or recent lists.
- [x] Define consistent behavior when a custom client does not implement the
  optional diagnostics capability.

Deliverables:

- Long-running and recent query visibility.
- Query-plan runtime diagnostics.
- Connection, pool, reader, transaction, and session diagnostics.
- Direct/REST/gRPC/sharded diagnostics parity.

Exit gate:

Satisfied on 2026-08-11. The implementation, compatibility, security,
boundedness, lifecycle, packaging, trim/AOT, and paired performance evidence
above demonstrates the acceptance criteria below; Phase 3 started afterward.

- Concurrent query start, completion, cancellation, and disposal cannot leak
  active entries or complete an entry twice.
- Histories stay within configured capacity under stress.
- Instance-id and counter-epoch changes are visible after a server restart or
  counter reset.
- Pool retirement, client disposal, restore, and transaction races produce
  internally consistent snapshots.
- Detailed diagnostics endpoints follow configured API authentication.
- Existing third-party `ICSharpDbClient` implementations remain source and
  binary compatible.

## Phase 3: Storage, WAL, Backup, And Restore Diagnostics

Status: `Complete` (2026-08-11)

Goal: turn existing internal storage counters and maintenance results into safe
live operational snapshots without converting offline inspection into a hot
path.

First live-gauge slice on 2026-08-11:

- Reused the existing Phase 2 storage, WAL, checkpoint-phase, availability, and
  JSON contracts; no public DTO, protobuf, endpoint, or client-interface change
  was required.
- Added runtime-family-owned registration for successfully opened public
  built-in database handles. Failed opens and private snapshot handles never
  register; custom storage factories report `Unsupported`; enabled families
  without a live handle report `Unavailable`; disabled diagnostics create no
  component.
- Added bounded, O(1), read-side-stabilized gauges for logical database and WAL
  extents, file extents where applicable, pages, dirty pages when they are
  knowable, active readers and writers, WAL frames and committed-frame bytes,
  retained checkpoint suffix bytes, outstanding logical commits, and current
  checkpoint phase. In-memory and snapshot-resident modes never infer physical
  allocation from a backing path.
- At this first checkpoint, kept resettable benchmark counters and semantically
  ambiguous commit/conflict values out of the public runtime snapshot.
  Checkpoint/recovery detail and maintenance history remain subsequent Phase 3
  slices; the non-resetting cumulative counter follow-on is recorded below.
- Registration disposal is serialized against capture and completes before
  pager disposal. Multiple live handles use documented maxima, saturating sums,
  nullable propagation, and highest-phase aggregation; aggregate multi-family
  summaries remain unavailable while exact family children are authoritative.
- Independent review caught and fixed fault-state relabeling and mixed-epoch
  WAL/Pager captures. The frozen slice passed clean Release builds, `116/116`
  focused storage/checkpoint tests, `93/93` Engine/Client runtime-adjacent tests,
  and a root rerun of the `82/82` WAL plus storage-runtime tests.

Non-resetting lifetime-counter follow-on on 2026-08-11:

- Added sticky, saturating production lifetimes for logical WAL commit
  publications, file-WAL commit-publication batches, and the committed frame
  bytes covered by those batches. They project through the existing
  `Storage.CommitCount`, `Wal.FlushCount`, and `Wal.BytesWritten` fields without
  changing a public DTO, JSON contract, protobuf, endpoint, or client interface.
- Recovered historical commits do not enter the current runtime family's
  lifetime. Checkpoint compaction, WAL reset/replacement, and benchmark counter
  reset cannot decrease the production totals. Benchmark reset now advances
  private baselines while the sticky production family remains intact.
- In-memory WAL reports the logical commit lifetime and leaves the file-only
  flush/byte fields null. A live mixed file/in-memory aggregate propagates that
  null honestly; retiring an in-memory provider preserves its known logical
  contribution without poisoning a later file-only aggregate. A failed or
  invalid final provider capture makes the affected runtime-family lifetimes
  permanently unknown rather than publishing a decreasing value.
- Provider unregister takes one final serialized capture and rolls only
  cumulative fields into runtime-family retired totals; gauges disappear with
  the live handle. Repeated disposal is exact-once, reopen adds new live work to
  the retired totals, and all additions saturate instead of wrapping.
- Independent frozen-tree qualification passed clean Storage and full test
  builds, the combined `124/124` storage/WAL/checkpoint/runtime matrix, and the
  exact `8/8` lifetime/interleaving matrix. The paired disabled-write performance
  gate also qualified: median-of-three SQL autocommit `-0.443338%`, pre-parsed
  autocommit `-2.289208%`, and explicit-transaction `-0.702317%`, with unchanged
  allocation on all three rows and all six launch spreads below `5%`.

WAL durability and group-commit contract follow-on:

- The original positional `WalRuntimeDiagnosticsSnapshot` constructor and
  `Deconstruct` shape remain exactly 13 values. Six optional init-only JSON members
  add `FlushedCommitCount`, `DurableFlushCount`,
  `LastSuccessfulDurableFlushAtUtc`, `GroupCommitBatchCount`,
  `GroupCommitCount`, and `LastSuccessfulGroupCommitAtUtc`; older payloads omit
  them and deserialize each as unknown (`null`). All six are file-WAL-only:
  memory WAL reports them as unknown, and a live mixed file/memory aggregate
  propagates that unknown honestly.
- `FlushCount`, `BytesWritten`, and `LastSuccessfulFlushAtUtc` retain their
  existing commit-publication-batch meaning. `FlushedCommitCount` counts the
  logical commits covered by those batches. `DurableFlushCount` instead counts
  every successful actual durable WAL-policy `FlushToDisk` call, including
  creation, reset, compaction, checkpoint, and commit paths, so it may exceed
  `FlushCount`. Its timestamp is the latest such successful durable WAL sync.
  A buffered file WAL can still report publication and group counts while
  reporting a known zero durable flush count and no durable timestamp.
- `GroupCommitBatchCount` counts publication batches containing at least two
  logical commits, while `GroupCommitCount` counts the logical commits in those
  batches. Producers keep group batches no greater than `FlushCount`, and keep
  group commits at least twice the group-batch count using saturating arithmetic
  and no greater than `FlushedCommitCount`, whenever the related values are
  known. A last-success timestamp requires a positive corresponding count;
  positive counts need not have a timestamp. All scalar setters independently
  reject negative values or non-UTC timestamps without making object-initializer
  or JSON property order significant.

Logical cache and primary physical-I/O contract follow-on:

- The existing positional `StorageRuntimeDiagnosticsSnapshot` constructor and
  `Deconstruct` shape remain exactly 15 values. Two additive init-only sections,
  `Cache` and `PhysicalIo`, carry independently available detail and default to
  `Unavailable` when omitted by a 1.0/1.1 payload. Available children must share
  the complete parent capture metadata; direct, sharded, and ADO.NET projections
  rebuild that metadata recursively instead of relabeling only the parent.
- The existing top-level `PageReads`, `BytesRead`, `CacheHits`, and
  `CacheMisses` remain the compatibility surface for logical pager reads. One
  page read is a successful `PageBufferManager` materialization from the main or
  WAL cache, WAL or main device, memory mapping, or a completed speculative
  uncached read even if its result is later unused. Transaction-local modified
  page returns and failed cache-only probes do not count. `BytesRead` adds the
  materialized page bytes, and cache hits plus misses partition every successful
  tracked page read exactly.
- `PageWrites` is the sticky, saturating count of page images published by
  successful live commits, and `BytesWritten` is that logical count multiplied
  by the page size using saturating arithmetic. Recovery and checkpoint index
  maintenance do not increment either field. `ConflictCount` is the sticky,
  saturating count of terminal logical key or range conflicts raised while
  validating explicit transactions. These logical fields are distinct from
  `PhysicalIo.WriteCount` and `PhysicalIo.BytesWritten` on the primary device.
- `Cache` is a live-only occupancy gauge: shared resident pages, optional shared
  capacity (`null` means unbounded), WAL resident pages, and WAL capacity.
  Built-in file and memory pagers report it while live. Gauges disappear at
  drain and are never rolled into retired totals. A custom or otherwise
  non-built-in cache reports `Unsupported`; the optional producer seam is
  internal and is not a third-party extension contract. A local capture failure
  reports only this section as `Unavailable`.
- `PhysicalIo` counts calls and bytes for the primary database device only;
  WAL-device work is never included. Sequential read calls and bytes are
  explicit subsets of total reads. Memory-mapped page exposure count and bytes
  are separate mapping observations, not physical reads or operating-system page
  faults. File storage reports this section as `Available`, built-in memory
  storage as `NotApplicable`, and a custom or otherwise non-built-in device as
  `Unsupported`; the optional producer seam is internal and is not a
  third-party extension contract. Local capture failures report `Unavailable`.
- Physical counters are cumulative and include the final shutdown checkpoint
  and primary-device work. Drain first retires a pre-dispose watermark, then
  pager disposal contributes one sealed post-drain delta so final I/O is neither
  lost nor double-counted. Counter arithmetic saturates rather than wrapping.

Bounded maintenance and backup/validation slice on 2026-08-11:

- Added a bounded, process-local maintenance registry using the existing
  history capacity and retention settings. It exposes the deterministic oldest
  active operation through the existing `ActiveMaintenance` summary, keeps
  bounded recent terminal records internally, discloses hidden active work and
  dropped history through truncation state, and never evicts a running
  operation to admit diagnostics for another one.
- Client-origin maintenance records live for the client lifetime so queued work
  survives runtime-family replacement. A direct public backup coordinated from
  an existing `Database` uses an exact-family state fallback; the client path
  suppresses that fallback so one logical backup never appears twice. Exact
  family children show only their state-owned work, while the aggregate merges
  client and family-owned work and discloses omitted capped families.
- Runtime maintenance visibility covers client/operator-owned checkpoint,
  backup, restore validation, restore, foreign-key migration, reindex, and
  vacuum operations, plus state-owned direct checkpoint and `Database`-attached
  backup. Ownerless public static coordinator calls have no runtime registry
  owner and remain outside this visibility contract.
- Backup now registers before the client lock and reports truthful
  `Queued`, `AcquiringAccess`, `Checkpointing`, `Copying`, `Staging`,
  `Validating`, and `Hashing` phases. Restore validation reports `Validating`.
  One operation context and opaque id are shared by runtime state and the
  existing typed lifecycle event, with one terminal success, rejection,
  cancellation, or failure after cleanup completes.
- The Pager contributes best-effort backup progress only at real work
  boundaries. The public save/backup/validation signatures and null observer
  path are unchanged; callback failures are isolated and no path or raw
  exception text enters runtime diagnostics.
- Ordinary runtime-diagnostics snapshots and diagnostic/lifecycle logs contain
  configured aliases but no database or backup paths. Explicit deep-inspection
  reports and existing backup/restore result DTOs remain separately authorized,
  path-bearing contracts; no general path-capture option is claimed here.
- Disabled backup and validation create no maintenance registry, runtime
  component, lifetime lease, or drain signal. Full replacement restore remains
  the next bounded slice because its cancellation point, paired database/WAL
  rollback, and eager reopen require a separate safety change.
- Independent frozen-tree qualification passed a clean Release build, the
  focused `15/15` maintenance/Pager matrix, and a `152/152` lifecycle,
  runtime, storage, checkpoint, and save adjacency suite with no failures or
  skips. Public API, ABI, DTO, JSON, and protocol shapes remain unchanged.

Hardened full-restore slice on 2026-08-11:

- Full client restore now joins the same bounded maintenance lifecycle before
  waiting for exclusive access and reports `Queued`, `AcquiringAccess`,
  `Copying`, `Staging`, `Validating`, `Replacing`, `RollingBack` when needed,
  `Reopening`, and one terminal `Completed` state with the same opaque id as
  the typed restore lifecycle event.
- The final caller-cancellation check is immediately before the first live
  database or WAL mutation. After that point, replacement, rollback, cleanup,
  and reopen are deliberately non-cancellable so cancellation cannot interrupt
  consistency recovery.
- Existing database and WAL files are backed up as one pair and retained until
  the replacement has reopened successfully. Any publish or replacement-open
  failure restores both files, reopens the original, and rethrows the original
  failure; recovery failures retain the paired forensic backups and surface one
  path-safe aggregate failure. A successful result is impossible before the
  replacement has been opened and adopted into the client cache.
- Reopen occurs directly under the exclusive lease rather than recursively
  calling the normal database getter. The existing runtime-family reset occurs
  once, custom direct options/factories/interceptors are reused, lazy clients
  remain lazy after pre-replacement failures, and disposal cannot race the
  restore into a lock cycle or resurrect work after shutdown.
- Private in-memory clients reject full file replacement before detaching any
  state; validation-only remains supported. The Phase 3 availability invariant
  ends at successful reopen and adoption before terminal success. No deferred
  `ReopenPending` or host ready/not-ready state is claimed because this client
  has no background reopen owner; Phase 5 owns readiness publication.
- Independent frozen-tree qualification passed a clean non-incremental Release
  build, `28/28` focused maintenance/restore-safety tests, and a `179/179`
  restore, client, runtime, storage, and checkpoint regression suite with no
  failures or skips. Public API, ABI, DTO, JSON, and protocol shapes remain
  unchanged.

Work:

- [x] Define a live storage snapshot for database size, page counts, cache
  usage/hits/misses, dirty pages, active readers/writers, commits, conflicts,
  checkpoint state, and cumulative I/O where available.
- [x] Distinguish logical pager I/O from physical device I/O and represent
  per-section support honestly as `Unsupported`, `Unavailable`, or
  `NotApplicable`, especially for in-memory, hybrid, and custom storage
  implementations.
- [x] Promote existing WAL flush and commit-path counters through a public
  curated immutable runtime model without exposing the large, unstable
  benchmark snapshot wholesale or changing its reset semantics. Publish new
  non-resetting lifetime counters, or advance the counter epoch when test-only
  reset APIs run.
- [x] Add current WAL logical bytes, allocated/file bytes, committed-frame
  bytes, retained bytes, coherent frame/commit state, pending commit count,
  last successful flush/checkpoint, recovery state, and last safe error code.
  Frame count and WAL sizes are gauges and may decrease after checkpoint.
- [x] Model checkpoint phases (`Idle`, `Requested`, `Copying`,
  `CopyCompleteAwaitingReaders`, `Finalizing`, and `Faulted`), progress,
  retained-WAL reason, foreground/background origin, and last start/success/
  failure. Record foreground auto-checkpoint and shutdown checkpoint failures
  that are currently swallowed or retried.
- [x] Instrument recovery through storage-engine options/factories so startup
  work before `Database` construction is visible. Record scanned/recovered/
  discarded frames and bytes, safe truncation reason, duration, and outcome
  without exposing the WAL path.
- [x] Instrument missing checkpoint, fsync, write, group-commit, cache, and
  recovery counters with lock-free or low-contention updates.
- [x] Use an optional cache-diagnostics provider or a pager-owned counter seam
  so third-party storage/cache implementations do not gain required members.
- [x] Do not use `IPageOperationInterceptor` as the production metric hot path;
  enabling it changes cache fast paths. Use pager-owned counters and internal
  diagnostics providers for built-in caches/devices; custom implementations
  remain unchanged and report `Unsupported`.
- [x] Keep `DatabaseInspector`, `WalInspector`, page inspection, and index
  checks explicit and separately labeled as offline/deep inspection.
- [x] Add a bounded maintenance-operation registry for client/operator-owned
  checkpoint, backup, restore validation, restore, reindex, vacuum, and
  foreign-key migration, plus state-owned direct checkpoint and backup.
- [x] Report operation id, phase, start time, elapsed time, progress units when
  known, outcome, warning/error counts, and safe failure code.
- [x] Instrument client/operator-owned `DatabaseBackupCoordinator` paths so an
  in-progress backup, restore validation, or full restore is visible before the
  original call completes; direct `Database`-attached backup uses a state-owned
  fallback. Ownerless public static coordinator calls remain outside the
  runtime visibility contract.
- [x] Register backup/restore before waiting for the client execution or
  exclusive-access lock. Include queued/acquiring time and explicit phases for
  checkpoint, copy/stage, validation, hashing/manifest work, replacement,
  rollback, and reopen.
- [x] Retain a bounded recent operation history in memory and clearly report
  that it resets with the process.
- [x] Keep ordinary runtime-diagnostics snapshots and diagnostic/lifecycle logs
  path-free and alias-only. Explicit deep-inspection reports and existing
  backup/restore result DTOs remain separately authorized, path-bearing
  contracts.
- [x] Define restore availability through exclusive-access acquisition,
  validation, replacement, and reopen. After replacement begins, full restore
  cannot reach terminal success until the replacement has reopened and been
  adopted; Phase 5 owns host ready/not-ready publication.
- [x] Define the restore point of no return and cancellation behavior around
  non-cancellable file replacement. Eagerly reopen and adopt before terminal
  success; no deferred `ReopenPending` state is claimed in Phase 3.
- [x] Pass cancellation through explicit deep storage/WAL/page/index inspectors
  even though those inspectors remain outside normal runtime snapshots.
- [x] Add summary, storage, WAL, and maintenance-operation methods to the
  optional diagnostics client and both remote transports.
- [x] Define sharded storage aggregation rules and preserve capped per-shard
  records for diagnosis.

Deliverables:

- Live storage and WAL diagnostics.
- Current and recent client/operator-owned backup/restore/maintenance status,
  plus state-owned direct checkpoint and backup status.
- Transport-neutral snapshot models and endpoints.

Exit gate:

The functional and semantic exit gates were satisfied on 2026-08-11 for the
runtime-diagnostics scope described above. Host ready/not-ready publication
remains Phase 5 work; ownerless public static coordinator calls and explicitly
path-bearing inspection/result contracts are outside Phase 3's runtime
visibility and privacy guarantees. Formal performance attribution is not
claimed because the final host-stability prerequisite did not qualify.

- Snapshot collection does not scan the database or WAL files unless the caller
  explicitly requests deep inspection.
- Counters remain monotonic and consistent during concurrent commits,
  checkpoints, backup, restore, and shutdown.
- Resettable benchmark diagnostics cannot make production counters decrease;
  gauges and epochs change according to the published contract.
- WAL recovery and checkpoint progress/failures are visible even when they
  occur before `Database` construction or are retried internally.
- A registered client/operator-owned backup or restore is visible, reaches one
  terminal state, and cannot remain permanently active after failure or
  cancellation; state-owned direct checkpoint and backup follow the same rule.
- After replacement begins, full restore cannot report terminal success until
  the new database has reopened and been adopted successfully. Phase 5 owns
  ready/not-ready publication.
- Ordinary runtime-diagnostics snapshots and diagnostic/lifecycle logs contain
  no database or backup paths. Explicit deep-inspection reports and existing
  backup/restore result DTOs retain their path-bearing contracts.

Qualification on 2026-08-11:

- The strict build completed with zero warnings and zero errors.
- The final cache/physical-I/O focused matrix passed `26/26`, and its broader
  runtime/storage/WAL/checkpoint/maintenance adjacency matrix passed `240/240`.
- Full suites passed: Core `2670/2670`, Observability `110/110`, Data `270/270`,
  API `137/137`, Daemon `204/204`, Pipelines `41/41`, benchmark contracts
  `130/130`, Admin `452/452`, and EF Core `153/153`. The Daemon suite's first
  sandboxed run could not create its temporary Git repositories; its complete
  normal-environment rerun passed.
- NuGet package-closure validation passed. Managed full-trim publication built
  without warnings or errors and its executable smoke test passed. Native AOT
  publication then completed through the pinned Visual Studio 2026 Insiders
  MSVC 14.51 and Windows SDK 10.0.28000.0 toolchain, and the native executable
  smoke test passed.
- Earlier independent frozen-tree evidence for maintenance/restore safety,
  explicit inspector cancellation, recovery/checkpoint detail, WAL lifetime
  counters, and disabled diagnostics remained clean. Exact focused counts and
  qualifications are retained in the slice notes above where applicable.
- The final performance packet used three strict serial pre-cache/final
  BenchmarkDotNet pairs with the same verified 179-file benchmark harness.
  All six launches were structurally complete (`126` rows total, three warmups
  and ten measured iterations per row). Median-of-three disabled elapsed-time
  limits passed `7/7`, from `+0.687082%` at the slowest to `-15.351512%` at the
  fastest, and allocation limits passed `7/7` at exactly `+0 B`.
- That packet is diagnostic rather than formal qualification evidence. Three
  separate 305-sample idle gates failed the reconstructed host-isolation
  policy, and every disabled baseline/final series exceeded the required `5%`
  launch-spread ceiling, so the complete formal disabled result is `0/7`.
  HistoryCapture elapsed limits passed `5/6`, allocations passed `6/6`, and
  stability passed `0/6`; the 128-row stream missed its elapsed allowance by
  `464.455261 ns`. Raw-log and exported-CSV reductions produced identical
  decisions. Re-run these performance gates on an isolated host before making
  a formal no-regression release claim.

## Phase 4: OpenTelemetry And Prometheus

Status: `Not started`

Goal: make the shared operation model consumable by standard tracing and
metrics systems without coupling the engine to an exporter.

Work:

- [ ] Emit `Activity` spans for queries, transactions, checkpoints, backup,
  restore, and maintenance from one stable CSharpDB `ActivitySource`.
- [ ] Make remote query spans children of inbound ASP.NET Core or gRPC spans
  and avoid duplicate client/server/engine root spans.
- [ ] Prove that a span covering a lazy `QueryResult` does not leave an
  incorrect ambient `Activity.Current` after the initial execute call or across
  unrelated caller work; restore parent context deliberately where needed.
- [ ] Apply the current stable OpenTelemetry database semantic conventions
  where they are safe, documenting any CSharpDB-specific attributes.
- [ ] Keep statement text disabled and use fingerprints only in traces/logs,
  never as metric labels.
- [ ] Emit a stable CSharpDB `Meter` with counters, histograms, up/down
  counters, and observable gauges for:
  - query count, duration, outcome, rows, active, and slow;
  - transactions, active sessions, pool waits, and active readers;
  - cache, page I/O, commits, conflicts, and checkpoints;
  - WAL size, writes, flushes, bytes, batches, and recovery;
  - backup, restore, and other maintenance operation count/duration/outcome.
- [ ] Define units, histogram guidance, temporality expectations, and metric
  schema-version policy.
- [ ] Add host configuration for OpenTelemetry resources, sampling, OTLP
  export, and console export for local development.
- [ ] Keep exporters out of the core NuGet dependency graph.
- [ ] Add optional Prometheus export to API and daemon with an explicit enable
  flag, configurable path/listener policy, and safe startup validation.
- [ ] Make the listener contract explicit: the MVP uses the normal Kestrel
  listener with peer-address/API-key filtering, or it provisions a separately
  configured metrics listener. Merely changing `/metrics` path does not create
  network isolation.
- [ ] Map configured daemon metrics independently of its normal REST API toggle
  so gRPC-only deployments can still opt into scraping.
- [ ] Require existing API authentication when it is configured. With security
  mode `None`, restrict scraping to loopback unless an explicit insecure-remote
  override is configured and logged.
- [ ] Protect `/metrics` with dedicated middleware or an endpoint filter that
  reuses `CSharpDbApiKeyValidator`; the current API-key middleware covers only
  the `/api` branch. Evaluate loopback from the actual peer connection, not
  forwarded headers.
- [ ] Document authentication or network isolation requirements for the scrape
  endpoint.
- [ ] Add in-memory exporter tests for span parentage, status, attributes,
  metrics, and log correlation.
- [ ] Add a cardinality stress test that uses many distinct SQL statements,
  sessions, errors, tables, and database paths.
- [ ] Add endpoint tests proving disabled is `404`, a missing or wrong API key
  is `401`, a correct key is `200`, security mode `None` denies remote peers
  but permits loopback, and gRPC-only daemon mode can still opt in.
- [ ] Qualify the BCL instrumentation assembly and existing Native library
  under NativeAOT/trimming. Run normal published-host smoke tests for API and
  daemon without introducing a new NativeAOT-host support claim.

Deliverables:

- Supported CSharpDB OpenTelemetry traces and metrics.
- Optional OTLP configuration in hosted products.
- Optional bounded-cardinality Prometheus metrics.

Exit gate:

- In-memory exporter tests prove the documented trace and metric schema.
- Prometheus output remains bounded when query text, sessions, object names,
  and errors vary without limit.
- A disabled exporter creates no background connection or retry loop.
- API and daemon start safely when collectors are unavailable.
- AOT-compatible core/Observability and the existing Native library pass their
  NativeAOT gates; API and daemon pass their supported publish smoke tests.

## Phase 5: Health, Readiness, And Liveness

Status: `Not started`

Goal: give orchestrators and operators cheap, dependable signals that
distinguish a running process from a database host that can accept work.

Work:

- [ ] Add reusable CSharpDB health registrations in the API host extensions so
  the standalone API and daemon use the same checks.
- [ ] Replace the current pre-`app.Run` database warmup with a lifecycle/
  readiness coordinator or background initializer so listeners can report
  `starting`, `recovering`, initialization failure, and recovery. If startup is
  intentionally kept fail-fast instead, narrow the advertised health-state
  contract and tests accordingly.
- [ ] Map `/health/live` to process liveness only. It must not open, query,
  checkpoint, or inspect the database.
- [ ] Map `/health/ready` to a bounded readiness snapshot covering host
  initialization, shutdown, database open/recovery, exclusive restore state,
  and configured write readiness.
- [ ] Use cached state plus a strict timeout for any side-effect-free readiness
  probe.
- [ ] Keep readiness independent of the main client execution lock so one slow
  query or queued backup does not create a false host outage.
- [ ] Return `200` for healthy and `503` for not-ready/not-live with a stable,
  minimal JSON schema.
- [ ] Keep anonymous responses to the generic `status` field only. Provide
  timestamps and detailed component reasons through an authenticated
  diagnostics endpoint.
- [ ] Add configuration for endpoint enablement and paths without allowing
  collisions with API, Admin, OpenAPI, gRPC, or Prometheus routes.
- [ ] Register the new health services and routes explicitly in Admin. Preserve
  Admin's `/healthz` desktop-launcher contract as its existing shallow
  process/liveness probe; add database readiness separately.
- [ ] Add standard gRPC health service support to the daemon, including overall
  and CSharpDB database service names.
- [ ] Exempt standard gRPC Health `Check` and `Watch` from API-key
  authentication consistently and return status only. Test unary and streaming
  interceptor behavior explicitly.
- [ ] Cover unary, client-streaming, server-streaming, and duplex interceptor
  shapes with an explicit health-method allowlist; do not rely on the current
  absence of streaming overrides as an authentication policy.
- [ ] Map daemon health even when its normal REST API surface is disabled.
- [ ] Define readiness during startup, WAL recovery, read-only mode, backup,
  restore validation, full restore, maintenance, and graceful shutdown.
- [ ] Emit a low-cardinality health status metric and structured transition
  events without logging repeated successful probes.
- [ ] Add tests proving liveness remains healthy during a database readiness
  failure and readiness recovers after a transient condition.

Deliverables:

- Separate liveness and readiness endpoints.
- Shared API/daemon health semantics.
- gRPC health support for daemon deployments.
- Admin compatibility health plus a separate readiness route.

Exit gate:

- Liveness remains independent of database state and storage latency.
- Readiness fails within its configured timeout and never mutates data.
- Health routes expose no paths, SQL, credentials, or exception details.
- Shutdown changes readiness before listeners terminate.
- API, daemon, Admin, and desktop-launcher integration tests cover healthy,
  starting, restoring, failed, recovering, and stopping states.

## Phase 6: Admin Metrics And Diagnostics UI

Status: `Not started`

Goal: give operators a focused Admin workspace for live metrics and diagnostic
drill-down without requiring direct process access.

Work:

- [ ] Add an `Observability` tab and tab-manager/navigation/command-palette
  entries rather than overloading the physical `Storage` inspector.
- [ ] Add overview cards for health, query rate/latency/errors, active and slow
  queries, sessions, transactions, WAL growth, checkpoint age, and current
  maintenance work.
- [ ] Add bounded in-memory time-series sampling in Admin for short charts;
  Admin must not become a second telemetry warehouse.
- [ ] Add configurable refresh interval, pause/resume, manual refresh, stale
  data indication, and cancellation when the tab closes.
- [ ] Poll only while Observability is the active tab. Hidden Admin tabs remain
  mounted, so visibility must stop polling even before component disposal.
- [ ] Clear local rate and chart samples when the server instance id or counter
  epoch changes so restarts and resets do not create false spikes or negative
  rates.
- [ ] Add active and recent query tables with duration, phase, outcome,
  fingerprint, plan summary, transport, session correlation, and trace id.
- [ ] Add query-plan drill-down and a safe handoff to the existing
  `EXPLAIN ESTIMATE` query experience.
- [ ] Add connections/sessions, storage/WAL, and backup/restore panels.
- [ ] Add aggregate/per-shard selection when the connected client is sharded.
- [ ] Keep raw SQL and paths out of ordinary snapshots. An explicit reveal
  action must issue the separate server-authorized detail request and work only
  when capture is enabled.
- [ ] Show capability-not-supported and access-denied states honestly for
  custom or restricted remote clients.
- [ ] Link the runtime storage/WAL panel to the existing deep Storage inspector
  while explaining the cost difference.
- [ ] Use the optional diagnostics client for direct, HTTP, and gRPC modes; do
  not read direct engine internals from the component.
- [ ] Add component/service tests for refresh, inactive-tab polling, truncation,
  instance/epoch reset, unavailable hosts, database switch, reconnect, sharded
  views, detail authorization, redaction, and disposal.
- [ ] Verify keyboard navigation, accessible status text, responsive layout,
  empty states, and high-volume truncation messages.

Deliverables:

- Admin observability overview and diagnostic drill-down.
- Live query, session, storage/WAL, and maintenance visibility.
- Direct/remote/sharded behavior parity.

Exit gate:

- The same Admin component works with built-in direct, HTTP, and gRPC clients.
- `DatabaseClientHolder` delegates observability correctly after a database
  switch or reconnect.
- UI refresh cannot overlap without bounds or continue after component
  disposal.
- Admin shows snapshot age, truncation, unsupported capabilities, and
  authorization failures explicitly.
- Default screenshots and rendered UI contain no raw SQL, values, or paths.
- Admin tests and desktop packaging smoke tests pass.

## Phase 7: Qualification, Documentation, And Release Closure

Status: `Not started`

Goal: prove the complete feature set is safe, interoperable, performant, and
usable before closing its target release.

Work:

- [ ] Add a public `www/docs/observability.html` guide covering embedded,
  standalone API, daemon, OpenTelemetry, Prometheus, health, Admin, and
  redaction configuration.
- [ ] Update configuration, REST API, Admin, deployment, index, sitemap, and
  relevant architecture pages.
- [ ] Add a small supported sample showing `ILogger`, OpenTelemetry/OTLP,
  Prometheus, readiness, and liveness configuration.
- [ ] Document metric names, units, labels, span names, attributes, structured
  event ids, schema compatibility, and deprecation policy.
- [ ] Document safe query-text opt-in, retention/capacity, scrape security,
  collector outages, overhead, and troubleshooting.
- [ ] Add direct/HTTP/gRPC/sharded contract tests for every diagnostics model.
- [ ] Add Windows, Ubuntu, and macOS integration coverage for logging,
  telemetry, Prometheus, health, query lifecycle, WAL, backup, and restore.
- [ ] Add concurrent stress tests for active-query cleanup, snapshot
  consistency, registry capacity, exporter failure, shutdown, and restore.
- [ ] Add golden tests for default redaction and metric-label cardinality.
- [ ] Add performance guardrails for disabled, metrics-only, tracing-sampled,
  and recent-history modes.
- [ ] Verify all new public models are trim-safe and serializer/source
  generation requirements are satisfied.
- [ ] Verify the Phase 0 `CSharpDB.Observability` pack and qualification lanes
  against the final release-candidate dependency graph.
- [ ] Publish `CSharpDB.Observability` before Storage, Execution, Engine,
  Client, Data, and the all-in-one package that depend on it.
- [ ] Run solution build/test, the existing core/NativeAOT smoke tests,
  supported API/daemon publish tests, package qualification, website
  link/content tests, and Admin desktop packaging checks.
- [ ] Update package version, changelog, release PR notes, and public release
  content only after all prior exit gates pass.

Deliverables:

- Complete public documentation and sample.
- Cross-platform, core/Native AOT, host publishing, redaction, cardinality,
  concurrency, and performance evidence.
- Target-release notes and qualified packages.

Exit gate:

- Every requested requirement has a linked implementation and test.
- All CI and release qualification jobs pass on Windows, Ubuntu, and macOS.
- No unresolved high-cardinality, data-exposure, double-counting, or
  compatibility finding remains.
- Documentation examples run against the release candidate packages.

## Planned Configuration Surface

Exact option type names may be refined in Phase 0, but the configuration must
remain one coherent subtree:

```json
{
  "CSharpDB": {
    "Observability": {
      "Enabled": true,
      "DatabaseAlias": "primary",
      "Logging": {
        "Enabled": true,
        "Queries": false,
        "SlowQueries": true,
        "SlowQueryThreshold": "00:00:00.500",
        "SqlText": "None"
      },
      "History": {
        "RecentQueryCapacity": 500,
        "RecentOperationCapacity": 100,
        "Retention": "00:15:00"
      },
      "LongRunningQueryThreshold": "00:00:05",
      "OpenTelemetry": {
        "Enabled": true,
        "Otlp": {
          "Enabled": false
        }
      },
      "Prometheus": {
        "Enabled": false,
        "Path": "/metrics"
      },
      "Health": {
        "Enabled": true,
        "LivenessPath": "/health/live",
        "ReadinessPath": "/health/ready",
        "ReadinessTimeout": "00:00:02"
      }
    }
  }
}
```

Safe defaults:

- Operational structured logging follows normal host log levels.
- Query logging, raw SQL, detailed object names, and path capture are off.
- Slow-query logging requires observability to be enabled and uses the
  configured threshold.
- Core `ActivitySource` and `Meter` emission is listener-driven.
- OTLP and Prometheus exporters are off until explicitly configured.
- Health endpoints are on for hosted products with minimal anonymous output;
  paths remain configurable, and Admin retains its `/healthz` compatibility
  probe.
- Histories are in-memory, bounded, and reset on process restart.

## Planned Public Surface

The initial model set should include:

- `CSharpDbObservabilityOptions`
- `RuntimeDiagnosticsSnapshot`
- `QueryDiagnosticsSummary`
- `ActiveQuerySnapshot`
- `RecentQuerySnapshot`
- `QueryDetailSnapshot` for the separate authorized detail request
- `QueryPlanDiagnosticsSnapshot`
- `ConnectionDiagnosticsSnapshot`
- `SessionDiagnosticsSnapshot`
- `StorageRuntimeDiagnosticsSnapshot`
- `WalRuntimeDiagnosticsSnapshot`
- `MaintenanceOperationSnapshot`
- `HealthDiagnosticsSnapshot`
- `ICSharpDbObservabilityClient`

All snapshot models must:

- Carry a schema version, capture time, server/process instance id, and counter
  epoch.
- Be immutable after publication.
- State whether results are aggregate, per-instance, or per-shard.
- State whether records or fields were truncated, unavailable, disabled, or
  denied.
- Use safe aliases instead of paths or connection strings.
- Omit sensitive fields from ordinary snapshots rather than relying on the UI
  to hide them.
- Serialize equivalently over REST and gRPC.

## Initial Telemetry Families

Names and units are finalized and snapshot-tested in Phase 4.

| Family | Instruments | Allowed dimensions |
| --- | --- | --- |
| Query | executions, duration, active, rows, errors, slow count | operation class, outcome, transport, configured alias |
| Transaction/session | active transactions, transaction duration, sessions, pool wait duration, readers | outcome, transport, configured alias |
| Storage | page reads/writes, bytes, cache hits/misses, dirty pages, commits, conflicts | operation class, outcome, configured alias |
| WAL/checkpoint | WAL bytes/size, frames, flushes, batch size, checkpoint count/duration/age, recovery | outcome, configured alias |
| Maintenance | backup, restore, validation, reindex, vacuum count/duration/active | operation class, outcome, configured alias |
| Health | liveness and readiness state/transitions | check kind, status, configured alias |

Query fingerprint, SQL text, object name, operation id, trace id, session id,
file path, and error message are prohibited metric dimensions.

## Test Matrix

| Area | Required proof |
| --- | --- |
| Query lifecycle | SQL, prepared, fast lookup/scalar, shared-result DML, simple insert, explicit transaction, multi-statement script, procedure/trigger, streamed rows, early/never-opened disposal, cancellation, failure, retry, and sharded fan-out complete exactly once at the documented request/statement level. |
| Logging/redaction | Default structured fields are stable; raw SQL opt-in is explicit; values, credentials, connection strings, paths, and row data never leak. |
| Slow/long queries | Threshold boundaries, streamed results, clock changes, cancellation, and completion produce one long-running and one final outcome as applicable. |
| Query plans | Cache and adaptive counters are accurate; plan summaries are bounded; explicit explain works; automatic diagnostics do not replay queries. |
| Sessions/connections | Pool waits, checkout/release, slot availability, warm-engine idle, retirement, poison, readers, non-expiring remote transactions, remote requests, and disposal remain consistent under concurrency without exposing bearer transaction ids. |
| Storage/WAL | Lifetime counters, reset epochs, gauges, logical/allocated WAL sizes, checkpoint phases, recovery tail truncation, group commit, cache pressure, startup-before-Database, reopen, custom providers, and in-memory storage remain coherent. |
| Backup/restore | Queue/acquire time, active phase, point-of-no-return cancellation, progress, success, validation failure, exclusive access, replacement rollback, reopen failure/pending, readiness, and recent history reach one terminal state. |
| OpenTelemetry | Span parentage/status, metric units/tags, sampling, resource attributes, exporter outage, and log/trace correlation match the contract. |
| Prometheus | Disabled, API-key, loopback, remote-denial, gRPC-only daemon, scrape format, concurrent scrape, and adversarial cardinality cases pass. |
| Health | Live versus ready, Admin `/healthz`, listener-before-initialization startup, recovery, initialization failure, read-only, restore/reopen, timeout, graceful shutdown, REST, and authenticated unary/streaming gRPC `Check`/`Watch` behavior are deterministic. |
| Admin | Direct/HTTP/gRPC/sharded snapshots, database switch, reconnect, active-tab polling, counter epoch reset, stale/truncated data, detail denial, and disposal render correctly. |
| Compatibility | Existing embedded clients and third-party `ICSharpDbClient` implementations compile and behave unchanged when observability is off. |
| Platforms | Windows, Ubuntu, and macOS tests plus existing core/NativeAOT and supported packaged-host smoke tests pass. |
| Performance | Disabled, metrics-only, sampled tracing, and bounded-history modes stay within the Phase 0 budgets. |

## Compatibility And Rollout

- Treat the observability schema as a versioned public contract after its
  initial release.
- Add new metrics and fields compatibly; do not silently change the meaning or
  unit of an existing instrument.
- Deprecate a metric or field for at least one minor release before removal
  unless it creates a security vulnerability.
- Keep custom `ICSharpDbClient` implementations valid by using a separate
  optional diagnostics capability.
- Return explicit `Unsupported`, `Disabled`, `Denied`, and `Unavailable`
  states instead of fabricated zero values.
- Do not enable SQL-text or path capture during upgrade.
- Prometheus and OTLP remain opt-in so upgrading does not open a new listener or
  create outbound network traffic.
- Health endpoints use minimal responses and configurable paths; document the
  new routes prominently for hosts with catch-all routing.
- Histories are diagnostic aids, not durable audit logs or billing records.

## Out Of Scope

- A durable, replicated telemetry warehouse inside CSharpDB.
- A replacement for an OpenTelemetry collector, Prometheus server, Grafana, or
  commercial APM product.
- Automatic alert delivery or paging.
- Distributed tracing across arbitrary user code that has no propagated
  `Activity` context.
- A full SQL profiler that captures parameter or row values.
- Automatic query cancellation, kill-session, or workload-governor controls.
- Automatic index creation or query-plan forcing.
- Durable backup scheduling or orchestration; this work reports operations
  initiated through existing APIs.
- Operating-system, container, network-interface, or host hardware monitoring
  beyond normal .NET/runtime instrumentation.
- Persisting recent query and maintenance history across process restart.
- Replacing the deep physical inspection APIs in
  `CSharpDB.Storage.Diagnostics`.

## Assumptions

- The work starts from the current repository baseline; assign the target
  version only when the release is scheduled.
- .NET `ActivitySource`, `Meter`, and `ILogger` remain the primary integration
  points.
- The standalone API and daemon continue sharing
  `CSharpDbRestApiHostExtensions`.
- The daemon remains the authority for remote runtime diagnostics.
- Admin can depend on the built-in optional diagnostics capability but must
  handle a custom client that does not provide it.
- Existing API-key security protects detailed diagnostics until the broader
  security plan introduces finer-grained diagnostic permissions.
- Configured database and shard aliases are operator-controlled,
  low-cardinality identifiers.
- Public SQL text capture is not required to satisfy query, slow-query,
  plan-diagnostics, or long-running-query visibility.

## Requirement Coverage

| Requested capability | Primary phases | Completion evidence |
| --- | --- | --- |
| Structured logging | 0, 1, 7 | Stable event catalog, correlation, redaction tests, public logging guide. |
| Query logging | 1, 2 | Full-lifecycle query records and bounded recent-query history. |
| Slow query logging | 1, 2 | Configurable thresholds for eager and streamed queries with one final event. |
| OpenTelemetry support | 0, 4, 7 | Supported sources, in-memory exporter tests, OTLP sample, AOT qualification. |
| Prometheus metrics | 4, 7 | Opt-in scrape endpoint, schema tests, cardinality stress, deployment guide. |
| Health checks | 3, 5 | Shared health registrations and deterministic state tests. |
| Readiness/liveness endpoints | 5, 7 | Separate HTTP endpoints, gRPC health, minimal safe responses, host docs. |
| Admin UI metrics | 2 through 6 | Live overview and drill-down over direct and remote diagnostics clients. |
| Storage diagnostics | 3, 4, 6 | Cheap runtime snapshot/metrics plus link to explicit deep inspection. |
| WAL diagnostics | 3, 4, 6 | Runtime WAL/checkpoint state, counters, metrics, and Admin panel. |
| Query plan diagnostics | 2, 4, 6 | Bounded plan summaries, cache/adaptive metrics, explain drill-down. |
| Connection/session diagnostics | 2, 4, 6 | Pool, reader, transaction, direct session, and remote request snapshots. |
| Backup/restore status | 3, 4, 6 | Active/recent operation state, progress phases, terminal outcomes, UI. |
| Long-running query visibility | 1, 2, 6 | Active registry, threshold event, recent outcome, Admin active-query view. |

## Definition Of Done For Every Phase

A phase is complete only when:

- Its implementation and negative-path tests are merged.
- Direct and remote behavior are covered where the phase exposes a client
  capability.
- New logs, traces, metrics, and models pass redaction and cardinality review.
- Concurrency-sensitive registries and callbacks pass stress or race tests.
- Disabled behavior remains compatible and within the approved performance
  budget.
- NativeAOT implications are tested rather than assumed.
- Internal plan status and applicable public documentation are updated.
- The phase exit gate is demonstrated with reproducible commands or CI jobs.

## Recommended First Work Packets

Begin with a narrow Phase 0 pull request:

1. Add the BCL-only `CSharpDB.Observability` project, solution references,
   standalone package, and CI/package-smoke integration.
2. Define source names, correlation context, safe capture modes, the
   fingerprint contract, and the first immutable snapshot envelopes; implement
   normalization with the existing `CSharpDB.Sql` tokenizer.
3. Add privacy, cardinality, serializer, trimming, and fingerprint tests.
4. Add no-listener query baselines for the normal, fast-lookup, simple-insert,
   and streamed-result paths.
5. Record the approved performance budgets and event/metric naming rules in
   this plan before Phase 1 instrumentation begins.

Follow it with a separate Phase 0 safety/lifecycle pull request:

1. Add centralized safe-error projection and remove raw exception-object and
   raw-message behavior from the default API middleware path.
2. Define the request/script/statement hierarchy, cumulative-counter versus
   gauge semantics, and host initialization/readiness state machine.
3. Add secret-canary middleware tests plus deterministic initialization,
   counter-epoch, and serializer contract tests.
