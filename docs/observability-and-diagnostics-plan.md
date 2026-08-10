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

## Current Baseline

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

Status: `Not started`

Goal: emit correlated, structured lifecycle events for database operations
while keeping sensitive query data disabled by default.

Work:

- [ ] Add an `ILogger` bridge in the client/hosting layer that subscribes to
  strongly typed core events.
- [ ] Publish stable event ids and message templates for host startup,
  database open/close, query completion, slow query, query failure,
  cancellation, transaction completion, checkpoint, and maintenance events.
- [ ] Instrument all root `Database.ExecuteAsync` paths without double-counting
  cached statements, fast lookups, simple inserts, or explicit transactions.
- [ ] Instrument scripts and procedures as a parent operation plus child
  statements so a slow child is visible without double-counting a request.
- [ ] Carry the same root context through procedures, triggers, pipelines, and
  sharded fan-out.
- [ ] Attach query completion to `QueryResult` so streamed queries finish on
  exhaustion, failure, cancellation, or disposal, not when the result object is
  returned.
- [ ] Use a once-only terminal observer invoked directly from exhaustion,
  materialization, failure, cancellation, early disposal, and never-opened
  disposal paths. Do not attach mutable operation callbacks to the shared
  zero/one-row `QueryResult` instances.
- [ ] Record total duration, time to first result, queue duration,
  execution/result-consumption duration, rows produced or affected, outcome,
  operation class, query fingerprint, transport, and correlation ids.
- [ ] Add configurable query logging independent of the general operational
  log level.
- [ ] Add configurable slow-query logging with one threshold and optional
  per-operation overrides.
- [ ] Classify and emit a slow-query event once when the full logical query
  completes. Phase 2 adds the separate in-flight long-running notification.
- [ ] Ensure failure events expose stable error type/code fields without
  copying exception messages into metrics or default query history.
- [ ] Use the Phase 0 centralized safe-error projection in the logging bridge
  and all new events. Do not attach raw exception objects to query or
  maintenance events by default.
- [ ] Add a startup warning when raw SQL capture is explicitly enabled.
- [ ] Add log snapshot and secret-canary tests for direct, REST, gRPC, and API
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

## Phase 2: Query, Plan, Connection, And Session Visibility

Status: `Not started`

Goal: make active and recent query work, plan behavior, pools, readers,
transactions, and sessions inspectable through one bounded model.

Work:

- [ ] Add a lock-safe active-query registry keyed by opaque operation id.
- [ ] Track query phase (`queued`, `planning`, `executing`, `streaming`,
  `waiting`, or `disposing`), start time, elapsed time, operation class,
  fingerprint, outcome, transport, trace id, and safe session correlation.
- [ ] Add a bounded recent-query history with explicit capacity, retention,
  dropped-count, and truncation fields.
- [ ] Add a single registry sweep mechanism for long-running thresholds; do
  not allocate one timer per query.
- [ ] Emit the long-running event once while retaining the final slow-query
  completion event.
- [ ] Expose selected access-path category, plan-cache hit/miss,
  reclassification, adaptive reoptimization, estimated rows, and actual rows
  where they can be collected without replaying the query.
- [ ] Reuse `EXPLAIN ESTIMATE FOR` for explicit deep inspection and add a link
  from recent-query detail only when separately authorized captured SQL is
  available. A fingerprint is non-reversible and normalized SQL with redacted
  literals is not executable; otherwise require the operator to resubmit SQL.
- [ ] Never auto-run `EXPLAIN ANALYZE` from diagnostics. It executes the target
  and can mutate data; Admin must label that behavior explicitly.
- [ ] Keep automatic full-plan capture off by default and cap any requested
  plan tree by nodes and serialized bytes.
- [ ] Snapshot ADO.NET pool capacity, waiters, active/idle sessions, active
  readers, retired/poisoned pools, transaction owner, and oldest transaction
  age without exposing raw data-source paths.
- [ ] Model the pool accurately: active logical sessions, available slots,
  waiters, active snapshot readers, transaction owner/age, warm-engine idle,
  and disabled/poisoned/retiring state. The current `IdleCount` is not a count
  of retained logical sessions.
- [ ] Snapshot direct-client transaction sessions, active snapshot readers,
  exclusive maintenance state, and in-flight REST/gRPC requests.
- [ ] Add created/last-active/current-operation timestamps to remote
  transaction sessions and make abandoned non-expiring sessions visible, while
  replacing their bearer transaction ids with separate opaque diagnostic ids.
- [ ] Define aggregate and capped per-shard query/session snapshots.
- [ ] Add additive `ICSharpDbObservabilityClient` methods for summary, active
  queries, recent queries, query-plan diagnostics, and session diagnostics.
- [ ] Implement direct, HTTP, gRPC, and sharded transports with matching
  contracts.
- [ ] Make Admin's `DatabaseClientHolder` implement and delegate
  `ICSharpDbObservabilityClient`, including after database switches and
  reconnects.
- [ ] Add authenticated REST routes under `/api/diagnostics` and matching gRPC
  methods.
- [ ] Enforce API authentication when configured and loopback-only access when
  security mode is `None`, unless the operator explicitly enables and
  acknowledges insecure remote diagnostics.
- [ ] Preserve the existing HTTP SQL/transaction and gRPC cancellation paths;
  propagate request cancellation through maintenance and inspection endpoints,
  and pass the already-accepted token through direct inspector helpers.
- [ ] Document that parsing/planning and synchronous fast paths may not observe
  cancellation immediately. Keep ADO.NET `CommandTimeout`/`Cancel()` and query
  termination explicitly unsupported until they are implemented and tested.
- [ ] Add a separate authorized query-detail request for any captured SQL or
  path field. Never include those fields in summary, active, or recent lists.
- [ ] Define consistent behavior when a custom client does not implement the
  optional diagnostics capability.

Deliverables:

- Long-running and recent query visibility.
- Query-plan runtime diagnostics.
- Connection, pool, reader, transaction, and session diagnostics.
- Direct/REST/gRPC/sharded diagnostics parity.

Exit gate:

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

Status: `Not started`

Goal: turn existing internal storage counters and maintenance results into safe
live operational snapshots without converting offline inspection into a hot
path.

Work:

- [ ] Define a live storage snapshot for database size, page counts, cache
  usage/hits/misses, dirty pages, active readers/writers, commits, conflicts,
  checkpoint state, and cumulative I/O where available.
- [ ] Distinguish logical pager I/O from physical device I/O and represent
  unsupported fields as `Unavailable` or `NotApplicable`, especially for
  in-memory, hybrid, and custom storage implementations.
- [ ] Promote existing WAL flush and commit-path counters through a public
  curated immutable runtime model without exposing the large, unstable
  benchmark snapshot wholesale or changing its reset semantics. Publish new
  non-resetting lifetime counters, or advance the counter epoch when test-only
  reset APIs run.
- [ ] Add current WAL logical bytes, allocated/file bytes, committed-frame
  bytes, retained bytes, coherent frame/commit state, pending commit count,
  last successful flush/checkpoint, recovery state, and last safe error code.
  Frame count and WAL sizes are gauges and may decrease after checkpoint.
- [ ] Model checkpoint phases (`Idle`, `Requested`, `Copying`,
  `CopyCompleteAwaitingReaders`, `Finalizing`, and `Faulted`), progress,
  retained-WAL reason, foreground/background origin, and last start/success/
  failure. Record foreground auto-checkpoint and shutdown checkpoint failures
  that are currently swallowed or retried.
- [ ] Instrument recovery through storage-engine options/factories so startup
  work before `Database` construction is visible. Record scanned/recovered/
  discarded frames and bytes, safe truncation reason, duration, and outcome
  without exposing the WAL path.
- [ ] Instrument missing checkpoint, fsync, write, group-commit, cache, and
  recovery counters with lock-free or low-contention updates.
- [ ] Use an optional cache-diagnostics provider or a pager-owned counter seam
  so third-party storage/cache implementations do not gain required members.
- [ ] Do not use `IPageOperationInterceptor` as the production metric hot path;
  enabling it changes cache fast paths. Use pager-owned counters and optional
  diagnostics providers for custom caches/devices instead.
- [ ] Keep `DatabaseInspector`, `WalInspector`, page inspection, and index
  checks explicit and separately labeled as offline/deep inspection.
- [ ] Add a bounded maintenance-operation registry for checkpoint, backup,
  restore validation, restore, reindex, vacuum, and foreign-key migration.
- [ ] Report operation id, phase, start time, elapsed time, progress units when
  known, outcome, warning/error counts, and safe failure code.
- [ ] Instrument `DatabaseBackupCoordinator` so an in-progress backup or
  restore is visible before the original call completes.
- [ ] Register backup/restore before waiting for the client execution or
  exclusive-access lock. Include queued/acquiring time and explicit phases for
  checkpoint, copy/stage, validation, hashing/manifest work, replacement,
  rollback, and reopen.
- [ ] Retain a bounded recent operation history in memory and clearly report
  that it resets with the process.
- [ ] Suppress source and destination paths by default; use configured aliases
  or final file names only under an explicit path-capture option.
- [ ] Define restore readiness behavior while exclusive access is being
  acquired, validation is running, the database is being replaced, and the
  replacement is reopening.
- [ ] Define the restore point of no return and cancellation behavior around
  non-cancellable file replacement. Eagerly reopen before terminal success, or
  publish `ReopenPending` and remain not-ready until a deferred reopen passes.
- [ ] Pass cancellation through explicit deep storage/WAL/page/index inspectors
  even though those inspectors remain outside normal runtime snapshots.
- [ ] Add summary, storage, WAL, and maintenance-operation methods to the
  optional diagnostics client and both remote transports.
- [ ] Define sharded storage aggregation rules and preserve capped per-shard
  records for diagnosis.

Deliverables:

- Live storage and WAL diagnostics.
- Current and recent backup/restore/maintenance status.
- Transport-neutral snapshot models and endpoints.

Exit gate:

- Snapshot collection does not scan the database or WAL files unless the caller
  explicitly requests deep inspection.
- Counters remain monotonic and consistent during concurrent commits,
  checkpoints, backup, restore, and shutdown.
- Resettable benchmark diagnostics cannot make production counters decrease;
  gauges and epochs change according to the published contract.
- WAL recovery and checkpoint progress/failures are visible even when they
  occur before `Database` construction or are retried internally.
- A running backup or restore is visible, reaches one terminal state, and
  cannot remain permanently active after failure or cancellation.
- Restore never reports ready after replacement until the new database has
  reopened successfully.
- Default models and logs contain no database or backup paths.

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
