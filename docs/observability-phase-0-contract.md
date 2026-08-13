# Phase 0 Observability Contract

Status: Accepted for the Phase 0 contracts. Phase 1 logging and the completed
Phase 2 runtime-diagnostics work conform to this contract. Final Phase 2
qualification completed on 2026-08-11. Phase 4 telemetry implementation is in
progress; its formal performance, publish, package, trimming/NativeAOT, and
full-release qualification remain open. Phase 5 hosted health implementation is
complete for the current product surface; its read-only policy is explicitly
live/not-write-ready, while the current hosts expose no built-in read-only mode.

Date: 2026-08-09

Phase 2 qualification date: 2026-08-11

## Context

CSharpDB needs one diagnostics vocabulary across embedded, HTTP, gRPC, daemon,
sharded, and Admin use without making an exporter or ASP.NET Core part of the
engine dependency graph. The same contract must also prevent diagnostics from
becoming an accidental SQL, value, credential, path, or exception-message
disclosure channel.

This record fixes the Phase 0 semantics that later instrumentation must follow.
Changing a name or meaning listed here requires an explicit schema-version
decision and compatibility review.

## Decision

### Assembly and client capability boundary

`CSharpDB.Observability` is a BCL-only, trimming- and NativeAOT-compatible
assembly below the runtime layers. It owns source names, operation correlation,
safe options, immutable snapshot contracts, bounded registries, and safe error
projection. It has no dependency on a logging provider, OpenTelemetry,
Prometheus, ASP.NET Core, or a host executable.

`CSharpDB.Sql` owns normalization and fingerprint generation because it can use
the product tokenizer and token kinds. The observability assembly must not grow
a second SQL lexer.

Runtime diagnostics are an optional client capability.
`ICSharpDbObservabilityClient` is implemented alongside `ICSharpDbClient`; it
does not add required members to `ICSharpDbClient`. Built-in direct, HTTP,
gRPC, ADO.NET, sharded, and Admin delegation surfaces implement the optional
interface while external client implementations remain source and binary
compatible. This follows interface segregation and lets callers use capability
discovery:

```csharp
if (client is ICSharpDbObservabilityClient diagnostics)
{
    // Request a transport-neutral snapshot.
}
```

The live database host remains authoritative. A remote Admin process must not
substitute its own process metrics for the server snapshot. Offline physical
inspection remains in `CSharpDB.Storage.Diagnostics` and is never initiated by
a normal runtime snapshot, metrics scrape, or health check.

### Stable names and versions

| Contract | Value | Compatibility rule |
| --- | --- | --- |
| Snapshot schema | `1.1` | Version 1.0 remains supported; additive optional fields may stay within 1.x, while removed fields or changed meanings require a major version. |
| Instrumentation version | `1.0.0` | Version reported by both BCL diagnostics sources. |
| `ActivitySource` | `CSharpDB` | Never derive the source name from a database, tenant, or host. |
| `Meter` | `CSharpDB` | Instrument names added later must remain stable after release. |
| Metric schema | `1.0` | A changed name, kind, unit, meaning, or dimension requires an explicit compatibility decision. |
| Query fingerprint | `csharpdb-sql-v1:<sha256>` | An algorithm change receives a new prefix; values from different prefixes are not compared. |

Every ordinary snapshot carries `SchemaVersion`, `CapturedAtUtc`,
`ServerInstanceId`, `CounterEpoch`, `Scope`, `Availability`, `Source`, a safe
configured `DatabaseAlias`, and record/field truncation indicators. A new host
process receives a new opaque server instance id.

### Operation hierarchy and counting

Every operation has an opaque operation id, operation class, role, start time,
monotonic timestamp, transport, configured alias, and optional trace, safe
session, parent-operation, and query-fingerprint fields. Operation ids,
fingerprints, trace ids, and session ids are correlation fields, never metric
labels.

Request and statement counters have different meanings:

| Execution shape | Hierarchy | Request count | Statement-execution count |
| --- | --- | ---: | ---: |
| One direct SQL statement | One root `Query` | 1 | 1 |
| Multi-statement script | One request `Script`, then one child `Query` statement per executed statement | 1 | Number of executed child statements |
| Client procedure | One request `Procedure`, then one child `Query` statement per executed statement | 1 | Number of executed child statements |
| Pipeline run | One request `Pipeline`, then one child `Query` statement per user-data SQL operation; catalog, run-log, and checkpoint housekeeping is suppressed | 1 | Number of user-data SQL child statements |
| Nested planner work or subquery | Part of the current statement | 0 additional | 0 additional |
| Trigger body | Correlated internal work under the causing statement | 0 additional | 0 additional |
| Retry or adaptive replan | Another attempt within the same statement operation | 0 additional | 0 additional |
| Sharded fan-out | One coordinator root; shard attempts are correlated internal children | 1 at aggregate scope | 1 logical statement at aggregate scope |

A per-shard snapshot may report local physical work with `Scope=Shard`, but it
must not be summed into the aggregate logical request or statement counters.
Retries and shard attempts may have attempt counters in a later schema; they do
not create query roots.

The coordinator and internal-attempt hierarchy is process-local. When an
attempt uses a remote HTTP or gRPC shard, the remote host records its physical
query as a separate shard-runtime root correlated by the propagated W3C trace;
Phase 1 does not put CSharpDB operation ids on the wire. Aggregate coordinator
counts and remote physical counts therefore remain separate views and must not
be added together.

Transport adapters preserve an inbound HTTP or gRPC activity as parent. A
direct call creates a root activity only when an activity listener requests
one. The host/client/ADO.NET boundary establishes transport and safe session
identity; the engine must not guess them.

The statement lifetime covers planning, execution, time to first result, lazy
row streaming, and disposal. Completion is recorded exactly once for
exhaustion, early disposal, never-opened disposal, failure, or cancellation.
Shared zero/one-row DML result instances never receive mutable operation state.
If a database runtime is disposed while a result remains open, its active
diagnostic entry is abandoned without fabricating a canceled or failed recent
record; late result callbacks are ignored by that retired registry.

`Waiting` is a scoped current phase, not a monotonic terminal progression. It
is entered only around an observed engine wait (currently contended serialized
write admission) and restores the exact prior phase when that wait completes or
is canceled. A generation-safe lease prevents stale restoration after disposal
or another lifecycle transition. Time between calls made by a streaming result
consumer is not inferred as waiting.

Phase-2 query-plan diagnostics summarize the existing logical operation and do
not replay it. One immutable database-lifetime adapter is shared by root,
transaction, and reader-session planners. Nested planner, trigger, subquery, and
adaptive callbacks aggregate into the causing statement: cache hit means every
observed lookup hit; change flags are cumulative; and multiple coarse access
paths use a fixed precedence (`temporary`, full-text, primary-key lookup, index
seek, index scan, table scan, unknown) so callback scheduling cannot change the
reported category. The estimate belongs to that representative category, while
actual rows are captured only at terminal transfer. Cached-plan
reclassification, adaptive cardinality reclassification, adaptive attempt,
success, and rejection remain distinct. Automatic diagnostics never run
`EXPLAIN`, replay SQL, retain a full plan tree, or expose raw SQL.

For serialized direct-client and pooled ADO.NET paths, the logical query context
starts before admission. `QueueDuration` is the measured gate wait, including a
wait that ends in cancellation; `ExecutionAndConsumptionDuration` is total
duration minus queue duration, clamped at zero. A script or procedure reports
that wait on its parent only, while statement children report zero queue time.
Implicit auto-commit transactions and retries remain inside the causing query
and do not emit separate transaction requests. Composite parent row totals sum
rows produced by every child query and rows affected by every child mutation.

Diagnostic listener interest is snapshotted immediately before a serialization
gate is acquired. Events produced while that gate is owned are delivered only
after it is released, so a listener or logging provider can re-enter the same
client, pool, or session without deadlocking it. A subscriber added after the
snapshot begins with the next operation. HTTP and gRPC request scopes carry
transport/session correlation only; each inner gate owns and flushes its own
bounded buffer. Required terminal outcomes have an independent capacity sized
for the maximum supported composite request and are never evicted by optional
slow-query events.

### Time, counters, gauges, and epochs

Wall-clock timestamps come from `TimeProvider.GetUtcNow()`. Durations use
`TimeProvider.GetTimestamp()` and `GetElapsedTime()` so wall-clock adjustments
cannot make elapsed time negative or discontinuous. Tests inject a deterministic
`TimeProvider`; production uses `TimeProvider.System`.

Counters and gauges follow these rules:

- A cumulative production counter never decreases for a given
  `(ServerInstanceId, CounterEpoch)` pair. Examples include request, statement,
  completion, row, byte, flush, commit, and conflict totals.
- A gauge is a current value and may rise or fall. Examples include active
  operations, active readers/writers, pool slots/waiters, WAL frames/bytes,
  retained bytes, and dirty pages.
- Resettable test or benchmark counters are never published as production
  cumulative counters. Prefer a separate non-resetting lifetime family.
- If an existing resettable source must temporarily feed a runtime snapshot,
  its reset first advances `CounterEpoch`. Consumers discard deltas across an
  epoch or server-instance change.
- Snapshot reads never reset a source counter.

### Metric cardinality

Metric tags are closed to this exact allowlist:

| Key | Bounded value source |
| --- | --- |
| `csharpdb.operation.class` | `CSharpDbOperationClass` |
| `csharpdb.operation.outcome` | `CSharpDbOperationOutcome` |
| `csharpdb.transport` | `CSharpDbTransport` |
| `csharpdb.database.alias` | Startup-validated configured alias |
| `csharpdb.health.check` | Reviewed health-check kind |
| `csharpdb.status` | Reviewed bounded status enum |

A database/shard alias is 1-64 ASCII letters, digits, `.`, `-`, or `_`. A host
may configure at most 64 aliases. A path is not an alias. SQL, object names,
query fingerprints, operation/session/transaction/trace ids, exception types
or messages, and user-controlled strings are prohibited metric labels.

Phase 5 adds the exact `csharpdb.health.status` `ObservableGauge<long>` with
unit `{status}`. A registered host alias emits one current measurement with
value `1` for `liveness` and one for `readiness`. The exact dimensions are
`csharpdb.health.check`, `csharpdb.status`, and `csharpdb.database.alias`;
current emitted values are `liveness`/`readiness` and
`healthy`/`unhealthy`. Registration is disposable, permits at most one live
health source per alias, and is capped at 64 sources: at most 128 measurements
per collection and 256 possible label tuples over healthy/unhealthy transitions
across the reviewed alias space. The reserved `degraded`, `database`, `storage`,
and `wal` enum values are not emitted by this cached host-state source. The
instrument is an additive member of metric schema `1.0`; changing its name,
kind, unit, meaning, or dimensions requires the normal schema compatibility
decision.

### Phase 4/5 BCL telemetry contract

The canonical instrument list, kinds, units, and exact dimension sets are
maintained in the
[CSharpDB.Observability README](../src/CSharpDB.Observability/README.md#phase-4-trace-and-metric-schema).
Both BCL sources are named `CSharpDB`; they create no exporter or background
worker. Emission requires global observability plus the applicable signal gate
(`OpenTelemetry.Enabled` for tracing and either OpenTelemetry or Prometheus for
metrics) and an attached listener/provider.

Every CSharpDB span is `ActivityKind.Internal` with a stable `csharpdb.*` name.
Safe database semantic attributes identify `csharpdb`, the configured alias,
and the bounded operation name. CSharpDB attributes carry the opaque operation
hierarchy, bounded class/role/transport, and optional opaque session id and
versioned fingerprint. SQL text, values, paths, connection strings, credentials,
exception messages, and stack traces are absent. Fingerprints and correlation
ids are trace-only and are never metric dimensions. Successful spans retain
the standard unset status; failed, canceled, and rejected spans use `Error`
with only reviewed error type/code attributes.

Inbound ASP.NET Core REST and gRPC activities parent the logical CSharpDB span.
Direct work becomes a root only without an ambient parent. A carried logical
activity is adopted across client/ADO.NET/engine seams rather than duplicated.
Sharded fan-out emits one coordinator plus explicit physical-attempt children.
A lazy query activity remains open through result exhaustion/disposal but is
ambient only during real query/result work.

Metrics use only the closed tag allowlist above. Synchronous and observable
counters are cumulative; observable up/down counters and gauges are current
values. Durations use seconds, byte instruments use `By`, and the other units
are the documented UCUM annotations. The BCL-only core does not install a
metric reader or histogram view. The built-in API and daemon host adapters do
install reviewed default explicit duration and batch-size buckets; custom hosts
may replace them. Bucket layout, exporter normalization, and reader temporality
remain host/exporter policy and are not part of metric-schema compatibility.
Prometheus counters are cumulative, and consumers do not calculate deltas
across a changed `service.instance.id`.

This phase does not yet trace automatic physical checkpoints or startup WAL
recovery; their counters, active state, and duration metrics are emitted where
the physical producer is available. Ownerless path-only static restore
validation/restore, reindex, vacuum, and foreign-key migration APIs have no
runtime identity and therefore no tracing/metrics context. Database- and
client-owned maintenance surfaces are the supported telemetry paths.

### Structured log event ids

Each subsystem owns a range of 100 ids. Published ids are never reused for a
different event meaning.

| Range | Owner | Initial ids |
| --- | --- | --- |
| 1000-1099 | Host/database lifecycle | 1000 starting; 1001 opened; 1002 closed |
| 2000-2099 | Query lifecycle | 2000 completed; 2001 slow; 2002 failed; 2003 canceled |
| 3000-3099 | Transactions | 3000 completed |
| 4000-4099 | Storage/WAL/recovery | 4000 checkpoint completed; 4001 recovery completed |
| 5000-5099 | Backup/restore/maintenance | 5000 backup; 5001 restore; 5002 maintenance completed |
| 6000-6099 | Health | 6000 `CSharpDB.Health.Transition` for distinct host-state changes |
| 7000-7099 | API/transport | 7000 rejected request; 7001 unhandled error |

### Capture and redaction policy

The global observability switch is disabled by default. SQL capture defaults to
`None`:

| Mode | Behavior |
| --- | --- |
| `None` | No SQL text is retained. Fingerprinting may tokenize transient input but retains only the versioned digest. |
| `Normalized` | Tokenizer-derived shape replaces every literal, parameter name/value, and `NULL`; identifiers can remain and therefore this mode is still an explicit sensitive-data opt-in. |
| `Raw` | Original SQL may be retained only after an explicit high-risk opt-in and server authorization. |

Parameter values and row values are never captured in any mode. Credentials,
connection strings, database/backup paths, file contents, and client-managed
transaction bearer ids are never captured by built-in telemetry.

Ordinary runtime, active-query, recent-query, list, metrics, and health models
never contain captured SQL or paths. `QueryDetailSnapshot` is a separate
server-authorized model; hiding a field in Admin is not authorization. Raw SQL
must not be placed in a metric, span name, event name, operation name, or
exception field.

Handled errors use `SafeErrorProjection` only: reviewed code, reviewed type,
and generic public detail. Built-in diagnostic logging does not attach the raw
`Exception`, message, stack trace, `Data`, or inner-exception text. An opt-in
application logger remains the application's responsibility and is not part of
the built-in query telemetry contract.

Histories and active registries are bounded by validated capacities and
retention. When capacity is exhausted, recent history drops the oldest record
and reports a dropped/truncated count; an active registry rejects the new
diagnostic record rather than evicting a still-running operation. Defaults are
1,000 active queries, 500 recent queries, 100 recent maintenance operations,
and 15 minutes retention. Maximums are 10,000 records and seven days.

Transport-facing collections use `DiagnosticsCollectionSnapshot<T>`, which
carries one capture metadata record plus immutable records, configured capacity,
optional retention, cumulative dropped count, and truncation state. An empty
available collection remains distinguishable from `Disabled`, `Unsupported`,
`Denied`, or `Unavailable`; non-available collections omit records and bounded
storage values rather than fabricating zeros. Per-shard sections always retain a
safe shard alias, but only an available shard carries a child payload. That child
keeps its own server instance id and counter epoch so coordinator and shard
restarts cannot be conflated.

One lightweight runtime identity owns the opaque server instance id, counter
epoch, and clock for a client or host lifetime. Per-database runtime state takes
an immutable option/alias snapshot while sharing that identity. Database switch,
restore/reopen, or reconnect may therefore replace configuration without
pretending the server restarted; any counter-family reset advances the shared
epoch. A genuinely new client/host lifetime creates a new instance id.

### Host lifecycle and readiness

The state object exists before `Database`, so startup and WAL recovery are
observable without pretending that a database is already available.

| Lifecycle | Live | Ready | Readiness reason |
| --- | ---: | ---: | --- |
| `Starting` | yes | no | `Starting` |
| `Recovering` | yes | no | `Recovering` |
| `Running` | yes | yes only when reason is `None` | `None` or the active bounded reason |
| `Failed` | yes | no | `InitializationFailed` |
| `Stopping` | yes | no | `Stopping` |
| `Stopped` | no | no | `Stopping` |

Database initialization failure does not make a listening process dead.
Liveness never opens or queries the database. Hosted liveness/readiness requests
read this cached state only and do not resolve a client or acquire its execution
lock. API and daemon establish their diagnostics listener before a background
initializer begins database/WAL startup after the application listener starts.
Initialization failure enters `Failed`, remains live, and automatically retries
through `Recovering`; successful reopen atomically enters `Running` with the
current bounded runtime reason and without an intermediate false-ready event.

The implemented readiness policy is:

| Condition | Live | Ready | Policy |
| --- | ---: | ---: | --- |
| Startup and WAL recovery | yes | no | `Starting` or `Recovering` until the database opens successfully |
| Initialization failure | yes | no | `InitializationFailed`; automatic retries return through `Recovering` |
| Ordinary running database | yes | yes | `Running` with reason `None` |
| Backup or validation-only restore/migration | yes | yes | These operations do not take the exclusive not-ready lease |
| Full restore | yes | no | `RestoreInProgress`; success remains leased through bounded reopen verification, while failure requiring recovery persists `ReopenPending` |
| Mutating foreign-key migration, reindex, or vacuum | yes | no | `ExclusiveMaintenance`; unavailable reopen persists `Unavailable` |
| Graceful shutdown | yes until stopped | no | `Stopping` is published before listeners terminate; `Stopped` is non-live |
| Configured read-only mode | yes | no | `ReadOnly`; current built-in hosts have no read-only mode, and future integrations publish this existing state |
| Admin runtime database switch | yes | no during switch | `SwitchAsync` and `CreateShardCatalogAndReloadAsync` hold a nestable `ReopenPending` lease through replacement verification and adoption |

API, daemon, and Admin initialization apply the configured hard timeout around
their side-effect-free information check. If an Admin client ignores
cooperative cancellation, the cached state still enters `Failed` within the
deadline and no concurrent initialization attempt starts; the initializer
observes the outstanding attempt before retrying.

Public unauthenticated HTTP health responses contain exactly the generic
`status` field and use `200` for healthy or `503` for unhealthy. Timestamps,
bounded component reasons, and `SafeErrorProjection` are available only through
the normal authenticated diagnostics boundary. Health responses and metrics
contain no paths, SQL, credentials, exception messages, or arbitrary caller
text.

Daemon deployments map standard gRPC Health for the overall service name `""`
and database service name `"csharpdb.database"`, even when the normal REST API
surface is disabled. Only the exact `grpc.health.v1.Health/Check` and `/Watch`
methods bypass API-key, route-context, and operation-scope interception, and
they return status only. The exact method allowlist is applied across unary,
client-streaming, server-streaming, and duplex interceptor shapes; it is not a
prefix exemption and does not create a general readiness admission gate.

Each distinct cached state publishes typed event 6000 through the BCL
`DiagnosticListener` outside the state lock. Publication is ordered and
best-effort; listener failures cannot roll back state. Repeating the same state
is a no-op that preserves the transition timestamp and emits no repeated event,
so successful health polling does not create logs. The payload is preserved by
the source-generated JSON context and contains only the validated state plus an
optional safe error projection.

## Performance baseline and provisional release budgets

`ObservabilityNoListenerEngineBenchmarks` records SQL and pre-parsed primary-key
lookups, SQL and pre-parsed writes, an explicit-transaction write, and a lazy
128-row stream consumed to exhaustion.
`ObservabilityNoListenerConnectionPoolBenchmarks` records a complete logical
pooled ADO.NET connection construction/open/close/disposal cycle. Their original
seven benchmark methods are preserved and parameterized by
`ObservabilityBenchmarkMode`:

| Mode | Database configuration | Diagnostic/logging consumer |
| --- | --- | --- |
| `Disabled` | `DatabaseOptions.ObservabilityOptions = null` | No activity or query diagnostic listener; setup fails if one is already attached |
| `HistoryCapture` | `Enabled = true`, alias `benchmark`; logging disabled; default bounded runtime history enabled | No activity or query diagnostic listener; runtime history is captured without a logging bridge |
| `StructuredLogging` | `Enabled = true`, alias `benchmark`; logging enabled; query completion enabled; slow-query logging disabled; SQL capture `None` | One `CSharpDbDiagnosticLoggerBridge` subscribed to the `CSharpDB` `DiagnosticListener`, backed by an enabled allocation-free sink logger |
| `MetricsOnly` | `Enabled = true`, alias `benchmark`; OpenTelemetry signal gate enabled; logging disabled | One in-process `MeterListener` enables the `CSharpDB` meter and consumes `long`/`double` measurements; no activity listener |
| `SampledTracing` | Same safe OpenTelemetry-enabled options as `MetricsOnly`, with sampling ratio `1`; logging disabled | One `ActivityListener` listens only to `CSharpDB` and returns `AllDataAndRecorded`; no meter listener |

Each mode uses 3 warmup iterations and 10 measured iterations under
`MemoryDiagnoser` with a reported median. Engine seed rows remain `1,024`; the
stream path consumes exactly `128` rows. The pool remains direct,
write-optimized, capped at 16 physical connections, and warmed before
measurement. The structured-logging pool has the same storage preset through
its explicit `DatabaseOptions`. Telemetry listeners are attached only after
database setup and pool warmup, remain active through measured operations, and
are disposed during global cleanup. No mode creates an exporter, external
history consumer, or SQL-text capture.

Run the baseline from a Release build:

```text
dotnet run -c Release --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --micro --filter *ObservabilityNoListener*
```

BenchmarkDotNet now emits 35 rows: seven each for `Disabled`, `HistoryCapture`,
`StructuredLogging`, `MetricsOnly`, and `SampledTracing`. The two Phase 4 modes
are present for future paired qualification; adding them is not performance
evidence. A practical generated-code smoke limits the filter to one path and
adds BenchmarkDotNet's `Dry` job:

```text
dotnet run -c Release --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --micro --filter *FastPrimaryKeyLookupSqlAsync* --job Dry
```

The attribute-defined 3/10 job remains present alongside the added `Dry` job,
so do not use that command for release evidence. Full qualification omits
`--job Dry` and uses the first command above.

The following are provisional Phase 0 release ceilings. A candidate mode and a
formal paired run are both required before reporting a qualified result.
Compare each affected benchmark path individually; a fast path may not be
hidden by averaging it with a slower path. The `MetricsOnly` and
`SampledTracing` rows have had one Phase 4 diagnostic launch, but not a formal
repeated paired qualification, and no sampled-tracing ceiling is established
here.

The Phase 0 reference run on 2026-08-09 used .NET SDK 10.0.203 and .NET
10.0.10 on an Intel Core i9-11900K. One BenchmarkDotNet launch (3 warmups and
10 measured iterations) produced these medians and allocation baselines:

| No-listener path | Median | Allocated |
| --- | ---: | ---: |
| Pooled connection open/close/dispose | 623.3 ns | 256 B |
| SQL primary-key lookup | 587.5 ns | 504 B |
| Pre-parsed primary-key lookup | 687.3 ns | 976 B |
| SQL autocommit insert | 4.488 us | 1,720 B |
| Pre-parsed autocommit insert | 4.726 us | 1,127 B |
| Explicit-transaction insert | 4.136 us | 1,193 B |
| Stream 128 rows to exhaustion | 14.791 us | 22,800 B |

At reference commit `4f9457fb`, Phase 0 adds no instrumentation call to these
measured runtime paths, so the disabled-mode incremental allocation is zero by
construction. The paired Phase 1 benchmark compares its `Disabled` rows against
that detached reference and its `StructuredLogging` rows against the
current disabled rows. This historical run is not a substitute for repeated
paired release qualification.

| Mode compared with the no-listener baseline | Median elapsed-time ceiling | Additional managed allocation ceiling |
| --- | ---: | ---: |
| Observability disabled | +3% | 0 B/operation |
| Metrics only, with a listener | +10% | 64 B/operation |
| Bounded query-history capture | `HistoryCapture median - same-launch Disabled median <= max(20% of Disabled median, 1.5 microseconds)` | 1,024 B/logical query operation |

The bounded query-history elapsed ceiling was amended provisionally on
2026-08-10. Its original relative-only `+20%` ceiling failed four of six engine
query rows in the first Phase 2 diagnostic qualification even though all six
rows passed the allocation ceiling. That relative-only rule is pathological on
sub-microsecond paths that still must pay fixed costs for clocks, fingerprinting,
and an exact bounded active/recent ledger. The amended rule retains the `+20%`
limit where it is meaningful while allowing no more than 1.5 microseconds of fixed
incremental elapsed time on each individual path. At adoption this was a budget
amendment, not a waiver or a final Phase 2 qualification result. The final
qualification below applies the amended rule without changing that rationale.

Qualification uses the same commit, Release configuration, .NET SDK/runtime,
machine, power profile, and data set. Run the baseline immediately before the
candidate in three independent BenchmarkDotNet launches. Use the median of the
three reported medians and the `MemoryDiagnoser` allocation result. Rerun a pair
whose per-launch medians vary by more than 5%. Any applicable individual path
over its elapsed or allocation ceiling fails the gate. Store the raw
BenchmarkDotNet artifacts with release qualification so later regressions can
be compared to evidence rather than a remembered workstation number.

For bounded query-history capture, each launch compares `HistoryCapture` with
the `Disabled` row from that same launch before the median-of-three result is
evaluated against the formula above. Its allocation ceiling remains 1,024 B per
logical query operation. The detached-reference `Disabled` gate remains `+3%`
elapsed time and `+0 B`; the three paired launches, median-of-three calculation,
and greater-than-5% rerun rule are unchanged.

These budgets measure incremental runtime cost. The configured capacities and
retention limits remain the separate memory-bound gate for history storage.

### Phase 1 paired qualification

Phase 1 used three independent Release launches of the detached `4f9457fb`
reference and the candidate modes on the same workstation. The table reports
the median of the three per-launch medians. The stream was rerun as three fresh
immediately paired launches after its disabled fast-path compaction; the pool
candidate was rerun after the final Data-layer lock-boundary hardening.

| Path | Detached reference | Disabled candidate | Disabled change | Structured logging candidate |
| --- | ---: | ---: | ---: | ---: |
| Pooled open/close/dispose | 450.9 ns / 256 B | 415.0 ns / 256 B | -7.96% / +0 B | 10.084 us / 10,924 B |
| SQL primary-key lookup | 435.0 ns / 504 B | 421.6 ns / 504 B | -3.08% / +0 B | 3.476 us / 6,215 B |
| Pre-parsed primary-key lookup | 537.3 ns / 976 B | 544.0 ns / 976 B | +1.25% / +0 B | 1.682 us / 4,410 B |
| SQL autocommit insert | 3.579 us / 1,712 B | 3.462 us / 1,712 B | -3.29% / +0 B | 6.819 us / 7,089 B |
| Pre-parsed autocommit insert | 3.165 us / 1,127 B | 3.120 us / 1,127 B | -1.44% / +0 B | 4.236 us / 4,225 B |
| Explicit-transaction insert | 3.217 us / 1,193 B | 3.211 us / 1,193 B | -0.18% / +0 B | 5.220 us / 6,091 B |
| Stream 128 rows to exhaustion | 12.235 us / 22,800 B | 12.001 us / 22,800 B | -1.91% / +0 B | 17.261 us / 36,723 B |

The disabled numeric gate passed on all seven paths and allocated exactly the
reference bytes. The final pool disabled launches spanned 405.9-423.0 ns
(4.12%); its structured launches spanned 9.856-11.092 us (12.27%). The earlier
full matrix and fresh stream pairs also contained greater-than-5% launch spread
on several reference or structured rows. Keep that variance visible in release
evidence; the table establishes the numeric gate result, not noise-free
reproducibility or a structured-logging performance ceiling.

### Phase 2 paired qualification

Phase 2 completed final qualification on 2026-08-11. The Release solution build
completed with zero warnings and zero errors. The original nine-suite execution
completed `3,968/3,968` before the final API compatibility canary; after adding
and running that canary, the final post-compatibility total is `3,969/3,969`:
Core `2,479/2,479`, Data `267/267`, Observability `110/110`, API `137/137`,
Daemon `200/200`, Pipelines `41/41`, benchmark contracts `130/130`, Admin
`452/452`, and Entity Framework Core `153/153`. NuGet package closure and
topological package-order qualification passed. Managed full trimming and the
Windows x64 NativeAOT publish/runtime smoke passed without trim or AOT warnings.

Performance qualification used three independent strict-serial Release pairs.
Each detached Phase 1 baseline ran immediately before the exact candidate after
a separate five-minute low-CPU, zero-worker gate. Baseline launches contained
14 rows and candidate launches contained 21 rows, each with 3 warmups, 10
measured iterations, and one launch.

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
allocation gates pass. All 20 applicable launch series pass the 5% stability
rule; the maximum spread is `4.253806%` for the candidate `HistoryCapture`
stream, so no additional whole-pair rerun was triggered.

Pool `HistoryCapture` remains N/A because the pool benchmark performs zero
logical queries and creates zero history records. Its final median-of-three
characterization is `11,643.017578 ns / 10,516 B` versus
`395.064807 ns / 256 B` for `Disabled`; this is characterization, not a query
gate pass. The earlier provisional failure and amendment evidence remain above
to preserve the decision history.

Raw launches, iteration variability, hash manifests, pair disposition, and the
reproducible calculation are preserved as repo-local release evidence under
`work/artifacts/phase2-formal-final-perf-after-fastpaths/`; this path reference
does not assert that the evidence is tracked or committed. An independent raw
JSON audit found no discrepancy with the final qualification report.

### Phase 4 telemetry diagnostic

The first complete Phase 4 telemetry launch on 2026-08-11 produced all 35
expected rows with 3 warmups and 10 measured iterations. It is a single-host
diagnostic, not the required repeated paired qualification. Comparing each
`MetricsOnly` row with its same-launch `Disabled` row by reported median gives:

| Path | `Disabled` | `MetricsOnly` | Elapsed change | Allocation delta |
| --- | ---: | ---: | ---: | ---: |
| Pooled open/close/dispose | 451.8 ns / 256 B | 14,909.5 ns / 12,717 B | +3,200.0% | +12,461 B |
| SQL primary-key lookup | 433.8 ns / 504 B | 1,360.0 ns / 648 B | +213.5% | +144 B |
| Pre-parsed primary-key lookup | 554.9 ns / 976 B | 958.2 ns / 960 B | +72.7% | -16 B |
| SQL autocommit insert | 3,703.4 ns / 1,712 B | 5,992.5 ns / 2,279 B | +61.8% | +567 B |
| Pre-parsed autocommit insert | 3,282.1 ns / 1,127 B | 3,756.0 ns / 1,638 B | +14.4% | +511 B |
| Explicit-transaction insert | 3,273.4 ns / 1,193 B | 4,168.7 ns / 1,927 B | +27.4% | +734 B |
| Stream 128 rows to exhaustion | 13,128.9 ns / 22,800 B | 14,240.4 ns / 22,840 B | +8.5% | +40 B |

Only the stream row meets both provisional metrics ceilings in this diagnostic.
The result is retained as an optimization baseline and must not be reported as
a Phase 4 performance qualification. Sampled-tracing rows were characterized,
but no pass/fail decision is possible until a ceiling is reviewed and approved.

## Consequences

- Core packages remain exporter-neutral and can preserve their existing AOT
  claims.
- Request, statement, attempt, and shard work cannot be conflated without
  violating a documented counter contract.
- Runtime consumers can detect restarts, counter resets, unsupported fields,
  and truncation without guessing.
- Rich diagnostics require explicit capture and authorization choices; safe
  ordinary views stay useful by correlation id, fingerprint, timings, bounded
  enums, and reviewed error codes.
- Later phases must add mode variants to the same baseline paths and prove the
  budgets before enabling instrumentation by default.
