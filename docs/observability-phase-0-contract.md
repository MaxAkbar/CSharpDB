# Phase 0 Observability Contract

Status: Accepted for the Phase 0 contracts. Runtime instrumentation and host
adapters remain later-phase work.

Date: 2026-08-09

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

Runtime diagnostics are an optional client capability. A later phase will add
`ICSharpDbObservabilityClient` alongside `ICSharpDbClient`; it will not add
required members to `ICSharpDbClient`. Built-in direct, HTTP, gRPC, and sharded
clients can implement the optional interface while external client
implementations remain source and binary compatible. This follows interface
segregation and lets callers use capability discovery:

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
| Snapshot schema | `1.0` | Additive optional fields may stay within 1.x; removed fields or changed meanings require a major version. |
| Instrumentation version | `1.0.0` | Version reported by both BCL diagnostics sources. |
| `ActivitySource` | `CSharpDB` | Never derive the source name from a database, tenant, or host. |
| `Meter` | `CSharpDB` | Instrument names added later must remain stable after release. |
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
| 6000-6099 | Health | 6000 transition |
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
Liveness never opens or queries the database. Readiness may perform a bounded,
side-effect-free check. Exclusive maintenance, restore, reopen-pending, and
bounded-check timeout keep readiness false. A restore is not ready until the
replacement database has reopened successfully. Public unauthenticated health
responses contain status only; safe failure details remain behind the normal
diagnostics security boundary.

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
| `StructuredLogging` | `Enabled = true`, alias `benchmark`; logging enabled; query completion enabled; slow-query logging disabled; SQL capture `None` | One `CSharpDbDiagnosticLoggerBridge` subscribed to the `CSharpDB` `DiagnosticListener`, backed by an enabled allocation-free sink logger |

Each mode uses 3 warmup iterations and 10 measured iterations under
`MemoryDiagnoser` with a reported median. Engine seed rows remain `1,024`; the
stream path consumes exactly `128` rows. The pool remains direct,
write-optimized, capped at 16 physical connections, and warmed before
measurement. The structured-logging pool has the same storage preset through
its explicit `DatabaseOptions`. No mode enables an `ActivityListener`, meter
listener, exporter, history consumer, or SQL-text capture.

Run the baseline from a Release build:

```text
dotnet run -c Release --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --micro --filter *ObservabilityNoListener*
```

BenchmarkDotNet emits one row per method and mode: seven `Disabled` rows and
seven `StructuredLogging` rows. A practical generated-code smoke limits
the filter to one path and adds BenchmarkDotNet's `Dry` job:

```text
dotnet run -c Release --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --micro --filter *FastPrimaryKeyLookupSqlAsync* --job Dry
```

The attribute-defined 3/10 job remains present alongside the added `Dry` job,
so do not use that command for release evidence. Full qualification omits
`--job Dry` and uses the first command above.

The following are provisional Phase 0 release ceilings. They become observed
budgets when the corresponding candidate modes exist. Compare each affected
benchmark path individually; a fast path may not be hidden by averaging it with
a slower path.

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
| Bounded query-history capture | +20% | 1,024 B/logical query operation |

Qualification uses the same commit, Release configuration, .NET SDK/runtime,
machine, power profile, and data set. Run the baseline immediately before the
candidate in three independent BenchmarkDotNet launches. Use the median of the
three reported medians and the `MemoryDiagnoser` allocation result. Rerun a pair
whose per-launch medians vary by more than 5%. Any applicable individual path
over its elapsed or allocation ceiling fails the gate. Store the raw
BenchmarkDotNet artifacts with release qualification so later regressions can
be compared to evidence rather than a remembered workstation number.

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
