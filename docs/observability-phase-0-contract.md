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
| Nested planner work or subquery | Part of the current statement | 0 additional | 0 additional |
| Trigger body | Correlated internal work under the causing statement | 0 additional | 0 additional |
| Retry or adaptive replan | Another attempt within the same statement operation | 0 additional | 0 additional |
| Sharded fan-out | One coordinator root; shard attempts are correlated internal children | 1 at aggregate scope | 1 logical statement at aggregate scope |

A per-shard snapshot may report local physical work with `Scope=Shard`, but it
must not be summed into the aggregate logical request or statement counters.
Retries and shard attempts may have attempt counters in a later schema; they do
not create query roots.

Transport adapters preserve an inbound HTTP or gRPC activity as parent. A
direct call creates a root activity only when an activity listener requests
one. The host/client/ADO.NET boundary establishes transport and safe session
identity; the engine must not guess them.

The statement lifetime covers planning, execution, time to first result, lazy
row streaming, and disposal. Completion is recorded exactly once for
exhaustion, early disposal, never-opened disposal, failure, or cancellation.
Shared zero/one-row DML result instances never receive mutable operation state.

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
pooled ADO.NET connection construction/open/close/disposal cycle. The benchmark
worker intentionally attaches no activity listener, meter listener, exporter,
logging bridge, or history consumer and fails setup if the CSharpDB activity
source already has a listener.

Run the baseline from a Release build:

```text
dotnet run -c Release --project tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj -- --micro --filter *ObservabilityNoListener*
```

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

Phase 0 adds no instrumentation call to these measured runtime paths, so the
disabled-mode incremental allocation is zero by construction. This run records
the comparison baseline; it is not a substitute for the three paired launches
required below when Phase 1 first adds a disabled fast-path branch.

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
