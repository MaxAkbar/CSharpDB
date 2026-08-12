# CSharpDB.Observability

BCL-only observability contracts and safe runtime-diagnostics models for
[CSharpDB](https://github.com/MaxAkbar/CSharpDB).

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Observability)](https://www.nuget.org/packages/CSharpDB.Observability)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Observability` defines the versioned vocabulary shared by embedded,
hosted, and remote CSharpDB diagnostics. It contains no exporter, ASP.NET Core,
logging, or OpenTelemetry package dependency. Exporters and host integrations
remain opt-in at application boundaries.

Safe defaults never capture SQL text, parameter values, row values, credentials,
connection strings, or file paths. SQL normalization and fingerprinting are
implemented by `CSharpDB.Sql`, which uses the product tokenizer rather than a
second SQL parser.

## Contract highlights

- Activity and metric source name: `CSharpDB`
- Snapshot schema version: `1.1` (1.0 payloads remain supported)
- Metric schema version: `1.0`; instrumentation version: `1.0.0`
- SQL capture default: `None`
- Metric dimensions: reviewed bounded enums plus a validated configured alias
- Ordinary snapshots never contain raw SQL, values, paths, or exception text
- Cumulative counters are monotonic within a server-instance/counter-epoch pair

The complete hierarchy, counter, privacy, host-state, and performance contract
is recorded in the
[Phase 0 observability contract](https://github.com/MaxAkbar/CSharpDB/blob/main/docs/observability-phase-0-contract.md).

## Key types

| Type | Purpose |
|------|---------|
| `CSharpDbObservabilityOptions` | Coherent configuration model with dependency-free validation |
| `CSharpDbDiagnostics` | Stable schema, `ActivitySource`, and `Meter` names |
| `CSharpDbOperationContext` | Opaque operation correlation and request/statement hierarchy |
| `CSharpDbOperationScope` | Async parent-operation and transport propagation without creating an `Activity` |
| `CSharpDbLogEvents` | Stable event ids, names, categories, message templates, and typed payload contracts |
| `CSharpDbDiagnosticEventPublisher` | No-throw, listener-gated typed event publication |
| `QueryFingerprint` | Versioned, non-SQL fingerprint contract |
| `SafeErrorProjection` | Stable error code/type projection without exception messages |
| `RuntimeDiagnosticsSnapshot` | Immutable, versioned runtime snapshot envelope |
| `DiagnosticsValueSnapshot<T>` | Identified optional value with explicit availability |
| `DiagnosticsCollectionSnapshot<T>` | Identified bounded collection with explicit availability, capacity, retention, drops, and truncation |
| `DiagnosticsTopologySnapshot<T>` | Single-instance or aggregate-plus-shard topology envelope |
| `RuntimeDiagnosticsFamilySection<T>` | Current and retained runtime-family values without conflating counter epochs |
| `ShardDiagnosticsSection<T>` | Safe per-shard availability wrapper that preserves the child instance identity |
| `ActiveQuerySnapshot`, `RecentQuerySnapshot` | Safe bounded query-ledger records |
| `QueryPlanDiagnosticsSnapshot` | Bounded automatic plan summary that never replays SQL |
| `QueryDetailSnapshot` | Separately requested captured query text, subject to capture and host policy |
| `ConnectionDiagnosticsSnapshot`, `SessionDiagnosticsSnapshot` | Safe physical-owner and logical-session state |
| `CSharpDbHostState` | Thread-safe startup, recovery, readiness, and shutdown state |
| `BoundedDiagnosticHistory<T>` | Capacity- and retention-bounded in-memory history |

## Phase 1 structured event contract

Core instrumentation publishes immutable payloads through the `CSharpDB`
`DiagnosticListener`; it has no dependency on a logging framework. Hosting and
client layers can bridge those events to `Microsoft.Extensions.Logging`.
Definitions in `CSharpDbLogEvents` carry the stable numeric id, event name,
category, and reviewed message template. Phase 1 reserves these ranges:

| Category | Event-id range | Current events |
| --- | ---: | --- |
| Host | 1000-1099 | host starting, database opened/closed, raw-SQL-capture warning |
| Query | 2000-2099 | completed, slow, failed, canceled, `LongRunningQuery` (2004) |
| Transaction | 3000-3099 | transaction completed |
| Storage | 4000-4099 | checkpoint and recovery completed |
| Maintenance | 5000-5099 | backup, restore, and maintenance completed |
| Health | 6000-6099 | health transition |
| API | 7000-7099 | request rejected and unhandled error |

Query terminal payloads contain an immutable `CSharpDbOperationContext`, UTC
completion time, total/time-to-first-result/queue/execution-and-consumption
durations, rows produced and affected, outcome, and a `SafeErrorProjection`
when applicable. Stable structured fields include:

- `csharpdb.operation.id`, `csharpdb.operation.parent_id`,
  `csharpdb.operation.class`, `csharpdb.operation.role`, and
  `csharpdb.operation.outcome`
- `csharpdb.database.alias`, `csharpdb.transport`, `csharpdb.session.id`,
  `trace.id`, and `csharpdb.query.fingerprint`
- `csharpdb.query.duration_ms`, `csharpdb.query.time_to_first_result_ms`,
  `csharpdb.query.queue_duration_ms`,
  `csharpdb.query.execution_consumption_ms`,
  `csharpdb.query.rows_produced`, and `csharpdb.query.rows_affected`
- `error.code` and `error.type`

No built-in payload contains a raw exception, exception message, parameter or
row value, connection string, credential, or path. `Logging.SqlText` defaults
to `None`. `Normalized` captures tokenizer-normalized SQL; `Raw` is an explicit
sensitive-data opt-in. Hosts publish warning event
`CSharpDB.Host.RawSqlCaptureEnabled` (id `1003`) at startup when raw capture is
active.

Publishers check `DiagnosticListener.IsEnabled` before allocating a payload and
isolate listener/filter failures. Ambient operation and transport scopes nest
and restore through `AsyncLocal`; they do not replace an inbound `Activity`.
Listener interest around serialization locks is snapshotted before admission,
then buffered events are flushed after the lock is released. Correlation-only
HTTP/gRPC scopes do not own a request-wide buffer; each inner lock boundary
remains independently bounded.

## Phase 2 runtime collection contract

Runtime collection responses carry one exact capture metadata value shared by
every returned record. Available collections may be empty, but still report
their configured capacity, optional retention, dropped count, and truncation.
`Disabled`, `Unsupported`, `Denied`, and `Unavailable` collections omit those
bounded values instead of returning ambiguous zeroes.

Per-shard responses always expose only a validated shard alias. An available
child retains its own opaque server-instance id and counter epoch; an
unavailable child has no fabricated payload. A host identity can be retained
across database switch/reopen while the immutable per-database alias/options
snapshot is replaced. This lets consumers distinguish configuration change,
counter reset, and a genuine server restart.

`DiagnosticsTopologySnapshot<T>` represents either one exact instance or a
coordinator aggregate with capped per-shard children. Aggregate collection
views are bounded, but physical shard counters are not summed across distinct
server lifetimes or counter epochs. A reachable shard may truthfully be
`Available`, `Disabled`, `Unsupported`, `Denied`, or `Unavailable` without
fabricating a child payload.

Query-plan diagnostics retain only a bounded summary and never execute or
replay SQL. Ordinary runtime, active-query, recent-query, plan, connection, and
session snapshots remain safe. SQL text, when capture is explicitly enabled,
is available only through the separate query-detail value and is subject to an
additional host-owned authorization policy. Authentication, loopback policy,
HTTP/gRPC status mapping, and cancellation are deliberately implemented by the
API/client host layers rather than this BCL-only contracts package.

Accepted cancellation tokens are preserved by the contracts and forwarded by
built-in producers and transports, but cancellation remains cooperative. These
models expose runtime state only; they do not define query/session termination,
ADO.NET command timeout enforcement, or `DbCommand.Cancel()` behavior.

## Phase 4 trace and metric schema

The BCL sources have no consumer-visible emission unless runtime options enable
the signal and a listener is attached; enabling a signal may still maintain its
bounded runtime counters before a listener arrives. Every CSharpDB activity
uses `ActivityKind.Internal` and one low-cardinality name: `csharpdb.query`,
`csharpdb.script`, `csharpdb.procedure`, `csharpdb.transaction`, `csharpdb.database`,
`csharpdb.recovery`, `csharpdb.checkpoint`, `csharpdb.backup`,
`csharpdb.restore`, `csharpdb.reindex`, `csharpdb.vacuum`,
`csharpdb.maintenance`, `csharpdb.pipeline`, or the fallback
`csharpdb.operation`.

Start attributes are `db.system.name=csharpdb`, `db.namespace`,
`db.operation.name`, `csharpdb.schema.version=1.1`, `csharpdb.operation.id`,
`csharpdb.operation.class`, `csharpdb.operation.role`, `csharpdb.transport`,
and `csharpdb.database.alias`. `csharpdb.operation.parent_id`,
`csharpdb.session.id`, `csharpdb.query.fingerprint`, and
`csharpdb.maintenance.kind` are present only when applicable.
`db.operation.name` uses `QUERY`, `SCRIPT`, `CALL`, `TRANSACTION`, `DATABASE`,
`RECOVERY`, `CHECKPOINT`, `BACKUP`, `RESTORE`, `REINDEX`, `VACUUM`,
`MAINTENANCE`, `PIPELINE`, or `OPERATION`. Roles are `root`, `request`,
`statement`, `internal`, or `unknown`; transports are `embedded`, `direct`,
`http`, `grpc`, `tcp`, `namedpipe`, `sharded`, or `unknown`. Operation classes
are `query`, `script`, `procedure`, `transaction`, `database`, `recovery`,
`checkpoint`, `backup`, `restore`, `reindex`, `vacuum`, `maintenance`,
`pipeline`, or `unknown`.

Completion adds `csharpdb.operation.outcome` (`succeeded`, `failed`,
`canceled`, `rejected`, or the defensive fallback `unknown`). Query completion
may also add `csharpdb.query.rows_produced`, `csharpdb.query.rows_affected`,
`csharpdb.query.queue_duration_ms`,
`csharpdb.query.time_to_first_result_ms`, and `csharpdb.query.slow`.
Maintenance completion may add `csharpdb.maintenance.completed_units`,
`csharpdb.maintenance.total_units`, `csharpdb.maintenance.warning_count`, and
`csharpdb.maintenance.error_count`. A successful operation leaves the standard
activity status unset. Failed, canceled, and rejected operations use `Error`
plus only reviewed `error.type` and `csharpdb.error.code`; exception messages
and stack traces are never attached. Traces never attach statement text,
including when a separate logging capture mode is enabled.

Direct calls become roots only when there is no ambient parent. REST and gRPC
operations are children of ASP.NET Core's inbound server activity. A sharded
logical query has one coordinator span and only explicit physical-attempt child
spans; the engine adopts a carried activity rather than creating a duplicate.
Lazy results keep their activity running until exhaustion, failure, or disposal,
but make it ambient only while actual result work is executing.

The following table is the metric schema. `counter` means cumulative,
`up/down` and `gauge` mean current value, and `histogram` records a distribution.
Braced units are UCUM annotations.

In the table, operation class, outcome, transport, and alias abbreviate the
exact keys `csharpdb.operation.class`, `csharpdb.operation.outcome`,
`csharpdb.transport`, and `csharpdb.database.alias`.

| Instruments (kind; unit) | Dimensions |
| --- | --- |
| `csharpdb.requests` (counter; `{request}`), `csharpdb.statements` (counter; `{statement}`), `csharpdb.query.duration` (histogram; `s`), `csharpdb.rows.produced` and `csharpdb.rows.affected` (counter; `{row}`), `csharpdb.queries.slow` (counter; `{query}`) | operation class, outcome, transport, alias |
| `csharpdb.queries.active` (observable up/down; `{query}`) | alias |
| `csharpdb.transactions` (counter; `{transaction}`), `csharpdb.transaction.duration` (histogram; `s`) | operation class, outcome, transport, alias |
| `csharpdb.transactions.active` (observable up/down; `{transaction}`) | alias |
| `csharpdb.maintenance.operations` (counter; `{operation}`), `csharpdb.maintenance.duration` (histogram; `s`) | operation class, outcome, transport, alias |
| `csharpdb.maintenance.active` (observable up/down; `{operation}`) | operation class, alias |
| `csharpdb.checkpoints` (counter; `{checkpoint}`), `csharpdb.checkpoint.duration` (histogram; `s`) | outcome, alias |
| `csharpdb.checkpoints.active` (observable up/down; `{checkpoint}`), `csharpdb.checkpoint.age` (observable gauge; `s`) | alias |
| `csharpdb.wal.recoveries` (counter; `{recovery}`), `csharpdb.wal.recovery.duration` (histogram; `s`) | outcome, alias |
| `csharpdb.wal.recoveries.active` (observable up/down; `{recovery}`) | alias |
| `csharpdb.wal.commit.batch.size` (histogram; `{commit}`) | alias |
| `csharpdb.storage.logical_bytes`, `csharpdb.storage.allocated_bytes` (observable gauge; `By`), `csharpdb.storage.page_count`, `csharpdb.storage.dirty_pages` (observable gauge; `{page}`) | alias |
| `csharpdb.storage.page.reads`, `csharpdb.storage.page.writes`, `csharpdb.storage.cache.hits`, `csharpdb.storage.cache.misses` (observable counter; `{page}`) | alias |
| `csharpdb.storage.bytes.read`, `csharpdb.storage.bytes.written` (observable counter; `By`) | alias |
| `csharpdb.storage.readers.active` (observable up/down; `{reader}`), `csharpdb.storage.writers.active` (observable up/down; `{writer}`) | alias |
| `csharpdb.storage.commits` (observable counter; `{commit}`), `csharpdb.storage.conflicts` (observable counter; `{conflict}`) | alias |
| `csharpdb.wal.logical_bytes`, `csharpdb.wal.allocated_bytes`, `csharpdb.wal.committed_bytes`, `csharpdb.wal.retained_bytes` (observable gauge; `By`), `csharpdb.wal.frame_count` (observable gauge; `{frame}`) | alias |
| `csharpdb.wal.commit_batches` (observable counter; `{batch}`), `csharpdb.wal.bytes.written` (observable counter; `By`), `csharpdb.wal.commits.flushed` (observable counter; `{commit}`), `csharpdb.wal.flushes` (observable counter; `{flush}`), `csharpdb.wal.group_commit.batches` (observable counter; `{batch}`), `csharpdb.wal.group_commit.commits` (observable counter; `{commit}`) | alias |
| `csharpdb.wal.commits.pending` (observable up/down; `{commit}`) | alias |
| `csharpdb.sessions.active` (observable up/down; `{session}`), `csharpdb.readers.active` (observable up/down; `{reader}`), `csharpdb.pool.waiters` (observable up/down; `{request}`), `csharpdb.connections.available` (observable gauge; `{connection}`) | transport, alias |
| `csharpdb.pool.wait.duration` (histogram; `s`) | outcome, transport, alias |

The closed tag-key allowlist remains `csharpdb.operation.class`,
`csharpdb.operation.outcome`, `csharpdb.transport`,
`csharpdb.database.alias`, `csharpdb.health.check`, and `csharpdb.status`; the
last two are reserved for the health phase and are not added to the metrics
above. SQL, fingerprints, operation/session/trace ids, object names, paths,
exception types/messages, and arbitrary user strings are prohibited metric
dimensions. Prometheus export disables exemplars so trace ids do not appear in
the pull surface. The registry is capped at 64 configured aliases and 64 live
runtime families. Sources with the same validated alias and tag tuple
aggregate; an unavailable optional producer omits its measurement rather than
publishing a fabricated zero.

Duration histograms report seconds and WAL batch size reports logical commits.
The BCL-only core does not install a metric reader or bucket view. The built-in
API and daemon host adapter installs reviewed default explicit buckets for the
six duration histograms and the WAL commit-batch-size histogram; custom hosts
may replace them. Those hosted defaults, exporter-specific name normalization,
and reader temporality are deployment policy rather than part of the canonical
instrument schema. Prometheus counters are cumulative. Other readers must
preserve the distinction between cumulative counters and current-value gauges
and must not compute a delta across a changed `service.instance.id`.

`MetricSchemaVersion` is `1.0`. Adding an instrument is additive; changing an
existing name, kind, unit, meaning, or allowed dimensions requires an explicit
schema-version and compatibility decision. `InstrumentationVersion` is the
version advertised by both BCL sources. Activity payloads carry the snapshot
schema in `csharpdb.schema.version`; it is not a metric label.

### Enablement and support boundary

| Global `Enabled` | OpenTelemetry | Prometheus | Result in a configured API/daemon host |
| --- | --- | --- | --- |
| `false` | `false` | `false` | No CSharpDB runtime history, tracing, metrics provider, exporter, or scrape route. |
| `true` | `false` | `false` | Configured history/logging can run; no CSharpDB activity or metric provider is registered. |
| `true` | `true` | either | The host registers the CSharpDB activity source and meter. Parent-based ratio sampling applies; console and OTLP remain separate opt-ins. |
| `true` | `false` | `true` | Metrics and the protected exact scrape route are registered without a CSharpDB tracing provider. |

Enabling OpenTelemetry or Prometheus while global observability is disabled is
invalid. `Otlp.Enabled=false` creates no OTLP exporter, connection, or retry
loop. Merely setting `OpenTelemetry.Enabled=true` creates in-process providers
but no export destination unless console or OTLP is also enabled. Embedded
applications may attach their own BCL listeners; this package never creates an
exporter or background worker.

Hosted sampling is parent-based over the configured trace-id ratio (`0` through
`1`). Hosted resources publish `service.name`, optional `service.namespace` and
`service.version`, an opaque process-lifetime `service.instance.id`, and
`deployment.environment.name`. API and daemon supply their application name,
informational version, and ASP.NET Core environment as safe defaults. Console
and OTLP exporters are independent opt-ins; OTLP endpoints, protocol, headers,
and timeout use the standard `OTEL_EXPORTER_OTLP_*` environment variables.

Logical query/transaction/database lifecycle and runtime-owned checkpoint,
backup, restore, reindex, vacuum, and generic maintenance paths are traced.
Automatic physical checkpoints and startup WAL recovery currently contribute
metrics but do not create physical `Activity` spans. Ownerless path-only static
restore validation/restore, reindex, vacuum, and foreign-key migration APIs have
no runtime identity from which to enable or correlate telemetry; use the
database/client-owned surfaces when telemetry is required. These limitations
must not be presented as traced support.

## Installation

```text
dotnet add package CSharpDB.Observability
```

For application development, the all-in-one `CSharpDB` package includes this
package transitively.

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
