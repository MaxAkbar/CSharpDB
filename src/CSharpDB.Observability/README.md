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
- Snapshot schema version: `1.0`
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

## Installation

```text
dotnet add package CSharpDB.Observability
```

For application development, the all-in-one `CSharpDB` package includes this
package transitively.

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
