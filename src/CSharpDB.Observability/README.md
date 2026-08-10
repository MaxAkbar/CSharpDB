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
| `QueryFingerprint` | Versioned, non-SQL fingerprint contract |
| `SafeErrorProjection` | Stable error code/type projection without exception messages |
| `RuntimeDiagnosticsSnapshot` | Immutable, versioned runtime snapshot envelope |
| `CSharpDbHostState` | Thread-safe startup, recovery, readiness, and shutdown state |
| `BoundedDiagnosticHistory<T>` | Capacity- and retention-bounded in-memory history |

## Installation

```text
dotnet add package CSharpDB.Observability
```

For application development, the all-in-one `CSharpDB` package includes this
package transitively.

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
