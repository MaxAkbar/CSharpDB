# CSharpDB.Storage.Diagnostics

Read-only storage diagnostics and integrity checking toolkit for [CSharpDB](https://github.com/MaxAkbar/CSharpDB) database files.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Storage.Diagnostics)](https://www.nuget.org/packages/CSharpDB.Storage.Diagnostics)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Storage.Diagnostics` provides read-only inspection and integrity verification for CSharpDB database files, WAL files, and indexes. Use it to validate database health, diagnose corruption, inspect page layouts, and verify index consistency. All operations are non-destructive and safe to run on production databases.

## Key Types

| Type | Description |
|------|-------------|
| `DatabaseInspector` | Inspects database files: validates headers, walks B+trees, produces page-type histograms, and reports issues |
| `WalInspector` | Validates WAL files: header checks, frame-by-frame validation (salt, checksums), commit marker detection |
| `IndexInspector` | Verifies index integrity: root page validity, table/column existence, B+tree reachability |
| `DatabaseInspectReport` | Report model with header info, page histogram, scanned pages, and issue list |

## Usage

### Database Inspection

```csharp
using CSharpDB.Storage.Diagnostics;

// Full database inspection
var report = await DatabaseInspector.InspectAsync("mydb.db");

Console.WriteLine($"Pages scanned: {report.PageCountScanned}");
Console.WriteLine($"Issues found: {report.Issues.Count}");

foreach (var issue in report.Issues)
{
    Console.WriteLine($"  [{issue.Severity}] {issue.Message}");
}

// Inspect a specific page
var page = await DatabaseInspector.InspectPageAsync("mydb.db", pageId: 3, includeHex: true);
```

### WAL Inspection

```csharp
// Validate WAL integrity
var walReport = await WalInspector.InspectAsync("mydb.db");

// Check for frame validation errors, salt mismatches, checksum failures
```

### Index Verification

```csharp
// Verify an index is consistent with its table
var indexReport = await IndexInspector.CheckAsync("mydb.db", "idx_users_email", sampleSize: 100);
```

## Installation

```
dotnet add package CSharpDB.Storage.Diagnostics
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Primitives` - shared type system
- `CSharpDB.Storage` - storage engine types and page format definitions

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Storage](https://www.nuget.org/packages/CSharpDB.Storage) | The storage engine this package inspects |
| [CSharpDB.Client](https://www.nuget.org/packages/CSharpDB.Client) | Client SDK surface that exposes diagnostics for direct and remote consumers |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
