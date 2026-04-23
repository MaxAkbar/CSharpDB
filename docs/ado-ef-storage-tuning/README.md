# ADO.NET and EF Core Embedded Storage Tuning

`CSharpDB.Data` and `CSharpDB.EntityFrameworkCore` now expose the same
embedded storage-engine tuning surface that already existed at the engine
level. That lets ADO.NET and EF Core callers opt into the read, write, and
hybrid performance presets documented in `src/CSharpDB.Storage/README.md`
without changing default behavior for existing applications.

## Summary

- No behavior changes unless you opt in.
- Connection strings expose a small convenience surface:
  `Storage Preset` and `Embedded Open Mode`.
- Full engine composition still lives on `DatabaseOptions` and
  `HybridDatabaseOptions`.
- EF Core can apply the same settings through the provider-specific
  `CSharpDbDbContextOptionsBuilder`.

## Public Surface

ADO.NET:

- `CSharpDbConnection.DirectDatabaseOptions`
- `CSharpDbConnection.HybridDatabaseOptions`
- additive `CSharpDbConnection` constructor overloads for direct and hybrid
  options
- `CSharpDbConnectionStringBuilder.StoragePreset`
- `CSharpDbConnectionStringBuilder.EmbeddedOpenMode`

EF Core:

- `UseDirectDatabaseOptions(DatabaseOptions)`
- `UseHybridDatabaseOptions(HybridDatabaseOptions)`
- `UseStoragePreset(CSharpDbStoragePreset)`
- `UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode)`

Connection-string keywords:

- `Storage Preset`
- `Embedded Open Mode`

Supported `Storage Preset` values:

- `DirectLookupOptimized`
- `DirectColdFileLookup`
- `HybridFileCache`
- `WriteOptimized`
- `LowLatencyDurableWrite`

Supported `Embedded Open Mode` values:

- `Direct`
- `HybridIncrementalDurable`
- `HybridSnapshot`

Both keywords parse case-insensitively.

## ADO.NET Examples

Use full direct options when you want exact engine control:

```csharp
using CSharpDB.Data;
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

await using var connection = new CSharpDbConnection(
    "Data Source=ingest.cdb",
    directOptions);

await connection.OpenAsync();
```

Use hybrid open mode plus direct options when you want a lazy-resident
file-backed runtime:

```csharp
using CSharpDB.Data;
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseDirectLookupOptimizedPreset());

var hybridOptions = new HybridDatabaseOptions
{
    PersistenceMode = HybridPersistenceMode.IncrementalDurable,
    HotTableNames = ["users", "sessions"],
};

await using var connection = new CSharpDbConnection("Data Source=app.cdb")
{
    DirectDatabaseOptions = directOptions,
    HybridDatabaseOptions = hybridOptions,
};

await connection.OpenAsync();
```

Use connection-string convenience keywords when a named preset is enough:

```csharp
await using var connection = new CSharpDbConnection(
    "Data Source=app.cdb;Storage Preset=WriteOptimized;Embedded Open Mode=HybridIncrementalDurable");

await connection.OpenAsync();
```

## EF Core Examples

Apply engine options directly:

```csharp
using CSharpDB.Engine;
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCSharpDb(
        "Data Source=app.cdb",
        csharpdb => csharpdb.UseDirectDatabaseOptions(directOptions))
    .Options;
```

Apply named presets and embedded open mode through the provider builder:

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseCSharpDb(
        "Data Source=app.cdb",
        csharpdb =>
        {
            csharpdb.UseStoragePreset(CSharpDbStoragePreset.WriteOptimized);
            csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.HybridIncrementalDurable);
        })
    .Options;
```

If you pass an existing `CSharpDbConnection`, EF Core validates that any
provider builder tuning matches the supplied connection. It does not mutate the
existing connection object.

## Precedence and Pooling

- Explicit `DirectDatabaseOptions` override `Storage Preset`.
- Explicit `HybridDatabaseOptions` override `Embedded Open Mode`.
- Connection-string preset and open-mode keywords only fill gaps when the
  corresponding explicit options object is absent.
- Pooling remains supported for file-backed embedded opens.
- Pool identity is options-aware in v1.
- The pool key includes the normalized file target, max pool size, effective
  embedded open mode, effective preset selection, and explicit options object
  identity.
- Separate explicit options object instances do not share a pool in v1 even if
  their contents are equivalent.
- `ClearPool(connectionString)` clears all pooled entries for the normalized
  file-backed target.

## Unsupported Cases

- Remote `Http`, `Grpc`, and `NamedPipes` connections reject embedded tuning.
- Named shared-memory databases reject embedded tuning.
- Private `:memory:` databases support direct tuning, but not hybrid open
  modes.
- EF Core still rejects pooled connections, endpoint connections, non-direct
  transports, and named shared-memory databases.

## Practical Preset Guidance

Use these as the first measurement targets:

- `WriteOptimized` for file-backed durable ingest and general write-heavy
  embedded workloads.
- `LowLatencyDurableWrite` only as a measure-first variant when you want to
  test deferred advisory planner-stat persistence.
- `DirectLookupOptimized` for hot local file-backed lookup workloads.
- `DirectColdFileLookup` for colder or cache-pressured direct file reads where
  memory-mapped clean-page reads help.
- `HybridFileCache` for explicit bounded file-cache experiments.

As of April 20, 2026, the repository benchmark guidance still treats
`WriteOptimized` as the stable first preset for file-backed durable write
paths, while `LowLatencyDurableWrite` remains a workload-specific experiment
rather than a blanket default change.
