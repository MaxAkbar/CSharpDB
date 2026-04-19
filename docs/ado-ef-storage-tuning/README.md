# ADO.NET and EF Core Embedded Storage Tuning Plan

`src/CSharpDB.Storage/README.md` already documents useful embedded read and write presets, but `CSharpDB.Data` and `CSharpDB.EntityFrameworkCore` do not yet expose those tuning paths cleanly. This document captures the planned API, behavior, and validation rules for bringing those storage-performance options into the ADO.NET and EF Core surfaces without changing current defaults.

## Summary

- Add first-class embedded tuning to `CSharpDbConnection` and `UseCSharpDb(...)` so callers can opt into the storage README's read, write, and hybrid-oriented performance paths.
- Keep existing behavior unchanged when callers do not opt in.
- Limit the convenience connection-string surface to named presets and embedded open mode selection; keep advanced tuning on full `DatabaseOptions` and `HybridDatabaseOptions`.

## Proposed Public API

- `CSharpDbConnection` gains `DirectDatabaseOptions` and `HybridDatabaseOptions` properties.
- `CSharpDbConnection` gains additive constructor overloads so callers can provide `DatabaseOptions` and `HybridDatabaseOptions` at construction time.
- `CSharpDbConnectionStringBuilder` gains a small convenience surface for preset and open-mode selection only.
- The new connection-string keywords are `Storage Preset` and `Embedded Open Mode`.
- `Storage Preset` values are `DirectLookupOptimized`, `DirectColdFileLookup`, `HybridFileCache`, `WriteOptimized`, and `LowLatencyDurableWrite`.
- `Embedded Open Mode` values are `Direct`, `HybridIncrementalDurable`, and `HybridSnapshot`.
- `CSharpDbDbContextOptionsBuilder` gains provider-specific builder methods for direct options, hybrid options, storage preset, and embedded open mode.
- The planned EF Core builder methods are `UseDirectDatabaseOptions(DatabaseOptions)`, `UseHybridDatabaseOptions(HybridDatabaseOptions)`, `UseStoragePreset(CSharpDbStoragePreset)`, and `UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode)`.

The following examples show planned API shape, not current behavior.

```csharp
using CSharpDB.Data;
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
    });

await using var connection = new CSharpDbConnection(
    "Data Source=ingest.cdb",
    directDatabaseOptions: directOptions);

await connection.OpenAsync();
```

```csharp
using CSharpDB.Data;
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseDirectLookupOptimizedPreset();
    });

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

```csharp
using Microsoft.EntityFrameworkCore;

optionsBuilder.UseCSharpDb(
    "Data Source=app.cdb;Storage Preset=DirectColdFileLookup;Embedded Open Mode=Direct",
    csharpdb =>
    {
        csharpdb.UseStoragePreset(CSharpDbStoragePreset.DirectColdFileLookup);
        csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.Direct);
    });
```

## Pooling and Configuration Rules

- Explicit `DirectDatabaseOptions` and `HybridDatabaseOptions` win over connection-string preset keywords.
- Connection-string preset and open-mode keywords only fill gaps when explicit options objects are absent.
- Pooling remains supported for file-backed direct opens.
- Pool identity becomes options-aware instead of relying only on normalized data source and max pool size.
- The options-aware pool key includes the effective embedded open mode and effective preset selection.
- Explicit options objects participate in pool identity by object instance in v1.
- Separate explicit options object instances do not share a pool in v1, even if their contents are equivalent.
- Pooling remains correctness-first in v1; maximal pool sharing across equivalent-but-distinct option instances is deferred.

## Unsupported in V1

- Remote `Http`, `Grpc`, and `NamedPipes` transports do not accept direct or hybrid tuning.
- Hybrid mode is file-backed direct only.
- Named shared-memory tuning is deferred.
- Private `:memory:` can use direct tuning, but it does not use hybrid mode.
- EF builder tuning does not mutate an existing supplied `CSharpDbConnection`.
- If EF Core is given an existing `CSharpDbConnection`, provider builder tuning is validated and conflicting configuration is rejected.

## Test Plan

- Verify a file-backed ADO.NET connection honors `DirectDatabaseOptions` with an externally visible read-path setting such as `UseMemoryMappedReads`.
- Verify a file-backed ADO.NET connection honors `HybridDatabaseOptions` and opens through the hybrid path.
- Verify a private `:memory:` ADO.NET connection honors `DirectDatabaseOptions`.
- Verify a private `:memory:` ADO.NET connection rejects `HybridDatabaseOptions`.
- Verify remote endpoint connections reject direct and hybrid tuning properties.
- Verify `Storage Preset` and `Embedded Open Mode` parse case-insensitively and map to the expected effective embedded settings.
- Verify explicit options override conflicting preset and open-mode values from the connection string.
- Verify pooled file-backed direct connections reuse the pool when the effective configuration matches.
- Verify pooled file-backed direct connections do not share a pool when explicit options object instances differ.
- Verify `ClearPool(connectionString)` still clears the relevant pooled entries for the normalized file-backed target.
- Verify `UseCSharpDb(connectionString, builder => ...)` flows direct, hybrid, preset, and open-mode settings into the created `CSharpDbConnection`.
- Verify `UseCSharpDb(existingConnection, builder => ...)` rejects conflicting provider builder tuning instead of mutating the supplied connection.
- Verify current rejection behavior for pooled EF connections, endpoint connections, non-direct transports, and named shared-memory inputs remains intact.
- Verify default behavior is unchanged when callers do not use any new tuning properties, builder methods, or connection-string keywords.

## Assumptions

- This is a design and handoff document only; it does not imply that the behavior exists yet.
- No behavior changes occur without explicit opt-in.
- Connection strings expose named presets and embedded open mode only.
- Advanced knobs such as durable group commit, WAL preallocation, hot-table lists, and `ImplicitInsertExecutionMode` remain on full options objects.
- Pooling favors correctness over maximal sharing in v1.
- Existing package READMEs remain unchanged in this step.
