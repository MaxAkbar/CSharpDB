# What's New

## v2.4.0

### ADO.NET Metadata and Provider Ergonomics

- Implemented `DbConnection.GetSchema()` metadata collections for the `CSharpDB.Data` provider, including `MetaDataCollections`, `Tables`, `Columns`, `Indexes`, and `Views`.
- Added a typed `CSharpDbConnection.CreateCommand()` helper so concrete-provider callers get `CSharpDbCommand` directly and can use provider conveniences such as `Parameters.AddWithValue(...)` without casting.
- Added connection-level coverage around schema metadata discovery and typed command behavior.

### Pipelines Packaging and NuGet Readiness

- Added a full `CSharpDB.Pipelines` README with a simple end-to-end CSV-to-JSON example, package validation flow, execution modes, and current built-in runtime boundaries.
- Wired `CSharpDB.Pipelines` into the CI and release packaging workflows so it is packed and published alongside the rest of the NuGet package set.
- Updated package metadata to point package project links at `https://csharpdb.com/`.

### Documentation and Website Accuracy

- Refreshed package READMEs and the top-level `CSharpDB` README with fuller examples and clearer current-surface guidance.
- Expanded the engine README examples for hybrid open/save flows, collection open/create behavior, and concurrent snapshot readers.
- Added a dedicated full-text-search docs page on the static site and refreshed the website examples/API reference so the snippets match the current public APIs across collections, storage, pipelines, database modes, client SDK, and ADO.NET.
- Centralized website page metadata and shared code-block headers so the static docs stay more consistent to maintain going forward.