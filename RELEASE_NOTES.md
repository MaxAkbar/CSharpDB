# What's New

## v3.0.0

### Generated Collections and Trim-Safe Models

- Added source-generated collection model support through the new `CSharpDB.Generators` package so typed collections can use generated metadata instead of reflection.
- Added `GetGeneratedCollectionAsync<T>()` and `GeneratedCollection<T>` for trim-safe and NativeAOT-friendly collection access when a generated or manually registered collection model is available.
- Added collection model attributes, runtime model registration, generated field metadata, and expanded generated collection coverage across the engine and tests.
- Added a generated collections sample plus a trim-smoke validation project to prove the end-to-end generated-model path.

### Collection Runtime and Performance

- Recovered collection write-path performance after merging `main` by separating collection write probes from the read-side B-tree routing-cache path and reusing traversal scratch during insert and replace operations.
- Buffered collection row-count and catalog mutation bookkeeping inside explicit transactions so collection counts remain correct without paying row-by-row catalog churn during transactional write batches.
- Added regression coverage for generated collection behavior, collection model generation, and explicit-transaction collection count visibility.

### Mainline Engine and CLI Updates

- Merged the latest `main` branch engine work into `version3.0.0`, including planner-statistics improvements, durable-write batching infrastructure, covered composite-index fast-path recovery, and related stabilization work.
- Pulled in the current CLI improvements, including the refactored help and info command organization and the newer `Spectre.Console`-based shell presentation.

### Documentation and Samples

- Added the generated collections sample and supporting generator documentation.
- Added the collection-versus-SQL boundary note to capture the recommended future refactor direction without splitting the engine core.
- Updated the roadmap to reflect generated collections work.

### Validation

- Validated with `dotnet build CSharpDB.slnx`.
- Validated with `dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --no-build` (`35` passed).
- Validated with `dotnet test tests/CSharpDB.Tests/CSharpDB.Tests.csproj --no-build --filter "(FullyQualifiedName~GeneratedCollectionModelTests|FullyQualifiedName~CollectionModelGeneratorTests|FullyQualifiedName~CollectionTests|FullyQualifiedName~CollectionIndexTests)"` (`95` passed).
- Validated with `dotnet build tests/CSharpDB.GeneratedCollections.TrimSmoke/CSharpDB.GeneratedCollections.TrimSmoke.csproj --no-restore -m:1 -p:UseSharedCompilation=false`.
