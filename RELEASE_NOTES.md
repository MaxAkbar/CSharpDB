# What's New

## New Package: CSharpDB.Storage.Diagnostics
Read-only inspection and integrity checking for CSharpDB database files:
- **DatabaseInspector** — validate headers, walk B+trees, produce page-type histograms
- **WalInspector** — frame-by-frame WAL validation with salt/checksum verification
- **IndexInspector** — verify index consistency against table data
- Integrated into the Admin UI, REST API, and CLI

## Storage Engine Improvements
- Reorganized into clean subsystem modules (BTree, Caching, Catalog, Paging, WAL, Indexing, etc.)
- Configurable checkpoint policies (frame count, time interval, WAL size, composite)
- New extensibility model with interceptors, checksum providers, and storage engine factory
- Cache-aware index store for faster index lookups

## Performance Optimizations
- **WAL batch writes** — multi-page commits in a single I/O operation
- **Fast-path PK lookups** — direct B+tree access for `SELECT ... WHERE pk = value`
- **Selective column decoding** — only deserialize columns the query actually needs
- **Projection pushdown** — table scan skips unused columns
- **Row buffer reuse** — reduced allocations across join, sort, and scan operators
- **B+tree stack allocation** — small cells (<=256 bytes) avoid heap allocation
- **Statement caching** — LRU cache for parsed and planned queries

## Per-Package NuGet READMEs
Each library package now ships with its own README on NuGet:
CSharpDB.Core, CSharpDB.Sql, CSharpDB.Storage, CSharpDB.Execution, CSharpDB.Engine, CSharpDB.Data, CSharpDB.Storage.Diagnostics, CSharpDB.Service

## CI/CD
- GitHub Actions CI workflow
- Automated release workflow with NuGet publishing
- Performance guardrail workflow with threshold tracking

## Documentation
- Storage inspector guide
- Architecture enforcement plan
- Collection optimization plan
- Cross-platform deployment guide
- VS Code extension documentation
- Updated benchmark results
