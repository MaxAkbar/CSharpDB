# What's New

## v2.3.0

### WAL Durability and Commit Throughput

- Added configurable WAL durability modes so file-backed storage can run with explicit `Durable` or `Buffered` commit behavior.
- Added grouped WAL commit coordination for durable writes to reduce commit contention under concurrent write load.
- Moved pager commit waiting out from under the writer lock to improve write-path scheduling.
- Routed WAL flushing through explicit flush policies so durability behavior stays clear at the storage-engine boundary.
- Added durable-vs-buffered benchmark coverage and refreshed v2.3 comparison results.

### Pipelines and Collection Reliability

- Improved pipeline editor resilience so visual mode preserves valid package state and no longer resets user work when package JSON is invalid or empty.
- Added CSV source column previews, schema-aware destination coercion, resolved file-path handling, and richer transform/destination failure diagnostics.
- Preserved explicit `null` values during pipeline table writes and extended pipeline resume coverage around checkpoint rewinds.
- Hardened collection storage by falling back to JSON when binary collection storage does not support a value type.
- Fixed collection codec regressions so unsupported typed document shapes still round-trip through JSON fallback and `UInt64` values preserve binary round-tripping.

### Documentation, Site, and Admin Publishing

- Added a static documentation site under `www` with landing pages, architecture docs, benchmarks, getting-started guidance, storage/SQL/pipeline/collection docs, and sample content.
- Added a publish helper script for `CSharpDB.Admin` and `CSharpDB.Daemon` to simplify deployment artifact generation.
- Updated the release workflow to publish the docs site to GitHub Pages and added the supporting Pages workflow.
- Updated the admin UI pipeline designer with a New Pipeline action and enabled static asset mapping so site assets are served correctly.
- Added a storage internals walkthrough script in `docs` for release and educational content.
