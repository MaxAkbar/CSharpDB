# What's New

## v2.7.0

### Foreign Key Constraints v1

- Added single-column, column-level `REFERENCES` support with optional `ON DELETE CASCADE`.
- Foreign keys are enforced immediately on `INSERT`, `UPDATE`, and parent-row `DELETE` paths, with internal child-side support indexes to keep parent checks bounded.
- Added `ALTER TABLE ... DROP CONSTRAINT <name>` for foreign key removal without requiring a full child-table rebuild.
- Added a first-class foreign-key retrofit migration workflow for older databases across `ICSharpDbClient`, HTTP, gRPC, CLI, and Admin so existing tables can be validated and rewritten with persisted FK metadata.
- Added foreign-key metadata across SQL and client tooling, including `sys.foreign_keys`, richer `sys.objects`, CLI `.schema`, MCP schema inspection, and ADO.NET `GetSchema("ForeignKeys")`.

## v2.6.0

### Multilingual Collations and Ordered Text Indexes

- Expanded SQL and collection collation support with explicit `BINARY`, `NOCASE`, `NOCASE_AI`, and built-in `ICU:<locale>` options for comparisons, ordering, unique enforcement, and path indexes.
- Added ordered SQL text index options and planning support, including collation-aware metadata, guardrails, and benchmark coverage for text-heavy index shapes.
- Stabilized ordered-text overflow handling so large duplicate or prefix-heavy text indexes can spill safely instead of overloading a single index page.

### Maintenance, Diagnostics, and Admin Hardening

- Hardened reindex rebuilds, WAL open failure cleanup, and advisory statistics freshness after reopen so maintenance and recovery paths behave more predictably after index or storage edge cases.
- Improved the Admin experience for large datasets by reusing table-style pagination and filters in SQL and designer results, making welcome-page row counts on-demand, and clarifying storage-maintenance failures.
- Tightened storage diagnostics and repair coverage around corrupt indexes, live-database inspection, and maintenance workflows.

### Reusable Benchmark Data and Release Validation

- Added `tests/CSharpDB.DataGen`, a JSON-spec-driven data generator for relational, document, and time-series workloads with CSV, JSONL, and direct binary-load output modes.
- Added focused release benchmark mode plus refreshed benchmark docs and baseline tooling so release validation is easier to reproduce on a clean machine.
- Reorganized sample and tutorial project paths, refreshed NuGet search metadata, and updated docs and site content to match the current shipped surface.
