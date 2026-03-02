# CSharpDB Roadmap

This document outlines the planned direction for CSharpDB, organized by timeframe and priority. Items are roughly ordered by expected impact within each tier.

---

## Near-Term

Focused improvements to SQL completeness and query performance.

| Feature | Description | Status |
|---------|-------------|--------|
| **`DISTINCT` keyword** | Deduplicate rows in SELECT output | Planned |
| **Composite indexes** | Multi-column indexes for covering more query patterns | Planned |
| **Index range scans** | Use indexes for `<`, `>`, `<=`, `>=`, `BETWEEN` — not just equality | Planned |
| **Prepared statement cache** | Cache parsed ASTs and query plans to avoid re-parsing identical SQL | Planned |
| **Cached max rowid** | Store in table metadata to avoid O(n) scan when generating row IDs on insert | Planned |
| **B+tree delete rebalancing** | Merge underflowed pages on delete to reclaim space | Planned |

---

## Mid-Term

SQL feature parity and ecosystem expansion.

| Feature | Description | Status |
|---------|-------------|--------|
| **Subqueries** | Scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)` | Planned |
| **`UNION` / `INTERSECT` / `EXCEPT`** | Set operations across SELECT results | Planned |
| **Window functions** | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LEAD()`, `LAG()` | Planned |
| **`DEFAULT` column values** | Allow default expressions in column definitions | Planned |
| **`CHECK` constraints** | Arbitrary expression-based constraints per column or per table | Planned |
| **Foreign key constraints** | `REFERENCES` with optional `ON DELETE CASCADE` | Planned |
| **NuGet package** | Publish `CSharpDB.Engine`, `CSharpDB.Data`, and `CSharpDB.Service` as NuGet packages | Planned |
| **Connection pooling** | Pool `CSharpDbConnection` instances to amortize open/close cost | Planned |
| **Admin dashboard improvements** | Schema editing, SQL editor with syntax highlighting, query history | In progress |
| **VS Code extension** | Schema explorer, SQL editor with IntelliSense, data browser, table designer, storage diagnostics | Planned |

---

## Long-Term

Advanced features and fundamental architecture enhancements.

| Feature | Description | Status |
|---------|-------------|--------|
| **Memory-mapped I/O (mmap)** | Zero-copy read path to eliminate per-page `byte[]` allocation and GC pressure | Planned |
| **Full-text search** | Inverted index with tokenization, stemming, and relevance ranking | Planned |
| **JSON path querying** | Query into JSON document fields in the Collection API (e.g., `$.address.city`) | Planned |
| **Collection secondary indexes** | Secondary B+tree indexes on JSON paths for the Collection API | Planned |
| **Page-level compression** | Compress cell content within pages to reduce I/O and storage | Planned |
| **Cost-based query optimizer** | Statistics-driven join ordering and index selection | Planned |
| **Async I/O batching** | Group multiple page writes into fewer system calls during batch operations | Planned |
| **Write-ahead buffering** | Buffer WAL writes before flushing to improve auto-commit throughput | Planned |
| **Multi-writer support** | Allow concurrent write transactions (conflict detection + retry) | Research |
| **Replication / change feed** | Stream committed changes for read replicas or event-driven architectures | Research |

---

## Current Limitations

These are known simplifications in the current implementation:

| Area | Limitation |
|------|-----------|
| **B+tree** | Delete does not rebalance or merge underflowed pages |
| **Query** | No subqueries, no DISTINCT, no UNION/INTERSECT/EXCEPT |
| **Query** | No window functions |
| **Schema** | No DEFAULT values, CHECK constraints, or foreign keys |
| **Indexes** | Single-column only (no composite indexes) |
| **Indexes** | Equality lookups only (no range scans via index) |
| **Concurrency** | Single writer only (no multi-writer) |
| **Storage** | No page-level compression |
| **Storage** | No mmap read path |

---

## Completed Milestones

Major features already implemented:

- Single-file database with 4 KB page-oriented storage
- B+tree-backed tables and secondary indexes
- Write-Ahead Log with crash recovery and auto-checkpoint
- Concurrent snapshot-isolated readers via WAL-based MVCC
- Full SQL pipeline: tokenizer, parser, query planner, operator tree
- JOINs (INNER, LEFT, RIGHT, CROSS), aggregates, GROUP BY, HAVING, CTEs
- Views and triggers (BEFORE/AFTER on INSERT/UPDATE/DELETE)
- ADO.NET provider (DbConnection, DbCommand, DbDataReader, DbTransaction)
- Document Collection API (NoSQL) with typed Put/Get/Delete/Scan/Find
- Interactive CLI with meta-commands and file execution
- REST API with 34 endpoints and OpenAPI/Scalar documentation
- Blazor Server admin dashboard
- Comprehensive benchmark suite (micro, macro, stress, scaling)

---

## See Also

- [Architecture Guide](architecture.md) — How the engine is structured
- [Internals & Contributing](internals.md) — How to extend the engine
- [Backup/Export/Import Plan](backup-export-import/README.md) — Planned tooling for diagnostics, backups, import/export, and reclaim
- [ETL Pipelines Plan](etl-pipelines/README.md) — SSIS-lite proposal for package-based data movement and transforms
- [VS Code Extension Plan](vscode-extension/README.md) — IDE extension for schema exploration, SQL editing, and data browsing
- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md) — Performance data informing optimization priorities
