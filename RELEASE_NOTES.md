# What's New

## v2.0.0 (Unreleased)

### SQL Query Expansion
- Added `UNION`, `INTERSECT`, and `EXCEPT` for combining `SELECT` results, including use inside top-level queries, views, and CTE query bodies.
- Added scalar subqueries, `IN (SELECT ...)`, and `EXISTS (SELECT ...)`.
- Added correlated subquery evaluation for `WHERE`, non-aggregate projection expressions, and `UPDATE`/`DELETE` expressions.
- Correlated subqueries are still rejected in `JOIN ON`, `GROUP BY`, `HAVING`, `ORDER BY`, and aggregate projections, and `UNION ALL` is not implemented yet.

### Statistics And Planning
- Added `sys.column_stats` alongside `sys.table_stats`.
- Added exact `distinct_count`, `non_null_count`, `min_value`, and `max_value` refresh during `ANALYZE`.
- Added stale tracking for persisted column stats after writes, with rollback/reopen and VACUUM copy preserving catalog correctness.
- Added initial planner use of fresh column stats to avoid obviously low-selectivity non-unique equality lookups and prefer more selective lookup terms.

### Client Transport Completion
- Added the REST-backed `Http` transport implementation for `CSharpDB.Client`, so the unified client now has working Direct, HTTP, and gRPC paths.
- Completed the API coverage needed by the HTTP client for collections, saved queries, procedures, transaction sessions, checkpointing, maintenance, and storage inspection flows.
- Updated client/CLI endpoint resolution so `http://` and `https://` endpoints map cleanly to the dedicated `CSharpDB.Api` host, while `CSharpDB.Daemon` remains the gRPC host.

### Breaking Transport Cleanup
- Removed the unsupported `Tcp` transport placeholder from the public client transport model, CLI parsing, and related docs so the contract matches the transports that actually exist.
- Left `NamedPipes` as the only planned additional client transport for future same-machine daemon scenarios.

### Compatibility Surface Cleanup
- Removed the legacy `CSharpDB.Service` and `CSharpDB.Core` compatibility projects from the `v2.0` repo and solution.
- Removed `CSharpDB.Service` from the all-in-one `CSharpDB` package so the `v2.0` entry package only pulls in the primary client, engine, ADO.NET, and diagnostics surface.
- Updated the active docs to point legacy consumers directly to `CSharpDB.Client` and `CSharpDB.Primitives`.

