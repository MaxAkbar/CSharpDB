# What's New

## v2.0.1 (Unreleased)

### NuGet README Link Rewriting

- Added `scripts/fix-nuget-readme-links.sh` — a pre-pack step that rewrites relative markdown links in project README files to absolute GitHub blob URLs so they resolve correctly when displayed on NuGet.org.
- The release workflow passes the git tag (e.g. `v2.0.1`) as the blob ref, so NuGet README links point to the exact tagged source.
- The CI workflow uses `main` as the ref for artifact-only packs.
- Links to docs, sibling projects, and local source files are all rewritten; absolute URLs, anchors, and badge images are left unchanged.

### Visual Query Designer

- Added a **Designer mode** toggle to the existing query tab, alongside the existing SQL mode. Switching to Designer opens the visual query builder without leaving the query workspace.
- Added a source canvas with draggable table node cards. Each card lists the table's columns with type labels and primary-key indicators. Nodes can be repositioned by dragging the header.
- Added SVG bezier join lines rendered over the canvas. Lines are clickable to open an inline edit popup for changing join type (`INNER`, `LEFT`, `RIGHT`, `FULL OUTER`) or removing the join.
- Added an **Add Join** dialog in the designer toolbar that lets users pick left table/column, right table/column, and join type without drawing lines by hand.
- Added a design grid with per-column rows covering: `Column`, `Alias`, `Table`, output toggle (`Out`), sort direction, sort order number, and filter expression. Checking a column on the canvas automatically adds it to the grid; unchecking removes it.
- Added a live SQL preview panel below the design grid that updates on every state change and displays the `SELECT … FROM … JOIN … WHERE … ORDER BY` statement the designer would produce.
- Added collapsible section headers for the Canvas, Design Grid, and Results sections so users can focus on the area they need.
- Added save and load for designer layouts stored as named entries in the existing saved-query store using the `__designer_layout:` prefix. Saved layouts are filtered out of the regular saved-query dropdown in SQL mode.
- Added **Copy SQL to Editor** button that sends the generated SQL to the SQL editor and switches the tab back to SQL mode.
- Added `QueryDesignerState`, `DesignerTableNode`, `DesignerColumn`, `DesignerJoin`, and `DesignerGridRow` model classes in `CSharpDB.Admin`.
- Added `QueryDesignerSqlBuilder` — a pure static SQL generator that converts the designer state into a valid `SELECT` statement.
- Designer state is persisted to `TabDescriptor.State` so it survives tab switching within the same session.
- JS drag is handled by a single set of document-level `mousedown` / `mousemove` / `mouseup` listeners initialized once on first render, avoiding re-render interference between Blazor's SignalR cycle and active drag operations.

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

