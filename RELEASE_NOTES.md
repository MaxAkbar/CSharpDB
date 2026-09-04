# What's New

## CSharpDB 4.6.3

CSharpDB 4.6.3 turns the Admin Data Model workspace into a relationship-aware
schema modeler, makes internal storage easier to inspect, and fixes diagram
persistence and shared table-count/cache issues. These notes cover changes
since the published v4.6.2 release only.

### Relationship-Aware Data Modeler

- Start an unsaved global workspace with an empty canvas and searchable Sources.
  Add one table, add its immediate relationships, or explicitly Load All without
  moving existing cards.
- Arrange parent/child relationships deterministically, with cycle handling,
  group-aware Auto Arrange, and compact Keys First, All, or Collapsed cards.
  Manual positions survive metadata refresh and diagram reopening.
- Read one crow's-foot connector per foreign-key constraint, including ordered
  composite-column mappings and cardinality derived from complete keys and
  nullability. Inspect declared type facets, defaults, collation, identity,
  rowversion, keys, checks, and user-managed versus supporting indexes.
- Pan, zoom, Fit Model, locate tables, and focus direct relationships. Multi-table
  selection, keyboard movement, background/Escape deselection, and session-local
  undo/redo support everyday diagram work.
- Organize selected tables into named, colored groups. Move a group as one unit
  and retain its internal arrangement during Auto Arrange; membership changes
  are explicit and never change database schema.
- Edit connector bends and move whole orthogonal segments without adding an
  unwanted split. Saved segment guides follow endpoint-row movement; internal
  routes move with their group, while crossing routes retain their guides.
  Routing and relationship labels avoid table cards and protected group titles.
- Create separate named diagrams, Save As a copy, switch between saved diagrams,
  and confirm Delete Diagram by name. Save status and failed-save handling
  protect the active canvas when switching. Deleting a diagram does not silently
  recreate its saved record.

### Inspect, Review, and Apply Supported Schema Changes

- Stage supported table and column operations, type facets, literal defaults,
  collation, scalar/composite primary and unique keys, checks, indexes, and
  scalar/composite foreign keys with supported referential actions.
- Review exact SQL, affected objects, warnings, destructive operations, and known
  dependencies before applying. Unsupported engine combinations are blocked
  rather than implemented through implicit table rebuilding.
- Reject stale schema plans and apply the reviewed batch, including native
  foreign-key DDL, in one route-local transaction when the client supports it.
  Failed batches retain pending edits; a later diagram-save failure is reported
  separately from successful schema application.
- Explicitly check existing data for relevant nulls, duplicate candidate keys,
  and orphaned relationships, with cancellation and skipped-check reporting.
  Opening a diagram does not launch expensive data scans.
- Export the visible model as SVG or PNG and export reviewed pending SQL without
  executing it. Open related table data, Query Designer, Data Hygiene, System
  Catalog, and Compare/Deploy workflows from the inspector.

### Internal Storage and System Catalog Visibility

- Centralize internal-table classification and virtual-catalog descriptions in
  shared registries used by Admin and the relevant client/schema services.
- Expose supported virtual catalogs in System Catalog and add a read-only
  Internal Storage view, backed by `sys.internal_tables`, showing physical
  backing tables, ownership, and logical replacements such as
  `__data_model_diagrams -> sys.diagrams`.
- Keep internal objects out of normal source pickers and schema comparisons
  according to the shared visibility policy, without treating every
  underscore-prefixed user table as product-owned storage.

### Persistence and Storage Fixes

- Fix negative row counts during diagram deletion and other table mutations:
  recounts no longer apply an already-completed mutation twice, and stale shared
  statistics do not overwrite a corrected committed count.
- Invalidate shared B+tree read-routing caches when the pager changes, including
  leaf redistribution that leaves the tree root unchanged.
- Preserve diagram membership, groups, connector routes, pending schema intent,
  and viewport through saving, refresh, compatible metadata changes, and load.

### Documentation and Release Maintenance

- Add a dedicated offline Data Modeler guide and an illustrated website tutorial
  with a runnable sample schema.
- Refresh the public changelog and downloads page with verified release history,
  platform-specific Admin/server/CLI assets, package coverage, prerequisites,
  checksum guidance, and source-only Node client instructions.
- Add offline documentation guardrails and optional read-only GitHub/NuGet
  reconciliation for published release and download metadata.
- Advance package and migration-tool defaults to 4.6.3. Add the matching migration
  capability catalog while retaining immutable catalogs for prior targets.

### Compatibility and Upgrade Notes

- Diagram JSON is version 5. Versions 1–4 migrate on load without intentionally
  discarding layout or pending intent. The diagram storage table schema and
  database/route-local ownership are unchanged. Back up saved diagrams before
  upgrading; older Admin versions may not preserve newer diagram metadata.
- Removing cards, clearing the canvas, ungrouping tables, and deleting saved
  diagrams do not drop database tables. Schema edits still require separate
  review and application. External tables and archive relationships remain
  read-only in the modeler.
- This release does not introduce a new database file format or SQL dialect.
  Existing migration capability catalogs remain available for replaying older
  plans. The Admin plugin architecture document is a proposal, not a shipped
  plugin system.
