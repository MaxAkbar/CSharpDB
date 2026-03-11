# CSharpDB Query Designer - Roadmap & Design

> **Status (March 11, 2026):** Planned. This document captures the recommended v1 direction for a classic visual query designer in `CSharpDB.Admin`. The current Admin UI supports text SQL, saved queries, schema browsing, data editing, procedures, and storage inspection, but it does not yet provide a drag-and-drop query builder.

CSharpDB already has enough SQL and schema infrastructure to support a useful designer: the Admin UI can enumerate tables, views, indexes, and schemas; the SQL layer can parse `SELECT` statements with joins, filters, grouping, ordering, and limits; and the query tab already has execution, result rendering, and saved-query flows.

What is missing is a visual authoring surface that helps users build multi-table `SELECT` queries without hand-writing joins and filter clauses.

---

## Problem

The current query workflow is efficient for users who are comfortable writing SQL, but it has three gaps:

- discovering join paths requires manual schema inspection
- composing multi-table queries is slower than it needs to be for exploratory work
- saved queries preserve SQL text only, so there is no recoverable visual layout or relationship context

This is most visible in Admin, where the rest of the product already supports interactive browsing and editing. Query authoring is still text-first.

---

## Current-State Review

The current repository gives the query designer a solid base, but also defines its constraints.

### Existing strengths

- `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor` already provides a query workspace with run, format, saved-query load/save, and results rendering.
- `src/CSharpDB.Admin/Components/Layout/NavMenu.razor` already loads tables, views, indexes, triggers, and procedures from `ICSharpDbClient`.
- `src/CSharpDB.Client/ICSharpDbClient.cs` already exposes table names, table schemas, views, indexes, triggers, and SQL execution.
- `src/CSharpDB.Sql/Ast.cs` and `src/CSharpDB.Sql/Parser.cs` already model and parse `SELECT`, joins, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, and `OFFSET`.
- `src/CSharpDB.Execution/QueryPlanner.cs` already contains a private `SelectStatement` serializer used for view storage.

### Current constraints

- There is no foreign-key implementation or relationship catalog today, so the designer cannot rely on authoritative join metadata.
- `SavedQueryDefinition` stores SQL text and timestamps only; it has no layout or designer metadata.
- `QueryTab.razor` currently mixes execution logic, saved-query logic, and rendering in one component, which makes a large visual feature harder to add cleanly.
- There is no Admin UI component test project or browser automation harness in `tests/` today, so the feature needs its own test foundation.

### Design implication

This should be treated as a query-workspace feature, not as a one-off canvas bolted onto the existing editor.

---

## Goal

Add a classic relational query designer to the Admin UI with the following behavior:

1. users can add tables and views to a canvas
2. users can create or accept join links between sources
3. users can choose output columns, aliases, sort order, and filter criteria from a design grid
4. the designer continuously generates valid CSharpDB SQL
5. users can switch between visual mode, SQL mode, and split mode without losing work
6. supported SQL can be imported back into the designer
7. designer-backed queries can be saved and reopened with layout intact

The target experience is the classic "canvas + design grid + SQL preview" workflow shown in traditional desktop query designers, adapted to the current tabbed Admin shell.

---

## Recommended Product Shape

The recommended product shape is a unified query workspace inside the existing query tab, not a completely separate tool window.

### Why this is the right shape

- execution, elapsed-time reporting, result grids, and saved queries already live in the query tab
- users need fast switching between visual editing and raw SQL
- a separate tab kind would duplicate too much workspace logic
- `TabDescriptor.State` already supports storing extra tab-local state

### Recommended modes

- `SQL` - current text editor first
- `Designer` - visual canvas first
- `Split` - visual designer and SQL preview side by side or stacked

The default should remain `SQL` for normal query tabs. "New Designer" should open a query tab directly in `Designer` mode.

---

## V1 Scope

### In scope

- single `SELECT` statement design surface
- table and view sources
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `CROSS JOIN`
- source aliases
- column projection with output toggle
- column aliasing
- per-column sort direction and sort ordinal
- per-column filter operator and value
- simple `AND` / grouped `OR` filter rows in the design grid
- `DISTINCT`
- `LIMIT` and `OFFSET`
- live SQL preview
- query execution using the existing query result panel
- save/load of designer-backed queries
- import of supported single-`SELECT` SQL into the designer

### Explicitly out of scope for v1

- `INSERT`, `UPDATE`, `DELETE`, DDL, or procedure design
- `WITH` / CTE visual design
- subqueries
- `UNION`, `INTERSECT`, or `EXCEPT`
- window functions
- aggregate and `GROUP BY` design surface
- view/procedure authoring from inside the designer
- globally persisted database relationship definitions
- pixel-perfect parity with old desktop designers

Unsupported SQL should still run in `SQL` mode. The designer should only claim round-trip support for the subset above.

---

## UX Blueprint

### Workspace layout

Recommended layout for desktop:

1. top toolbar
2. source canvas
3. design grid
4. SQL preview / results area

Recommended toolbar actions:

- `Run`
- `Add Table`
- `Add View`
- `Auto Layout`
- `Clear`
- `Format SQL`
- mode toggle: `SQL | Designer | Split`
- `Save`
- `Load`

### Source canvas

Each source card should show:

- source name
- alias
- selectable columns
- checkbox or toggle for projection
- key/index badges where known

Join lines should be rendered over an SVG layer so the cards remain normal HTML/Blazor content.

### Design grid

The lower design grid should map closely to generated SQL:

| Grid Field | Meaning |
|------------|---------|
| `Column` | Source column or expression target |
| `Alias` | `AS alias` |
| `Table` | Source alias or source name |
| `Output` | Include in `SELECT` list |
| `Sort Type` | `ASC` / `DESC` |
| `Sort Order` | Multi-column ordering |
| `Filter` | Predicate value |
| `Operator` | `=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `IS NULL`, `IS NOT NULL` |
| `Group` | Filter group index; rows in the same group `AND`, groups `OR` |

This preserves the classic designer mental model without forcing users into a completely new abstraction.

### Responsive behavior

This should be desktop-first. On narrow screens:

- the canvas and grid should stack vertically
- SQL preview should collapse behind a tab
- drag operations may be disabled in favor of list selection if necessary

The feature should remain usable on smaller screens, but it does not need to be mobile-optimized in v1.

---

## Recommended Architecture

### 1. Refactor query workspace state first

Before adding the designer UI, split the current `QueryTab.razor` responsibilities into reusable state and subcomponents.

Recommended new pieces:

- `QueryWorkspaceState` - current SQL text, active mode, results, elapsed time, errors, dirty flags
- `QueryExecutionController` - execution, non-query messaging, result binding, change notifications
- `SavedQueryController` - load/save/delete behavior
- `SqlEditorPanel` - current text editor surface
- `QueryResultsPanel` - current results / message surface

This keeps the designer from becoming a second copy of the query tab.

### 2. Introduce a designer document model

The designer needs a structured, UI-independent model. Recommended shape:

```csharp
public sealed class QueryDesignerDocument
{
    public bool IsDistinct { get; init; }
    public int? Limit { get; init; }
    public int? Offset { get; init; }
    public List<QuerySourceNode> Sources { get; init; } = [];
    public List<QueryJoinLink> Joins { get; init; } = [];
    public List<QueryProjectedColumn> Columns { get; init; } = [];
}

public sealed class QuerySourceNode
{
    public required string Id { get; init; }
    public required string SourceName { get; init; }
    public required QuerySourceKind Kind { get; init; }
    public string? Alias { get; init; }
    public double X { get; init; }
    public double Y { get; init; }
    public bool Expanded { get; init; } = true;
}

public sealed class QueryJoinLink
{
    public required string LeftSourceId { get; init; }
    public required string LeftColumnName { get; init; }
    public required string RightSourceId { get; init; }
    public required string RightColumnName { get; init; }
    public required QueryJoinType JoinType { get; init; }
}

public sealed class QueryProjectedColumn
{
    public required string SourceId { get; init; }
    public required string ColumnName { get; init; }
    public string? Alias { get; init; }
    public bool Output { get; init; } = true;
    public QuerySortDirection? SortDirection { get; init; }
    public int? SortOrdinal { get; init; }
    public QueryFilterOperator? FilterOperator { get; init; }
    public string? FilterValueText { get; init; }
    public int FilterGroup { get; init; }
}
```

This model should live outside the Razor component so it can be unit tested directly.

### 3. Use the SQL AST as the translation boundary

Do not build SQL by concatenating strings in the component tree.

Recommended translation pipeline:

`Designer document -> SelectStatement AST -> shared SQL serializer -> SQL text`

And for import:

`SQL text -> parser -> SelectStatement AST -> designer document`

This requires extracting the private `SelectToSql` logic from `QueryPlanner.cs` into a reusable serializer in `CSharpDB.Sql` or another shared project.

### 4. Keep visual layout and query semantics separate

Layout data such as node coordinates and expansion state must not be mixed into SQL semantics. Semantics belong in the designer document. Layout belongs in a sidecar object.

Recommended layout model:

```csharp
public sealed class QueryDesignerLayout
{
    public List<QuerySourceLayout> Sources { get; init; } = [];
    public double Zoom { get; init; } = 1.0;
    public double ScrollX { get; init; }
    public double ScrollY { get; init; }
}
```

This matters for save/load and future layout-version upgrades.

### 5. Prefer Blazor-first rendering with narrow JS interop

Use Blazor for:

- card rendering
- grid editing
- mode switching
- query state
- save/load flows

Use JS interop only where Blazor is awkward:

- pointer capture for drag operations
- measuring DOM anchor points for SVG connectors
- optional auto-pan while dragging

This matches the existing Admin architecture, which already uses light JS interop for keyboard shortcuts and resizing.

---

## Metadata and Relationship Strategy

### Metadata source

The designer can bootstrap from existing client APIs:

- `GetTableNamesAsync`
- `GetViewNamesAsync`
- `GetTableSchemaAsync`
- `GetViewsAsync`
- `GetIndexesAsync`

No backend change is required for the first metadata pass.

### Relationship inference

Because CSharpDB does not yet implement foreign keys, join suggestions must be heuristic and always user-correctable.

Recommended confidence rules:

1. exact match to target PK name and type
2. exact `<TargetName>Id` match to target PK or unique index
3. exact same-name column match where one side is PK/unique
4. indexed same-type name match with lower confidence

Rules 1 and 2 may be auto-suggested. Rules 3 and 4 should be shown as suggestions but never silently applied.

### Manual joins stay authoritative

Users must always be able to:

- draw a join manually
- change join type
- remove an inferred join
- join on non-key columns

The designer is an aid, not a schema authority.

### Future relationship catalog

If v1 adoption is strong, a follow-up feature can add a persistent relationship-hint catalog such as `__designer_relationships` and expose it via `sys.relationships`. That should be deferred until the non-persistent heuristics prove useful.

---

## SQL Round-Trip Rules

### Supported import subset

Import into the visual designer should work for queries that are:

- a single `SELECT`
- table/view sources only
- joins expressible as pairwise links
- projection columns that map to source columns
- filter predicates representable by the grid model
- sort clauses on supported expressions
- optional `DISTINCT`, `LIMIT`, and `OFFSET`

### Fallback behavior

If a query contains unsupported constructs, the tab should:

- stay in `SQL` mode
- show a banner explaining why visual import is unavailable
- preserve execution and save/load behavior

Examples of unsupported import cases:

- aggregates
- `GROUP BY` / `HAVING`
- functions in projection or filter expressions that the grid cannot represent cleanly
- nested expressions that cannot be edited visually without losing fidelity

### Losslessness requirement

The designer must never silently rewrite a query into a different meaning. If round-trip fidelity cannot be guaranteed, the feature should refuse import and remain in SQL mode.

---

## Persistence Strategy

### Tab-local state

The first increment should store the current designer document and layout in `TabDescriptor.State`. That gives immediate usability without a database migration.

Recommended keys:

- `QueryMode`
- `DesignerDocument`
- `DesignerLayout`
- `DesignerDirty`
- `LastGeneratedSqlHash`

### Saved-query persistence

Designer-backed saved queries need durable layout metadata. Recommended approach:

- keep `__saved_queries` as the SQL-first catalog
- add a sidecar table such as `__saved_query_layouts`
- key the layout row by saved-query id
- store JSON payload plus layout version and timestamps

Suggested columns:

| Column | Meaning |
|--------|---------|
| `saved_query_id` | FK-like reference to `__saved_queries.id` |
| `layout_kind` | `query_designer_v1` |
| `document_json` | semantic designer document |
| `layout_json` | visual positions, zoom, scroll |
| `generated_sql_hash` | drift detection |
| `created_utc` | created timestamp |
| `updated_utc` | updated timestamp |

This keeps existing saved-query consumers compatible while letting Admin load richer state when present.

### Drift handling

If a user edits SQL directly after loading a designer-backed query:

- if the SQL still imports cleanly, regenerate the designer document
- if it does not, mark the designer as out of sync and keep SQL mode authoritative

Never overwrite raw SQL with designer output unless the designer is the active editing source.

---

## Implementation Phases

### Phase 1: Query workspace refactor

**Goal:** Make the existing query tab ready for multi-mode editing.

Deliverables:

- extract workspace state and results rendering out of `QueryTab.razor`
- add mode toggle state to query tabs
- keep current SQL editing behavior unchanged
- add unit tests for workspace state transitions

Primary files:

- `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor`
- `src/CSharpDB.Admin/Models/TabDescriptor.cs`
- new query workspace models/services under `src/CSharpDB.Admin`

### Phase 2: Shared SQL translation layer

**Goal:** Create a canonical translation path between designer model and SQL.

Deliverables:

- extract reusable `SELECT` serializer from `QueryPlanner.cs`
- add AST-to-designer and designer-to-AST mappers
- add unit tests for supported round-trips
- define unsupported-import detection rules

Primary files:

- `src/CSharpDB.Execution/QueryPlanner.cs`
- `src/CSharpDB.Sql/Ast.cs`
- `src/CSharpDB.Sql/Parser.cs`
- new serializer / mapper files

### Phase 3: Metadata provider and relationship suggestions

**Goal:** Feed the designer canvas with tables, views, columns, and suggested join candidates.

Deliverables:

- cached metadata service using `ICSharpDbClient`
- relationship inference rules
- refresh on `DatabaseChangeService` notifications
- source-picker UI

Primary files:

- `src/CSharpDB.Client/ICSharpDbClient.cs`
- `src/CSharpDB.Admin/Services/DatabaseChangeService.cs`
- `src/CSharpDB.Admin/Components/Layout/NavMenu.razor`
- new metadata service classes

### Phase 4: Designer canvas MVP

**Goal:** Deliver the first usable visual query builder.

Deliverables:

- source cards with draggable placement
- selectable columns
- manual join creation and deletion
- SVG join connectors
- live SQL generation
- run query from designer mode

Primary files:

- new `QueryDesignerCanvas.razor`
- new `QueryDesignerSourceCard.razor`
- `src/CSharpDB.Admin/wwwroot/js/interop.js`
- `src/CSharpDB.Admin/wwwroot/css/app.css`

### Phase 5: Design grid and query controls

**Goal:** Match the classic lower-grid workflow.

Deliverables:

- projection/output toggles
- alias editing
- sort direction and order
- filter operator and value editing
- grouped `OR` support
- `DISTINCT`, `LIMIT`, `OFFSET`

This is the phase where the designer becomes a serious replacement for handwritten exploratory joins.

### Phase 6: Saved layouts and SQL import

**Goal:** Make the designer durable and reopenable.

Deliverables:

- sidecar saved-query layout storage
- load/save/update flows for designer-backed queries
- SQL-to-designer import for supported queries
- drift/out-of-sync detection banners

This phase requires client, API, and gRPC surface updates if the persistence is exposed outside direct engine access.

### Phase 7: Polish, testing, and documentation

**Goal:** Make the feature maintainable and shippable.

Deliverables:

- keyboard accessibility pass
- auto-layout button
- empty-state polish
- desktop and narrow-screen layout verification
- README screenshots and docs refresh
- release notes and roadmap updates

---

## Testing Plan

### Unit tests

Add focused tests for:

- relationship inference scoring
- designer document validation
- designer-to-AST mapping
- AST-to-designer import
- supported SQL round-trip cases
- unsupported SQL rejection cases
- saved-layout serialization and drift detection

### Admin component tests

The repo currently has no Admin UI test project. Add one.

Recommended new project:

- `tests/CSharpDB.Admin.Tests`

Recommended coverage:

- mode switching between SQL / Designer / Split
- add/remove source cards
- join creation behavior
- design grid editing
- SQL preview updates
- save/load state restoration

`bUnit` is the pragmatic choice for Razor component coverage here.

### Integration tests

Add client/API/daemon tests for any saved-layout persistence surface so direct, HTTP, and gRPC behavior stay aligned.

### Manual verification checklist

1. Open `New Designer`
2. Add two related tables
3. Accept or create a join
4. Select output columns
5. Add sort and filter rows
6. Verify generated SQL
7. Run query and inspect results
8. Save query, close tab, reopen query
9. Edit SQL directly, verify sync or explicit fallback behavior
10. Change schema, reopen designer, verify missing-column handling is graceful

---

## Risks

### 1. Relationship inference can be wrong

Without foreign keys, some join suggestions will be guesses. The UI must make them editable and must not hide that uncertainty.

### 2. Round-trip fidelity is easy to over-promise

The parser can represent more than the first designer UI should edit. The product must clearly distinguish "supported visual subset" from "general SQL."

### 3. QueryTab refactor is a prerequisite

Trying to add the designer directly into the current monolithic `QueryTab.razor` will create an unmaintainable component quickly.

### 4. Persistence is cross-surface work

If designer layouts become part of saved-query APIs, the change touches direct client, REST API, gRPC, DTOs, and tests.

### 5. UI drag behavior may need JS help

Pure Blazor pointer handling may be good enough, but connector anchoring and drag smoothness may still require targeted JS interop.

---

## Recommended Roadmap Status

This should appear in the main roadmap as a **Mid-Term / Planned** item, separate from the broader "Admin dashboard improvements" workstream.

Reasoning:

- it is large enough to justify its own design and acceptance criteria
- it depends on some workspace refactoring before user-visible delivery
- it does not block core engine progress
- it meaningfully improves Admin usability once the current SQL-first workflows are stable

---

## Acceptance Criteria

The feature should be considered shipped only when all of the following are true:

- a user can build a multi-table `SELECT` visually without writing SQL
- generated SQL executes through the existing query pipeline
- supported SQL can be re-imported into the designer
- saved designer queries reopen with layout and semantics preserved
- unsupported SQL clearly falls back to text mode without semantic loss
- the feature has dedicated component and translation tests

---

## Key Reference Files

These existing files should guide implementation:

| File | Why it matters |
|------|----------------|
| `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor` | current query workspace behavior |
| `src/CSharpDB.Admin/Components/Layout/NavMenu.razor` | existing schema explorer and metadata-loading patterns |
| `src/CSharpDB.Admin/Models/TabDescriptor.cs` | tab-local state storage |
| `src/CSharpDB.Client/ICSharpDbClient.cs` | schema and SQL execution surface |
| `src/CSharpDB.Client/Models/SchemaModels.cs` | source metadata shapes |
| `src/CSharpDB.Client/Models/ProcedureModels.cs` | current saved-query model |
| `src/CSharpDB.Sql/Ast.cs` | `SELECT` AST model |
| `src/CSharpDB.Sql/Parser.cs` | SQL import path |
| `src/CSharpDB.Execution/QueryPlanner.cs` | existing private `SELECT` serializer to extract |
| `src/CSharpDB.Admin/wwwroot/js/interop.js` | current JS interop pattern for drag/keyboard support |

---

## See Also

- [Roadmap](../roadmap.md) - Project-wide feature roadmap
- [VS Code Extension Plan](../vscode-extension/README.md) - Parallel future client surface for query authoring
- [Architecture Guide](../architecture.md) - Current engine and Admin layering
