# Visual Query Designer — Implementation Plan

> This document is the concrete implementation plan for building the Query Designer in `CSharpDB.Admin`. It covers the exact files to create and modify, the component design, JS interop, CSS, and a verification checklist. For the broader design rationale, goals, and phasing strategy, see [README.md](./README.md).

---

## Layout

```
┌────────────────────────────────────────────────────────────────────────┐
│ [Run] [Add Table] [Clear]  ─────────────  [Save] [Load] [→ Query Tab]  │
├────────────────────────────────────────────────────────────────────────┤
│  SOURCE CANVAS                                                         │
│                                                                        │
│  ┌─────────────────┐               ┌─────────────────┐                 │
│  │  Person         │               │  Section        │                 │
│  ├─────────────────┤               ├─────────────────┤                 │
│  │ [x] PersonName  ├───────────────┤ [x] SectionTitle│                 │
│  │ [ ] PersonTypeID│               │ [x] Time        │                 │
│  │ [ ] IsActive    │               │ [ ] CourseID    │                 │
│  └─────────────────┘               └─────────────────┘                 │
│                                                                        │
├──── splitter ──────────────────────────────────────────────────────────┤
│  DESIGN GRID                                                           │
│  ┌───────────────┬───────┬─────────┬─────┬───────────┬───┬────────┐    │
│  │ Column        │ Alias │ Table   │ Out │ Sort Type │ # │ Filter │    │
│  ├───────────────┼───────┼─────────┼─────┼───────────┼───┼────────┤    │
│  │ PersonName    │       │ Person  │  x  │ Ascending │ 2 │        │    │
│  │ SectionTitle  │       │ Section │  x  │ Ascending │ 1 │        │    │
│  │ Time          │       │ Section │  x  │           │   │        │    │
│  └───────────────┴───────┴─────────┴─────┴───────────┴───┴────────┘    │
├────────────────────────────────────────────────────────────────────────┤
│  SQL PREVIEW                                                           │
│  SELECT Person.PersonName, Section.SectionTitle, Section.Time          │
│  FROM Section                                                          │
│    INNER JOIN Person ON Section.PersonID = Person.PersonID             │
│  ORDER BY Section.SectionTitle, Person.PersonName                      │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Files to Create

### `src/CSharpDB.Admin/Models/QueryDesignerModels.cs`

State models for the designer. All state is kept in plain C# classes serializable to JSON.

```csharp
public class QueryDesignerState
{
    public List<DesignerTableNode> Tables { get; set; } = [];
    public List<DesignerJoin> Joins { get; set; } = [];
    public List<DesignerGridRow> GridRows { get; set; } = [];
    public string? SavedLayoutName { get; set; }
}

public class DesignerTableNode
{
    public string TableName { get; set; } = "";
    public double X { get; set; } = 20;
    public double Y { get; set; } = 20;
    public List<DesignerColumn> Columns { get; set; } = [];
}

public class DesignerColumn
{
    public string Name { get; set; } = "";
    public string TypeLabel { get; set; } = "";   // "INT", "TEXT", "REAL", "BLOB"
    public bool IsPrimaryKey { get; set; }
    public bool IsSelected { get; set; }
}

public class DesignerJoin
{
    public string LeftTable { get; set; } = "";
    public string LeftColumn { get; set; } = "";
    public string RightTable { get; set; } = "";
    public string RightColumn { get; set; } = "";
    public DesignerJoinType JoinType { get; set; } = DesignerJoinType.Inner;
}

public enum DesignerJoinType { Inner, Left, Right, Full }

public class DesignerGridRow
{
    public string ColumnExpr { get; set; } = "";
    public string TableName { get; set; } = "";
    public string? Alias { get; set; }
    public bool Output { get; set; } = true;
    public DesignerSortDirection? SortType { get; set; }
    public int? SortOrder { get; set; }
    public string? Filter { get; set; }
}

public enum DesignerSortDirection { Ascending, Descending }
```

---

### `src/CSharpDB.Admin/Helpers/QueryDesignerSqlBuilder.cs`

Pure static SQL generation from `QueryDesignerState`. No Blazor or UI dependencies.

**Output format:**

```sql
SELECT [qualified columns with optional aliases]
FROM [first table]
    INNER JOIN [second table] ON [left.col = right.col]
    ...
WHERE [filter clauses AND-combined]
ORDER BY [columns with SortOrder set, sorted by SortOrder ascending]
```

Returns `-- Add tables to begin` when no tables are present.

---

### `src/CSharpDB.Admin/Components/Tabs/QueryDesignerTab.razor`

Main tab component. Four-panel vertical layout:

| Panel | Content |
|-------|---------|
| **Toolbar** | Run, Add Table dropdown, Clear, separator, Save Layout, Load Layout dropdown, separator, Open in Query Tab |
| **Source Canvas** | `position: relative` container with SVG overlay for join bezier lines and absolutely-positioned table node cards |
| **Design Grid** | HTML table with rows for: ColumnExpr, Alias, Table, Output checkbox, Sort Type select, Sort Order number, Filter text |
| **SQL Preview** | Read-only `<pre>` with monospace font showing the live-generated SQL |

**State:** `QueryDesignerState _state` managed in-component. Serialized to `Tab.DesignerStateJson` on every change for persistence across tab switches.

**Key interactions:**

- **Add Table** — dropdown lists all tables from `DbClient.GetTableNamesAsync()`. On select, calls `DbClient.GetTableSchemaAsync(name)`, creates a `DesignerTableNode` offset from the last node (e.g. 20 + n×220 px), adds it to state. Auto-selects all columns.
- **Column checkboxes** — toggling a column in a table node card adds/removes a `DesignerGridRow` from the grid.
- **Add Join** — toolbar button opens an inline dialog with four dropdowns (left table, left column, right table, right column) plus join type. On confirm, appends to `_state.Joins` and triggers SVG re-render.
- **Join line click** — shows an inline edit popup with join type selector and delete button.
- **Run** — calls `DbClient.ExecuteSqlAsync(generatedSql)`, displays result rows in a standard results table below the SQL preview (same styling as `QueryTab`).
- **Save Layout** — calls `DbClient.UpsertSavedQueryAsync("__designer_layout:" + name, json)`.
- **Load Layout** — dropdown filters saved queries by the `__designer_layout:` prefix, deserializes JSON back into `_state`.
- **Open in Query Tab** — calls `TabManager.OpenQueryTab(generatedSql)`.

---

## Files to Modify

### `src/CSharpDB.Admin/Models/TabDescriptor.cs`

Add `QueryDesigner` to the `TabKind` enum:

```csharp
public enum TabKind
{
    Welcome, Query, TableData, ViewData, Procedure, Storage,
    QueryDesigner   // ← add
}
```

Add a convenience property for persisting designer state JSON across tab switches:

```csharp
public string? DesignerStateJson
{
    get => State.TryGetValue("DesignerStateJson", out var v) ? v as string : null;
    set => State["DesignerStateJson"] = value;
}
```

---

### `src/CSharpDB.Admin/Services/TabManagerService.cs`

Add `OpenQueryDesignerTab()`. Uses a fixed ID (`"query-designer"`) so re-opening activates the existing tab rather than creating a duplicate:

```csharp
public TabDescriptor OpenQueryDesignerTab()
{
    var tab = new TabDescriptor("query-designer", "Query Designer",
        "bi-diagram-3", TabKind.QueryDesigner);
    OpenTab(tab);
    return _tabs.First(t => t.Id == tab.Id);
}
```

---

### `src/CSharpDB.Admin/Components/Layout/MainLayout.razor`

Add a `QueryDesigner` case in the `@switch` block:

```razor
case TabKind.QueryDesigner:
    <QueryDesignerTab @key="TabManager.ActiveTab.Id" Tab="TabManager.ActiveTab" />
    break;
```

---

### `src/CSharpDB.Admin/Components/Layout/NavMenu.razor`

Add a "Query Designer" button to the sidebar footer alongside the existing New Query, New Procedure, and Storage buttons:

```razor
<button class="sidebar-action-btn" @onclick="() => TabManager.OpenQueryDesignerTab()"
        title="Visual Query Designer">
    <i class="bi bi-diagram-3"></i> Query Designer
</button>
```

---

### `src/CSharpDB.Admin/wwwroot/js/interop.js`

Add a `designerInterop` namespace for table node drag-and-drop on the canvas:

```js
window.designerInterop = {
    // Attach drag handlers to all .designer-table-node elements inside canvasId.
    // On mouseup, calls dotNetRef.invokeMethodAsync('OnTableMoved', tableName, x, y).
    initDrag: (dotNetRef, canvasId) => { ... },

    // Remove all drag handlers attached to the canvas.
    dispose: (canvasId) => { ... }
};
```

**Drag behavior:**
- `mousedown` on `.designer-table-node-header` → capture pointer, record start offsets
- `mousemove` → update `style.left` / `style.top` on the node element directly (no Blazor round-trip during drag, for smoothness)
- `mouseup` → call `dotNetRef.invokeMethodAsync('OnTableMoved', tableName, x, y)` → Blazor updates `_state` and re-renders SVG join lines

This matches the existing `resizeInterop` pattern in the same file.

---

### `src/CSharpDB.Admin/wwwroot/css/app.css`

Add designer CSS classes at the end of the file. All values use existing CSS variables — no hardcoded colors.

| Class | Purpose |
|-------|---------|
| `.query-designer-tab` | Outer flex column filling the content area |
| `.designer-canvas` | `position: relative; overflow: auto` — the drag/SVG surface |
| `.designer-canvas-svg` | `position: absolute; inset: 0; pointer-events: none` — SVG overlay for join lines |
| `.designer-table-node` | `position: absolute` card with border, shadow, and rounded corners |
| `.designer-table-node-header` | Drag handle — colored with `--accent-blue` background tint, table icon and name |
| `.designer-table-node-col` | Each column row — checkbox, type badge, column name |
| `.designer-splitter` | 4 px horizontal resize bar between canvas and grid |
| `.designer-grid` | Design grid container with `overflow-y: auto` |
| `.designer-grid table` | Inherits `.data-grid` column styling |
| `.designer-sql-preview` | Read-only SQL area — `font-family: var(--font-mono)`, `background: var(--bg-secondary)`, `border-top: 1px solid var(--border-color)` |
| `.designer-join-popup` | Inline edit popup for a selected join line |

---

## Saved Layouts

Designer layouts are stored using the existing `UpsertSavedQueryAsync` / `GetSavedQueriesAsync` APIs.

- **Naming convention:** `__designer_layout:<user-chosen name>`
- **Stored value:** `QueryDesignerState` serialized as JSON
- **Filtering:** Load dropdown calls `GetSavedQueriesAsync()` and filters for names starting with `__designer_layout:`
- **Regular query list isolation:** `QueryTab` and `SavedQuery` dropdowns filter out entries starting with `__designer_layout:` so the two lists never mix

This approach requires zero backend changes for v1.

---

## Reusable Existing Code

| Existing item | How it is reused |
|---|---|
| `TabManagerService.OpenQueryTab(sql)` | "Open in Query Tab" button |
| `DbClient.GetTableNamesAsync()` | Populate Add Table dropdown |
| `DbClient.GetTableSchemaAsync(name)` | Load columns for a table node |
| `DbClient.UpsertSavedQueryAsync()` / `GetSavedQueriesAsync()` | Save and load designer layouts |
| `DbClient.ExecuteSqlAsync()` | Run the generated SQL |
| CSS variables + `.toolbar-btn`, `.btn` classes | Consistent styling with zero extra work |
| `ToastService` | Success/error feedback |
| `JS resizeInterop` pattern | Model for the vertical splitter drag |

---

## Verification Checklist

1. **Open designer tab** — click "Query Designer" in the sidebar footer → tab opens with empty canvas and toolbar.
2. **Add tables** — click "Add Table" → dropdown lists all tables → click a table → node appears on canvas with all columns listed and checked.
3. **Drag node** — drag a table node header → node moves smoothly → releasing fires `.NET` callback → SVG join lines update position.
4. **Create a join** — add a second table → click "Add Join" → configure left/right table+column and join type → bezier line appears between nodes.
5. **Edit/delete a join** — click a join line → edit popup appears with join type selector and delete button.
6. **Column checkboxes** — uncheck a column on the canvas → grid row disappears → SQL preview updates.
7. **Sort** — set Sort Order on a grid row → `ORDER BY` appears correctly in SQL preview.
8. **Filter** — type a filter value on a grid row → `WHERE` clause appears in SQL preview.
9. **Run** — click Run → results table renders below SQL preview.
10. **Save layout** — enter a name → Save → close tab → reopen Query Designer → Load dropdown lists the layout → loading restores the canvas, joins, and grid.
11. **Open in Query Tab** — button opens a new Query Tab pre-populated with the generated SQL.
12. **Theme** — toggle dark/light → all designer elements re-skin correctly via CSS variables.
13. **Saved query isolation** — regular Query Tab "Load saved query" dropdown does not show designer layouts.

---

## See Also

- [README.md](./README.md) — full design rationale, scope, architecture phases, and acceptance criteria
- [Roadmap](../roadmap.md) — project-wide feature roadmap
