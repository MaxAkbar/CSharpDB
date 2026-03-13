# CSharpDB Visual Query Designer

> **Status (v2.0.1):** Shipped. The Visual Query Designer is available as a mode toggle inside every query tab in `CSharpDB.Admin`.

The Visual Query Designer is a classic relational query builder built into the Admin UI. It provides a drag-and-drop source canvas, SVG join lines, a design grid, and a live SQL preview — all within the existing query tab, so execution, saved queries, and results remain immediately accessible.

---

## Opening the Designer

Every query tab has a **SQL | Designer** toggle in its toolbar. Click **Designer** to switch to the visual builder. The SQL editor and results are preserved; switching back to **SQL** restores the text editor with any SQL that was already present.

---

## Building a Query

### 1. Add tables to the canvas

Click **Add Table** in the toolbar. A dropdown lists all tables in the connected database. Click a table name to add it as a node on the canvas. Each node shows the table's columns with type labels (`INT`, `TEXT`, `REAL`, `BLOB`) and a key icon on primary-key columns.

Nodes can be dragged by their header to rearrange the canvas layout.

### 2. Select output columns

Each column row in a table node has a checkbox. Checking a column adds it to the design grid below. Unchecking removes it. All columns are checked by default when a table is added.

### 3. Add joins

Click **Add Join** in the toolbar to open the join dialog. Select the left table and column, the right table and column, and the join type (`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`), then click **Add**. A bezier join line is drawn between the two table nodes on the canvas.

Click an existing join line to open an inline edit popup where the join type can be changed or the join can be removed.

### 4. Configure the design grid

The design grid shows one row per selected column. Each row exposes:

| Field | Purpose |
|-------|---------|
| `Column` | Column name (read-only, reflects canvas selection) |
| `Alias` | Optional `AS alias` added to the `SELECT` list |
| `Table` | Source table name (read-only) |
| `Out` | Include the column in the `SELECT` clause |
| `Sort` | Sort direction — `Ascending` or `Descending` |
| `#` | Sort ordinal — used to order multiple `ORDER BY` columns |
| `Filter` | Filter expression appended as `column filter` in `WHERE` (e.g. `> 5`, `LIKE '%abc%'`) |

### 5. Run the query

Click **Run** in the toolbar. The generated SQL is executed against the connected database and the results appear in the **Results** section below the SQL preview. The results section header shows the row count and can be collapsed.

---

## SQL Preview

A live SQL preview panel shows the `SELECT` statement that the current designer state will produce. The statement updates on every change to column selection, aliases, sort, or filters. The generated SQL follows this shape:

```sql
SELECT table1.col1, table1.col2 AS alias, table2.col3
FROM table1
    INNER JOIN table2 ON table1.id = table2.table1_id
WHERE table1.col2 > 5
ORDER BY table2.col3, table1.col1
```

Click **Copy SQL to Editor** to send the generated SQL to the SQL editor and switch back to SQL mode.

---

## Collapsible Sections

The Canvas, Design Grid, and Results sections each have a clickable header that collapses or expands the section. This lets users focus on the part of the designer they are actively using without losing state.

---

## Saving and Loading Layouts

### Saving

Enter a name in the **Layout name…** input in the toolbar and click **Save**. The current designer state is stored as a named entry in the saved-query store under the prefix `__designer_layout:`.

### Loading

Click the **Load** dropdown to see all saved designer layouts. Selecting one restores the full canvas, joins, grid rows, and filter configuration.

Designer layouts are stored separately from regular saved SQL queries and do not appear in the saved-query list in SQL mode.

---

## Clearing the Canvas

Click **Clear** to remove all tables, joins, and grid rows and reset the designer to an empty state.

---

## Implementation Notes

The designer lives entirely inside `CSharpDB.Admin` with no changes to the engine, client, or API layers.

| File | Role |
|------|------|
| `src/CSharpDB.Admin/Components/Tabs/QueryDesignerPanel.razor` | Main designer component — canvas, grid, SQL preview, toolbar |
| `src/CSharpDB.Admin/Models/QueryDesignerModels.cs` | State model — `QueryDesignerState`, `DesignerTableNode`, `DesignerColumn`, `DesignerJoin`, `DesignerGridRow` |
| `src/CSharpDB.Admin/Helpers/QueryDesignerSqlBuilder.cs` | Pure static SQL generator from designer state |
| `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor` | Host tab — adds SQL/Designer mode toggle |
| `src/CSharpDB.Admin/Models/TabDescriptor.cs` | Stores `DesignerStateJson` for session persistence |
| `src/CSharpDB.Admin/wwwroot/js/interop.js` | `designerInterop` — document-level drag listeners and splitter drag |
| `src/CSharpDB.Admin/wwwroot/css/app.css` | Designer CSS using existing CSS custom property theme system |

Drag is implemented with a single set of document-level `mousedown` / `mousemove` / `mouseup` listeners, initialized once on first render. This avoids the Blazor Server re-render timing issues that would otherwise interrupt an active drag operation.

---

## Known Limitations (v1)

The v1 designer targets single `SELECT` statements with straightforward projection, filtering, and sorting. The following are not currently supported in the visual surface:

- `GROUP BY` / `HAVING` / aggregate functions
- `DISTINCT`, `LIMIT`, `OFFSET`
- `UNION`, `INTERSECT`, `EXCEPT`
- Subqueries
- Expressions in the projection list beyond simple column references
- SQL-to-designer round-trip import (SQL typed in SQL mode does not auto-populate the designer canvas)

Unsupported queries can still be written and executed in SQL mode.

---

## See Also

- [Roadmap](../roadmap.md) — Project-wide feature roadmap
- [Architecture Guide](../architecture.md) — Admin layering and component patterns
