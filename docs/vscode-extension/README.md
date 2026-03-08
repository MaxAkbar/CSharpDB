# CSharpDB VS Code Extension

A VS Code extension providing a full IDE experience for working with CSharpDB databases: schema exploration, SQL editing with IntelliSense, query results, data browsing with CRUD, table designer, and storage diagnostics.

The extension connects via the existing [REST API](../architecture.md) (34 endpoints on port 61818) and can auto-start the `CSharpDB.Api` server when `.db` files are detected.

---

## Project Structure

```
vscode-extension/
├── .vscode/
│   ├── launch.json                    # Debug config for extension dev
│   └── tasks.json                     # Build tasks
├── src/
│   ├── extension.ts                   # Entry point (activate/deactivate)
│   ├── api/
│   │   ├── client.ts                  # REST API client (all 34 endpoints)
│   │   └── types.ts                   # TypeScript interfaces matching API DTOs
│   ├── providers/
│   │   ├── schemaTreeProvider.ts      # TreeDataProvider for schema explorer
│   │   ├── sqlCompletionProvider.ts   # CompletionItemProvider for IntelliSense
│   │   └── sqlHoverProvider.ts        # HoverProvider for table/column info
│   ├── panels/
│   │   ├── queryResultsPanel.ts       # WebviewPanel for query results grid
│   │   ├── dataBrowserPanel.ts        # WebviewPanel for table data CRUD
│   │   ├── tableDesignerPanel.ts      # WebviewPanel for create/alter table
│   │   └── storageDiagnosticsPanel.ts # WebviewPanel for storage/WAL diagnostics
│   ├── server/
│   │   ├── serverManager.ts           # Auto-start/stop CSharpDB.Api process
│   │   └── connectionManager.ts       # Connection state, health checks
│   └── utils/
│       ├── htmlBuilder.ts             # HTML template helpers for webviews
│       └── disposable.ts              # Disposable pattern helpers
├── media/
│   ├── styles/                        # VS Code theme-aware CSS
│   ├── scripts/                       # Client-side JS for webview panels
│   └── icons/                         # SVG icons (table, column, index, view, trigger, database)
├── syntaxes/
│   └── csharpdb-sql.tmLanguage.json   # TextMate grammar for SQL highlighting
├── language-configuration.json        # Bracket/comment config for SQL
├── package.json                       # Extension manifest
├── tsconfig.json
├── webpack.config.js
├── .vscodeignore
└── README.md
```

---

## Implementation Phases

### Phase 1: Foundation

**Delivers:** Working extension that connects to CSharpDB.Api and shows connection status in the status bar.

| File | Purpose |
|------|---------|
| `package.json` | Manifest: activity bar icon, schema explorer view, commands (`connect`, `disconnect`, `refresh`, `newQuery`, `executeQuery`, `browseTable`, `openTableDesigner`, `openDiagnostics`), settings (`serverUrl`, `autoStartServer`, `dotnetPath`, `apiProjectPath`), language registration (`csharpdb-sql` / `.csql`), keybinding (`Ctrl+Enter`) |
| `src/api/types.ts` | TypeScript interfaces mirroring API DTOs: `TableSchemaResponse`, `ColumnResponse`, `BrowseResponse`, `SqlResultResponse`, `DatabaseInfoResponse`, `IndexResponse`, `ViewResponse`, `TriggerResponse`, plus diagnostics types |
| `src/api/client.ts` | REST client using `fetch` covering all 34 endpoints |
| `src/server/serverManager.ts` | Spawns `dotnet run --project <apiPath>`, polls `/api/info` until ready (30 s timeout), graceful shutdown, auto-detect project path |
| `src/server/connectionManager.ts` | Connection state, health checks every 15 s, `onConnectionChanged` event |
| `src/extension.ts` | Creates managers, registers commands, status bar item ("CSharpDB: Connected (N tables)"), `FileSystemWatcher` for `*.db` files |

**Connection modes:**
- **Auto-start** — Extension spawns the `CSharpDB.Api` process and manages its lifecycle.
- **Manual** — User starts the server externally and configures `csharpdb.serverUrl` in settings.

---

### Phase 2: Schema Explorer Sidebar

**Delivers:** Tree view sidebar showing tables (with columns), indexes, views, triggers with context menus.

`src/providers/schemaTreeProvider.ts` implements `TreeDataProvider<SchemaTreeItem>`:

- **Tree hierarchy:** Database > Tables / Views / Indexes / Triggers groups > items > columns
- **Context menus:** Browse Data, View Schema, New Query, Drop, Rename
- **Refresh:** On command and after DDL mutations
- **Icons:** Custom SVG icons in `media/icons/`
- **Blueprint:** Modeled after `src/CSharpDB.Admin/Components/Layout/NavMenu.razor`

---

### Phase 3: SQL Editor with Language Support

**Delivers:** `.csql` files with syntax highlighting, IntelliSense for table/column names, and `Ctrl+Enter` execute.

| File | Purpose |
|------|---------|
| `syntaxes/csharpdb-sql.tmLanguage.json` | TextMate grammar with keywords ported from `SqlHighlighter.cs` (SELECT, FROM, WHERE, JOIN, CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, etc.) and functions (COUNT, SUM, AVG, MIN, MAX, CAST, COALESCE, UPPER, LOWER, LENGTH, etc.) |
| `src/providers/sqlCompletionProvider.ts` | Table/view names after FROM/JOIN/INTO, column names after `table.`, SQL keywords, function names, snippet templates |
| `src/providers/sqlHoverProvider.ts` | Hover table name to see column list; hover column to see type and constraints |

Execute command sends selected text (or full document) to `executeSql()` and opens the results panel.

---

### Phase 4: Query Results Panel

**Depends on:** Phase 3

**Delivers:** SQL results in a sortable data grid webview.

| File | Purpose |
|------|---------|
| `src/panels/queryResultsPanel.ts` | Renders `SqlResultResponse`: table grid for queries, "N rows affected" for mutations, elapsed time, error display |
| `media/scripts/grid.js` | Column sorting, resizing, cell copy, NULL/BLOB styling |
| `media/styles/grid.css` | VS Code theme-aware using CSS custom properties |

Blueprint: modeled after `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor` and `DataGrid.razor`.

---

### Phase 5: Data Browser with CRUD

**Delivers:** Full table data browsing with inline editing, pagination, sort, filter.

| File | Purpose |
|------|---------|
| `src/panels/dataBrowserPanel.ts` | Webview: toolbar (Refresh, Add Row, Delete, Save, Discard), pagination (10/25/50/100), read-only mode for views |
| `media/scripts/dataBrowser.js` | Double-click inline editing, row states (Unmodified/Modified/New/Deleted), batched save via `insertRow`/`updateRow`/`deleteRow`, client-side sort/filter |

Blueprint: modeled after `src/CSharpDB.Admin/Components/Tabs/DataTab.razor` and `DataGrid.razor`.

---

### Phase 6: Table Designer

**Delivers:** Form UI for CREATE/ALTER TABLE, index management, trigger management.

`src/panels/tableDesignerPanel.ts` provides a webview with:

- **Create mode:** Table name, dynamic column list (name, type dropdown, PK/NOT NULL), SQL preview
- **Alter mode:** Existing columns/indexes/triggers, add/drop/rename operations
- **Index management:** List, create/drop forms
- **Trigger management:** List, create/drop forms

---

### Phase 7: Storage Diagnostics

**Delivers:** Storage internals panel matching the Blazor admin's storage tab.

`src/panels/storageDiagnosticsPanel.ts` provides a webview with sections:

1. **Database Header** — File length, page count, magic, version, schema root, freelist head
2. **WAL** — Path, frames, commit frames, trailing bytes
3. **Page Type Histogram** — Visual breakdown by page type
4. **Index Checks** — Root OK, Table OK, Columns OK, Reachable
5. **Integrity Issues** — Severity badges
6. **Page Drill-Down** — Page ID input with type, cells, hex dump

Blueprint: modeled after `src/CSharpDB.Admin/Components/Tabs/StorageTab.razor`.

---

### Phase 8: Polish, Testing, Packaging

1. **Error handling** — Parse `ProblemDetails` responses, `vscode.window.showErrorMessage()`
2. **Settings validation** — URL format, dotnet path, project path
3. **Theming** — Test light, dark, high-contrast themes
4. **Tests** — Unit tests for API client and schema tree; integration test against live server
5. **Build** — Webpack bundle, `.vscodeignore`, `vsce package` to produce `.vsix`
6. **README** — Screenshots, quick start, configuration reference

---

## Parallelization

```
Phase 1 (Foundation)
  ├── Phase 2 (Schema Explorer)
  ├── Phase 3 (SQL Editor) ──> Phase 4 (Query Results)
  ├── Phase 5 (Data Browser)
  ├── Phase 6 (Table Designer)
  └── Phase 7 (Storage Diagnostics)
Phase 8 (Polish & Packaging)
```

Phases 2, 3, 5, 6, and 7 can proceed in parallel once Phase 1 is complete. Phase 4 depends on Phase 3. Phase 8 is the final integration pass.

---

## Key Reference Files

These existing files inform the extension's design and should be referenced during implementation:

| File | Reuse For |
|------|-----------|
| `src/CSharpDB.Api/Dtos/Responses.cs` | TypeScript type definitions (mirror JSON shapes) |
| `src/CSharpDB.Api/Dtos/Requests.cs` | Request body shapes |
| `src/CSharpDB.Api/Endpoints/*.cs` | All 34 endpoint route and parameter definitions |
| `src/CSharpDB.Admin/Components/Layout/NavMenu.razor` | Schema explorer tree hierarchy blueprint |
| `src/CSharpDB.Admin/Helpers/SqlHighlighter.cs` | SQL keyword and function lists for TextMate grammar |
| `src/CSharpDB.Admin/Components/Tabs/DataTab.razor` | Data browser and table designer interaction patterns |
| `src/CSharpDB.Admin/Components/Tabs/QueryTab.razor` | SQL editor and query results panel blueprint |
| `src/CSharpDB.Admin/Components/Tabs/StorageTab.razor` | Storage diagnostics panel blueprint |
| `src/CSharpDB.Admin/Components/Shared/DataGrid.razor` | Grid interaction patterns (sort, edit, pagination) |

---

## Verification Checklist

1. `F5` from `vscode-extension/` launches extension development host
2. Open a folder containing a `.db` file — server auto-starts, schema tree populates
3. Create a `.csql` file — syntax highlighting and IntelliSense work
4. `Ctrl+Enter` — query results appear in grid panel
5. Right-click table in explorer — Browse Data — inline editing, save changes
6. Table Designer — create new table, verify it appears in schema explorer
7. Storage Diagnostics — verify data matches CLI `.info` output
8. Manual mode — start server externally, set `csharpdb.serverUrl`, connect successfully

---

## See Also

- [Roadmap](../roadmap.md) — Project-wide feature roadmap
- [Architecture Guide](../architecture.md) — How the engine is structured
- [Storage Extensibility Guide](../tutorials/storage/extensibility.md) — Interface catalog and extension points
