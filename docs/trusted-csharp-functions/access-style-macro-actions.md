# Access-Style Macro Actions

Phase 8 extends Admin Forms action sequences with Access-style UI and data actions. The action metadata still stays declarative: host applications own executable C# callbacks, SQL/procedure opt-in policy, database connections, and navigation behavior.

## Supported Actions

| Action | Runtime behavior |
| --- | --- |
| `openForm` | Resolves a form by id or name and asks the host to open it. Arguments can include `mode`, `recordId`, `primaryKey`, `id`, `filter`, or `where`. |
| `closeForm` | Asks the host to close the active form entry tab or surface. |
| `applyFilter` | Filters the current form record list when `target` is `form`; filters a rendered `datagrid` control when `target` is that control id. |
| `clearFilter` | Clears the form or data-grid filter selected by `target`. |
| `runSql` | Executes SQL only when the host enables SQL actions. `@name` parameters are resolved from action arguments. |
| `runProcedure` | Executes a named database procedure only when the host enables procedure actions. `target` is the procedure name; action arguments become procedure arguments. |
| `setControlProperty` | Overrides rendered control properties such as `visible`, `enabled`, `readOnly`, `text`, `placeholder`, and bound `value`. |
| `setControlVisibility`, `setControlEnabled`, `setControlReadOnly` | Short forms for the corresponding `setControlProperty` calls. |

Existing actions such as `setFieldValue`, `runCommand`, `runActionSequence`, `newRecord`, `saveRecord`, `deleteRecord`, `refreshRecords`, `previousRecord`, `nextRecord`, and `goToRecord` continue to work in the same action sequence model.

## Filter Expressions

Filters use the same bracketed field expression style as conditions:

```json
{
  "kind": "applyFilter",
  "target": "ordersGrid",
  "value": "[Status] = @status AND [Total] > @minimum",
  "arguments": {
    "status": "Open",
    "minimum": 100
  }
}
```

Use `target: "form"` for the parent form list. Use a DataGrid control id for child-row filtering.

## Open Form Arguments

`openForm` carries navigation arguments to the host:

```json
{
  "kind": "openForm",
  "target": "Orders Entry",
  "arguments": {
    "recordId": "$record.Id",
    "mode": "view",
    "filter": "[Status] = 'Open'"
  }
}
```

The built-in admin tab host forwards those values to `DataEntry` as initial state. `mode: "new"` starts a writable form on a new record. `recordId` navigates to the requested primary key after load. `filter` or `where` applies an initial form filter.

## SQL And Procedure Actions

Use `runProcedure` when the workflow should invoke reusable database-owned SQL,
for example allocating an order, receiving a purchase order, processing a
return, or returning a packaged operational snapshot. A procedure body can run
multiple SQL statements in one execution and can return follow-up result sets.

```json
{
  "kind": "runProcedure",
  "target": "AllocateOrder",
  "arguments": {
    "orderId": 7005,
    "allocatedBy": "Wave Planner",
    "note": "Allocated from form action sequence."
  },
  "stopOnFailure": true
}
```

Use `runCommand` instead when the workflow needs host-owned C# behavior such as
email, queues, external APIs, filesystem access, or other services. A common
sequence is `runProcedure` for database updates followed by `runCommand` for a
host notification.

Rendered Admin form runtimes can leave `runSql` and `runProcedure` disabled by
policy. That keeps database-mutating actions explicit at the host boundary even
though procedure definitions themselves are database metadata.

## Conditional UI Rules

Form-level `rules` apply control property effects whenever their condition is true:

```json
{
  "ruleId": "archived-state",
  "condition": "[Status] = 'Archived'",
  "effects": [
    { "controlId": "statusBox", "property": "readOnly", "value": true },
    { "controlId": "archiveButton", "property": "enabled", "value": false }
  ]
}
```

The designer property inspector includes a rules editor and a validation panel for action/rule readiness.

## Diagnostics

Subscribe to `FormActionDiagnostics.Listener` to observe action execution:

```csharp
using CSharpDB.Admin.Forms.Contracts;

using IDisposable subscription = FormActionDiagnostics.Listener.Subscribe(observer);
```

Events use `FormActionDiagnostics.InvocationEventName` and carry `FormActionInvocationDiagnostic`, including action kind, target, form id, event name, action sequence name, step index, elapsed time, success state, cancellation state, result message, exception message, and metadata.

## Sample

See `samples/trusted-csharp-host/access-style-macro-form.json` for a form manifest that combines open form, data-grid filtering, SQL execution, control property changes, and conditional UI rules.
