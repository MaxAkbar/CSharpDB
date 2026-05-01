# Fulfillment Ops Admin Automation Tutorial

This tutorial walks through a realistic CSharpDB Admin scenario for an app
builder. You will use the fulfillment demo database to inspect operational
tables, work with JSON document collections, review Access-style forms and
reports, run stored procedures, build a small form automation workflow, and
inspect the trusted host callbacks that make executable behavior visible
without storing C# code in the database.

The goal is not just to click through Admin. The goal is to understand the
product model:

- tables and collections hold operational data
- stored procedures package reusable SQL work inside the database
- forms and reports provide Access-style application surfaces
- macro actions are saved as database metadata
- trusted callbacks are C# code owned and registered by the host application
- callback readiness tells app builders whether saved metadata can run in the
  current host

## Scenario

You are building an internal fulfillment console for an operations team. The
team needs to:

- review open orders
- create or review shipments
- receive purchase orders
- process returns
- inspect scanner-session JSON captured by warehouse devices
- inspect webhook payloads received from external systems
- run repeatable SQL workflows such as allocation, receiving, and status
  snapshots
- run a few host-owned automation hooks from buttons and form events
- verify that every callback referenced by forms, reports, or automation is
  registered by the current Admin host

CSharpDB Admin fits this scenario because it combines database browsing,
document inspection, form design, reports, declarative macro actions, and
stored procedure execution with trusted C# callback visibility in one tool.

## Prerequisites

Use a copy of the demo database for tutorial work. The tutorial includes create,
edit, and delete steps for disposable objects.

From the repository root:

```powershell
Copy-Item `
  -LiteralPath .\src\CSharpDB.Admin\fulfillment-hub-demo.db `
  -Destination .\src\CSharpDB.Admin\fulfillment-hub-tutorial.db `
  -Force
```

Start Admin against the copied database:

```powershell
$env:ConnectionStrings__CSharpDB = 'Data Source=fulfillment-hub-tutorial.db'
dotnet run --project .\src\CSharpDB.Admin\CSharpDB.Admin.csproj --urls http://localhost:62818
```

Open:

```text
http://localhost:62818/
```

The Admin header should show a connected database. If it still shows
`fulfillment-hub-demo.db`, stop Admin and restart it with the environment
variable above.

Important trust boundary:

- trusted callbacks are ordinary in-process C# code registered by the host
- database metadata can reference callback names, but does not store callback
  implementations
- CSharpDB does not use WASM, source scanning, or in-process database-owned
  plugin assemblies for this workflow

## Tour The Admin App

Start in the Object Explorer on the left.

You should see these high-level groups:

- **User Tables**
- **Collections**
- **Forms**
- **Reports**
- **Procedures**
- **Callbacks**

Use the filter chips near the top of Object Explorer to focus the list. The
tutorial uses these objects:

| Area | Objects |
| --- | --- |
| Tables | `orders`, `shipments`, `returns`, `products`, `inventory_positions`, `purchase_orders` |
| Collections | `scanner_sessions`, `webhook_archive` |
| Forms | `Order Workbench`, `Purchase Order Receiving`, `Return Intake` |
| Reports | `Low Stock Watch`, `Open Order Queue`, `Shipment Manifest` |
| Procedures | `AllocateOrder`, `CreateShipment`, `ReceivePurchaseOrder`, `RecordReturn`, `RefreshOperationalStats` |
| Callbacks | `Slugify`, `EchoAutomationEvent` |

If Object Explorer does not show a new object after a write, click the refresh
button in the Object Explorer header.

## Browse Operational Tables

1. Click **Tables** in Object Explorer.
2. Open `orders`.
3. Page through the order rows.
4. Open `shipments`.
5. Open `returns`.
6. Open `inventory_positions`.

Notice the table browser pattern:

- each table opens in its own tab
- the toolbar contains refresh and paging controls
- the grid is for row browsing
- the detail or edit area is for record-level work

This is the relational side of the fulfillment app. The next sections use
forms, procedures, collections, reports, and callbacks to build an application
workflow on top of these tables.

## Use The Existing Forms

1. Click **Forms** in Object Explorer.
2. Open `Order Workbench`.
3. Use the data-entry runtime to move through order records.
4. Open `Purchase Order Receiving`.
5. Open `Return Intake`.

These forms are the Access-style app surfaces. An app builder can design forms
over tables and views, then attach event bindings and action sequences.

Use the designer entry for `Order Workbench` when you want to edit layout or
actions. Use the data-entry entry when you want to run the form as an operator
would.

## Use Stored Procedures For Reusable Database Work

Stored procedures are database-owned SQL workflows. They are the right tool
when the behavior is still database work:

| Use a stored procedure when... | Use something else when... |
| --- | --- |
| the logic is a reusable set of SQL statements | the user is running one ad hoc query |
| multiple table updates should succeed or fail together | the workflow needs external APIs, files, email, queues, or services |
| the operation needs named parameters and defaults | the workflow only changes form UI state |
| the operation should return follow-up result sets after it writes | the behavior must be host-owned C# |

In this model:

- procedures store SQL and parameter metadata in the database
- procedure execution runs inside one transaction
- `@parameter` references in the SQL body are bound from Args JSON or `EXEC`
  arguments
- procedures can read and write tables, write audit/event rows, and return one
  or more result sets
- procedures do not store C# source and are not host callbacks

### Inspect And Run A Read-Only Procedure

1. Click **Procedures** in Object Explorer.
2. Open `RefreshOperationalStats`.
3. Review the body SQL. It returns table stats, shortage-watch rows, and the
   open order board.
4. Confirm the procedure has no parameters.
5. In **Args JSON**, keep:

   ```json
   {}
   ```

6. Click **Run**.

Expected result:

- the execution summary reports success
- the tab lists each statement in the procedure body
- result grids appear for the `SELECT` statements

You can also run the same procedure from the SQL editor:

```sql
EXEC RefreshOperationalStats;
```

### Inspect Write Procedures Before Running Them

Open these procedures and read their body SQL and parameter lists:

- `AllocateOrder`
- `ReceivePurchaseOrder`
- `CreateShipment`
- `RecordReturn`

These are good stored-procedure candidates because they coordinate several
related table changes and then return review data. For example, `AllocateOrder`
updates inventory reservations, updates order-line allocation state, updates
the order, writes an `ops_events` row, and returns follow-up rows for review.

Run write procedures only against the copied tutorial database. They are meant
to demonstrate operational workflows, but they still mutate real rows in that
copy.

### Create A Tutorial Read-Only Procedure

Create one disposable procedure so you can practice the editor without changing
operational data.

1. Right-click **Procedures** and choose **New Procedure...**, or use the
   command palette item **New Procedure**.
2. Set **Name** to:

   ```text
   tutorial_OpenOrderSnapshot
   ```

3. Set **Description** to:

   ```text
   Tutorial read-only snapshot of order board rows by status.
   ```

4. Leave **Enabled** checked.
5. Set **Body SQL** to:

   ```sql
   SELECT order_number,
          customer_name,
          warehouse_code,
          order_status,
          priority_code,
          total_amount
   FROM order_fulfillment_board
   WHERE order_status = @status
   ORDER BY required_ship_date, priority_code DESC, order_number;
   ```

6. Add one parameter:

   | Name | Type | Required | Default | Description |
   | --- | --- | --- | --- | --- |
   | `status` | `TEXT` | unchecked | `released` | Order status to show. |

7. Click **Save**.
8. In **Args JSON**, enter:

   ```json
   {
     "status": "released"
   }
   ```

9. Click **Run** and review the result grid.

The same procedure can be run from the SQL editor with either JSON args:

```sql
EXEC tutorial_OpenOrderSnapshot { "status": "allocated" };
```

or SQL-style args:

```sql
EXEC tutorial_OpenOrderSnapshot @status = 'allocated';
```

### When Procedures Meet Forms

Forms can reference stored procedures through the `RunProcedure` macro action.
Use that when a button or form event should invoke a reviewed database workflow,
such as allocation, receiving, or a read-only snapshot. The action target is the
procedure name, and the action arguments become procedure arguments.

Use `RunCommand` instead when the workflow must leave the database and call
host-owned C# services. A common pattern is:

1. `RunProcedure` performs the database work.
2. `RunCommand` tells host-owned C# to send a notification, publish a message,
   or call an external service.

The default Admin rendered form host may keep `RunProcedure` disabled unless
the host explicitly opts in. The procedure editor and SQL editor can still run
procedures directly.

## Use Collections For Operational JSON

Collections are for JSON documents that are better kept as document payloads
than flattened relational rows. In this demo they represent warehouse scanner
sessions and webhook payload archives.

### Inspect `scanner_sessions`

1. Click **Collections** in Object Explorer.
2. Open `scanner_sessions`.
3. Confirm the collection tab shows a paged document grid.
4. Select a document.
5. Review the indented JSON in the detail panel.
6. Change the page size to `10`, `25`, `50`, or `100`.
7. Use the exact-key lookup if you know a document key.

The demo data may fit on a single page. The important behavior is that the tab
uses the same paged browsing model for two documents as it does for thousands.

The grid shows:

- row number
- document key
- JSON kind
- compact preview

The detail panel shows the selected document as formatted JSON.

### Create And Delete A Disposable Document

Use a tutorial-only key so cleanup is obvious.

1. In `scanner_sessions`, click **New Document**.
2. Enter this key:

   ```text
   tutorial_scanner_session
   ```

3. Enter this JSON:

   ```json
   {
     "sessionId": "tutorial_scanner_session",
     "warehouse": "SEA-01",
     "operator": "tutorial",
     "startedAt": "2026-05-01T09:00:00Z",
     "events": [
       {
         "kind": "scan",
         "sku": "TUTORIAL-SKU",
         "quantity": 1
       }
     ],
     "status": "review"
   }
   ```

4. Confirm **Save** is enabled only while the JSON is valid.
5. Click **Save**.
6. Select the saved document and confirm the preview updates.
7. Click **Delete**.
8. Confirm the delete prompt.

If you intentionally break the JSON, for example by removing a closing brace,
Admin keeps **Save** disabled and shows a validation message.

### Inspect `webhook_archive`

1. Open `webhook_archive`.
2. Select a document.
3. Review the payload structure.
4. Review the page controls and page-size selector. If your copied database has
   more webhook documents than one page can hold, move between pages.

This is the document side of the fulfillment app. The app builder can inspect
payloads without needing a new transport contract or a custom JSON viewer.

## Build An Access-Style Workflow

This section adds a tutorial-only workflow to `Order Workbench`. The workflow
demonstrates the macro/action model. It stores action metadata in the form; it
does not store executable C# code in the database.

Use a copied tutorial database before making these edits.

### Add Tutorial Controls

1. Open the `Order Workbench` designer.
2. Add a label near the top of the form.
3. Set the label text to:

   ```text
   Tutorial review mode is active.
   ```

4. Copy the generated read-only control ID from the property inspector and
   write it down as `<review-banner-id>`.
5. Add a command button.
6. Set the button text to:

   ```text
   Tutorial Review
   ```

7. Copy its generated control ID as `<review-button-id>`.
8. Add a second command button.
9. Set the button text to:

   ```text
   Clear Tutorial Filter
   ```

10. Copy its generated control ID as `<clear-filter-button-id>`.

The current designer generates control IDs and shows them as read-only. Use the
recorded IDs anywhere this tutorial asks for a target control. These controls
are tutorial-owned because their visible text and action sequence names are
tutorial-specific, so the cleanup step is still easy.

### Add The Review Action Sequence

Select the **Tutorial Review** button. Add an `OnClick` event, then add an
action sequence named:

```text
tutorial_prepare_review
```

Add these steps in order:

| Step | Action | Target | Value or settings |
| ---: | --- | --- | --- |
| 1 | `SetControlVisibility` | `<review-banner-id>` | `true` |
| 2 | `SetControlEnabled` | `<clear-filter-button-id>` | `true` |
| 3 | `SetControlReadOnly` | `<review-button-id>` | `true` |
| 4 | `ApplyFilter` | `form` | `Status <> 'Closed'` |
| 5 | `OpenForm` | `Purchase Order Receiving` | arguments JSON shown below |
| 6 | `RunCommand` | empty | command name: `EchoAutomationEvent`; arguments JSON shown below |
| 7 | `ShowMessage` | empty | `Tutorial review mode is active.` |

For step 5, use JSON in the arguments field:

```json
{
  "mode": "browse",
  "filter": "Status <> 'Closed'"
}
```

For step 6, use JSON in the arguments field:

```json
{
  "source": "tutorial_review_button"
}
```

Save the form.

Run the data-entry form and click **Tutorial Review**.

Expected result:

- the banner becomes visible
- the clear-filter button becomes enabled
- the review button becomes read-only or disabled for editing where supported
- the current form applies the `Status <> 'Closed'` filter
- `Purchase Order Receiving` opens in a new tab with the supplied mode/filter
- `EchoAutomationEvent` runs as a trusted host command
- the form shows the tutorial message

If the filter fails, check that the form's source has a `Status` field. If your
copy of the form uses a different field name, change the filter to a field that
exists on the form source.

### Add The Clear Filter Sequence

Select the **Clear Tutorial Filter** button. Add an `OnClick` event, then add
an action sequence named:

```text
tutorial_clear_review
```

Add these steps:

| Step | Action | Target | Value or settings |
| ---: | --- | --- | --- |
| 1 | `ClearFilter` | `form` | empty |
| 2 | `SetControlVisibility` | `<review-banner-id>` | `false` |
| 3 | `SetControlReadOnly` | `<review-button-id>` | `false` |
| 4 | `ShowMessage` | empty | `Tutorial review mode cleared.` |

Save the form, run data entry, and click **Clear Tutorial Filter**.

Expected result:

- the form filter is cleared
- the banner is hidden
- the review button becomes editable again where supported
- a confirmation message appears

### Optional Power Actions: SQL And Procedures

The action model includes `RunSql` and `RunProcedure`. The default Admin
rendered form host may leave those actions disabled by host policy, even for
read-only procedure bodies. That is intentional: SQL/procedure actions can
change data, so a host should enable them deliberately.

To understand the design-time shape, add a disabled or non-running tutorial
sequence named:

```text
tutorial_power_actions_reference
```

Add these steps with `StopOnFailure = false`:

| Step | Action | Target | Value or settings |
| ---: | --- | --- | --- |
| 1 | `RunSql` | empty | `SELECT COUNT(*) AS open_orders FROM orders WHERE Status <> 'Closed'` |
| 2 | `RunProcedure` | `tutorial_OpenOrderSnapshot` | arguments JSON shown below |
| 3 | `ShowMessage` | empty | `Power action reference completed.` |

For step 2, use JSON in the arguments field:

```json
{
  "status": "released"
}
```

If you run this sequence in a host where SQL/procedure actions are disabled,
the expected result is a clear failure message such as `RunSql action is
disabled by host policy` or `RunProcedure action is disabled by host policy`.
That is the correct default posture. In a host that enables these actions, use
a copied tutorial database and only run idempotent or disposable operations.

If `tutorial_OpenOrderSnapshot` was not created earlier, use
`RefreshOperationalStats` instead and leave the arguments field as `{}`.

## Trusted Host Callbacks

Callbacks are the bridge between saved database metadata and host-owned C#.

1. Click **Callbacks** in Object Explorer.
2. Open the callback catalog.
3. Select `Slugify`.
4. Review its descriptor:
   - kind: scalar function
   - runtime: `HostCallback`
   - return type: text
   - deterministic/null behavior
   - capability requests and policy decision
5. Select `EchoAutomationEvent`.
6. Review its descriptor:
   - kind: command
   - runtime: `HostCallback`
   - command capability
   - policy decision

The readiness badge summarizes whether saved metadata references callbacks that
are missing from the current host.

Important:

- `Slugify` and `EchoAutomationEvent` are registered by the Admin host at
  startup
- the database can reference these names
- the database does not contain their C# implementation
- descriptor and policy metadata make callbacks visible, but do not sandbox
  them

## Demonstrate Missing Callback Readiness

Use the copied tutorial database for this step.

1. Open the `Order Workbench` designer.
2. Select the **Tutorial Review** button.
3. Add one more `RunCommand` step to `tutorial_prepare_review`.
4. Set the command name to:

   ```text
   tutorial_SendOpsDigest
   ```

5. Set `StopOnFailure = false`.
6. Add arguments:

   ```json
   {
     "source": "tutorial_missing_callback_demo"
   }
   ```

7. Save the form.
8. Open **Callbacks**.
9. Click **Refresh**.

Expected result:

- the readiness badge reports a missing callback
- the grid includes `tutorial_SendOpsDigest`
- the row is marked missing
- the details panel shows the form/reference location
- **Stubs** is enabled

Click **Stubs** to copy registration stub source for all missing callbacks, or
select the missing row and click **Copy Stub** for only that callback.

The copied stub is a developer handoff. It is not stored in the database and it
is not automatically trusted. A host developer must implement and register the
command in the host application.

Cleanup options:

- remove the `tutorial_SendOpsDigest` step from the form, or
- keep it as a deliberate missing-callback example in the copied tutorial
  database

## Implement The Missing Callback In A Host

The previous section created the app-builder side of the story: database
metadata now references a command named `tutorial_SendOpsDigest`. The database
still does not contain C# code. A host developer must add that command to the
host application and register it at startup.

For the Admin demo host, the registration point is:

```text
src/CSharpDB.Admin/Services/AdminHostCallbacks.cs
```

For a smaller VS Code debugging walkthrough, open the sample host project:

```powershell
code .\samples\trusted-csharp-host
```

That sample shows the same pattern with `Slugify` and
`AuditCustomerChange`: callbacks are ordinary C# methods/delegates, registered
with `DbFunctionRegistry` or `DbCommandRegistry`, then invoked by SQL or form
automation metadata that references them by name.

A host developer would implement the tutorial command like this inside the
host's command registry setup:

```csharp
commands.AddCommand(
    "tutorial_SendOpsDigest",
    new DbCommandOptions("Sends a tutorial fulfillment operations digest."),
    static context =>
    {
        string source = context.Arguments.TryGetValue("source", out DbValue value)
            ? value.AsText
            : "unknown";

        // Call host-owned services here: email, queues, logging, APIs, etc.
        return DbCommandResult.Success($"Tutorial digest requested by {source}.");
    });
```

Debug flow:

1. Put a breakpoint inside the command delegate.
2. Start the host from VS Code with `F5`.
3. Open Admin against the copied tutorial database.
4. Run the form action that references `tutorial_SendOpsDigest`.
5. Confirm the breakpoint is hit in host-owned C# code.
6. Refresh **Callbacks** and confirm readiness changes from missing to
   registered/allowed when the name and kind match.

The generated stub from Admin is only a starting point for the developer. The
trusted implementation is the reviewed C# code compiled into the host app.

## Reports And Review

Reports let app builders validate that the workflow supports operational
review, not only data entry.

1. Click **Reports** in Object Explorer.
2. Open `Low Stock Watch`.
3. Preview the report.
4. Open `Open Order Queue`.
5. Preview the report.
6. Open `Shipment Manifest`.
7. Preview the report.

Use these reports to answer:

- do order changes show up where operators expect?
- does low-stock review still surface the right products?
- can a shipment manifest be reviewed after order/shipment work?
- do report expressions and data sources still load after form automation
  changes?

If a report uses host-registered scalar functions in calculated expressions,
those functions appear in the same callback catalog as form commands.

## Security Model

This tutorial uses several different kinds of behavior:

| Behavior | Stored in database? | Executes C#? | Trust model |
| --- | --- | --- | --- |
| Tables and rows | yes | no | data only |
| Collection documents | yes | no | JSON data only |
| Stored procedures | yes | no | declarative SQL executed by CSharpDB |
| Form/report metadata | yes | no | declarative metadata |
| Macro/action sequences | yes | no | interpreted by Admin/runtime |
| Trusted callbacks | name/reference only | yes | host-owned in-process C# |

CSharpDB deliberately does not treat database files as executable plugin
packages.

Rejected for this feature track:

- WASM plugin execution
- source scanning as a security boundary
- runtime-loaded database-owned assemblies in the CSharpDB process
- C# source stored in the database and compiled on normal open paths

Future out-of-process .NET/C# workers remain a gated exploration path for
portable extension packages. They are not part of this tutorial workflow.

## Troubleshooting

### The Admin app opened the original demo database

Stop Admin and restart with:

```powershell
$env:ConnectionStrings__CSharpDB = 'Data Source=fulfillment-hub-tutorial.db'
dotnet run --project .\src\CSharpDB.Admin\CSharpDB.Admin.csproj --urls http://localhost:62818
```

### Object Explorer does not show the object I expect

Click the refresh icon in Object Explorer. If the object is still missing,
check that the tutorial database copy is the active database.

### A collection document will not save

Check:

- the key is not blank
- the JSON is valid
- the collection name is `scanner_sessions` or another existing collection
- you are not editing a read-only existing key

### A form filter fails

Check:

- the filter references a field that exists on the form source
- text values are quoted
- brackets are balanced
- parameters such as `@status` have matching action arguments

Examples:

```text
Status <> 'Closed'
[Status] = 'Ready'
Quantity > 0
ClosedAt = null
```

### `RunSql` or `RunProcedure` is disabled

That is expected in the default safe host posture unless the host explicitly
enables those rendered form actions. Use `RunCommand` for trusted host-owned C#
callbacks, or enable SQL/procedure actions only in a host that has reviewed the
risk.

### A procedure will not save or run

Check:

- the procedure name is a simple identifier
- every `@parameter` referenced by Body SQL is listed in Parameters
- Args JSON contains every required parameter
- argument names match parameter names without the `@` prefix
- text values are JSON strings in Args JSON or single-quoted in `EXEC`
- the procedure is enabled

Examples:

```json
{
  "status": "released"
}
```

```sql
EXEC tutorial_OpenOrderSnapshot @status = 'released';
```

### A callback is missing

Open **Callbacks**, refresh the catalog, select the missing callback, and use
**Copy Stub**. Give the generated stub to the host developer who owns the
application. The fix is to register host-owned C# code with the same callback
name and compatible arity.

### A callback is denied

Review the capability grid and policy reason in the callback details. A denied
callback should not execute. The host policy must grant the requested capability
before the callback is considered ready.

### The tutorial controls should be removed

Open the `Order Workbench` designer and delete:

- the label with text `Tutorial review mode is active.`
- the `Tutorial Review` button
- the `Clear Tutorial Filter` button
- action sequences whose names start with `tutorial_`

Save the form and refresh Object Explorer.

Also delete the disposable procedure:

- `tutorial_OpenOrderSnapshot`

## What You Built

You used one copied fulfillment database to exercise the full current app-builder
story:

- browsed relational operational tables
- inspected existing stored procedures and created a disposable read-only
  procedure
- inspected paged JSON collections
- edited a disposable collection document
- used forms as Access-style data-entry surfaces
- added macro-style form action sequences
- used trusted host callbacks from saved metadata
- inspected callback readiness and generated missing registration stubs
- reviewed reports as operational validation surfaces
- kept executable code host-owned instead of database-owned

That is the intended production posture for the current CSharpDB Admin
automation model.
