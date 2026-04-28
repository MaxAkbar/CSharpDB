# Trusted C# Scalar Functions

CSharpDB can call host-registered C# scalar functions from SQL and the embedded expression surfaces that sit on top of the engine. This is the CSharpDB equivalent of an Access-style application function integration: the application owns the C# code, registers it while opening or hosting the database, and users call the function by name in database expressions.

This feature is intentionally trusted and in-process. It does not store C# source code in the database, sandbox user code, load plugin assemblies from database files, or serialize delegates over HTTP or gRPC.

---

## Trusted Commands

CSharpDB also supports trusted host-registered commands for application automation surfaces. Commands are different from scalar functions:

- Scalar functions return a `DbValue` and can be used inside SQL or formulas.
- Commands return a `DbCommandResult` and are invoked by host-driven events such as Admin Forms lifecycle events, Admin Reports render events, and pipeline run hooks.

Commands are intended for Access-style application automation such as auditing, calling application services, sending notifications, refreshing derived state, coordinating UI workflows, or publishing operational run events. They are trusted in-process callbacks registered by the host application.

```csharp
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

builder.Services.AddCSharpDbAdminForms(commands =>
{
    commands.AddCommand(
        "AuditCustomerChange",
        new DbCommandOptions("Writes an application audit entry."),
        static async (context, ct) =>
        {
            long customerId = context.Arguments["Id"].AsInteger;
            string eventName = context.Metadata["event"];

            await WriteAuditAsync(customerId, eventName, ct);
            return DbCommandResult.Success();
        });
});
```

Command names are case-insensitive identifiers. Duplicate command names are rejected during registration.

---

## What You Can Register

V1 supports synchronous scalar functions:

```csharp
public delegate DbValue DbScalarFunctionDelegate(
    DbScalarFunctionContext context,
    ReadOnlySpan<DbValue> arguments);
```

A scalar function receives database values and returns one database value. Supported value types are:

| CSharpDB type | Read with | Return with |
| --- | --- | --- |
| `DbType.Integer` | `value.AsInteger` | `DbValue.FromInteger(...)` |
| `DbType.Real` | `value.AsReal` | `DbValue.FromReal(...)` |
| `DbType.Text` | `value.AsText` | `DbValue.FromText(...)` |
| `DbType.Blob` | `value.AsBlob` | `DbValue.FromBlob(...)` |
| `DbType.Null` | `value.IsNull` | `DbValue.Null` |

Functions are registered with:

```csharp
using CSharpDB.Engine;
using CSharpDB.Primitives;

var options = new DatabaseOptions()
    .ConfigureFunctions(functions =>
    {
        functions.AddScalar(
            "Slugify",
            arity: 1,
            options: new DbScalarFunctionOptions(
                ReturnType: DbType.Text,
                IsDeterministic: true,
                NullPropagating: true),
            invoke: static (_, args) =>
                DbValue.FromText(args[0].AsText.ToLowerInvariant().Replace(' ', '-')));
    });
```

Open the database with those options:

```csharp
await using var db = await Database.OpenAsync("app.db", options);
```

For tests or transient data:

```csharp
await using var db = await Database.OpenInMemoryAsync(options);
```

---

## Complete Example

```csharp
using CSharpDB.Engine;
using CSharpDB.Primitives;

static string Slugify(string text)
{
    return text.Trim().ToLowerInvariant().Replace(' ', '-');
}

var options = new DatabaseOptions()
    .ConfigureFunctions(functions =>
    {
        functions.AddScalar(
            "Slugify",
            arity: 1,
            options: new DbScalarFunctionOptions(
                ReturnType: DbType.Text,
                IsDeterministic: true,
                NullPropagating: true),
            invoke: static (_, args) => DbValue.FromText(Slugify(args[0].AsText)));

        functions.AddScalar(
            "IsEven",
            arity: 1,
            options: new DbScalarFunctionOptions(
                ReturnType: DbType.Integer,
                IsDeterministic: true,
                NullPropagating: true),
            invoke: static (_, args) =>
                DbValue.FromInteger(args[0].AsInteger % 2 == 0 ? 1 : 0));
    });

await using var db = await Database.OpenAsync("app.db", options);

await db.ExecuteAsync("""
    CREATE TABLE articles (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        slug TEXT
    );
    """);

await db.ExecuteAsync("INSERT INTO articles VALUES (1, 'Hello World', Slugify('Hello World'))");
await db.ExecuteAsync("INSERT INTO articles VALUES (2, 'Second Post', Slugify('Second Post'))");

await using var result = await db.ExecuteAsync("""
    SELECT id, Slugify(title)
    FROM articles
    WHERE IsEven(id) = 1
    ORDER BY Slugify(title);
    """);
```

### VS Code Host Project Workflow

For a runnable C# host project, open
`samples/trusted-csharp-host` in VS Code. The sample includes `.vscode` launch
and task files so a developer can press `F5`, set breakpoints inside the
registered callbacks, and watch SQL and Admin Forms automation invoke ordinary
C# code.

```powershell
dotnet run --project samples\trusted-csharp-host\TrustedCSharpHostSample.csproj
```

The sample demonstrates:

- `DatabaseOptions.ConfigureFunctions(...)` for a trusted scalar function.
- SQL calling the function by name.
- `DbCommandRegistry` for a trusted host command.
- An Admin Forms `DbActionSequence` that sets a field and then runs the
  command.

The VS Code story stays host-owned: VS Code is the editor/debugger for the C#
host project, while database metadata stores names and declarative action data
only.

---

## Registration Rules

Function names are SQL identifiers:

- They must start with a letter or `_`.
- Remaining characters must be letters, digits, or `_`.
- Lookup is case-insensitive, so `Slugify`, `slugify`, and `SLUGIFY` refer to the same function.
- A user function name can only be registered once. V1 does not support overloads by arity.
- Reserved built-ins cannot be overridden. Current reserved names are `TEXT`, `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- `arity` must match the number of arguments used by the expression.

Registration failures throw immediately so host applications fail at startup instead of later during a query.

`ConfigureFunctions` sets the function registry for the returned `DatabaseOptions`. If you chain multiple option helpers, keep all function registrations in one `ConfigureFunctions` call or assign a single `DbFunctionRegistry` to `DatabaseOptions.Functions`.

---

## Function Options

Each function can include `DbScalarFunctionOptions`:

```csharp
new DbScalarFunctionOptions(
    ReturnType: DbType.Text,
    IsDeterministic: true,
    NullPropagating: true)
```

| Option | Meaning |
| --- | --- |
| `ReturnType` | Optional metadata describing the expected return type. |
| `IsDeterministic` | Marks the function as returning the same output for the same inputs. V1 exposes the metadata but does not use it for constant folding or index planning. |
| `NullPropagating` | If any argument is `NULL`, CSharpDB returns `NULL` without invoking the delegate. |

Without `NullPropagating`, `DbValue.Null` is passed to the delegate and the function decides what to do.

```csharp
functions.AddScalar(
    "CoalesceText",
    arity: 2,
    options: new DbScalarFunctionOptions(DbType.Text),
    invoke: static (_, args) =>
        args[0].IsNull ? args[1] : args[0]);
```

---

## SQL Usage

Registered scalar functions can be used in non-aggregate SQL expression positions:

```sql
SELECT Slugify(title) FROM articles;
SELECT * FROM articles WHERE IsEven(id) = 1;
SELECT * FROM articles ORDER BY Slugify(title);
INSERT INTO articles VALUES (3, 'New Title', Slugify('New Title'));
UPDATE articles SET slug = Slugify(title) WHERE slug IS NULL;
```

They also work in trigger bodies and SQL procedure bodies because those paths execute through the same SQL expression evaluator:

```sql
CREATE TABLE article_audit (article_id INTEGER, slug TEXT);

CREATE TRIGGER articles_ai AFTER INSERT ON articles
BEGIN
    INSERT INTO article_audit VALUES (NEW.id, Slugify(NEW.title));
END;
```

Custom functions stay on the residual expression path in V1:

- No index pushdown is inferred from a custom function.
- No generated-column or expression-index behavior is added.
- No constant folding or cost assumptions are made from custom function metadata.

That keeps existing query and storage paths unchanged unless a query actually calls a registered function.

---

## Direct Client Usage

Direct clients pass functions through `DirectDatabaseOptions`:

```csharp
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Primitives;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    DataSource = "app.db",
    DirectDatabaseOptions = new DatabaseOptions()
        .ConfigureFunctions(functions =>
        {
            functions.AddScalar(
                "AddOne",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromInteger(args[0].AsInteger + 1));
        }),
});

await client.ExecuteSqlAsync("CREATE TABLE numbers (value INTEGER);");
await client.ExecuteSqlAsync("INSERT INTO numbers VALUES (41);");

var result = await client.ExecuteSqlAsync("SELECT AddOne(value) FROM numbers;");
```

`DirectDatabaseOptions` is only valid for direct transport. It is rejected for HTTP and gRPC clients because delegates cannot be serialized to another process.

---

## Remote Host Usage

HTTP and gRPC clients cannot send C# delegates. Remote SQL can call a custom function only when that function is registered inside the host process that owns the database.

The practical rule is:

- Embedded or direct client: register functions in `DatabaseOptions` or `DirectDatabaseOptions`.
- Remote client: register functions where the daemon, API host, or application server opens the database.
- Pipeline packages, report definitions, form definitions, procedures, and SQL text store function names and expressions only. They do not store C# function bodies.
- Admin Forms, Admin Reports, and pipeline packages also store generated `automation` metadata that lists required trusted commands and scalar functions by name, surface, and location. This is an import/export contract for hosts; it is not executable code.

---

## Admin Forms

Admin Forms computed formulas can call registered scalar functions when the formula evaluator receives a `DbFunctionRegistry`.

```csharp
using CSharpDB.Admin.Forms.Evaluation;
using CSharpDB.Primitives;

var functions = DbFunctionRegistry.Create(builder =>
{
    builder.AddScalar(
        "Tax",
        1,
        new DbScalarFunctionOptions(DbType.Real, IsDeterministic: true, NullPropagating: true),
        static (_, args) => DbValue.FromReal(args[0].AsReal * 0.0825));
});

double? tax = FormulaEvaluator.Evaluate(
    "=Tax(Subtotal)",
    fieldResolver: name => name == "Subtotal" ? 100.00 : null,
    functions: functions);
```

Forms formulas are numeric formulas. A custom function used from `FormulaEvaluator.Evaluate` should return `INTEGER` or `REAL`; other return types evaluate to `null` in that surface. Existing aggregate formulas such as `=SUM(OrderItems.LineTotal)` remain built-in form behavior and are not replaced by custom scalar functions.

Admin Forms can also bind lifecycle events to trusted commands. Form definitions store event names and command names only; the C# command bodies stay registered in the host process.

```csharp
var form = existingForm with
{
    EventBindings =
    [
        new FormEventBinding(FormEventKind.OnOpen, "AuditFormOpen"),
        new FormEventBinding(FormEventKind.BeforeInsert, "ValidateCustomerCreate"),
        new FormEventBinding(FormEventKind.AfterUpdate, "AuditCustomerChange"),
    ],
};
```

Supported form-level events in this slice are:

| Event | When it runs |
| --- | --- |
| `OnOpen` | After the form definition and source table are resolved, before records load. |
| `OnLoad` | After the initial record page loads. |
| `BeforeInsert` | Before a new record is inserted. Returning `DbCommandResult.Failure(...)` cancels the insert. |
| `AfterInsert` | After a new record is inserted. |
| `BeforeUpdate` | Before an existing record is updated. Returning failure cancels the update. |
| `AfterUpdate` | After an existing record is updated. |
| `BeforeDelete` | Before the current record is deleted. Returning failure cancels the delete. |
| `AfterDelete` | After the current record is deleted. |

Command context arguments include the current record fields converted to `DbValue`. Static arguments configured on the event binding override same-named record fields. Metadata includes `surface`, `formId`, `formName`, `tableName`, and `event`.

The Admin Forms designer preserves form event bindings and exposes them in the property inspector when no control is selected. If the host has registered trusted commands, the designer shows those command names; otherwise it stores the command name typed by the designer. The same editor can attach a visual action sequence to the event.

Admin Forms also include control-level trusted command events. Form controls store event names, command names, and optional JSON arguments in the form definition. At runtime, the renderer invokes the registered host command with the current record fields plus event-specific arguments.

```csharp
var textBox = existingTextBox with
{
    EventBindings =
    [
        new ControlEventBinding(
            ControlEventKind.OnChange,
            "NormalizeCustomerName",
            new Dictionary<string, object?> { ["source"] = "name-textbox" }),
        new ControlEventBinding(ControlEventKind.OnLostFocus, "ValidateCustomerName"),
    ],
};
```

Supported control events in this slice are:

| Event | When it runs |
| --- | --- |
| `OnClick` | When a label or command button is clicked. |
| `OnChange` | After an input, checkbox, radio, select, lookup, or textarea updates its bound field. |
| `OnGotFocus` | When an interactive control receives focus. |
| `OnLostFocus` | When an interactive control loses focus. |

Control event metadata includes the Forms metadata plus `event`, `controlId`, `controlType`, and `fieldName` for bound controls. Arguments include current record fields and event details such as `fieldName`, `value`, and `oldValue` for field changes. Static arguments configured on the event binding override same-named runtime arguments.

The Admin Forms designer exposes selected-control event bindings in the property inspector. If the host has registered trusted commands, the designer shows those command names; otherwise it stores the command name typed by the designer. Selected-control events use the same visual action-sequence editor as form lifecycle events.

Admin Forms also include a command button control. Command buttons store a display label, a command name, and optional JSON arguments in the form definition. At runtime, clicking the button invokes the registered host command with the current record fields plus the configured arguments. Command buttons can also use `ControlEventKind.OnClick` bindings, which allows a button to be driven entirely by the shared control-event model.

```csharp
var button = new ControlDefinition(
    "btn-ship",
    "commandButton",
    new Rect(24, 320, 160, 34),
    Binding: null,
    Props: new PropertyBag(new Dictionary<string, object?>
    {
        ["text"] = "Ship Order",
        ["commandName"] = "ShipOrder",
        ["commandArguments"] = new Dictionary<string, object?>
        {
            ["source"] = "form-button",
        },
    }),
    ValidationOverride: null);
```

Command button direct-command metadata includes the same form metadata as lifecycle events, plus `event = "Click"`, `controlId`, and `controlType`.

---

## Declarative Admin Forms Action Sequences

Admin Forms event bindings can also store small declarative action sequences.
This is the first Access-style macro layer for CSharpDB Forms: the form stores
action metadata, while any executable C# still lives in host-registered trusted
commands.

```csharp
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

var shipButton = existingButton with
{
    EventBindings =
    [
        new ControlEventBinding(
            ControlEventKind.OnClick,
            CommandName: string.Empty,
            ActionSequence: new DbActionSequence(
            [
                new DbActionStep(
                    DbActionKind.SetFieldValue,
                    Target: "Status",
                    Value: "Shipped"),
                new DbActionStep(
                    DbActionKind.RunCommand,
                    CommandName: "AuditOrderStatus",
                    Arguments: new Dictionary<string, object?>
                    {
                        ["source"] = "ship-button",
                    }),
                new DbActionStep(
                    DbActionKind.ShowMessage,
                    Message: "Order marked as shipped."),
            ],
            Name: "ShipButtonActions")),
    ],
};
```

The initial action set is intentionally small:

| Action | Behavior |
| --- | --- |
| `RunCommand` | Invokes a host-registered trusted command by name. |
| `SetFieldValue` | Updates a target field in the current mutable form record. |
| `ShowMessage` | Sends a message when the current Forms surface provides a command/message callback. |
| `Stop` | Ends the current sequence successfully. |

Action sequences can be attached to form lifecycle bindings or selected-control
bindings. A binding can contain only a command, only an action sequence, or a
command followed by an action sequence:

```csharp
var form = existingForm with
{
    EventBindings =
    [
        new FormEventBinding(
            FormEventKind.BeforeInsert,
            "ValidateOrder",
            ActionSequence: new DbActionSequence(
            [
                new DbActionStep(
                    DbActionKind.SetFieldValue,
                    Target: "Status",
                    Value: "Draft"),
            ])),
    ],
};
```

The Admin Forms property inspector exposes action sequences with a visual
editor on form-level and selected-control event bindings. Designers can add a
sequence, name it, add `RunCommand`, `SetFieldValue`, `ShowMessage`, and `Stop`
steps, reorder or remove steps, choose registered commands when available, and
toggle per-step `StopOnFailure`. JSON editing remains only for optional binding
or `RunCommand` step argument payloads.

For `RunCommand`, command arguments are built from current record fields,
binding arguments, runtime event arguments, and step arguments, with later
sources overriding earlier ones. Command metadata includes the Forms metadata
plus `actionKind`, `actionStep`, and optional `actionSequence`.

When forms are saved through `DbFormRepository` or exported through
`FormAutomationMetadata.NormalizeForExport(...)`, the definition's `automation`
metadata is regenerated from form events, command buttons, selected-control
events, action-sequence `RunCommand` steps, and computed-control formulas.
Older form JSON without automation metadata is backfilled when it is loaded.

`SetFieldValue` can update mutable records in form lifecycle events such as
`BeforeInsert` and `BeforeUpdate`, and it can update the current rendered record
from control events or command-button clicks. It does not add built-in database
operations by itself; use `RunCommand` for host-owned work.

V1 action sequences do not include conditions, loops, stored C# source,
database-owned plugins, built-in navigation actions, built-in save/delete
actions, direct SQL/procedure execution actions, or remote delegate
serialization.

---

## Admin Reports

Admin Reports preview rendering accepts the same registry through `DefaultReportPreviewService`:

```csharp
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Primitives;

var previewService = new DefaultReportPreviewService(
    dbClient,
    sourceProvider,
    functions);
```

Numeric calculated expressions can call numeric-returning functions:

```text
=Tax([Subtotal])
```

Calculated text can use a scalar function as the whole expression, including text-returning functions:

```text
=FormatInvoiceLabel([InvoiceNumber], [CustomerName])
```

Report aggregate formulas such as `=SUM([Subtotal])` remain built-in report behavior.

Admin Reports can also bind preview-render lifecycle events to trusted commands. Report definitions store event names, command names, and optional static arguments only; the C# command bodies stay registered by the host process.

```csharp
using CSharpDB.Admin.Reports.Models;

var report = existingReport with
{
    EventBindings =
    [
        new ReportEventBinding(ReportEventKind.OnOpen, "AuditReportOpen"),
        new ReportEventBinding(ReportEventKind.BeforeRender, "PrepareReportContext"),
        new ReportEventBinding(ReportEventKind.AfterRender, "PublishReportRendered"),
    ],
};
```

Supported report events are:

| Event | When it runs |
| --- | --- |
| `OnOpen` | After the report source is resolved, before preview rows are loaded. |
| `BeforeRender` | After preview rows are loaded and capped, before pagination and calculated text rendering. |
| `AfterRender` | After preview pages are produced, before the preview result is returned. |

Command context arguments include render metrics such as `rowCount`, `loadedRowCount`, `rowTruncated`, `pageCount`, `isTruncated`, and `hasSchemaDrift` depending on the event. Static arguments configured on the binding override same-named runtime arguments. Metadata includes `surface = AdminReports`, `reportId`, `reportName`, `sourceKind`, `sourceName`, and `event`.

When reports are saved through `DbReportRepository` or exported through
`ReportAutomationMetadata.NormalizeForExport(...)`, the definition's
`automation` metadata is regenerated from report event bindings and calculated
text expressions. Older report JSON without automation metadata is backfilled
when it is loaded.

Register report commands through the reports service registration overload:

```csharp
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Primitives;

builder.Services.AddCSharpDbAdminReports(commands =>
{
    commands.AddCommand("PublishReportRendered", static context =>
    {
        string reportName = context.Metadata["reportName"];
        long pageCount = context.Arguments["pageCount"].AsInteger;

        PublishReportMetric(reportName, pageCount);
        return DbCommandResult.Success();
    });
});
```

---

## Pipelines

Pipelines can call registered scalar functions in filter expressions and derived-column expressions when the runner or component factory is constructed with a registry.

```csharp
using CSharpDB.Client.Pipelines;
using CSharpDB.Pipelines.Models;
using CSharpDB.Primitives;

var functions = DbFunctionRegistry.Create(builder =>
{
    builder.AddScalar(
        "NormalizeStatus",
        1,
        new DbScalarFunctionOptions(DbType.Text, IsDeterministic: true, NullPropagating: true),
        static (_, args) => DbValue.FromText(args[0].AsText.Trim().ToLowerInvariant()));
});

var runner = new CSharpDbPipelineRunner(client, functions);

var package = new PipelinePackageDefinition
{
    Name = "active-customers",
    Version = "1.0.0",
    Source = new PipelineSourceDefinition
    {
        Kind = PipelineSourceKind.CsvFile,
        Path = "customers.csv",
        HasHeaderRow = true,
    },
    Transforms =
    [
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Filter,
            FilterExpression = "NormalizeStatus(status) == 'active'",
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Derive,
            DerivedColumns =
            [
                new PipelineDerivedColumn
                {
                    Name = "status_key",
                    Expression = "NormalizeStatus(status)",
                },
            ],
        },
    ],
    Destination = new PipelineDestinationDefinition
    {
        Kind = PipelineDestinationKind.JsonFile,
        Path = "active-customers.json",
    },
};

await runner.RunPackageAsync(package);
```

Pipeline package JSON stores expressions such as `NormalizeStatus(status)` plus generated `automation` metadata listing the required scalar function names. The C# delegate must be registered by the process that runs the package.

Pipelines can also invoke trusted commands from run hooks. Hook definitions are serialized with the package, but they store only the hook event, command name, optional static arguments, and generated automation metadata:

```csharp
var commands = DbCommandRegistry.Create(builder =>
{
    builder.AddCommand("NotifyPipeline", static context =>
    {
        string pipelineName = context.Metadata["pipelineName"];
        string status = context.Arguments["status"].AsText;
        long rowsWritten = context.Arguments["rowsWritten"].AsInteger;

        NotifyOps(pipelineName, status, rowsWritten);
        return DbCommandResult.Success();
    });
});

var runner = new CSharpDbPipelineRunner(client, functions, commands);

var package = new PipelinePackageDefinition
{
    Name = "active-customers",
    Version = "1.0.0",
    Source = new PipelineSourceDefinition
    {
        Kind = PipelineSourceKind.CsvFile,
        Path = "customers.csv",
    },
    Destination = new PipelineDestinationDefinition
    {
        Kind = PipelineDestinationKind.JsonFile,
        Path = "active-customers.json",
    },
    Hooks =
    [
        new PipelineCommandHookDefinition
        {
            Event = PipelineCommandHookEvent.OnRunSucceeded,
            CommandName = "NotifyPipeline",
            Arguments = new Dictionary<string, object?>
            {
                ["channel"] = "ops",
            },
        },
    ],
};
```

Supported pipeline hook events are:

| Event | When it runs |
| --- | --- |
| `OnRunStarted` | After package validation and run logging, before components are created. |
| `OnBatchCompleted` | After each source batch is transformed/written, metrics and checkpoints are updated, and reject limits pass. |
| `OnRunSucceeded` | After destination completion and before the successful run is logged as completed. |
| `OnRunFailed` | When the orchestrator is about to return a failed `PipelineRunResult`. |

Hook arguments include `runId`, `pipelineName`, `pipelineVersion`, `mode`, `event`, `status`, `rowsRead`, `rowsWritten`, `rowsRejected`, and `batchesCompleted`. Batch hooks also include `batchNumber`, `startingRowNumber`, and `batchRowCount`. Failure hooks include `errorSummary`. Metadata includes `surface = Pipelines`, `pipelineName`, `pipelineVersion`, `runId`, `mode`, and `event`.

`PipelinePackageSerializer` refreshes the `automation` manifest when packages are serialized, saved, deserialized, or loaded from disk. `PipelinePackageValidator` accepts older packages without a manifest, but if a manifest is present and no longer matches the package expressions/hooks, validation reports stale automation metadata so the package can be re-exported.

`Validate` mode does not invoke command hooks, so package validation stays side-effect free. Missing command registration or a failing hook with `StopOnFailure = true` fails the run normally. For `OnRunFailed`, hook failures are appended to the failed run's error summary instead of recursively dispatching more failure hooks.

---

## Error Handling

Missing SQL functions fail with the existing unknown scalar function error. Function exceptions are wrapped with the function name and the surrounding statement follows normal rollback behavior.

```csharp
functions.AddScalar(
    "RequirePositive",
    1,
    new DbScalarFunctionOptions(DbType.Integer, NullPropagating: true),
    static (context, args) =>
    {
        long value = args[0].AsInteger;
        if (value <= 0)
            throw new ArgumentOutOfRangeException(context.FunctionName, "Value must be positive.");

        return DbValue.FromInteger(value);
    });
```

For SQL write statements, a failing function aborts the statement. If the statement is inside a transaction, normal transaction rollback rules apply.

Admin Forms formulas intentionally return `null` for invalid formulas, unsupported function return types, missing functions, division by zero, or exceptions. Pipeline functions throw runtime errors unless the pipeline error mode handles the affected row.

Trusted command failures are surface-specific. Form before-events can cancel writes, report event failures fail preview rendering, and pipeline hook failures produce a failed `PipelineRunResult` unless the binding sets `StopOnFailure = false`.

Forms action-sequence failures follow the same binding-level `StopOnFailure`
rule. Step-level `StopOnFailure = false` lets a later step continue after that
step fails; otherwise the sequence reports the failure to the surrounding form
or control event.

---

## Performance Guidance

Custom functions run only when an expression calls them. Queries and writes that do not use custom functions stay on the existing paths.

For low overhead:

- Prefer `NullPropagating = true` when a function naturally returns null for null input.
- Avoid database calls, blocking I/O, sleeps, and long network calls inside delegates.
- Keep delegates thread-safe. A function may be called by concurrent queries in the same host process.
- Capture immutable services or thread-safe services in closures when application integration is needed.
- Use `IsDeterministic = true` for accurate metadata, but do not rely on V1 to optimize from it.

---

## Current Limitations

V1 does not support:

- Aggregate UDFs.
- Table-valued UDFs.
- Stored C# source code or database-owned compiled modules.
- Sandboxed execution.
- Async scalar delegates.
- Passing a database handle into the function context.
- Sending delegates over HTTP, gRPC, or pipeline package files.
- Optimizer pushdown, expression indexes, generated columns, or constant folding based on custom function metadata.
- Additional Access-style control events such as double-click, key, mouse, timer, and dirty/current events.
- Richer macro/action scripts with conditions, loops, built-in navigation, built-in save/delete, direct SQL/procedure actions, or database-owned executable code.
