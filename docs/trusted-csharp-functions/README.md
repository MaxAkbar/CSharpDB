# Trusted C# Scalar Functions

CSharpDB can call host-registered C# scalar functions from SQL and the embedded expression surfaces that sit on top of the engine. This is the CSharpDB equivalent of an Access-style application function integration: the application owns the C# code, registers it while opening or hosting the database, and users call the function by name in database expressions.

This feature is intentionally trusted and in-process. It does not store C# source code in the database, sandbox user code, load plugin assemblies from database files, or serialize delegates over HTTP or gRPC.

---

## Trusted Commands

CSharpDB also supports trusted host-registered commands for application automation surfaces. Commands are different from scalar functions:

- Scalar functions return a `DbValue` and can be used inside SQL or formulas.
- Commands return a `DbCommandResult` and are invoked by host-driven events such as Admin Forms lifecycle events.

Commands are intended for Access-style application automation such as auditing, calling application services, sending notifications, refreshing derived state, or coordinating UI workflows. They are trusted in-process callbacks registered by the host application.

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

Pipeline package JSON stores only expressions such as `NormalizeStatus(status)`. The C# delegate must be registered by the process that runs the package.

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
- Control-level form events such as button `OnClick`.
- Stored macro/action scripts.
