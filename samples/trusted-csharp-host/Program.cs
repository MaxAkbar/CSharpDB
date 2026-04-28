using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Engine;
using CSharpDB.Primitives;

Console.WriteLine("CSharpDB trusted C# host sample");
Console.WriteLine();

DatabaseOptions databaseOptions = new DatabaseOptions()
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
                DbValue.FromText(Slugify(args[0].AsText)));
    });

await using Database db = await Database.OpenInMemoryAsync(databaseOptions);

await db.ExecuteAsync("""
    CREATE TABLE articles (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        slug TEXT
    );
    """);

await db.ExecuteAsync("INSERT INTO articles VALUES (1, 'Hello From VS Code', Slugify('Hello From VS Code'));");
await db.ExecuteAsync("INSERT INTO articles VALUES (2, 'Trusted CSharpDB Callbacks', Slugify('Trusted CSharpDB Callbacks'));");

Console.WriteLine("SQL scalar function result:");
await using var rows = await db.ExecuteAsync("""
    SELECT id, title, slug
    FROM articles
    ORDER BY id;
    """);

while (await rows.MoveNextAsync())
{
    IReadOnlyList<DbValue> row = rows.Current;
    Console.WriteLine($"  {row[0].AsInteger}: {row[1].AsText} -> {row[2].AsText}");
}

List<string> auditLog = [];
DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
{
    builder.AddCommand(
        "AuditCustomerChange",
        new DbCommandOptions("Records a customer workflow event."),
        context =>
        {
            long customerId = context.Arguments["Id"].AsInteger;
            string status = context.Arguments["Status"].AsText;
            string source = context.Arguments["source"].AsText;
            string eventName = context.Metadata["event"];
            string actionSequence = context.Metadata.TryGetValue("actionSequence", out string? value)
                ? value
                : "(none)";

            auditLog.Add(
                $"Customer {customerId} -> {status}; source={source}; event={eventName}; sequence={actionSequence}");
            return DbCommandResult.Success();
        });
});

FormDefinition form = new(
    "customers-entry",
    "Customers Entry",
    "Customers",
    DefinitionVersion: 1,
    SourceSchemaSignature: "sample:customers:v1",
    Layout: new LayoutDefinition("absolute", 8, SnapToGrid: true, Breakpoints: []),
    Controls: [],
    EventBindings:
    [
        new FormEventBinding(
            FormEventKind.BeforeInsert,
            CommandName: string.Empty,
            ActionSequence: new DbActionSequence(
            [
                new DbActionStep(
                    DbActionKind.SetFieldValue,
                    Target: "Status",
                    Value: "Ready"),
                new DbActionStep(
                    DbActionKind.RunCommand,
                    CommandName: "AuditCustomerChange",
                    Arguments: new Dictionary<string, object?>
                    {
                        ["source"] = "trusted-csharp-host-sample",
                    }),
            ],
            Name: "PrepareCustomerInsert")),
    ]);

var record = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
{
    ["Id"] = 42L,
    ["Name"] = "Ada Lovelace",
};

var dispatcher = new DefaultFormEventDispatcher(commands);
FormEventDispatchResult dispatchResult = await dispatcher.DispatchAsync(form, FormEventKind.BeforeInsert, record);

Console.WriteLine();
Console.WriteLine("Admin Forms action sequence result:");
Console.WriteLine($"  Succeeded: {dispatchResult.Succeeded}");
Console.WriteLine($"  Status field: {record["Status"]}");
foreach (string auditEntry in auditLog)
    Console.WriteLine($"  Audit: {auditEntry}");

Console.WriteLine();
Console.WriteLine("Set a breakpoint inside Slugify or AuditCustomerChange, then run this sample from VS Code.");

static string Slugify(string text)
{
    return text
        .Trim()
        .ToLowerInvariant()
        .Replace(' ', '-');
}
