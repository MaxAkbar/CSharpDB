using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Engine;
using CSharpDB.Primitives;

Console.WriteLine("CSharpDB trusted C# host sample");
Console.WriteLine();

DbFunctionRegistry functions = DbFunctionRegistry.Create(builder =>
{
    builder.AddScalar(
        "Slugify",
        arity: 1,
        options: new DbScalarFunctionOptions(
            ReturnType: DbType.Text,
            IsDeterministic: true,
            NullPropagating: true),
        invoke: static (_, args) =>
            DbValue.FromText(Slugify(args[0].AsText)));
});

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

FormDefinition form = FormAutomationMetadata.NormalizeForExport(CreateCustomerEntryForm());
DbAutomationMetadata automation = form.Automation
    ?? throw new InvalidOperationException("The sample form should export automation metadata.");

PrintAutomationMetadata(automation);
ValidateAutomationMetadata(automation, functions, commands);
PrintGeneratedStub(automation);

await RunSqlScalarFunctionDemoAsync(functions);
await RunAdminFormsAutomationDemoAsync(form, commands, auditLog);

Console.WriteLine();
Console.WriteLine("Set breakpoints inside Slugify or AuditCustomerChange, then run this sample from VS Code.");

static async Task RunSqlScalarFunctionDemoAsync(DbFunctionRegistry functions)
{
    await using Database db = await Database.OpenInMemoryAsync(new DatabaseOptions
    {
        Functions = functions,
    });

    await db.ExecuteAsync("""
        CREATE TABLE articles (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            slug TEXT
        );
        """);

    await db.ExecuteAsync("INSERT INTO articles VALUES (1, 'Hello From VS Code', Slugify('Hello From VS Code'));");
    await db.ExecuteAsync("INSERT INTO articles VALUES (2, 'Trusted CSharpDB Callbacks', Slugify('Trusted CSharpDB Callbacks'));");

    Console.WriteLine();
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
}

static async Task RunAdminFormsAutomationDemoAsync(
    FormDefinition form,
    DbCommandRegistry commands,
    IReadOnlyList<string> auditLog)
{
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
}

static void PrintAutomationMetadata(DbAutomationMetadata automation)
{
    Console.WriteLine("Exported automation metadata:");
    foreach (DbAutomationScalarFunctionReference function in automation.ScalarFunctions ?? [])
    {
        Console.WriteLine(
            $"  scalar {function.Name}/{function.Arity} from {function.Surface}:{function.Location}");
    }

    foreach (DbAutomationCommandReference command in automation.Commands ?? [])
        Console.WriteLine($"  command {command.Name} from {command.Surface}:{command.Location}");
}

static void ValidateAutomationMetadata(
    DbAutomationMetadata automation,
    DbFunctionRegistry functions,
    DbCommandRegistry commands)
{
    AutomationValidationResult result = AutomationManifestValidator.Validate(
        automation,
        functions,
        commands,
        new AutomationManifestValidationOptions(RequireMetadata: true));

    Console.WriteLine();
    Console.WriteLine("Automation validation:");
    if (result.Succeeded)
    {
        Console.WriteLine("  All referenced callbacks are registered.");
        return;
    }

    foreach (AutomationValidationIssue issue in result.Issues)
        Console.WriteLine($"  {issue.Severity}: {issue.Message}");
}

static void PrintGeneratedStub(DbAutomationMetadata automation)
{
    string source = AutomationStubGenerator.GenerateCSharp(
        automation,
        new AutomationStubGenerationOptions(
            Namespace: "MyApp.CSharpDbAutomation",
            ClassName: "CSharpDbAutomationRegistration"));

    Console.WriteLine();
    Console.WriteLine("Starter C# registration stub:");
    Console.WriteLine(source);
}

static FormDefinition CreateCustomerEntryForm()
    => new(
        "customers-entry",
        "Customers Entry",
        "Customers",
        DefinitionVersion: 1,
        SourceSchemaSignature: "sample:customers:v1",
        Layout: new LayoutDefinition("absolute", 8, SnapToGrid: true, Breakpoints: []),
        Controls:
        [
            new ControlDefinition(
                "slug-preview",
                "computed",
                new Rect(24, 24, 240, 32),
                Binding: null,
                Props: new PropertyBag(new Dictionary<string, object?>
                {
                    ["formula"] = "=Slugify(Name)",
                }),
                ValidationOverride: null),
        ],
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
                        DbActionKind.RunActionSequence,
                        SequenceName: "ReusableCustomerAudit",
                        Arguments: new Dictionary<string, object?>
                        {
                            ["source"] = "trusted-csharp-host-sample",
                        }),
                ],
                Name: "PrepareCustomerInsert")),
        ],
        ActionSequences:
        [
            new DbActionSequence(
            [
                new DbActionStep(
                    DbActionKind.RunCommand,
                    CommandName: "AuditCustomerChange"),
            ],
            Name: "ReusableCustomerAudit"),
        ]);

static string Slugify(string text)
{
    return text
        .Trim()
        .ToLowerInvariant()
        .Replace(' ', '-');
}
