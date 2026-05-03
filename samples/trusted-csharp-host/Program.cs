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

DbValidationRuleRegistry validationRules = DbValidationRuleRegistry.Create(builder =>
{
    builder.AddRule(
        "CustomerNamePolicy",
        new DbValidationRuleOptions(
            Description: "Rejects placeholder customer names.",
            Timeout: TimeSpan.FromSeconds(2)),
        static (context, _) =>
        {
            string text = context.Value.IsNull ? string.Empty : context.Value.AsText;
            string blockedText = context.Parameters.TryGetValue("blockedText", out DbValue configured)
                && !configured.IsNull
                    ? configured.AsText
                    : "test";

            DbValidationRuleResult result = text.Contains(blockedText, StringComparison.OrdinalIgnoreCase)
                ? DbValidationRuleResult.Failure(
                    context.FallbackMessage ?? "Customer name is not allowed.",
                    context.FieldName,
                    context.RuleName)
                : DbValidationRuleResult.Success();
            return ValueTask.FromResult(result);
        });

    builder.AddRule(
        "CustomerReadyForInsert",
        new DbValidationRuleOptions(
            Description: "Requires the customer workflow status before save."),
        static context =>
        {
            string requiredStatus = context.Parameters.TryGetValue("requiredStatus", out DbValue configured)
                && !configured.IsNull
                    ? configured.AsText
                    : "Ready";
            string status = context.Record.TryGetValue("Status", out DbValue value) && !value.IsNull
                ? value.AsText
                : string.Empty;

            return string.Equals(status, requiredStatus, StringComparison.OrdinalIgnoreCase)
                ? DbValidationRuleResult.Success()
                : DbValidationRuleResult.Failure(
                    [
                        new DbValidationFailure(
                            "Status",
                            context.FallbackMessage ?? $"Status must be {requiredStatus}.",
                            context.RuleName),
                    ],
                    "Customer record is not ready.");
        });
});

FormDefinition form = FormAutomationMetadata.NormalizeForExport(CreateCustomerEntryForm());
DbAutomationMetadata automation = form.Automation
    ?? throw new InvalidOperationException("The sample form should export automation metadata.");

PrintAutomationMetadata(automation);
ValidateAutomationMetadata(automation, functions, commands, validationRules);
PrintGeneratedStub(automation);

await RunSqlScalarFunctionDemoAsync(functions);
await RunAdminFormsAutomationDemoAsync(form, commands, auditLog);
await RunAdminFormsValidationDemoAsync(form, validationRules);

Console.WriteLine();
Console.WriteLine("Set breakpoints inside Slugify, AuditCustomerChange, or a validation rule, then run this sample from VS Code.");

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

static async Task RunAdminFormsValidationDemoAsync(
    FormDefinition form,
    DbValidationRuleRegistry validationRules)
{
    var record = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
    {
        ["Id"] = 43L,
        ["Name"] = "Test Account",
        ["Status"] = "Draft",
    };

    var validation = new DefaultValidationInferenceService(validationRules, DbExtensionPolicies.DefaultHostCallbackPolicy);
    IReadOnlyList<ValidationError> errors = await validation.EvaluateAsync(form, record);

    Console.WriteLine();
    Console.WriteLine("Admin Forms validation result:");
    foreach (ValidationError error in errors)
        Console.WriteLine($"  {error.FieldName}: {error.RuleId} - {error.Message}");
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

    foreach (DbAutomationValidationRuleReference rule in automation.ValidationRules ?? [])
        Console.WriteLine($"  validation {rule.Name} from {rule.Surface}:{rule.Location}");
}

static void ValidateAutomationMetadata(
    DbAutomationMetadata automation,
    DbFunctionRegistry functions,
    DbCommandRegistry commands,
    DbValidationRuleRegistry validationRules)
{
    AutomationValidationResult result = AutomationManifestValidator.Validate(
        automation,
        functions,
        commands,
        validationRules,
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
            new ControlDefinition(
                "customer-name",
                "text",
                new Rect(24, 72, 240, 32),
                Binding: new BindingDefinition("Name", "TwoWay"),
                Props: new PropertyBag(new Dictionary<string, object?>
                {
                    ["label"] = "Name",
                }),
                ValidationOverride: new ValidationOverride(
                    DisableInferredRules: false,
                    AddRules:
                    [
                        new ValidationRule(
                            "CustomerNamePolicy",
                            "Use the real customer name, not a placeholder.",
                            new Dictionary<string, object?>
                            {
                                ["blockedText"] = "test",
                            }),
                    ],
                    DisableRuleIds: [])),
            new ControlDefinition(
                "customer-status",
                "text",
                new Rect(24, 120, 160, 32),
                Binding: new BindingDefinition("Status", "TwoWay"),
                Props: new PropertyBag(new Dictionary<string, object?>
                {
                    ["label"] = "Status",
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
        ],
        ValidationRules:
        [
            new ValidationRule(
                "CustomerReadyForInsert",
                "Customer status must be Ready before save.",
                new Dictionary<string, object?>
                {
                    ["requiredStatus"] = "Ready",
                }),
        ]);

static string Slugify(string text)
{
    return text
        .Trim()
        .ToLowerInvariant()
        .Replace(' ', '-');
}
