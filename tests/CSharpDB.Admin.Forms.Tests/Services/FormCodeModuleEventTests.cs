using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Serialization;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.Forms.Tests;
using CSharpDB.CodeModules;
using CSharpDB.CodeModules.Trust;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class FormCodeModuleEventTests
{
    [Fact]
    public void FormAndControlEventBindings_WithCodeHandlers_RoundTripJson()
    {
        var form = CreateForm(
            [
                new FormEventBinding(
                    FormEventKind.OnLoad,
                    string.Empty,
                    CodeHandler: new CodeModuleHandler("form:customers", "TestModules.CustomersModule", "OnLoad")),
            ],
            [
                new ControlDefinition(
                    "nameBox",
                    "text",
                    new Rect(0, 0, 100, 24),
                    new BindingDefinition("Name", "TwoWay"),
                    new PropertyBag(new Dictionary<string, object?>()),
                    null,
                    EventBindings:
                    [
                        new ControlEventBinding(
                            ControlEventKind.OnChange,
                            string.Empty,
                            CodeHandler: new CodeModuleHandler("form:customers", "TestModules.CustomersModule", "nameBox_OnChange")),
                    ]),
            ]);

        string json = JsonSerializer.Serialize(form, JsonDefaults.Options);
        FormDefinition deserialized = JsonSerializer.Deserialize<FormDefinition>(json, JsonDefaults.Options)!;

        CodeModuleHandler formHandler = Assert.Single(deserialized.EventBindings!).CodeHandler!;
        Assert.Equal("form:customers", formHandler.ModuleId);
        Assert.Equal("OnLoad", formHandler.MethodName);
        CodeModuleHandler controlHandler = Assert.Single(deserialized.Controls[0].EventBindings!).CodeHandler!;
        Assert.Equal("nameBox_OnChange", controlHandler.MethodName);
    }

    [Fact]
    public async Task DispatchAsync_UntrustedModuleBlocksExecution()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var codeClient = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());
        await codeClient.UpsertAsync(CreateModule("""
            using CSharpDB.CodeModules.Runtime;
            namespace TestModules;
            public sealed class CustomersModule : FormCodeModule
            {
                public void OnLoad(FormEventContext context)
                {
                    Me.Name = "Bob";
                }
            }
            """), ct);
        var dispatcher = CreateDispatcher(codeClient);
        var record = new Dictionary<string, object?> { ["Name"] = "Alice" };

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            CreateForm([new FormEventBinding(FormEventKind.OnLoad, string.Empty, CodeHandler: Handler())]),
            FormEventKind.OnLoad,
            record,
            ct);

        Assert.False(result.Succeeded);
        Assert.Contains("not trusted", result.Message);
        Assert.Equal("Alice", record["Name"]);
    }

    [Fact]
    public async Task DispatchAsync_CompileErrorBlocksExecutionWithDiagnostic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var codeClient = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());
        await codeClient.UpsertAsync(CreateModule("public sealed class Broken {"), ct);
        var dispatcher = CreateDispatcher(codeClient);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            CreateForm([new FormEventBinding(FormEventKind.OnLoad, string.Empty, CodeHandler: Handler())]),
            FormEventKind.OnLoad,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            ct);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Message);
    }

    [Fact]
    public async Task DispatchAsync_TrustedHandlerMutatesMe()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var trust = new InMemoryCodeModuleTrustStore();
        var codeClient = new CSharpDbCodeModuleClient(db.Client, trust);
        await codeClient.UpsertAsync(CreateModule("""
            using CSharpDB.CodeModules.Runtime;
            namespace TestModules;
            public sealed class CustomersModule : FormCodeModule
            {
                public void OnLoad(FormEventContext context)
                {
                    Me.Name = "Bob";
                }
            }
            """), ct);
        await codeClient.TrustAsync(ct);
        var dispatcher = CreateDispatcher(codeClient);
        var record = new Dictionary<string, object?> { ["Name"] = "Alice" };

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            CreateForm([new FormEventBinding(FormEventKind.OnLoad, string.Empty, CodeHandler: Handler())]),
            FormEventKind.OnLoad,
            record,
            ct);

        Assert.True(result.Succeeded);
        Assert.Equal("Bob", record["Name"]);
    }

    [Fact]
    public async Task DispatchAsync_BeforeUpdateHandlerCanCancelSave()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var codeClient = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());
        await codeClient.UpsertAsync(CreateModule("""
            using CSharpDB.CodeModules.Runtime;
            namespace TestModules;
            public sealed class CustomersModule : FormCodeModule
            {
                public void BeforeUpdate(FormBeforeEventContext context)
                {
                    context.Cancel("Rejected by code.");
                }
            }
            """), ct);
        await codeClient.TrustAsync(ct);
        var dispatcher = CreateDispatcher(codeClient);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            CreateForm([new FormEventBinding(
                FormEventKind.BeforeUpdate,
                string.Empty,
                CodeHandler: Handler("BeforeUpdate"))]),
            FormEventKind.BeforeUpdate,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            ct);

        Assert.False(result.Succeeded);
        Assert.Equal("Rejected by code.", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_HandlerCanRunHostCommand()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var codeClient = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());
        await codeClient.UpsertAsync(CreateModule("""
            using System.Collections.Generic;
            using System.Threading.Tasks;
            using CSharpDB.CodeModules.Runtime;
            namespace TestModules;
            public sealed class CustomersModule : FormCodeModule
            {
                public async Task OnLoad(FormEventContext context)
                {
                    await DoCmd.RunHostCommandAsync("Audit", new Dictionary<string, object?> { ["source"] = "code" });
                }
            }
            """), ct);
        await codeClient.TrustAsync(ct);

        DbCommandContext? captured = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("Audit", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });
        var dispatcher = CreateDispatcher(codeClient, commands);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            CreateForm([new FormEventBinding(FormEventKind.OnLoad, string.Empty, CodeHandler: Handler())]),
            FormEventKind.OnLoad,
            new Dictionary<string, object?> { ["Name"] = "Alice" },
            ct);

        Assert.True(result.Succeeded);
        Assert.NotNull(captured);
        Assert.Equal("code", captured!.Arguments["source"].AsText);
        Assert.Equal("OnLoad", captured.Metadata["event"]);
    }

    private static DefaultFormEventDispatcher CreateDispatcher(
        CSharpDbCodeModuleClient codeClient,
        DbCommandRegistry? commands = null)
        => new(
            commands ?? DbCommandRegistry.Empty,
            DbExtensionPolicies.DefaultHostCallbackPolicy,
            NullFormActionRuntime.Instance,
            new CodeModuleFormEventDispatcher(
                codeClient,
                new CodeModuleRuntimeOptions { EnableInProcessExecution = true }));

    private static CodeModuleDefinition CreateModule(string source)
        => new(
            "form:customers",
            "Customers",
            CodeModuleKind.Form,
            source,
            OwnerKind: "Form",
            OwnerId: "customers-form",
            TypeName: "TestModules.CustomersModule");

    private static CodeModuleHandler Handler(string methodName = "OnLoad")
        => new("form:customers", "TestModules.CustomersModule", methodName);

    private static FormDefinition CreateForm(
        IReadOnlyList<FormEventBinding> eventBindings,
        IReadOnlyList<ControlDefinition>? controls = null)
        => new(
            "customers-form",
            "Customers",
            "Customers",
            DefinitionVersion: 1,
            SourceSchemaSignature: "customers:v1",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: controls ?? [],
            EventBindings: eventBindings);
}
