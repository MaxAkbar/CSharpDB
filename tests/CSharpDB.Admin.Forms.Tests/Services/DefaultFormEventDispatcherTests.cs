using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class DefaultFormEventDispatcherTests
{
    [Fact]
    public async Task DispatchAsync_InvokesMatchingCommandsWithRecordArgumentsAndMetadata()
    {
        DbCommandContext? captured = null;
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditChange", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.AfterUpdate,
                "AuditChange",
                new Dictionary<string, object?> { ["Reason"] = "manual" }),
        ]);

        var result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.AfterUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L, ["Name"] = "Alice" },
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.NotNull(captured);
        Assert.Equal("AfterUpdate", captured!.Metadata["event"]);
        Assert.Equal("AdminForms", captured.Metadata["surface"]);
        Assert.Equal("customers-form", captured.Metadata["formId"]);
        Assert.Equal("Customers", captured.Metadata["tableName"]);
        Assert.Equal(7, captured.Arguments["Id"].AsInteger);
        Assert.Equal("Alice", captured.Arguments["Name"].AsText);
        Assert.Equal("manual", captured.Arguments["Reason"].AsText);
    }

    [Fact]
    public async Task DispatchAsync_FailsForMissingCommand()
    {
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        var form = CreateForm([new FormEventBinding(FormEventKind.BeforeDelete, "MissingCommand")]);

        var result = await dispatcher.DispatchAsync(form, FormEventKind.BeforeDelete, ct: TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("Unknown form command 'MissingCommand'", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_StopsOnCommandFailureByDefault()
    {
        var calls = new List<string>();
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("Reject", _ =>
            {
                calls.Add("reject");
                return DbCommandResult.Failure("Rejected.");
            });
            builder.AddCommand("After", _ =>
            {
                calls.Add("after");
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(FormEventKind.BeforeUpdate, "Reject"),
            new FormEventBinding(FormEventKind.BeforeUpdate, "After"),
        ]);

        var result = await dispatcher.DispatchAsync(form, FormEventKind.BeforeUpdate, ct: TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Equal("Rejected.", result.Message);
        Assert.Equal(["reject"], calls);
    }

    [Fact]
    public async Task DispatchAsync_ContinuesWhenStopOnFailureIsFalse()
    {
        var calls = new List<string>();
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("Reject", _ =>
            {
                calls.Add("reject");
                return DbCommandResult.Failure("Rejected.");
            });
            builder.AddCommand("After", _ =>
            {
                calls.Add("after");
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(FormEventKind.BeforeUpdate, "Reject", StopOnFailure: false),
            new FormEventBinding(FormEventKind.BeforeUpdate, "After"),
        ]);

        var result = await dispatcher.DispatchAsync(form, FormEventKind.BeforeUpdate, ct: TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal(["reject", "after"], calls);
    }

    [Fact]
    public async Task DispatchAsync_ReturnsFailureWhenCommandTimesOut()
    {
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddAsyncCommand(
                "SlowAudit",
                new DbCommandOptions(Timeout: TimeSpan.FromMilliseconds(10)),
                static async (_, ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    return DbCommandResult.Success();
                });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([new FormEventBinding(FormEventKind.AfterUpdate, "SlowAudit")]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.AfterUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("SlowAudit", result.Message);
        Assert.Contains("timed out", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_ExecutesActionSequenceAndMutatesMutableRecord()
    {
        DbCommandContext? captured = null;
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditChange", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.BeforeUpdate,
                string.Empty,
                new Dictionary<string, object?> { ["Reason"] = "action-sequence" },
                ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.SetFieldValue, Target: "Name", Value: "Bob"),
                        new DbActionStep(
                            DbActionKind.RunCommand,
                            CommandName: "AuditChange",
                            Arguments: new Dictionary<string, object?> { ["Step"] = "audit" }),
                    ],
                    Name: "BeforeUpdateActions")),
        ]);
        var record = new Dictionary<string, object?> { ["Id"] = 7L, ["Name"] = "Alice" };

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            record,
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal("Bob", record["Name"]);
        Assert.NotNull(captured);
        Assert.Equal("Bob", captured!.Arguments["Name"].AsText);
        Assert.Equal("action-sequence", captured.Arguments["Reason"].AsText);
        Assert.Equal("audit", captured.Arguments["Step"].AsText);
        Assert.Equal("RunCommand", captured.Metadata["actionKind"]);
        Assert.Equal("1", captured.Metadata["actionStep"]);
        Assert.Equal("BeforeUpdateActions", captured.Metadata["actionSequence"]);
    }

    [Fact]
    public async Task DispatchAsync_ActionSequenceConditionsSkipAndRunSteps()
    {
        DbCommandContext? captured = null;
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditReady", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.BeforeUpdate,
                string.Empty,
                ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(
                            DbActionKind.SetFieldValue,
                            Target: "Status",
                            Value: "Skipped",
                            Condition: "Name = 'Not Alice'"),
                        new DbActionStep(
                            DbActionKind.SetFieldValue,
                            Target: "Status",
                            Value: "Ready",
                            Condition: "Name = 'Alice'"),
                        new DbActionStep(
                            DbActionKind.RunCommand,
                            CommandName: "AuditReady",
                            Condition: "Status = 'Ready'"),
                    ])),
        ]);
        var record = new Dictionary<string, object?> { ["Id"] = 7L, ["Name"] = "Alice", ["Status"] = "Draft" };

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            record,
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal("Ready", record["Status"]);
        Assert.NotNull(captured);
        Assert.Equal("Ready", captured!.Arguments["Status"].AsText);
        Assert.Equal("Status = 'Ready'", captured.Metadata["actionCondition"]);
    }

    [Fact]
    public async Task DispatchAsync_RunActionSequenceInvokesReusableSequence()
    {
        DbCommandContext? captured = null;
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditPrepared", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm(
            [
                new FormEventBinding(
                    FormEventKind.BeforeUpdate,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(
                            DbActionKind.RunActionSequence,
                            SequenceName: "PrepareOrder",
                            Arguments: new Dictionary<string, object?> { ["source"] = "event-binding" }),
                    ],
                    Name: "BeforeUpdateActions")),
            ],
            [
                new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Ready"),
                    new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditPrepared"),
                ],
                Name: "PrepareOrder"),
            ]);
        var record = new Dictionary<string, object?> { ["Id"] = 7L, ["Status"] = "Draft" };

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            record,
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal("Ready", record["Status"]);
        Assert.NotNull(captured);
        Assert.Equal("AuditPrepared", captured!.CommandName);
        Assert.Equal("Ready", captured.Arguments["Status"].AsText);
        Assert.Equal("event-binding", captured.Arguments["source"].AsText);
        Assert.Equal("PrepareOrder", captured.Metadata["actionSequence"]);
        Assert.Equal("RunCommand", captured.Metadata["actionKind"]);
        Assert.Equal("1", captured.Metadata["actionStep"]);
    }

    [Fact]
    public async Task DispatchAsync_RunActionSequenceFailsForMissingReusableSequence()
    {
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.BeforeUpdate,
                string.Empty,
                ActionSequence: new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "MissingSequence"),
                ])),
        ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("Unknown form action sequence 'MissingSequence'", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_RunActionSequenceStopsRecursiveLoops()
    {
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        var form = CreateForm(
            [
                new FormEventBinding(
                    FormEventKind.BeforeUpdate,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "A"),
                    ])),
            ],
            [
                new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "B"),
                ],
                Name: "A"),
                new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "A"),
                ],
                Name: "B"),
            ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("nesting limit", result.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DispatchAsync_ActionSequenceConditionFailureStopsByDefault()
    {
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.BeforeUpdate,
                string.Empty,
                ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(
                            DbActionKind.SetFieldValue,
                            Target: "Status",
                            Value: "Ready",
                            Condition: "MissingField = 'Ready'"),
                    ])),
        ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.BeforeUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L, ["Status"] = "Draft" },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("condition failed", result.Message);
        Assert.Contains("MissingField", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_ActionSequenceFailureStopsByDefault()
    {
        var commands = DbCommandRegistry.Empty;
        var dispatcher = new DefaultFormEventDispatcher(commands);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.AfterUpdate,
                string.Empty,
                ActionSequence: new DbActionSequence(
                    [new DbActionStep(DbActionKind.RunCommand, CommandName: "MissingCommand")])),
        ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.AfterUpdate,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("Unknown form command 'MissingCommand'", result.Message);
    }

    [Fact]
    public async Task DispatchAsync_Phase8ActionsUseTypedRuntime()
    {
        var runtime = new RecordingFormActionRuntime();
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty, runtime);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.OnLoad,
                string.Empty,
                ActionSequence: new DbActionSequence(
                [
                    new DbActionStep(
                        DbActionKind.OpenForm,
                        Target: "orders-detail",
                        Arguments: new Dictionary<string, object?> { ["mode"] = "dialog" }),
                    new DbActionStep(
                        DbActionKind.ApplyFilter,
                        Target: "orders-grid",
                        Value: "[Status] = 'Open'"),
                    new DbActionStep(DbActionKind.ClearFilter, Target: "orders-grid"),
                    new DbActionStep(
                        DbActionKind.RunSql,
                        Value: "UPDATE Orders SET Status = @status WHERE Id = @id",
                        Arguments: new Dictionary<string, object?> { ["status"] = "Ready" }),
                    new DbActionStep(DbActionKind.RunProcedure, Target: "RepriceOrder"),
                    new DbActionStep(DbActionKind.SetControlVisibility, Target: "internalNotes", Value: false),
                ],
                Name: "LoadActions")),
        ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.OnLoad,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal(
            ["OpenForm:orders-detail", "ApplyFilter:orders-grid:[Status] = 'Open'", "ClearFilter:orders-grid", "RunSql:UPDATE Orders SET Status = @status WHERE Id = @id", "RunProcedure:RepriceOrder", "SetControlProperty:internalNotes.visible=False"],
            runtime.Calls);
        Assert.NotNull(runtime.LastContext);
        Assert.Equal("customers-form", runtime.LastContext!.FormId);
        Assert.Equal("Customers Form", runtime.LastContext.FormName);
        Assert.Equal("OnLoad", runtime.LastContext.EventName);
        Assert.Equal("LoadActions", runtime.LastContext.ActionSequenceName);
    }

    [Fact]
    public async Task DispatchAsync_Phase8ActionsFailClearlyWithoutRuntime()
    {
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        var form = CreateForm([
            new FormEventBinding(
                FormEventKind.OnLoad,
                string.Empty,
                ActionSequence: new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.OpenForm, Target: "orders-detail"),
                ])),
        ]);

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.OnLoad,
            new Dictionary<string, object?> { ["Id"] = 7L },
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        Assert.Contains("OpenForm", result.Message);
        Assert.Contains("rendered form runtime", result.Message);
    }

    private static FormDefinition CreateForm(
        IReadOnlyList<FormEventBinding> eventBindings,
        IReadOnlyList<DbActionSequence>? actionSequences = null)
        => new(
            "customers-form",
            "Customers Form",
            "Customers",
            DefinitionVersion: 1,
            SourceSchemaSignature: "customers:v1",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: [],
            EventBindings: eventBindings,
            ActionSequences: actionSequences);

    private sealed class RecordingFormActionRuntime : IFormActionRuntime
    {
        public List<string> Calls { get; } = [];

        public FormActionRuntimeContext? LastContext { get; private set; }

        public Task<FormEventDispatchResult> ExecuteRecordActionAsync(
            FormActionRuntimeContext context,
            DbActionStep step,
            CancellationToken ct)
            => RecordAsync(context, step.Kind.ToString());

        public Task<FormEventDispatchResult> OpenFormAsync(
            FormActionRuntimeContext context,
            string formName,
            IReadOnlyDictionary<string, object?> arguments,
            CancellationToken ct)
            => RecordAsync(context, $"OpenForm:{formName}");

        public Task<FormEventDispatchResult> CloseFormAsync(
            FormActionRuntimeContext context,
            string? formName,
            CancellationToken ct)
            => RecordAsync(context, $"CloseForm:{formName}");

        public Task<FormEventDispatchResult> ApplyFilterAsync(
            FormActionRuntimeContext context,
            string target,
            string filter,
            IReadOnlyDictionary<string, object?> arguments,
            CancellationToken ct)
            => RecordAsync(context, $"ApplyFilter:{target}:{filter}");

        public Task<FormEventDispatchResult> ClearFilterAsync(
            FormActionRuntimeContext context,
            string target,
            CancellationToken ct)
            => RecordAsync(context, $"ClearFilter:{target}");

        public Task<FormEventDispatchResult> RunSqlAsync(
            FormActionRuntimeContext context,
            string sqlOrName,
            IReadOnlyDictionary<string, object?> arguments,
            CancellationToken ct)
            => RecordAsync(context, $"RunSql:{sqlOrName}");

        public Task<FormEventDispatchResult> RunProcedureAsync(
            FormActionRuntimeContext context,
            string procedureName,
            IReadOnlyDictionary<string, object?> arguments,
            CancellationToken ct)
            => RecordAsync(context, $"RunProcedure:{procedureName}");

        public Task<FormEventDispatchResult> SetControlPropertyAsync(
            FormActionRuntimeContext context,
            string controlId,
            string propertyName,
            object? value,
            CancellationToken ct)
            => RecordAsync(context, $"SetControlProperty:{controlId}.{propertyName}={value}");

        private Task<FormEventDispatchResult> RecordAsync(FormActionRuntimeContext context, string call)
        {
            LastContext = context;
            Calls.Add(call);
            return Task.FromResult(FormEventDispatchResult.Success());
        }
    }
}
