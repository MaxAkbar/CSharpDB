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

    private static FormDefinition CreateForm(IReadOnlyList<FormEventBinding> eventBindings)
        => new(
            "customers-form",
            "Customers Form",
            "Customers",
            DefinitionVersion: 1,
            SourceSchemaSignature: "customers:v1",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: [],
            EventBindings: eventBindings);
}
