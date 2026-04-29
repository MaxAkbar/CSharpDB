using System.Reflection;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;

namespace CSharpDB.Admin.Forms.Tests.Components.Designer;

public sealed class FormRendererCommandButtonTests
{
    [Fact]
    public async Task CommandButton_InvokesRegisteredCommandWithRecordAndConfiguredArguments()
    {
        DbCommandContext? captured = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("ShipOrder", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        ControlDefinition button = CreateCommandButton("ShipOrder");
        var renderer = CreateRenderer(commands, CreateForm(button));

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        Assert.NotNull(captured);
        Assert.Equal("ShipOrder", captured.CommandName);
        Assert.Equal(42, captured.Arguments["OrderId"].AsInteger);
        Assert.Equal("manual", captured.Arguments["reason"].AsText);
        Assert.Equal("AdminForms", captured.Metadata["surface"]);
        Assert.Equal("orders-form", captured.Metadata["formId"]);
        Assert.Equal("Click", captured.Metadata["event"]);
        Assert.Equal("button1", captured.Metadata["controlId"]);
    }

    [Fact]
    public async Task CommandButton_ReportsMissingCommand()
    {
        string? error = null;
        ControlDefinition button = CreateCommandButton("MissingCommand");
        var renderer = CreateRenderer(DbCommandRegistry.Empty, CreateForm(button), message => error = message);

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        Assert.Equal("Unknown form command 'MissingCommand'.", error);
    }

    [Fact]
    public async Task ControlOnChange_InvokesRegisteredCommandWithFieldRuntimeArguments()
    {
        DbCommandContext? captured = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditStatus", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        ControlDefinition text = new(
            "status",
            "text",
            new Rect(10, 20, 180, 34),
            new BindingDefinition("Status", "TwoWay"),
            PropertyBag.Empty,
            null,
            EventBindings:
            [
                new ControlEventBinding(
                    ControlEventKind.OnChange,
                    "AuditStatus",
                    new Dictionary<string, object?> { ["source"] = "control-event" }),
            ]);
        var renderer = CreateRenderer(commands, CreateForm(text));

        await InvokeNonPublicAsync(renderer, "SetFieldValueAsync", text, "Status", "Shipped");

        Assert.NotNull(captured);
        Assert.Equal("AuditStatus", captured.CommandName);
        Assert.Equal("AdminForms", captured.Metadata["surface"]);
        Assert.Equal("OnChange", captured.Metadata["event"]);
        Assert.Equal("status", captured.Metadata["controlId"]);
        Assert.Equal("Status", captured.Metadata["fieldName"]);
        Assert.Equal("Shipped", captured.Arguments["Status"].AsText);
        Assert.Equal("Status", captured.Arguments["fieldName"].AsText);
        Assert.Equal("Shipped", captured.Arguments["value"].AsText);
        Assert.Equal("Ready", captured.Arguments["oldValue"].AsText);
        Assert.Equal("control-event", captured.Arguments["source"].AsText);
    }

    [Fact]
    public async Task CommandButton_UsesOnClickBindingWhenNoDirectCommandIsConfigured()
    {
        DbCommandContext? captured = null;
        string? error = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("ShipOrder", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        ControlDefinition button = new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Ship" }),
            null,
            EventBindings:
            [
                new ControlEventBinding(ControlEventKind.OnClick, "ShipOrder"),
            ]);
        var renderer = CreateRenderer(commands, CreateForm(button), message => error = message);

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        Assert.Null(error);
        Assert.NotNull(captured);
        Assert.Equal("OnClick", captured!.Metadata["event"]);
        Assert.Equal("button1", captured.Metadata["controlId"]);
        Assert.Equal(42, captured.Arguments["OrderId"].AsInteger);
    }

    [Fact]
    public async Task CommandButton_ExecutesOnClickActionSequenceWhenNoDirectCommandIsConfigured()
    {
        DbCommandContext? captured = null;
        string? error = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditButtonAction", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        ControlDefinition button = new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Ship" }),
            null,
            EventBindings:
            [
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Shipped"),
                        new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditButtonAction"),
                    ],
                    Name: "ShipButtonActions")),
            ]);
        var renderer = CreateRenderer(commands, CreateForm(button), message => error = message);

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        var record = (Dictionary<string, object?>)GetProperty(renderer, nameof(FormRenderer.Record))!;
        Assert.Null(error);
        Assert.Equal("Shipped", record["Status"]);
        Assert.NotNull(captured);
        Assert.Equal("Shipped", captured!.Arguments["Status"].AsText);
        Assert.Equal("RunCommand", captured.Metadata["actionKind"]);
        Assert.Equal("1", captured.Metadata["actionStep"]);
        Assert.Equal("ShipButtonActions", captured.Metadata["actionSequence"]);
    }

    [Fact]
    public async Task CommandButton_ExecutesReusableActionSequence()
    {
        DbCommandContext? captured = null;
        string? error = null;
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("AuditReusableAction", context =>
            {
                captured = context;
                return DbCommandResult.Success();
            });
        });

        ControlDefinition button = new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Ship" }),
            null,
            EventBindings:
            [
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(
                            DbActionKind.RunActionSequence,
                            SequenceName: "ReusableShip",
                            Arguments: new Dictionary<string, object?> { ["source"] = "button" }),
                    ])),
            ]);
        FormDefinition form = CreateForm(
            button,
            [
                new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Shipped"),
                    new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditReusableAction"),
                ],
                Name: "ReusableShip"),
            ]);
        var renderer = CreateRenderer(commands, form, message => error = message);

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        var record = (Dictionary<string, object?>)GetProperty(renderer, nameof(FormRenderer.Record))!;
        Assert.Null(error);
        Assert.Equal("Shipped", record["Status"]);
        Assert.NotNull(captured);
        Assert.Equal("AuditReusableAction", captured!.CommandName);
        Assert.Equal("button", captured.Arguments["source"].AsText);
        Assert.Equal("ReusableShip", captured.Metadata["actionSequence"]);
    }

    [Fact]
    public async Task CommandButton_ExecutesBuiltInFormAction()
    {
        DbActionStep? captured = null;
        string? error = null;
        ControlDefinition button = new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Next" }),
            null,
            EventBindings:
            [
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.NextRecord),
                    ])),
            ]);
        var renderer = CreateRenderer(DbCommandRegistry.Empty, CreateForm(button), message => error = message);
        SetProperty(
            renderer,
            nameof(FormRenderer.OnBuiltInAction),
            new Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>((step, _) =>
            {
                captured = step;
                return Task.FromResult(FormEventDispatchResult.Success());
            }));

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        Assert.Null(error);
        Assert.NotNull(captured);
        Assert.Equal(DbActionKind.NextRecord, captured!.Kind);
    }

    [Fact]
    public async Task CommandButton_SkipsBuiltInFormActionWhenConditionIsFalse()
    {
        bool invoked = false;
        string? error = null;
        ControlDefinition button = new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Next" }),
            null,
            EventBindings:
            [
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.NextRecord, Condition: "Status = 'Archived'"),
                    ])),
            ]);
        var renderer = CreateRenderer(DbCommandRegistry.Empty, CreateForm(button), message => error = message);
        SetProperty(
            renderer,
            nameof(FormRenderer.OnBuiltInAction),
            new Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>((_, _) =>
            {
                invoked = true;
                return Task.FromResult(FormEventDispatchResult.Success());
            }));

        await InvokeNonPublicAsync(renderer, "InvokeCommandButtonAsync", button);

        Assert.Null(error);
        Assert.False(invoked);
    }

    private static FormRenderer CreateRenderer(
        DbCommandRegistry commands,
        FormDefinition form,
        Action<string>? onCommandError = null)
    {
        var renderer = new FormRenderer();
        SetProperty(renderer, nameof(FormRenderer.Form), form);
        SetProperty(renderer, nameof(FormRenderer.Record), new Dictionary<string, object?>
        {
            ["OrderId"] = 42L,
            ["Status"] = "Ready",
        });
        SetProperty(renderer, "Commands", commands);

        if (onCommandError is not null)
        {
            EventCallback<string> callback = EventCallback.Factory.Create(new object(), onCommandError);
            SetProperty(renderer, nameof(FormRenderer.OnCommandError), callback);
        }

        return renderer;
    }

    private static ControlDefinition CreateCommandButton(string commandName)
        => new(
            "button1",
            "commandButton",
            new Rect(10, 20, 120, 34),
            null,
            new PropertyBag(new Dictionary<string, object?>
            {
                ["text"] = "Ship",
                ["commandName"] = commandName,
                ["commandArguments"] = new Dictionary<string, object?> { ["reason"] = "manual" },
            }),
            null);

    private static FormDefinition CreateForm(
        ControlDefinition button,
        IReadOnlyList<DbActionSequence>? actionSequences = null)
        => new(
            "orders-form",
            "Orders",
            "Orders",
            1,
            "sig:orders",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            [button],
            ActionSequences: actionSequences);

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
    }

    private static object? GetProperty(object instance, string propertyName)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        return property.GetValue(instance);
    }

    private static async Task InvokeNonPublicAsync(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task?)method.Invoke(instance, args)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        await task;
    }
}
