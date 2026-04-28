using System.Reflection;
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

    private static FormDefinition CreateForm(ControlDefinition button)
        => new(
            "orders-form",
            "Orders",
            "Orders",
            1,
            "sig:orders",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            [button]);

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
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
