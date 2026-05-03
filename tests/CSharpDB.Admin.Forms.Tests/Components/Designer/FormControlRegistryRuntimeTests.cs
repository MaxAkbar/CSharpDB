using System.Reflection;
using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Components.Designer;

public sealed class FormControlRegistryRuntimeTests
{
    [Fact]
    public void FormRenderer_ResolvesCustomRuntimeComponent()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition control = CreateRatingControl();
        var renderer = CreateRenderer(registry, DbCommandRegistry.Empty, CreateForm(control));

        object?[] args = [control, null];
        bool resolved = InvokeNonPublic<bool>(renderer, "TryGetRuntimeComponent", args);

        Assert.True(resolved);
        Assert.Equal(typeof(RatingRuntimeComponent), Assert.IsAssignableFrom<Type>(args[1]));
    }

    [Fact]
    public void FormRenderer_KeepsBuiltInRendererUnlessReplacementIsOptedIn()
    {
        IFormControlRegistry registry = CreateRegistry();
        ControlDefinition control = new(
            "name",
            "text",
            new Rect(0, 0, 180, 32),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            null);
        var renderer = CreateRenderer(registry, DbCommandRegistry.Empty, CreateForm(control));

        object?[] args = [control, null];
        bool resolved = InvokeNonPublic<bool>(renderer, "TryGetRuntimeComponent", args);

        Assert.False(resolved);
        Assert.Null(args[1]);
    }

    [Fact]
    public void FormRenderer_ResolvesBuiltInRuntimeReplacementWhenOptedIn()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "text",
            DisplayName = "Text Replacement",
            RuntimeComponentType = typeof(RatingRuntimeComponent),
            ReplaceBuiltInRuntime = true,
        }));
        ControlDefinition control = new(
            "name",
            "text",
            new Rect(0, 0, 180, 32),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            null);
        var renderer = CreateRenderer(registry, DbCommandRegistry.Empty, CreateForm(control));

        object?[] args = [control, null];
        bool resolved = InvokeNonPublic<bool>(renderer, "TryGetRuntimeComponent", args);

        Assert.True(resolved);
        Assert.Equal(typeof(RatingRuntimeComponent), Assert.IsAssignableFrom<Type>(args[1]));
    }

    [Fact]
    public void FormRenderer_BuildsRuntimeContextForCustomComponent()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition control = CreateRatingControl() with
        {
            Props = new PropertyBag(new Dictionary<string, object?>
            {
                ["enabled"] = false,
                ["readOnly"] = true,
            }),
        };
        var renderer = CreateRenderer(
            registry,
            DbCommandRegistry.Empty,
            CreateForm(control),
            record: new Dictionary<string, object?> { ["Rating"] = "3" },
            choices: new Dictionary<string, IReadOnlyList<EnumChoice>>
            {
                ["Rating"] = [new EnumChoice("3", "Three")],
            });

        Dictionary<string, object?> parameters = InvokeNonPublic<Dictionary<string, object?>>(
            renderer,
            "GetRuntimeComponentParameters",
            control,
            "Rating",
            "Rating is required.",
            4);
        var context = Assert.IsType<FormControlRuntimeContext>(parameters["Context"]);

        Assert.Equal("custom-form", context.Form.FormId);
        Assert.Equal(control.ControlId, context.Control.ControlId);
        Assert.Equal("Rating", context.FieldName);
        Assert.Equal("3", context.BoundValue);
        Assert.Single(context.Choices);
        Assert.False(context.IsEnabled);
        Assert.True(context.IsReadOnly);
        Assert.Equal("Rating is required.", context.ValidationError);
        Assert.Equal(4, context.TabIndex);
    }

    [Fact]
    public async Task RuntimeContext_SetValueAndDispatchEventUseRendererRuntimePath()
    {
        List<DbCommandContext> captured = [];
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("CaptureChange", context =>
            {
                captured.Add(context);
                return DbCommandResult.Success();
            });
            builder.AddCommand("CaptureClick", context =>
            {
                captured.Add(context);
                return DbCommandResult.Success();
            });
        });
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition control = CreateRatingControl() with
        {
            EventBindings =
            [
                new ControlEventBinding(ControlEventKind.OnChange, "CaptureChange"),
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    "CaptureClick",
                    new Dictionary<string, object?> { ["configured"] = "yes" }),
            ],
        };
        var record = new Dictionary<string, object?> { ["Rating"] = "2" };
        var renderer = CreateRenderer(registry, commands, CreateForm(control), record);
        FormControlRuntimeContext context = GetRuntimeContext(renderer, control, "Rating");

        await context.SetValueAsync("4");
        await context.DispatchEventAsync(
            ControlEventKind.OnClick,
            new Dictionary<string, object?> { ["source"] = "custom-runtime" });

        Assert.Equal("4", record["Rating"]);
        Assert.Equal(2, captured.Count);
        Assert.Equal("CaptureChange", captured[0].CommandName);
        Assert.Equal("OnChange", captured[0].Metadata["event"]);
        Assert.Equal("4", captured[0].Arguments["value"].AsText);
        Assert.Equal("2", captured[0].Arguments["oldValue"].AsText);
        Assert.Equal("CaptureClick", captured[1].CommandName);
        Assert.Equal("OnClick", captured[1].Metadata["event"]);
        Assert.Equal("custom-runtime", captured[1].Arguments["source"].AsText);
        Assert.Equal("yes", captured[1].Arguments["configured"].AsText);
    }

    private static FormControlRuntimeContext GetRuntimeContext(FormRenderer renderer, ControlDefinition control, string fieldName)
    {
        Dictionary<string, object?> parameters = InvokeNonPublic<Dictionary<string, object?>>(
            renderer,
            "GetRuntimeComponentParameters",
            control,
            fieldName,
            null,
            1);
        return Assert.IsType<FormControlRuntimeContext>(parameters["Context"]);
    }

    private static FormRenderer CreateRenderer(
        IFormControlRegistry registry,
        DbCommandRegistry commands,
        FormDefinition form,
        Dictionary<string, object?>? record = null,
        IReadOnlyDictionary<string, IReadOnlyList<EnumChoice>>? choices = null)
    {
        var renderer = new FormRenderer();
        SetProperty(renderer, nameof(FormRenderer.Form), form);
        SetProperty(renderer, nameof(FormRenderer.Record), record ?? new Dictionary<string, object?>());
        SetProperty(renderer, nameof(FormRenderer.Choices), choices);
        SetProperty(renderer, nameof(FormRenderer.ControlRegistry), registry);
        SetProperty(renderer, "Commands", commands);
        return renderer;
    }

    private static IFormControlRegistry CreateRegistry(Action<FormControlRegistryBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
        if (configure is not null)
            services.AddCSharpDbAdminFormControls(configure);

        using ServiceProvider provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IFormControlRegistry>();
    }

    private static FormControlDescriptor CreateRatingDescriptor()
        => new()
        {
            ControlType = "rating",
            DisplayName = "Rating",
            ToolboxGroup = "Custom",
            IconText = "*",
            DefaultWidth = 180,
            DefaultHeight = 42,
            SupportsBinding = true,
            RuntimeComponentType = typeof(RatingRuntimeComponent),
        };

    private static ControlDefinition CreateRatingControl()
        => new(
            "rating1",
            "rating",
            new Rect(0, 0, 180, 42),
            new BindingDefinition("Rating", "TwoWay"),
            PropertyBag.Empty,
            null);

    private static FormDefinition CreateForm(ControlDefinition control)
        => new(
            "custom-form",
            "Custom Form",
            "Orders",
            1,
            "orders:v1",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            [control]);

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
    }

    private static T InvokeNonPublic<T>(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        return (T)method.Invoke(instance, args)!;
    }

    private sealed class RatingRuntimeComponent : ComponentBase
    {
        [Parameter] public FormControlRuntimeContext Context { get; set; } = default!;
    }
}
