using System.Reflection;
using System.Runtime.CompilerServices;
using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Components.Designer;

public sealed class FormControlRegistryDesignerTests
{
    [Fact]
    public void Toolbox_GroupsCustomControlsFromRegistry()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        var toolbox = new Toolbox
        {
            State = new DesignerState(),
            ControlRegistry = registry,
        };

        var groups = InvokeNonPublic<IReadOnlyList<(string Name, IReadOnlyList<FormControlDescriptor> Controls)>>(
            toolbox,
            "GetToolboxGroups");

        Assert.Contains(
            groups,
            group => group.Name == "Custom" && group.Controls.Any(control => control.ControlType == "rating"));
    }

    [Fact]
    public void DesignCanvas_PlacesRegisteredControlWithDefaultSizePropsAndBinding()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        var state = new DesignerState();
        state.LoadForm(CreateForm([]));
        state.ActiveTool = "rating";
        var canvas = new DesignCanvas
        {
            State = state,
            ControlRegistry = registry,
        };

        InvokeNonPublic(
            canvas,
            "PlaceNewControl",
            new PointerEventArgs
            {
                OffsetX = 13,
                OffsetY = 21,
            });

        ControlDefinition control = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Equal("rating", control.ControlType);
        Assert.Equal(new Rect(16, 24, 180, 42), control.Rect);
        Assert.NotNull(control.Binding);
        Assert.Equal(string.Empty, control.Binding!.FieldName);
        Assert.Equal("star", control.Props.Values["displayMode"]);
        Assert.Null(state.ActiveTool);
    }

    [Fact]
    public void DesignCanvas_PlacesNonBindingRegisteredControlWithoutBinding()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateBadgeDescriptor()));
        var state = new DesignerState();
        state.LoadForm(CreateForm([]));
        state.ActiveTool = "badge";
        var canvas = new DesignCanvas
        {
            State = state,
            ControlRegistry = registry,
        };

        InvokeNonPublic(
            canvas,
            "PlaceNewControl",
            new PointerEventArgs
            {
                OffsetX = 8,
                OffsetY = 8,
            });

        ControlDefinition control = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Equal("badge", control.ControlType);
        Assert.Null(control.Binding);
        Assert.Equal("info", control.Props.Values["tone"]);
    }

    [Fact]
    public void DesignCanvas_UsesRegisteredDesignerPreviewComponent()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition control = CreateRatingControl();
        var canvas = new DesignCanvas
        {
            State = new DesignerState(),
            ControlRegistry = registry,
        };

        object?[] args = [control, null];
        bool resolved = InvokeNonPublic<bool>(canvas, "TryGetDesignerPreviewComponent", args);
        Type componentType = Assert.IsAssignableFrom<Type>(args[1]);
        Dictionary<string, object?> parameters = InvokeNonPublic<Dictionary<string, object?>>(
            canvas,
            "GetDesignerPreviewParameters",
            control,
            true,
            control.Rect);

        Assert.True(resolved);
        Assert.Equal(typeof(RatingPreviewComponent), componentType);
        var context = Assert.IsType<FormControlDesignContext>(parameters["Context"]);
        Assert.Equal(control.ControlId, context.Control.ControlId);
        Assert.True(context.IsSelected);
        Assert.Equal("Rating", context.Descriptor.DisplayName);
    }

    [Fact]
    public void PropertyInspector_TypeDropdownUsesRegistry()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        var inspector = new PropertyInspector
        {
            State = new DesignerState(),
            ControlRegistry = registry,
        };

        IReadOnlyList<FormControlDescriptor> controls = InvokeNonPublic<IReadOnlyList<FormControlDescriptor>>(
            inspector,
            "GetTypeDropdownControls");

        Assert.Contains(controls, control => control.ControlType == "rating");
    }

    [Fact]
    public void PropertyInspector_GenericPropertyEditingUpdatesSelectedControlProps()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition rating = CreateRatingControl();
        var state = new DesignerState();
        state.LoadForm(CreateForm([rating]));
        state.SelectControl(rating.ControlId, addToSelection: false);
        var inspector = new PropertyInspector
        {
            State = state,
            ControlRegistry = registry,
        };
        SetField(inspector, "_selected", rating);

        FormControlDescriptor descriptor = registry.Controls.Single(control => control.ControlType == "rating");
        FormControlPropertyDescriptor maxProperty = descriptor.PropertyDescriptors.Single(property => property.Name == "max");
        FormControlPropertyDescriptor requiredProperty = descriptor.PropertyDescriptors.Single(property => property.Name == "required");

        InvokeNonPublic(inspector, "OnRegisteredPropertyChanged", maxProperty, "7");
        InvokeNonPublic(inspector, "OnRegisteredPropertyChanged", requiredProperty, true);

        ControlDefinition updated = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Equal(7L, updated.Props.Values["max"]);
        Assert.Equal(true, updated.Props.Values["required"]);
    }

    [Fact]
    public async Task PropertyInspector_CustomPropertyEditorContextUpdatesProps()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));
        ControlDefinition rating = CreateRatingControl();
        var state = new DesignerState();
        state.LoadForm(CreateForm([rating]));
        state.SelectControl(rating.ControlId, addToSelection: false);
        var inspector = new PropertyInspector
        {
            State = state,
            ControlRegistry = registry,
        };
        SetField(inspector, "_selected", rating);
        FormControlDescriptor descriptor = registry.Controls.Single(control => control.ControlType == "rating");

        Dictionary<string, object?> parameters = InvokeNonPublic<Dictionary<string, object?>>(
            inspector,
            "GetPropertyEditorParameters",
            descriptor);
        var context = Assert.IsType<FormControlPropertyContext>(parameters["Context"]);

        await context.SetPropertyAsync("displayMode", "compact");

        ControlDefinition updated = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Equal("compact", updated.Props.Values["displayMode"]);
    }

    private static IFormControlRegistry CreateRegistry(Action<FormControlRegistryBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
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
            ParticipatesInTabOrder = true,
            DefaultProps = new Dictionary<string, object?> { ["displayMode"] = "star" },
            DesignerPreviewComponentType = typeof(RatingPreviewComponent),
            RuntimeComponentType = typeof(RatingRuntimeComponent),
            PropertyEditorComponentType = typeof(RatingPropertyEditorComponent),
            PropertyDescriptors =
            [
                new FormControlPropertyDescriptor
                {
                    Name = "max",
                    Label = "Max",
                    Editor = FormControlPropertyEditor.Number,
                    DefaultValue = 5L,
                },
                new FormControlPropertyDescriptor
                {
                    Name = "required",
                    Label = "Required",
                    Editor = FormControlPropertyEditor.Checkbox,
                    DefaultValue = false,
                },
            ],
        };

    private static FormControlDescriptor CreateBadgeDescriptor()
        => new()
        {
            ControlType = "badge",
            DisplayName = "Badge",
            ToolboxGroup = "Custom",
            IconText = "B",
            DefaultWidth = 96,
            DefaultHeight = 28,
            SupportsBinding = false,
            ParticipatesInTabOrder = false,
            DefaultProps = new Dictionary<string, object?> { ["tone"] = "info" },
        };

    private static ControlDefinition CreateRatingControl()
        => new(
            "rating1",
            "rating",
            new Rect(0, 0, 180, 42),
            new BindingDefinition("Rating", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?> { ["displayMode"] = "star" }),
            null);

    private static FormDefinition CreateForm(IReadOnlyList<ControlDefinition> controls)
        => new(
            "custom-form",
            "Custom Form",
            "Orders",
            1,
            "orders:v1",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            controls);

    private static void InvokeNonPublic(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        method.Invoke(instance, args);
    }

    private static T InvokeNonPublic<T>(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        return (T)method.Invoke(instance, args)!;
    }

    private static void SetField(object instance, string fieldName, object? value)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        field.SetValue(instance, value);
    }

    private sealed class RatingPreviewComponent : ComponentBase
    {
        [Parameter] public FormControlDesignContext Context { get; set; } = default!;
    }

    private sealed class RatingRuntimeComponent : ComponentBase
    {
        [Parameter] public FormControlRuntimeContext Context { get; set; } = default!;
    }

    private sealed class RatingPropertyEditorComponent : ComponentBase
    {
        [Parameter] public FormControlPropertyContext Context { get; set; } = default!;
    }
}
