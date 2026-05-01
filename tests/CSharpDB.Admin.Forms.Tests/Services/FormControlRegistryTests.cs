using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class FormControlRegistryTests
{
    [Fact]
    public void AddCSharpDbAdminForms_SeedsBuiltInControls()
    {
        IFormControlRegistry registry = CreateRegistry();

        Assert.Contains(registry.Controls, control => control.ControlType == "comboBox" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "listBox" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "optionGroup" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "toggleButton" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "tabControl" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "subform" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "attachment" && control.IsBuiltIn);
        Assert.Contains(registry.Controls, control => control.ControlType == "image" && control.IsBuiltIn);
    }

    [Fact]
    public void AddCSharpDbAdminFormControls_AddsCustomControlDescriptor()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(CreateRatingDescriptor()));

        Assert.True(registry.TryGetControl("RATING", out FormControlDescriptor descriptor));
        Assert.Equal("rating", descriptor.ControlType);
        Assert.Equal("Rating", descriptor.DisplayName);
        Assert.Equal("Custom", descriptor.ToolboxGroup);
        Assert.Equal(180, descriptor.DefaultWidth);
        Assert.Equal(42, descriptor.DefaultHeight);
        Assert.Equal(typeof(RatingRuntimeComponent), descriptor.RuntimeComponentType);
        Assert.Equal("star", descriptor.CreateDefaultProps()["displayMode"]);
        Assert.Contains(registry.GetToolboxControls(), control => control.ControlType == "rating");
    }

    [Fact]
    public void CreateDefaultProps_ClonesNestedDefaults()
    {
        var descriptor = new FormControlDescriptor
        {
            ControlType = "compound",
            DisplayName = "Compound",
            DefaultProps = new Dictionary<string, object?>
            {
                ["items"] = new object?[]
                {
                    new Dictionary<string, object?> { ["label"] = "One" },
                },
            },
        };

        Dictionary<string, object?> first = descriptor.CreateDefaultProps();
        Dictionary<string, object?> second = descriptor.CreateDefaultProps();

        Assert.NotSame(first["items"], second["items"]);
        var firstItems = Assert.IsType<object?[]>(first["items"]);
        var secondItems = Assert.IsType<object?[]>(second["items"]);
        Assert.NotSame(firstItems[0], secondItems[0]);
    }

    [Fact]
    public void DuplicateControlType_ThrowsWhenRegistryIsResolved()
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
        services.AddCSharpDbAdminFormControls(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "text",
            DisplayName = "Text Replacement",
        }));

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IFormControlRegistry>());
    }

    [Fact]
    public void ReplaceBuiltInRuntime_RequiresExplicitOptIn()
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
        services.AddCSharpDbAdminFormControls(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "text",
            DisplayName = "Text Replacement",
            RuntimeComponentType = typeof(RatingRuntimeComponent),
        }));

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IFormControlRegistry>());
    }

    [Fact]
    public void ReplaceBuiltInRuntime_UpdatesBuiltInDescriptorWhenOptedIn()
    {
        IFormControlRegistry registry = CreateRegistry(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "text",
            DisplayName = "Text Replacement",
            RuntimeComponentType = typeof(RatingRuntimeComponent),
            ReplaceBuiltInRuntime = true,
        }));

        Assert.True(registry.TryGetControl("text", out FormControlDescriptor descriptor));
        Assert.True(descriptor.IsBuiltIn);
        Assert.True(descriptor.ReplaceBuiltInRuntime);
        Assert.Equal(typeof(RatingRuntimeComponent), descriptor.RuntimeComponentType);
        Assert.Equal("Text", descriptor.DisplayName);
    }

    [Fact]
    public void InvalidComponentType_IsRejected()
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
        services.AddCSharpDbAdminFormControls(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "invalid",
            DisplayName = "Invalid",
            RuntimeComponentType = typeof(string),
        }));

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Throws<ArgumentException>(() => provider.GetRequiredService<IFormControlRegistry>());
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
            ParticipatesInTabOrder = true,
            DefaultProps = new Dictionary<string, object?> { ["displayMode"] = "star" },
            RuntimeComponentType = typeof(RatingRuntimeComponent),
            PropertyDescriptors =
            [
                new FormControlPropertyDescriptor
                {
                    Name = "max",
                    Label = "Max",
                    Editor = FormControlPropertyEditor.Number,
                    DefaultValue = 5L,
                },
            ],
        };

    private sealed class RatingRuntimeComponent : ComponentBase
    {
        [Parameter] public FormControlRuntimeContext Context { get; set; } = default!;
    }
}
