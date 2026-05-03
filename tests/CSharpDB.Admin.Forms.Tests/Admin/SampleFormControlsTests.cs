using CSharpDB.Admin.Components.Samples.FormControls;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class SampleFormControlsTests
{
    [Fact]
    public void AddSampleFormControls_RegistersCompiledRatingControl()
    {
        var services = new ServiceCollection();
        services.AddCSharpDbAdminForms();
        services.AddSampleFormControls();

        using ServiceProvider provider = services.BuildServiceProvider();
        IFormControlRegistry registry = provider.GetRequiredService<IFormControlRegistry>();

        Assert.True(registry.TryGetControl("sampleRating", out FormControlDescriptor descriptor));
        Assert.Equal("Rating", descriptor.DisplayName);
        Assert.Equal("Custom", descriptor.ToolboxGroup);
        Assert.Equal(typeof(RatingDesignerPreview), descriptor.DesignerPreviewComponentType);
        Assert.Equal(typeof(RatingRuntimeControl), descriptor.RuntimeComponentType);
        Assert.Equal(typeof(RatingPropertyEditor), descriptor.PropertyEditorComponentType);
        Assert.Equal(5, descriptor.CreateDefaultProps()["max"]);
    }
}
