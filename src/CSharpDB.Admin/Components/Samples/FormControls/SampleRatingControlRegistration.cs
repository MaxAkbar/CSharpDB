using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;

namespace CSharpDB.Admin.Components.Samples.FormControls;

public static class SampleRatingControlRegistration
{
    public static IServiceCollection AddSampleFormControls(this IServiceCollection services)
        => services.AddCSharpDbAdminFormControls(builder => builder.Add(new FormControlDescriptor
        {
            ControlType = "sampleRating",
            DisplayName = "Rating",
            ToolboxGroup = "Custom",
            IconText = "*",
            Description = "Sample custom scalar rating control.",
            DefaultWidth = 220,
            DefaultHeight = 48,
            SupportsBinding = true,
            ParticipatesInTabOrder = true,
            DefaultProps = new Dictionary<string, object?>
            {
                ["max"] = 5,
                ["displayMode"] = "buttons",
                ["required"] = false,
            },
            DesignerPreviewComponentType = typeof(RatingDesignerPreview),
            RuntimeComponentType = typeof(RatingRuntimeControl),
            PropertyEditorComponentType = typeof(RatingPropertyEditor),
        }));
}
