using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class FormControlRegistryBuilder
{
    private readonly Dictionary<string, FormControlDescriptor> _controls = new(StringComparer.OrdinalIgnoreCase);

    public FormControlRegistryBuilder Add(FormControlDescriptor descriptor)
    {
        ValidateDescriptor(descriptor);

        if (_controls.TryGetValue(descriptor.ControlType, out FormControlDescriptor? existing))
        {
            if (existing.IsBuiltIn && descriptor.ReplaceBuiltInRuntime && descriptor.RuntimeComponentType is not null)
            {
                _controls[existing.ControlType] = existing with
                {
                    RuntimeComponentType = descriptor.RuntimeComponentType,
                    ReplaceBuiltInRuntime = true,
                };
                return this;
            }

            throw new InvalidOperationException($"A form control with type '{descriptor.ControlType}' is already registered.");
        }

        _controls.Add(descriptor.ControlType, descriptor);
        return this;
    }

    internal FormControlRegistry Build() => new(_controls.Values);

    private static void ValidateDescriptor(FormControlDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        if (string.IsNullOrWhiteSpace(descriptor.ControlType))
            throw new ArgumentException("Control type is required.", nameof(descriptor));
        if (string.IsNullOrWhiteSpace(descriptor.DisplayName))
            throw new ArgumentException("Display name is required.", nameof(descriptor));
        if (descriptor.DefaultWidth <= 0)
            throw new ArgumentException("Default width must be greater than zero.", nameof(descriptor));
        if (descriptor.DefaultHeight <= 0)
            throw new ArgumentException("Default height must be greater than zero.", nameof(descriptor));

        FormControlDescriptor.ValidateComponentType(descriptor.DesignerPreviewComponentType, nameof(descriptor.DesignerPreviewComponentType));
        FormControlDescriptor.ValidateComponentType(descriptor.RuntimeComponentType, nameof(descriptor.RuntimeComponentType));
        FormControlDescriptor.ValidateComponentType(descriptor.PropertyEditorComponentType, nameof(descriptor.PropertyEditorComponentType));

        foreach (FormControlPropertyDescriptor property in descriptor.PropertyDescriptors)
        {
            if (string.IsNullOrWhiteSpace(property.Name))
                throw new ArgumentException("Property descriptor name is required.", nameof(descriptor));
            if (string.IsNullOrWhiteSpace(property.Label))
                throw new ArgumentException("Property descriptor label is required.", nameof(descriptor));
            if (property.Editor == FormControlPropertyEditor.Select && property.Options.Count == 0)
                throw new ArgumentException($"Select property '{property.Name}' must define options.", nameof(descriptor));
        }
    }
}
