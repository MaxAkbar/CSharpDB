using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class FormControlRegistry : IFormControlRegistry
{
    private readonly Dictionary<string, FormControlDescriptor> _controls;
    private readonly IReadOnlyList<FormControlDescriptor> _orderedControls;

    internal FormControlRegistry(IEnumerable<FormControlDescriptor> controls)
    {
        _orderedControls = controls
            .OrderBy(control => control.ToolboxGroupOrder)
            .ThenBy(control => control.ToolboxOrder)
            .ThenBy(control => control.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        _controls = _orderedControls.ToDictionary(
            control => control.ControlType,
            control => control,
            StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyList<FormControlDescriptor> Controls => _orderedControls;

    public bool TryGetControl(string controlType, out FormControlDescriptor descriptor)
        => _controls.TryGetValue(controlType, out descriptor!);

    public IReadOnlyList<FormControlDescriptor> GetToolboxControls()
        => _orderedControls
            .Where(control => control.ShowInToolbox)
            .ToArray();
}
