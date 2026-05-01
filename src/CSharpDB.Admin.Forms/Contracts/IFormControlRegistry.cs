using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IFormControlRegistry
{
    IReadOnlyList<FormControlDescriptor> Controls { get; }

    bool TryGetControl(string controlType, out FormControlDescriptor descriptor);

    IReadOnlyList<FormControlDescriptor> GetToolboxControls();
}
