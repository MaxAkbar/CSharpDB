using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Models;

public enum ControlEventKind
{
    OnClick,
    OnChange,
    OnGotFocus,
    OnLostFocus,
}

public sealed record ControlEventBinding(
    ControlEventKind Event,
    string CommandName,
    IReadOnlyDictionary<string, object?>? Arguments = null,
    bool StopOnFailure = true,
    DbActionSequence? ActionSequence = null);
