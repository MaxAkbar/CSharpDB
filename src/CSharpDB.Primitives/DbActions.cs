namespace CSharpDB.Primitives;

public enum DbActionKind
{
    RunCommand,
    SetFieldValue,
    ShowMessage,
    Stop,
}

public sealed record DbActionSequence(
    IReadOnlyList<DbActionStep> Steps,
    string? Name = null);

public sealed record DbActionStep(
    DbActionKind Kind,
    string? CommandName = null,
    string? Target = null,
    object? Value = null,
    string? Message = null,
    IReadOnlyDictionary<string, object?>? Arguments = null,
    bool StopOnFailure = true);
