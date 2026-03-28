namespace CSharpDB.Admin.Models;

public sealed class ContextMenuItem
{
    public string Label { get; init; } = string.Empty;
    public string? Icon { get; init; }
    public Action? OnClick { get; init; }
    public bool IsDanger { get; init; }
    public bool IsSeparator { get; init; }

    public static ContextMenuItem Separator() => new() { IsSeparator = true };
}
