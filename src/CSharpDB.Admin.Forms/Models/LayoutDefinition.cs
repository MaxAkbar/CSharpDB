namespace CSharpDB.Admin.Forms.Models;

public sealed record LayoutDefinition(
    string LayoutMode,
    double GridSize,
    bool SnapToGrid,
    IReadOnlyList<Breakpoint> Breakpoints);
