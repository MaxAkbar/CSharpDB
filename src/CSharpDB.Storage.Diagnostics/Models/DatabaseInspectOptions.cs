namespace CSharpDB.Storage.Diagnostics;

public sealed class DatabaseInspectOptions
{
    public DatabaseInspectionMode Mode { get; init; }
    public bool IncludePages { get; init; }
}
