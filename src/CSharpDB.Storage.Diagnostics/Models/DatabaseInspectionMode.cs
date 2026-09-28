namespace CSharpDB.Storage.Diagnostics;

public enum DatabaseInspectionMode
{
    Full = 0,
    /// <summary>Read persisted file metadata only, without scanning database pages or WAL frames.</summary>
    Summary = 1,
    /// <summary>Run database and index checks against the same parsed snapshot.</summary>
    FullWithIndexes = 2,
}
