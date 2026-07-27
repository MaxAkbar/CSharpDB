namespace CSharpDB.Admin.ImportExport.Services;

/// <summary>
/// Resource limits for the Admin native table-archive restore workflow.
/// </summary>
public sealed record TableArchiveRestoreOptions
{
    /// <summary>
    /// Directory used for the immutable archive snapshot and checksum spill
    /// workspace. The operating-system temporary directory is used by default.
    /// </summary>
    public string? ScratchDirectory { get; init; }

    /// <summary>
    /// Maximum archive bytes copied into the immutable restore snapshot.
    /// </summary>
    public long MaxArchiveSnapshotBytes { get; init; } = 4L * 1024 * 1024 * 1024;

    /// <summary>
    /// Maximum live bytes used by canonical checksum spill files.
    /// </summary>
    public long MaxValidationSpillBytes { get; init; } = 4L * 1024 * 1024 * 1024;
}
