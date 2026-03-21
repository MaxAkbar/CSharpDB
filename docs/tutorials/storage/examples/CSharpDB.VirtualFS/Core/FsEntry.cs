namespace CSharpDB.VirtualFS;

/// <summary>
/// Metadata stored for every file-system entry (directory, file, or shortcut).
/// File content is stored separately from metadata so large files don't bloat the metadata tree.
/// </summary>
public sealed class FsEntry
{
    public long Id { get; set; }
    public long ParentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public EntryKind Kind { get; set; }
    public long Size { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime ModifiedUtc { get; set; }

    /// <summary>For shortcuts: the ID of the entry this shortcut points to.</summary>
    public long? TargetId { get; set; }

    /// <summary>Optional user-defined attributes (tags, permissions, etc.).</summary>
    public Dictionary<string, string>? Attributes { get; set; }
}