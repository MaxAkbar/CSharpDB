namespace CSharpDB.Storage.Diagnostics;

public sealed class FileHeaderReport
{
    public long FileLengthBytes { get; init; }
    public int PhysicalPageCount { get; init; }

    public string Magic { get; init; } = string.Empty;
    public bool MagicValid { get; init; }

    public int Version { get; init; }
    public bool VersionValid { get; init; }

    public int PageSize { get; init; }
    public bool PageSizeValid { get; init; }

    public uint DeclaredPageCount { get; init; }
    public bool DeclaredPageCountMatchesPhysical { get; init; }

    public uint SchemaRootPage { get; init; }
    public uint FreelistHead { get; init; }
    public uint ChangeCounter { get; init; }
}
