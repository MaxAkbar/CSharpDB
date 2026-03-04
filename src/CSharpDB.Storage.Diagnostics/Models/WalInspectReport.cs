namespace CSharpDB.Storage.Diagnostics;

public sealed class WalInspectReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required string WalPath { get; init; }
    public bool Exists { get; init; }

    public long FileLengthBytes { get; init; }
    public int FullFrameCount { get; init; }
    public int CommitFrameCount { get; init; }
    public int TrailingBytes { get; init; }

    public string Magic { get; init; } = string.Empty;
    public bool MagicValid { get; init; }
    public int Version { get; init; }
    public bool VersionValid { get; init; }
    public int PageSize { get; init; }
    public bool PageSizeValid { get; init; }

    public uint Salt1 { get; init; }
    public uint Salt2 { get; init; }

    public required List<IntegrityIssue> Issues { get; init; }
}
