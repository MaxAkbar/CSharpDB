namespace CSharpDB.ImportExport.Models;

/// <summary>
/// Cryptographic digests of the native archive data sections. Digest text is
/// canonical lowercase hexadecimal; uppercase hexadecimal is not accepted.
/// </summary>
public sealed class TableArchiveSectionDigests
{
    public const string Sha256Algorithm = "sha256";
    public const string LowercaseHexEncoding = "lowercase-hex";

    public string Algorithm { get; init; } = Sha256Algorithm;
    public string Encoding { get; init; } = LowercaseHexEncoding;
    public required string Schema { get; init; }
    public required string Rows { get; init; }
    public required string PhysicalIndex { get; init; }
}
