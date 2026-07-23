using System.Text.Json.Serialization;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Stable identifiers for the strict CSV export artifact. These values are
/// serialized into the manifest and are part of its compatibility boundary.
/// </summary>
public static class CsvExportContracts
{
    public const string ManifestFormat = "csharpdb-csv-export-manifest/v1";
    public const string Schema = "csharpdb-csv-export-schema/v1";
    public const string SourceKind = "csharpdb";
    public const string RowOrder = "csharpdb-table-rowid-ascending/v1";
    public const string OrderedContentDigest = "csharpdb-csv-export-ordered-content/v1";
    public const string OrderedContentDigestDomain = "CSDBCSV1";

    public const string Encoding = "utf-8";
    public const string Culture = "invariant";
    public const string Newline = "crlf";
    public const string NullToken = "\\N";
    public const string TextEscape = "rfc4180-quote-null-token-literal/v1";

    public const string IntegerValueEncoding = "int64-invariant-decimal/v1";
    public const string RealValueEncoding = "finite-binary64-roundtrip/v1";
    public const string TextValueEncoding = "strict-utf8-text/v1";
    public const string BlobValueEncoding = "rfc4648-base64-padded/v1";

    /// <summary>
    /// Largest decoded BLOB that remains representable within the strict CSV
    /// reader's absolute 16 Mi-character field ceiling.
    /// </summary>
    public const int MaximumSupportedDecodedBlobBytes =
        CsvReaderOptions.MaximumSupportedFieldCharacters / 4 * 3;

    public const string SpreadsheetFormulaRuleId = "MIG-CSV-EXPORT-FORMULA-001";
    public const string SpreadsheetFormulaTransform =
        "spreadsheet-formula-prefix-apostrophe/v1";

    public const string Canonicalization = CanonicalRowCodec.CanonicalizationId;
    public const string CanonicalizationContractDigest = CanonicalRowCodec.ContractHashHex;
}

/// <summary>
/// Lossless output is the default compatibility profile. Spreadsheet-safe
/// output is separately named because prefixing formula-like text changes data.
/// </summary>
public enum CsvExportProfile
{
    LosslessV1,
    SpreadsheetSafeLossyV1,
}

/// <summary>CSharpDB storage type retained by one exported column.</summary>
public enum CsvExportDatabaseType
{
    Integer,
    Real,
    Text,
    Blob,
}

/// <summary>
/// Canonical typed sidecar for one table export. The manifest deliberately
/// excludes paths, timestamps, host identity, random identifiers, and values.
/// </summary>
public sealed record CsvExportManifest
{
    [JsonPropertyOrder(0)]
    public required CsvExportProfile Profile { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvExportSourceManifest Source { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportTableManifest Table { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportFormatManifest Csv { get; init; }

    [JsonPropertyOrder(4)]
    public required CsvExportContentManifest Content { get; init; }

    [JsonPropertyOrder(5)]
    public CsvExportLossyTransformManifest? LossyTransform { get; init; }
}

public sealed record CsvExportSourceManifest
{
    [JsonPropertyOrder(0)]
    public required string Kind { get; init; }

    [JsonPropertyOrder(1)]
    public required string Version { get; init; }

    [JsonPropertyOrder(2)]
    public required long SnapshotByteLength { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportHashManifest SnapshotDigest { get; init; }
}

public sealed record CsvExportTableManifest
{
    [JsonPropertyOrder(0)]
    public required string Name { get; init; }

    [JsonPropertyOrder(1)]
    public required string SchemaContract { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportHashManifest SchemaDigest { get; init; }

    [JsonPropertyOrder(3)]
    public required string RowOrder { get; init; }

    [JsonPropertyOrder(4)]
    public required IReadOnlyList<CsvExportColumnManifest> Columns { get; init; }
}

public sealed record CsvExportColumnManifest
{
    [JsonPropertyOrder(0)]
    public required int Ordinal { get; init; }

    /// <summary>Exact source schema name.</summary>
    [JsonPropertyOrder(1)]
    public required string SourceName { get; init; }

    /// <summary>
    /// Exact header written to the CSV. It equals <see cref="SourceName"/> for
    /// lossless output and may differ under the explicit lossy profile.
    /// </summary>
    [JsonPropertyOrder(2)]
    public required string Header { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportDatabaseType DatabaseType { get; init; }

    [JsonPropertyOrder(4)]
    public required bool Nullable { get; init; }

    [JsonPropertyOrder(5)]
    public required string ValueEncoding { get; init; }

    /// <summary>
    /// Per-value decoded byte ceiling for BLOB columns. Non-BLOB columns use
    /// zero because their bounds are enforced by their scalar codec.
    /// </summary>
    [JsonPropertyOrder(6)]
    public required int MaximumDecodedBytes { get; init; }
}

/// <summary>Exact fixed RFC 4180-compatible CSV v1 settings.</summary>
public sealed record CsvExportFormatManifest
{
    [JsonPropertyOrder(0)]
    public required string Encoding { get; init; }

    [JsonPropertyOrder(1)]
    public required bool HasByteOrderMark { get; init; }

    [JsonPropertyOrder(2)]
    public required string Culture { get; init; }

    [JsonPropertyOrder(3)]
    public required string Delimiter { get; init; }

    [JsonPropertyOrder(4)]
    public required char Quote { get; init; }

    [JsonPropertyOrder(5)]
    public required string Newline { get; init; }

    [JsonPropertyOrder(6)]
    public required bool HasHeaderRecord { get; init; }

    [JsonPropertyOrder(7)]
    public required bool HasFinalNewline { get; init; }

    [JsonPropertyOrder(8)]
    public required string NullToken { get; init; }

    [JsonPropertyOrder(9)]
    public required bool NullTokenMatchesQuotedFields { get; init; }

    [JsonPropertyOrder(10)]
    public required string TextEscape { get; init; }
}

public sealed record CsvExportContentManifest
{
    [JsonPropertyOrder(0)]
    public required long RowCount { get; init; }

    [JsonPropertyOrder(1)]
    public required long DataByteLength { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportHashManifest DataDigest { get; init; }

    [JsonPropertyOrder(3)]
    public required string Canonicalization { get; init; }

    [JsonPropertyOrder(4)]
    public required string CanonicalizationContractDigest { get; init; }

    [JsonPropertyOrder(5)]
    public required string Aggregation { get; init; }

    [JsonPropertyOrder(6)]
    public required CsvExportHashManifest SourceLogicalDigest { get; init; }

    [JsonPropertyOrder(7)]
    public required CsvExportHashManifest ExportedLogicalDigest { get; init; }
}

/// <summary>
/// Aggregate-only evidence for the explicitly lossy spreadsheet profile.
/// Cell values are intentionally absent.
/// </summary>
public sealed record CsvExportLossyTransformManifest
{
    [JsonPropertyOrder(0)]
    public required string RuleId { get; init; }

    [JsonPropertyOrder(1)]
    public required string Algorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required int TransformedHeaderCount { get; init; }

    [JsonPropertyOrder(3)]
    public required long TransformedRowCount { get; init; }

    [JsonPropertyOrder(4)]
    public required long TransformedCellCount { get; init; }
}

public sealed record CsvExportHashManifest
{
    public const string Sha256Algorithm = "sha256";

    [JsonPropertyOrder(0)]
    public required string Algorithm { get; init; }

    [JsonPropertyOrder(1)]
    public required string Value { get; init; }
}
