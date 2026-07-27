using System.Text.Json.Serialization;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Stable identifiers for the strict JSON and NDJSON export artifact.
/// These values are serialized and form part of the compatibility boundary.
/// </summary>
public static class JsonExportContracts
{
    public const string ManifestFormat =
        "csharpdb-json-export-manifest/v1";

    public const string Schema =
        "csharpdb-json-export-schema/v1";

    public const string SourceKind = "csharpdb";

    public const string RowOrder =
        "csharpdb-table-rowid-ascending/v1";

    public const string OrderedContentDigest =
        "csharpdb-json-export-ordered-content/v1";

    public const string OrderedContentDigestDomain =
        "CSDBJSON1";

    public const string Encoding = "utf-8";

    public const string Culture = "invariant";

    public const string Newline = "lf";

    public const string PropertyOrder =
        "schema-order/v1";

    public const string NullEncoding = "json-null/v1";

    public const string IntegerValueEncoding =
        "int64-json-number/v1";

    public const string RealValueEncoding =
        "finite-binary64-json-number-roundtrip/v1";

    public const string TextValueEncoding =
        "strict-json-string/v1";

    public const string TextEscape =
        TextValueEncoding;

    public const string BlobValueEncoding =
        "rfc4648-base64-padded-json-string/v1";

    /// <summary>
    /// Largest decoded BLOB whose padded base64 representation fits within
    /// the strict JSON reader's absolute string-byte ceiling.
    /// </summary>
    public const int MaximumSupportedDecodedBlobBytes =
        JsonInputContracts.MaximumStringBytes / 4 * 3;

    public const string Canonicalization =
        CanonicalRowCodec.CanonicalizationId;

    public const string CanonicalizationContractDigest =
        CanonicalRowCodec.ContractHashHex;
}

/// <summary>
/// V1 has one lossless compatibility profile.
/// </summary>
public enum JsonExportProfile
{
    LosslessV1,
}

/// <summary>
/// Physical top-level framing selected for the exported table rows.
/// </summary>
public enum JsonExportFraming
{
    RootArray,
    Ndjson,
}

/// <summary>CSharpDB storage type retained by one exported column.</summary>
public enum JsonExportDatabaseType
{
    Integer,
    Real,
    Text,
    Blob,
}

/// <summary>
/// Canonical typed manifest for one table exported as JSON or NDJSON.
/// Paths, timestamps, host identity, random identifiers, and row values are
/// deliberately excluded.
/// </summary>
public sealed record JsonExportManifest
{
    [JsonPropertyOrder(0)]
    public required JsonExportProfile Profile { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonExportSourceManifest Source { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportTableManifest Table { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportFormatManifest Json { get; init; }

    [JsonPropertyOrder(4)]
    public required JsonExportContentManifest Content { get; init; }
}

/// <summary>Exact retained CSharpDB snapshot evidence.</summary>
public sealed record JsonExportSourceManifest
{
    [JsonPropertyOrder(0)]
    public required string Kind { get; init; }

    [JsonPropertyOrder(1)]
    public required string Version { get; init; }

    [JsonPropertyOrder(2)]
    public required long SnapshotByteLength { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportHashManifest SnapshotDigest { get; init; }
}

/// <summary>
/// Ordered physical table schema and deterministic row traversal contract.
/// </summary>
public sealed record JsonExportTableManifest
{
    [JsonPropertyOrder(0)]
    public required string Name { get; init; }

    [JsonPropertyOrder(1)]
    public required string SchemaContract { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportHashManifest SchemaDigest { get; init; }

    [JsonPropertyOrder(3)]
    public required string RowOrder { get; init; }

    [JsonPropertyOrder(4)]
    public required IReadOnlyList<JsonExportColumnManifest> Columns
    {
        get;
        init;
    }
}

/// <summary>
/// One source column and its exact JSON object-property representation.
/// </summary>
public sealed record JsonExportColumnManifest
{
    [JsonPropertyOrder(0)]
    public required int Ordinal { get; init; }

    [JsonPropertyOrder(1)]
    public required string SourceName { get; init; }

    [JsonPropertyOrder(2)]
    public required string PropertyName { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportDatabaseType DatabaseType { get; init; }

    [JsonPropertyOrder(4)]
    public required bool Nullable { get; init; }

    [JsonPropertyOrder(5)]
    public required string ValueEncoding { get; init; }

    /// <summary>
    /// Per-value decoded byte ceiling for BLOB columns. Non-BLOB columns use
    /// zero because their scalar codecs have no decoded binary payload.
    /// </summary>
    [JsonPropertyOrder(6)]
    public required int MaximumDecodedBytes { get; init; }
}

/// <summary>
/// Fixed compact JSON v1 representation and retained resource ceilings.
/// </summary>
public sealed record JsonExportFormatManifest
{
    [JsonPropertyOrder(0)]
    public required string Encoding { get; init; }

    [JsonPropertyOrder(1)]
    public required bool HasByteOrderMark { get; init; }

    [JsonPropertyOrder(2)]
    public required string Culture { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportFraming Framing { get; init; }

    [JsonPropertyOrder(4)]
    public required bool Compact { get; init; }

    [JsonPropertyOrder(5)]
    public required string PropertyOrder { get; init; }

    [JsonPropertyOrder(6)]
    public required string Newline { get; init; }

    [JsonPropertyOrder(7)]
    public required bool HasFinalNewline { get; init; }

    [JsonPropertyOrder(8)]
    public required string NullEncoding { get; init; }

    [JsonPropertyOrder(9)]
    public required string TextEscape { get; init; }

    [JsonPropertyOrder(10)]
    public required long MaxDataBytes { get; init; }

    [JsonPropertyOrder(11)]
    public required int MaximumDecodedBlobBytes { get; init; }

    [JsonPropertyOrder(12)]
    public required int MaximumValueBytes { get; init; }

    [JsonPropertyOrder(13)]
    public required int MaximumStringBytes { get; init; }

    [JsonPropertyOrder(14)]
    public required int MaximumPropertyNameBytes { get; init; }

    [JsonPropertyOrder(15)]
    public required int MaximumPropertiesPerObject { get; init; }
}

/// <summary>Physical and logical evidence for the completed export.</summary>
public sealed record JsonExportContentManifest
{
    [JsonPropertyOrder(0)]
    public required long RowCount { get; init; }

    [JsonPropertyOrder(1)]
    public required long DataByteLength { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportHashManifest DataDigest { get; init; }

    [JsonPropertyOrder(3)]
    public required string Canonicalization { get; init; }

    [JsonPropertyOrder(4)]
    public required string CanonicalizationContractDigest { get; init; }

    [JsonPropertyOrder(5)]
    public required string Aggregation { get; init; }

    [JsonPropertyOrder(6)]
    public required JsonExportHashManifest SourceLogicalDigest { get; init; }

    [JsonPropertyOrder(7)]
    public required JsonExportHashManifest ExportedLogicalDigest { get; init; }
}

/// <summary>One lowercase SHA-256 value and its fixed algorithm name.</summary>
public sealed record JsonExportHashManifest
{
    public const string Sha256Algorithm = "sha256";

    [JsonPropertyOrder(0)]
    public required string Algorithm { get; init; }

    [JsonPropertyOrder(1)]
    public required string Value { get; init; }
}
