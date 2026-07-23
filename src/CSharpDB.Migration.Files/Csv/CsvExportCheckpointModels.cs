using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Stable identifiers for the durable CSV export checkpoint contract.
/// </summary>
public static class CsvExportCheckpointContracts
{
    public const string Format = "csharpdb-csv-export-checkpoint/v1";
    public const string BindingContract = "csharpdb-csv-export-checkpoint-binding/v1";
    public const string LogicalPrefixAggregation =
        "csharpdb-csv-export-ordered-content-prefix/v1";
    public const string RetainedSnapshotIdentityPrefix =
        "csharpdb-retained-snapshot/v1:";
}

/// <summary>Durable phase represented by one CSV export checkpoint.</summary>
public enum CsvExportCheckpointPhase
{
    Writing,
    DataComplete,
}

/// <summary>
/// Canonical durable evidence for one prepared CSV output generation.
/// </summary>
public sealed record CsvExportCheckpoint
{
    [JsonPropertyOrder(0)]
    public required long Generation { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvExportCheckpointPhase Phase { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportCheckpointBinding Binding { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportHashManifest BindingDigest { get; init; }

    [JsonPropertyOrder(4)]
    public required CsvExportCheckpointProgress Progress { get; init; }

    [JsonPropertyOrder(5)]
    public CsvExportCheckpointCompletion? Completion { get; init; }
}

/// <summary>
/// Immutable source, schema, codec, profile, and resource-policy binding.
/// </summary>
public sealed record CsvExportCheckpointBinding
{
    [JsonPropertyOrder(0)]
    public required CsvExportProfile Profile { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvExportSourceManifest Source { get; init; }

    [JsonPropertyOrder(2)]
    public required string SourceSnapshotIdentity { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportTableManifest Table { get; init; }

    [JsonPropertyOrder(4)]
    public required CsvExportFormatManifest Csv { get; init; }

    [JsonPropertyOrder(5)]
    public required long MaxDataBytes { get; init; }

    [JsonPropertyOrder(6)]
    public required int MaximumDecodedBlobBytes { get; init; }
}

/// <summary>
/// Evidence for the last complete, durable CSV record boundary.
/// Logical-prefix digests exclude the ordered digest's final row-count suffix.
/// </summary>
public sealed record CsvExportCheckpointProgress
{
    [JsonPropertyOrder(0)]
    public required long CompletedRowCount { get; init; }

    [JsonPropertyOrder(1)]
    public long? LastCompletedRowId { get; init; }

    [JsonPropertyOrder(2)]
    public required long DataPrefixByteLength { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvExportHashManifest DataPrefixDigest { get; init; }

    [JsonPropertyOrder(4)]
    public required string LogicalPrefixAggregation { get; init; }

    [JsonPropertyOrder(5)]
    public required CsvExportHashManifest SourceLogicalRowHashPrefixDigest { get; init; }

    [JsonPropertyOrder(6)]
    public required CsvExportHashManifest ExportedLogicalRowHashPrefixDigest { get; init; }

    [JsonPropertyOrder(7)]
    public required long TransformedRowCount { get; init; }

    [JsonPropertyOrder(8)]
    public required long TransformedCellCount { get; init; }
}

/// <summary>
/// Final ordered logical and manifest evidence present only after source EOF.
/// </summary>
public sealed record CsvExportCheckpointCompletion
{
    [JsonPropertyOrder(0)]
    public required CsvExportHashManifest SourceLogicalDigest { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvExportHashManifest ExportedLogicalDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required string ManifestDigest { get; init; }
}
