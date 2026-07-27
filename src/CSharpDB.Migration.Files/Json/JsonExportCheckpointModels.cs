using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Stable identifiers for the durable JSON and NDJSON export checkpoint
/// contract.
/// </summary>
public static class JsonExportCheckpointContracts
{
    public const string Format =
        "csharpdb-json-export-checkpoint/v1";

    public const string BindingContract =
        "csharpdb-json-export-checkpoint-binding/v1";

    public const string LogicalPrefixAggregation =
        "csharpdb-json-export-ordered-content-prefix/v1";

    public const string RetainedSnapshotIdentityPrefix =
        "csharpdb-retained-snapshot/v1:";
}

/// <summary>Durable phase represented by one JSON export checkpoint.</summary>
public enum JsonExportCheckpointPhase
{
    Writing,
    DataComplete,
}

/// <summary>
/// Canonical durable evidence for one prepared JSON or NDJSON output
/// generation.
/// </summary>
public sealed record JsonExportCheckpoint
{
    [JsonPropertyOrder(0)]
    public required long Generation { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonExportCheckpointPhase Phase { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportCheckpointBinding Binding { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportHashManifest BindingDigest { get; init; }

    [JsonPropertyOrder(4)]
    public required JsonExportCheckpointProgress Progress { get; init; }

    [JsonPropertyOrder(5)]
    public JsonExportCheckpointCompletion? Completion { get; init; }
}

/// <summary>
/// Immutable source, schema, codec, framing, and resource-policy binding.
/// </summary>
public sealed record JsonExportCheckpointBinding
{
    [JsonPropertyOrder(0)]
    public required JsonExportProfile Profile { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonExportSourceManifest Source { get; init; }

    [JsonPropertyOrder(2)]
    public required string SourceSnapshotIdentity { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportTableManifest Table { get; init; }

    [JsonPropertyOrder(4)]
    public required JsonExportFormatManifest Json { get; init; }
}

/// <summary>
/// Evidence for the last complete, durable JSON object boundary. Logical
/// prefix digests exclude the ordered digest's final row-count suffix.
/// </summary>
public sealed record JsonExportCheckpointProgress
{
    [JsonPropertyOrder(0)]
    public required long CompletedRowCount { get; init; }

    [JsonPropertyOrder(1)]
    public long? LastCompletedRowId { get; init; }

    [JsonPropertyOrder(2)]
    public required long DataPrefixByteLength { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonExportHashManifest DataPrefixDigest { get; init; }

    [JsonPropertyOrder(4)]
    public required string LogicalPrefixAggregation { get; init; }

    [JsonPropertyOrder(5)]
    public required JsonExportHashManifest SourceLogicalRowHashPrefixDigest
    {
        get;
        init;
    }

    [JsonPropertyOrder(6)]
    public required JsonExportHashManifest ExportedLogicalRowHashPrefixDigest
    {
        get;
        init;
    }
}

/// <summary>
/// Final ordered logical and manifest evidence present only after source EOF.
/// </summary>
public sealed record JsonExportCheckpointCompletion
{
    [JsonPropertyOrder(0)]
    public required JsonExportHashManifest SourceLogicalDigest { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonExportHashManifest ExportedLogicalDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required string ManifestDigest { get; init; }
}
