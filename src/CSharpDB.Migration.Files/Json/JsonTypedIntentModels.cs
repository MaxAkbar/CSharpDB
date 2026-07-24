using System.Collections.ObjectModel;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Typed interpretations available to an explicitly bound JSON column.</summary>
public enum JsonTypedValueCodec
{
    BinaryBase64,
    DecimalString,
    DecimalNumber,
    GuidD,
    DateCSharpDbText,
    TimeCSharpDbText,
    DateTimeCSharpDbText,
    DateTimeOffsetCSharpDbText,
    Int64String,
    UInt64String,
}

/// <summary>
/// One ordinal-addressed typed interpretation. The exact decoded property
/// name prevents discovery-order changes from silently retargeting it.
/// </summary>
public sealed record JsonTypedColumnIntent
{
    [JsonPropertyOrder(0)]
    public required int ColumnIndex { get; init; }

    [JsonPropertyOrder(1)]
    public required string ExpectedPropertyName { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonTypedValueCodec Codec { get; init; }

    [JsonPropertyOrder(3)]
    public bool? Nullable { get; init; }

    [JsonPropertyOrder(4)]
    public JsonMissingPropertyPolicy MissingPolicy { get; init; } =
        JsonMissingPropertyPolicy.Reject;

    [JsonPropertyOrder(5)]
    public int? Precision { get; init; }

    [JsonPropertyOrder(6)]
    public int? Scale { get; init; }
}

/// <summary>Bounded policy serialized into a typed JSON intent sidecar.</summary>
public sealed record JsonTypedIntentOptions
{
    public IReadOnlyList<JsonTypedColumnIntent> Columns { get; init; } = [];

    public int MaxDecodedBinaryBytes { get; init; } =
        12 * 1024 * 1024;

    public int MaxDecimalDigits { get; init; } = 1024 * 1024;
}

/// <summary>Trust policy for opening a typed JSON intent sidecar.</summary>
public sealed record JsonTypedIntentOpenOptions
{
    /// <summary>
    /// Optional independently retained SHA-256 of the exact canonical sidecar
    /// bytes. Hashes are integrity pins, not signatures.
    /// </summary>
    public string? ExpectedManifestDigest { get; init; }
}

/// <summary>Stable rules raised while publishing or opening an intent sidecar.</summary>
public static class JsonTypedIntentRules
{
    public const string InvalidFormat =
        "MIG-JSON-INTENT-FORMAT-001";

    public const string IntegrityMismatch =
        "MIG-JSON-INTENT-INTEGRITY-001";

    public const string SourceMismatch =
        "MIG-JSON-INTENT-SOURCE-001";

    public const string PolicyMismatch =
        "MIG-JSON-INTENT-POLICY-001";

    public const string SizeLimitExceeded =
        "MIG-JSON-INTENT-LIMIT-001";

    public const string UnsafePath =
        "MIG-JSON-INTENT-PATH-001";
}

public sealed class JsonTypedIntentException : IOException
{
    internal JsonTypedIntentException(
        string ruleId,
        string message)
        : base(message)
    {
        RuleId = ruleId;
    }

    internal JsonTypedIntentException(
        string ruleId,
        string message,
        Exception innerException)
        : base(message, innerException)
    {
        RuleId = ruleId;
    }

    public string RuleId { get; }
}

/// <summary>
/// One canonical, source-bound, value-free typed intent manifest.
/// </summary>
public sealed class JsonTypedIntentManifest
{
    private readonly byte[] canonicalUtf8Bytes;

    internal JsonTypedIntentManifest(
        string manifestDigest,
        JsonTypedIntentManifestPayload payload,
        byte[] canonicalUtf8Bytes)
    {
        ManifestDigest = manifestDigest;
        SnapshotIdentity = payload.Source.SnapshotIdentity;
        ContentDigest = payload.Source.ContentDigest;
        ContentLength = payload.Source.ContentLength;
        SourceIdentity = payload.Source.Identity;
        SourceFingerprint = payload.Source.Fingerprint;
        OptionsDigest = payload.Source.OptionsDigest;
        MaxDecodedBinaryBytes =
            payload.Limits.MaxDecodedBinaryBytes;
        MaxDecimalDigits = payload.Limits.MaxDecimalDigits;
        Columns = Array.AsReadOnly(
            payload.Columns
                .Select(JsonTypedIntentSidecar.CloneIntent)
                .ToArray());
        this.canonicalUtf8Bytes =
            canonicalUtf8Bytes.ToArray();
        Payload = payload;
    }

    public string ManifestDigest { get; }

    public string SnapshotIdentity { get; }

    public string ContentDigest { get; }

    public long ContentLength { get; }

    public string SourceIdentity { get; }

    public string SourceFingerprint { get; }

    public string OptionsDigest { get; }

    public int MaxDecodedBinaryBytes { get; }

    public int MaxDecimalDigits { get; }

    public ReadOnlyCollection<JsonTypedColumnIntent> Columns { get; }

    public byte[] ToCanonicalUtf8Bytes() =>
        canonicalUtf8Bytes.ToArray();

    internal JsonTypedIntentManifestPayload Payload { get; }
}
