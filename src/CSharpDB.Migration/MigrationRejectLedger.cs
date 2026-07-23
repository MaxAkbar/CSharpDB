using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace CSharpDB.Migration;

/// <summary>
/// One target-owned reject-ledger entry. The plan digest and source position
/// bind the canonical rejected-row payload to its migration batch.
/// </summary>
public sealed record MigrationRejectLedgerEntry
{
    public required string PlanDigest { get; init; }

    public required string SourceObjectId { get; init; }

    public long BatchOrdinal { get; init; }

    public required MigrationRejectedRow RejectedRow { get; init; }

    public int RawValueByteCount { get; init; }

    public int CanonicalEntryByteCount { get; init; }
}

/// <summary>
/// Optional target capability for streaming the authoritative reject ledger
/// in canonical source-object, batch, and source-row order.
/// </summary>
public interface IMigrationRejectLedgerTarget
{
    IAsyncEnumerable<MigrationRejectLedgerEntry> ReadRejectLedgerAsync(
        string planDigest,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Canonical JSON codec shared by target ledger storage and later artifact
/// projection. Noncanonical JSON is rejected on read rather than normalized.
/// </summary>
public static class MigrationRejectLedgerCodec
{
    public const string EntryFormat = "csharpdb-migration-reject-entry/v1";

    public const string ArtifactFormat = "csharpdb-migration-reject-artifact/v1";

    public const string RawValueEvidenceName = "rawValue";

    public const int MaximumCanonicalEvidenceBytes = 128 * 1024;

    public const int MaximumCanonicalEntryBytes = 256 * 1024;

    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    public static int MinimumCanonicalArtifactBytes { get; } =
        GetArtifactHeaderByteCount(new string('0', 64));

    public static string SerializeEvidence(IReadOnlyList<MigrationRejectEvidence> evidence)
    {
        ArgumentNullException.ThrowIfNull(evidence);
        ValidateEvidence(evidence);

        var output = new ArrayBufferWriter<byte>();
        using (var writer = CreateWriter(output))
        {
            WriteEvidence(writer, evidence);
        }

        if (output.WrittenCount > MaximumCanonicalEvidenceBytes)
            throw new InvalidDataException("Migration reject evidence JSON exceeds the contract ceiling.");
        return StrictUtf8.GetString(output.WrittenSpan);
    }

    public static IReadOnlyList<MigrationRejectEvidence> DeserializeEvidence(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        int byteCount = StrictByteCount(json, "Migration reject evidence JSON");
        if (byteCount > MaximumCanonicalEvidenceBytes)
            throw new InvalidDataException("Migration reject evidence JSON exceeds the contract ceiling.");

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json, new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = 8,
            });
        }
        catch (JsonException error)
        {
            throw new InvalidDataException("Migration reject evidence JSON is invalid.", error);
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Array)
                throw new InvalidDataException("Migration reject evidence JSON must be an array.");

            var evidence = new List<MigrationRejectEvidence>();
            foreach (JsonElement item in document.RootElement.EnumerateArray())
            {
                if (evidence.Count == MigrationRejectContract.MaximumEvidenceEntriesPerRow)
                {
                    throw new InvalidDataException(
                        "Migration reject evidence count exceeds the contract ceiling.");
                }
                evidence.Add(ReadEvidenceItem(item));
            }

            string canonical = SerializeEvidence(evidence);
            if (!string.Equals(canonical, json, StringComparison.Ordinal))
                throw new InvalidDataException("Migration reject evidence JSON is not canonical.");
            return evidence.ToArray();
        }
    }

    public static string SerializeEntry(
        string sourceObjectId,
        long batchOrdinal,
        MigrationRejectedRow rejectedRow)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceObjectId);
        ArgumentNullException.ThrowIfNull(rejectedRow);
        if (!MigrationRejectContract.IsBoundedIdentifier(sourceObjectId))
        {
            throw new ArgumentException(
                "The source object ID is not a bounded migration identifier.",
                nameof(sourceObjectId));
        }
        if (batchOrdinal < 0)
            throw new ArgumentOutOfRangeException(nameof(batchOrdinal));
        MigrationRejectDigest.ValidateRejectedRows([rejectedRow]);

        var output = new ArrayBufferWriter<byte>();
        using (var writer = CreateWriter(output))
        {
            writer.WriteStartObject();
            writer.WriteString("format", EntryFormat);
            writer.WriteString("sourceObjectId", sourceObjectId);
            writer.WriteNumber("batchOrdinal", batchOrdinal);
            writer.WriteNumber("sourceRowOrdinal", rejectedRow.SourceRowOrdinal);
            writer.WriteString("ruleId", rejectedRow.RuleId);
            if (rejectedRow.ColumnObjectId is null)
                writer.WriteNull("columnObjectId");
            else
                writer.WriteString("columnObjectId", rejectedRow.ColumnObjectId);
            writer.WritePropertyName("evidence");
            WriteEvidence(writer, rejectedRow.Evidence);
            writer.WriteEndObject();
        }

        if (output.WrittenCount > MaximumCanonicalEntryBytes)
            throw new InvalidDataException("Migration reject ledger entry exceeds the contract ceiling.");
        return StrictUtf8.GetString(output.WrittenSpan);
    }

    public static int GetCanonicalEntryByteCount(
        string sourceObjectId,
        long batchOrdinal,
        MigrationRejectedRow rejectedRow) =>
        StrictByteCount(
            SerializeEntry(sourceObjectId, batchOrdinal, rejectedRow),
            "Migration reject ledger entry");

    public static int GetCanonicalArtifactEntryByteCount(
        string sourceObjectId,
        long batchOrdinal,
        MigrationRejectedRow rejectedRow) =>
        checked(GetCanonicalEntryByteCount(sourceObjectId, batchOrdinal, rejectedRow) + 1);

    public static string SerializeArtifactHeader(string planDigest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(planDigest);
        if (!IsLowerSha256(planDigest))
            throw new ArgumentException("The plan digest must be lowercase SHA-256.", nameof(planDigest));

        var output = new ArrayBufferWriter<byte>();
        using (var writer = CreateWriter(output))
        {
            writer.WriteStartObject();
            writer.WriteString("format", ArtifactFormat);
            writer.WriteString("planDigest", planDigest);
            writer.WriteEndObject();
        }
        return StrictUtf8.GetString(output.WrittenSpan);
    }

    public static int GetArtifactHeaderByteCount(string planDigest) =>
        checked(StrictByteCount(
            SerializeArtifactHeader(planDigest),
            "Migration reject artifact header") + 1);

    public static int GetRawValueByteCount(MigrationRejectedRow rejectedRow)
    {
        ArgumentNullException.ThrowIfNull(rejectedRow);
        MigrationRejectDigest.ValidateRejectedRows([rejectedRow]);
        int sensitiveValueBytes = 0;
        foreach (MigrationRejectEvidence item in rejectedRow.Evidence)
        {
            if (item.Value is not null)
            {
                sensitiveValueBytes = checked(sensitiveValueBytes +
                    StrictByteCount(item.Value, "Migration reject evidence value"));
            }
        }
        return sensitiveValueBytes;
    }

    private static Utf8JsonWriter CreateWriter(IBufferWriter<byte> output) => new(
        output,
        new JsonWriterOptions
        {
            Encoder = JavaScriptEncoder.Default,
            Indented = false,
            SkipValidation = false,
        });

    private static void WriteEvidence(
        Utf8JsonWriter writer,
        IReadOnlyList<MigrationRejectEvidence> evidence)
    {
        writer.WriteStartArray();
        foreach (MigrationRejectEvidence item in evidence)
        {
            writer.WriteStartObject();
            writer.WriteString("name", item.Name);
            if (item.Value is null)
                writer.WriteNull("value");
            else
                writer.WriteString("value", item.Value);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
    }

    private static MigrationRejectEvidence ReadEvidenceItem(JsonElement item)
    {
        if (item.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("Migration reject evidence entries must be objects.");

        string? name = null;
        string? value = null;
        bool sawName = false;
        bool sawValue = false;
        foreach (JsonProperty property in item.EnumerateObject())
        {
            switch (property.Name)
            {
                case "name" when !sawName && property.Value.ValueKind == JsonValueKind.String:
                    name = property.Value.GetString();
                    sawName = true;
                    break;
                case "value" when !sawValue && property.Value.ValueKind is
                    (JsonValueKind.String or JsonValueKind.Null):
                    value = property.Value.ValueKind == JsonValueKind.Null
                        ? null
                        : property.Value.GetString();
                    sawValue = true;
                    break;
                default:
                    throw new InvalidDataException(
                        "Migration reject evidence JSON contains an unknown, duplicate, or invalid property.");
            }
        }

        if (!sawName || !sawValue || name is null)
            throw new InvalidDataException("Migration reject evidence JSON has an incomplete entry.");
        return new MigrationRejectEvidence { Name = name, Value = value };
    }

    private static void ValidateEvidence(IReadOnlyList<MigrationRejectEvidence> evidence) =>
        MigrationRejectDigest.ValidateRejectedRows(
        [
            new MigrationRejectedRow
            {
                SourceRowOrdinal = 0,
                RuleId = "MIG-REJECT-CODEC-001",
                Evidence = evidence,
            },
        ]);

    private static int StrictByteCount(string value, string field)
    {
        try
        {
            return StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException error)
        {
            throw new InvalidDataException($"{field} must contain valid Unicode scalar data.", error);
        }
    }

    private static bool IsLowerSha256(string value)
    {
        if (value.Length != 64 ||
            !string.Equals(value, value.ToLowerInvariant(), StringComparison.Ordinal))
        {
            return false;
        }
        try
        {
            return Convert.FromHexString(value).Length == 32;
        }
        catch (FormatException)
        {
            return false;
        }
    }
}
