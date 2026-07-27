using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Strict canonical serializer for restartable CSV export record-boundary
/// checkpoints. Prefix digests are verification evidence; they are not
/// serializable incremental-hash state.
/// </summary>
public static class CsvExportCheckpointSerializer
{
    public const int MaximumCheckpointBytes =
        CsvExportManifestSerializer.MaximumManifestBytes;

    private const int MaximumJsonDepth = 64;
    private const string ZeroSha256 =
        "0000000000000000000000000000000000000000000000000000000000000000";

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions s_options = CreateOptions();
    private static readonly string s_emptyLogicalPrefixDigest =
        ComputeEmptyLogicalDigest(complete: false);
    private static readonly string s_emptyLogicalDigest =
        ComputeEmptyLogicalDigest(complete: true);

    /// <summary>Serializes one validated checkpoint to canonical UTF-8.</summary>
    public static byte[] Serialize(CsvExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);

        string digest = ComputeCheckpointDigestCore(checkpoint);
        byte[] bytes = SerializeEnvelope(checkpoint, digest);
        if (bytes.Length > MaximumCheckpointBytes)
        {
            throw Invalid(
                $"The CSV export checkpoint exceeds the {MaximumCheckpointBytes}-byte safety limit.");
        }

        return bytes;
    }

    /// <summary>
    /// Parses, validates, verifies, and requires the exact canonical byte form
    /// of one checkpoint.
    /// </summary>
    public static CsvExportCheckpoint Deserialize(ReadOnlyMemory<byte> utf8Json)
    {
        ValidateInputEncoding(utf8Json.Span);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(utf8Json, new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = MaximumJsonDepth,
            });
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The CSV export checkpoint JSON is invalid.",
                exception);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            CsvExportCheckpointEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<CsvExportCheckpointEnvelope<JsonElement>>(s_options)
                    ?? throw Invalid("The CSV export checkpoint did not contain an envelope.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV export checkpoint envelope is invalid.",
                    exception);
            }

            if (!string.Equals(
                    envelope.Format,
                    CsvExportCheckpointContracts.Format,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    $"CSV export checkpoint format '{envelope.Format}' is not supported.");
            }
            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    CsvExportHashManifest.Sha256Algorithm,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    $"CSV export checkpoint digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }
            if (envelope.Payload.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw Invalid("The CSV export checkpoint payload is missing.");

            CsvExportCheckpoint checkpoint;
            try
            {
                checkpoint = envelope.Payload.Deserialize<CsvExportCheckpoint>(s_options)
                    ?? throw Invalid("The CSV export checkpoint payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV export checkpoint payload is invalid.",
                    exception);
            }

            Validate(checkpoint);
            VerifyRawDigest(
                envelope.Digest,
                ComputeCheckpointDigestCore(checkpoint),
                "CSV export checkpoint envelope digest");

            byte[] canonicalBytes = SerializeEnvelope(checkpoint, envelope.Digest);
            if (!utf8Json.Span.SequenceEqual(canonicalBytes))
            {
                throw Invalid(
                    "The CSV export checkpoint is not in the required canonical UTF-8 form.");
            }

            return checkpoint;
        }
    }

    /// <summary>Computes the checkpoint envelope's lowercase SHA-256.</summary>
    public static string ComputeCheckpointDigest(CsvExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);
        return ComputeCheckpointDigestCore(checkpoint);
    }

    /// <summary>
    /// Computes the stable digest used to bind a stateful writer and prepared
    /// output to one immutable export definition.
    /// </summary>
    public static CsvExportHashManifest ComputeBindingDigest(
        CsvExportCheckpointBinding binding)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ValidateBinding(binding);
        return HashCanonical(new CsvExportCheckpointBindingDigestInput
        {
            Contract = CsvExportCheckpointContracts.BindingContract,
            Binding = binding,
        });
    }

    /// <summary>
    /// Reconstructs and validates the final sidecar represented by a
    /// data-complete checkpoint.
    /// </summary>
    public static CsvExportManifest CreateCompletedManifest(
        CsvExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);
        if (checkpoint.Phase != CsvExportCheckpointPhase.DataComplete ||
            checkpoint.Completion is null)
        {
            throw Invalid(
                "Only a data-complete CSV export checkpoint has a final manifest.");
        }

        return CreateCompletedManifestCore(checkpoint);
    }

    private static void Validate(CsvExportCheckpoint checkpoint)
    {
        if (checkpoint.Generation < 0)
            throw Invalid("CSV export checkpoint generation cannot be negative.");
        if (!Enum.IsDefined(checkpoint.Phase))
            throw Invalid("CSV export checkpoint phase is unsupported.");
        if (checkpoint.Binding is null)
            throw Invalid("CSV export checkpoint binding is required.");
        if (checkpoint.BindingDigest is null)
            throw Invalid("CSV export checkpoint binding digest is required.");
        if (checkpoint.Progress is null)
            throw Invalid("CSV export checkpoint progress is required.");

        ValidateBinding(checkpoint.Binding);
        ValidateHash(checkpoint.BindingDigest, "binding digest");
        CsvExportHashManifest expectedBindingDigest =
            ComputeBindingDigestCore(checkpoint.Binding);
        VerifyHash(
            checkpoint.BindingDigest,
            expectedBindingDigest,
            "CSV export checkpoint binding digest");

        ValidateProgress(checkpoint.Binding, checkpoint.Progress);

        switch (checkpoint.Phase)
        {
            case CsvExportCheckpointPhase.Writing:
                if (checkpoint.Completion is not null)
                {
                    throw Invalid(
                        "A writing CSV export checkpoint cannot contain completion evidence.");
                }
                break;

            case CsvExportCheckpointPhase.DataComplete:
                if (checkpoint.Completion is null)
                {
                    throw Invalid(
                        "A data-complete CSV export checkpoint requires completion evidence.");
                }
                ValidateHash(
                    checkpoint.Completion.SourceLogicalDigest,
                    "final source logical digest");
                ValidateHash(
                    checkpoint.Completion.ExportedLogicalDigest,
                    "final exported logical digest");
                ValidateRawDigest(
                    checkpoint.Completion.ManifestDigest,
                    "CSV export manifest digest");

                if (checkpoint.Progress.CompletedRowCount == 0 &&
                    (!string.Equals(
                         checkpoint.Completion.SourceLogicalDigest.Value,
                         s_emptyLogicalDigest,
                         StringComparison.Ordinal) ||
                     !string.Equals(
                         checkpoint.Completion.ExportedLogicalDigest.Value,
                         s_emptyLogicalDigest,
                         StringComparison.Ordinal)))
                {
                    throw Invalid(
                        "An empty completed CSV export must use the frozen empty logical digest.");
                }

                CsvExportManifest manifest = CreateCompletedManifestCore(checkpoint);
                _ = CsvExportManifestSerializer.Serialize(manifest);
                VerifyRawDigest(
                    checkpoint.Completion.ManifestDigest,
                    CsvExportManifestSerializer.ComputeManifestDigest(manifest),
                    "CSV export manifest digest");
                break;

            default:
                throw Invalid("CSV export checkpoint phase is unsupported.");
        }
    }

    private static void ValidateBinding(CsvExportCheckpointBinding binding)
    {
        if (binding.Source is null)
            throw Invalid("CSV export checkpoint source binding is required.");
        if (binding.Table is null)
            throw Invalid("CSV export checkpoint table binding is required.");
        if (binding.Csv is null)
            throw Invalid("CSV export checkpoint CSV binding is required.");
        if (binding.MaxDataBytes <= 0)
            throw Invalid("CSV export checkpoint data-byte ceiling must be positive.");
        if (binding.MaximumDecodedBlobBytes is < 1 or >
            CsvExportContracts.MaximumSupportedDecodedBlobBytes)
        {
            throw Invalid(
                "CSV export checkpoint decoded BLOB ceiling is outside the supported range.");
        }

        CsvExportManifest provisional = CreateManifest(
            binding,
            completedRowCount: 0,
            dataByteLength: 1,
            Hash(ZeroSha256),
            Hash(s_emptyLogicalPrefixDigest),
            Hash(s_emptyLogicalPrefixDigest),
            transformedRowCount: 0,
            transformedCellCount: 0);
        _ = CsvExportManifestSerializer.Serialize(provisional);

        CsvExportHeaderEvidence header = ComputeHeaderEvidence(binding.Table.Columns);
        if (header.ByteLength > binding.MaxDataBytes)
        {
            throw Invalid(
                "CSV export checkpoint data-byte ceiling cannot contain the deterministic header.");
        }

        string expectedSnapshotIdentity =
            CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
            binding.Source.SnapshotByteLength.ToString(CultureInfo.InvariantCulture) +
            ":" +
            CsvExportHashManifest.Sha256Algorithm +
            ":" +
            binding.Source.SnapshotDigest.Value;
        if (!string.Equals(
                binding.SourceSnapshotIdentity,
                expectedSnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "CSV export checkpoint retained snapshot identity is not canonical for its source evidence.");
        }

        foreach (CsvExportColumnManifest column in binding.Table.Columns)
        {
            if (column.DatabaseType == CsvExportDatabaseType.Blob &&
                column.MaximumDecodedBytes != binding.MaximumDecodedBlobBytes)
            {
                throw Invalid(
                    "CSV export checkpoint decoded BLOB ceiling does not match the table binding.");
            }
        }
    }

    private static void ValidateProgress(
        CsvExportCheckpointBinding binding,
        CsvExportCheckpointProgress progress)
    {
        if (progress.CompletedRowCount < 0)
            throw Invalid("CSV export checkpoint row count cannot be negative.");
        if ((progress.CompletedRowCount == 0) !=
            (progress.LastCompletedRowId is null))
        {
            throw Invalid(
                "CSV export checkpoint last row ID must be absent exactly when no rows are complete.");
        }
        if (progress.DataPrefixByteLength <= 0 ||
            progress.DataPrefixByteLength > binding.MaxDataBytes)
        {
            throw Invalid(
                "CSV export checkpoint data prefix is outside the configured byte ceiling.");
        }

        CsvExportHeaderEvidence header = ComputeHeaderEvidence(binding.Table.Columns);
        if (progress.DataPrefixByteLength < header.ByteLength ||
            (progress.CompletedRowCount > 0 &&
             progress.DataPrefixByteLength == header.ByteLength))
        {
            throw Invalid(
                "CSV export checkpoint data prefix is shorter than its completed-record boundary.");
        }
        if (!string.Equals(
                progress.LogicalPrefixAggregation,
                CsvExportCheckpointContracts.LogicalPrefixAggregation,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "CSV export checkpoint logical-prefix aggregation is unsupported.");
        }

        ValidateHash(progress.DataPrefixDigest, "data prefix digest");
        ValidateHash(
            progress.SourceLogicalRowHashPrefixDigest,
            "source logical row-hash prefix digest");
        ValidateHash(
            progress.ExportedLogicalRowHashPrefixDigest,
            "exported logical row-hash prefix digest");

        if (progress.CompletedRowCount == 0 &&
            (!string.Equals(
                 progress.SourceLogicalRowHashPrefixDigest.Value,
                 s_emptyLogicalPrefixDigest,
                 StringComparison.Ordinal) ||
             !string.Equals(
                 progress.ExportedLogicalRowHashPrefixDigest.Value,
                 s_emptyLogicalPrefixDigest,
                 StringComparison.Ordinal)))
        {
            throw Invalid(
                "A header-only CSV export checkpoint must use the frozen empty logical-prefix digest.");
        }
        if (progress.CompletedRowCount == 0)
        {
            if (progress.DataPrefixByteLength != header.ByteLength)
            {
                throw Invalid(
                    "A header-only CSV export checkpoint must end at the exact header boundary.");
            }
            VerifyRawDigest(
                progress.DataPrefixDigest.Value,
                header.Digest,
                "CSV export checkpoint header data-prefix digest");
        }
        if (binding.Profile == CsvExportProfile.LosslessV1 &&
            (progress.TransformedRowCount != 0 ||
             progress.TransformedCellCount != 0))
        {
            throw Invalid(
                "A lossless CSV export checkpoint cannot report transformed rows or cells.");
        }

        CsvExportManifest progressManifest = CreateManifest(
            binding,
            progress.CompletedRowCount,
            progress.DataPrefixByteLength,
            progress.DataPrefixDigest,
            progress.SourceLogicalRowHashPrefixDigest,
            progress.ExportedLogicalRowHashPrefixDigest,
            progress.TransformedRowCount,
            progress.TransformedCellCount);
        _ = CsvExportManifestSerializer.Serialize(progressManifest);
    }

    private static CsvExportHeaderEvidence ComputeHeaderEvidence(
        IReadOnlyList<CsvExportColumnManifest> columns)
    {
        var rendered = new StringBuilder();
        for (int index = 0; index < columns.Count; index++)
        {
            if (index != 0)
                rendered.Append(',');

            string header = columns[index].Header;
            bool quote =
                string.Equals(header, CsvExportContracts.NullToken, StringComparison.Ordinal) ||
                header.Contains(',', StringComparison.Ordinal) ||
                header.Contains('"', StringComparison.Ordinal) ||
                header.Contains('\r', StringComparison.Ordinal) ||
                header.Contains('\n', StringComparison.Ordinal);
            if (!quote)
            {
                rendered.Append(header);
                continue;
            }

            rendered.Append('"');
            foreach (char character in header)
            {
                if (character == '"')
                    rendered.Append("\"\"");
                else
                    rendered.Append(character);
            }
            rendered.Append('"');
        }
        rendered.Append("\r\n");

        byte[] bytes = s_strictUtf8.GetBytes(rendered.ToString());
        try
        {
            return new CsvExportHeaderEvidence(
                bytes.LongLength,
                Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant());
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }

    private static CsvExportManifest CreateCompletedManifestCore(
        CsvExportCheckpoint checkpoint)
    {
        CsvExportCheckpointCompletion completion = checkpoint.Completion
            ?? throw Invalid("CSV export checkpoint completion evidence is required.");
        return CreateManifest(
            checkpoint.Binding,
            checkpoint.Progress.CompletedRowCount,
            checkpoint.Progress.DataPrefixByteLength,
            checkpoint.Progress.DataPrefixDigest,
            completion.SourceLogicalDigest,
            completion.ExportedLogicalDigest,
            checkpoint.Progress.TransformedRowCount,
            checkpoint.Progress.TransformedCellCount);
    }

    private static CsvExportManifest CreateManifest(
        CsvExportCheckpointBinding binding,
        long completedRowCount,
        long dataByteLength,
        CsvExportHashManifest dataDigest,
        CsvExportHashManifest sourceLogicalDigest,
        CsvExportHashManifest exportedLogicalDigest,
        long transformedRowCount,
        long transformedCellCount)
    {
        int transformedHeaderCount = binding.Table.Columns.Count(
            static column => !string.Equals(
                column.Header,
                column.SourceName,
                StringComparison.Ordinal));
        return new CsvExportManifest
        {
            Profile = binding.Profile,
            Source = binding.Source,
            Table = binding.Table,
            Csv = binding.Csv,
            Content = new CsvExportContentManifest
            {
                RowCount = completedRowCount,
                DataByteLength = dataByteLength,
                DataDigest = dataDigest,
                Canonicalization = CsvExportContracts.Canonicalization,
                CanonicalizationContractDigest =
                    CsvExportContracts.CanonicalizationContractDigest,
                Aggregation = CsvExportContracts.OrderedContentDigest,
                SourceLogicalDigest = sourceLogicalDigest,
                ExportedLogicalDigest = exportedLogicalDigest,
            },
            LossyTransform = binding.Profile == CsvExportProfile.SpreadsheetSafeLossyV1
                ? new CsvExportLossyTransformManifest
                {
                    RuleId = CsvExportContracts.SpreadsheetFormulaRuleId,
                    Algorithm = CsvExportContracts.SpreadsheetFormulaTransform,
                    TransformedHeaderCount = transformedHeaderCount,
                    TransformedRowCount = transformedRowCount,
                    TransformedCellCount = transformedCellCount,
                }
                : null,
        };
    }

    private static CsvExportHashManifest ComputeBindingDigestCore(
        CsvExportCheckpointBinding binding) =>
        HashCanonical(new CsvExportCheckpointBindingDigestInput
        {
            Contract = CsvExportCheckpointContracts.BindingContract,
            Binding = binding,
        });

    private static byte[] SerializeEnvelope(
        CsvExportCheckpoint checkpoint,
        string digest) =>
        SerializeCanonical(new CsvExportCheckpointEnvelope<CsvExportCheckpoint>
        {
            Format = CsvExportCheckpointContracts.Format,
            DigestAlgorithm = CsvExportHashManifest.Sha256Algorithm,
            Digest = digest,
            Payload = checkpoint,
        });

    private static string ComputeCheckpointDigestCore(
        CsvExportCheckpoint checkpoint) =>
        HashCanonicalRaw(new CsvExportCheckpointDigestInput
        {
            Format = CsvExportCheckpointContracts.Format,
            DigestAlgorithm = CsvExportHashManifest.Sha256Algorithm,
            Payload = checkpoint,
        });

    private static CsvExportHashManifest HashCanonical<T>(T value) =>
        Hash(HashCanonicalRaw(value));

    private static string HashCanonicalRaw<T>(T value)
    {
        byte[] canonical = SerializeCanonical(value);
        return Convert.ToHexString(SHA256.HashData(canonical)).ToLowerInvariant();
    }

    private static byte[] SerializeCanonical<T>(T value)
    {
        try
        {
            JsonElement element = JsonSerializer.SerializeToElement(value, s_options);
            return CsvSnapshotPackageCanonicalJson.Serialize(element);
        }
        catch (Exception exception) when (
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The CSV export checkpoint is invalid.",
                exception);
        }
    }

    private static string ComputeEmptyLogicalDigest(bool complete)
    {
        using var digest = new CsvExportOrderedContentDigest();
        return (complete ? digest.Complete() : digest.GetCurrentPrefixDigest()).Value;
    }

    private static CsvExportHashManifest Hash(string value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = value,
    };

    private static void ValidateHash(
        CsvExportHashManifest? hash,
        string description)
    {
        if (hash is null)
            throw Invalid($"CSV export checkpoint {description} is required.");
        if (!string.Equals(
                hash.Algorithm,
                CsvExportHashManifest.Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw Invalid(
                $"CSV export checkpoint {description} uses an unsupported algorithm.");
        }
        ValidateRawDigest(hash.Value, $"CSV export checkpoint {description}");
    }

    private static void VerifyHash(
        CsvExportHashManifest supplied,
        CsvExportHashManifest expected,
        string description)
    {
        ValidateHash(supplied, description);
        VerifyRawDigest(supplied.Value, expected.Value, description);
    }

    private static void ValidateRawDigest(string? value, string description)
    {
        if (value is null || value.Length != SHA256.HashSizeInBytes * 2)
            throw Invalid($"{description} is not lowercase SHA-256 text.");
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                throw Invalid($"{description} is not lowercase SHA-256 text.");
        }
    }

    private static void VerifyRawDigest(
        string? supplied,
        string expected,
        string description)
    {
        ValidateRawDigest(supplied, description);
        byte[] suppliedBytes = Convert.FromHexString(supplied!);
        byte[] expectedBytes = Convert.FromHexString(expected);
        if (!CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
            throw Invalid($"{description} does not match its canonical content.");
    }

    private static void ValidateInputEncoding(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
            throw Invalid("The CSV export checkpoint is empty.");
        if (utf8Json.Length > MaximumCheckpointBytes)
        {
            throw Invalid(
                $"The CSV export checkpoint exceeds the {MaximumCheckpointBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
            throw Invalid("The CSV export checkpoint cannot contain a UTF-8 byte-order mark.");
        try
        {
            _ = s_strictUtf8.GetString(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The CSV export checkpoint contains invalid UTF-8.",
                exception);
        }
    }

    private static void RejectDuplicateProperties(JsonElement element, string path)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                {
                    var names = new HashSet<string>(StringComparer.Ordinal);
                    foreach (JsonProperty property in element.EnumerateObject())
                    {
                        if (!names.Add(property.Name))
                        {
                            throw Invalid(
                                $"The CSV export checkpoint contains duplicate property '{property.Name}' at '{path}'.");
                        }
                        RejectDuplicateProperties(
                            property.Value,
                            path + "." + property.Name);
                    }
                    break;
                }
            case JsonValueKind.Array:
                {
                    int index = 0;
                    foreach (JsonElement item in element.EnumerateArray())
                    {
                        RejectDuplicateProperties(item, $"{path}[{index}]");
                        index++;
                    }
                    break;
                }
        }
    }

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            AllowTrailingCommas = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = MaximumJsonDepth,
        };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    private static InvalidDataException Invalid(string message) => new(message);

    private readonly record struct CsvExportHeaderEvidence(
        long ByteLength,
        string Digest);
}

internal sealed record CsvExportCheckpointEnvelope<TPayload>
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required string Digest { get; init; }

    [JsonPropertyOrder(3)]
    public required TPayload Payload { get; init; }
}

internal sealed record CsvExportCheckpointDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportCheckpoint Payload { get; init; }
}

internal sealed record CsvExportCheckpointBindingDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Contract { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvExportCheckpointBinding Binding { get; init; }
}
