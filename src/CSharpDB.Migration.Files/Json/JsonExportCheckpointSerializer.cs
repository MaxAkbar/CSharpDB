using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Strict canonical serializer for restartable JSON and NDJSON export
/// object-boundary checkpoints. Prefix digests are verification evidence;
/// they are not serializable incremental-hash state.
/// </summary>
public static class JsonExportCheckpointSerializer
{
    public const int MaximumCheckpointBytes =
        JsonExportManifestSerializer.MaximumManifestBytes;

    private const int MaximumJsonDepth = 64;

    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private static readonly JsonSerializerOptions s_options =
        CreateOptions();

    private static readonly JsonExportHashManifest
        s_emptyLogicalPrefixDigest =
            ComputeEmptyLogicalDigest(complete: false);

    private static readonly JsonExportHashManifest
        s_emptyLogicalDigest =
            ComputeEmptyLogicalDigest(complete: true);

    /// <summary>
    /// Serializes one validated checkpoint to canonical UTF-8 without a BOM.
    /// </summary>
    public static byte[] Serialize(
        JsonExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);

        string digest =
            ComputeCheckpointDigestCore(checkpoint);
        byte[] bytes =
            SerializeEnvelope(checkpoint, digest);
        if (bytes.Length > MaximumCheckpointBytes)
        {
            CryptographicOperations.ZeroMemory(bytes);
            throw Invalid(
                $"The JSON export checkpoint exceeds the {MaximumCheckpointBytes}-byte safety limit.");
        }

        return bytes;
    }

    /// <summary>
    /// Parses, validates, verifies, and requires the exact canonical byte form
    /// of one checkpoint.
    /// </summary>
    public static JsonExportCheckpoint Deserialize(
        ReadOnlyMemory<byte> utf8Json)
    {
        ValidateInputEncoding(utf8Json.Span);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(
                utf8Json,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling =
                        JsonCommentHandling.Disallow,
                    MaxDepth = MaximumJsonDepth,
                });
        }
        catch (JsonException)
        {
            throw new InvalidDataException(
                "The JSON export checkpoint JSON is invalid.");
        }

        using (document)
        {
            RejectDuplicateProperties(
                document.RootElement);

            JsonExportCheckpointEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<
                        JsonExportCheckpointEnvelope<JsonElement>>(
                        s_options)
                    ?? throw Invalid(
                        "The JSON export checkpoint did not contain an envelope.");
            }
            catch (Exception exception) when (
                exception is JsonException or
                NotSupportedException)
            {
                throw new InvalidDataException(
                    "The JSON export checkpoint envelope is invalid.");
            }

            if (!string.Equals(
                    envelope.Format,
                    JsonExportCheckpointContracts.Format,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "The JSON export checkpoint format is not supported.");
            }
            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    JsonExportHashManifest.Sha256Algorithm,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "The JSON export checkpoint digest algorithm is not supported.");
            }
            if (envelope.Payload.ValueKind is
                JsonValueKind.Null or
                JsonValueKind.Undefined)
            {
                throw Invalid(
                    "The JSON export checkpoint payload is missing.");
            }

            JsonExportCheckpoint checkpoint;
            try
            {
                checkpoint = envelope.Payload
                    .Deserialize<JsonExportCheckpoint>(
                        s_options)
                    ?? throw Invalid(
                        "The JSON export checkpoint payload is missing.");
            }
            catch (Exception exception) when (
                exception is JsonException or
                NotSupportedException)
            {
                throw new InvalidDataException(
                    "The JSON export checkpoint payload is invalid.");
            }

            Validate(checkpoint);
            VerifyRawDigest(
                envelope.Digest,
                ComputeCheckpointDigestCore(checkpoint),
                "JSON export checkpoint envelope digest");

            byte[] canonicalBytes =
                SerializeEnvelope(
                    checkpoint,
                    envelope.Digest);
            try
            {
                if (!utf8Json.Span.SequenceEqual(
                        canonicalBytes))
                {
                    throw Invalid(
                        "The JSON export checkpoint is not in the required canonical UTF-8 form.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(
                    canonicalBytes);
            }

            return checkpoint;
        }
    }

    /// <summary>Computes the checkpoint envelope's lowercase SHA-256.</summary>
    public static string ComputeCheckpointDigest(
        JsonExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);
        return ComputeCheckpointDigestCore(checkpoint);
    }

    /// <summary>
    /// Computes the stable digest binding a prepared output to one immutable
    /// source, schema, profile, framing, codec, and resource policy.
    /// </summary>
    public static JsonExportHashManifest ComputeBindingDigest(
        JsonExportCheckpointBinding binding)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ValidateBinding(binding);
        return ComputeBindingDigestCore(binding);
    }

    /// <summary>
    /// Reconstructs and validates the final sidecar represented by a
    /// data-complete checkpoint.
    /// </summary>
    public static JsonExportManifest CreateCompletedManifest(
        JsonExportCheckpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        Validate(checkpoint);
        if (checkpoint.Phase !=
                JsonExportCheckpointPhase.DataComplete ||
            checkpoint.Completion is null)
        {
            throw Invalid(
                "Only a data-complete JSON export checkpoint has a final manifest.");
        }

        return CreateCompletedManifestCore(checkpoint);
    }

    private static void Validate(
        JsonExportCheckpoint checkpoint)
    {
        if (checkpoint.Generation < 0)
        {
            throw Invalid(
                "JSON export checkpoint generation cannot be negative.");
        }
        if (!Enum.IsDefined(checkpoint.Phase))
        {
            throw Invalid(
                "JSON export checkpoint phase is unsupported.");
        }
        if (checkpoint.Binding is null)
        {
            throw Invalid(
                "JSON export checkpoint binding is required.");
        }
        if (checkpoint.BindingDigest is null)
        {
            throw Invalid(
                "JSON export checkpoint binding digest is required.");
        }
        if (checkpoint.Progress is null)
        {
            throw Invalid(
                "JSON export checkpoint progress is required.");
        }

        ValidateBinding(checkpoint.Binding);
        ValidateHash(
            checkpoint.BindingDigest,
            "binding digest");
        VerifyHash(
            checkpoint.BindingDigest,
            ComputeBindingDigestCore(
                checkpoint.Binding),
            "JSON export checkpoint binding digest");

        ValidateProgress(
            checkpoint.Binding,
            checkpoint.Phase,
            checkpoint.Progress);

        switch (checkpoint.Phase)
        {
            case JsonExportCheckpointPhase.Writing:
                if (checkpoint.Completion is not null)
                {
                    throw Invalid(
                        "A writing JSON export checkpoint cannot contain completion evidence.");
                }
                break;

            case JsonExportCheckpointPhase.DataComplete:
                if (checkpoint.Completion is null)
                {
                    throw Invalid(
                        "A data-complete JSON export checkpoint requires completion evidence.");
                }

                ValidateCompletion(
                    checkpoint.Progress,
                    checkpoint.Completion);

                JsonExportManifest manifest =
                    CreateCompletedManifestCore(
                        checkpoint);
                _ = JsonExportManifestSerializer
                    .Serialize(manifest);
                VerifyRawDigest(
                    checkpoint.Completion
                        .ManifestDigest,
                    JsonExportManifestSerializer
                        .ComputeManifestDigest(
                            manifest),
                    "JSON export manifest digest");
                break;

            default:
                throw Invalid(
                    "JSON export checkpoint phase is unsupported.");
        }
    }

    private static void ValidateBinding(
        JsonExportCheckpointBinding binding)
    {
        if (binding.Source is null)
        {
            throw Invalid(
                "JSON export checkpoint source binding is required.");
        }
        if (binding.Table is null)
        {
            throw Invalid(
                "JSON export checkpoint table binding is required.");
        }
        if (binding.Json is null)
        {
            throw Invalid(
                "JSON export checkpoint format binding is required.");
        }

        JsonExportHashManifest emptyPhysical =
            HashBytes(
                binding.Json.Framing ==
                    JsonExportFraming.RootArray
                    ? "[]\n"u8
                    : ReadOnlySpan<byte>.Empty);
        JsonExportManifest provisional =
            CreateManifest(
                binding,
                completedRowCount: 0,
                dataByteLength:
                    binding.Json.Framing ==
                        JsonExportFraming.RootArray
                        ? 3
                        : 0,
                emptyPhysical,
                s_emptyLogicalDigest,
                s_emptyLogicalDigest);
        _ = JsonExportManifestSerializer
            .Serialize(provisional);

        string expectedSnapshotIdentity =
            JsonExportCheckpointContracts
                .RetainedSnapshotIdentityPrefix +
            binding.Source.SnapshotByteLength
                .ToString(
                    CultureInfo.InvariantCulture) +
            ":" +
            JsonExportHashManifest.Sha256Algorithm +
            ":" +
            binding.Source.SnapshotDigest.Value;
        if (!string.Equals(
                binding.SourceSnapshotIdentity,
                expectedSnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export checkpoint retained snapshot identity is not canonical for its source evidence.");
        }
    }

    private static void ValidateProgress(
        JsonExportCheckpointBinding binding,
        JsonExportCheckpointPhase phase,
        JsonExportCheckpointProgress progress)
    {
        if (progress.CompletedRowCount < 0)
        {
            throw Invalid(
                "JSON export checkpoint row count cannot be negative.");
        }
        if ((progress.CompletedRowCount == 0) !=
            (progress.LastCompletedRowId is null))
        {
            throw Invalid(
                "JSON export checkpoint last row ID must be absent exactly when no rows are complete.");
        }
        if (!string.Equals(
                progress.LogicalPrefixAggregation,
                JsonExportCheckpointContracts
                    .LogicalPrefixAggregation,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export checkpoint logical-prefix aggregation is unsupported.");
        }

        ValidateHash(
            progress.DataPrefixDigest,
            "data prefix digest");
        ValidateHash(
            progress
                .SourceLogicalRowHashPrefixDigest,
            "source logical row-hash prefix digest");
        ValidateHash(
            progress
                .ExportedLogicalRowHashPrefixDigest,
            "exported logical row-hash prefix digest");

        if (!HashValuesEqual(
                progress
                    .SourceLogicalRowHashPrefixDigest
                    .Value,
                progress
                    .ExportedLogicalRowHashPrefixDigest
                    .Value))
        {
            throw Invalid(
                "Lossless JSON source and exported logical prefix digests must be identical.");
        }

        _ = JsonExportCheckpointFraming
            .ValidateGeometry(
                binding,
                phase,
                progress);

        if (progress.CompletedRowCount == 0)
        {
            VerifyHash(
                progress
                    .SourceLogicalRowHashPrefixDigest,
                s_emptyLogicalPrefixDigest,
                "JSON export checkpoint empty logical prefix digest");

            ReadOnlySpan<byte> emptyPrefix =
                binding.Json.Framing switch
                {
                    JsonExportFraming.RootArray
                        when phase ==
                            JsonExportCheckpointPhase
                                .Writing =>
                        "["u8,
                    JsonExportFraming.RootArray =>
                        "[]\n"u8,
                    JsonExportFraming.Ndjson =>
                        ReadOnlySpan<byte>.Empty,
                    _ => throw Invalid(
                        "JSON export checkpoint framing is unsupported."),
                };
            VerifyHash(
                progress.DataPrefixDigest,
                HashBytes(emptyPrefix),
                "JSON export checkpoint empty data prefix digest");
        }
    }

    private static void ValidateCompletion(
        JsonExportCheckpointProgress progress,
        JsonExportCheckpointCompletion completion)
    {
        ValidateHash(
            completion.SourceLogicalDigest,
            "final source logical digest");
        ValidateHash(
            completion.ExportedLogicalDigest,
            "final exported logical digest");
        ValidateRawDigest(
            completion.ManifestDigest,
            "JSON export manifest digest");

        if (!HashValuesEqual(
                completion.SourceLogicalDigest.Value,
                completion.ExportedLogicalDigest.Value))
        {
            throw Invalid(
                "Lossless JSON source and exported final logical digests must be identical.");
        }

        if (progress.CompletedRowCount == 0)
        {
            VerifyHash(
                completion.SourceLogicalDigest,
                s_emptyLogicalDigest,
                "JSON export checkpoint empty final logical digest");
        }
    }

    private static JsonExportManifest
        CreateCompletedManifestCore(
            JsonExportCheckpoint checkpoint)
    {
        JsonExportCheckpointCompletion completion =
            checkpoint.Completion
            ?? throw Invalid(
                "JSON export checkpoint completion evidence is required.");
        return CreateManifest(
            checkpoint.Binding,
            checkpoint.Progress.CompletedRowCount,
            checkpoint.Progress
                .DataPrefixByteLength,
            checkpoint.Progress.DataPrefixDigest,
            completion.SourceLogicalDigest,
            completion.ExportedLogicalDigest);
    }

    private static JsonExportManifest CreateManifest(
        JsonExportCheckpointBinding binding,
        long completedRowCount,
        long dataByteLength,
        JsonExportHashManifest dataDigest,
        JsonExportHashManifest sourceLogicalDigest,
        JsonExportHashManifest exportedLogicalDigest) =>
        new()
        {
            Profile = binding.Profile,
            Source = binding.Source,
            Table = binding.Table,
            Json = binding.Json,
            Content = new JsonExportContentManifest
            {
                RowCount = completedRowCount,
                DataByteLength = dataByteLength,
                DataDigest = dataDigest,
                Canonicalization =
                    JsonExportContracts
                        .Canonicalization,
                CanonicalizationContractDigest =
                    JsonExportContracts
                        .CanonicalizationContractDigest,
                Aggregation =
                    JsonExportContracts
                        .OrderedContentDigest,
                SourceLogicalDigest =
                    sourceLogicalDigest,
                ExportedLogicalDigest =
                    exportedLogicalDigest,
            },
        };

    private static JsonExportHashManifest
        ComputeBindingDigestCore(
            JsonExportCheckpointBinding binding) =>
        HashCanonical(
            new JsonExportCheckpointBindingDigestInput
            {
                Contract =
                    JsonExportCheckpointContracts
                        .BindingContract,
                Binding = binding,
            });

    private static byte[] SerializeEnvelope(
        JsonExportCheckpoint checkpoint,
        string digest) =>
        SerializeCanonical(
            new JsonExportCheckpointEnvelope<
                JsonExportCheckpoint>
            {
                Format =
                    JsonExportCheckpointContracts
                        .Format,
                DigestAlgorithm =
                    JsonExportHashManifest
                        .Sha256Algorithm,
                Digest = digest,
                Payload = checkpoint,
            });

    private static string ComputeCheckpointDigestCore(
        JsonExportCheckpoint checkpoint) =>
        HashCanonicalRaw(
            new JsonExportCheckpointDigestInput
            {
                Format =
                    JsonExportCheckpointContracts
                        .Format,
                DigestAlgorithm =
                    JsonExportHashManifest
                        .Sha256Algorithm,
                Payload = checkpoint,
            });

    private static JsonExportHashManifest HashCanonical<T>(
        T value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = HashCanonicalRaw(value),
        };

    private static string HashCanonicalRaw<T>(
        T value)
    {
        byte[]? canonical = null;
        byte[]? digest = null;
        try
        {
            canonical = SerializeCanonical(value);
            digest = SHA256.HashData(canonical);
            return Hex(digest);
        }
        finally
        {
            Zero(canonical);
            Zero(digest);
        }
    }

    private static byte[] SerializeCanonical<T>(
        T value)
    {
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    value,
                    s_options);
            return JsonSnapshotPackageCanonicalJson
                .Serialize(element);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON export checkpoint is invalid.");
        }
    }

    private static JsonExportHashManifest
        ComputeEmptyLogicalDigest(
            bool complete)
    {
        using var digest =
            new JsonExportOrderedContentDigest();
        return complete
            ? digest.Complete()
            : digest.GetCurrentPrefixDigest();
    }

    private static JsonExportHashManifest HashBytes(
        ReadOnlySpan<byte> bytes)
    {
        byte[] digest = SHA256.HashData(bytes);
        try
        {
            return new JsonExportHashManifest
            {
                Algorithm =
                    JsonExportHashManifest
                        .Sha256Algorithm,
                Value = Hex(digest),
            };
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                digest);
        }
    }

    private static void ValidateHash(
        JsonExportHashManifest? hash,
        string description)
    {
        if (hash is null)
        {
            throw Invalid(
                $"JSON export checkpoint {description} is required.");
        }
        if (!string.Equals(
                hash.Algorithm,
                JsonExportHashManifest
                    .Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw Invalid(
                $"JSON export checkpoint {description} uses an unsupported algorithm.");
        }
        ValidateRawDigest(
            hash.Value,
            $"JSON export checkpoint {description}");
    }

    private static void VerifyHash(
        JsonExportHashManifest supplied,
        JsonExportHashManifest expected,
        string description)
    {
        ValidateHash(supplied, description);
        ValidateHash(expected, description);
        VerifyRawDigest(
            supplied.Value,
            expected.Value,
            description);
    }

    private static void ValidateRawDigest(
        string? value,
        string description)
    {
        if (value is null ||
            value.Length !=
                SHA256.HashSizeInBytes * 2)
        {
            throw Invalid(
                $"{description} is not lowercase SHA-256 text.");
        }
        foreach (char character in value)
        {
            if (character is not
                    (>= '0' and <= '9') and not
                    (>= 'a' and <= 'f'))
            {
                throw Invalid(
                    $"{description} is not lowercase SHA-256 text.");
            }
        }
    }

    private static void VerifyRawDigest(
        string? supplied,
        string expected,
        string description)
    {
        ValidateRawDigest(supplied, description);
        byte[] suppliedBytes =
            Convert.FromHexString(supplied!);
        byte[] expectedBytes =
            Convert.FromHexString(expected);
        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        suppliedBytes,
                        expectedBytes))
            {
                throw Invalid(
                    $"{description} does not match its canonical content.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                suppliedBytes);
            CryptographicOperations.ZeroMemory(
                expectedBytes);
        }
    }

    private static bool HashValuesEqual(
        string? left,
        string? right)
    {
        ValidateRawDigest(
            left,
            "JSON export checkpoint hash");
        ValidateRawDigest(
            right,
            "JSON export checkpoint hash");
        byte[] leftBytes =
            Convert.FromHexString(left!);
        byte[] rightBytes =
            Convert.FromHexString(right!);
        try
        {
            return CryptographicOperations
                .FixedTimeEquals(
                    leftBytes,
                    rightBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                leftBytes);
            CryptographicOperations.ZeroMemory(
                rightBytes);
        }
    }

    private static void ValidateInputEncoding(
        ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
        {
            throw Invalid(
                "The JSON export checkpoint is empty.");
        }
        if (utf8Json.Length >
            MaximumCheckpointBytes)
        {
            throw Invalid(
                $"The JSON export checkpoint exceeds the {MaximumCheckpointBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(
                Encoding.UTF8.Preamble))
        {
            throw Invalid(
                "The JSON export checkpoint cannot contain a UTF-8 byte-order mark.");
        }
        try
        {
            _ = s_strictUtf8.GetString(utf8Json);
        }
        catch (DecoderFallbackException)
        {
            throw new InvalidDataException(
                "The JSON export checkpoint contains invalid UTF-8.");
        }
    }

    private static void RejectDuplicateProperties(
        JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names =
                    new HashSet<string>(
                        StringComparer.Ordinal);
                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    if (!names.Add(
                            property.Name))
                    {
                        throw Invalid(
                            "The JSON export checkpoint contains a duplicate property.");
                    }
                    RejectDuplicateProperties(
                        property.Value);
                }
                break;

            case JsonValueKind.Array:
                foreach (JsonElement item in
                         element.EnumerateArray())
                {
                    RejectDuplicateProperties(item);
                }
                break;
        }
    }

    private static JsonSerializerOptions
        CreateOptions()
    {
        var options =
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web)
            {
                WriteIndented = false,
                DefaultIgnoreCondition =
                    JsonIgnoreCondition
                        .WhenWritingNull,
                PropertyNameCaseInsensitive =
                    false,
                UnmappedMemberHandling =
                    JsonUnmappedMemberHandling
                        .Disallow,
                AllowTrailingCommas = false,
                ReadCommentHandling =
                    JsonCommentHandling.Disallow,
                MaxDepth = MaximumJsonDepth,
            };
        options.Converters.Add(
            new JsonStringEnumConverter(
                JsonNamingPolicy.CamelCase,
                allowIntegerValues: false));
        return options;
    }

    private static string Hex(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(bytes)
            .ToLowerInvariant();

    private static void Zero(byte[]? bytes)
    {
        if (bytes is not null)
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }

    private static InvalidDataException Invalid(
        string message) =>
        new(message);
}

internal sealed record JsonExportCheckpointEnvelope<
    TPayload>
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

internal sealed record JsonExportCheckpointDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportCheckpoint Payload { get; init; }
}

internal sealed record
    JsonExportCheckpointBindingDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Contract { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonExportCheckpointBinding Binding { get; init; }
}
