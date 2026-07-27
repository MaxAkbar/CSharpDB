using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Migration.Files.Json;

internal static partial class
    JsonTypedSnapshotPackageManifestSerializer
{
    internal const string Format =
        "csharpdb-json-snapshot-package/v2";
    internal const string DigestAlgorithm = "sha256";
    internal const int MaximumManifestBytes =
        JsonSnapshotPackageManifestSerializer.MaximumManifestBytes;
    private const int MaximumJsonDepth = 64;
    private const int MaximumPayloadTextCharacters =
        1024 * 1024;

    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private static readonly JsonSerializerOptions s_options =
        CreateOptions();

    internal static byte[] Serialize(
        JsonTypedSnapshotPackageManifestPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ValidatePayloadShape(payload);
        ValidateSerializationBudget(payload);

        JsonElement payloadElement;
        try
        {
            payloadElement =
                JsonSerializer.SerializeToElement(
                    payload,
                    s_options);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest payload is invalid.",
                exception);
        }

        ValidateNoSecrets(payloadElement);
        string digest = ComputePayloadDigest(payload);
        byte[] bytes = SerializeEnvelope(payload, digest);
        if (bytes.Length > MaximumManifestBytes)
        {
            CryptographicOperations.ZeroMemory(bytes);
            throw new InvalidDataException(
                $"The typed JSON snapshot package manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return bytes;
    }

    internal static JsonTypedSnapshotPackageManifestPayload
        Deserialize(ReadOnlyMemory<byte> utf8Json)
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
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest JSON is invalid.",
                exception);
        }

        using (document)
        {
            RejectDuplicateProperties(
                document.RootElement);

            JsonTypedSnapshotPackageManifestEnvelope<
                JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<
                        JsonTypedSnapshotPackageManifestEnvelope<
                            JsonElement>>(s_options)
                    ?? throw new InvalidDataException(
                        "The typed JSON snapshot package manifest envelope is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package manifest envelope is invalid.",
                    exception);
            }

            if (!string.Equals(
                    envelope.Format,
                    Format,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    envelope.DigestAlgorithm,
                    DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package manifest uses an unsupported format or digest contract.");
            }
            if (envelope.Payload.ValueKind is
                JsonValueKind.Null or
                JsonValueKind.Undefined)
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package manifest payload is missing.");
            }

            ValidateNoSecrets(envelope.Payload);
            JsonTypedSnapshotPackageManifestPayload payload;
            try
            {
                payload = envelope.Payload
                    .Deserialize<
                        JsonTypedSnapshotPackageManifestPayload>(
                        s_options)
                    ?? throw new InvalidDataException(
                        "The typed JSON snapshot package manifest payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package manifest payload is invalid.",
                    exception);
            }

            ValidatePayloadShape(payload);
            ValidateSerializationBudget(payload);
            VerifyDigest(
                envelope.Digest,
                ComputePayloadDigest(payload));

            byte[] canonical =
                SerializeEnvelope(payload, envelope.Digest);
            try
            {
                if (!utf8Json.Span.SequenceEqual(canonical))
                {
                    throw new InvalidDataException(
                        "The typed JSON snapshot package manifest is not in the required canonical UTF-8 form.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(canonical);
            }

            return payload;
        }
    }

    private static byte[] SerializeEnvelope(
        JsonTypedSnapshotPackageManifestPayload payload,
        string digest)
    {
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonTypedSnapshotPackageManifestEnvelope<
                        JsonTypedSnapshotPackageManifestPayload>
                    {
                        Format = Format,
                        DigestAlgorithm = DigestAlgorithm,
                        Digest = digest,
                        Payload = payload,
                    },
                    s_options);
            return JsonSnapshotPackageCanonicalJson.Serialize(
                element);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest is invalid.",
                exception);
        }
    }

    private static string ComputePayloadDigest(
        JsonTypedSnapshotPackageManifestPayload payload)
    {
        byte[] canonical;
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonTypedSnapshotPackageManifestDigestInput
                    {
                        Format = Format,
                        DigestAlgorithm = DigestAlgorithm,
                        Payload = payload,
                    },
                    s_options);
            canonical =
                JsonSnapshotPackageCanonicalJson.Serialize(
                    element);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package digest input is invalid.",
                exception);
        }

        byte[] digest = SHA256.HashData(canonical);
        try
        {
            return Convert.ToHexString(digest)
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(digest);
            CryptographicOperations.ZeroMemory(canonical);
        }
    }

    private static void VerifyDigest(
        string? suppliedDigest,
        string expectedDigest)
    {
        if (!IsCanonicalHexDigest(suppliedDigest))
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest digest is not canonical SHA-256 text.");
        }

        byte[] supplied =
            Convert.FromHexString(suppliedDigest!);
        byte[] expected =
            Convert.FromHexString(expectedDigest);
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    supplied,
                    expected))
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package manifest digest does not match its payload.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(supplied);
            CryptographicOperations.ZeroMemory(expected);
        }
    }

    private static void ValidatePayloadShape(
        JsonTypedSnapshotPackageManifestPayload payload)
    {
        RequireMember(payload.Contracts);
        RequireMember(payload.Snapshot);
        RequireMember(payload.Source);
        RequireMember(payload.Reader);
        RequireMember(payload.Inference);
        RequireMember(payload.TypedIntent);
        RequireMember(payload.Catalog);
        RequireMember(payload.Inference.ColumnOverrides);

        RequireText(payload.Contracts.Snapshot);
        RequireText(payload.Contracts.Binding);
        RequireText(payload.Contracts.Options);
        RequireText(payload.Contracts.RepresentationSchema);
        RequireText(payload.Contracts.RepresentationScalar);
        RequireText(payload.Contracts.TypedSchema);
        RequireText(payload.Contracts.TypedScalar);
        RequireText(payload.Contracts.CanonicalValue);
        RequireText(payload.Contracts.CatalogFormat);
        RequireText(payload.Contracts.IntentFormat);
        RequireText(payload.Contracts.TypedValue);
        RequireText(payload.Contracts.TextCodec);
        RequireText(payload.Snapshot.ContentDigest);
        RequireText(payload.Snapshot.SnapshotIdentity);
        RequireText(payload.Source.Identity);
        RequireText(payload.Source.Fingerprint);
        RequireText(payload.Source.OptionsDigest);
        RequireText(payload.Inference.TableName);
        RequireText(payload.TypedIntent.ManifestDigest);
        RequireText(payload.Catalog.TargetCSharpDbVersion);
        RequireText(payload.Catalog.Digest);

        if (!Enum.IsDefined(payload.Reader.Framing))
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package reader framing is invalid.");
        }

        int previousIndex = -1;
        for (int index = 0;
             index <
             payload.Inference.ColumnOverrides.Count;
             index++)
        {
            JsonSnapshotPackageColumnOverrideManifest? item =
                payload.Inference.ColumnOverrides[index];
            if (item is null)
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package ordinary overrides cannot contain null members.");
            }
            if (item.ColumnIndex <= previousIndex ||
                !Enum.IsDefined(item.LogicalType) ||
                !Enum.IsDefined(item.MissingPolicy))
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package ordinary overrides are invalid or not ordered.");
            }
            previousIndex = item.ColumnIndex;
            RequireText(item.ExpectedPropertyName);
        }
    }

    private static void ValidateSerializationBudget(
        JsonTypedSnapshotPackageManifestPayload payload)
    {
        long totalCharacters = 0;
        Add(payload.Contracts.Snapshot);
        Add(payload.Contracts.Binding);
        Add(payload.Contracts.Options);
        Add(payload.Contracts.RepresentationSchema);
        Add(payload.Contracts.RepresentationScalar);
        Add(payload.Contracts.TypedSchema);
        Add(payload.Contracts.TypedScalar);
        Add(payload.Contracts.CanonicalValue);
        Add(payload.Contracts.CatalogFormat);
        Add(payload.Contracts.IntentFormat);
        Add(payload.Contracts.TypedValue);
        Add(payload.Contracts.TextCodec);
        Add(payload.Snapshot.ContentDigest);
        Add(payload.Snapshot.SnapshotIdentity);
        Add(payload.Source.Identity);
        Add(payload.Source.Fingerprint);
        Add(payload.Source.OptionsDigest);
        Add(payload.Inference.TableName);
        for (int index = 0;
             index <
             payload.Inference.ColumnOverrides.Count;
             index++)
        {
            JsonSnapshotPackageColumnOverrideManifest? item =
                payload.Inference.ColumnOverrides[index];
            if (item is null)
            {
                throw new InvalidDataException(
                    "The typed JSON snapshot package ordinary overrides cannot contain null members.");
            }
            Add(item.ExpectedPropertyName);
        }
        Add(payload.TypedIntent.ManifestDigest);
        Add(payload.Catalog.TargetCSharpDbVersion);
        Add(payload.Catalog.Digest);

        void Add(string? value)
        {
            if (value is null)
                return;
            totalCharacters =
                checked(totalCharacters + value.Length);
            if (totalCharacters >
                MaximumPayloadTextCharacters)
            {
                throw new InvalidDataException(
                    $"The typed JSON snapshot package manifest text exceeds the {MaximumPayloadTextCharacters}-character budget.");
            }
        }
    }

    private static void ValidateInputEncoding(
        ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty ||
            utf8Json.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest is empty or exceeds its safety bound.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble) ||
            utf8Json.IndexOf((byte)0) >= 0)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest contains a forbidden BOM or NUL byte.");
        }

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest is not strict UTF-8.",
                exception);
        }
    }

    private static void RequireMember(object? value)
    {
        if (value is null)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest is missing a required member.");
        }
    }

    private static void RequireText(string? value)
    {
        RequireMember(value);
        try
        {
            _ = s_strictUtf8.GetByteCount(value!);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest contains invalid Unicode.",
                exception);
        }
    }

    private static void RejectDuplicateProperties(
        JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names =
                    new HashSet<string>(StringComparer.Ordinal);
                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    if (!names.Add(property.Name))
                    {
                        throw new InvalidDataException(
                            "The typed JSON snapshot package manifest contains a duplicate property.");
                    }
                    RejectDuplicateProperties(property.Value);
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

    private static void ValidateNoSecrets(
        JsonElement element,
        string? propertyName = null)
    {
        if (propertyName is not null &&
            IsSecretPropertyName(propertyName))
        {
            throw new InvalidDataException(
                "The typed JSON snapshot package manifest cannot contain secret-bearing members.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty(
                        "name",
                        out JsonElement name) &&
                    name.ValueKind == JsonValueKind.String &&
                    IsSecretPropertyName(
                        name.GetString() ?? string.Empty) &&
                    element.TryGetProperty("value", out _))
                {
                    throw new InvalidDataException(
                        "The typed JSON snapshot package manifest cannot contain secret-bearing key/value entries.");
                }
                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    ValidateNoSecrets(
                        property.Value,
                        property.Name);
                }
                break;
            case JsonValueKind.Array:
                foreach (JsonElement item in
                         element.EnumerateArray())
                {
                    ValidateNoSecrets(item);
                }
                break;
            case JsonValueKind.String:
                string? value = element.GetString();
                if (value is not null &&
                    (value.Contains(
                         '\0',
                         StringComparison.Ordinal) ||
                     LooksLikeSecret(value)))
                {
                    throw new InvalidDataException(
                        "The typed JSON snapshot package manifest cannot contain credential material.");
                }
                break;
        }
    }

    private static bool LooksLikeSecret(string value) =>
        SecretAssignmentPattern().IsMatch(value) ||
        BearerAuthorizationPattern().IsMatch(value) ||
        CredentialUriPattern().IsMatch(value) ||
        PrivateKeyPattern().IsMatch(value);

    private static bool IsSecretPropertyName(
        string propertyName)
    {
        string normalized = propertyName
            .Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace(" ", string.Empty, StringComparison.Ordinal)
            .ToLowerInvariant();
        return normalized is
            "password" or
            "pwd" or
            "token" or
            "secret" or
            "credential" or
            "connectionstring" or
            "apikey" or
            "accesskey" or
            "clientsecret" or
            "authorization";
    }

    private static bool IsCanonicalHexDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 64 &&
        digest.All(
            character =>
                character is >= '0' and <= '9' or
                    >= 'a' and <= 'f');

    private static JsonSerializerOptions CreateOptions()
    {
        var options =
            new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                WriteIndented = false,
                DefaultIgnoreCondition =
                    JsonIgnoreCondition.WhenWritingNull,
                PropertyNameCaseInsensitive = false,
                UnmappedMemberHandling =
                    JsonUnmappedMemberHandling.Disallow,
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

    [GeneratedRegex(
        @"(?i)(password|pwd|token|secret|api[_ -]?key|access[_ -]?key|client[_ -]?secret)\s*=\s*[^;,\s]+",
        RegexOptions.CultureInvariant)]
    private static partial Regex SecretAssignmentPattern();

    [GeneratedRegex(
        @"(?i)authorization\s*:\s*bearer\s+\S+",
        RegexOptions.CultureInvariant)]
    private static partial Regex BearerAuthorizationPattern();

    [GeneratedRegex(
        @"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^@\s/]+@",
        RegexOptions.CultureInvariant)]
    private static partial Regex CredentialUriPattern();

    [GeneratedRegex(
        @"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----",
        RegexOptions.CultureInvariant)]
    private static partial Regex PrivateKeyPattern();
}

internal sealed record
    JsonTypedSnapshotPackageManifestPayload
{
    [JsonPropertyOrder(0)]
    public required JsonTypedSnapshotPackageContractIdsManifest
        Contracts { get; init; }

    [JsonPropertyOrder(1)]
    public required JsonSnapshotPackageSnapshotManifest
        Snapshot { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonSnapshotPackageSourceManifest
        Source { get; init; }

    [JsonPropertyOrder(3)]
    public required JsonSnapshotPackageReaderManifest
        Reader { get; init; }

    [JsonPropertyOrder(4)]
    public required JsonSnapshotPackageInferenceManifest
        Inference { get; init; }

    [JsonPropertyOrder(5)]
    public required JsonTypedSnapshotPackageIntentManifest
        TypedIntent { get; init; }

    [JsonPropertyOrder(6)]
    public required JsonSnapshotPackageCatalogManifest
        Catalog { get; init; }
}

internal sealed record
    JsonTypedSnapshotPackageContractIdsManifest
{
    [JsonPropertyOrder(0)]
    public required string Snapshot { get; init; }

    [JsonPropertyOrder(1)]
    public required string Binding { get; init; }

    [JsonPropertyOrder(2)]
    public required string Options { get; init; }

    [JsonPropertyOrder(3)]
    public required string RepresentationSchema { get; init; }

    [JsonPropertyOrder(4)]
    public required string RepresentationScalar { get; init; }

    [JsonPropertyOrder(5)]
    public required string TypedSchema { get; init; }

    [JsonPropertyOrder(6)]
    public required string TypedScalar { get; init; }

    [JsonPropertyOrder(7)]
    public required string CanonicalValue { get; init; }

    [JsonPropertyOrder(8)]
    public required string CatalogFormat { get; init; }

    [JsonPropertyOrder(9)]
    public required string IntentFormat { get; init; }

    [JsonPropertyOrder(10)]
    public required string TypedValue { get; init; }

    [JsonPropertyOrder(11)]
    public required string TextCodec { get; init; }
}

internal sealed record
    JsonTypedSnapshotPackageIntentManifest
{
    [JsonPropertyOrder(0)]
    public required int ByteLength { get; init; }

    [JsonPropertyOrder(1)]
    public required string ManifestDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required int MaxDecodedBinaryBytes { get; init; }

    [JsonPropertyOrder(3)]
    public required int MaxDecimalDigits { get; init; }

    [JsonPropertyOrder(4)]
    public required int ColumnCount { get; init; }
}

internal sealed record
    JsonTypedSnapshotPackageManifestEnvelope<TPayload>
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

internal sealed record
    JsonTypedSnapshotPackageManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonTypedSnapshotPackageManifestPayload
        Payload { get; init; }
}
