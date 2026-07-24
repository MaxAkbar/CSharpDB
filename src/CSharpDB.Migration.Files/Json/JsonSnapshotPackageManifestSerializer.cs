using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Migration.Files.Json;

internal static partial class JsonSnapshotPackageManifestSerializer
{
    internal const string Format =
        "csharpdb-json-snapshot-package/v1";
    internal const string DigestAlgorithm = "sha256";
    internal const int MaximumManifestBytes = 16 * 1024 * 1024;
    internal const int MaximumJsonDepth = 64;
    internal const int MaximumPayloadTextCharacters = 1024 * 1024;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions s_options =
        CreateOptions();

    internal static byte[] Serialize(
        JsonSnapshotPackageManifestPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ValidatePayload(payload);
        ValidateSerializationBudget(payload);

        JsonElement payloadElement;
        try
        {
            payloadElement =
                JsonSerializer.SerializeToElement(payload, s_options);
        }
        catch (Exception exception) when (
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest payload is invalid.",
                exception);
        }

        ValidateNoSecrets(payloadElement);
        string digest = ComputeDigest(payload);
        byte[] manifestBytes = SerializeEnvelope(payload, digest);
        if (manifestBytes.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The JSON snapshot package manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return manifestBytes;
    }

    internal static JsonSnapshotPackageManifestPayload Deserialize(
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
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = MaximumJsonDepth,
                });
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest JSON is invalid.",
                exception);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            JsonSnapshotPackageManifestEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<
                        JsonSnapshotPackageManifestEnvelope<JsonElement>>(
                        s_options)
                    ?? throw new InvalidDataException(
                        "The JSON snapshot package manifest did not contain an envelope.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The JSON snapshot package manifest envelope is invalid.",
                    exception);
            }

            if (!string.Equals(
                    envelope.Format,
                    Format,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"JSON snapshot package manifest format '{envelope.Format}' is not supported.");
            }

            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"JSON snapshot package manifest digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }

            if (envelope.Payload.ValueKind is
                JsonValueKind.Null or JsonValueKind.Undefined)
            {
                throw new InvalidDataException(
                    "The JSON snapshot package manifest payload is missing.");
            }

            ValidateNoSecrets(envelope.Payload);

            JsonSnapshotPackageManifestPayload payload;
            try
            {
                payload = envelope.Payload
                    .Deserialize<JsonSnapshotPackageManifestPayload>(
                        s_options)
                    ?? throw new InvalidDataException(
                        "The JSON snapshot package manifest payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The JSON snapshot package manifest payload is invalid.",
                    exception);
            }

            ValidatePayload(payload);
            ValidateSerializationBudget(payload);
            VerifyDigest(envelope.Digest, ComputeDigest(payload));

            byte[] canonicalBytes =
                SerializeEnvelope(payload, envelope.Digest);
            if (!utf8Json.Span.SequenceEqual(canonicalBytes))
            {
                throw new InvalidDataException(
                    "The JSON snapshot package manifest is not in the required canonical UTF-8 form.");
            }

            return payload;
        }
    }

    private static byte[] SerializeEnvelope(
        JsonSnapshotPackageManifestPayload payload,
        string digest)
    {
        try
        {
            JsonElement envelope = JsonSerializer.SerializeToElement(
                new JsonSnapshotPackageManifestEnvelope<
                    JsonSnapshotPackageManifestPayload>
                {
                    Format = Format,
                    DigestAlgorithm = DigestAlgorithm,
                    Digest = digest,
                    Payload = payload,
                },
                s_options);
            return JsonSnapshotPackageCanonicalJson.Serialize(envelope);
        }
        catch (Exception exception) when (
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest is invalid.",
                exception);
        }
    }

    private static string ComputeDigest(
        JsonSnapshotPackageManifestPayload payload)
    {
        byte[] canonicalBytes;
        try
        {
            JsonElement digestInput = JsonSerializer.SerializeToElement(
                new JsonSnapshotPackageManifestDigestInput
                {
                    Format = Format,
                    DigestAlgorithm = DigestAlgorithm,
                    Payload = payload,
                },
                s_options);
            canonicalBytes =
                JsonSnapshotPackageCanonicalJson.Serialize(digestInput);
        }
        catch (Exception exception) when (
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest payload is invalid.",
                exception);
        }

        return Convert.ToHexString(SHA256.HashData(canonicalBytes))
            .ToLowerInvariant();
    }

    private static void VerifyDigest(
        string? suppliedDigest,
        string expectedDigest)
    {
        if (suppliedDigest is null || suppliedDigest.Length != 64)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest digest is not lowercase 64-character SHA-256 text.");
        }

        foreach (char character in suppliedDigest)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                throw new InvalidDataException(
                    "The JSON snapshot package manifest digest is not lowercase 64-character SHA-256 text.");
            }
        }

        byte[] suppliedBytes = Convert.FromHexString(suppliedDigest);
        byte[] expectedBytes = Convert.FromHexString(expectedDigest);
        if (!CryptographicOperations.FixedTimeEquals(
                suppliedBytes,
                expectedBytes))
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest digest does not match its payload.");
        }
    }

    private static void ValidateInputEncoding(
        ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest is empty.");
        }
        if (utf8Json.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The JSON snapshot package manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest must not contain a UTF-8 BOM.");
        }
        if (utf8Json.IndexOf((byte)0) >= 0)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest must not contain NUL bytes.");
        }

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The JSON snapshot package manifest is not strict UTF-8.",
                exception);
        }
    }

    private static void ValidatePayload(
        JsonSnapshotPackageManifestPayload payload)
    {
        RequireMember(payload.Contracts, "payload.contracts");
        RequireMember(payload.Snapshot, "payload.snapshot");
        RequireMember(payload.Source, "payload.source");
        RequireMember(payload.Reader, "payload.reader");
        RequireMember(payload.Inference, "payload.inference");
        RequireMember(payload.Catalog, "payload.catalog");

        RequireText(
            payload.Contracts.Snapshot,
            "payload.contracts.snapshot");
        RequireText(
            payload.Contracts.Binding,
            "payload.contracts.binding");
        RequireText(
            payload.Contracts.Options,
            "payload.contracts.options");
        RequireText(
            payload.Contracts.Schema,
            "payload.contracts.schema");
        RequireText(
            payload.Contracts.Scalar,
            "payload.contracts.scalar");
        RequireText(
            payload.Contracts.CanonicalValue,
            "payload.contracts.canonicalValue");
        RequireText(
            payload.Contracts.CatalogFormat,
            "payload.contracts.catalogFormat");

        RequireText(
            payload.Snapshot.ContentDigest,
            "payload.snapshot.contentDigest");
        RequireText(
            payload.Snapshot.SnapshotIdentity,
            "payload.snapshot.snapshotIdentity");
        RequireText(
            payload.Source.Identity,
            "payload.source.identity");
        RequireText(
            payload.Source.Fingerprint,
            "payload.source.fingerprint");
        RequireText(
            payload.Source.OptionsDigest,
            "payload.source.optionsDigest");

        if (!Enum.IsDefined(payload.Reader.Framing))
        {
            throw new InvalidDataException(
                "JSON snapshot package manifest member 'payload.reader.framing' is invalid.");
        }

        RequireText(
            payload.Inference.TableName,
            "payload.inference.tableName");
        RequireMember(
            payload.Inference.ColumnOverrides,
            "payload.inference.columnOverrides");
        int previousIndex = -1;
        for (int index = 0;
             index < payload.Inference.ColumnOverrides.Count;
             index++)
        {
            JsonSnapshotPackageColumnOverrideManifest? item =
                payload.Inference.ColumnOverrides[index];
            RequireMember(
                item,
                $"payload.inference.columnOverrides[{index}]");
            if (item.ColumnIndex <= previousIndex)
            {
                throw new InvalidDataException(
                    "JSON snapshot package column overrides must be unique and ordered by ascending index.");
            }

            previousIndex = item.ColumnIndex;
            RequireText(
                item.ExpectedPropertyName,
                $"payload.inference.columnOverrides[{index}].expectedPropertyName");
            if (!Enum.IsDefined(item.LogicalType) ||
                !Enum.IsDefined(item.MissingPolicy))
            {
                throw new InvalidDataException(
                    $"JSON snapshot package column override {item.ColumnIndex} contains an invalid policy.");
            }
        }

        RequireText(
            payload.Catalog.TargetCSharpDbVersion,
            "payload.catalog.targetCSharpDbVersion");
        RequireText(
            payload.Catalog.Digest,
            "payload.catalog.digest");
    }

    private static void ValidateSerializationBudget(
        JsonSnapshotPackageManifestPayload payload)
    {
        long totalCharacters = 0;
        Add(payload.Contracts.Snapshot, "payload.contracts.snapshot");
        Add(payload.Contracts.Binding, "payload.contracts.binding");
        Add(payload.Contracts.Options, "payload.contracts.options");
        Add(payload.Contracts.Schema, "payload.contracts.schema");
        Add(payload.Contracts.Scalar, "payload.contracts.scalar");
        Add(
            payload.Contracts.CanonicalValue,
            "payload.contracts.canonicalValue");
        Add(
            payload.Contracts.CatalogFormat,
            "payload.contracts.catalogFormat");
        Add(
            payload.Snapshot.ContentDigest,
            "payload.snapshot.contentDigest");
        Add(
            payload.Snapshot.SnapshotIdentity,
            "payload.snapshot.snapshotIdentity");
        Add(payload.Source.Identity, "payload.source.identity");
        Add(payload.Source.Fingerprint, "payload.source.fingerprint");
        Add(
            payload.Source.OptionsDigest,
            "payload.source.optionsDigest");
        Add(
            payload.Inference.TableName,
            "payload.inference.tableName");
        for (int index = 0;
             index < payload.Inference.ColumnOverrides.Count;
             index++)
        {
            Add(
                payload.Inference.ColumnOverrides[index]
                    .ExpectedPropertyName,
                $"payload.inference.columnOverrides[{index}].expectedPropertyName");
        }
        Add(
            payload.Catalog.TargetCSharpDbVersion,
            "payload.catalog.targetCSharpDbVersion");
        Add(payload.Catalog.Digest, "payload.catalog.digest");

        void Add(string? value, string path)
        {
            if (value is null)
                return;

            totalCharacters = checked(
                totalCharacters + value.Length);
            if (totalCharacters > MaximumPayloadTextCharacters)
            {
                throw new InvalidDataException(
                    $"JSON snapshot package manifest text exceeds the {MaximumPayloadTextCharacters}-character serialization budget at '{path}'.");
            }
        }
    }

    private static void RequireMember(object? value, string path)
    {
        if (value is null)
        {
            throw new InvalidDataException(
                $"JSON snapshot package manifest member '{path}' is required.");
        }
    }

    private static void RequireText(string? value, string path)
    {
        RequireMember(value, path);
        ValidateText(value, path);
    }

    private static void ValidateText(string? value, string path)
    {
        if (value is null)
            return;

        try
        {
            _ = s_strictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                $"JSON snapshot package manifest member '{path}' contains invalid UTF-16 text.",
                exception);
        }
    }

    private static void RejectDuplicateProperties(
        JsonElement element,
        string path)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    if (!names.Add(property.Name))
                    {
                        throw new InvalidDataException(
                            $"JSON snapshot package manifest contains duplicate property '{path}.{property.Name}'.");
                    }

                    RejectDuplicateProperties(
                        property.Value,
                        $"{path}.{property.Name}");
                }

                break;

            case JsonValueKind.Array:
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    RejectDuplicateProperties(
                        item,
                        $"{path}[{index}]");
                    index++;
                }

                break;
        }
    }

    private static void ValidateNoSecrets(JsonElement payload) =>
        ValidateElement(payload, path: "$", propertyName: null);

    private static void ValidateElement(
        JsonElement element,
        string path,
        string? propertyName)
    {
        if (propertyName is not null &&
            IsSecretPropertyName(propertyName))
        {
            throw new InvalidDataException(
                $"JSON snapshot package manifests cannot contain secret-bearing property '{path}'.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty(
                        "name",
                        out JsonElement nameElement) &&
                    nameElement.ValueKind == JsonValueKind.String &&
                    IsSecretPropertyName(
                        nameElement.GetString() ?? string.Empty) &&
                    element.TryGetProperty("value", out _))
                {
                    throw new InvalidDataException(
                        $"JSON snapshot package manifest key/value entry '{path}' uses a secret-bearing key.");
                }

                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    ValidateElement(
                        property.Value,
                        $"{path}.{property.Name}",
                        property.Name);
                }

                break;

            case JsonValueKind.Array:
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    ValidateElement(
                        item,
                        $"{path}[{index}]",
                        propertyName: null);
                    index++;
                }

                break;

            case JsonValueKind.String:
                string? value = element.GetString();
                if (value is not null &&
                    value.Contains('\0', StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        $"JSON snapshot package manifest value '{path}' contains a NUL character.");
                }
                if (value is not null && LooksLikeSecret(value))
                {
                    throw new InvalidDataException(
                        $"JSON snapshot package manifest value '{path}' appears to contain credential material.");
                }

                break;
        }
    }

    private static bool LooksLikeSecret(string value) =>
        SecretAssignmentPattern().IsMatch(value) ||
        BearerAuthorizationPattern().IsMatch(value) ||
        CredentialUriPattern().IsMatch(value) ||
        PrivateKeyPattern().IsMatch(value);

    private static bool IsSecretPropertyName(string propertyName)
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

internal sealed record JsonSnapshotPackageManifestPayload
{
    [JsonPropertyOrder(0)]
    public required JsonSnapshotPackageContractIdsManifest Contracts
    {
        get;
        init;
    }

    [JsonPropertyOrder(1)]
    public required JsonSnapshotPackageSnapshotManifest Snapshot
    {
        get;
        init;
    }

    [JsonPropertyOrder(2)]
    public required JsonSnapshotPackageSourceManifest Source
    {
        get;
        init;
    }

    [JsonPropertyOrder(3)]
    public required JsonSnapshotPackageReaderManifest Reader
    {
        get;
        init;
    }

    [JsonPropertyOrder(4)]
    public required JsonSnapshotPackageInferenceManifest Inference
    {
        get;
        init;
    }

    [JsonPropertyOrder(5)]
    public required JsonSnapshotPackageCatalogManifest Catalog
    {
        get;
        init;
    }
}

internal sealed record JsonSnapshotPackageContractIdsManifest
{
    [JsonPropertyOrder(0)]
    public required string Snapshot { get; init; }

    [JsonPropertyOrder(1)]
    public required string Binding { get; init; }

    [JsonPropertyOrder(2)]
    public required string Options { get; init; }

    [JsonPropertyOrder(3)]
    public required string Schema { get; init; }

    [JsonPropertyOrder(4)]
    public required string Scalar { get; init; }

    [JsonPropertyOrder(5)]
    public required string CanonicalValue { get; init; }

    [JsonPropertyOrder(6)]
    public required string CatalogFormat { get; init; }
}

internal sealed record JsonSnapshotPackageSnapshotManifest
{
    [JsonPropertyOrder(0)]
    public required long ContentLength { get; init; }

    [JsonPropertyOrder(1)]
    public required string ContentDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required string SnapshotIdentity { get; init; }
}

internal sealed record JsonSnapshotPackageSourceManifest
{
    [JsonPropertyOrder(0)]
    public required string Identity { get; init; }

    [JsonPropertyOrder(1)]
    public required string Fingerprint { get; init; }

    [JsonPropertyOrder(2)]
    public required string OptionsDigest { get; init; }
}

internal sealed record JsonSnapshotPackageReaderManifest
{
    [JsonPropertyOrder(0)]
    public required JsonInputFraming Framing { get; init; }

    [JsonPropertyOrder(1)]
    public required int MaxValueBytes { get; init; }

    [JsonPropertyOrder(2)]
    public required int MaxDepth { get; init; }

    [JsonPropertyOrder(3)]
    public required int MaxPropertiesPerObject { get; init; }

    [JsonPropertyOrder(4)]
    public required int MaxArrayElements { get; init; }

    [JsonPropertyOrder(5)]
    public required int MaxTotalNodes { get; init; }

    [JsonPropertyOrder(6)]
    public required int MaxPropertyNameBytes { get; init; }

    [JsonPropertyOrder(7)]
    public required int MaxStringBytes { get; init; }

    [JsonPropertyOrder(8)]
    public required int MaxNumberBytes { get; init; }
}

internal sealed record JsonSnapshotPackageInferenceManifest
{
    [JsonPropertyOrder(0)]
    public required bool CollectProfile { get; init; }

    [JsonPropertyOrder(1)]
    public required int MaxProfileRecords { get; init; }

    [JsonPropertyOrder(2)]
    public required string TableName { get; init; }

    [JsonPropertyOrder(3)]
    public required int MaxColumns { get; init; }

    [JsonPropertyOrder(4)]
    public required long MaxTotalColumnNameBytes { get; init; }

    [JsonPropertyOrder(5)]
    public required long MaxProfileBytes { get; init; }

    [JsonPropertyOrder(6)]
    public required IReadOnlyList<
        JsonSnapshotPackageColumnOverrideManifest> ColumnOverrides
    {
        get;
        init;
    }
}

internal sealed record JsonSnapshotPackageColumnOverrideManifest
{
    [JsonPropertyOrder(0)]
    public required int ColumnIndex { get; init; }

    [JsonPropertyOrder(1)]
    public required string ExpectedPropertyName { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonTableColumnLogicalType LogicalType { get; init; }

    [JsonPropertyOrder(3)]
    public bool? Nullable { get; init; }

    [JsonPropertyOrder(4)]
    public required JsonMissingPropertyPolicy MissingPolicy { get; init; }
}

internal sealed record JsonSnapshotPackageCatalogManifest
{
    [JsonPropertyOrder(0)]
    public required string TargetCSharpDbVersion { get; init; }

    [JsonPropertyOrder(1)]
    public required string Digest { get; init; }
}

internal sealed record JsonSnapshotPackageManifestEnvelope<TPayload>
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

internal sealed record JsonSnapshotPackageManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonSnapshotPackageManifestPayload Payload
    {
        get;
        init;
    }
}
