using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Migration.Files.Csv;

internal static partial class CsvSnapshotPackageManifestSerializer
{
    internal const string Format = "csharpdb-csv-snapshot-package/v1";
    internal const string DigestAlgorithm = "sha256";
    internal const int MaximumManifestBytes = 16 * 1024 * 1024;
    internal const int MaximumJsonDepth = 64;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions s_options = CreateOptions();

    internal static byte[] Serialize(CsvSnapshotPackageManifestPayload payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ValidatePayload(payload);

        JsonElement payloadElement;
        try
        {
            payloadElement = JsonSerializer.SerializeToElement(payload, s_options);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV snapshot package manifest payload is invalid.", exception);
        }

        ValidateNoSecrets(payloadElement);
        string digest = ComputeDigest(payload);
        byte[] manifestBytes = SerializeEnvelope(payload, digest);
        if (manifestBytes.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The CSV snapshot package manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return manifestBytes;
    }

    internal static CsvSnapshotPackageManifestPayload Deserialize(ReadOnlySpan<byte> utf8Json)
    {
        ValidateInputEncoding(utf8Json);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(utf8Json.ToArray(), new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = MaximumJsonDepth,
            });
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The CSV snapshot package manifest JSON is invalid.", exception);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            CsvSnapshotPackageManifestEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<CsvSnapshotPackageManifestEnvelope<JsonElement>>(s_options)
                    ?? throw new InvalidDataException(
                        "The CSV snapshot package manifest did not contain an envelope.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV snapshot package manifest envelope is invalid.",
                    exception);
            }

            if (!string.Equals(envelope.Format, Format, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"CSV snapshot package manifest format '{envelope.Format}' is not supported.");
            }

            if (!string.Equals(envelope.DigestAlgorithm, DigestAlgorithm, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"CSV snapshot package manifest digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }

            if (envelope.Payload.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw new InvalidDataException("The CSV snapshot package manifest payload is missing.");

            ValidateNoSecrets(envelope.Payload);

            CsvSnapshotPackageManifestPayload payload;
            try
            {
                payload = envelope.Payload.Deserialize<CsvSnapshotPackageManifestPayload>(s_options)
                    ?? throw new InvalidDataException(
                        "The CSV snapshot package manifest payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV snapshot package manifest payload is invalid.",
                    exception);
            }

            ValidatePayload(payload);
            VerifyDigest(envelope.Digest, ComputeDigest(payload));

            byte[] canonicalBytes = SerializeEnvelope(payload, envelope.Digest);
            if (!utf8Json.SequenceEqual(canonicalBytes))
            {
                throw new InvalidDataException(
                    "The CSV snapshot package manifest is not in the required canonical UTF-8 form.");
            }

            return payload;
        }
    }

    private static byte[] SerializeEnvelope(
        CsvSnapshotPackageManifestPayload payload,
        string digest)
    {
        try
        {
            JsonElement envelope = JsonSerializer.SerializeToElement(
                new CsvSnapshotPackageManifestEnvelope<CsvSnapshotPackageManifestPayload>
                {
                    Format = Format,
                    DigestAlgorithm = DigestAlgorithm,
                    Digest = digest,
                    Payload = payload,
                },
                s_options);
            return CsvSnapshotPackageCanonicalJson.Serialize(envelope);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV snapshot package manifest is invalid.", exception);
        }
    }

    private static string ComputeDigest(CsvSnapshotPackageManifestPayload payload)
    {
        byte[] canonicalBytes;
        try
        {
            JsonElement digestInput = JsonSerializer.SerializeToElement(
                new CsvSnapshotPackageManifestDigestInput
                {
                    Format = Format,
                    DigestAlgorithm = DigestAlgorithm,
                    Payload = payload,
                },
                s_options);
            canonicalBytes = CsvSnapshotPackageCanonicalJson.Serialize(digestInput);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV snapshot package manifest payload is invalid.", exception);
        }

        return Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant();
    }

    private static void VerifyDigest(string? suppliedDigest, string expectedDigest)
    {
        if (suppliedDigest is null || suppliedDigest.Length != 64)
        {
            throw new InvalidDataException(
                "The CSV snapshot package manifest digest is not lowercase 64-character SHA-256 text.");
        }

        foreach (char character in suppliedDigest)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
            {
                throw new InvalidDataException(
                    "The CSV snapshot package manifest digest is not lowercase 64-character SHA-256 text.");
            }
        }

        byte[] suppliedBytes = Convert.FromHexString(suppliedDigest);
        byte[] expectedBytes = Convert.FromHexString(expectedDigest);
        if (!CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
        {
            throw new InvalidDataException(
                "The CSV snapshot package manifest digest does not match its payload.");
        }
    }

    private static void ValidateInputEncoding(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
            throw new InvalidDataException("The CSV snapshot package manifest is empty.");
        if (utf8Json.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The CSV snapshot package manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
            throw new InvalidDataException("The CSV snapshot package manifest must not contain a UTF-8 BOM.");
        if (utf8Json.IndexOf((byte)0) >= 0)
            throw new InvalidDataException("The CSV snapshot package manifest must not contain NUL bytes.");

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The CSV snapshot package manifest is not strict UTF-8.",
                exception);
        }
    }

    private static void ValidatePayload(CsvSnapshotPackageManifestPayload payload)
    {
        RequireMember(payload.Contracts, "payload.contracts");
        RequireMember(payload.Snapshot, "payload.snapshot");
        RequireMember(payload.Source, "payload.source");
        RequireMember(payload.Reader, "payload.reader");
        RequireMember(payload.Inference, "payload.inference");
        RequireMember(payload.Catalog, "payload.catalog");

        RequireMember(payload.Contracts.Snapshot, "payload.contracts.snapshot");
        RequireMember(payload.Contracts.Binding, "payload.contracts.binding");
        RequireMember(payload.Contracts.Format, "payload.contracts.format");
        RequireMember(payload.Contracts.Inspection, "payload.contracts.inspection");
        RequireMember(payload.Contracts.Schema, "payload.contracts.schema");
        RequireMember(payload.Contracts.Scalar, "payload.contracts.scalar");
        RequireMember(payload.Contracts.CatalogFormat, "payload.contracts.catalogFormat");

        ValidateText(payload.Contracts.Snapshot, "payload.contracts.snapshot");
        ValidateText(payload.Contracts.Binding, "payload.contracts.binding");
        ValidateText(payload.Contracts.Format, "payload.contracts.format");
        ValidateText(payload.Contracts.Inspection, "payload.contracts.inspection");
        ValidateText(payload.Contracts.Schema, "payload.contracts.schema");
        ValidateText(payload.Contracts.Scalar, "payload.contracts.scalar");
        ValidateText(payload.Contracts.CatalogFormat, "payload.contracts.catalogFormat");

        RequireMember(payload.Snapshot.ContentDigest, "payload.snapshot.contentDigest");
        RequireMember(payload.Snapshot.SnapshotIdentity, "payload.snapshot.snapshotIdentity");
        RequireMember(payload.Source.Identity, "payload.source.identity");
        RequireMember(payload.Source.Fingerprint, "payload.source.fingerprint");
        RequireMember(payload.Source.OptionsDigest, "payload.source.optionsDigest");

        ValidateText(payload.Snapshot.ContentDigest, "payload.snapshot.contentDigest");
        ValidateText(payload.Snapshot.SnapshotIdentity, "payload.snapshot.snapshotIdentity");
        ValidateText(payload.Source.Identity, "payload.source.identity");
        ValidateText(payload.Source.Fingerprint, "payload.source.fingerprint");
        ValidateText(payload.Source.OptionsDigest, "payload.source.optionsDigest");

        RequireMember(payload.Reader.Delimiter, "payload.reader.delimiter");
        RequireMember(payload.Reader.ConfiguredEncodingName, "payload.reader.configuredEncodingName");
        RequireMember(payload.Reader.ResolvedEncodingName, "payload.reader.resolvedEncodingName");
        RequireMember(payload.Reader.CultureName, "payload.reader.cultureName");
        RequireMember(payload.Reader.CulturePolicyDigest, "payload.reader.culturePolicyDigest");
        RequireMember(payload.Reader.NewlinePolicy, "payload.reader.newlinePolicy");

        ValidateText(payload.Reader.Delimiter, "payload.reader.delimiter");
        if (char.IsSurrogate(payload.Reader.Quote))
        {
            throw new InvalidDataException(
                "CSV snapshot package manifest member 'payload.reader.quote' contains invalid UTF-16 text.");
        }
        ValidateText(
            payload.Reader.ConfiguredEncodingName,
            "payload.reader.configuredEncodingName");
        ValidateText(payload.Reader.ResolvedEncodingName, "payload.reader.resolvedEncodingName");
        ValidateText(payload.Reader.CultureName, "payload.reader.cultureName");
        ValidateText(payload.Reader.CulturePolicyDigest, "payload.reader.culturePolicyDigest");
        ValidateText(payload.Reader.NullToken, "payload.reader.nullToken");
        ValidateText(payload.Reader.NewlinePolicy, "payload.reader.newlinePolicy");

        RequireMember(payload.Inference.TableName, "payload.inference.tableName");
        RequireMember(payload.Inference.ColumnOverrides, "payload.inference.columnOverrides");
        ValidateText(payload.Inference.TableName, "payload.inference.tableName");
        int previousIndex = -1;
        for (int index = 0; index < payload.Inference.ColumnOverrides.Count; index++)
        {
            CsvSnapshotPackageColumnOverrideManifest? item = payload.Inference.ColumnOverrides[index];
            RequireMember(item, $"payload.inference.columnOverrides[{index}]");
            if (item.Index <= previousIndex)
            {
                throw new InvalidDataException(
                    "CSV snapshot package column overrides must be unique and ordered by ascending index.");
            }

            previousIndex = item.Index;
            ValidateText(
                item.ExpectedHeader,
                $"payload.inference.columnOverrides[{index}].expectedHeader");
        }

        RequireMember(payload.Catalog.TargetCSharpDbVersion, "payload.catalog.targetCSharpDbVersion");
        RequireMember(payload.Catalog.Digest, "payload.catalog.digest");
        ValidateText(payload.Catalog.TargetCSharpDbVersion, "payload.catalog.targetCSharpDbVersion");
        ValidateText(payload.Catalog.Digest, "payload.catalog.digest");
    }

    private static void RequireMember(object? value, string path)
    {
        if (value is null)
            throw new InvalidDataException($"CSV snapshot package manifest member '{path}' is required.");
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
                $"CSV snapshot package manifest member '{path}' contains invalid UTF-16 text.",
                exception);
        }
    }

    private static void RejectDuplicateProperties(JsonElement element, string path)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (!names.Add(property.Name))
                    {
                        throw new InvalidDataException(
                            $"CSV snapshot package manifest contains duplicate property '{path}.{property.Name}'.");
                    }

                    RejectDuplicateProperties(property.Value, $"{path}.{property.Name}");
                }

                break;

            case JsonValueKind.Array:
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    RejectDuplicateProperties(item, $"{path}[{index}]");
                    index++;
                }

                break;
        }
    }

    private static void ValidateNoSecrets(JsonElement payload) =>
        ValidateElement(payload, path: "$", propertyName: null);

    private static void ValidateElement(JsonElement element, string path, string? propertyName)
    {
        if (propertyName is not null && IsSecretPropertyName(propertyName))
        {
            throw new InvalidDataException(
                $"CSV snapshot package manifests cannot contain secret-bearing property '{path}'.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty("name", out JsonElement nameElement) &&
                    nameElement.ValueKind == JsonValueKind.String &&
                    IsSecretPropertyName(nameElement.GetString() ?? string.Empty) &&
                    element.TryGetProperty("value", out _))
                {
                    throw new InvalidDataException(
                        $"CSV snapshot package manifest key/value entry '{path}' uses a secret-bearing key.");
                }

                foreach (JsonProperty property in element.EnumerateObject())
                {
                    ValidateElement(property.Value, $"{path}.{property.Name}", property.Name);
                }

                break;

            case JsonValueKind.Array:
                int index = 0;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    ValidateElement(item, $"{path}[{index}]", propertyName: null);
                    index++;
                }

                break;

            case JsonValueKind.String:
                string? value = element.GetString();
                if (value is not null && value.Contains('\0', StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        $"CSV snapshot package manifest value '{path}' contains a NUL character.");
                }
                if (value is not null && LooksLikeSecret(value))
                {
                    throw new InvalidDataException(
                        $"CSV snapshot package manifest value '{path}' appears to contain credential material.");
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
        string normalized = propertyName.Replace("_", string.Empty, StringComparison.Ordinal)
            .Replace("-", string.Empty, StringComparison.Ordinal)
            .Replace(" ", string.Empty, StringComparison.Ordinal)
            .ToLowerInvariant();

        return normalized is "password" or "pwd" or "token" or "secret" or "credential" or
            "connectionstring" or "apikey" or "accesskey" or "clientsecret" or "authorization";
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
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
        return options;
    }

    [GeneratedRegex(
        @"(?i)(password|pwd|token|secret|api[_ -]?key|access[_ -]?key|client[_ -]?secret)\s*=\s*[^;,\s]+",
        RegexOptions.CultureInvariant)]
    private static partial Regex SecretAssignmentPattern();

    [GeneratedRegex(@"(?i)authorization\s*:\s*bearer\s+\S+", RegexOptions.CultureInvariant)]
    private static partial Regex BearerAuthorizationPattern();

    [GeneratedRegex(@"(?i)\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^@\s/]+@", RegexOptions.CultureInvariant)]
    private static partial Regex CredentialUriPattern();

    [GeneratedRegex(@"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----", RegexOptions.CultureInvariant)]
    private static partial Regex PrivateKeyPattern();
}

internal sealed record CsvSnapshotPackageManifestPayload
{
    [JsonPropertyOrder(0)]
    public required CsvSnapshotPackageContractIdsManifest Contracts { get; init; }

    [JsonPropertyOrder(1)]
    public required CsvSnapshotPackageSnapshotManifest Snapshot { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvSnapshotPackageSourceManifest Source { get; init; }

    [JsonPropertyOrder(3)]
    public required CsvSnapshotPackageReaderManifest Reader { get; init; }

    [JsonPropertyOrder(4)]
    public required CsvSnapshotPackageInferenceManifest Inference { get; init; }

    [JsonPropertyOrder(5)]
    public required CsvSnapshotPackageCatalogManifest Catalog { get; init; }
}

internal sealed record CsvSnapshotPackageContractIdsManifest
{
    [JsonPropertyOrder(0)]
    public required string Snapshot { get; init; }

    [JsonPropertyOrder(1)]
    public required string Binding { get; init; }

    [JsonPropertyOrder(2)]
    public required string Format { get; init; }

    [JsonPropertyOrder(3)]
    public required string Inspection { get; init; }

    [JsonPropertyOrder(4)]
    public required string Schema { get; init; }

    [JsonPropertyOrder(5)]
    public required string Scalar { get; init; }

    [JsonPropertyOrder(6)]
    public required string CatalogFormat { get; init; }
}

internal sealed record CsvSnapshotPackageSnapshotManifest
{
    [JsonPropertyOrder(0)]
    public required long ContentLength { get; init; }

    [JsonPropertyOrder(1)]
    public required string ContentDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required string SnapshotIdentity { get; init; }
}

internal sealed record CsvSnapshotPackageSourceManifest
{
    [JsonPropertyOrder(0)]
    public required string Identity { get; init; }

    [JsonPropertyOrder(1)]
    public required string Fingerprint { get; init; }

    [JsonPropertyOrder(2)]
    public required string OptionsDigest { get; init; }
}

internal sealed record CsvSnapshotPackageReaderManifest
{
    [JsonPropertyOrder(0)]
    public required bool HasHeaderRecord { get; init; }

    [JsonPropertyOrder(1)]
    public required string Delimiter { get; init; }

    [JsonPropertyOrder(2)]
    public required char Quote { get; init; }

    [JsonPropertyOrder(3)]
    public required string ConfiguredEncodingName { get; init; }

    [JsonPropertyOrder(4)]
    public required int ConfiguredEncodingCodePage { get; init; }

    [JsonPropertyOrder(5)]
    public required bool DetectEncodingFromByteOrderMarks { get; init; }

    [JsonPropertyOrder(6)]
    public required string ResolvedEncodingName { get; init; }

    [JsonPropertyOrder(7)]
    public required int ResolvedEncodingCodePage { get; init; }

    [JsonPropertyOrder(8)]
    public required bool HasByteOrderMark { get; init; }

    [JsonPropertyOrder(9)]
    public required string CultureName { get; init; }

    [JsonPropertyOrder(10)]
    public required bool CultureUseUserOverride { get; init; }

    [JsonPropertyOrder(11)]
    public required string CulturePolicyDigest { get; init; }

    [JsonPropertyOrder(12)]
    public string? NullToken { get; init; }

    [JsonPropertyOrder(13)]
    public required bool NullTokenMatchesQuotedFields { get; init; }

    [JsonPropertyOrder(14)]
    public int? ExpectedFieldCount { get; init; }

    [JsonPropertyOrder(15)]
    public required string NewlinePolicy { get; init; }

    [JsonPropertyOrder(16)]
    public required int MaxFieldCharacters { get; init; }

    [JsonPropertyOrder(17)]
    public required int MaxRecordCharacters { get; init; }

    [JsonPropertyOrder(18)]
    public required int MaxFieldsPerRecord { get; init; }
}

internal sealed record CsvSnapshotPackageInferenceManifest
{
    [JsonPropertyOrder(0)]
    public required bool CollectProfile { get; init; }

    [JsonPropertyOrder(1)]
    public required int MaxDataRecords { get; init; }

    [JsonPropertyOrder(2)]
    public required long MaxProfileCharacters { get; init; }

    [JsonPropertyOrder(3)]
    public required string TableName { get; init; }

    [JsonPropertyOrder(4)]
    public required IReadOnlyList<CsvSnapshotPackageColumnOverrideManifest> ColumnOverrides { get; init; }
}

internal sealed record CsvSnapshotPackageColumnOverrideManifest
{
    [JsonPropertyOrder(0)]
    public required int Index { get; init; }

    [JsonPropertyOrder(1)]
    public string? ExpectedHeader { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvColumnLogicalType LogicalType { get; init; }

    [JsonPropertyOrder(3)]
    public bool? Nullable { get; init; }
}

internal sealed record CsvSnapshotPackageCatalogManifest
{
    [JsonPropertyOrder(0)]
    public required string TargetCSharpDbVersion { get; init; }

    [JsonPropertyOrder(1)]
    public required string Digest { get; init; }
}

internal sealed record CsvSnapshotPackageManifestEnvelope<TPayload>
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

internal sealed record CsvSnapshotPackageManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvSnapshotPackageManifestPayload Payload { get; init; }
}
