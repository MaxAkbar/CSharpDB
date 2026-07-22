using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Migration;

public static partial class MigrationArtifactSerializer
{
    private const int MaxArtifactCharacters = 256 * 1024 * 1024;
    private const int MaxJsonDepth = 64;

    private static readonly JsonSerializerOptions s_compactOptions = CreateOptions(writeIndented: false);
    private static readonly JsonSerializerOptions s_indentedOptions = CreateOptions(writeIndented: true);

    public static string SerializeCatalog(MigrationCatalog catalog, bool writeIndented = true)
    {
        MigrationCatalog normalized = MigrationArtifactNormalizer.Normalize(catalog);
        MigrationContractValidator.ValidateCatalog(normalized);
        return SerializeNormalized(MigrationArtifactKind.Catalog, normalized, writeIndented);
    }

    public static MigrationCatalog DeserializeCatalog(string json)
    {
        JsonElement payload = ReadVerifiedPayload(MigrationArtifactKind.Catalog, json);
        MigrationCatalog catalog = DeserializePayload<MigrationCatalog>(payload);
        MigrationCatalog normalized = MigrationArtifactNormalizer.Normalize(catalog);
        MigrationContractValidator.ValidateCatalog(normalized);
        RequireCanonicalPayload(payload, normalized);
        return normalized;
    }

    public static string SerializePlan(
        MigrationPlan plan,
        MigrationCatalog catalog,
        bool writeIndented = true) =>
        SerializePlanCore(plan, catalog, mappingPolicy: null, writeIndented);

    public static string SerializePlan(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IDataTypeMappingProvider mappingPolicy,
        bool writeIndented = true)
    {
        ArgumentNullException.ThrowIfNull(mappingPolicy);
        return SerializePlanCore(plan, catalog, mappingPolicy, writeIndented);
    }

    private static string SerializePlanCore(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IDataTypeMappingProvider? mappingPolicy,
        bool writeIndented)
    {
        MigrationCatalog normalizedCatalog = MigrationArtifactNormalizer.Normalize(catalog);
        MigrationPlan normalizedPlan = MigrationArtifactNormalizer.Normalize(plan);
        string catalogDigest = ComputeCatalogDigest(normalizedCatalog);
        CSharpDbCapabilityCatalog capabilities = CSharpDbCapabilityCatalogLoader.LoadEmbedded(
            normalizedPlan.TargetCSharpDbVersion);
        MigrationContractValidator.ValidatePlan(
            normalizedPlan,
            normalizedCatalog,
            catalogDigest,
            capabilities.Digest,
            mappingPolicy);
        return SerializeNormalized(MigrationArtifactKind.Plan, normalizedPlan, writeIndented);
    }

    public static MigrationPlan DeserializePlan(string json, MigrationCatalog catalog) =>
        DeserializePlanCore(json, catalog, mappingPolicy: null);

    public static MigrationPlan DeserializePlan(
        string json,
        MigrationCatalog catalog,
        IDataTypeMappingProvider mappingPolicy)
    {
        ArgumentNullException.ThrowIfNull(mappingPolicy);
        return DeserializePlanCore(json, catalog, mappingPolicy);
    }

    private static MigrationPlan DeserializePlanCore(
        string json,
        MigrationCatalog catalog,
        IDataTypeMappingProvider? mappingPolicy)
    {
        MigrationCatalog normalizedCatalog = MigrationArtifactNormalizer.Normalize(catalog);
        JsonElement payload = ReadVerifiedPayload(MigrationArtifactKind.Plan, json);
        MigrationPlan plan = DeserializePayload<MigrationPlan>(payload);
        MigrationPlan normalizedPlan = MigrationArtifactNormalizer.Normalize(plan);
        CSharpDbCapabilityCatalog capabilities = CSharpDbCapabilityCatalogLoader.LoadEmbedded(
            normalizedPlan.TargetCSharpDbVersion);
        MigrationContractValidator.ValidatePlan(
            normalizedPlan,
            normalizedCatalog,
            ComputeCatalogDigest(normalizedCatalog),
            capabilities.Digest,
            mappingPolicy);
        RequireCanonicalPayload(payload, normalizedPlan);
        return normalizedPlan;
    }

    public static string ComputeCatalogDigest(MigrationCatalog catalog)
    {
        MigrationCatalog normalized = MigrationArtifactNormalizer.Normalize(catalog);
        MigrationContractValidator.ValidateCatalog(normalized);
        JsonElement payload = JsonSerializer.SerializeToElement(normalized, s_compactOptions);
        ValidateNoSecrets(payload);
        return ComputeDigest(MigrationArtifactFormats.CatalogV1, payload);
    }

    /// <summary>
    /// Computes the deterministic digest used by a serialized plan envelope.
    /// Callers that bind this digest for execution must first validate the plan
    /// against its catalog (serialization and deserialization already do so).
    /// </summary>
    public static string ComputePlanDigest(MigrationPlan plan)
    {
        MigrationPlan normalized = MigrationArtifactNormalizer.Normalize(plan);
        JsonElement payload = JsonSerializer.SerializeToElement(normalized, s_compactOptions);
        ValidateNoSecrets(payload);
        return ComputeDigest(MigrationArtifactFormats.PlanV1, payload);
    }

    private static string SerializeNormalized<TPayload>(
        MigrationArtifactKind kind,
        TPayload normalizedPayload,
        bool writeIndented)
    {
        JsonElement payload = JsonSerializer.SerializeToElement(normalizedPayload, s_compactOptions);
        ValidateNoSecrets(payload);

        string format = MigrationArtifactFormats.For(kind);
        var envelope = new MigrationArtifactEnvelope<JsonElement>
        {
            Format = format,
            DigestAlgorithm = MigrationArtifactFormats.DigestAlgorithm,
            Digest = ComputeDigest(format, payload),
            Payload = payload,
        };

        return JsonSerializer.Serialize(envelope, writeIndented ? s_indentedOptions : s_compactOptions);
    }

    private static JsonElement ReadVerifiedPayload(MigrationArtifactKind kind, string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        if (json.Length > MaxArtifactCharacters)
        {
            throw new InvalidDataException(
                $"Migration artifact exceeds the {MaxArtifactCharacters}-character safety limit.");
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json, new JsonDocumentOptions
            {
                AllowTrailingCommas = false,
                CommentHandling = JsonCommentHandling.Disallow,
                MaxDepth = MaxJsonDepth,
            });
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Migration artifact JSON is invalid.", ex);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            MigrationArtifactEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement.Deserialize<MigrationArtifactEnvelope<JsonElement>>(s_compactOptions)
                    ?? throw new InvalidDataException("Migration artifact JSON did not contain an artifact envelope.");
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException("Migration artifact JSON is invalid.", ex);
            }

            string expectedFormat = MigrationArtifactFormats.For(kind);
            if (!string.Equals(envelope.Format, expectedFormat, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Migration artifact format '{envelope.Format}' does not match expected format '{expectedFormat}'.");
            }

            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    MigrationArtifactFormats.DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Migration artifact digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }

            if (envelope.Payload.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw new InvalidDataException("Migration artifact payload is missing.");

            ValidateNoSecrets(envelope.Payload);
            VerifyDigest(envelope.Digest, ComputeDigest(expectedFormat, envelope.Payload));
            return envelope.Payload.Clone();
        }
    }

    private static TPayload DeserializePayload<TPayload>(JsonElement payload)
    {
        try
        {
            return payload.Deserialize<TPayload>(s_compactOptions)
                ?? throw new InvalidDataException("Migration artifact payload is missing.");
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException("Migration artifact payload is invalid.", ex);
        }
    }

    private static void RequireCanonicalPayload<TPayload>(JsonElement supplied, TPayload normalized)
    {
        byte[] suppliedBytes = JsonSerializer.SerializeToUtf8Bytes(supplied, s_compactOptions);
        byte[] normalizedBytes = JsonSerializer.SerializeToUtf8Bytes(normalized, s_compactOptions);
        if (!suppliedBytes.AsSpan().SequenceEqual(normalizedBytes))
        {
            throw new InvalidDataException(
                "Migration artifact payload is not in the required deterministic order or shape.");
        }
    }

    private static string ComputeDigest(string format, JsonElement payload)
    {
        byte[] canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(
            new MigrationDigestInput
            {
                Format = format,
                DigestAlgorithm = MigrationArtifactFormats.DigestAlgorithm,
                Payload = payload,
            },
            s_compactOptions);

        return Convert.ToHexString(SHA256.HashData(canonicalBytes)).ToLowerInvariant();
    }

    private static void VerifyDigest(string? suppliedDigest, string expectedDigest)
    {
        if (string.IsNullOrWhiteSpace(suppliedDigest))
            throw new InvalidDataException("Migration artifact digest is missing.");
        if (suppliedDigest.Length != 64 || !suppliedDigest.All(Uri.IsHexDigit))
        {
            throw new InvalidDataException(
                "Migration artifact digest is not a 64-character hexadecimal SHA-256 value.");
        }

        byte[] suppliedBytes;
        try
        {
            suppliedBytes = Convert.FromHexString(suppliedDigest);
        }
        catch (FormatException ex)
        {
            throw new InvalidDataException("Migration artifact digest is not valid hexadecimal SHA-256.", ex);
        }

        byte[] expectedBytes = Convert.FromHexString(expectedDigest);
        if (suppliedBytes.Length != expectedBytes.Length ||
            !CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
        {
            throw new InvalidDataException("Migration artifact digest does not match its payload.");
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
                            $"Migration artifact contains duplicate property '{path}.{property.Name}'.");
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

    internal static void ValidateNoSecrets(JsonElement payload) =>
        ValidateElement(payload, path: "$", propertyName: null);

    private static void ValidateElement(JsonElement element, string path, string? propertyName)
    {
        if (propertyName is not null && IsSecretPropertyName(propertyName))
        {
            throw new InvalidDataException(
                $"Migration artifacts cannot contain secret-bearing property '{path}'.");
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
                        $"Migration artifact key/value entry '{path}' uses a secret-bearing key.");
                }

                foreach (JsonProperty property in element.EnumerateObject())
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
                    ValidateElement(item, $"{path}[{index}]", propertyName: null);
                    index++;
                }

                break;

            case JsonValueKind.String:
                string? value = element.GetString();
                if (value is not null && LooksLikeSecret(value))
                {
                    throw new InvalidDataException(
                        $"Migration artifact value '{path}' appears to contain credential material.");
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

    private static JsonSerializerOptions CreateOptions(bool writeIndented)
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = writeIndented,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = false,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
            AllowTrailingCommas = false,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            MaxDepth = MaxJsonDepth,
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: false));
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

    private sealed record MigrationDigestInput
    {
        public required string Format { get; init; }

        public required string DigestAlgorithm { get; init; }

        public required JsonElement Payload { get; init; }
    }
}
