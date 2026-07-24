using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace CSharpDB.Migration.Files.Json;

internal static partial class JsonTypedIntentManifestSerializer
{
    internal const string Format =
        "csharpdb-json-table-intent/v1";
    internal const string DigestAlgorithm = "sha256";
    internal const string TypedValueContract =
        "csharpdb-json-typed-value/v1";
    internal const string TextCodecContract =
        "csharpdb-text-codec/v1";
    internal const int MaximumManifestBytes =
        4 * 1024 * 1024;
    internal const int MaximumJsonDepth = 64;
    internal const int MaximumPayloadTextCharacters =
        1024 * 1024;
    internal const int MaximumColumns =
        JsonTableSchemaInferenceOptions.MaximumSupportedColumns;
    internal const int MaximumDecodedBinaryBytes =
        12 * 1024 * 1024;
    internal const int MaximumDecimalDigits =
        JsonInputContracts.MaximumNumberBytes;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions s_options =
        CreateOptions();

    internal static byte[] Serialize(
        JsonTypedIntentManifestPayload payload)
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
                "The typed JSON intent payload is invalid.",
                exception);
        }

        ValidateNoSecrets(payloadElement);
        string digest = ComputePayloadDigest(payload);
        byte[] manifestBytes =
            SerializeEnvelope(payload, digest);
        if (manifestBytes.Length > MaximumManifestBytes)
        {
            throw new JsonTypedIntentManifestValidationException(
                JsonTypedIntentManifestFailureKind.Limit,
                $"The typed JSON intent manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return manifestBytes;
    }

    internal static JsonTypedIntentManifestPayload Deserialize(
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
                "The typed JSON intent manifest JSON is invalid.",
                exception);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement);

            JsonTypedIntentManifestEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<
                        JsonTypedIntentManifestEnvelope<JsonElement>>(
                        s_options)
                    ?? throw new InvalidDataException(
                        "The typed JSON intent manifest envelope is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The typed JSON intent manifest envelope is invalid.",
                    exception);
            }

            if (!string.Equals(
                    envelope.Format,
                    Format,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The typed JSON intent format is not supported.");
            }
            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    DigestAlgorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    "The typed JSON intent digest algorithm is not supported.");
            }
            if (envelope.Payload.ValueKind is
                JsonValueKind.Null or JsonValueKind.Undefined)
            {
                throw new InvalidDataException(
                    "The typed JSON intent payload is missing.");
            }

            ValidateNoSecrets(envelope.Payload);

            JsonTypedIntentManifestPayload payload;
            try
            {
                payload = envelope.Payload
                    .Deserialize<JsonTypedIntentManifestPayload>(
                        s_options)
                    ?? throw new InvalidDataException(
                        "The typed JSON intent payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The typed JSON intent payload is invalid.",
                    exception);
            }

            VerifyPayloadDigest(
                envelope.Digest,
                ComputePayloadDigest(payload));
            ValidatePayload(payload);
            ValidateSerializationBudget(payload);

            byte[] canonical =
                SerializeEnvelope(payload, envelope.Digest);
            try
            {
                if (!utf8Json.Span.SequenceEqual(canonical))
                {
                    throw new InvalidDataException(
                        "The typed JSON intent manifest is not in canonical UTF-8 form.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(canonical);
            }

            return payload;
        }
    }

    internal static void ValidatePayload(
        JsonTypedIntentManifestPayload payload)
    {
        RequireMember(payload.Contracts, "payload.contracts");
        RequireMember(payload.Source, "payload.source");
        RequireMember(payload.Limits, "payload.limits");
        RequireMember(payload.Columns, "payload.columns");

        RequireText(
            payload.Contracts.SourceBinding,
            "payload.contracts.sourceBinding");
        RequireText(
            payload.Contracts.ReaderOptions,
            "payload.contracts.readerOptions");
        RequireText(
            payload.Contracts.PropertyNameComparison,
            "payload.contracts.propertyNameComparison");
        RequireText(
            payload.Contracts.TypedValue,
            "payload.contracts.typedValue");
        RequireText(
            payload.Contracts.TextCodec,
            "payload.contracts.textCodec");
        if (!string.Equals(
                payload.Contracts.SourceBinding,
                JsonSourceBinding.SourceFingerprintAlgorithm,
                StringComparison.Ordinal) ||
            !string.Equals(
                payload.Contracts.ReaderOptions,
                JsonSourceBinding.OptionsAlgorithm,
                StringComparison.Ordinal) ||
            !string.Equals(
                payload.Contracts.PropertyNameComparison,
                JsonInputContracts.DecodedPropertyNameComparison,
                StringComparison.Ordinal) ||
            !string.Equals(
                payload.Contracts.TypedValue,
                TypedValueContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                payload.Contracts.TextCodec,
                TextCodecContract,
                StringComparison.Ordinal))
        {
            throw new JsonTypedIntentManifestValidationException(
                JsonTypedIntentManifestFailureKind.Policy,
                "The typed JSON intent manifest uses an unsupported contract version.");
        }

        RequireText(
            payload.Source.SnapshotIdentity,
            "payload.source.snapshotIdentity");
        RequireText(
            payload.Source.ContentDigest,
            "payload.source.contentDigest");
        RequireText(
            payload.Source.Identity,
            "payload.source.identity");
        RequireText(
            payload.Source.Fingerprint,
            "payload.source.fingerprint");
        RequireText(
            payload.Source.OptionsDigest,
            "payload.source.optionsDigest");
        if (payload.Source.ContentLength < 0 ||
            !IsCanonicalPrefixedDigest(
                payload.Source.ContentDigest) ||
            !IsCanonicalSafeSourceIdentity(
                payload.Source.Identity,
                payload.Source.ContentDigest) ||
            !IsCanonicalPrefixedDigest(
                payload.Source.Fingerprint) ||
            !IsCanonicalPrefixedDigest(
                payload.Source.OptionsDigest) ||
            !string.Equals(
                payload.Source.SnapshotIdentity,
                $"{JsonSourceSnapshot.IdentityAlgorithm}:{payload.Source.ContentDigest}:bytes:{payload.Source.ContentLength}",
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                "The typed JSON intent source binding is noncanonical.");
        }

        if (payload.Limits.MaxDecodedBinaryBytes is
                < 1 or > MaximumDecodedBinaryBytes ||
            payload.Limits.MaxDecimalDigits is
                < 1 or > MaximumDecimalDigits)
        {
            throw new JsonTypedIntentManifestValidationException(
                JsonTypedIntentManifestFailureKind.Limit,
                "The typed JSON intent resource policy exceeds supported safety ceilings.");
        }

        if (payload.Columns.Count is < 1 or > MaximumColumns)
        {
            throw new JsonTypedIntentManifestValidationException(
                JsonTypedIntentManifestFailureKind.Limit,
                "The typed JSON intent column count exceeds supported safety ceilings.");
        }

        int previousColumnIndex = -1;
        var propertyNames =
            new HashSet<string>(StringComparer.Ordinal);
        for (int index = 0;
             index < payload.Columns.Count;
             index++)
        {
            JsonTypedColumnIntent? column =
                payload.Columns[index];
            RequireMember(
                column,
                $"payload.columns[{index}]");
            RequireText(
                column.ExpectedPropertyName,
                $"payload.columns[{index}].expectedPropertyName");
            if (column.ColumnIndex <= previousColumnIndex ||
                column.ColumnIndex >= MaximumColumns)
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Policy,
                    "Typed JSON intent columns must have unique ascending indexes within the supported range.");
            }
            previousColumnIndex = column.ColumnIndex;
            if (!propertyNames.Add(
                    column.ExpectedPropertyName))
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Policy,
                    "Typed JSON intent columns must have unique decoded property names.");
            }

            if (!Enum.IsDefined(column.Codec) ||
                !Enum.IsDefined(column.MissingPolicy))
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Policy,
                    $"Typed JSON intent column {column.ColumnIndex} contains an invalid policy.");
            }
            if (column.MissingPolicy ==
                    JsonMissingPropertyPolicy.AsNull &&
                column.Nullable == false)
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Policy,
                    $"Typed JSON intent column {column.ColumnIndex} cannot combine missing-as-null with non-nullability.");
            }

            bool decimalCodec = column.Codec is
                JsonTypedValueCodec.DecimalString or
                JsonTypedValueCodec.DecimalNumber;
            if (decimalCodec)
            {
                if (column.Precision is not int precision ||
                    column.Scale is not int scale ||
                    precision < 1 ||
                    precision >
                        payload.Limits.MaxDecimalDigits ||
                    scale < 0 ||
                    scale > precision)
                {
                    throw new JsonTypedIntentManifestValidationException(
                        JsonTypedIntentManifestFailureKind.Policy,
                        $"Typed JSON intent decimal column {column.ColumnIndex} has invalid precision or scale.");
                }
            }
            else if (column.Precision is not null ||
                     column.Scale is not null)
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Policy,
                    $"Typed JSON intent non-decimal column {column.ColumnIndex} cannot declare decimal facets.");
            }
        }
    }

    private static byte[] SerializeEnvelope(
        JsonTypedIntentManifestPayload payload,
        string digest)
    {
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonTypedIntentManifestEnvelope<
                        JsonTypedIntentManifestPayload>
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
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest is invalid.",
                exception);
        }
    }

    private static string ComputePayloadDigest(
        JsonTypedIntentManifestPayload payload)
    {
        byte[] canonical;
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonTypedIntentManifestDigestInput
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
            exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException(
                "The typed JSON intent digest input is invalid.",
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

    private static void VerifyPayloadDigest(
        string? suppliedDigest,
        string expectedDigest)
    {
        if (!IsCanonicalHexDigest(suppliedDigest))
        {
            throw new InvalidDataException(
                "The typed JSON intent payload digest is not canonical SHA-256 text.");
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
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Integrity,
                    "The typed JSON intent payload digest does not match its payload.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(supplied);
            CryptographicOperations.ZeroMemory(expected);
        }
    }

    private static void ValidateInputEncoding(
        ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest is empty.");
        }
        if (utf8Json.Length > MaximumManifestBytes)
        {
            throw new JsonTypedIntentManifestValidationException(
                JsonTypedIntentManifestFailureKind.Limit,
                $"The typed JSON intent manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest must not contain a UTF-8 BOM.");
        }
        if (utf8Json.IndexOf((byte)0) >= 0)
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest must not contain NUL bytes.");
        }

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest is not strict UTF-8.",
                exception);
        }
    }

    private static void ValidateSerializationBudget(
        JsonTypedIntentManifestPayload payload)
    {
        long totalCharacters = 0;
        Add(
            payload.Contracts.SourceBinding,
            "payload.contracts.sourceBinding");
        Add(
            payload.Contracts.ReaderOptions,
            "payload.contracts.readerOptions");
        Add(
            payload.Contracts.PropertyNameComparison,
            "payload.contracts.propertyNameComparison");
        Add(
            payload.Contracts.TypedValue,
            "payload.contracts.typedValue");
        Add(
            payload.Contracts.TextCodec,
            "payload.contracts.textCodec");
        Add(
            payload.Source.SnapshotIdentity,
            "payload.source.snapshotIdentity");
        Add(
            payload.Source.ContentDigest,
            "payload.source.contentDigest");
        Add(
            payload.Source.Identity,
            "payload.source.identity");
        Add(
            payload.Source.Fingerprint,
            "payload.source.fingerprint");
        Add(
            payload.Source.OptionsDigest,
            "payload.source.optionsDigest");
        for (int index = 0;
             index < payload.Columns.Count;
             index++)
        {
            Add(
                payload.Columns[index].ExpectedPropertyName,
                $"payload.columns[{index}].expectedPropertyName");
        }

        void Add(string? value, string path)
        {
            if (value is null)
                return;

            totalCharacters =
                checked(totalCharacters + value.Length);
            if (totalCharacters >
                MaximumPayloadTextCharacters)
            {
                throw new JsonTypedIntentManifestValidationException(
                    JsonTypedIntentManifestFailureKind.Limit,
                    $"Typed JSON intent text exceeds the {MaximumPayloadTextCharacters}-character budget.");
            }
        }
    }

    private static void RequireMember(
        object? value,
        string path)
    {
        if (value is null)
        {
            throw new InvalidDataException(
                $"Typed JSON intent member '{path}' is required.");
        }
    }

    private static void RequireText(
        string? value,
        string path)
    {
        RequireMember(value, path);
        try
        {
            _ = s_strictUtf8.GetByteCount(value!);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                $"Typed JSON intent member '{path}' contains invalid Unicode.",
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
                            "The typed JSON intent manifest contains a duplicate member.");
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

    private static void ValidateNoSecrets(JsonElement payload) =>
        ValidateElement(payload, propertyName: null);

    private static void ValidateElement(
        JsonElement element,
        string? propertyName)
    {
        if (propertyName is not null &&
            IsSecretPropertyName(propertyName))
        {
            throw new InvalidDataException(
                "The typed JSON intent manifest cannot contain secret-bearing members.");
        }

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty(
                        "name",
                        out JsonElement nameElement) &&
                    nameElement.ValueKind ==
                        JsonValueKind.String &&
                    IsSecretPropertyName(
                        nameElement.GetString() ??
                        string.Empty) &&
                    element.TryGetProperty("value", out _))
                {
                    throw new InvalidDataException(
                        "The typed JSON intent manifest cannot contain secret-bearing members.");
                }

                foreach (JsonProperty property in
                         element.EnumerateObject())
                {
                    ValidateElement(
                        property.Value,
                        property.Name);
                }
                break;

            case JsonValueKind.Array:
                foreach (JsonElement item in
                         element.EnumerateArray())
                {
                    ValidateElement(
                        item,
                        propertyName: null);
                }
                break;

            case JsonValueKind.String:
                string? value = element.GetString();
                if (value is not null &&
                    value.Contains(
                        '\0',
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException(
                        "The typed JSON intent manifest cannot contain NUL text.");
                }
                if (value is not null &&
                    LooksLikeSecret(value))
                {
                    throw new InvalidDataException(
                        "The typed JSON intent manifest cannot contain credential material.");
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
            .Replace(
                "_",
                string.Empty,
                StringComparison.Ordinal)
            .Replace(
                "-",
                string.Empty,
                StringComparison.Ordinal)
            .Replace(
                " ",
                string.Empty,
                StringComparison.Ordinal)
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

    private static bool IsCanonicalPrefixedDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        IsLowerHex(digest.AsSpan(7));

    private static bool IsCanonicalSafeSourceIdentity(
        string? identity,
        string contentDigest)
    {
        if (string.Equals(
                identity,
                "json-content:" + contentDigest,
                StringComparison.Ordinal))
        {
            return true;
        }

        const string logicalPrefix = "json-logical:";
        return identity is not null &&
            identity.StartsWith(
                logicalPrefix,
                StringComparison.Ordinal) &&
            IsCanonicalPrefixedDigest(
                identity[logicalPrefix.Length..]);
    }

    private static bool IsCanonicalHexDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 64 &&
        IsLowerHex(digest);

    private static bool IsLowerHex(
        ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
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

internal enum JsonTypedIntentManifestFailureKind
{
    Integrity,
    Policy,
    Limit,
}

internal sealed class JsonTypedIntentManifestValidationException
    : Exception
{
    internal JsonTypedIntentManifestValidationException(
        JsonTypedIntentManifestFailureKind failureKind,
        string message)
        : base(message)
    {
        FailureKind = failureKind;
    }

    internal JsonTypedIntentManifestFailureKind FailureKind
    {
        get;
    }
}

internal sealed record JsonTypedIntentManifestPayload
{
    [JsonPropertyOrder(0)]
    public required JsonTypedIntentContractsManifest Contracts
    {
        get;
        init;
    }

    [JsonPropertyOrder(1)]
    public required JsonTypedIntentSourceManifest Source
    {
        get;
        init;
    }

    [JsonPropertyOrder(2)]
    public required JsonTypedIntentLimitsManifest Limits
    {
        get;
        init;
    }

    [JsonPropertyOrder(3)]
    public required IReadOnlyList<JsonTypedColumnIntent> Columns
    {
        get;
        init;
    }
}

internal sealed record JsonTypedIntentContractsManifest
{
    [JsonPropertyOrder(0)]
    public required string SourceBinding { get; init; }

    [JsonPropertyOrder(1)]
    public required string ReaderOptions { get; init; }

    [JsonPropertyOrder(2)]
    public required string PropertyNameComparison { get; init; }

    [JsonPropertyOrder(3)]
    public required string TypedValue { get; init; }

    [JsonPropertyOrder(4)]
    public required string TextCodec { get; init; }
}

internal sealed record JsonTypedIntentSourceManifest
{
    [JsonPropertyOrder(0)]
    public required string SnapshotIdentity { get; init; }

    [JsonPropertyOrder(1)]
    public required string ContentDigest { get; init; }

    [JsonPropertyOrder(2)]
    public required long ContentLength { get; init; }

    [JsonPropertyOrder(3)]
    public required string Identity { get; init; }

    [JsonPropertyOrder(4)]
    public required string Fingerprint { get; init; }

    [JsonPropertyOrder(5)]
    public required string OptionsDigest { get; init; }
}

internal sealed record JsonTypedIntentLimitsManifest
{
    [JsonPropertyOrder(0)]
    public required int MaxDecodedBinaryBytes { get; init; }

    [JsonPropertyOrder(1)]
    public required int MaxDecimalDigits { get; init; }
}

internal sealed record JsonTypedIntentManifestEnvelope<TPayload>
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

internal sealed record JsonTypedIntentManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonTypedIntentManifestPayload Payload
    {
        get;
        init;
    }
}
