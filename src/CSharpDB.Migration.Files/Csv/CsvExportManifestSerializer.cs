using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Strict canonical serializer for <c>csharpdb-csv-export-manifest/v1</c>.
/// The envelope digest authenticates consistency only; it is not a signature.
/// </summary>
public static class CsvExportManifestSerializer
{
    public const int MaximumManifestBytes = 16 * 1024 * 1024;
    public const int MaximumColumns = CsvReaderOptions.MaximumSupportedFieldsPerRecord;
    public const long MaximumTextCharacters = 1024 * 1024;

    private const int MaximumJsonDepth = 64;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);
    private static readonly JsonSerializerOptions s_options = CreateOptions();

    /// <summary>Serializes one validated manifest to canonical UTF-8 without a BOM.</summary>
    public static byte[] Serialize(CsvExportManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        Validate(manifest);

        string digest = ComputeManifestDigestCore(manifest);
        byte[] bytes = SerializeEnvelope(manifest, digest);
        if (bytes.Length > MaximumManifestBytes)
        {
            throw new InvalidDataException(
                $"The CSV export manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return bytes;
    }

    /// <summary>
    /// Parses, validates, verifies, and requires the exact canonical byte form
    /// of one export manifest.
    /// </summary>
    public static CsvExportManifest Deserialize(ReadOnlyMemory<byte> utf8Json)
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
            throw new InvalidDataException("The CSV export manifest JSON is invalid.", exception);
        }

        using (document)
        {
            RejectDuplicateProperties(document.RootElement, path: "$");

            CsvExportManifestEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<CsvExportManifestEnvelope<JsonElement>>(s_options)
                    ?? throw new InvalidDataException(
                        "The CSV export manifest did not contain an envelope.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV export manifest envelope is invalid.",
                    exception);
            }

            if (!string.Equals(
                    envelope.Format,
                    CsvExportContracts.ManifestFormat,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"CSV export manifest format '{envelope.Format}' is not supported.");
            }

            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    CsvExportHashManifest.Sha256Algorithm,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"CSV export manifest digest algorithm '{envelope.DigestAlgorithm}' is not supported.");
            }

            if (envelope.Payload.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
                throw new InvalidDataException("The CSV export manifest payload is missing.");

            CsvExportManifest manifest;
            try
            {
                manifest = envelope.Payload.Deserialize<CsvExportManifest>(s_options)
                    ?? throw new InvalidDataException(
                        "The CSV export manifest payload is missing.");
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The CSV export manifest payload is invalid.",
                    exception);
            }

            Validate(manifest);
            VerifyDigest(envelope.Digest, ComputeManifestDigestCore(manifest));

            byte[] canonicalBytes = SerializeEnvelope(manifest, envelope.Digest);
            if (!utf8Json.Span.SequenceEqual(canonicalBytes))
            {
                throw new InvalidDataException(
                    "The CSV export manifest is not in the required canonical UTF-8 form.");
            }

            return manifest;
        }
    }

    /// <summary>
    /// Computes the lowercase SHA-256 text stored in the canonical manifest
    /// envelope. The format and algorithm identifiers are domain inputs.
    /// </summary>
    public static string ComputeManifestDigest(CsvExportManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        Validate(manifest);
        return ComputeManifestDigestCore(manifest);
    }

    /// <summary>
    /// Computes the schema hash that binds ordered original/rendered names,
    /// storage types, nullability, and scalar encodings.
    /// </summary>
    public static CsvExportHashManifest ComputeSchemaDigest(
        IReadOnlyList<CsvExportColumnManifest> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        long textCharacters = 0;
        ValidateColumns(columns, ref textCharacters);
        EnsureTextBudget(textCharacters);
        return CreateHash(ComputeSchemaDigestCore(columns));
    }

    private static byte[] SerializeEnvelope(CsvExportManifest manifest, string digest)
    {
        try
        {
            JsonElement element = JsonSerializer.SerializeToElement(
                new CsvExportManifestEnvelope<CsvExportManifest>
                {
                    Format = CsvExportContracts.ManifestFormat,
                    DigestAlgorithm = CsvExportHashManifest.Sha256Algorithm,
                    Digest = digest,
                    Payload = manifest,
                },
                s_options);
            return CsvSnapshotPackageCanonicalJson.Serialize(element);
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV export manifest is invalid.", exception);
        }
    }

    private static string ComputeManifestDigestCore(CsvExportManifest manifest)
    {
        try
        {
            JsonElement element = JsonSerializer.SerializeToElement(
                new CsvExportManifestDigestInput
                {
                    Format = CsvExportContracts.ManifestFormat,
                    DigestAlgorithm = CsvExportHashManifest.Sha256Algorithm,
                    Payload = manifest,
                },
                s_options);
            byte[] canonicalBytes = CsvSnapshotPackageCanonicalJson.Serialize(element);
            return Hex(SHA256.HashData(canonicalBytes));
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV export manifest is invalid.", exception);
        }
    }

    private static string ComputeSchemaDigestCore(
        IReadOnlyList<CsvExportColumnManifest> columns)
    {
        try
        {
            JsonElement element = JsonSerializer.SerializeToElement(
                new CsvExportSchemaDigestInput
                {
                    Contract = CsvExportContracts.Schema,
                    Columns = columns,
                },
                s_options);
            byte[] canonicalBytes = CsvSnapshotPackageCanonicalJson.Serialize(element);
            return Hex(SHA256.HashData(canonicalBytes));
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            throw new InvalidDataException("The CSV export schema is invalid.", exception);
        }
    }

    private static void Validate(CsvExportManifest manifest)
    {
        RequireMember(manifest.Source, "payload.source");
        RequireMember(manifest.Table, "payload.table");
        RequireMember(manifest.Csv, "payload.csv");
        RequireMember(manifest.Content, "payload.content");

        if (!Enum.IsDefined(manifest.Profile))
            throw Invalid("CSV export profile is unsupported.");

        long textCharacters = 0;
        ValidateSource(manifest.Source, ref textCharacters);
        ValidateTable(manifest.Table, ref textCharacters);
        ValidateFormat(manifest.Csv, ref textCharacters);
        ValidateContent(manifest.Content, ref textCharacters);
        ValidateProfile(manifest, ref textCharacters);
        EnsureTextBudget(textCharacters);
    }

    private static void ValidateSource(
        CsvExportSourceManifest source,
        ref long textCharacters)
    {
        RequireText(source.Kind, "payload.source.kind", ref textCharacters);
        RequireText(source.Version, "payload.source.version", ref textCharacters);
        RequireMember(source.SnapshotDigest, "payload.source.snapshotDigest");

        if (!string.Equals(source.Kind, CsvExportContracts.SourceKind, StringComparison.Ordinal))
            throw Invalid("CSV export source kind is unsupported.");
        if (source.SnapshotByteLength <= 0)
            throw Invalid("CSV export source snapshot byte length must be positive.");
        ValidateHash(source.SnapshotDigest, "payload.source.snapshotDigest", ref textCharacters);
    }

    private static void ValidateTable(
        CsvExportTableManifest table,
        ref long textCharacters)
    {
        RequireText(table.Name, "payload.table.name", ref textCharacters);
        RequireText(table.SchemaContract, "payload.table.schemaContract", ref textCharacters);
        RequireMember(table.SchemaDigest, "payload.table.schemaDigest");
        RequireText(table.RowOrder, "payload.table.rowOrder", ref textCharacters);
        RequireMember(table.Columns, "payload.table.columns");

        if (!string.Equals(
                table.SchemaContract,
                CsvExportContracts.Schema,
                StringComparison.Ordinal))
        {
            throw Invalid("CSV export schema contract is unsupported.");
        }
        if (!string.Equals(table.RowOrder, CsvExportContracts.RowOrder, StringComparison.Ordinal))
            throw Invalid("CSV export row-order contract is unsupported.");

        ValidateColumns(table.Columns, ref textCharacters);
        ValidateHash(table.SchemaDigest, "payload.table.schemaDigest", ref textCharacters);
        string expectedSchemaDigest = ComputeSchemaDigestCore(table.Columns);
        VerifyHashValue(
            table.SchemaDigest.Value,
            expectedSchemaDigest,
            "CSV export schema digest does not match the ordered columns.");
    }

    private static void ValidateColumns(
        IReadOnlyList<CsvExportColumnManifest> columns,
        ref long textCharacters)
    {
        if (columns.Count is < 1 or > MaximumColumns)
        {
            throw Invalid(
                $"CSV export column count must be between 1 and {MaximumColumns}.");
        }

        var sourceNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var headers = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int index = 0; index < columns.Count; index++)
        {
            CsvExportColumnManifest? column = columns[index];
            RequireMember(column, $"payload.table.columns[{index}]");
            if (column.Ordinal != index)
            {
                throw Invalid(
                    "CSV export columns must have contiguous zero-based ordinals in array order.");
            }

            RequireText(
                column.SourceName,
                $"payload.table.columns[{index}].sourceName",
                ref textCharacters);
            RequireText(
                column.Header,
                $"payload.table.columns[{index}].header",
                ref textCharacters);
            RequireText(
                column.ValueEncoding,
                $"payload.table.columns[{index}].valueEncoding",
                ref textCharacters);

            if (!sourceNames.Add(column.SourceName))
                throw Invalid("CSV export source column names must be unique ignoring case.");
            if (!headers.Add(column.Header))
                throw Invalid("CSV export header names must be unique ignoring case.");
            if (!Enum.IsDefined(column.DatabaseType))
                throw Invalid("CSV export column database type is unsupported.");

            string expectedEncoding = column.DatabaseType switch
            {
                CsvExportDatabaseType.Integer => CsvExportContracts.IntegerValueEncoding,
                CsvExportDatabaseType.Real => CsvExportContracts.RealValueEncoding,
                CsvExportDatabaseType.Decimal => CsvExportContracts.DecimalValueEncoding,
                CsvExportDatabaseType.Text => CsvExportContracts.TextValueEncoding,
                CsvExportDatabaseType.Blob => CsvExportContracts.BlobValueEncoding,
                _ => throw Invalid("CSV export column database type is unsupported."),
            };
            if (!string.Equals(column.ValueEncoding, expectedEncoding, StringComparison.Ordinal))
            {
                throw Invalid(
                    "CSV export column value encoding does not match its database type.");
            }

            if (column.DatabaseType == CsvExportDatabaseType.Blob)
            {
                if (column.MaximumDecodedBytes is < 1 or >
                    CsvExportContracts.MaximumSupportedDecodedBlobBytes)
                {
                    throw Invalid(
                        "CSV export BLOB columns require a supported positive decoded-size ceiling.");
                }
            }
            else if (column.MaximumDecodedBytes != 0)
            {
                throw Invalid(
                    "Only CSV export BLOB columns can declare a decoded-size ceiling.");
            }
        }
    }

    private static void ValidateFormat(
        CsvExportFormatManifest csv,
        ref long textCharacters)
    {
        RequireText(csv.Encoding, "payload.csv.encoding", ref textCharacters);
        RequireText(csv.Culture, "payload.csv.culture", ref textCharacters);
        RequireText(csv.Delimiter, "payload.csv.delimiter", ref textCharacters);
        RequireText(csv.Newline, "payload.csv.newline", ref textCharacters);
        RequireText(csv.NullToken, "payload.csv.nullToken", ref textCharacters);
        RequireText(csv.TextEscape, "payload.csv.textEscape", ref textCharacters);

        if (!string.Equals(csv.Encoding, CsvExportContracts.Encoding, StringComparison.Ordinal) ||
            csv.HasByteOrderMark ||
            !string.Equals(csv.Culture, CsvExportContracts.Culture, StringComparison.Ordinal) ||
            !string.Equals(csv.Delimiter, ",", StringComparison.Ordinal) ||
            csv.Quote != '"' ||
            !string.Equals(csv.Newline, CsvExportContracts.Newline, StringComparison.Ordinal) ||
            !csv.HasHeaderRecord ||
            !csv.HasFinalNewline ||
            !string.Equals(csv.NullToken, CsvExportContracts.NullToken, StringComparison.Ordinal) ||
            csv.NullTokenMatchesQuotedFields ||
            !string.Equals(csv.TextEscape, CsvExportContracts.TextEscape, StringComparison.Ordinal))
        {
            throw Invalid("CSV export format settings do not match the fixed lossless v1 codec.");
        }
    }

    private static void ValidateContent(
        CsvExportContentManifest content,
        ref long textCharacters)
    {
        RequireMember(content.DataDigest, "payload.content.dataDigest");
        RequireText(
            content.Canonicalization,
            "payload.content.canonicalization",
            ref textCharacters);
        RequireText(
            content.CanonicalizationContractDigest,
            "payload.content.canonicalizationContractDigest",
            ref textCharacters);
        RequireText(content.Aggregation, "payload.content.aggregation", ref textCharacters);
        RequireMember(content.SourceLogicalDigest, "payload.content.sourceLogicalDigest");
        RequireMember(content.ExportedLogicalDigest, "payload.content.exportedLogicalDigest");

        if (content.RowCount < 0)
            throw Invalid("CSV export row count cannot be negative.");
        if (content.DataByteLength <= 0)
            throw Invalid("CSV export data byte length must be positive.");

        ValidateHash(content.DataDigest, "payload.content.dataDigest", ref textCharacters);
        ValidateHash(
            content.SourceLogicalDigest,
            "payload.content.sourceLogicalDigest",
            ref textCharacters);
        ValidateHash(
            content.ExportedLogicalDigest,
            "payload.content.exportedLogicalDigest",
            ref textCharacters);

        if (!string.Equals(
                content.Canonicalization,
                CsvExportContracts.Canonicalization,
                StringComparison.Ordinal) ||
            !string.Equals(
                content.CanonicalizationContractDigest,
                CsvExportContracts.CanonicalizationContractDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                content.Aggregation,
                CsvExportContracts.OrderedContentDigest,
                StringComparison.Ordinal))
        {
            throw Invalid("CSV export logical checksum contract is unsupported.");
        }
    }

    private static void ValidateProfile(
        CsvExportManifest manifest,
        ref long textCharacters)
    {
        int changedHeaders = 0;
        foreach (CsvExportColumnManifest column in manifest.Table.Columns)
        {
            string expectedHeader = manifest.Profile == CsvExportProfile.LosslessV1
                ? column.SourceName
                : CsvSpreadsheetFormulaPolicy.Transform(column.SourceName);
            if (!string.Equals(column.Header, expectedHeader, StringComparison.Ordinal))
            {
                throw Invalid(
                    "CSV export rendered headers do not match the selected export profile.");
            }
            if (!string.Equals(column.Header, column.SourceName, StringComparison.Ordinal))
                changedHeaders++;
        }

        bool logicalDigestsEqual = HashValuesEqual(
            manifest.Content.SourceLogicalDigest.Value,
            manifest.Content.ExportedLogicalDigest.Value);

        if (manifest.Profile == CsvExportProfile.LosslessV1)
        {
            if (manifest.LossyTransform is not null)
                throw Invalid("Lossless CSV exports cannot contain a lossy transform record.");
            if (changedHeaders != 0)
                throw Invalid("Lossless CSV export headers must preserve source names exactly.");
            if (!logicalDigestsEqual)
            {
                throw Invalid(
                    "Lossless CSV source and exported logical digests must be identical.");
            }
            return;
        }

        CsvExportLossyTransformManifest transform = manifest.LossyTransform
            ?? throw Invalid(
                "Spreadsheet-safe lossy CSV exports require aggregate transform evidence.");
        if (manifest.Table.Columns.Any(
                static column => column.DatabaseType == CsvExportDatabaseType.Blob))
        {
            throw Invalid(
                "Spreadsheet-safe lossy CSV exports do not support BLOB columns because base64 can resemble a formula.");
        }
        RequireText(transform.RuleId, "payload.lossyTransform.ruleId", ref textCharacters);
        RequireText(transform.Algorithm, "payload.lossyTransform.algorithm", ref textCharacters);
        if (!string.Equals(
                transform.RuleId,
                CsvExportContracts.SpreadsheetFormulaRuleId,
                StringComparison.Ordinal) ||
            !string.Equals(
                transform.Algorithm,
                CsvExportContracts.SpreadsheetFormulaTransform,
                StringComparison.Ordinal))
        {
            throw Invalid("CSV export spreadsheet transform contract is unsupported.");
        }
        if (transform.TransformedHeaderCount != changedHeaders)
            throw Invalid("CSV export transformed-header count is inconsistent with the schema.");
        if (transform.TransformedRowCount < 0 ||
            transform.TransformedRowCount > manifest.Content.RowCount ||
            transform.TransformedCellCount < 0 ||
            transform.TransformedCellCount < transform.TransformedRowCount)
        {
            throw Invalid("CSV export spreadsheet transform counts are invalid.");
        }

        int eligibleTextColumns = manifest.Table.Columns.Count(
            static column => column.DatabaseType == CsvExportDatabaseType.Text);
        long maximumCells = eligibleTextColumns == 0 ||
            transform.TransformedRowCount == 0
            ? 0
            : transform.TransformedRowCount > long.MaxValue / eligibleTextColumns
            ? long.MaxValue
            : transform.TransformedRowCount * eligibleTextColumns;
        if (transform.TransformedCellCount > maximumCells ||
            (transform.TransformedCellCount == 0) != (transform.TransformedRowCount == 0))
        {
            throw Invalid("CSV export spreadsheet transform counts are inconsistent.");
        }
        if ((transform.TransformedCellCount == 0) != logicalDigestsEqual)
        {
            throw Invalid(
                "CSV export spreadsheet transform counts are inconsistent with the logical digests.");
        }
    }

    private static void ValidateHash(
        CsvExportHashManifest hash,
        string path,
        ref long textCharacters)
    {
        RequireText(hash.Algorithm, $"{path}.algorithm", ref textCharacters);
        RequireText(hash.Value, $"{path}.value", ref textCharacters);
        if (!string.Equals(
                hash.Algorithm,
                CsvExportHashManifest.Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw Invalid($"CSV export hash '{path}' uses an unsupported algorithm.");
        }
        if (!IsLowercaseSha256(hash.Value))
            throw Invalid($"CSV export hash '{path}' is not lowercase SHA-256 text.");
    }

    private static void VerifyDigest(string? suppliedDigest, string expectedDigest)
    {
        if (!IsLowercaseSha256(suppliedDigest))
            throw Invalid("CSV export manifest digest is not lowercase SHA-256 text.");
        VerifyHashValue(
            suppliedDigest!,
            expectedDigest,
            "CSV export manifest digest does not match its payload.");
    }

    private static void VerifyHashValue(
        string supplied,
        string expected,
        string mismatchMessage)
    {
        byte[] suppliedBytes = Convert.FromHexString(supplied);
        byte[] expectedBytes = Convert.FromHexString(expected);
        if (!CryptographicOperations.FixedTimeEquals(suppliedBytes, expectedBytes))
            throw Invalid(mismatchMessage);
    }

    private static bool HashValuesEqual(string first, string second)
    {
        byte[] firstBytes = Convert.FromHexString(first);
        byte[] secondBytes = Convert.FromHexString(second);
        return CryptographicOperations.FixedTimeEquals(firstBytes, secondBytes);
    }

    private static CsvExportHashManifest CreateHash(string value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = value,
    };

    private static string Hex(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(bytes).ToLowerInvariant();

    private static bool IsLowercaseSha256(string? value)
    {
        if (value is null || value.Length != 64)
            return false;
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                return false;
        }
        return true;
    }

    private static void RequireText(
        string? value,
        string path,
        ref long textCharacters)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw Invalid($"CSV export manifest member '{path}' must be nonblank.");
        ValidateText(value, path, ref textCharacters);
    }

    private static void ValidateText(
        string value,
        string path,
        ref long textCharacters)
    {
        if (value.Contains('\0', StringComparison.Ordinal))
            throw Invalid($"CSV export manifest member '{path}' contains a NUL character.");

        try
        {
            _ = s_strictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                $"CSV export manifest member '{path}' contains invalid UTF-16 text.",
                exception);
        }

        textCharacters = checked(textCharacters + value.Length);
    }

    private static void EnsureTextBudget(long textCharacters)
    {
        if (textCharacters > MaximumTextCharacters)
        {
            throw Invalid(
                $"CSV export manifest text exceeds the {MaximumTextCharacters}-character safety limit.");
        }
    }

    private static void ValidateInputEncoding(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
            throw Invalid("The CSV export manifest is empty.");
        if (utf8Json.Length > MaximumManifestBytes)
        {
            throw Invalid(
                $"The CSV export manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
            throw Invalid("The CSV export manifest must not contain a UTF-8 BOM.");
        if (utf8Json.IndexOf((byte)0) >= 0)
            throw Invalid("The CSV export manifest must not contain NUL bytes.");

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The CSV export manifest is not strict UTF-8.",
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
                        throw Invalid(
                            $"CSV export manifest contains duplicate property '{path}.{property.Name}'.");
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

    private static void RequireMember<T>(T? value, string path)
        where T : class
    {
        if (value is null)
            throw Invalid($"CSV export manifest member '{path}' is required.");
    }

    private static InvalidDataException Invalid(string message) => new(message);

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
}

/// <summary>
/// Versioned best-effort formula transform for the explicitly lossy profile.
/// Quoting alone does not provide spreadsheet formula protection.
/// </summary>
public static class CsvSpreadsheetFormulaPolicy
{
    public static bool RequiresTransform(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return value.Length > 0 && value[0] is '=' or '+' or '-' or '@' or ' ' or '\t' or '\r' or '\n';
    }

    public static string Transform(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return RequiresTransform(value) ? "'" + value : value;
    }
}

internal sealed record CsvExportManifestEnvelope<TPayload>
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

internal sealed record CsvExportManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required CsvExportManifest Payload { get; init; }
}

internal sealed record CsvExportSchemaDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Contract { get; init; }

    [JsonPropertyOrder(1)]
    public required IReadOnlyList<CsvExportColumnManifest> Columns { get; init; }
}
