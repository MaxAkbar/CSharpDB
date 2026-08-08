using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Strict canonical serializer for
/// <c>csharpdb-json-export-manifest/v1</c>. The envelope digest establishes
/// consistency only; it is not a signature.
/// </summary>
public static class JsonExportManifestSerializer
{
    public const int MaximumManifestBytes =
        16 * 1024 * 1024;

    public const int MaximumColumns =
        JsonTableSchemaInferenceOptions.MaximumSupportedColumns;

    public const long MaximumTextCharacters =
        1024 * 1024;

    private const int MaximumJsonDepth = 64;

    private static readonly UTF8Encoding s_strictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    private static readonly JsonSerializerOptions s_options =
        CreateOptions();

    /// <summary>
    /// Serializes one validated manifest to canonical UTF-8 without a BOM.
    /// </summary>
    public static byte[] Serialize(
        JsonExportManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        Validate(manifest);

        string digest =
            ComputeManifestDigestCore(manifest);
        byte[] bytes =
            SerializeEnvelope(manifest, digest);
        if (bytes.Length > MaximumManifestBytes)
        {
            CryptographicOperations.ZeroMemory(bytes);
            throw Invalid(
                $"The JSON export manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }

        return bytes;
    }

    /// <summary>
    /// Parses, validates, verifies, and requires the exact canonical bytes of
    /// one JSON export manifest.
    /// </summary>
    public static JsonExportManifest Deserialize(
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
                "The JSON export manifest JSON is invalid.");
        }

        using (document)
        {
            RejectDuplicateProperties(
                document.RootElement);

            JsonExportManifestEnvelope<JsonElement> envelope;
            try
            {
                envelope = document.RootElement
                    .Deserialize<
                        JsonExportManifestEnvelope<JsonElement>>(
                        s_options)
                    ?? throw Invalid(
                        "The JSON export manifest did not contain an envelope.");
            }
            catch (JsonException)
            {
                throw new InvalidDataException(
                    "The JSON export manifest envelope is invalid.");
            }

            if (!string.Equals(
                    envelope.Format,
                    JsonExportContracts.ManifestFormat,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "The JSON export manifest format is not supported.");
            }
            if (!string.Equals(
                    envelope.DigestAlgorithm,
                    JsonExportHashManifest.Sha256Algorithm,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "The JSON export manifest digest algorithm is not supported.");
            }
            if (envelope.Payload.ValueKind is
                JsonValueKind.Null or
                JsonValueKind.Undefined)
            {
                throw Invalid(
                    "The JSON export manifest payload is missing.");
            }

            JsonExportManifest manifest;
            try
            {
                manifest = envelope.Payload
                    .Deserialize<JsonExportManifest>(
                        s_options)
                    ?? throw Invalid(
                        "The JSON export manifest payload is missing.");
            }
            catch (JsonException)
            {
                throw new InvalidDataException(
                    "The JSON export manifest payload is invalid.");
            }

            Validate(manifest);
            VerifyDigest(
                envelope.Digest,
                ComputeManifestDigestCore(manifest));

            byte[] canonical =
                SerializeEnvelope(
                    manifest,
                    envelope.Digest);
            try
            {
                if (!utf8Json.Span.SequenceEqual(canonical))
                {
                    throw Invalid(
                        "The JSON export manifest is not in the required canonical UTF-8 form.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(canonical);
            }

            return manifest;
        }
    }

    /// <summary>
    /// Computes the lowercase SHA-256 stored in the canonical manifest
    /// envelope. The format and digest algorithm are domain inputs.
    /// </summary>
    public static string ComputeManifestDigest(
        JsonExportManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        Validate(manifest);
        return ComputeManifestDigestCore(manifest);
    }

    /// <summary>
    /// Computes the schema hash binding ordered names, storage types,
    /// nullability, value encodings, and decoded binary ceilings.
    /// </summary>
    public static JsonExportHashManifest ComputeSchemaDigest(
        IReadOnlyList<JsonExportColumnManifest> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        long textCharacters = 0;
        ValidateColumns(
            columns,
            format: null,
            ref textCharacters);
        EnsureTextBudget(textCharacters);
        return CreateHash(
            ComputeSchemaDigestCore(columns));
    }

    private static byte[] SerializeEnvelope(
        JsonExportManifest manifest,
        string digest)
    {
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonExportManifestEnvelope<
                        JsonExportManifest>
                    {
                        Format =
                            JsonExportContracts
                                .ManifestFormat,
                        DigestAlgorithm =
                            JsonExportHashManifest
                                .Sha256Algorithm,
                        Digest = digest,
                        Payload = manifest,
                    },
                    s_options);
            return JsonSnapshotPackageCanonicalJson
                .Serialize(element);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON export manifest is invalid.");
        }
    }

    private static string ComputeManifestDigestCore(
        JsonExportManifest manifest)
    {
        byte[]? canonical = null;
        byte[]? digest = null;
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonExportManifestDigestInput
                    {
                        Format =
                            JsonExportContracts
                                .ManifestFormat,
                        DigestAlgorithm =
                            JsonExportHashManifest
                                .Sha256Algorithm,
                        Payload = manifest,
                    },
                    s_options);
            canonical =
                JsonSnapshotPackageCanonicalJson
                    .Serialize(element);
            digest = SHA256.HashData(canonical);
            return Hex(digest);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON export manifest is invalid.");
        }
        finally
        {
            Zero(canonical);
            Zero(digest);
        }
    }

    private static string ComputeSchemaDigestCore(
        IReadOnlyList<JsonExportColumnManifest> columns)
    {
        byte[]? canonical = null;
        byte[]? digest = null;
        try
        {
            JsonElement element =
                JsonSerializer.SerializeToElement(
                    new JsonExportSchemaDigestInput
                    {
                        Contract =
                            JsonExportContracts.Schema,
                        Columns = columns,
                    },
                    s_options);
            canonical =
                JsonSnapshotPackageCanonicalJson
                    .Serialize(element);
            digest = SHA256.HashData(canonical);
            return Hex(digest);
        }
        catch (Exception exception) when (
            exception is JsonException or
            NotSupportedException)
        {
            throw new InvalidDataException(
                "The JSON export schema is invalid.");
        }
        finally
        {
            Zero(canonical);
            Zero(digest);
        }
    }

    private static void Validate(
        JsonExportManifest manifest)
    {
        RequireMember(
            manifest.Source,
            "payload.source");
        RequireMember(
            manifest.Table,
            "payload.table");
        RequireMember(
            manifest.Json,
            "payload.json");
        RequireMember(
            manifest.Content,
            "payload.content");

        if (manifest.Profile !=
                JsonExportProfile.LosslessV1 ||
            !Enum.IsDefined(manifest.Profile))
        {
            throw Invalid(
                "JSON export profile is unsupported.");
        }

        long textCharacters = 0;
        ValidateSource(
            manifest.Source,
            ref textCharacters);
        ValidateFormat(
            manifest.Json,
            ref textCharacters);
        ValidateTable(
            manifest.Table,
            manifest.Json,
            ref textCharacters);
        ValidateContent(
            manifest.Content,
            manifest.Json,
            manifest.Table,
            ref textCharacters);

        if (!HashValuesEqual(
                manifest.Content
                    .SourceLogicalDigest.Value,
                manifest.Content
                    .ExportedLogicalDigest.Value))
        {
            throw Invalid(
                "Lossless JSON source and exported logical digests must be identical.");
        }

        EnsureTextBudget(textCharacters);
    }

    private static void ValidateSource(
        JsonExportSourceManifest source,
        ref long textCharacters)
    {
        RequireText(
            source.Kind,
            "payload.source.kind",
            ref textCharacters);
        RequireText(
            source.Version,
            "payload.source.version",
            ref textCharacters);
        RequireMember(
            source.SnapshotDigest,
            "payload.source.snapshotDigest");

        if (!string.Equals(
                source.Kind,
                JsonExportContracts.SourceKind,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export source kind is unsupported.");
        }
        if (source.SnapshotByteLength <= 0)
        {
            throw Invalid(
                "JSON export source snapshot byte length must be positive.");
        }

        ValidateHash(
            source.SnapshotDigest,
            "payload.source.snapshotDigest",
            ref textCharacters);
    }

    private static void ValidateTable(
        JsonExportTableManifest table,
        JsonExportFormatManifest format,
        ref long textCharacters)
    {
        RequireText(
            table.Name,
            "payload.table.name",
            ref textCharacters);
        RequireText(
            table.SchemaContract,
            "payload.table.schemaContract",
            ref textCharacters);
        RequireMember(
            table.SchemaDigest,
            "payload.table.schemaDigest");
        RequireText(
            table.RowOrder,
            "payload.table.rowOrder",
            ref textCharacters);
        RequireMember(
            table.Columns,
            "payload.table.columns");

        if (!string.Equals(
                table.SchemaContract,
                JsonExportContracts.Schema,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export schema contract is unsupported.");
        }
        if (!string.Equals(
                table.RowOrder,
                JsonExportContracts.RowOrder,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export row-order contract is unsupported.");
        }

        ValidateColumns(
            table.Columns,
            format,
            ref textCharacters);
        ValidateHash(
            table.SchemaDigest,
            "payload.table.schemaDigest",
            ref textCharacters);

        string expected =
            ComputeSchemaDigestCore(table.Columns);
        VerifyHashValue(
            table.SchemaDigest.Value,
            expected,
            "JSON export schema digest does not match the ordered columns.");
    }

    private static void ValidateColumns(
        IReadOnlyList<JsonExportColumnManifest> columns,
        JsonExportFormatManifest? format,
        ref long textCharacters)
    {
        if (columns.Count is < 1 or > MaximumColumns)
        {
            throw Invalid(
                $"JSON export column count must be between 1 and {MaximumColumns}.");
        }
        if (format is not null &&
            columns.Count >
                format.MaximumPropertiesPerObject)
        {
            throw Invalid(
                "JSON export column count exceeds the retained object-property ceiling.");
        }

        var sourceNames =
            new HashSet<string>(
                StringComparer.OrdinalIgnoreCase);
        var propertyNames =
            new HashSet<string>(
                StringComparer.Ordinal);
        for (int index = 0;
             index < columns.Count;
             index++)
        {
            JsonExportColumnManifest? column =
                columns[index];
            RequireMember(
                column,
                $"payload.table.columns[{index}]");
            if (column.Ordinal != index)
            {
                throw Invalid(
                    "JSON export columns must have contiguous zero-based ordinals in array order.");
            }

            RequireText(
                column.SourceName,
                $"payload.table.columns[{index}].sourceName",
                ref textCharacters);
            RequireText(
                column.PropertyName,
                $"payload.table.columns[{index}].propertyName",
                ref textCharacters);
            RequireText(
                column.ValueEncoding,
                $"payload.table.columns[{index}].valueEncoding",
                ref textCharacters);

            if (!sourceNames.Add(column.SourceName))
            {
                throw Invalid(
                    "JSON export source column names must be unique ignoring case.");
            }
            if (!propertyNames.Add(column.PropertyName))
            {
                throw Invalid(
                    "JSON export property names must be unique using Unicode ordinal comparison.");
            }
            if (!string.Equals(
                    column.SourceName,
                    column.PropertyName,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "Lossless JSON export property names must exactly preserve source column names.");
            }
            if (!Enum.IsDefined(column.DatabaseType))
            {
                throw Invalid(
                    "JSON export column database type is unsupported.");
            }

            string expectedEncoding =
                column.DatabaseType switch
                {
                    JsonExportDatabaseType.Integer =>
                        JsonExportContracts
                            .IntegerValueEncoding,
                    JsonExportDatabaseType.Real =>
                        JsonExportContracts
                            .RealValueEncoding,
                    JsonExportDatabaseType.Decimal =>
                        JsonExportContracts
                            .DecimalValueEncoding,
                    JsonExportDatabaseType.Text =>
                        JsonExportContracts
                            .TextValueEncoding,
                    JsonExportDatabaseType.Blob =>
                        JsonExportContracts
                            .BlobValueEncoding,
                    _ => throw Invalid(
                        "JSON export column database type is unsupported."),
                };
            if (!string.Equals(
                    column.ValueEncoding,
                    expectedEncoding,
                    StringComparison.Ordinal))
            {
                throw Invalid(
                    "JSON export column value encoding does not match its database type.");
            }

            if (column.DatabaseType ==
                JsonExportDatabaseType.Blob)
            {
                if (column.MaximumDecodedBytes is
                    < 1 or >
                    JsonExportContracts
                        .MaximumSupportedDecodedBlobBytes)
                {
                    throw Invalid(
                        "JSON export BLOB columns require a supported positive decoded-size ceiling.");
                }
                if (format is not null &&
                    column.MaximumDecodedBytes !=
                        format.MaximumDecodedBlobBytes)
                {
                    throw Invalid(
                        "JSON export BLOB column ceilings must match the retained format policy.");
                }
            }
            else if (column.MaximumDecodedBytes != 0)
            {
                throw Invalid(
                    "Only JSON export BLOB columns can declare a decoded-size ceiling.");
            }

            int propertyNameBytes =
                StrictUtf8ByteCount(
                    column.PropertyName,
                    $"payload.table.columns[{index}].propertyName");
            int maximumPropertyNameBytes =
                format?.MaximumPropertyNameBytes ??
                JsonInputContracts.MaximumPropertyNameBytes;
            if (propertyNameBytes >
                maximumPropertyNameBytes)
            {
                throw Invalid(
                    "A JSON export property name exceeds the retained compatibility ceiling.");
            }
        }
    }

    private static void ValidateFormat(
        JsonExportFormatManifest format,
        ref long textCharacters)
    {
        RequireText(
            format.Encoding,
            "payload.json.encoding",
            ref textCharacters);
        RequireText(
            format.Culture,
            "payload.json.culture",
            ref textCharacters);
        RequireText(
            format.PropertyOrder,
            "payload.json.propertyOrder",
            ref textCharacters);
        RequireText(
            format.Newline,
            "payload.json.newline",
            ref textCharacters);
        RequireText(
            format.NullEncoding,
            "payload.json.nullEncoding",
            ref textCharacters);
        RequireText(
            format.TextEscape,
            "payload.json.textEscape",
            ref textCharacters);

        if (!string.Equals(
                format.Encoding,
                JsonExportContracts.Encoding,
                StringComparison.Ordinal) ||
            format.HasByteOrderMark ||
            !string.Equals(
                format.Culture,
                JsonExportContracts.Culture,
                StringComparison.Ordinal) ||
            !Enum.IsDefined(format.Framing) ||
            !format.Compact ||
            !string.Equals(
                format.PropertyOrder,
                JsonExportContracts.PropertyOrder,
                StringComparison.Ordinal) ||
            !string.Equals(
                format.Newline,
                JsonExportContracts.Newline,
                StringComparison.Ordinal) ||
            !format.HasFinalNewline ||
            !string.Equals(
                format.NullEncoding,
                JsonExportContracts.NullEncoding,
                StringComparison.Ordinal) ||
            !string.Equals(
                format.TextEscape,
                JsonExportContracts.TextEscape,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export format settings do not match the fixed lossless v1 codec.");
        }
        if (format.MaxDataBytes <= 0)
        {
            throw Invalid(
                "JSON export maximum data bytes must be positive.");
        }
        if (format.Framing ==
                JsonExportFraming.RootArray &&
            format.MaxDataBytes < 3)
        {
            throw Invalid(
                "A root-array JSON export requires at least three data bytes.");
        }
        if (format.MaximumDecodedBlobBytes is
            < 1 or >
            JsonExportContracts
                .MaximumSupportedDecodedBlobBytes)
        {
            throw Invalid(
                "JSON export decoded BLOB ceiling is unsupported.");
        }
        if (format.MaximumValueBytes !=
                JsonInputContracts.MaximumValueBytes ||
            format.MaximumStringBytes !=
                JsonInputContracts.MaximumStringBytes ||
            format.MaximumPropertyNameBytes !=
                JsonInputContracts.MaximumPropertyNameBytes ||
            format.MaximumPropertiesPerObject !=
                JsonInputContracts.MaximumPropertiesPerObject)
        {
            throw Invalid(
                "JSON export compatibility ceilings do not match the fixed v1 contract.");
        }
    }

    private static void ValidateContent(
        JsonExportContentManifest content,
        JsonExportFormatManifest format,
        JsonExportTableManifest table,
        ref long textCharacters)
    {
        RequireMember(
            content.DataDigest,
            "payload.content.dataDigest");
        RequireText(
            content.Canonicalization,
            "payload.content.canonicalization",
            ref textCharacters);
        RequireText(
            content.CanonicalizationContractDigest,
            "payload.content.canonicalizationContractDigest",
            ref textCharacters);
        RequireText(
            content.Aggregation,
            "payload.content.aggregation",
            ref textCharacters);
        RequireMember(
            content.SourceLogicalDigest,
            "payload.content.sourceLogicalDigest");
        RequireMember(
            content.ExportedLogicalDigest,
            "payload.content.exportedLogicalDigest");

        if (content.RowCount < 0)
        {
            throw Invalid(
                "JSON export row count cannot be negative.");
        }
        if (content.DataByteLength < 0 ||
            content.DataByteLength >
                format.MaxDataBytes)
        {
            throw Invalid(
                "JSON export data byte length is outside the retained resource policy.");
        }
        if ((format.Framing ==
                 JsonExportFraming.RootArray ||
             content.RowCount > 0) &&
            content.DataByteLength == 0)
        {
            throw Invalid(
                "JSON export data bytes cannot be empty for this framing and row count.");
        }

        ValidateHash(
            content.DataDigest,
            "payload.content.dataDigest",
            ref textCharacters);
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
                JsonExportContracts.Canonicalization,
                StringComparison.Ordinal) ||
            !string.Equals(
                content.CanonicalizationContractDigest,
                JsonExportContracts
                    .CanonicalizationContractDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                content.Aggregation,
                JsonExportContracts.OrderedContentDigest,
                StringComparison.Ordinal))
        {
            throw Invalid(
                "JSON export logical checksum contract is unsupported.");
        }

        ValidateContentGeometry(
            content,
            format,
            table);
    }

    private static void ValidateContentGeometry(
        JsonExportContentManifest content,
        JsonExportFormatManifest format,
        JsonExportTableManifest table)
    {
        if (content.RowCount == 0)
        {
            long expectedLength =
                format.Framing ==
                    JsonExportFraming.RootArray
                    ? 3
                    : 0;
            if (content.DataByteLength !=
                expectedLength)
            {
                throw Invalid(
                    "Empty JSON export content has impossible framing geometry.");
            }

            ReadOnlySpan<byte> emptyData =
                format.Framing ==
                    JsonExportFraming.RootArray
                    ? "[]\n"u8
                    : ReadOnlySpan<byte>.Empty;
            byte[] physicalHash =
                SHA256.HashData(emptyData);
            try
            {
                if (!HashValuesEqual(
                        content.DataDigest.Value,
                        Hex(physicalHash)))
                {
                    throw Invalid(
                        "Empty JSON export content has an invalid physical digest.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(
                    physicalHash);
            }

            using var emptyLogical =
                new JsonExportOrderedContentDigest();
            JsonExportHashManifest expectedLogical =
                emptyLogical.Complete();
            if (!HashValuesEqual(
                    content.SourceLogicalDigest.Value,
                    expectedLogical.Value) ||
                !HashValuesEqual(
                    content.ExportedLogicalDigest.Value,
                    expectedLogical.Value))
            {
                throw Invalid(
                    "Empty JSON export content has an invalid logical digest.");
            }

            return;
        }

        long minimumObjectBytes =
            GetMinimumObjectByteLength(
                table.Columns);
        long minimumDataBytes;
        try
        {
            minimumDataBytes =
                format.Framing switch
                {
                    JsonExportFraming.RootArray =>
                        checked(
                            3L +
                            checked(
                                content.RowCount *
                                minimumObjectBytes) +
                            content.RowCount -
                            1L),
                    JsonExportFraming.Ndjson =>
                        checked(
                            content.RowCount *
                            checked(
                                minimumObjectBytes +
                                1L)),
                    _ => throw Invalid(
                        "JSON export framing is unsupported."),
                };
        }
        catch (OverflowException)
        {
            throw Invalid(
                "JSON export row count and schema exceed bounded content geometry.");
        }

        if (content.DataByteLength <
            minimumDataBytes)
        {
            throw Invalid(
                "JSON export data length is too short for its row count and schema.");
        }

        long maximumDataBytes =
            GetMaximumDataByteLength(
                content.RowCount,
                format);
        if (content.DataByteLength >
            maximumDataBytes)
        {
            throw Invalid(
                "JSON export data length exceeds its row-count and value-size geometry.");
        }
    }

    private static long GetMaximumDataByteLength(
        long rowCount,
        JsonExportFormatManifest format)
    {
        try
        {
            return format.Framing switch
            {
                JsonExportFraming.RootArray =>
                    checked(
                        3L +
                        checked(
                            rowCount *
                            format.MaximumValueBytes) +
                        rowCount -
                        1L),
                JsonExportFraming.Ndjson =>
                    checked(
                        rowCount *
                        checked(
                            format.MaximumValueBytes +
                            1L)),
                _ => throw Invalid(
                    "JSON export framing is unsupported."),
            };
        }
        catch (OverflowException)
        {
            return long.MaxValue;
        }
    }

    private static long GetMinimumObjectByteLength(
        IReadOnlyList<JsonExportColumnManifest> columns)
    {
        long length =
            checked(2L + columns.Count - 1L);
        foreach (JsonExportColumnManifest column in
                 columns)
        {
            length = checked(
                length +
                GetJsonStringLiteralByteLength(
                    column.PropertyName) +
                1L +
                GetMinimumValueByteLength(
                    column.DatabaseType));
        }

        return length;
    }

    private static int GetMinimumValueByteLength(
        JsonExportDatabaseType databaseType) =>
        databaseType switch
        {
            JsonExportDatabaseType.Integer => 1,
            JsonExportDatabaseType.Real => 1,
            JsonExportDatabaseType.Decimal => 1,
            JsonExportDatabaseType.Text => 2,
            JsonExportDatabaseType.Blob => 2,
            _ => throw Invalid(
                "JSON export column type is unsupported."),
        };

    private static long GetJsonStringLiteralByteLength(
        string value)
    {
        long length = 2;
        for (int index = 0;
             index < value.Length;
             index++)
        {
            char character = value[index];
            if (char.IsHighSurrogate(character))
            {
                length = checked(length + 4L);
                index++;
                continue;
            }

            length = checked(
                length +
                (character switch
                {
                    '"' or '\\' => 2,
                    < '\u0020' =>
                        character is
                            '\b' or '\t' or '\n' or
                            '\f' or '\r'
                            ? 2
                            : 6,
                    <= '\u007f' => 1,
                    <= '\u07ff' => 2,
                    _ => 3,
                }));
        }

        return length;
    }

    private static void ValidateHash(
        JsonExportHashManifest hash,
        string path,
        ref long textCharacters)
    {
        RequireText(
            hash.Algorithm,
            $"{path}.algorithm",
            ref textCharacters);
        RequireText(
            hash.Value,
            $"{path}.value",
            ref textCharacters);
        if (!string.Equals(
                hash.Algorithm,
                JsonExportHashManifest.Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw Invalid(
                $"JSON export hash '{path}' uses an unsupported algorithm.");
        }
        if (!IsLowercaseSha256(hash.Value))
        {
            throw Invalid(
                $"JSON export hash '{path}' is not lowercase SHA-256 text.");
        }
    }

    private static void VerifyDigest(
        string? suppliedDigest,
        string expectedDigest)
    {
        if (!IsLowercaseSha256(suppliedDigest))
        {
            throw Invalid(
                "JSON export manifest digest is not lowercase SHA-256 text.");
        }
        VerifyHashValue(
            suppliedDigest!,
            expectedDigest,
            "JSON export manifest digest does not match its payload.");
    }

    private static void VerifyHashValue(
        string supplied,
        string expected,
        string mismatchMessage)
    {
        byte[] suppliedBytes =
            Convert.FromHexString(supplied);
        byte[] expectedBytes =
            Convert.FromHexString(expected);
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    suppliedBytes,
                    expectedBytes))
            {
                throw Invalid(mismatchMessage);
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
        string first,
        string second)
    {
        byte[] firstBytes =
            Convert.FromHexString(first);
        byte[] secondBytes =
            Convert.FromHexString(second);
        try
        {
            return CryptographicOperations.FixedTimeEquals(
                firstBytes,
                secondBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(firstBytes);
            CryptographicOperations.ZeroMemory(secondBytes);
        }
    }

    private static JsonExportHashManifest CreateHash(
        string value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest.Sha256Algorithm,
            Value = value,
        };

    private static string Hex(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(bytes)
            .ToLowerInvariant();

    private static bool IsLowercaseSha256(
        string? value)
    {
        if (value is null || value.Length != 64)
            return false;

        foreach (char character in value)
        {
            if (character is not
                    (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    private static void RequireText(
        string? value,
        string path,
        ref long textCharacters)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw Invalid(
                $"JSON export manifest member '{path}' must be nonblank.");
        }

        ValidateText(
            value,
            path,
            ref textCharacters);
    }

    private static void ValidateText(
        string value,
        string path,
        ref long textCharacters)
    {
        if (value.Contains(
                '\0',
                StringComparison.Ordinal))
        {
            throw Invalid(
                $"JSON export manifest member '{path}' contains a NUL character.");
        }

        try
        {
            _ = s_strictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException)
        {
            throw new InvalidDataException(
                $"JSON export manifest member '{path}' contains invalid UTF-16 text.");
        }

        textCharacters =
            checked(textCharacters + value.Length);
        EnsureTextBudget(textCharacters);
    }

    private static int StrictUtf8ByteCount(
        string value,
        string path)
    {
        try
        {
            return s_strictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException)
        {
            throw new InvalidDataException(
                $"JSON export manifest member '{path}' contains invalid UTF-16 text.");
        }
    }

    private static void EnsureTextBudget(
        long textCharacters)
    {
        if (textCharacters > MaximumTextCharacters)
        {
            throw Invalid(
                $"JSON export manifest text exceeds the {MaximumTextCharacters}-character safety limit.");
        }
    }

    private static void ValidateInputEncoding(
        ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
        {
            throw Invalid(
                "The JSON export manifest is empty.");
        }
        if (utf8Json.Length > MaximumManifestBytes)
        {
            throw Invalid(
                $"The JSON export manifest exceeds the {MaximumManifestBytes}-byte safety limit.");
        }
        if (utf8Json.StartsWith(Encoding.UTF8.Preamble))
        {
            throw Invalid(
                "The JSON export manifest must not contain a UTF-8 BOM.");
        }
        if (utf8Json.IndexOf((byte)0) >= 0)
        {
            throw Invalid(
                "The JSON export manifest must not contain NUL bytes.");
        }

        try
        {
            _ = s_strictUtf8.GetCharCount(utf8Json);
        }
        catch (DecoderFallbackException)
        {
            throw new InvalidDataException(
                "The JSON export manifest is not strict UTF-8.");
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
                    if (!names.Add(property.Name))
                    {
                        throw Invalid(
                            "The JSON export manifest contains a duplicate property.");
                    }

                    RejectDuplicateProperties(
                        property.Value);
                }

                break;

            case JsonValueKind.Array:
                foreach (JsonElement item in
                         element.EnumerateArray())
                {
                    RejectDuplicateProperties(
                        item);
                }

                break;
        }
    }

    private static void RequireMember<T>(
        T? value,
        string path)
        where T : class
    {
        if (value is null)
        {
            throw Invalid(
                $"JSON export manifest member '{path}' is required.");
        }
    }

    private static InvalidDataException Invalid(
        string message) =>
        new(message);

    private static void Zero(byte[]? value)
    {
        if (value is not null)
            CryptographicOperations.ZeroMemory(value);
    }

    private static JsonSerializerOptions CreateOptions()
    {
        var options =
            new JsonSerializerOptions(
                JsonSerializerDefaults.Web)
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
}

internal sealed record JsonExportManifestEnvelope<TPayload>
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

internal sealed record JsonExportManifestDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Format { get; init; }

    [JsonPropertyOrder(1)]
    public required string DigestAlgorithm { get; init; }

    [JsonPropertyOrder(2)]
    public required JsonExportManifest Payload { get; init; }
}

internal sealed record JsonExportSchemaDigestInput
{
    [JsonPropertyOrder(0)]
    public required string Contract { get; init; }

    [JsonPropertyOrder(1)]
    public required IReadOnlyList<JsonExportColumnManifest> Columns
    {
        get;
        init;
    }
}
