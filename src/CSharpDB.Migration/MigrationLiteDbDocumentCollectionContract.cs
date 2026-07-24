using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace CSharpDB.Migration;

/// <summary>
/// Versioned source and row-bridge contract for preserving a LiteDB BSON
/// collection as tagged canonical JSON in a CSharpDB document collection.
/// This contract deliberately has no dependency on the LiteDB provider.
/// </summary>
public static class MigrationLiteDbDocumentCollectionContract
{
    private const int MaximumTypedKeyCharacters = 16 * 1024 * 1024;

    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public const string ProjectionContract =
        "csharpdb-litedb-collection-projection/v1";

    public const string RowContract =
        MigrationDocumentCollectionContract.RowContract;

    public const string KeyContract =
        "csharpdb-litedb-typed-source-key/v1";

    public const string CursorContract =
        "csharpdb-litedb-collection-cursor/v1";

    public const string SchemaContract =
        "csharpdb-litedb-collection-schema/v1";

    public const string DocumentEncoding =
        "csharpdb-litedb-tagged-bson-document/v1";

    public const string TypedKeyPrefix = "litedb-key-v1:";

    public const string TaggedBsonTypeProperty = "$bson";

    public const string TaggedBsonValueProperty = "value";

    public const string DocumentEntryNameProperty = "name";

    public const string DocumentEntryValueProperty = "value";

    public const string ProjectionFacet = "liteDbCollectionProjection";

    public const string RowContractFacet = "liteDbCollectionRowContract";

    public const string KeyContractFacet = "liteDbCollectionKeyContract";

    public const string CursorContractFacet = "liteDbCollectionCursorContract";

    public const string SchemaContractFacet = "liteDbSchemaAlgorithm";

    public const string DocumentEncodingFacet = "liteDbDocumentEncoding";

    public const string FieldRoleFacet = "liteDbCollectionFieldRole";

    public const string KeyRole = MigrationDocumentCollectionContract.KeyRole;

    public const string DocumentRole =
        MigrationDocumentCollectionContract.DocumentRole;

    public const string KeyColumnName =
        MigrationDocumentCollectionContract.KeyColumnName;

    public const string DocumentColumnName =
        MigrationDocumentCollectionContract.DocumentColumnName;

    public const string KeyNativeType = "LITEDB_TYPED_SOURCE_KEY";

    public const string DocumentNativeType = "LITEDB_TAGGED_BSON_DOCUMENT";

    public const string LogicalTypeFacet =
        MigrationDocumentCollectionContract.LogicalTypeFacet;

    public const string NullableFacet =
        MigrationDocumentCollectionContract.NullableFacet;

    public const string TextLogicalType =
        MigrationDocumentCollectionContract.TextLogicalType;

    public const string JsonLogicalType =
        MigrationDocumentCollectionContract.JsonLogicalType;

    public static IReadOnlyList<MigrationCatalogFacet>
        RequiredCollectionFacets
    { get; } = ReadOnly(
        [
            Facet(ProjectionFacet, ProjectionContract),
            Facet(RowContractFacet, RowContract),
            Facet(KeyContractFacet, KeyContract),
            Facet(CursorContractFacet, CursorContract),
            Facet(SchemaContractFacet, SchemaContract),
            Facet(DocumentEncodingFacet, DocumentEncoding),
        ]);

    public static IReadOnlyList<MigrationCatalogFacet> CreateKeyFacets() =>
        ReadOnly(
        [
            Facet(LogicalTypeFacet, TextLogicalType),
            Facet(NullableFacet, "false"),
            Facet(FieldRoleFacet, KeyRole),
            Facet(KeyContractFacet, KeyContract),
        ]);

    public static IReadOnlyList<MigrationCatalogFacet> CreateDocumentFacets() =>
        ReadOnly(
        [
            Facet(LogicalTypeFacet, JsonLogicalType),
            Facet(NullableFacet, "false"),
            Facet(FieldRoleFacet, DocumentRole),
            Facet(DocumentEncodingFacet, DocumentEncoding),
        ]);

    /// <summary>
    /// Validates the exact, canonical provider-neutral representation used for
    /// a LiteDB scalar <c>_id</c>. The migration target uses this check before
    /// accepting a stable source key so a malformed key cannot masquerade as
    /// the versioned typed-key contract.
    /// </summary>
    public static bool TryValidateTypedKey(
        string? key,
        out string? reason)
    {
        reason = null;
        if (key is null ||
            key.Length <= TypedKeyPrefix.Length ||
            key.Length > MaximumTypedKeyCharacters ||
            !key.StartsWith(TypedKeyPrefix, StringComparison.Ordinal))
        {
            return Fail(
                "The LiteDB typed source key has an invalid prefix or length.",
                out reason);
        }

        string encoded = key[TypedKeyPrefix.Length..];
        if (!TryDecodeCanonicalBase64Url(encoded, out byte[] bytes))
        {
            return Fail(
                "The LiteDB typed source key payload is not canonical base64url.",
                out reason);
        }

        string tagged;
        try
        {
            tagged = StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return Fail(
                "The LiteDB typed source key payload is not valid UTF-8.",
                out reason);
        }

        if (!TryGetCanonicalTaggedScalar(tagged, out string? canonical) ||
            !string.Equals(tagged, canonical, StringComparison.Ordinal))
        {
            return Fail(
                "The LiteDB typed source key payload is not a canonical tagged BSON scalar.",
                out reason);
        }

        return true;
    }

    public static bool TryBindExactV1Collection(
        MigrationCatalogObject collection,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        out MigrationCatalogObject? keyColumn,
        out MigrationCatalogObject? documentColumn,
        out string? reason)
    {
        ArgumentNullException.ThrowIfNull(collection);
        ArgumentNullException.ThrowIfNull(objectsById);

        keyColumn = null;
        documentColumn = null;
        reason = null;

        if (collection.Kind != MigrationObjectKind.Collection)
            return Fail($"Object '{collection.ObjectId}' is not a collection.", out reason);
        if (collection.NativeType is not null)
        {
            return Fail(
                $"Collection '{collection.ObjectId}' cannot declare a scalar native type.",
                out reason);
        }
        if (collection.ParentObjectId is not null &&
            (!objectsById.TryGetValue(
                collection.ParentObjectId,
                out MigrationCatalogObject? parent) ||
             parent.Kind != MigrationObjectKind.Namespace))
        {
            return Fail(
                $"Collection '{collection.ObjectId}' must be top-level or contained by a namespace.",
                out reason);
        }
        if (collection.Members.Count != 0)
        {
            return Fail(
                $"Collection '{collection.ObjectId}' cannot declare ordered members.",
                out reason);
        }
        if (collection.DependsOn.Count != 0)
        {
            return Fail(
                $"Collection '{collection.ObjectId}' cannot declare dependencies.",
                out reason);
        }

        foreach (MigrationCatalogFacet required in RequiredCollectionFacets)
        {
            if (!HasExactFacet(collection, required.Name, required.Value!))
            {
                return Fail(
                    $"Collection '{collection.ObjectId}' requires facet '{required.Name}' with value '{required.Value}'.",
                    out reason);
            }
        }

        MigrationCatalogObject[] children = objectsById.Values
            .Where(candidate =>
                string.Equals(
                    candidate.ParentObjectId,
                    collection.ObjectId,
                    StringComparison.Ordinal))
            .OrderBy(candidate => candidate.ObjectId, StringComparer.Ordinal)
            .ToArray();
        MigrationCatalogObject[] columns = children
            .Where(candidate => candidate.Kind == MigrationObjectKind.Column)
            .ToArray();
        if (columns.Length != 2 ||
            children.Any(candidate =>
                candidate.Kind is not (
                    MigrationObjectKind.Column or MigrationObjectKind.Index)))
        {
            return Fail(
                $"Collection '{collection.ObjectId}' requires exactly two direct child columns and permits only separately validated index siblings.",
                out reason);
        }

        keyColumn = columns.SingleOrDefault(candidate =>
            string.Equals(
                candidate.SourceName,
                KeyColumnName,
                StringComparison.Ordinal));
        documentColumn = columns.SingleOrDefault(candidate =>
            string.Equals(
                candidate.SourceName,
                DocumentColumnName,
                StringComparison.Ordinal));
        if (keyColumn is null || documentColumn is null)
        {
            keyColumn = null;
            documentColumn = null;
            return Fail(
                $"Collection '{collection.ObjectId}' requires columns named '{KeyColumnName}' and '{DocumentColumnName}'.",
                out reason);
        }

        if (!ValidateColumn(
                keyColumn,
                KeyNativeType,
                KeyRole,
                TextLogicalType,
                KeyContractFacet,
                KeyContract,
                out reason) ||
            !ValidateColumn(
                documentColumn,
                DocumentNativeType,
                DocumentRole,
                JsonLogicalType,
                DocumentEncodingFacet,
                DocumentEncoding,
                out reason))
        {
            keyColumn = null;
            documentColumn = null;
            return false;
        }

        string keyColumnId = keyColumn.ObjectId;
        string documentColumnId = documentColumn.ObjectId;
        if (objectsById.Values.Any(candidate =>
                string.Equals(
                    candidate.ParentObjectId,
                    keyColumnId,
                    StringComparison.Ordinal) ||
                string.Equals(
                    candidate.ParentObjectId,
                    documentColumnId,
                    StringComparison.Ordinal)))
        {
            keyColumn = null;
            documentColumn = null;
            return Fail(
                $"Collection '{collection.ObjectId}' cannot declare descendants beneath its key or document bridge columns.",
                out reason);
        }

        return true;
    }

    internal static bool IsExactV1DocumentColumn(
        MigrationCatalogObject source) =>
        string.Equals(
            source.NativeType,
            DocumentNativeType,
            StringComparison.Ordinal) &&
        HasExactFacet(source, LogicalTypeFacet, JsonLogicalType) &&
        HasExactFacet(source, NullableFacet, "false") &&
        HasExactFacet(source, FieldRoleFacet, DocumentRole) &&
        HasExactFacet(
            source,
            DocumentEncodingFacet,
            DocumentEncoding);

    private static bool ValidateColumn(
        MigrationCatalogObject column,
        string nativeType,
        string role,
        string logicalType,
        string versionFacet,
        string version,
        out string? reason)
    {
        if (!string.Equals(column.NativeType, nativeType, StringComparison.Ordinal))
        {
            return Fail(
                $"Collection column '{column.ObjectId}' requires native type '{nativeType}'.",
                out reason);
        }
        if (column.Members.Count != 0)
        {
            return Fail(
                $"Collection column '{column.ObjectId}' cannot declare ordered members.",
                out reason);
        }
        if (column.DependsOn.Count != 0)
        {
            return Fail(
                $"Collection column '{column.ObjectId}' cannot declare dependencies.",
                out reason);
        }
        if (!HasExactFacet(column, FieldRoleFacet, role))
        {
            return Fail(
                $"Collection column '{column.ObjectId}' requires facet '{FieldRoleFacet}' with value '{role}'.",
                out reason);
        }
        if (!HasExactFacet(column, LogicalTypeFacet, logicalType))
        {
            return Fail(
                $"Collection column '{column.ObjectId}' requires facet '{LogicalTypeFacet}' with value '{logicalType}'.",
                out reason);
        }
        if (!HasExactFacet(column, NullableFacet, "false"))
        {
            return Fail(
                $"Collection column '{column.ObjectId}' requires facet '{NullableFacet}' with value 'false'.",
                out reason);
        }
        if (!HasExactFacet(column, versionFacet, version))
        {
            return Fail(
                $"Collection column '{column.ObjectId}' requires facet '{versionFacet}' with value '{version}'.",
                out reason);
        }

        reason = null;
        return true;
    }

    private static bool TryDecodeCanonicalBase64Url(
        string encoded,
        out byte[] bytes)
    {
        bytes = [];
        if (encoded.Length == 0 ||
            encoded.Length % 4 == 1 ||
            encoded.Any(static character =>
                character is not (
                    >= 'A' and <= 'Z' or
                    >= 'a' and <= 'z' or
                    >= '0' and <= '9' or
                    '-' or '_')))
        {
            return false;
        }

        string padded = encoded
            .Replace('-', '+')
            .Replace('_', '/')
            .PadRight(checked(encoded.Length + ((4 - encoded.Length % 4) % 4)), '=');
        try
        {
            bytes = Convert.FromBase64String(padded);
        }
        catch (FormatException)
        {
            return false;
        }

        string roundTrip = Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        return string.Equals(encoded, roundTrip, StringComparison.Ordinal);
    }

    private static bool TryGetCanonicalTaggedScalar(
        string tagged,
        out string? canonical)
    {
        canonical = null;
        try
        {
            using JsonDocument document = JsonDocument.Parse(
                tagged,
                new JsonDocumentOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = 4,
                });
            JsonElement root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;

            JsonProperty[] properties = root.EnumerateObject().ToArray();
            if (properties.Length is < 1 or > 2 ||
                !string.Equals(
                    properties[0].Name,
                    TaggedBsonTypeProperty,
                    StringComparison.Ordinal) ||
                properties[0].Value.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            string? type = properties[0].Value.GetString();
            if (type is "min" or "max")
            {
                if (properties.Length != 1)
                    return false;
                canonical = string.Concat(
                    "{\"",
                    TaggedBsonTypeProperty,
                    "\":\"",
                    type,
                    "\"}");
                return true;
            }

            if (properties.Length != 2 ||
                !string.Equals(
                    properties[1].Name,
                    TaggedBsonValueProperty,
                    StringComparison.Ordinal))
            {
                return false;
            }

            JsonElement value = properties[1].Value;
            return type switch
            {
                "int32" => TryCanonicalInteger(
                    type,
                    value,
                    static text => int.TryParse(
                        text,
                        NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture,
                        out int parsed)
                        ? parsed.ToString(CultureInfo.InvariantCulture)
                        : null,
                    out canonical),
                "int64" => TryCanonicalInteger(
                    type,
                    value,
                    static text => long.TryParse(
                        text,
                        NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture,
                        out long parsed)
                        ? parsed.ToString(CultureInfo.InvariantCulture)
                        : null,
                    out canonical),
                "double" => TryCanonicalPattern(
                    type,
                    value,
                    IsUpperHex(value, groups: 1),
                    out canonical),
                "decimal" => TryCanonicalDecimal(type, value, out canonical),
                "string" => TryCanonicalString(type, value, out canonical),
                "binary" => TryCanonicalBinary(type, value, out canonical),
                "objectId" => TryCanonicalObjectId(type, value, out canonical),
                "guid" => TryCanonicalGuid(type, value, out canonical),
                "boolean" => TryCanonicalBoolean(type, value, out canonical),
                "dateTime" => TryCanonicalDateTime(type, value, out canonical),
                _ => false,
            };
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool TryCanonicalInteger(
        string type,
        JsonElement value,
        Func<string, string?> normalize,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        string? normalized = text is null ? null : normalize(text);
        if (normalized is null ||
            !string.Equals(text, normalized, StringComparison.Ordinal))
        {
            return false;
        }

        canonical = ScalarString(type, normalized);
        return true;
    }

    private static bool TryCanonicalPattern(
        string type,
        JsonElement value,
        bool patternMatches,
        out string? canonical)
    {
        canonical = null;
        if (!patternMatches || value.ValueKind != JsonValueKind.String)
            return false;
        canonical = ScalarString(type, value.GetString()!);
        return true;
    }

    private static bool TryCanonicalDecimal(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        string[] parts = text?.Split('-') ?? [];
        if (parts.Length != 4 ||
            parts.Any(static part => !IsUpperHexText(part, 8)) ||
            !uint.TryParse(
                parts[3],
                NumberStyles.AllowHexSpecifier,
                CultureInfo.InvariantCulture,
                out uint flags))
        {
            return false;
        }

        uint scale = (flags >> 16) & 0xFF;
        if ((flags & 0x7F00FFFFU) != 0 || scale > 28)
            return false;

        canonical = ScalarString(type, text!);
        return true;
    }

    private static bool TryCanonicalString(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        if (text is null)
            return false;
        canonical = string.Concat(
            "{\"",
            TaggedBsonTypeProperty,
            "\":\"",
            type,
            "\",\"",
            TaggedBsonValueProperty,
            "\":",
            QuoteCanonicalJsonString(text),
            "}");
        return true;
    }

    private static bool TryCanonicalBinary(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        try
        {
            byte[] bytes = Convert.FromBase64String(text ?? string.Empty);
            if (!string.Equals(
                    text,
                    Convert.ToBase64String(bytes),
                    StringComparison.Ordinal))
            {
                return false;
            }
        }
        catch (FormatException)
        {
            return false;
        }

        canonical = ScalarString(type, text!);
        return true;
    }

    private static bool TryCanonicalObjectId(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        if (text is null ||
            text.Length != 24 ||
            text.Any(static character =>
                character is not (
                    >= '0' and <= '9' or
                    >= 'a' and <= 'f')))
        {
            return false;
        }
        canonical = ScalarString(type, text);
        return true;
    }

    private static bool TryCanonicalGuid(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.String)
            return false;
        string? text = value.GetString();
        if (!Guid.TryParseExact(text, "D", out Guid parsed) ||
            !string.Equals(
                text,
                parsed.ToString("D", CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            return false;
        }
        canonical = ScalarString(type, text!);
        return true;
    }

    private static bool TryCanonicalBoolean(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind is not (
                JsonValueKind.True or JsonValueKind.False))
        {
            return false;
        }
        canonical = string.Concat(
            "{\"",
            TaggedBsonTypeProperty,
            "\":\"",
            type,
            "\",\"",
            TaggedBsonValueProperty,
            "\":",
            value.GetBoolean() ? "true" : "false",
            "}");
        return true;
    }

    private static bool TryCanonicalDateTime(
        string type,
        JsonElement value,
        out string? canonical)
    {
        canonical = null;
        if (value.ValueKind != JsonValueKind.Object)
            return false;
        JsonProperty[] properties = value.EnumerateObject().ToArray();
        if (properties.Length != 2 ||
            properties[0].Name != "ticks" ||
            properties[0].Value.ValueKind != JsonValueKind.String ||
            properties[1].Name != "kind" ||
            properties[1].Value.ValueKind != JsonValueKind.String)
        {
            return false;
        }

        string? ticksText = properties[0].Value.GetString();
        string? kindText = properties[1].Value.GetString();
        if (!long.TryParse(
                ticksText,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out long ticks) ||
            ticks < DateTime.MinValue.Ticks ||
            ticks > DateTime.MaxValue.Ticks ||
            !int.TryParse(
                kindText,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int kind) ||
            kind is < (int)DateTimeKind.Unspecified or > (int)DateTimeKind.Local ||
            ticksText != ticks.ToString(CultureInfo.InvariantCulture) ||
            kindText != kind.ToString(CultureInfo.InvariantCulture))
        {
            return false;
        }

        canonical = string.Concat(
            "{\"",
            TaggedBsonTypeProperty,
            "\":\"",
            type,
            "\",\"",
            TaggedBsonValueProperty,
            "\":{\"ticks\":\"",
            ticksText,
            "\",\"kind\":\"",
            kindText,
            "\"}}");
        return true;
    }

    private static bool IsUpperHex(JsonElement value, int groups) =>
        value.ValueKind == JsonValueKind.String &&
        IsUpperHexText(value.GetString(), checked(groups * 16));

    private static bool IsUpperHexText(string? text, int length) =>
        text is not null &&
        text.Length == length &&
        text.All(static character =>
            character is >= '0' and <= '9' or >= 'A' and <= 'F');

    private static string ScalarString(string type, string value) =>
        string.Concat(
            "{\"",
            TaggedBsonTypeProperty,
            "\":\"",
            type,
            "\",\"",
            TaggedBsonValueProperty,
            "\":\"",
            value,
            "\"}");

    private static string QuoteCanonicalJsonString(string value)
    {
        var builder = new StringBuilder(checked(value.Length + 2));
        builder.Append('"');
        foreach (char character in value)
        {
            switch (character)
            {
                case '"':
                    builder.Append("\\\"");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\b':
                    builder.Append("\\b");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\f':
                    builder.Append("\\f");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case <= '\u001F':
                    builder.Append("\\u00");
                    builder.Append(
                        "0123456789abcdef"[(character >> 4) & 0x0F]);
                    builder.Append(
                        "0123456789abcdef"[character & 0x0F]);
                    break;
                default:
                    builder.Append(character);
                    break;
            }
        }
        builder.Append('"');
        return builder.ToString();
    }

    private static bool HasExactFacet(
        MigrationCatalogObject item,
        string name,
        string value) =>
        item.Facets.Any(facet =>
            string.Equals(facet.Name, name, StringComparison.Ordinal) &&
            string.Equals(facet.Value, value, StringComparison.Ordinal));

    private static MigrationCatalogFacet Facet(
        string name,
        string value) => new()
        {
            Name = name,
            Value = value,
        };

    private static ReadOnlyCollection<MigrationCatalogFacet> ReadOnly(
        MigrationCatalogFacet[] facets) =>
        Array.AsReadOnly(facets);

    private static bool Fail(string failureReason, out string? reason)
    {
        reason = failureReason;
        return false;
    }
}
