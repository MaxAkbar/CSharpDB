using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public enum MigrationDocumentCollectionKeyMode
{
    SourceOrdinal,
    StableSourceKey,
}

public sealed record MigrationDocumentCollectionBinding
{
    public required MigrationDocumentCollectionKeyMode KeyMode { get; init; }

    public required MigrationCatalogObject KeyColumn { get; init; }

    public required MigrationCatalogObject DocumentColumn { get; init; }
}

/// <summary>
/// Versioned source and row-bridge contract for projecting ordered JSON values
/// into a CSharpDB document collection.
/// </summary>
public static class MigrationDocumentCollectionContract
{
    public const string ProjectionContract = "csharpdb-json-collection-projection/v1";

    public const string RowContract = "csharpdb-migration-collection-document-row/v1";

    public const string KeyContract = "csharpdb-json-source-ordinal-key/v1";

    public const string CursorContract = "csharpdb-json-collection-cursor/v1";

    public const string SchemaContract = "csharpdb-json-collection-schema/v1";

    public const string DocumentEncoding = "csharpdb-json-ordered-value/v1";

    public const string KeyFormat = "json-ordinal-v1:{zeroBasedOrdinal:D20}";

    public const string ProjectionFacet = "jsonCollectionProjection";

    public const string RowContractFacet = "jsonCollectionRowContract";

    public const string KeyContractFacet = "jsonCollectionKeyContract";

    public const string CursorContractFacet = "jsonCollectionCursorContract";

    public const string SchemaContractFacet = "jsonSchemaAlgorithm";

    public const string DocumentEncodingFacet = "jsonCanonicalValueVersion";

    public const string FieldRoleFacet = "jsonCollectionFieldRole";

    public const string KeyRole = "key";

    public const string DocumentRole = "document";

    public const string KeyColumnName = "_key";

    public const string DocumentColumnName = "_doc";

    public const string KeyNativeType = "JSON_COLLECTION_KEY";

    public const string DocumentNativeType = "JSON_ORDERED_DOCUMENT";

    public const string LogicalTypeFacet = "logicalType";

    public const string NullableFacet = "nullable";

    public const string TextLogicalType = "text";

    public const string JsonLogicalType = "json";

    public const string CollectionPhysicalNamePrefix = "_col_";

    public const int CollectionPhysicalNamePrefixLength = 5;

    public const int OrdinalKeyWidth = 20;

    public const int MaximumLogicalCollectionNameLength =
        SqlIdentifierRules.MaxLength - CollectionPhysicalNamePrefixLength;

    public static string FormatOrdinalKey(long zeroBasedOrdinal)
    {
        if (zeroBasedOrdinal < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(zeroBasedOrdinal),
                zeroBasedOrdinal,
                "A JSON source ordinal cannot be negative.");
        }

        return string.Concat(
            "json-ordinal-v1:",
            zeroBasedOrdinal.ToString($"D{OrdinalKeyWidth}", CultureInfo.InvariantCulture));
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
            return Fail($"Collection '{collection.ObjectId}' cannot declare ordered members.", out reason);
        if (collection.DependsOn.Count != 0)
            return Fail($"Collection '{collection.ObjectId}' cannot declare dependencies.", out reason);

        (string Name, string Value)[] requiredCollectionFacets =
        [
            (ProjectionFacet, ProjectionContract),
            (RowContractFacet, RowContract),
            (KeyContractFacet, KeyContract),
            (CursorContractFacet, CursorContract),
            (SchemaContractFacet, SchemaContract),
            (DocumentEncodingFacet, DocumentEncoding),
        ];
        foreach ((string name, string value) in requiredCollectionFacets)
        {
            if (!HasExactFacet(collection, name, value))
            {
                return Fail(
                    $"Collection '{collection.ObjectId}' requires facet '{name}' with value '{value}'.",
                    out reason);
            }
        }

        MigrationCatalogObject[] children = objectsById.Values
            .Where(candidate =>
                string.Equals(candidate.ParentObjectId, collection.ObjectId, StringComparison.Ordinal))
            .OrderBy(candidate => candidate.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (children.Length != 2 || children.Any(candidate => candidate.Kind != MigrationObjectKind.Column))
        {
            return Fail(
                $"Collection '{collection.ObjectId}' requires exactly two direct child columns.",
                out reason);
        }

        keyColumn = children.SingleOrDefault(candidate =>
            string.Equals(candidate.SourceName, KeyColumnName, StringComparison.Ordinal));
        documentColumn = children.SingleOrDefault(candidate =>
            string.Equals(candidate.SourceName, DocumentColumnName, StringComparison.Ordinal));
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

    public static bool TryBindSupportedV1Collection(
        MigrationCatalogObject collection,
        IReadOnlyDictionary<string, MigrationCatalogObject> objectsById,
        out MigrationDocumentCollectionBinding? binding,
        out string? reason)
    {
        ArgumentNullException.ThrowIfNull(collection);
        ArgumentNullException.ThrowIfNull(objectsById);

        binding = null;
        reason = null;

        bool isJson = HasExactFacet(
            collection,
            ProjectionFacet,
            ProjectionContract);
        bool isLiteDb = collection.Facets.Any(facet =>
            string.Equals(
                facet.Name,
                MigrationLiteDbDocumentCollectionContract.ProjectionFacet,
                StringComparison.Ordinal) &&
            string.Equals(
                facet.Value,
                MigrationLiteDbDocumentCollectionContract.ProjectionContract,
                StringComparison.Ordinal));
        if (isJson == isLiteDb)
        {
            reason = isJson
                ? $"Collection '{collection.ObjectId}' declares ambiguous document collection projection contracts."
                : $"Collection '{collection.ObjectId}' does not declare a supported version 1 document collection projection contract.";
            return false;
        }

        MigrationCatalogObject? keyColumn;
        MigrationCatalogObject? documentColumn;
        bool bound;
        if (isJson)
        {
            bound = TryBindExactV1Collection(
                collection,
                objectsById,
                out keyColumn,
                out documentColumn,
                out reason);
        }
        else
        {
            bound = MigrationLiteDbDocumentCollectionContract
                .TryBindExactV1Collection(
                    collection,
                    objectsById,
                    out keyColumn,
                    out documentColumn,
                    out reason);
        }
        if (!bound)
            return false;

        binding = new MigrationDocumentCollectionBinding
        {
            KeyMode = isJson
                ? MigrationDocumentCollectionKeyMode.SourceOrdinal
                : MigrationDocumentCollectionKeyMode.StableSourceKey,
            KeyColumn = keyColumn!,
            DocumentColumn = documentColumn!,
        };
        return true;
    }

    internal static bool IsSupportedV1DocumentColumn(
        MigrationCatalogObject source) =>
        IsExactV1DocumentColumn(source) ||
        MigrationLiteDbDocumentCollectionContract.IsExactV1DocumentColumn(
            source);

    public static string GetPhysicalCollectionName(string logicalCollectionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalCollectionName);
        SqlIdentifierRules.Validate(logicalCollectionName, "Logical collection name");
        if (logicalCollectionName.Length > MaximumLogicalCollectionNameLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(logicalCollectionName),
                logicalCollectionName,
                $"A logical collection name cannot exceed {MaximumLogicalCollectionNameLength} characters because CSharpDB prefixes its physical table with '{CollectionPhysicalNamePrefix}'.");
        }

        return CollectionPhysicalNamePrefix + logicalCollectionName;
    }

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
            return Fail($"Collection column '{column.ObjectId}' cannot declare ordered members.", out reason);
        if (column.DependsOn.Count != 0)
            return Fail($"Collection column '{column.ObjectId}' cannot declare dependencies.", out reason);
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

    private static bool HasExactFacet(
        MigrationCatalogObject item,
        string name,
        string value) =>
        item.Facets.Any(facet =>
            string.Equals(facet.Name, name, StringComparison.Ordinal) &&
            string.Equals(facet.Value, value, StringComparison.Ordinal));

    private static bool IsExactV1DocumentColumn(
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

    private static bool Fail(string failureReason, out string? reason)
    {
        reason = failureReason;
        return false;
    }
}
