using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Projects one fully scanned JSON document source into the provider-neutral
/// migration catalog.
/// </summary>
public static class JsonDocumentCollectionCatalogBuilder
{
    public static MigrationCatalog Build(
        JsonDocumentCollectionProjectionResult result,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (string.IsNullOrWhiteSpace(targetCSharpDbVersion))
        {
            throw new ArgumentException(
                "The target CSharpDB version must be nonblank.",
                nameof(targetCSharpDbVersion));
        }

        MigrationCatalogFacet[] collectionFacets =
        [
            Facet(
                MigrationDocumentCollectionContract
                    .ProjectionFacet,
                MigrationDocumentCollectionContract
                    .ProjectionContract),
            Facet(
                MigrationDocumentCollectionContract
                    .RowContractFacet,
                MigrationDocumentCollectionContract.RowContract),
            Facet(
                MigrationDocumentCollectionContract
                    .KeyContractFacet,
                MigrationDocumentCollectionContract.KeyContract),
            Facet(
                MigrationDocumentCollectionContract
                    .CursorContractFacet,
                MigrationDocumentCollectionContract
                    .CursorContract),
            Facet(
                MigrationDocumentCollectionContract
                    .SchemaContractFacet,
                MigrationDocumentCollectionContract
                    .SchemaContract),
            Facet(
                MigrationDocumentCollectionContract
                    .DocumentEncodingFacet,
                MigrationDocumentCollectionContract
                    .DocumentEncoding),
            Facet(
                "jsonSourceBindingOptionsDigest",
                result.Binding.OptionsDigest),
            Facet("jsonSnapshotIdentity", result.SnapshotIdentity),
            Facet("jsonContentDigest", result.ContentDigest),
            Facet(
                "jsonContentLength",
                Invariant(result.ContentLength)),
            Facet(
                "jsonInputFraming",
                Framing(result.Binding.Framing)),
            Facet(
                "jsonTotalRecords",
                Invariant(result.TotalRecords)),
            Facet(
                "jsonNullRecords",
                Invariant(result.NullRecords)),
            Facet(
                "jsonBooleanRecords",
                Invariant(result.BooleanRecords)),
            Facet(
                "jsonStringRecords",
                Invariant(result.StringRecords)),
            Facet(
                "jsonNumberRecords",
                Invariant(result.NumberRecords)),
            Facet(
                "jsonObjectRecords",
                Invariant(result.ObjectRecords)),
            Facet(
                "jsonArrayRecords",
                Invariant(result.ArrayRecords)),
            Facet(
                "jsonMaxCanonicalDocumentBytes",
                Invariant(result.MaxCanonicalDocumentBytes)),
        ];

        MigrationCatalogObject[] objects =
        [
            new()
            {
                ObjectId =
                    JsonDocumentCollectionObjectIds.MainNamespace,
                Kind = MigrationObjectKind.Namespace,
                SourceName = "main",
                Facets = [Facet("isDefault", "true")],
            },
            new()
            {
                ObjectId =
                    JsonDocumentCollectionObjectIds.Collection,
                Kind = MigrationObjectKind.Collection,
                ParentObjectId =
                    JsonDocumentCollectionObjectIds.MainNamespace,
                SourceNamespace = "main",
                SourceName = result.CollectionName,
                Facets = collectionFacets,
            },
            new()
            {
                ObjectId =
                    JsonDocumentCollectionObjectIds.KeyColumn,
                Kind = MigrationObjectKind.Column,
                ParentObjectId =
                    JsonDocumentCollectionObjectIds.Collection,
                SourceName =
                    MigrationDocumentCollectionContract
                        .KeyColumnName,
                NativeType = "JSON_COLLECTION_KEY",
                Facets =
                [
                    Facet(
                        MigrationDocumentCollectionContract
                            .LogicalTypeFacet,
                        MigrationDocumentCollectionContract
                            .TextLogicalType),
                    Facet(
                        MigrationDocumentCollectionContract
                            .NullableFacet,
                        "false"),
                    Facet(
                        MigrationDocumentCollectionContract
                            .FieldRoleFacet,
                        MigrationDocumentCollectionContract
                            .KeyRole),
                    Facet(
                        MigrationDocumentCollectionContract
                            .KeyContractFacet,
                        MigrationDocumentCollectionContract
                            .KeyContract),
                ],
            },
            new()
            {
                ObjectId =
                    JsonDocumentCollectionObjectIds
                        .DocumentColumn,
                Kind = MigrationObjectKind.Column,
                ParentObjectId =
                    JsonDocumentCollectionObjectIds.Collection,
                SourceName =
                    MigrationDocumentCollectionContract
                        .DocumentColumnName,
                NativeType = "JSON_ORDERED_DOCUMENT",
                Facets =
                [
                    Facet(
                        MigrationDocumentCollectionContract
                            .LogicalTypeFacet,
                        MigrationDocumentCollectionContract
                            .JsonLogicalType),
                    Facet(
                        MigrationDocumentCollectionContract
                            .NullableFacet,
                        "false"),
                    Facet(
                        MigrationDocumentCollectionContract
                            .FieldRoleFacet,
                        MigrationDocumentCollectionContract
                            .DocumentRole),
                    Facet(
                        MigrationDocumentCollectionContract
                            .DocumentEncodingFacet,
                        MigrationDocumentCollectionContract
                            .DocumentEncoding),
                ],
            },
        ];

        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = targetCSharpDbVersion,
            Source = result.Source,
            Objects = objects,
            Diagnostics = result.Diagnostics,
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private static MigrationCatalogFacet Facet(
        string name,
        string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static string Framing(JsonInputFraming framing) =>
        framing switch
        {
            JsonInputFraming.RootArray => "root-array",
            JsonInputFraming.MultipleValues =>
                "multiple-values",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private static string Invariant(long value) =>
        value.ToString(CultureInfo.InvariantCulture);
}
