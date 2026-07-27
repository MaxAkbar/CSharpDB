using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

internal static class JsonTypedMigrationCatalogBuilder
{
    internal static MigrationCatalog Build(
        JsonTypedTableSchemaInferenceResult result,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(result);
        MigrationCatalog representationCatalog =
            result.RepresentationSchema.CreateCatalog(
                targetCSharpDbVersion);

        MigrationCatalogObject[] objects =
            representationCatalog.Objects
                .Select(
                    item => TransformObject(
                        item,
                        result))
                .ToArray();
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion =
                representationCatalog.TargetCSharpDbVersion,
            Source = representationCatalog.Source,
            Objects = Array.AsReadOnly(objects),
            Diagnostics = result.Diagnostics,
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private static MigrationCatalogObject TransformObject(
        MigrationCatalogObject item,
        JsonTypedTableSchemaInferenceResult result)
    {
        if (string.Equals(
                item.ObjectId,
                JsonMigrationObjectIds.Table,
                StringComparison.Ordinal))
        {
            return item with
            {
                Facets = TransformTableFacets(
                    item.Facets,
                    result),
            };
        }

        if (!JsonMigrationObjectIds.TryParseColumn(
                item.ObjectId,
                out int columnIndex))
        {
            return item;
        }

        JsonTypedTableColumnSchema column =
            result.Columns[columnIndex];
        IReadOnlyList<MigrationCatalogFacet> facets =
            ReplaceFacet(
                item.Facets,
                "jsonSchemaAlgorithm",
                JsonTypedTableSchemaInferenceResult
                    .AlgorithmId);
        facets = ReplaceFacet(
            facets,
            "jsonScalarPolicy",
            JsonTypedTableSchemaInferenceResult
                .ScalarPolicyId);

        JsonTypedColumnIntent? intent = column.Intent;
        if (intent is null)
        {
            return item with
            {
                Facets = facets,
            };
        }

        facets = ReplaceFacet(
            facets,
            "logicalType",
            LogicalType(intent.Codec));
        facets = ReplaceFacet(
            facets,
            "jsonTableLogicalType",
            LogicalType(intent.Codec));
        facets = ReplaceFacet(
            facets,
            "profileRequiresFullStreamValidation",
            "true");
        facets = RemoveFacets(
            facets,
            "precision",
            "scale");

        var typedFacets =
            new List<MigrationCatalogFacet>(facets)
            {
                Facet(
                    "jsonTypedIntentManifestDigest",
                    result.IntentManifest.ManifestDigest),
                Facet(
                    "jsonTypedCodec",
                    Codec(intent.Codec)),
                Facet(
                    "jsonTypedJsonKind",
                    intent.Codec ==
                        JsonTypedValueCodec.DecimalNumber
                        ? "number"
                        : "string"),
                Facet(
                    "jsonTypedValueContract",
                    JsonTypedIntentManifestSerializer
                        .TypedValueContract),
                Facet(
                    "jsonTypedValidation",
                    "full-stream"),
            };
        if (UsesTextCodec(intent.Codec))
        {
            typedFacets.Add(
                Facet(
                    "jsonTextCodecContract",
                    JsonTypedIntentManifestSerializer
                        .TextCodecContract));
        }
        if (intent.Precision is int precision)
        {
            typedFacets.Add(
                Facet(
                    "precision",
                    Invariant(precision)));
        }
        if (intent.Scale is int scale)
        {
            typedFacets.Add(
                Facet(
                    "scale",
                    Invariant(scale)));
        }

        return item with
        {
            NativeType = NativeType(intent.Codec),
            Facets = typedFacets.AsReadOnly(),
        };
    }

    private static IReadOnlyList<MigrationCatalogFacet>
        TransformTableFacets(
            IReadOnlyList<MigrationCatalogFacet> source,
            JsonTypedTableSchemaInferenceResult result)
    {
        IReadOnlyList<MigrationCatalogFacet> facets =
            ReplaceFacet(
                source,
                "jsonSchemaAlgorithm",
                JsonTypedTableSchemaInferenceResult
                    .AlgorithmId);
        facets = ReplaceFacet(
            facets,
            "jsonScalarPolicy",
            JsonTypedTableSchemaInferenceResult
                .ScalarPolicyId);
        var transformed =
            new List<MigrationCatalogFacet>(facets)
            {
                Facet(
                    "jsonTypedIntentFormat",
                    JsonTypedIntentSidecar.Format),
                Facet(
                    "jsonTypedIntentManifestDigest",
                    result.IntentManifest.ManifestDigest),
                Facet(
                    "jsonTypedValueContract",
                    JsonTypedIntentManifestSerializer
                        .TypedValueContract),
                Facet(
                    "jsonTextCodecContract",
                    JsonTypedIntentManifestSerializer
                        .TextCodecContract),
                Facet(
                    "jsonMaxDecodedBinaryBytes",
                    Invariant(
                        result.IntentManifest
                            .MaxDecodedBinaryBytes)),
                Facet(
                    "jsonMaxDecimalDigits",
                    Invariant(
                        result.IntentManifest
                            .MaxDecimalDigits)),
            };
        return transformed.AsReadOnly();
    }

    private static IReadOnlyList<MigrationCatalogFacet>
        ReplaceFacet(
            IReadOnlyList<MigrationCatalogFacet> source,
            string name,
            string value)
    {
        var result =
            new MigrationCatalogFacet[source.Count];
        bool found = false;
        for (int index = 0;
             index < source.Count;
             index++)
        {
            MigrationCatalogFacet facet = source[index];
            if (string.Equals(
                    facet.Name,
                    name,
                    StringComparison.Ordinal))
            {
                result[index] = Facet(name, value);
                found = true;
            }
            else
            {
                result[index] = Facet(
                    facet.Name,
                    facet.Value);
            }
        }

        if (!found)
        {
            throw new InvalidDataException(
                "The JSON representation catalog is missing a required facet.");
        }

        return Array.AsReadOnly(result);
    }

    private static IReadOnlyList<MigrationCatalogFacet>
        RemoveFacets(
            IReadOnlyList<MigrationCatalogFacet> source,
            params string[] names)
    {
        HashSet<string> removed =
            names.ToHashSet(StringComparer.Ordinal);
        return Array.AsReadOnly(
            source
                .Where(
                    facet =>
                        !removed.Contains(facet.Name))
                .Select(
                    facet => Facet(
                        facet.Name,
                        facet.Value))
                .ToArray());
    }

    private static string LogicalType(
        JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.BinaryBase64 =>
                "binary",
            JsonTypedValueCodec.DecimalString or
            JsonTypedValueCodec.DecimalNumber =>
                "decimal",
            JsonTypedValueCodec.GuidD =>
                "guid",
            JsonTypedValueCodec.DateCSharpDbText =>
                "date",
            JsonTypedValueCodec.TimeCSharpDbText =>
                "time",
            JsonTypedValueCodec.DateTimeCSharpDbText =>
                "dateTime",
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText =>
                "dateTimeOffset",
            JsonTypedValueCodec.Int64String =>
                "signedInteger",
            JsonTypedValueCodec.UInt64String =>
                "unsignedInteger",
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static string NativeType(
        JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.BinaryBase64 =>
                "JSON_BASE64_STRING",
            JsonTypedValueCodec.DecimalString =>
                "JSON_DECIMAL_STRING",
            JsonTypedValueCodec.DecimalNumber =>
                "JSON_DECIMAL_NUMBER",
            JsonTypedValueCodec.GuidD =>
                "JSON_GUID_D_STRING",
            JsonTypedValueCodec.DateCSharpDbText =>
                "JSON_DATE_CSHARPDB_TEXT",
            JsonTypedValueCodec.TimeCSharpDbText =>
                "JSON_TIME_CSHARPDB_TEXT",
            JsonTypedValueCodec.DateTimeCSharpDbText =>
                "JSON_DATETIME_CSHARPDB_TEXT",
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText =>
                "JSON_DATETIMEOFFSET_CSHARPDB_TEXT",
            JsonTypedValueCodec.Int64String =>
                "JSON_INT64_STRING",
            JsonTypedValueCodec.UInt64String =>
                "JSON_UINT64_STRING",
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static string Codec(
        JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.BinaryBase64 =>
                "binaryBase64",
            JsonTypedValueCodec.DecimalString =>
                "decimalString",
            JsonTypedValueCodec.DecimalNumber =>
                "decimalNumber",
            JsonTypedValueCodec.GuidD => "guidD",
            JsonTypedValueCodec.DateCSharpDbText =>
                "dateCSharpDbText",
            JsonTypedValueCodec.TimeCSharpDbText =>
                "timeCSharpDbText",
            JsonTypedValueCodec.DateTimeCSharpDbText =>
                "dateTimeCSharpDbText",
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText =>
                "dateTimeOffsetCSharpDbText",
            JsonTypedValueCodec.Int64String =>
                "int64String",
            JsonTypedValueCodec.UInt64String =>
                "uint64String",
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static bool UsesTextCodec(
        JsonTypedValueCodec codec) =>
        codec is
            JsonTypedValueCodec.GuidD or
            JsonTypedValueCodec.DateCSharpDbText or
            JsonTypedValueCodec.TimeCSharpDbText or
            JsonTypedValueCodec.DateTimeCSharpDbText or
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText;

    private static MigrationCatalogFacet Facet(
        string name,
        string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static string Invariant(long value) =>
        value.ToString(CultureInfo.InvariantCulture);
}
