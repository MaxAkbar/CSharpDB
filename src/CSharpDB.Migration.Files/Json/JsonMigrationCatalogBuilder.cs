using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Projects one fully discovered JSON object-row schema into the shared
/// provider-neutral migration catalog.
/// </summary>
internal static class JsonMigrationCatalogBuilder
{
    internal static MigrationCatalog Build(
        JsonTableSchemaInferenceResult result,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (string.IsNullOrWhiteSpace(targetCSharpDbVersion))
        {
            throw new ArgumentException(
                "The target CSharpDB version must be nonblank.",
                nameof(targetCSharpDbVersion));
        }

        var tableFacets = new List<MigrationCatalogFacet>
        {
            Facet(
                "jsonSchemaAlgorithm",
                JsonTableSchemaInferenceResult.AlgorithmId),
            Facet(
                "jsonScalarPolicy",
                JsonTableSchemaInferenceResult.ScalarPolicyId),
            Facet(
                "jsonCanonicalValueVersion",
                JsonInputContracts.CanonicalNestedJsonVersion),
            Facet(
                "jsonSourceBindingOptionsDigest",
                result.Binding.OptionsDigest),
            Facet("jsonSnapshotIdentity", result.SnapshotIdentity),
            Facet("jsonContentDigest", result.ContentDigest),
            Facet("jsonContentLength", Invariant(result.ContentLength)),
            Facet(
                "jsonInputFraming",
                Framing(result.Binding.Framing)),
            Facet("jsonTotalRecords", Invariant(result.TotalRecords)),
            Facet(
                "jsonEligibleObjectRecords",
                Invariant(result.EligibleObjectRecords)),
            Facet(
                "jsonIneligibleRecords",
                Invariant(result.IneligibleRecords)),
            Facet(
                "jsonTotalColumnNameBytes",
                Invariant(result.TotalColumnNameBytes)),
            Facet(
                "jsonProfileRecordsExamined",
                Invariant(result.ProfileRecordsExamined)),
            Facet(
                "jsonProfileBytesExamined",
                Invariant(result.ProfileBytesExamined)),
            Facet(
                "jsonProfileRecordLimitReached",
                Boolean(result.ProfileRecordLimitReached)),
            Facet(
                "jsonProfileByteLimitReached",
                Boolean(result.ProfileByteLimitReached)),
        };
        tableFacets.AddRange(CoverageFacets(
            "jsonStructural",
            result.StructuralCoverage));
        tableFacets.AddRange(CoverageFacets(
            "jsonTypeProfile",
            result.TypeProfileCoverage));

        var objects = new List<MigrationCatalogObject>(
            result.Columns.Count + 2)
        {
            new()
            {
                ObjectId = JsonMigrationObjectIds.MainNamespace,
                Kind = MigrationObjectKind.Namespace,
                SourceName = "main",
                Facets = [Facet("isDefault", "true")],
            },
            new()
            {
                ObjectId = JsonMigrationObjectIds.Table,
                Kind = MigrationObjectKind.Table,
                ParentObjectId =
                    JsonMigrationObjectIds.MainNamespace,
                SourceNamespace = "main",
                SourceName = result.TableName,
                Facets = tableFacets,
            },
        };

        objects.AddRange(result.Columns.Select(
            column => CreateColumn(column, result)));
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

    private static MigrationCatalogObject CreateColumn(
        JsonTableColumnSchema column,
        JsonTableSchemaInferenceResult result)
    {
        var facets = new List<MigrationCatalogFacet>
        {
            Facet("logicalType", LogicalType(column.LogicalType)),
            Facet(
                "jsonTableLogicalType",
                JsonLogicalType(column.LogicalType)),
            Facet("nullable", Boolean(column.Nullable)),
            Facet("jsonColumnIndex", Invariant(column.ColumnIndex)),
            Facet(
                "jsonOriginalPropertyName",
                column.OriginalPropertyName),
            Facet(
                "jsonFirstSeenRecordOrdinal",
                Invariant(column.FirstSeenRecordOrdinal)),
            Facet(
                "jsonFirstSeenPropertyOrdinal",
                Invariant(column.FirstSeenPropertyOrdinal)),
            Facet(
                "jsonSchemaResolution",
                column.Resolution.ToString()),
            Facet(
                "jsonInferenceReason",
                column.Reason.ToString()),
            Facet(
                "jsonInferenceConfidence",
                column.Confidence.ToString()),
            Facet(
                "jsonMissingPropertyPolicy",
                column.MissingPolicy.ToString()),
            Facet(
                "jsonOverrideValidation",
                column.OverrideValidation.ToString()),
            Facet(
                "jsonSchemaAlgorithm",
                JsonTableSchemaInferenceResult.AlgorithmId),
            Facet(
                "jsonScalarPolicy",
                JsonTableSchemaInferenceResult.ScalarPolicyId),
            Facet(
                "jsonSourceBindingOptionsDigest",
                result.Binding.OptionsDigest),
            Facet("jsonPresentValues", Invariant(column.PresentCount)),
            Facet("jsonNullValues", Invariant(column.NullCount)),
            Facet("jsonMissingValues", Invariant(column.MissingCount)),
            Facet(
                "jsonProfiledNonNullValues",
                Invariant(column.ProfiledNonNullCount)),
            Facet(
                "jsonProfiledStringValues",
                Invariant(column.ProfiledStringCount)),
            Facet(
                "jsonProfiledBooleanValues",
                Invariant(column.ProfiledBooleanCount)),
            Facet(
                "jsonProfiledNumberValues",
                Invariant(column.ProfiledNumberCount)),
            Facet(
                "jsonProfiledObjectValues",
                Invariant(column.ProfiledObjectCount)),
            Facet(
                "jsonProfiledArrayValues",
                Invariant(column.ProfiledArrayCount)),
            Facet(
                "jsonProfiledLexemePreservationValues",
                Invariant(column.ProfiledLexemePreservationCount)),
            Facet(
                "observedMaxCanonicalValueBytes",
                Invariant(column.ObservedMaxCanonicalValueBytes)),
        };
        facets.AddRange(CoverageFacets(
            "jsonStructural",
            result.StructuralCoverage));
        facets.AddRange(CoverageFacets(
            "jsonTypeProfile",
            result.TypeProfileCoverage));

        if (column.ObservedPrecision is int observedPrecision)
        {
            facets.Add(Facet(
                "observedPrecision",
                Invariant(observedPrecision)));
        }
        if (column.ObservedScale is int observedScale)
        {
            facets.Add(Facet(
                "observedScale",
                Invariant(observedScale)));
        }
        if (column.LogicalType ==
            JsonTableColumnLogicalType.Json)
        {
            facets.Add(Facet(
                "jsonRepresentation",
                "canonical-json-text"));
            facets.Add(Facet(
                "jsonCanonicalValueVersion",
                JsonInputContracts.CanonicalNestedJsonVersion));
            facets.Add(Facet(
                "jsonPropertyOrderPolicy",
                JsonInputContracts.PropertyOrderPolicy));
            facets.Add(Facet(
                "jsonNumberLexemePolicy",
                JsonInputContracts.NumberLexemePolicy));
        }
        if (column.LogicalType ==
                JsonTableColumnLogicalType.Decimal &&
            result.TypeProfileCoverage.Kind ==
                MigrationCoverageKind.Full &&
            column.ObservedPrecision is int precision &&
            column.ObservedScale is int scale)
        {
            facets.Add(Facet("precision", Invariant(precision)));
            facets.Add(Facet("scale", Invariant(scale)));
        }
        facets.AddRange(StandardProfileFacets(
            result.TypeProfileCoverage));

        return new MigrationCatalogObject
        {
            ObjectId =
                JsonMigrationObjectIds.Column(column.ColumnIndex),
            Kind = MigrationObjectKind.Column,
            ParentObjectId = JsonMigrationObjectIds.Table,
            SourceName = column.SourceName,
            NativeType = NativeType(column.LogicalType),
            Facets = facets,
        };
    }

    private static IEnumerable<MigrationCatalogFacet> StandardProfileFacets(
        MigrationProfileCoverage coverage)
    {
        ArgumentNullException.ThrowIfNull(coverage);
        yield return Facet(
            "profileRequiresFullStreamValidation",
            Boolean(coverage.RequiresFullStreamValidation));
        if (coverage.Kind == MigrationCoverageKind.None)
            yield break;

        yield return Facet("profileKind", coverage.Kind.ToString());
        yield return Facet(
            "profileValuesExamined",
            Invariant(coverage.ValuesExamined));
        if (coverage.TotalValues is long total)
        {
            yield return Facet(
                "profileTotalValues",
                Invariant(total));
        }
    }

    private static IEnumerable<MigrationCatalogFacet> CoverageFacets(
        string prefix,
        MigrationProfileCoverage coverage)
    {
        ArgumentNullException.ThrowIfNull(coverage);
        yield return Facet(prefix + "CoverageKind", coverage.Kind.ToString());
        yield return Facet(
            prefix + "ValuesExamined",
            Invariant(coverage.ValuesExamined));
        if (coverage.TotalValues is long total)
        {
            yield return Facet(
                prefix + "TotalValues",
                Invariant(total));
        }
        yield return Facet(
            prefix + "RequiresFullStreamValidation",
            Boolean(coverage.RequiresFullStreamValidation));
    }

    private static string LogicalType(
        JsonTableColumnLogicalType logicalType) =>
        logicalType switch
        {
            JsonTableColumnLogicalType.Text => "text",
            JsonTableColumnLogicalType.Boolean => "boolean",
            JsonTableColumnLogicalType.SignedInteger =>
                "signedInteger",
            JsonTableColumnLogicalType.UnsignedInteger =>
                "unsignedInteger",
            JsonTableColumnLogicalType.Decimal => "decimal",
            JsonTableColumnLogicalType.Json => "text",
            _ => throw new ArgumentOutOfRangeException(
                nameof(logicalType)),
        };

    private static string JsonLogicalType(
        JsonTableColumnLogicalType logicalType) =>
        logicalType switch
        {
            JsonTableColumnLogicalType.Text => "text",
            JsonTableColumnLogicalType.Boolean => "boolean",
            JsonTableColumnLogicalType.SignedInteger =>
                "signedInteger",
            JsonTableColumnLogicalType.UnsignedInteger =>
                "unsignedInteger",
            JsonTableColumnLogicalType.Decimal => "decimal",
            JsonTableColumnLogicalType.Json => "json",
            _ => throw new ArgumentOutOfRangeException(
                nameof(logicalType)),
        };

    private static string NativeType(
        JsonTableColumnLogicalType logicalType) =>
        logicalType switch
        {
            JsonTableColumnLogicalType.Text => "JSON_STRING",
            JsonTableColumnLogicalType.Boolean => "JSON_BOOLEAN",
            JsonTableColumnLogicalType.SignedInteger =>
                "JSON_SIGNED_INTEGER",
            JsonTableColumnLogicalType.UnsignedInteger =>
                "JSON_UNSIGNED_INTEGER",
            JsonTableColumnLogicalType.Decimal => "JSON_DECIMAL",
            JsonTableColumnLogicalType.Json => "JSON_CANONICAL",
            _ => throw new ArgumentOutOfRangeException(
                nameof(logicalType)),
        };

    private static string Framing(JsonInputFraming framing) =>
        framing switch
        {
            JsonInputFraming.RootArray => "root-array",
            JsonInputFraming.MultipleValues => "multiple-values",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private static MigrationCatalogFacet Facet(
        string name,
        string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static string Boolean(bool value) =>
        value ? "true" : "false";

    private static string Invariant(long value) =>
        value.ToString(CultureInfo.InvariantCulture);
}
