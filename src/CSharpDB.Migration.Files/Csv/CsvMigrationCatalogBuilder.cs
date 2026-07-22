using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

internal static class CsvMigrationCatalogBuilder
{
    public static MigrationCatalog Build(
        CsvSchemaInferenceResult result,
        string targetCSharpDbVersion)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (string.IsNullOrWhiteSpace(targetCSharpDbVersion))
        {
            throw new ArgumentException(
                "The target CSharpDB version must be nonblank.",
                nameof(targetCSharpDbVersion));
        }

        var objects = new List<MigrationCatalogObject>(result.Columns.Count + 2)
        {
            new()
            {
                ObjectId = CsvMigrationObjectIds.MainNamespace,
                Kind = MigrationObjectKind.Namespace,
                SourceName = "main",
                Facets = [Facet("isDefault", "true")],
            },
            new()
            {
                ObjectId = CsvMigrationObjectIds.Table,
                Kind = MigrationObjectKind.Table,
                ParentObjectId = CsvMigrationObjectIds.MainNamespace,
                SourceNamespace = "main",
                SourceName = result.TableName,
                Facets =
                [
                    Facet("csvSchemaAlgorithm", CsvSchemaInferenceResult.AlgorithmId),
                    Facet("csvScalarPolicy", CsvSchemaInferenceResult.ScalarPolicyId),
                    Facet("csvSnapshotIdentity", result.SnapshotIdentity),
                    Facet("csvProfileCharactersExamined", Invariant(result.ProfileCharactersExamined)),
                    Facet("csvProfileCharacterLimitReached", Boolean(result.ProfileCharacterLimitReached)),
                ],
            },
        };

        objects.AddRange(result.Columns.Select(CreateColumn));
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

    private static MigrationCatalogObject CreateColumn(CsvColumnSchema column)
    {
        var facets = new List<MigrationCatalogFacet>
        {
            Facet("logicalType", LogicalType(column.LogicalType)),
            Facet("nullable", Boolean(column.Nullable)),
            Facet("csvColumnIndex", Invariant(column.ColumnIndex)),
            Facet("csvHeaderPresent", Boolean(column.OriginalHeader is not null)),
            Facet("csvSchemaResolution", column.Resolution.ToString()),
            Facet("csvInferenceReason", column.Reason.ToString()),
            Facet("csvInferenceConfidence", column.Confidence.ToString()),
            Facet("csvSchemaAlgorithm", CsvSchemaInferenceResult.AlgorithmId),
            Facet("csvScalarPolicy", CsvSchemaInferenceResult.ScalarPolicyId),
            Facet("csvOverrideValidation", column.OverrideValidation.ToString()),
            Facet("profileRequiresFullStreamValidation", Boolean(column.Coverage.RequiresFullStreamValidation)),
        };
        if (column.OriginalHeader is not null)
            facets.Add(Facet("csvOriginalHeader", column.OriginalHeader));
        if (column.SuggestedLogicalType is CsvColumnLogicalType suggestion)
            facets.Add(Facet("csvSuggestedLogicalType", LogicalType(suggestion)));

        if (column.Coverage.Kind != MigrationCoverageKind.None)
        {
            facets.Add(Facet("profileKind", column.Coverage.Kind.ToString()));
            facets.Add(Facet("profileValuesExamined", Invariant(column.Coverage.ValuesExamined)));
            if (column.Coverage.TotalValues is long total)
                facets.Add(Facet("profileTotalValues", Invariant(total)));
            facets.Add(Facet("profileSubstantiveValues", Invariant(column.SubstantiveValueCount)));
            facets.Add(Facet("profileNullValues", Invariant(column.NullCount)));
            facets.Add(Facet("profileEmptyValues", Invariant(column.EmptyCount)));
            facets.Add(Facet("profileMissingValues", Invariant(column.MissingCount)));
            facets.Add(Facet("profileQuotedValues", Invariant(column.QuotedCount)));
            facets.Add(Facet("profileNonCanonicalNumericValues", Invariant(column.NonCanonicalNumericCount)));
            facets.Add(Facet("observedMaxLength", Invariant(column.ObservedMaxLength)));
            if (column.ObservedPrecision is int observedPrecision)
                facets.Add(Facet("observedPrecision", Invariant(observedPrecision)));
            if (column.ObservedScale is int observedScale)
                facets.Add(Facet("observedScale", Invariant(observedScale)));
        }

        if (column.Coverage.Kind == MigrationCoverageKind.Full)
        {
            if (column.LogicalType == CsvColumnLogicalType.Text && column.ObservedMaxLength > 0)
                facets.Add(Facet("maxLength", Invariant(column.ObservedMaxLength)));
            if (column.LogicalType == CsvColumnLogicalType.Decimal &&
                column.ObservedPrecision is int precision &&
                column.ObservedScale is int scale)
            {
                facets.Add(Facet("precision", Invariant(precision)));
                facets.Add(Facet("scale", Invariant(scale)));
            }
        }

        return new MigrationCatalogObject
        {
            ObjectId = CsvMigrationObjectIds.Column(column.ColumnIndex),
            Kind = MigrationObjectKind.Column,
            ParentObjectId = CsvMigrationObjectIds.Table,
            SourceName = column.SourceName,
            NativeType = NativeType(column.LogicalType),
            Facets = facets,
        };
    }

    private static string LogicalType(CsvColumnLogicalType logicalType) => logicalType switch
    {
        CsvColumnLogicalType.Text => "text",
        CsvColumnLogicalType.Boolean => "boolean",
        CsvColumnLogicalType.SignedInteger => "signedInteger",
        CsvColumnLogicalType.UnsignedInteger => "unsignedInteger",
        CsvColumnLogicalType.Decimal => "decimal",
        CsvColumnLogicalType.FloatingPoint => "floatingPoint",
        CsvColumnLogicalType.Guid => "guid",
        CsvColumnLogicalType.Date => "date",
        CsvColumnLogicalType.Time => "time",
        CsvColumnLogicalType.DateTime => "dateTime",
        CsvColumnLogicalType.DateTimeOffset => "dateTimeOffset",
        _ => throw new ArgumentOutOfRangeException(nameof(logicalType)),
    };

    private static string NativeType(CsvColumnLogicalType logicalType) => logicalType switch
    {
        CsvColumnLogicalType.SignedInteger => "CSV_SIGNED_INTEGER",
        CsvColumnLogicalType.UnsignedInteger => "CSV_UNSIGNED_INTEGER",
        CsvColumnLogicalType.FloatingPoint => "CSV_FLOATING_POINT",
        CsvColumnLogicalType.DateTime => "CSV_DATETIME",
        CsvColumnLogicalType.DateTimeOffset => "CSV_DATETIME_OFFSET",
        _ => "CSV_" + logicalType.ToString().ToUpperInvariant(),
    };

    private static MigrationCatalogFacet Facet(string name, string? value) => new()
    {
        Name = name,
        Value = value,
    };

    private static string Boolean(bool value) => value ? "true" : "false";

    private static string Invariant(long value) => value.ToString(CultureInfo.InvariantCulture);
}
