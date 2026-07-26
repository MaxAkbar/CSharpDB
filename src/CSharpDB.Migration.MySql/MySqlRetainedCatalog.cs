using System.Collections.ObjectModel;
using System.Globalization;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.MySql;

internal static class MySqlRetainedCatalog
{
    internal const string AnalyzerCatalogContractFacet =
        "mysqlAnalyzerCatalogContract";
    internal const string DataContractFacet =
        "mysqlDataContract";
    internal const string ContentDigestFacet =
        "mysqlRetainedContentDigest";
    internal const string SnapshotIdentityFacet =
        "mysqlRetainedSnapshotIdentity";
    internal const string MetadataScopeFacet =
        "mysqlRetainedMetadataScope";
    internal const string DirectSchemaSelectProvenFacet =
        "mysqlRetainedDirectSchemaSelectProven";
    internal const string MetadataScope =
        "ordinary-base-tables";
    internal const string RowCountFacet =
        "mysqlRetainedRowCount";
    internal const string SectionDigestFacet =
        "mysqlRetainedSectionDigest";
    internal const string RowOrderContractFacet =
        "mysqlRowOrderContract";
    internal const string RowOrderKindFacet =
        "mysqlRowOrderKind";
    internal const string RowOrderObjectIdFacet =
        "mysqlRowOrderObjectId";
    internal const string ScalarCodecContractFacet =
        "mysqlScalarCodecContract";
    internal const string ScalarCodecFacet =
        "mysqlScalarCodec";
    internal const string ColumnDataAvailableFacet =
        "mysqlColumnDataAvailable";
    internal const string ColumnDataUnavailableReasonFacet =
        "mysqlColumnDataUnavailableReason";

    internal const string InventoryPartialRule =
        "MIG-MYSQL-INVENTORY-PARTIAL-001";
    internal const string MetadataCompletenessRule =
        "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001";
    internal const string LiveQualificationPendingRule =
        "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001";
    internal const string RetainedScopeRule =
        "MIG-MYSQL-RETAINED-SCOPE-001";
    internal const string RetainedQualificationRule =
        "MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001";

    internal static RetainedMigrationCatalogBinding Create(
        MySqlRetainedSourceBinding binding,
        RetainedMigrationContentSummary summary)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(summary);
        ValidateSummary(binding, summary);
        if (!MySqlRetainedBinding.BooleanFacet(
                binding.Database,
                "mysqlMetadataVisibilityProofAttempted") ||
            !MySqlRetainedBinding.BooleanFacet(
                binding.Database,
                "mysqlMetadataVisibilityAccountFormatSupported") ||
            !MySqlRetainedBinding.BooleanFacet(
                binding.Database,
                "mysqlMetadataVisibilityGranteeMatched") ||
            !MySqlRetainedBinding.BooleanFacet(
                binding.Database,
                "mysqlDirectSchemaSelect"))
        {
            throw new MySqlMigrationException(
                "The retained MySQL metadata scope is not proven.");
        }

        string snapshotIdentity = string.Concat(
            MySqlRetainedDataContract.SnapshotIdentityPrefix,
            summary.ContentDigest);
        IReadOnlyDictionary<string, MySqlRetainedTableBinding>
            tables = binding.Tables.ToDictionary(
                static table =>
                    table.CatalogObject.ObjectId,
                StringComparer.Ordinal);
        IReadOnlyDictionary<string, MySqlRetainedColumnBinding>
            columns = binding.Tables
                .SelectMany(static table => table.Columns)
                .ToDictionary(
                    static column =>
                        column.CatalogObject.ObjectId,
                    StringComparer.Ordinal);
        IReadOnlyDictionary<string, RetainedMigrationContentTableSummary>
            summaries = summary.Tables.ToDictionary(
                static table =>
                    table.Descriptor.SourceObjectId,
                StringComparer.Ordinal);

        MigrationCatalogObject[] objects =
            binding.AnalyzerCatalog.Objects
                .Select(item =>
                    TransformObject(
                        item,
                        binding.Database.ObjectId,
                        tables,
                        columns,
                        summaries,
                        summary.ContentDigest,
                        snapshotIdentity))
                .OrderBy(
                    static item => item.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
        MigrationDiagnostic[] diagnostics =
            binding.AnalyzerCatalog.Diagnostics
                .Where(static item =>
                    !string.Equals(
                        item.RuleId,
                        InventoryPartialRule,
                        StringComparison.Ordinal) &&
                    !string.Equals(
                        item.RuleId,
                        MetadataCompletenessRule,
                        StringComparison.Ordinal) &&
                    !string.Equals(
                        item.RuleId,
                        LiveQualificationPendingRule,
                        StringComparison.Ordinal))
                .Append(
                    CreateScopeDiagnostic(
                        binding.Database.ObjectId))
                .Append(
                    CreateQualificationDiagnostic(
                        binding.Database.ObjectId))
                .OrderBy(
                    static item =>
                        item.DiagnosticId,
                    StringComparer.Ordinal)
                .ToArray();

        MigrationCatalog catalog =
            binding.AnalyzerCatalog with
            {
                Source = binding.AnalyzerCatalog.Source with
                {
                    Fingerprint = summary.ContentDigest,
                    Consistency =
                        new MigrationConsistencyStrategy
                        {
                            Kind =
                                MigrationConsistencyKind
                                    .Snapshot,
                            Description =
                                "Catalog facts and retained rows were read on one non-pooled MySQL connection inside one read-only consistent-snapshot transaction.",
                        },
                },
                Objects = Array.AsReadOnly(objects),
                Diagnostics = Array.AsReadOnly(diagnostics),
            };
        MigrationContractValidator.ValidateCatalog(catalog);
        return new RetainedMigrationCatalogBinding
        {
            Catalog = catalog,
            SnapshotIdentity = snapshotIdentity,
        };
    }

    private static MigrationCatalogObject TransformObject(
        MigrationCatalogObject item,
        string databaseObjectId,
        IReadOnlyDictionary<string, MySqlRetainedTableBinding> tables,
        IReadOnlyDictionary<string, MySqlRetainedColumnBinding> columns,
        IReadOnlyDictionary<string, RetainedMigrationContentTableSummary>
            summaries,
        string contentDigest,
        string snapshotIdentity)
    {
        var additions =
            new List<MigrationCatalogFacet>();
        if (string.Equals(
                item.ObjectId,
                databaseObjectId,
                StringComparison.Ordinal))
        {
            additions.Add(
                Facet(
                    "mysqlCatalogContract",
                    MySqlRetainedDataContract
                        .CatalogContract));
            additions.Add(
                Facet(
                    AnalyzerCatalogContractFacet,
                    MySqlCatalogBuilder.CatalogContract));
            additions.Add(
                Facet(
                    DataContractFacet,
                    MySqlRetainedDataContract.DataContract));
            additions.Add(
                Facet(
                    ContentDigestFacet,
                    contentDigest));
            additions.Add(
                Facet(
                    SnapshotIdentityFacet,
                    snapshotIdentity));
            additions.Add(
                Facet(
                    MetadataScopeFacet,
                    MetadataScope));
            additions.Add(
                Facet(
                    DirectSchemaSelectProvenFacet,
                    "true"));
        }

        if (tables.TryGetValue(
                item.ObjectId,
                out MySqlRetainedTableBinding? table))
        {
            additions.Add(
                Facet(
                    MySqlRetainedDataContract
                        .DataAvailableFacet,
                    Boolean(table.IsAvailable)));
            if (!table.IsAvailable)
            {
                additions.Add(
                    Facet(
                        MySqlRetainedDataContract
                            .DataUnavailableReasonFacet,
                        table.AvailabilityReason));
            }
            else
            {
                MySqlRetainedOrderBinding order =
                    table.Order ??
                    throw new InvalidOperationException(
                        "A data-available MySQL table is missing its ordering binding.");
                RetainedMigrationContentTableSummary
                    tableSummary = summaries[item.ObjectId];
                additions.Add(
                    Facet(
                        RowOrderContractFacet,
                        MySqlRetainedDataContract
                            .RowOrderContract));
                additions.Add(
                    Facet(
                        RowOrderKindFacet,
                        order.Kind));
                additions.Add(
                    Facet(
                        RowOrderObjectIdFacet,
                        order.CatalogObject.ObjectId));
                additions.Add(
                    Facet(
                        RowCountFacet,
                        tableSummary.RowCount.ToString(
                            CultureInfo.InvariantCulture)));
                additions.Add(
                    Facet(
                        SectionDigestFacet,
                        tableSummary.SectionDigest));
            }
        }

        if (columns.TryGetValue(
                item.ObjectId,
                out MySqlRetainedColumnBinding? column))
        {
            additions.Add(
                Facet(
                    ColumnDataAvailableFacet,
                    Boolean(column.IsSupported)));
            if (!column.IsSupported)
            {
                additions.Add(
                    Facet(
                        ColumnDataUnavailableReasonFacet,
                        column.AvailabilityReason));
            }
            else
            {
                additions.Add(
                    Facet(
                        ScalarCodecContractFacet,
                        MySqlRetainedDataContract
                            .ScalarCodecContract));
                additions.Add(
                    Facet(
                        ScalarCodecFacet,
                        CodecToken(
                            column.Codec ??
                            throw new InvalidOperationException(
                                "A supported MySQL column is missing its scalar codec."))));
            }
        }

        return additions.Count == 0
            ? item
            : item with
            {
                Facets = MergeFacets(
                    item.Facets,
                    additions),
            };
    }

    private static void ValidateSummary(
        MySqlRetainedSourceBinding binding,
        RetainedMigrationContentSummary summary)
    {
        if (!string.Equals(
                summary.DigestAlgorithm,
                RetainedMigrationPackageContract
                    .ContentDigestAlgorithm,
                StringComparison.Ordinal) ||
            !IsSha256(summary.ContentDigest))
        {
            throw new ArgumentException(
                "The retained MySQL content summary is invalid.",
                nameof(summary));
        }

        MySqlRetainedTableBinding[] expected =
            binding.AvailableTables
                .OrderBy(
                    static table =>
                        table.CatalogObject.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
        RetainedMigrationContentTableSummary[] actual =
            summary.Tables
                .OrderBy(
                    static table =>
                        table.Descriptor.SourceObjectId,
                    StringComparer.Ordinal)
                .ToArray();
        if (expected.Length != actual.Length)
        {
            throw new ArgumentException(
                "The retained MySQL content summary has an unexpected table set.",
                nameof(summary));
        }

        for (int index = 0;
             index < expected.Length;
             index++)
        {
            MySqlRetainedTableBinding table =
                expected[index];
            RetainedMigrationContentTableSummary tableSummary =
                actual[index];
            string[] columnIds = table.Columns
                .Select(static column =>
                    column.CatalogObject.ObjectId)
                .ToArray();
            string[] orderingIds =
                (table.Order ??
                 throw new InvalidOperationException(
                     "A data-available MySQL table is missing its ordering binding."))
                .Columns
                .Select(static column =>
                    column.CatalogObject.ObjectId)
                .ToArray();
            if (!string.Equals(
                    table.CatalogObject.ObjectId,
                    tableSummary.Descriptor
                        .SourceObjectId,
                    StringComparison.Ordinal) ||
                !columnIds.SequenceEqual(
                    tableSummary.Descriptor
                        .ColumnObjectIds,
                    StringComparer.Ordinal) ||
                !orderingIds.SequenceEqual(
                    tableSummary.Descriptor
                        .OrderingKeyColumnObjectIds,
                    StringComparer.Ordinal) ||
                tableSummary.RowCount < 0 ||
                !IsSha256(tableSummary.SectionDigest))
            {
                throw new ArgumentException(
                    "The retained MySQL content summary does not match its capture binding.",
                    nameof(summary));
            }
        }
    }

    private static IReadOnlyList<MigrationCatalogFacet>
        MergeFacets(
        IReadOnlyList<MigrationCatalogFacet> existing,
        IReadOnlyList<MigrationCatalogFacet> additions)
    {
        var byName =
            new Dictionary<string, MigrationCatalogFacet>(
                StringComparer.Ordinal);
        foreach (MigrationCatalogFacet facet in existing)
            byName.Add(facet.Name, facet);
        foreach (MigrationCatalogFacet facet in additions)
            byName[facet.Name] = facet;
        return new ReadOnlyCollection<MigrationCatalogFacet>(
            byName.Values
                .OrderBy(
                    static facet => facet.Name,
                    StringComparer.Ordinal)
                .ToArray());
    }

    private static MigrationDiagnostic
        CreateScopeDiagnostic(
        string databaseObjectId) => new()
        {
            DiagnosticId = DiagnosticId(
                RetainedScopeRule,
                databaseObjectId),
            RuleId = RetainedScopeRule,
            Severity = MigrationDiagnosticSeverity.Warning,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            Summary =
                "Retained MySQL v1 is scoped to ordinary base tables and rows.",
            Explanation =
                "Only ordinary nonpartitioned InnoDB base tables with supported scalars and a complete nonnullable integer key are retained. Views, triggers, routines, events, and their bodies are outside retained v1 and may be incomplete or omitted.",
            ObjectId = databaseObjectId,
            Remediation =
                "Review and migrate excluded programmable or non-table objects through a separately qualified path.",
            CanOverride = false,
        };

    private static MigrationDiagnostic
        CreateQualificationDiagnostic(
        string databaseObjectId) => new()
        {
            DiagnosticId = DiagnosticId(
                RetainedQualificationRule,
                databaseObjectId),
            RuleId = RetainedQualificationRule,
            Severity = MigrationDiagnosticSeverity.Warning,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            Summary =
                "The retained MySQL package has not completed live qualification.",
            Explanation =
                "The package is content-addressed and captured inside one read-only MySQL consistent-snapshot transaction, but exact serviced-server, authentication, least-privilege, and live differential qualification remain deferred.",
            ObjectId = databaseObjectId,
            Remediation =
                "Run the retained MySQL package through the applicable live qualification matrix before treating this adapter as shipping-qualified.",
            CanOverride = false,
        };

    private static string DiagnosticId(
        string rule,
        string objectId) =>
        string.Concat(
            "mysql:diag:",
            rule.ToLowerInvariant(),
            ":",
            MySqlStableDigest.Text(
                "csharpdb-mysql-diagnostic/v1",
                rule,
                objectId,
                null)[..16]);

    private static string CodecToken(
        MySqlScalarCodecKind codec) =>
        codec switch
        {
            MySqlScalarCodecKind.SignedInteger =>
                "signed-integer",
            MySqlScalarCodecKind.UnsignedInteger =>
                "unsigned-integer",
            MySqlScalarCodecKind.Decimal =>
                "decimal",
            MySqlScalarCodecKind.Binary32 =>
                "binary32",
            MySqlScalarCodecKind.Binary64 =>
                "binary64",
            MySqlScalarCodecKind.Text =>
                "text",
            MySqlScalarCodecKind.Binary =>
                "binary",
            MySqlScalarCodecKind.Date =>
                "date",
            MySqlScalarCodecKind.DateTime =>
                "datetime",
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static MigrationCatalogFacet Facet(
        string name,
        string? value) => new()
        {
            Name = name,
            Value = value,
        };

    private static string Boolean(bool value) =>
        value ? "true" : "false";

    private static bool IsSha256(string value) =>
        value.Length == 71 &&
        value.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        value.AsSpan(7).IndexOfAnyExcept(
            "0123456789abcdef".AsSpan()) < 0;
}
