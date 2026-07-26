using System.Collections.ObjectModel;
using System.Globalization;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.SqlServer;

internal static class SqlServerRetainedCatalog
{
    internal const string AnalyzerCatalogContractFacet =
        "sqlServerAnalyzerCatalogContract";
    internal const string DataContractFacet =
        "sqlServerDataContract";
    internal const string ContentDigestFacet =
        "sqlServerRetainedContentDigest";
    internal const string SnapshotIdentityFacet =
        "sqlServerRetainedSnapshotIdentity";
    internal const string RowCountFacet =
        "sqlServerRetainedRowCount";
    internal const string SectionDigestFacet =
        "sqlServerRetainedSectionDigest";
    internal const string RowOrderContractFacet =
        "sqlServerRowOrderContract";
    internal const string RowOrderKindFacet =
        "sqlServerRowOrderKind";
    internal const string RowOrderObjectIdFacet =
        "sqlServerRowOrderObjectId";
    internal const string ScalarCodecContractFacet =
        "sqlServerScalarCodecContract";
    internal const string ScalarCodecFacet =
        "sqlServerScalarCodec";
    internal const string BinaryWidthFacet =
        "binaryWidth";
    internal const string ColumnDataAvailableFacet =
        "sqlServerColumnDataAvailable";
    internal const string ColumnDataUnavailableReasonFacet =
        "sqlServerColumnDataUnavailableReason";

    internal const string InventoryPartialRule =
        "MIG-SQLSERVER-INVENTORY-PARTIAL-001";
    internal const string LiveQualificationPendingRule =
        "MIG-SQLSERVER-LIVE-QUALIFICATION-PENDING-001";
    internal const string RetainedQualificationRule =
        "MIG-SQLSERVER-RETAINED-LIVE-QUALIFICATION-DEFERRED-001";

    internal static RetainedMigrationCatalogBinding Create(
        SqlServerRetainedSourceBinding binding,
        RetainedMigrationContentSummary summary)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(summary);
        ValidateSummary(binding, summary);
        string snapshotIdentity = string.Concat(
            SqlServerRetainedDataContract
                .SnapshotIdentityPrefix,
            summary.ContentDigest);

        IReadOnlyDictionary<string, SqlServerRetainedTableBinding>
            tables = binding.Tables.ToDictionary(
                static table =>
                    table.CatalogObject.ObjectId,
                StringComparer.Ordinal);
        IReadOnlyDictionary<string, SqlServerRetainedColumnBinding>
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
        MigrationDiagnostic qualification =
            CreateQualificationDiagnostic(
                binding.Database.ObjectId);
        MigrationDiagnostic[] diagnostics =
            binding.AnalyzerCatalog.Diagnostics
                .Where(static item =>
                    !string.Equals(
                        item.RuleId,
                        InventoryPartialRule,
                        StringComparison.Ordinal) &&
                    !string.Equals(
                        item.RuleId,
                        LiveQualificationPendingRule,
                        StringComparison.Ordinal))
                .Append(qualification)
                .OrderBy(
                    static item => item.DiagnosticId,
                    StringComparer.Ordinal)
                .ToArray();

        MigrationCatalog catalog =
            binding.AnalyzerCatalog with
            {
                Source = binding.AnalyzerCatalog.Source with
                {
                    Fingerprint = summary.ContentDigest,
                    Consistency = new MigrationConsistencyStrategy
                    {
                        Kind = MigrationConsistencyKind.Snapshot,
                        Description =
                            "Catalog facts and retained rows were read on one non-pooled, read-only SQL Server connection inside one SNAPSHOT transaction.",
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
        IReadOnlyDictionary<string, SqlServerRetainedTableBinding> tables,
        IReadOnlyDictionary<string, SqlServerRetainedColumnBinding> columns,
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
                    "sqlServerCatalogContract",
                    SqlServerRetainedDataContract.CatalogContract));
            additions.Add(
                Facet(
                    AnalyzerCatalogContractFacet,
                    SqlServerCatalogBuilder.CatalogContract));
            additions.Add(
                Facet(
                    DataContractFacet,
                    SqlServerRetainedDataContract.DataContract));
            additions.Add(
                Facet(
                    ContentDigestFacet,
                    contentDigest));
            additions.Add(
                Facet(
                    SnapshotIdentityFacet,
                    snapshotIdentity));
        }

        if (tables.TryGetValue(
                item.ObjectId,
                out SqlServerRetainedTableBinding? table))
        {
            additions.Add(
                Facet(
                    SqlServerRetainedDataContract.DataAvailableFacet,
                    Boolean(table.IsAvailable)));
            if (!table.IsAvailable)
            {
                additions.Add(
                    Facet(
                        SqlServerRetainedDataContract
                            .DataUnavailableReasonFacet,
                        table.AvailabilityReason));
            }
            else
            {
                SqlServerRetainedOrderBinding order =
                    table.Order ??
                    throw new InvalidOperationException(
                        "A data-available SQL Server table is missing its ordering binding.");
                RetainedMigrationContentTableSummary tableSummary =
                    summaries[item.ObjectId];
                additions.Add(
                    Facet(
                        RowOrderContractFacet,
                        SqlServerRetainedDataContract.RowOrderContract));
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
                out SqlServerRetainedColumnBinding? column))
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
                        SqlServerRetainedDataContract
                            .ScalarCodecContract));
                additions.Add(
                    Facet(
                        ScalarCodecFacet,
                        CodecToken(
                            column.Codec ??
                            throw new InvalidOperationException(
                                "A supported SQL Server column is missing its scalar codec."))));
                if (column.BinaryWidth is int binaryWidth)
                {
                    additions.Add(
                        Facet(
                            BinaryWidthFacet,
                            binaryWidth.ToString(
                                CultureInfo.InvariantCulture)));
                }
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
        SqlServerRetainedSourceBinding binding,
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
                "The retained SQL Server content summary is invalid.",
                nameof(summary));
        }

        SqlServerRetainedTableBinding[] expected =
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
                "The retained SQL Server content summary has an unexpected table set.",
                nameof(summary));
        }

        for (int index = 0;
             index < expected.Length;
             index++)
        {
            SqlServerRetainedTableBinding table =
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
                     "A data-available SQL Server table is missing its ordering binding."))
                .Columns
                .Select(static column =>
                    column.CatalogObject.ObjectId)
                .ToArray();
            if (!string.Equals(
                    table.CatalogObject.ObjectId,
                    tableSummary.Descriptor.SourceObjectId,
                    StringComparison.Ordinal) ||
                !columnIds.SequenceEqual(
                    tableSummary.Descriptor.ColumnObjectIds,
                    StringComparer.Ordinal) ||
                !orderingIds.SequenceEqual(
                    tableSummary.Descriptor.OrderingKeyColumnObjectIds,
                    StringComparer.Ordinal) ||
                tableSummary.RowCount < 0 ||
                !IsSha256(tableSummary.SectionDigest))
            {
                throw new ArgumentException(
                    "The retained SQL Server content summary does not match its capture binding.",
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
        CreateQualificationDiagnostic(
        string databaseObjectId) => new()
        {
            DiagnosticId = string.Concat(
                "sqlserver:diag:",
                RetainedQualificationRule.ToLowerInvariant(),
                ":",
                SqlServerStableDigest.Text(
                    "csharpdb-sqlserver-diagnostic/v1",
                    RetainedQualificationRule,
                    databaseObjectId,
                    null)[..16]),
            RuleId = RetainedQualificationRule,
            Severity = MigrationDiagnosticSeverity.Warning,
            Status = MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            Summary =
                "The retained SQL Server package has not completed live qualification.",
            Explanation =
                "The package is content-addressed and was captured inside one SQL Server SNAPSHOT transaction, but published-runtime, platform, authentication, least-privilege, and live differential qualification remain deferred.",
            ObjectId = databaseObjectId,
            Remediation =
                "Run the retained SQL Server package through the applicable live qualification matrix before treating this adapter as shipping-qualified.",
            CanOverride = false,
        };

    private static string CodecToken(
        SqlServerScalarCodecKind codec) =>
        codec switch
        {
            SqlServerScalarCodecKind.SignedInteger =>
                "signed-integer",
            SqlServerScalarCodecKind.Boolean =>
                "boolean",
            SqlServerScalarCodecKind.Decimal =>
                "decimal",
            SqlServerScalarCodecKind.Binary32 =>
                "binary32",
            SqlServerScalarCodecKind.Binary64 =>
                "binary64",
            SqlServerScalarCodecKind.Text =>
                "text",
            SqlServerScalarCodecKind.Binary =>
                "binary",
            SqlServerScalarCodecKind.Guid =>
                "guid",
            SqlServerScalarCodecKind.Date =>
                "date",
            SqlServerScalarCodecKind.Time =>
                "time",
            SqlServerScalarCodecKind.DateTime =>
                "datetime",
            SqlServerScalarCodecKind.DateTimeOffset =>
                "datetime-offset",
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
