using System.Collections.ObjectModel;
using System.Data.OleDb;
using System.Globalization;
using System.Runtime.Versioning;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Access;

/// <summary>
/// Inventories a bounded, unencrypted .mdb or .accdb through the process-local
/// ACE OLE DB provider while holding a write/delete-denying source lease.
/// </summary>
public sealed class AccessMigrationSourceInspector
    : IMigrationSourceInspector
{
    public const string CatalogContract =
        "csharpdb-access-catalog/v1";

    private readonly string sourceFilePath;
    private readonly AccessSourceOptions options;

    public AccessMigrationSourceInspector(
        string sourceFilePath,
        AccessSourceOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            sourceFilePath);
        this.sourceFilePath = sourceFilePath;
        this.options =
            options ?? new AccessSourceOptions();
        this.options.Validate();
    }

    public MigrationSourceKind SourceKind =>
        MigrationSourceKind.Access;

    public async ValueTask<MigrationCatalog> InspectAsync(
        MigrationInspectionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequest(request);
        if (!OperatingSystem.IsWindows())
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.UnsupportedPlatform,
                "Microsoft Access inspection requires Windows.");
        }

        await using AccessSourceSession session =
            await AccessSourceSession.OpenAsync(
                    sourceFilePath,
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
        AccessCatalogSnapshot snapshot =
            await AccessCatalogReader.ReadAsync(
                    session,
                    AccessInspectionLimits.Default,
                    cancellationToken)
                .ConfigureAwait(false);
        return AccessCatalogBuilder.Build(
                snapshot,
                request)
            .Catalog;
    }

    internal static void ValidateRequest(
        MigrationInspectionRequest request)
    {
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The Microsoft Access adapter targets CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "The profile sample size must be positive.");
        }
    }
}

[SupportedOSPlatform("windows")]
internal static class AccessCatalogBuilder
{
    internal const string LiveQualificationRule =
        "MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001";

    internal static AccessCatalogBinding Build(
        AccessCatalogSnapshot snapshot,
        MigrationInspectionRequest request)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(request);
        AccessMigrationSourceInspector.ValidateRequest(
            request);

        var objects =
            new List<MigrationCatalogObject>();
        var diagnostics =
            new List<MigrationDiagnostic>();
        string databaseId =
            AccessObjectIds.Database(
                snapshot.SourceContentDigest);
        string namespaceId =
            AccessObjectIds.Namespace(
                snapshot.SourceContentDigest);
        var database =
            new MigrationCatalogObject
            {
                ObjectId = databaseId,
                Kind = MigrationObjectKind.Database,
                SourceName = snapshot.SourceName,
                Facets =
                [
                    Facet(
                        "accessCatalogContract",
                        AccessMigrationSourceInspector
                            .CatalogContract),
                    Facet(
                        "accessProviderId",
                        snapshot.ProviderId),
                    Facet(
                        "accessSourceExtension",
                        snapshot.SourceExtension),
                    Facet(
                        "accessSourceContentDigest",
                        snapshot.SourceContentDigest),
                    Facet(
                        "accessSourceLease",
                        "share-deny-write-delete"),
                    Facet(
                        "accessEncryptedSource",
                        "false"),
                ],
            };
        objects.Add(database);
        objects.Add(
            new MigrationCatalogObject
            {
                ObjectId = namespaceId,
                Kind = MigrationObjectKind.Namespace,
                ParentObjectId = databaseId,
                SourceNamespace = "main",
                SourceName = "main",
                Facets =
                [
                    Facet("isDefault", "true"),
                    Facet(
                        "accessCatalogContract",
                        AccessMigrationSourceInspector
                            .CatalogContract),
                ],
            });

        var tableBindings =
            new List<AccessTableBinding>(
                snapshot.Tables.Count);
        foreach (AccessTableMetadata table in
                 snapshot.Tables.OrderBy(
                     static item => item.Name,
                     StringComparer.Ordinal))
        {
            string tableId =
                AccessObjectIds.Table(
                    table.Name);
            var tableObject =
                new MigrationCatalogObject
                {
                    ObjectId = tableId,
                    Kind = MigrationObjectKind.Table,
                    ParentObjectId = namespaceId,
                    SourceNamespace = "main",
                    SourceName = table.Name,
                    Facets =
                    [
                        Facet(
                            "accessCatalogContract",
                            AccessMigrationSourceInspector
                                .CatalogContract),
                        Facet(
                            "accessTableType",
                            "local-table"),
                        Facet(
                            "accessColumnCount",
                            Invariant(
                                table.Columns.Count)),
                        Facet(
                            "accessPrimaryKeyColumnCount",
                            Invariant(
                                table.PrimaryKeyColumns
                                    .Count)),
                    ],
                };
            objects.Add(tableObject);

            var columnBindings =
                new List<AccessColumnBinding>(
                    table.Columns.Count);
            foreach (AccessColumnMetadata column in
                     table.Columns.OrderBy(
                         static item => item.Ordinal))
            {
                bool primaryKeyColumn =
                    table.PrimaryKeyColumns.Any(
                        name =>
                            string.Equals(
                                name,
                                column.Name,
                                StringComparison
                                    .OrdinalIgnoreCase));
                AccessColumnMetadata effectiveColumn =
                    primaryKeyColumn
                        ? column with
                        {
                            Nullable = false,
                        }
                        : column;
                bool supported =
                    AccessTypeCatalog.TryResolve(
                        column.ProviderType,
                        out AccessTypeSemantics semantics);
                var facets =
                    new List<MigrationCatalogFacet>
                    {
                        Facet(
                            "logicalType",
                            supported
                                ? semantics.LogicalType
                                : "accessUnsupported"),
                        Facet(
                            "nullable",
                            Boolean(
                                effectiveColumn
                                    .Nullable)),
                        Facet(
                            "accessProviderNullable",
                            Boolean(column.Nullable)),
                        Facet(
                            "accessOrdinal",
                            Invariant(column.Ordinal)),
                        Facet(
                            "accessProviderType",
                            column.ProviderType
                                .ToString()),
                        Facet(
                            "accessDataAvailable",
                            Boolean(supported)),
                        Facet(
                            "accessHasDefault",
                            Boolean(
                                column.HasDefault)),
                    };
                if (column.MaximumLength is long length)
                {
                    facets.Add(
                        Facet(
                            "maximumLength",
                            Invariant(length)));
                }
                if (column.Precision is int precision)
                {
                    facets.Add(
                        Facet(
                            "precision",
                            Invariant(precision)));
                }
                if (column.Scale is int scale)
                {
                    facets.Add(
                        Facet(
                            "scale",
                            Invariant(scale)));
                }
                if (column.DefaultDigest is not null)
                {
                    facets.Add(
                        Facet(
                            "accessDefaultDigest",
                            column.DefaultDigest));
                }

                var columnObject =
                    new MigrationCatalogObject
                    {
                        ObjectId =
                            AccessObjectIds.Column(
                                table.Name,
                                column.Ordinal),
                        Kind =
                            MigrationObjectKind.Column,
                        ParentObjectId = tableId,
                        SourceNamespace = "main",
                        SourceName = column.Name,
                        NativeType =
                            AccessTypeCatalog.NativeType(
                                column.ProviderType),
                        Facets =
                            new ReadOnlyCollection<
                                MigrationCatalogFacet>(
                                facets),
                    };
                objects.Add(columnObject);
                columnBindings.Add(
                    new AccessColumnBinding
                    {
                        Metadata =
                            effectiveColumn,
                        CatalogObject =
                            columnObject,
                        Codec = supported
                            ? semantics.Codec
                            : null,
                    });
                if (!supported)
                {
                    diagnostics.Add(
                        Unsupported(
                            columnObject.ObjectId,
                            "MIG-ACCESS-COLUMN-TYPE-UNSUPPORTED-001",
                            $"Access column '{table.Name}.{column.Name}' uses unsupported OLE DB type '{column.ProviderType}'.",
                            "Normalize the source column to a supported scalar type or exclude the table."));
                }
                if (column.HasDefault)
                {
                    diagnostics.Add(
                        Conditional(
                            columnObject.ObjectId,
                            "MIG-ACCESS-COLUMN-DEFAULT-001",
                            $"Access default semantics for '{table.Name}.{column.Name}' are recorded but are not recreated automatically.",
                            "Review target insert behavior before accepting this diagnostic."));
                }
            }

            AccessColumnBinding[] keyColumns =
                table.PrimaryKeyColumns
                    .Select(name =>
                        columnBindings.Single(
                            column =>
                                string.Equals(
                                    column.Metadata.Name,
                                    name,
                                    StringComparison
                                        .OrdinalIgnoreCase)))
                    .ToArray();
            if (keyColumns.Length > 0)
            {
                MigrationObjectReference[] members =
                    keyColumns.Select(
                            (column, ordinal) =>
                                new MigrationObjectReference
                                {
                                    ObjectId =
                                        column.CatalogObject
                                            .ObjectId,
                                    Role =
                                        MigrationObjectReferenceRoles
                                            .Column,
                                    Ordinal = ordinal,
                                })
                        .ToArray();
                objects.Add(
                    new MigrationCatalogObject
                    {
                        ObjectId =
                            AccessObjectIds.PrimaryKey(
                                table.Name),
                        Kind = MigrationObjectKind.Key,
                        ParentObjectId = tableId,
                        SourceNamespace = "main",
                        SourceName =
                            "PRIMARY KEY " +
                            table.Name,
                        Facets =
                        [
                            Facet("kind", "primary"),
                            Facet("unique", "true"),
                            Facet(
                                "accessCatalogContract",
                                AccessMigrationSourceInspector
                                    .CatalogContract),
                        ],
                        Members =
                            Array.AsReadOnly(members),
                        DependsOn =
                            Array.AsReadOnly(
                                members.Select(
                                        static member =>
                                            member.ObjectId)
                                    .ToArray()),
                    });
            }
            else
            {
                diagnostics.Add(
                    Unsupported(
                        tableId,
                        "MIG-ACCESS-TABLE-STABLE-ORDER-001",
                        $"Access table '{table.Name}' has no primary key and cannot be retained in a deterministic replay order.",
                        "Add a non-null primary key or export the table through a reviewed file workflow."));
            }

            foreach (AccessIndexMetadata index in
                     table.Indexes
                         .Where(static item =>
                             !item.Primary)
                         .OrderBy(
                             static item => item.Name,
                             StringComparer.Ordinal))
            {
                AccessColumnBinding[] indexColumns =
                    index.Columns.Select(name =>
                            columnBindings.Single(
                                column =>
                                    string.Equals(
                                        column.Metadata.Name,
                                        name,
                                        StringComparison
                                            .OrdinalIgnoreCase)))
                        .ToArray();
                MigrationObjectReference[] members =
                    indexColumns.Select(
                            (column, ordinal) =>
                                new MigrationObjectReference
                                {
                                    ObjectId =
                                        column.CatalogObject
                                            .ObjectId,
                                    Role =
                                        MigrationObjectReferenceRoles
                                            .Column,
                                    Ordinal = ordinal,
                                })
                        .ToArray();
                string indexId =
                    AccessObjectIds.Index(
                        table.Name,
                        index.Name);
                objects.Add(
                    new MigrationCatalogObject
                    {
                        ObjectId = indexId,
                        Kind = MigrationObjectKind.Index,
                        ParentObjectId = tableId,
                        SourceNamespace = "main",
                        SourceName = index.Name,
                        Facets =
                        [
                            Facet("kind", "standard"),
                            Facet(
                                "unique",
                                Boolean(index.Unique)),
                            Facet(
                                "accessCatalogContract",
                                AccessMigrationSourceInspector
                                    .CatalogContract),
                        ],
                        Members =
                            Array.AsReadOnly(members),
                        DependsOn =
                            Array.AsReadOnly(
                                members.Select(
                                        static member =>
                                            member.ObjectId)
                                    .ToArray()),
                    });
                diagnostics.Add(
                    Unsupported(
                        indexId,
                        "MIG-ACCESS-INDEX-SEMANTICS-UNQUALIFIED-001",
                        $"Access index '{table.Name}.{index.Name}' is inventoried but its collation and NULL semantics are not qualified for automatic recreation.",
                        "Recreate the reviewed index after the table data has been validated."));
            }

            bool tableSupported =
                keyColumns.Length > 0 &&
                columnBindings.All(
                    static column =>
                        column.IsSupported);
            if (!tableSupported &&
                keyColumns.Length > 0)
            {
                diagnostics.Add(
                    Unsupported(
                        tableId,
                        "MIG-ACCESS-TABLE-DATA-UNAVAILABLE-001",
                        $"Access table '{table.Name}' contains a column that cannot be represented by the retained scalar contract.",
                        "Normalize unsupported columns or exclude the table."));
            }
            tableBindings.Add(
                new AccessTableBinding
                {
                    Metadata = table,
                    CatalogObject =
                        tableObject with
                        {
                            Facets =
                                tableObject.Facets
                                    .Append(
                                        Facet(
                                            AccessRetainedDataContract
                                                .DataAvailableFacet,
                                            Boolean(
                                                tableSupported)))
                                    .Concat(
                                        tableSupported
                                            ? []
                                            :
                                            [
                                                Facet(
                                                    AccessRetainedDataContract
                                                        .DataUnavailableReasonFacet,
                                                    keyColumns.Length == 0
                                                        ? AccessRetainedAvailabilityReasons
                                                            .StableOrder
                                                        : AccessRetainedAvailabilityReasons
                                                            .ScalarType),
                                            ])
                                    .ToArray(),
                        },
                    Columns =
                        new ReadOnlyCollection<
                            AccessColumnBinding>(
                            columnBindings),
                    PrimaryKeyColumns =
                        Array.AsReadOnly(keyColumns),
                });
            objects[objects.IndexOf(tableObject)] =
                tableBindings[^1].CatalogObject;
        }

        foreach (AccessForeignKeyMetadata foreignKey in
                 snapshot.ForeignKeys)
        {
            AccessTableBinding? sourceTable =
                tableBindings.SingleOrDefault(table =>
                    string.Equals(
                        table.Metadata.Name,
                        foreignKey.SourceTable,
                        StringComparison
                            .OrdinalIgnoreCase));
            AccessTableBinding? referencedTable =
                tableBindings.SingleOrDefault(table =>
                    string.Equals(
                        table.Metadata.Name,
                        foreignKey.ReferencedTable,
                        StringComparison
                            .OrdinalIgnoreCase));
            AccessColumnBinding[] sourceColumns =
                sourceTable is null
                    ? []
                    : foreignKey.Columns
                        .Select(pair =>
                            sourceTable.Columns
                                .SingleOrDefault(column =>
                                    string.Equals(
                                        column.Metadata.Name,
                                        pair.SourceColumn,
                                        StringComparison
                                            .OrdinalIgnoreCase)))
                        .Where(static column =>
                            column is not null)
                        .Cast<AccessColumnBinding>()
                        .ToArray();
            bool targetIsPrimaryKey =
                referencedTable is not null &&
                foreignKey.Columns.Count ==
                referencedTable.PrimaryKeyColumns
                    .Count &&
                foreignKey.Columns
                    .Select(static pair =>
                        pair.ReferencedColumn)
                    .SequenceEqual(
                        referencedTable
                            .PrimaryKeyColumns
                            .Select(static column =>
                                column.Metadata.Name),
                        StringComparer
                            .OrdinalIgnoreCase);
            bool complete =
                sourceTable is not null &&
                referencedTable is not null &&
                sourceColumns.Length ==
                    foreignKey.Columns.Count &&
                targetIsPrimaryKey;
            string foreignKeyId =
                AccessObjectIds.ForeignKey(
                    foreignKey.SourceTable,
                    foreignKey.Name);
            if (!complete)
            {
                objects.Add(
                    new MigrationCatalogObject
                    {
                        ObjectId = foreignKeyId,
                        Kind =
                            MigrationObjectKind.Other,
                        ParentObjectId =
                            sourceTable?.CatalogObject
                                .ObjectId ??
                            namespaceId,
                        SourceNamespace = "main",
                        SourceName = foreignKey.Name,
                        Facets =
                        [
                            Facet(
                                "kind",
                                "access-unresolved-foreign-key"),
                            Facet(
                                "accessReferencedTable",
                                foreignKey
                                    .ReferencedTable),
                        ],
                        DependsOn =
                            Array.AsReadOnly(
                                sourceColumns.Select(
                                        static column =>
                                            column.CatalogObject
                                                .ObjectId)
                                    .Distinct(
                                        StringComparer.Ordinal)
                                    .ToArray()),
                    });
                diagnostics.Add(
                    Unknown(
                        foreignKeyId,
                        "MIG-ACCESS-FK-BINDING-UNKNOWN-001",
                        $"Access relationship '{foreignKey.Name}' could not be bound to complete local source and primary-key metadata.",
                        "Replace linked targets or recreate the relationship against a visible local primary key."));
                continue;
            }

            string referencedKeyId =
                AccessObjectIds.PrimaryKey(
                    referencedTable!.Metadata.Name);
            MigrationObjectReference[] members =
                sourceColumns.Select(
                        (column, ordinal) =>
                            new MigrationObjectReference
                            {
                                ObjectId =
                                    column.CatalogObject
                                        .ObjectId,
                                Role =
                                    MigrationObjectReferenceRoles
                                        .SourceColumn,
                                Ordinal = ordinal,
                            })
                    .Append(
                        new MigrationObjectReference
                        {
                            ObjectId =
                                referencedKeyId,
                            Role =
                                MigrationObjectReferenceRoles
                                    .ReferencedKey,
                            Ordinal = 0,
                        })
                    .ToArray();
            var foreignKeyFacets =
                new List<MigrationCatalogFacet>
                {
                    Facet("timing", "immediate"),
                    Facet("match", "simple"),
                    Facet("deferrable", "false"),
                    Facet("deferred", "false"),
                    Facet(
                        "onDelete",
                        ReferentialAction(
                            foreignKey.DeleteRule)),
                    Facet(
                        "accessUpdateRule",
                        foreignKey.UpdateRule),
                    Facet(
                        "accessDeleteRule",
                        foreignKey.DeleteRule),
                };
            if (!IsNoAction(
                    foreignKey.UpdateRule))
            {
                foreignKeyFacets.Add(
                    Facet(
                        "onUpdate",
                        ReferentialAction(
                            foreignKey.UpdateRule)));
            }
            objects.Add(
                new MigrationCatalogObject
                {
                    ObjectId = foreignKeyId,
                    Kind =
                        MigrationObjectKind.ForeignKey,
                    ParentObjectId =
                        sourceTable!.CatalogObject
                            .ObjectId,
                    SourceNamespace = "main",
                    SourceName = foreignKey.Name,
                    Facets =
                        new ReadOnlyCollection<
                            MigrationCatalogFacet>(
                            foreignKeyFacets),
                    Members =
                        Array.AsReadOnly(members),
                    DependsOn =
                        Array.AsReadOnly(
                            sourceColumns.Select(
                                    static column =>
                                        column.CatalogObject
                                            .ObjectId)
                                .Append(
                                    referencedKeyId)
                                .Distinct(
                                    StringComparer.Ordinal)
                                .ToArray()),
                });
            if (!IsNoAction(
                    foreignKey.UpdateRule))
            {
                diagnostics.Add(
                    Unsupported(
                        foreignKeyId,
                        "MIG-ACCESS-FK-UPDATE-ACTION-UNSUPPORTED-001",
                        $"Access relationship '{foreignKey.Name}' uses ON UPDATE {foreignKey.UpdateRule}, which CSharpDB does not support.",
                        "Change the relationship to NO ACTION or recreate the behavior in reviewed application logic."));
            }
        }

        foreach (AccessSchemaObjectMetadata item in
                 snapshot.UnsupportedObjects)
        {
            string objectId =
                AccessObjectIds.SchemaObject(
                    item.Type,
                    item.Name);
            objects.Add(
                new MigrationCatalogObject
                {
                    ObjectId = objectId,
                    Kind =
                        string.Equals(
                            item.Type,
                            "VIEW",
                            StringComparison
                                .OrdinalIgnoreCase)
                            ? MigrationObjectKind.View
                            : MigrationObjectKind.Other,
                    ParentObjectId = namespaceId,
                    SourceNamespace = "main",
                    SourceName = item.Name,
                    Facets =
                    [
                        Facet(
                            "accessObjectType",
                            item.Type),
                    ],
                });
            diagnostics.Add(
                Unsupported(
                    objectId,
                    "MIG-ACCESS-OBJECT-UNSUPPORTED-001",
                    $"Access object '{item.Name}' of type '{item.Type}' is inventoried but is not part of the local-table data route.",
                    "Replace linked tables and saved queries with reviewed local-table or query migration steps."));
        }

        diagnostics.Add(
            Unknown(
                databaseId,
                LiveQualificationRule,
                "Microsoft Access retained import has not completed its release qualification matrix.",
                "Run the disposable Windows VM matrix with trusted .mdb and .accdb fixtures before removing this planning/apply blocker."));
        var catalog =
            new MigrationCatalog
            {
                TargetCSharpDbVersion =
                    request.TargetCSharpDbVersion,
                Source =
                    new MigrationSourceIdentity
                    {
                        Kind =
                            MigrationSourceKind.Access,
                        Identity =
                            "access:file:" +
                            snapshot
                                .SourceContentDigest[
                                    "sha256:".Length..],
                        Fingerprint =
                            snapshot.SourceContentDigest,
                        ProviderVersion =
                            snapshot.ProviderVersion,
                        SourceVersion =
                            snapshot.SourceVersion,
                        Consistency =
                            new MigrationConsistencyStrategy
                            {
                                Kind =
                                    MigrationConsistencyKind
                                        .Snapshot,
                                Description =
                                    "The Access file was held under a process-local write/delete-denying lease for the complete catalog read.",
                            },
                    },
                Objects =
                    new ReadOnlyCollection<
                        MigrationCatalogObject>(
                        objects.OrderBy(
                                static item =>
                                    item.ObjectId,
                                StringComparer.Ordinal)
                            .ToArray()),
                Diagnostics =
                    new ReadOnlyCollection<
                        MigrationDiagnostic>(
                        diagnostics.OrderBy(
                                static item =>
                                    item.DiagnosticId,
                                StringComparer.Ordinal)
                            .ToArray()),
            };
        MigrationContractValidator.ValidateCatalog(
            catalog);
        return new AccessCatalogBinding
        {
            Catalog = catalog,
            Database =
                catalog.Objects.Single(
                    static item =>
                        item.Kind ==
                        MigrationObjectKind.Database),
            Tables =
                new ReadOnlyCollection<
                    AccessTableBinding>(
                    tableBindings),
        };
    }

    private static MigrationDiagnostic Unsupported(
        string objectId,
        string ruleId,
        string summary,
        string remediation) =>
        Diagnostic(
            objectId,
            ruleId,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unsupported,
            summary,
            remediation,
            canOverride: false);

    private static MigrationDiagnostic Conditional(
        string objectId,
        string ruleId,
        string summary,
        string remediation) =>
        Diagnostic(
            objectId,
            ruleId,
            MigrationDiagnosticSeverity.Warning,
            MigrationCompatibilityStatus.Conditional,
            summary,
            remediation,
            canOverride: true);

    private static MigrationDiagnostic Unknown(
        string objectId,
        string ruleId,
        string summary,
        string remediation) =>
        Diagnostic(
            objectId,
            ruleId,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            summary,
            remediation,
            canOverride: false);

    private static MigrationDiagnostic Diagnostic(
        string objectId,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string remediation,
        bool canOverride)
    {
        string digest = AccessStableDigest.Text(
            "csharpdb-access-diagnostic/v1",
            ruleId,
            objectId);
        string token =
            digest["sha256:".Length..][..16];
        return new MigrationDiagnostic
        {
            DiagnosticId =
                "diag:" +
                ruleId.ToLowerInvariant() +
                ":" +
                token,
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation =
                "This result is derived from ACE OLE DB schema rowsets while the Access file is protected by a write/delete-denying lease.",
            ObjectId = objectId,
            Remediation = remediation,
            CanOverride = canOverride,
        };
    }

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

    private static string Invariant(int value) =>
        value.ToString(
            CultureInfo.InvariantCulture);

    private static string Invariant(long value) =>
        value.ToString(
            CultureInfo.InvariantCulture);

    private static bool IsNoAction(
        string value) =>
        string.Equals(
            value,
            "NO ACTION",
            StringComparison.OrdinalIgnoreCase) ||
        string.Equals(
            value,
            "RESTRICT",
            StringComparison.OrdinalIgnoreCase);

    private static string ReferentialAction(
        string value)
    {
        if (IsNoAction(value))
            return "restrict";
        return value.Trim()
            .Replace(
                ' ',
                '-')
            .ToLowerInvariant();
    }
}

internal static class AccessObjectIds
{
    internal static string Database(
        string sourceDigest) =>
        "access:database:" +
        Hash(
            "database",
            sourceDigest);

    internal static string Namespace(
        string sourceDigest) =>
        "access:namespace:" +
        Hash(
            "namespace",
            sourceDigest);

    internal static string Table(string name) =>
        "access:table:" +
        Hash("table", name);

    internal static string Column(
        string table,
        int ordinal) =>
        "access:column:" +
        Hash("table", table) +
        ":" +
        ordinal.ToString(
            "D10",
            CultureInfo.InvariantCulture);

    internal static string PrimaryKey(
        string table) =>
        "access:key:" +
        Hash("table", table) +
        ":primary";

    internal static string Index(
        string table,
        string index) =>
        "access:index:" +
        Hash("table", table) +
        ":" +
        Hash("index", index);

    internal static string ForeignKey(
        string table,
        string name) =>
        "access:foreign-key:" +
        Hash("table", table) +
        ":" +
        Hash("foreign-key", name);

    internal static string SchemaObject(
        string type,
        string name) =>
        "access:schema:" +
        Hash("type", type) +
        ":" +
        Hash("name", name);

    private static string Hash(
        string domain,
        string value) =>
        AccessStableDigest.Text(
                "csharpdb-access-object-id/v1",
                domain,
                value)
            ["sha256:".Length..][..32];
}
