using System.Globalization;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static partial class SqlServerCatalogBuilder
{
    public const string CatalogContract = "csharpdb-sqlserver-catalog/v6";

    internal const string RowLevelSecurityFilterFacet =
        "sqlServerEnabledRowLevelSecurityFilter";

    internal const string RowLevelSecurityInventoryCompleteFacet =
        "sqlServerRowLevelSecurityInventoryComplete";

    private static readonly UTF8Encoding s_strictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static MigrationCatalog Build(
        SqlServerCatalogSnapshot snapshot,
        MigrationInspectionRequest request,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The SQL Server analyzer is qualified for CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(request), "Profile sample size must be positive.");
        if (request.IncludeProfile)
        {
            throw new NotSupportedException(
                "The Phase 7A SQL Server checkpoint performs schema analysis only; data profiling is not supported.");
        }

        ValidateSnapshot(snapshot, limits, cancellationToken);
        SqlServerScriptDomAnalysisSnapshot scriptDomAnalysis =
            SqlServerScriptDomAnalyzer.Analyze(
                snapshot,
                limits,
                cancellationToken);

        var objects = new List<MigrationCatalogObject>(
            1 +
            snapshot.Schemas.Count +
            snapshot.Tables.Count +
            snapshot.Columns.Count +
            RelationalObjectCapacity(snapshot) +
            ProgrammableObjectCapacity(snapshot) +
            PhysicalObjectCapacity(snapshot));
        var diagnostics = new List<MigrationDiagnostic>();

        string databaseId = ObjectId("database", snapshot.Database.Name);
        MetadataVisibility visibility = GetMetadataVisibility(snapshot);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = databaseId,
            Kind = MigrationObjectKind.Database,
            SourceName = snapshot.Database.Name,
            Facets =
            [
                Facet("sqlServerCatalogContract", CatalogContract),
                Facet("sqlServerProductVersion", snapshot.Instance.ProductVersion),
                Facet("sqlServerProductMajorVersion", Invariant(snapshot.Instance.ProductMajorVersion)),
                Facet("sqlServerProductLevel", snapshot.Instance.ProductLevel),
                Facet("sqlServerEdition", snapshot.Instance.Edition),
                Facet("sqlServerEngineEdition", Invariant(snapshot.Instance.EngineEdition)),
                Facet("sqlServerDatabaseId", Invariant(snapshot.Database.DatabaseId)),
                Facet("sqlServerCompatibilityLevel", Invariant(snapshot.Database.CompatibilityLevel)),
                Facet("sqlServerCollation", snapshot.Database.Collation),
                Facet(
                    "sqlServerReadCommittedSnapshot",
                    Boolean(snapshot.Database.IsReadCommittedSnapshotOn)),
                Facet(
                    "sqlServerSnapshotIsolationState",
                    snapshot.Database.SnapshotIsolationState),
                Facet(
                    "sqlServerAutoCreateStatistics",
                    Boolean(snapshot.Database.IsAutoCreateStatsOn)),
                Facet(
                    "sqlServerAutoUpdateStatistics",
                    Boolean(snapshot.Database.IsAutoUpdateStatsOn)),
                Facet(
                    "sqlServerAnsiNullDefault",
                    Boolean(snapshot.Database.IsAnsiNullDefaultOn)),
                Facet(
                    "sqlServerQuotedIdentifier",
                    Boolean(snapshot.Database.IsQuotedIdentifierOn)),
                Facet(
                    "sqlServerParameterizationForced",
                    Boolean(snapshot.Database.IsParameterizationForced)),
                Facet("sqlServerContainment", snapshot.Database.Containment),
                Facet("sqlServerTrustworthy", Boolean(snapshot.Database.IsTrustworthyOn)),
                Facet("sqlServerMetadataVisibility", visibility.ToString().ToLowerInvariant()),
                Facet("sqlServerPermissionSysAdmin", NullableBoolean(snapshot.Database.IsSysAdmin)),
                Facet("sqlServerPermissionDbOwner", NullableBoolean(snapshot.Database.IsDbOwner)),
                Facet("sqlServerPermissionControl", NullableBoolean(snapshot.Database.HasControl)),
                Facet(
                    "sqlServerPermissionViewDefinition",
                    NullableBoolean(snapshot.Database.HasViewDefinition)),
                Facet(
                    "sqlServerPermissionViewSecurityDefinition",
                    NullableBoolean(snapshot.Database.HasViewSecurityDefinition)),
                Facet(
                    "sqlServerPermissionSelectExpressionDependencies",
                    NullableBoolean(snapshot.Database.HasSelectSqlExpressionDependencies)),
                Facet(
                    "sqlServerPermissionAuditAttempted",
                    Boolean(
                        snapshot.PermissionAuditBefore.Attempted &&
                        snapshot.PermissionAuditAfter.Attempted)),
                Facet(
                    "sqlServerPermissionTokenCount",
                    Invariant(snapshot.PermissionAuditAfter.Tokens.Count)),
                Facet(
                    "sqlServerPermissionDenyCount",
                    Invariant(snapshot.PermissionAuditAfter.Denials.Count)),
                Facet(
                    "sqlServerPermissionAuditDigest",
                    PermissionAuditDigest(snapshot.PermissionAuditAfter)),
                Facet(
                    "sqlServerPermissionAuditStable",
                    Boolean(PermissionAuditsEqual(
                        snapshot.PermissionAuditBefore,
                        snapshot.PermissionAuditAfter))),
                Facet(
                    "sqlServerExpressionDependencyAuditAttempted",
                    Boolean(snapshot.ExpressionDependencyAudit.Attempted)),
                Facet(
                    "sqlServerExpressionDependencyCount",
                    Invariant(snapshot.ExpressionDependencyAudit.Dependencies.Count)),
                Facet(
                    "sqlServerExpressionDependencyAuditDigest",
                    ExpressionDependencyAuditDigest(snapshot.ExpressionDependencyAudit)),
                Facet(
                    "sqlServerIndexedViewIndexCount",
                    Invariant(CountIndexedViewIndexes(snapshot))),
                Facet(
                    "sqlServerFullTextCatalogCount",
                    Invariant(snapshot.FullTextCatalogs.Count)),
                Facet(
                    "sqlServerFullTextStoplistCount",
                    Invariant(snapshot.FullTextStoplists.Count)),
                Facet(
                    "sqlServerSearchPropertyListCount",
                    Invariant(snapshot.SearchPropertyLists.Count)),
                Facet(
                    "sqlServerFullTextIndexCount",
                    Invariant(snapshot.FullTextIndexes.Count)),
                Facet(
                    "sqlServerFullTextIndexColumnCount",
                    Invariant(snapshot.FullTextIndexColumns.Count)),
                Facet(
                    "sqlServerDataSpaceCount",
                    Invariant(snapshot.DataSpaces.Count)),
                Facet(
                    "sqlServerPartitionFunctionCount",
                    Invariant(snapshot.PartitionFunctions.Count)),
                Facet(
                    "sqlServerPartitionSchemeCount",
                    Invariant(snapshot.PartitionSchemes.Count)),
                Facet(
                    "sqlServerPartitionParameterCount",
                    Invariant(snapshot.PartitionParameters.Count)),
                Facet(
                    "sqlServerPartitionBoundaryCount",
                    Invariant(snapshot.PartitionRangeValues.Count)),
                Facet(
                    "sqlServerPartitionDestinationCount",
                    Invariant(snapshot.PartitionSchemeDestinations.Count)),
                Facet(
                    "sqlServerPhysicalPartitionCount",
                    Invariant(snapshot.IndexPartitions.Count)),
                Facet(
                    "sqlServerXmlIndexCount",
                    Invariant(snapshot.XmlIndexes.Count)),
                Facet(
                    "sqlServerSelectiveXmlIndexPathCount",
                    Invariant(snapshot.SelectiveXmlIndexPaths.Count)),
                Facet(
                    "sqlServerSpatialIndexCount",
                    Invariant(snapshot.SpatialIndexes.Count)),
                Facet(
                    "sqlServerSpatialIndexTessellationCount",
                    Invariant(snapshot.SpatialIndexTessellations.Count)),
                Facet(
                    "sqlServerHashIndexCount",
                    Invariant(snapshot.HashIndexes.Count)),
                Facet(
                    "sqlServerColumnstoreIndexCount",
                    Invariant(CountColumnstoreIndexes(snapshot))),
                Facet(
                    "sqlServerJsonIndexCount",
                    Invariant(snapshot.JsonIndexes.Count)),
                Facet(
                    "sqlServerJsonIndexPathCount",
                    Invariant(snapshot.JsonIndexPaths.Count)),
                Facet(
                    "sqlServerIndexedViewIndexInventoryStatus",
                    PhysicalInventoryStatus(visibility)),
                Facet(
                    "sqlServerFullTextInventoryStatus",
                    PhysicalInventoryStatus(visibility)),
                Facet(
                    "sqlServerPartitionStorageInventoryStatus",
                    PhysicalInventoryStatus(visibility)),
                Facet(
                    "sqlServerIndexSubtypeInventoryStatus",
                    PhysicalInventoryStatus(visibility)),
            ],
        });

        AddQualificationDiagnostics(
            snapshot,
            databaseId,
            visibility,
            diagnostics);
        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-SQLSERVER-INVENTORY-PARTIAL-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "This checkpoint is an intentionally partial SQL Server inventory.",
            "Bounded indexed-view indexes, full-text configuration, data spaces, partition definitions, per-index partition compression, and XML, spatial, hash, columnstore, and SQL Server 2025 JSON index configuration are now inventoried. Raw XML and JSON paths remain in memory only and are retained durably as bounded length and domain-separated digest facts. Raw partition boundary values, full-text stopwords and registered property definitions, allocation and file placement, physical sizes, row estimates, and other volatile operational state are intentionally not retained. SQL definitions remain syntax-only evidence; binding, lowering, source-semantic proof, and live qualification are still pending.",
            "Complete semantic proof and finish live qualification before relying on the analyzer for migration approval.",
            canOverride: false));

        var schemasById =
            new Dictionary<int, (SqlServerSchemaMetadata Metadata, string ObjectId)>();
        foreach (SqlServerSchemaMetadata schema in snapshot.Schemas
                     .OrderBy(static item => item.SchemaId)
                     .ThenBy(static item => item.Name, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string schemaObjectId = ObjectId("schema", schema.Name);
            schemasById.Add(schema.SchemaId, (schema, schemaObjectId));
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = schemaObjectId,
                Kind = MigrationObjectKind.Namespace,
                ParentObjectId = databaseId,
                SourceNamespace = schema.Name,
                SourceName = schema.Name,
                Facets =
                [
                    Facet("isDefault", Boolean(
                        string.Equals(schema.Name, "dbo", StringComparison.Ordinal))),
                    Facet("sqlServerSchemaId", Invariant(schema.SchemaId)),
                    Facet(
                        "sqlServerPermissionViewDefinition",
                        NullableBoolean(schema.HasViewDefinition)),
                ],
            });
        }

        var tablesByObjectId = new Dictionary<int, (SqlServerTableMetadata Metadata, string Id)>();
        foreach (SqlServerTableMetadata table in snapshot.Tables
                     .OrderBy(static item => item.ObjectId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerSchemaMetadata schema, string namespaceId) =
                schemasById[table.SchemaId];
            string tableId = ObjectId("table", schema.Name, table.Name);
            tablesByObjectId.Add(table.ObjectId, (table, tableId));

            objects.Add(new MigrationCatalogObject
            {
                ObjectId = tableId,
                Kind = MigrationObjectKind.Table,
                ParentObjectId = namespaceId,
                SourceNamespace = schema.Name,
                SourceName = table.Name,
                Facets =
                [
                    Facet("sqlServerObjectId", Invariant(table.ObjectId)),
                    Facet("sqlServerMemoryOptimized", Boolean(table.IsMemoryOptimized)),
                    Facet("sqlServerDurability", table.Durability),
                    Facet("sqlServerFileTable", Boolean(table.IsFileTable)),
                    Facet("sqlServerTemporalType", table.TemporalType),
                    Facet("sqlServerGraphNode", Boolean(table.IsNode)),
                    Facet("sqlServerGraphEdge", Boolean(table.IsEdge)),
                    Facet(
                        "sqlServerLobDataSpaceId",
                        Invariant(table.LobDataSpaceId)),
                    Facet(
                        "sqlServerFileStreamDataSpaceId",
                        Invariant(table.FileStreamDataSpaceId)),
                    Facet(
                        RowLevelSecurityFilterFacet,
                        Boolean(
                            table
                                .HasEnabledRowLevelSecurityFilter)),
                    Facet(
                        RowLevelSecurityInventoryCompleteFacet,
                        Boolean(
                            table
                                .IsRowLevelSecurityInventoryComplete)),
                    Facet(
                        "sqlServerPermissionViewDefinition",
                        NullableBoolean(table.HasViewDefinition)),
                ],
            });

            if (HasSpecialTableShape(table))
            {
                diagnostics.Add(Diagnostic(
                    tableId,
                    "MIG-SQLSERVER-TABLE-SHAPE-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The table uses a SQL Server-specific storage or temporal shape.",
                    "Memory-optimized, non-schema-and-data durable, file, temporal, and graph tables are inventoried but are not in this analyzer checkpoint's supported CSharpDB subset.",
                    "Replace the feature with an ordinary disk-based table or provide a reviewed target design.",
                    canOverride: false));
            }
            if (table.HasEnabledRowLevelSecurityFilter)
            {
                diagnostics.Add(Diagnostic(
                    tableId,
                    "MIG-SQLSERVER-RLS-FILTER-UNSUPPORTED-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unsupported,
                    "The table has an enabled SQL Server row-level security filter.",
                    "An enabled FILTER predicate changes SELECT visibility and can omit source rows for the migration principal, so retained capture cannot prove that a table scan is complete.",
                    "Disable and independently review the security policy for the migration window, or migrate the table through a separately qualified complete-data export.",
                    canOverride: false));
            }
            if (!table.IsRowLevelSecurityInventoryComplete)
            {
                diagnostics.Add(Diagnostic(
                    tableId,
                    "MIG-SQLSERVER-RLS-INVENTORY-INCOMPLETE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "Complete row-level security metadata visibility could not be established for the table.",
                    "The migration principal is neither sysadmin nor proven to hold ALTER ANY SECURITY POLICY, so hidden FILTER predicates cannot be ruled out.",
                    "Inspect with complete security-policy metadata visibility before retaining rows from this table.",
                    canOverride: false));
            }
        }

        var columnsByCatalogId =
            new Dictionary<(int ObjectId, int ColumnId), (SqlServerColumnMetadata Metadata, string Id)>();
        foreach (SqlServerColumnMetadata column in snapshot.Columns
                     .OrderBy(static item => item.ObjectId)
                     .ThenBy(static item => item.ColumnId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            (SqlServerTableMetadata table, string tableId) = tablesByObjectId[column.ObjectId];
            SqlServerSchemaMetadata schema = schemasById[table.SchemaId].Metadata;
            string columnId = ObjectId("column", schema.Name, table.Name, column.Name);
            columnsByCatalogId.Add((column.ObjectId, column.ColumnId), (column, columnId));
            bool computed = column.IsComputed;
            bool rowVersion = IsRowVersion(column.SystemTypeName);
            bool userDefinedType = !string.Equals(
                column.TypeSchema,
                "sys",
                StringComparison.Ordinal);
            string logicalType = computed || rowVersion || userDefinedType
                ? "native"
                : LogicalType(column.SystemTypeName);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("logicalType", logicalType),
                Facet("nullable", Boolean(column.IsNullable)),
                Facet("identity", Boolean(column.IsIdentity)),
                Facet("rowVersion", Boolean(rowVersion)),
                Facet("sqlServerObjectId", Invariant(column.ObjectId)),
                Facet("sqlServerColumnId", Invariant(column.ColumnId)),
                Facet("sqlServerTypeSchema", column.TypeSchema),
                Facet("sqlServerTypeName", column.TypeName),
                Facet("sqlServerSystemTypeName", column.SystemTypeName),
                Facet("sqlServerUserDefinedType", Boolean(userDefinedType)),
                Facet("sqlServerMaxLengthBytes", Invariant(column.MaxLength)),
                Facet("sqlServerPrecision", Invariant(column.Precision)),
                Facet("sqlServerScale", Invariant(column.Scale)),
                Facet("sqlServerCollation", column.Collation),
                Facet("sqlServerSparse", Boolean(column.IsSparse)),
                Facet("sqlServerColumnSet", Boolean(column.IsColumnSet)),
                Facet("sqlServerHidden", Boolean(column.IsHidden)),
                Facet("sqlServerComputed", Boolean(column.IsComputed)),
                Facet("sqlServerFileStream", Boolean(column.IsFileStream)),
                Facet("sqlServerMasked", Boolean(column.IsMasked)),
                Facet("sqlServerEncryptionType", column.EncryptionType),
                Facet("sqlServerXmlCollectionId", Invariant(column.XmlCollectionId)),
                Facet("sqlServerGeneratedAlwaysType", column.GeneratedAlwaysType),
                Facet("sqlServerComputedPersisted", Boolean(column.IsPersisted)),
                Facet("sqlServerIdentityNotForReplication", Boolean(
                    column.IdentityNotForReplication)),
            };
            AddLogicalFacets(facets, column);
            AddDefaultFacets(facets, column);
            AddComputedFacets(facets, column);
            AddIdentityFacets(facets, column);
            SqlServerScriptDomDefinitionAnalysis? defaultAnalysis =
                GetScriptDomAnalysis(
                    scriptDomAnalysis,
                    SqlServerScriptDomDefinitionKind.DefaultExpression,
                    column.ObjectId,
                    column.ColumnId,
                    column.DefaultDefinition is not null);
            SqlServerScriptDomDefinitionAnalysis? computedAnalysis =
                GetScriptDomAnalysis(
                    scriptDomAnalysis,
                    SqlServerScriptDomDefinitionKind.ComputedExpression,
                    column.ObjectId,
                    column.ColumnId,
                    column.ComputedDefinition is not null);
            if (column.HasDefault)
            {
                AddScriptDomFacets(
                    facets,
                    "sqlServerDefaultTsqlAnalysis",
                    "sqlServerDefaultTsql",
                    defaultAnalysis);
            }
            if (column.IsComputed)
            {
                AddScriptDomFacets(
                    facets,
                    "sqlServerComputedTsqlAnalysis",
                    "sqlServerComputedTsql",
                    computedAnalysis);
            }

            objects.Add(new MigrationCatalogObject
            {
                ObjectId = columnId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = tableId,
                SourceNamespace = schema.Name,
                SourceName = column.Name,
                NativeType = FormatNativeType(column),
                Facets = facets.AsReadOnly(),
            });

            AddColumnDiagnostics(
                column,
                columnId,
                logicalType,
                defaultAnalysis,
                computedAnalysis,
                diagnostics);
        }

        AddRelationalObjects(
            snapshot,
            scriptDomAnalysis,
            schemasById,
            tablesByObjectId,
            columnsByCatalogId,
            objects,
            diagnostics,
            cancellationToken);

        AddProgrammableObjects(
            snapshot,
            scriptDomAnalysis,
            databaseId,
            schemasById,
            tablesByObjectId,
            columnsByCatalogId,
            objects,
            diagnostics,
            cancellationToken);

        AddPhysicalObjects(
            snapshot,
            scriptDomAnalysis,
            databaseId,
            schemasById,
            tablesByObjectId,
            columnsByCatalogId,
            objects,
            diagnostics,
            cancellationToken);

        string fingerprint = "sha256:" + ComputeSnapshotDigest(snapshot);
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.SqlServer,
                Identity = "sqlserver-database:" + SqlServerStableDigest.Text(
                    "csharpdb-sqlserver-source-identity/v1",
                    snapshot.EndpointDigest,
                    snapshot.Database.Name),
                Fingerprint = fingerprint,
                ProviderVersion = snapshot.ProviderVersion,
                SourceVersion = snapshot.Instance.ProductVersion,
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.BestEffort,
                    Description =
                        "One non-pooled SQL Server connection executed only static SELECT catalog queries with ApplicationIntent=ReadOnly; concurrent DDL is not excluded.",
                },
            },
            Objects = objects
                .OrderBy(static item => item.ObjectId, StringComparer.Ordinal)
                .ToArray(),
            Diagnostics = diagnostics
                .OrderBy(static item => item.DiagnosticId, StringComparer.Ordinal)
                .ToArray(),
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return catalog;
    }

    private static void ValidateSnapshot(
        SqlServerCatalogSnapshot snapshot,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        if (snapshot.Schemas.Count > limits.MaxSchemas)
            throw LimitExceeded("schema count");
        if (snapshot.Tables.Count > limits.MaxTables)
            throw LimitExceeded("table count");
        if (snapshot.Columns.Count > limits.MaxColumns)
            throw LimitExceeded("column count");
        ValidateRelationalCounts(snapshot, limits);
        ValidateProgrammableCounts(snapshot, limits);
        ValidatePhysicalCounts(snapshot, limits);
        ValidateAggregateStructuralCount(snapshot, limits);
        if (snapshot.Instance.ProductMajorVersion <= 0)
            throw new SqlServerMigrationException("SQL Server returned an invalid product major version.");
        if (snapshot.Database.DatabaseId <= 0)
            throw new SqlServerMigrationException("SQL Server returned an invalid database identifier.");

        var budget = new MetadataBudget(limits);
        if (!IsSha256(snapshot.EndpointDigest))
        {
            throw new SqlServerMigrationException(
                "The SQL Server endpoint digest is invalid.");
        }
        budget.Add(snapshot.EndpointDigest);
        budget.Add(snapshot.ProviderVersion);
        budget.Add(snapshot.Instance.ProductVersion);
        budget.Add(snapshot.Instance.ProductLevel);
        budget.Add(snapshot.Instance.Edition);
        budget.Add(snapshot.Database.Name, isName: true);
        budget.Add(snapshot.Database.Collation);
        budget.Add(snapshot.Database.SnapshotIsolationState);
        budget.Add(snapshot.Database.Containment);

        var schemaIds = new HashSet<int>();
        var schemaNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (SqlServerSchemaMetadata schema in snapshot.Schemas)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (schema.SchemaId <= 0 || !schemaIds.Add(schema.SchemaId))
                throw new SqlServerMigrationException("SQL Server returned duplicate or invalid schema metadata.");
            if (!schemaNames.Add(schema.Name))
                throw new SqlServerMigrationException("SQL Server returned duplicate schema names.");
            budget.Add(schema.Name, isName: true);
        }

        var tableIds = new HashSet<int>();
        var tableNames = new HashSet<(int SchemaId, string Name)>();
        foreach (SqlServerTableMetadata table in snapshot.Tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (table.ObjectId <= 0 || !tableIds.Add(table.ObjectId) ||
                !schemaIds.Contains(table.SchemaId) ||
                !tableNames.Add((table.SchemaId, table.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned table metadata.");
            }
            budget.Add(table.Name, isName: true);
            budget.Add(table.Durability);
            budget.Add(table.TemporalType);
        }

        var columnIds = new HashSet<(int ObjectId, int ColumnId)>();
        var columnNames = new HashSet<(int ObjectId, string Name)>();
        foreach (SqlServerColumnMetadata column in snapshot.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!tableIds.Contains(column.ObjectId) ||
                column.ColumnId <= 0 ||
                !columnIds.Add((column.ObjectId, column.ColumnId)) ||
                !columnNames.Add((column.ObjectId, column.Name)))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned duplicate, invalid, or unowned column metadata.");
            }
            budget.Add(column.Name, isName: true);
            budget.Add(column.TypeSchema, isName: true);
            budget.Add(column.TypeName, isName: true);
            budget.Add(column.SystemTypeName, isName: true);
            budget.Add(column.Collation);
            budget.Add(column.EncryptionType);
            budget.Add(column.GeneratedAlwaysType);
            budget.Add(column.DefaultConstraintName, isName: true);
            budget.ReserveExpression(column.DefaultDefinitionBytes);
            budget.AddExpression(column.DefaultDefinition);
            budget.ReserveExpression(column.ComputedDefinitionBytes);
            budget.AddExpression(column.ComputedDefinition);
            budget.Add(column.IdentitySeed);
            budget.Add(column.IdentityIncrement);

            ValidateColumnShape(column);
        }

        ValidateRelationalSnapshot(
            snapshot,
            schemaIds,
            tableIds,
            columnIds,
            budget,
            cancellationToken);
        ValidateProgrammableSnapshot(
            snapshot,
            schemaIds,
            tableIds,
            columnIds,
            budget,
            cancellationToken);
        ValidatePhysicalSnapshot(
            snapshot,
            schemaIds,
            tableIds,
            columnIds,
            budget,
            limits,
            cancellationToken);
    }

    private static void AddQualificationDiagnostics(
        SqlServerCatalogSnapshot snapshot,
        string databaseId,
        MetadataVisibility visibility,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (visibility != MetadataVisibility.Complete)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-METADATA-VISIBILITY-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "Complete SQL Server metadata visibility could not be established.",
                "A least-privilege proof requires stable before-and-after effective-token and DENY audits, database and per-object definition visibility, expression-dependency visibility, and complete row-level-security inventory.",
                "Grant only the documented read-only metadata permissions, remove applicable metadata DENY entries, and inspect again. Do not grant sysadmin to bypass this guard.",
                canOverride: false));
        }

        AddPermissionQualificationDiagnostics(
            snapshot,
            databaseId,
            diagnostics);

        if (snapshot.Instance.EngineEdition is not (2 or 3 or 4))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-ENGINE-VARIANT-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The connected SQL engine variant is not qualified.",
                $"Engine edition code {snapshot.Instance.EngineEdition} is inventoried, but this checkpoint qualifies only on-premises SQL Server Standard, Enterprise, and Express engines.",
                "Analyze an on-premises SQL Server 2019, 2022, or 2025 database, or add an independently tested provider lane.",
                canOverride: false));
        }

        int? expectedCompatibility = snapshot.Instance.ProductMajorVersion switch
        {
            15 => 150,
            16 => 160,
            17 => 170,
            _ => null,
        };
        if (expectedCompatibility is null)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-VERSION-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The SQL Server major version is not qualified.",
                $"Product major version {snapshot.Instance.ProductMajorVersion} is outside the SQL Server 2019, 2022, and 2025 qualification lanes.",
                "Use a qualified server version or add an executable qualification lane.",
                canOverride: false));
        }
        else if (snapshot.Database.CompatibilityLevel != expectedCompatibility)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-SQLSERVER-COMPATIBILITY-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The database compatibility level is not qualified.",
                $"Compatibility level {snapshot.Database.CompatibilityLevel} does not match the qualified default {expectedCompatibility} for product major version {snapshot.Instance.ProductMajorVersion}.",
                "Move the database to the qualified default compatibility level and re-run analysis, or add an independently tested lane.",
                canOverride: false));
        }

        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-SQLSERVER-LIVE-QUALIFICATION-PENDING-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "The detected SQL Server lane has not passed live qualification.",
            "Version and engine detection match the intended support matrix, but exact-tag ephemeral SQL Server fixtures and least-privilege read-only tests have not yet passed in this repository.",
            "Run the project-authored golden DDL against the exact serviced server tag and record the executable qualification evidence.",
            canOverride: false));
    }

    private static void AddColumnDiagnostics(
        SqlServerColumnMetadata column,
        string columnId,
        string logicalType,
        SqlServerScriptDomDefinitionAnalysis? defaultAnalysis,
        SqlServerScriptDomDefinitionAnalysis? computedAnalysis,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (logicalType == "native")
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-SQLSERVER-TYPE-OR-GENERATION-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The column requires provider-specific type or generation handling.",
                "Computed columns, rowversion values, and unregistered SQL Server types are preserved as native metadata and are not lowered by this checkpoint.",
                "Materialize a reviewed ordinary scalar representation or wait for a bounded provider-specific lowering rule.",
                canOverride: false));
        }

        if (column.HasDefault)
        {
            if (column.DefaultDefinition is null)
            {
                diagnostics.Add(Diagnostic(
                    columnId,
                    "MIG-SQLSERVER-DEFAULT-DEFINITION-UNAVAILABLE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server default definition is unavailable.",
                    "The catalog reports a default constraint, but no definition was visible for bounded syntax analysis.",
                    "Restore complete definition visibility and inspect again.",
                    canOverride: false,
                    occurrenceKey: column.DefaultConstraintName));
            }
            else
            {
                AddScriptDomDiagnostic(
                    columnId,
                    "default expression",
                    defaultAnalysis!,
                    diagnostics);
            }
        }

        if (column.IsComputed)
        {
            if (column.ComputedDefinition is null)
            {
                diagnostics.Add(Diagnostic(
                    columnId,
                    "MIG-SQLSERVER-COMPUTED-DEFINITION-UNAVAILABLE-001",
                    MigrationDiagnosticSeverity.Error,
                    MigrationCompatibilityStatus.Unknown,
                    "The SQL Server computed-column definition is unavailable.",
                    "The catalog reports a computed column, but no definition was visible for bounded syntax analysis.",
                    "Restore complete definition visibility and inspect again.",
                    canOverride: false));
            }
            else
            {
                AddScriptDomDiagnostic(
                    columnId,
                    "computed-column expression",
                    computedAnalysis!,
                    diagnostics);
            }
        }

        if (column.IsIdentity && column.IdentitySeed is null)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-SQLSERVER-IDENTITY-DETAILS-UNKNOWN-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server identity details are not visible.",
                "The column is explicitly marked as identity, but its seed and increment metadata were not returned.",
                "Grant sufficient metadata visibility and inspect again before planning identity lowering.",
                canOverride: false));
        }

        if (column.Collation is not null)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-SQLSERVER-COLLATION-UNANALYZED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "SQL Server collation semantics have not been proven equivalent.",
                "The provider collation is retained without treating its ordering, comparison, or uniqueness behavior as a CSharpDB collation.",
                "Select and test an explicit target collation policy before migration.",
                canOverride: false,
                occurrenceKey: column.Collation));
        }

        if (column.IsSparse ||
            column.IsColumnSet ||
            column.IsHidden ||
            column.IsFileStream ||
            column.IsMasked ||
            column.EncryptionType is not null ||
            column.XmlCollectionId != 0 ||
            !string.Equals(column.GeneratedAlwaysType, "NOT_APPLICABLE", StringComparison.Ordinal))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-SQLSERVER-COLUMN-FEATURE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The column uses an unsupported SQL Server-specific feature.",
                "Sparse, column-set, hidden, FILESTREAM, masked, encrypted, typed-XML, and generated-always facets are inventoried but cannot be silently discarded.",
                "Materialize an ordinary column shape with reviewed semantics.",
                canOverride: false));
        }
    }

    private static void AddLogicalFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerColumnMetadata column)
    {
        string type = column.SystemTypeName.ToLowerInvariant();
        if (type is "decimal" or "numeric" or "money" or "smallmoney")
        {
            facets.Add(Facet("precision", Invariant(column.Precision)));
            facets.Add(Facet("scale", Invariant(column.Scale)));
        }
        if (type is "time" or "datetime2" or "datetimeoffset")
            facets.Add(Facet("fractionalSeconds", Invariant(column.Scale)));
        if (IsLengthType(type))
        {
            facets.Add(Facet(
                "maxLength",
                column.MaxLength < 0
                    ? "max"
                    : Invariant(type is "nchar" or "nvarchar"
                        ? column.MaxLength / 2
                        : column.MaxLength)));
        }
    }

    private static void AddDefaultFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerColumnMetadata column)
    {
        if (!column.HasDefault)
            return;
        facets.Add(Facet("hasDefault", "true"));
        facets.Add(Facet("defaultKind", "source-expression"));
        facets.Add(Facet("sqlServerDefaultConstraintName", column.DefaultConstraintName));
        facets.Add(Facet(
            "sqlServerDefaultDefinitionSourceBytes",
            column.DefaultDefinitionBytes is null
                ? "unknown"
                : Invariant(column.DefaultDefinitionBytes.Value)));
        if (column.DefaultDefinition is null)
            return;
        facets.Add(Facet(
            "sqlServerDefaultDefinitionDigest",
            "sha256:" + SqlServerStableDigest.Text(
                "csharpdb-sqlserver-default-definition/v1",
                column.DefaultDefinition)));
        facets.Add(Facet(
            "sqlServerDefaultDefinitionLength",
            Invariant(column.DefaultDefinition.Length)));
    }

    private static void AddComputedFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerColumnMetadata column)
    {
        if (!column.IsComputed)
            return;
        facets.Add(Facet(
            "sqlServerComputedDefinitionSourceBytes",
            column.ComputedDefinitionBytes is null
                ? "unknown"
                : Invariant(column.ComputedDefinitionBytes.Value)));
        if (column.ComputedDefinition is null)
            return;
        facets.Add(Facet(
            "sqlServerComputedDefinitionDigest",
            "sha256:" + SqlServerStableDigest.Text(
                "csharpdb-sqlserver-computed-definition/v1",
                column.ComputedDefinition)));
        facets.Add(Facet(
            "sqlServerComputedDefinitionLength",
            Invariant(column.ComputedDefinition.Length)));
    }

    private static void AddIdentityFacets(
        ICollection<MigrationCatalogFacet> facets,
        SqlServerColumnMetadata column)
    {
        if (!column.IsIdentity)
            return;
        if (column.IdentitySeed is null)
            return;
        facets.Add(Facet("sqlServerIdentitySeed", column.IdentitySeed));
        facets.Add(Facet("sqlServerIdentityIncrement", column.IdentityIncrement));
    }

    private static string LogicalType(string systemTypeName) =>
        SqlServerTypeSemantics.LogicalType(systemTypeName);

    private static string FormatNativeType(SqlServerColumnMetadata column)
    {
        string type = $"{column.TypeSchema}.{column.TypeName}";
        if (!string.Equals(column.TypeSchema, "sys", StringComparison.Ordinal) ||
            !string.Equals(column.TypeName, column.SystemTypeName, StringComparison.Ordinal))
        {
            return type;
        }

        string systemType = column.SystemTypeName.ToLowerInvariant();
        if (systemType is "decimal" or "numeric")
            return $"{type}({Invariant(column.Precision)},{Invariant(column.Scale)})";
        if (systemType is "time" or "datetime2" or "datetimeoffset")
            return $"{type}({Invariant(column.Scale)})";
        if (IsLengthType(systemType))
        {
            string length = column.MaxLength < 0
                ? "max"
                : Invariant(systemType is "nchar" or "nvarchar"
                    ? column.MaxLength / 2
                    : column.MaxLength);
            return $"{type}({length})";
        }
        return type;
    }

    private static bool IsLengthType(string type) =>
        type is "binary" or "varbinary" or "char" or "varchar" or "nchar" or "nvarchar";

    private static bool IsRowVersion(string type) =>
        SqlServerTypeSemantics.IsRowVersion(type);

    private static bool HasSpecialTableShape(SqlServerTableMetadata table) =>
        table.IsMemoryOptimized ||
        !string.Equals(table.Durability, "SCHEMA_AND_DATA", StringComparison.Ordinal) ||
        table.IsFileTable ||
        !string.Equals(table.TemporalType, "NON_TEMPORAL_TABLE", StringComparison.Ordinal) ||
        table.IsNode ||
        table.IsEdge;

    private static void ValidateColumnShape(SqlServerColumnMetadata column)
    {
        bool hasSeed = column.IdentitySeed is not null;
        bool hasIncrement = column.IdentityIncrement is not null;
        if (hasSeed != hasIncrement ||
            (!column.IsIdentity &&
             (hasSeed || column.IdentityNotForReplication)))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent identity-column metadata.");
        }

        string systemType = column.SystemTypeName.ToLowerInvariant();
        if (IsLengthType(systemType))
        {
            bool permitsMax = systemType is "varchar" or "nvarchar" or "varbinary";
            if (column.MaxLength == 0 ||
                column.MaxLength < -1 ||
                (column.MaxLength == -1 && !permitsMax) ||
                (systemType is "nchar" or "nvarchar" &&
                 column.MaxLength != -1 &&
                 column.MaxLength % 2 != 0))
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid length metadata for a column.");
            }
        }

        if (systemType is "decimal" or "numeric")
        {
            if (column.Precision is < 1 or > 38 || column.Scale > column.Precision)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned invalid decimal precision or scale metadata.");
            }
        }
        if (systemType is "time" or "datetime2" or "datetimeoffset" &&
            column.Scale > 7)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid temporal scale metadata.");
        }
        if (column.XmlCollectionId < 0)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned invalid XML schema collection metadata.");
        }
        if (!column.IsComputed && column.ComputedDefinition is not null)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent computed-column metadata.");
        }
        if (!column.IsComputed &&
            (column.ComputedDefinitionBytes is not null || column.IsPersisted))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent computed-column metadata.");
        }
        if (!column.HasDefault &&
            (column.DefaultConstraintName is not null ||
             column.DefaultDefinitionBytes is not null ||
             column.DefaultDefinition is not null))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent default-constraint metadata.");
        }
        ValidateDefinitionLength(
            column.DefaultDefinition,
            column.DefaultDefinitionBytes,
            "default");
        ValidateDefinitionLength(
            column.ComputedDefinition,
            column.ComputedDefinitionBytes,
            "computed");
    }

    private static void ValidateDefinitionLength(
        string? definition,
        long? sourceBytes,
        string description)
    {
        if ((definition is null) != (sourceBytes is null))
        {
            throw new SqlServerMigrationException(
                $"SQL Server returned inconsistent {description}-definition metadata.");
        }
        if (definition is not null &&
            sourceBytes != checked(definition.Length * 2L))
        {
            throw new SqlServerMigrationException(
                $"SQL Server returned inconsistent {description}-definition length metadata.");
        }
    }

    private static MetadataVisibility GetMetadataVisibility(SqlServerCatalogSnapshot snapshot)
    {
        SqlServerDatabaseMetadata database = snapshot.Database;
        if (database.IsSysAdmin == true)
            return MetadataVisibility.Complete;

        if (database.HasViewDefinition == false ||
            database.HasSelectSqlExpressionDependencies == false ||
            snapshot.Instance.ProductMajorVersion >= 16 &&
            database.HasViewSecurityDefinition == false ||
            snapshot.Schemas.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Tables.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Views.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Triggers.Any(static item => item.HasViewDefinition == false) ||
            snapshot.Routines.Any(static item => item.HasViewDefinition == false) ||
            HasRelevantMetadataDeny(snapshot.PermissionAuditBefore) ||
            HasRelevantMetadataDeny(snapshot.PermissionAuditAfter))
        {
            return MetadataVisibility.Incomplete;
        }

        bool hasCompletePositiveProof =
            database.HasViewDefinition == true &&
            database.HasSelectSqlExpressionDependencies == true &&
            (snapshot.Instance.ProductMajorVersion < 16 ||
             database.HasViewSecurityDefinition == true) &&
            snapshot.PermissionAuditBefore.Attempted &&
            snapshot.PermissionAuditAfter.Attempted &&
            PermissionAuditsEqual(
                snapshot.PermissionAuditBefore,
                snapshot.PermissionAuditAfter) &&
            snapshot.ExpressionDependencyAudit.Attempted &&
            snapshot.Schemas.All(static item => item.HasViewDefinition == true) &&
            snapshot.Tables.All(static item =>
                item.HasViewDefinition == true &&
                item.IsRowLevelSecurityInventoryComplete) &&
            snapshot.Views.All(static item => item.HasViewDefinition == true) &&
            snapshot.Triggers.All(static item => item.HasViewDefinition == true) &&
            snapshot.Routines.All(static item => item.HasViewDefinition == true);
        if (hasCompletePositiveProof)
            return MetadataVisibility.Complete;

        return MetadataVisibility.Unknown;
    }

    internal static string ComputeSnapshotDigest(SqlServerCatalogSnapshot snapshot)
    {
        IEnumerable<string?> Fields()
        {
            yield return snapshot.Instance.ProductVersion;
            yield return Invariant(snapshot.Instance.ProductMajorVersion);
            yield return snapshot.Instance.ProductLevel;
            yield return snapshot.Instance.Edition;
            yield return Invariant(snapshot.Instance.EngineEdition);
            yield return Invariant(snapshot.Database.DatabaseId);
            yield return snapshot.Database.Name;
            yield return Invariant(snapshot.Database.CompatibilityLevel);
            yield return snapshot.Database.Collation;
            yield return Boolean(snapshot.Database.IsReadCommittedSnapshotOn);
            yield return snapshot.Database.SnapshotIsolationState;
            yield return Boolean(snapshot.Database.IsAutoCreateStatsOn);
            yield return Boolean(snapshot.Database.IsAutoUpdateStatsOn);
            yield return Boolean(snapshot.Database.IsAnsiNullDefaultOn);
            yield return Boolean(snapshot.Database.IsQuotedIdentifierOn);
            yield return Boolean(snapshot.Database.IsParameterizationForced);
            yield return snapshot.Database.Containment;
            yield return Boolean(snapshot.Database.IsTrustworthyOn);
            yield return NullableBoolean(snapshot.Database.IsSysAdmin);
            yield return NullableBoolean(snapshot.Database.IsDbOwner);
            yield return NullableBoolean(snapshot.Database.HasControl);
            yield return NullableBoolean(snapshot.Database.HasViewDefinition);
            yield return NullableBoolean(snapshot.Database.HasViewSecurityDefinition);
            yield return NullableBoolean(snapshot.Database.HasSelectSqlExpressionDependencies);

            foreach (SqlServerSchemaMetadata schema in snapshot.Schemas
                         .OrderBy(static item => item.SchemaId)
                         .ThenBy(static item => item.Name, StringComparer.Ordinal))
            {
                yield return "schema";
                yield return Invariant(schema.SchemaId);
                yield return schema.Name;
                yield return NullableBoolean(schema.HasViewDefinition);
            }
            foreach (SqlServerTableMetadata table in snapshot.Tables
                         .OrderBy(static item => item.ObjectId))
            {
                yield return "table";
                yield return Invariant(table.ObjectId);
                yield return Invariant(table.SchemaId);
                yield return table.Name;
                yield return Boolean(table.IsMemoryOptimized);
                yield return table.Durability;
                yield return Boolean(table.IsFileTable);
                yield return table.TemporalType;
                yield return Boolean(table.IsNode);
                yield return Boolean(table.IsEdge);
                yield return NullableBoolean(table.HasViewDefinition);
                yield return Invariant(table.LobDataSpaceId);
                yield return Invariant(table.FileStreamDataSpaceId);
                yield return Boolean(
                    table.HasEnabledRowLevelSecurityFilter);
                yield return Boolean(
                    table.IsRowLevelSecurityInventoryComplete);
            }
            foreach (SqlServerColumnMetadata column in snapshot.Columns
                         .OrderBy(static item => item.ObjectId)
                         .ThenBy(static item => item.ColumnId))
            {
                yield return "column";
                yield return Invariant(column.ObjectId);
                yield return Invariant(column.ColumnId);
                yield return column.Name;
                yield return column.TypeSchema;
                yield return column.TypeName;
                yield return column.SystemTypeName;
                yield return Invariant(column.MaxLength);
                yield return Invariant(column.Precision);
                yield return Invariant(column.Scale);
                yield return column.Collation;
                yield return Boolean(column.IsNullable);
                yield return Boolean(column.IsSparse);
                yield return Boolean(column.IsColumnSet);
                yield return Boolean(column.IsHidden);
                yield return Boolean(column.IsComputed);
                yield return Boolean(column.IsFileStream);
                yield return Boolean(column.IsMasked);
                yield return column.EncryptionType;
                yield return Invariant(column.XmlCollectionId);
                yield return column.GeneratedAlwaysType;
                yield return Boolean(column.HasDefault);
                yield return column.DefaultConstraintName;
                yield return column.DefaultDefinitionBytes is null
                    ? null
                    : Invariant(column.DefaultDefinitionBytes.Value);
                yield return column.DefaultDefinition;
                yield return column.ComputedDefinitionBytes is null
                    ? null
                    : Invariant(column.ComputedDefinitionBytes.Value);
                yield return column.ComputedDefinition;
                yield return Boolean(column.IsPersisted);
                yield return Boolean(column.IsIdentity);
                yield return column.IdentitySeed;
                yield return column.IdentityIncrement;
                yield return Boolean(column.IdentityNotForReplication);
            }

            foreach (string? field in RelationalSnapshotFields(snapshot))
                yield return field;
            foreach (string? field in ProgrammableSnapshotFields(snapshot))
                yield return field;
            foreach (string? field in PhysicalSnapshotFields(snapshot))
                yield return field;
            foreach (string? field in IndexSubtypeSnapshotFields(snapshot))
                yield return field;
        }

        return SqlServerStableDigest.Sequence(
            "csharpdb-sqlserver-snapshot/v5",
            Fields());
    }

    private static SqlServerScriptDomDefinitionAnalysis? GetScriptDomAnalysis(
        SqlServerScriptDomAnalysisSnapshot snapshot,
        SqlServerScriptDomDefinitionKind kind,
        int objectId,
        int subObjectId,
        bool definitionAvailable)
    {
        var key = new SqlServerScriptDomDefinitionKey(
            kind,
            objectId,
            subObjectId);
        bool found = snapshot.TryGet(
            key,
            out SqlServerScriptDomDefinitionAnalysis? analysis);
        if (found != definitionAvailable)
        {
            throw new SqlServerMigrationException(
                "ScriptDom analysis does not match the available SQL Server definitions.");
        }
        if (!found)
            return null;

        ValidateScriptDomAnalysis(analysis!, key);
        return analysis;
    }

    private static void ValidateScriptDomAnalysis(
        SqlServerScriptDomDefinitionAnalysis analysis,
        SqlServerScriptDomDefinitionKey expectedKey)
    {
        if (analysis.Key != expectedKey ||
            !Enum.IsDefined(analysis.Status) ||
            !Enum.IsDefined(analysis.ExpectedRootKind) ||
            !Enum.IsDefined(analysis.RootKind) ||
            string.IsNullOrWhiteSpace(analysis.Grammar) ||
            analysis.Grammar.Length > 32 ||
            analysis.Grammar.Any(static character =>
                !char.IsAsciiLetterOrDigit(character) && character != '-') ||
            analysis.TokenCount < 0 ||
            analysis.NodeCount < 0 ||
            analysis.NestingDepth < 0 ||
            analysis.StatementCount < 0 ||
            analysis.ParseErrorCount < 0 ||
            analysis.ExpectedRootKind == SqlServerScriptDomRootKind.None ||
            !IsSha256(analysis.SourceDigest) ||
            !IsSha256(analysis.AnalysisDigest))
        {
            throw new SqlServerMigrationException(
                "ScriptDom returned invalid bounded analysis metadata.");
        }

        bool hasFirstError =
            analysis.FirstErrorNumber is not null ||
            analysis.FirstErrorOffset is not null ||
            analysis.FirstErrorLine is not null ||
            analysis.FirstErrorColumn is not null;
        bool hasCompleteFirstError =
            analysis.FirstErrorNumber is not null &&
            analysis.FirstErrorOffset is >= 0 &&
            analysis.FirstErrorLine is >= 1 &&
            analysis.FirstErrorColumn is >= 1;
        if (hasFirstError != hasCompleteFirstError ||
            (analysis.ParseErrorCount == 0) == hasFirstError ||
            analysis.Status == SqlServerScriptDomStatus.Parsed &&
            analysis.RootKind != analysis.ExpectedRootKind)
        {
            throw new SqlServerMigrationException(
                "ScriptDom returned inconsistent analysis evidence.");
        }
    }

    private static void AddScriptDomFacets(
        ICollection<MigrationCatalogFacet> facets,
        string statusFacet,
        string prefix,
        SqlServerScriptDomDefinitionAnalysis? analysis)
    {
        facets.Add(Facet(
            statusFacet,
            analysis is null
                ? "not-attempted"
                : ScriptDomStatusToken(analysis.Status)));
        if (analysis is null)
            return;

        facets.Add(Facet($"{prefix}Grammar", analysis.Grammar));
        facets.Add(Facet(
            $"{prefix}QuotedIdentifiers",
            Boolean(analysis.QuotedIdentifiers)));
        facets.Add(Facet(
            $"{prefix}TokenCount",
            Invariant(analysis.TokenCount)));
        facets.Add(Facet(
            $"{prefix}NodeCount",
            Invariant(analysis.NodeCount)));
        facets.Add(Facet(
            $"{prefix}NestingDepth",
            Invariant(analysis.NestingDepth)));
        facets.Add(Facet(
            $"{prefix}StatementCount",
            Invariant(analysis.StatementCount)));
        facets.Add(Facet(
            $"{prefix}ParseErrorCount",
            Invariant(analysis.ParseErrorCount)));
        facets.Add(Facet(
            $"{prefix}ExpectedRootKind",
            ScriptDomRootToken(analysis.ExpectedRootKind)));
        facets.Add(Facet(
            $"{prefix}RootKind",
            ScriptDomRootToken(analysis.RootKind)));
        if (analysis.FirstErrorNumber is int firstErrorNumber)
        {
            facets.Add(Facet(
                $"{prefix}FirstErrorNumber",
                Invariant(firstErrorNumber)));
        }
        facets.Add(Facet($"{prefix}SourceDigest", analysis.SourceDigest));
        facets.Add(Facet($"{prefix}AnalysisDigest", analysis.AnalysisDigest));
    }

    private static void AddScriptDomDiagnostic(
        string objectId,
        string definitionDescription,
        SqlServerScriptDomDefinitionAnalysis analysis,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        (
            string RuleId,
            string Summary,
            string Explanation,
            string Remediation) shape = analysis.Status switch
            {
                SqlServerScriptDomStatus.Parsed => (
                    "MIG-SQLSERVER-TSQL-PARSED-NOT-LOWERED-001",
                    $"The SQL Server {definitionDescription} parsed but is not target-ready.",
                    "ScriptDom established bounded source syntax and the expected definition root only. Name binding, semantic equivalence, target lowering, scratch execution, and differential validation have not been established.",
                    "Bind the definition to the normalized catalog, lower a reviewed target representation, and validate it against a scratch CSharpDB database."),
                SqlServerScriptDomStatus.RootMismatch => (
                    "MIG-SQLSERVER-TSQL-ROOT-MISMATCH-001",
                    $"The SQL Server {definitionDescription} has an unexpected syntax root or identity.",
                    "The bounded definition parsed, but its fixed root category, object type, or declared schema/name identity does not match the catalog role.",
                    "Restore a definition whose declared kind and identity match the catalog metadata, then inspect again."),
                SqlServerScriptDomStatus.DialectUnqualified => (
                    "MIG-SQLSERVER-TSQL-DIALECT-UNQUALIFIED-001",
                    $"The SQL Server {definitionDescription} dialect is not qualified.",
                    "No qualified ScriptDom grammar corresponds to the database compatibility level captured by this inspection.",
                    "Use a qualified compatibility level or add and test an explicit parser lane."),
                SqlServerScriptDomStatus.LexerError or
                    SqlServerScriptDomStatus.ParserError => (
                        "MIG-SQLSERVER-TSQL-SYNTAX-001",
                        $"The SQL Server {definitionDescription} did not parse.",
                        "Bounded ScriptDom analysis reported a lexical or syntax error. Only its fixed number and source position are retained; parser messages and source tokens are not persisted.",
                        "Correct the source definition and inspect again."),
                SqlServerScriptDomStatus.TokenLimitExceeded or
                    SqlServerScriptDomStatus.NestingLimitExceeded or
                    SqlServerScriptDomStatus.NodeLimitExceeded or
                    SqlServerScriptDomStatus.ParseErrorLimitExceeded or
                    SqlServerScriptDomStatus.StatementLimitExceeded or
                    SqlServerScriptDomStatus.InputLimitExceeded => (
                        "MIG-SQLSERVER-TSQL-ANALYSIS-LIMIT-001",
                        $"The SQL Server {definitionDescription} exceeded a fixed analysis limit.",
                        "Syntax analysis stopped at a fixed input, token, nesting, AST-node, statement, or parse-error ceiling without truncating the durable result into a compatibility claim.",
                        "Simplify and split the definition, then inspect it again."),
                SqlServerScriptDomStatus.AnalyzerFailure => (
                    "MIG-SQLSERVER-TSQL-ANALYZER-FAILURE-001",
                    $"The SQL Server {definitionDescription} could not be analyzed safely.",
                    "The bounded analyzer failed without retaining provider exception text, source tokens, or parser messages.",
                    "Review the definition and analyzer support before retrying."),
                _ => throw new SqlServerMigrationException(
                    "ScriptDom returned an unknown analysis status."),
            };

        diagnostics.Add(Diagnostic(
            objectId,
            shape.RuleId,
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            shape.Summary,
            shape.Explanation,
            shape.Remediation,
            canOverride: false,
            occurrenceKey: string.Concat(
                ScriptDomDefinitionKindToken(analysis.Key.Kind),
                ":",
                analysis.AnalysisDigest),
            sourceSpan: new MigrationSourceSpan
            {
                SourceId = analysis.SourceDigest,
                Start = analysis.FirstErrorOffset,
                Line = analysis.FirstErrorLine,
                Column = analysis.FirstErrorColumn,
            }));
    }

    private static string ScriptDomStatusToken(
        SqlServerScriptDomStatus status) => status switch
        {
            SqlServerScriptDomStatus.Parsed => "parsed",
            SqlServerScriptDomStatus.RootMismatch => "root-mismatch",
            SqlServerScriptDomStatus.DialectUnqualified => "dialect-unqualified",
            SqlServerScriptDomStatus.LexerError => "lexer-error",
            SqlServerScriptDomStatus.ParserError => "parser-error",
            SqlServerScriptDomStatus.TokenLimitExceeded => "token-limit-exceeded",
            SqlServerScriptDomStatus.NestingLimitExceeded => "nesting-limit-exceeded",
            SqlServerScriptDomStatus.NodeLimitExceeded => "node-limit-exceeded",
            SqlServerScriptDomStatus.ParseErrorLimitExceeded => "parse-error-limit-exceeded",
            SqlServerScriptDomStatus.StatementLimitExceeded => "statement-limit-exceeded",
            SqlServerScriptDomStatus.InputLimitExceeded => "input-limit-exceeded",
            SqlServerScriptDomStatus.AnalyzerFailure => "analyzer-failure",
            _ => throw new SqlServerMigrationException(
                "ScriptDom returned an unknown analysis status."),
        };

    private static string ScriptDomRootToken(
        SqlServerScriptDomRootKind root) => root switch
        {
            SqlServerScriptDomRootKind.None => "none",
            SqlServerScriptDomRootKind.ScalarExpression => "scalar-expression",
            SqlServerScriptDomRootKind.BooleanExpression => "boolean-expression",
            SqlServerScriptDomRootKind.View => "view",
            SqlServerScriptDomRootKind.Trigger => "trigger",
            SqlServerScriptDomRootKind.Procedure => "procedure",
            SqlServerScriptDomRootKind.ScalarFunction => "scalar-function",
            SqlServerScriptDomRootKind.TableValuedFunction => "table-valued-function",
            SqlServerScriptDomRootKind.StandaloneRule => "standalone-rule",
            SqlServerScriptDomRootKind.StandaloneDefault => "standalone-default",
            _ => throw new SqlServerMigrationException(
                "ScriptDom returned an unknown root kind."),
        };

    private static string ScriptDomDefinitionKindToken(
        SqlServerScriptDomDefinitionKind kind) => kind switch
        {
            SqlServerScriptDomDefinitionKind.Module => "module",
            SqlServerScriptDomDefinitionKind.DefaultExpression => "default-expression",
            SqlServerScriptDomDefinitionKind.ComputedExpression => "computed-expression",
            SqlServerScriptDomDefinitionKind.CheckPredicate => "check-predicate",
            SqlServerScriptDomDefinitionKind.IndexFilterPredicate => "index-filter-predicate",
            _ => throw new SqlServerMigrationException(
                "ScriptDom returned an unknown definition kind."),
        };

    private static MigrationDiagnostic Diagnostic(
        string objectId,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        string? remediation,
        bool canOverride,
        string? occurrenceKey = null,
        MigrationSourceSpan? sourceSpan = null) =>
        new()
        {
            DiagnosticId = string.Concat(
                "sqlserver:diag:",
                ruleId.ToLowerInvariant(),
                ":",
                SqlServerStableDigest.Text(
                    "csharpdb-sqlserver-diagnostic/v1",
                    ruleId,
                    objectId,
                    occurrenceKey)[..16]),
            RuleId = ruleId,
            Severity = severity,
            Status = status,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = summary,
            Explanation = explanation,
            ObjectId = objectId,
            SourceSpan = sourceSpan,
            Remediation = remediation,
            CanOverride = canOverride,
        };

    private static MigrationCatalogFacet Facet(string name, string? value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static string ObjectId(string kind, params string[] names) =>
        string.Concat(
            "sqlserver:",
            kind,
            ":",
            SqlServerStableDigest.Text(
                "csharpdb-sqlserver-object-id/v1",
                names));

    private static string Boolean(bool value) => value ? "true" : "false";

    private static string NullableBoolean(bool? value) =>
        value is null ? "unknown" : Boolean(value.Value);

    private static bool IsSha256(string value) =>
        value.Length == 71 &&
        value.StartsWith("sha256:", StringComparison.Ordinal) &&
        value.AsSpan(7).IndexOfAnyExcept(
            "0123456789abcdef".AsSpan()) < 0;

    private static string Invariant<T>(T value)
        where T : IFormattable =>
        value.ToString(null, CultureInfo.InvariantCulture);

    private static SqlServerMigrationException LimitExceeded(
        string category) =>
        new(
            $"SQL Server inspection exceeded the fixed {category} limit.",
            SqlServerMigrationErrorCode.InspectionLimit);

    private enum MetadataVisibility
    {
        Complete,
        Incomplete,
        Unknown,
    }

    private sealed class MetadataBudget
    {
        private readonly SqlServerInspectionLimits limits;
        private long metadataBytes;
        private long expressionStorageBytes;

        public MetadataBudget(SqlServerInspectionLimits limits)
        {
            this.limits = limits;
        }

        public void Add(string? value, bool isName = false)
        {
            if (value is null)
                return;
            int bytes;
            try
            {
                bytes = s_strictUtf8.GetByteCount(value);
            }
            catch (EncoderFallbackException)
            {
                throw new SqlServerMigrationException(
                    "SQL Server metadata contains invalid Unicode.");
            }
            if (isName && bytes > limits.MaxNameBytes)
                throw LimitExceeded("identifier byte");
            metadataBytes = checked(metadataBytes + bytes);
            if (metadataBytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }

        public void AddExpression(string? value)
        {
            if (value is null)
                return;
            int before = checked((int)Math.Min(metadataBytes, int.MaxValue));
            Add(value);
            int bytes = checked((int)(metadataBytes - before));
            if (bytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }

        public void ReserveExpression(long? sourceBytes)
        {
            if (sourceBytes is null)
                return;
            if (sourceBytes < 0 || sourceBytes > limits.MaxExpressionBytes)
                throw LimitExceeded("expression byte");
            expressionStorageBytes = checked(expressionStorageBytes + sourceBytes.Value);
            if (expressionStorageBytes > limits.MaxExpressionBytesTotal)
                throw LimitExceeded("aggregate expression byte");
        }
    }
}
