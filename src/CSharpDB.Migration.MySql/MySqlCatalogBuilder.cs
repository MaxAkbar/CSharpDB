using System.Globalization;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.MySql;

internal static partial class MySqlCatalogBuilder
{
    public const string CatalogContract = "csharpdb-mysql-catalog/v2";

    private static readonly UTF8Encoding s_strictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static MigrationCatalog Build(
        MySqlCatalogSnapshot snapshot,
        MigrationInspectionRequest request,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();
        ValidateRequest(request);
        ValidateSnapshot(snapshot, limits, cancellationToken);

        var objects = new List<MigrationCatalogObject>(
            2 +
            snapshot.Tables.Count +
            snapshot.Columns.Count +
            RelationalObjectCapacity(snapshot));
        var diagnostics = new List<MigrationDiagnostic>();

        string databaseId = ObjectId("database", snapshot.Database.Name);
        string namespaceId = ObjectId("namespace", snapshot.Database.Name);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = databaseId,
            Kind = MigrationObjectKind.Database,
            SourceName = snapshot.Database.Name,
            Facets =
            [
                Facet("mysqlCatalogContract", CatalogContract),
                Facet("mysqlServerVersion", snapshot.Server.Version),
                Facet("mysqlVersionComment", snapshot.Server.VersionComment),
                Facet("mysqlCharacterSetServer", snapshot.Server.CharacterSetServer),
                Facet("mysqlCollationServer", snapshot.Server.CollationServer),
                Facet("mysqlSystemTimeZone", snapshot.Server.SystemTimeZone),
                Facet(
                    "mysqlLowerCaseTableNames",
                    Invariant(snapshot.Server.LowerCaseTableNames)),
                Facet(
                    "mysqlShowGeneratedInvisiblePrimaryKey",
                    NullableBoolean(
                        snapshot.Server.ShowGeneratedInvisiblePrimaryKey)),
                Facet("mysqlSqlMode", snapshot.Session.SqlMode),
                Facet(
                    "mysqlSqlQuoteShowCreate",
                    NullableBoolean(snapshot.Session.SqlQuoteShowCreate)),
                Facet(
                    "mysqlCharacterSetConnection",
                    snapshot.Session.CharacterSetConnection),
                Facet(
                    "mysqlCollationConnection",
                    snapshot.Session.CollationConnection),
                Facet("mysqlSessionTimeZone", snapshot.Session.TimeZone),
                Facet(
                    "mysqlDefaultCharacterSet",
                    snapshot.Database.DefaultCharacterSet),
                Facet(
                    "mysqlDefaultCollation",
                    snapshot.Database.DefaultCollation),
                Facet("mysqlBaseTableCount", Invariant(snapshot.Tables.Count)),
                Facet("mysqlColumnCount", Invariant(snapshot.Columns.Count)),
                Facet(
                    "mysqlTableDefinitionCount",
                    Invariant(snapshot.TableDefinitions.Count)),
                Facet("mysqlKeyCount", Invariant(snapshot.Keys.Count)),
                Facet(
                    "mysqlKeyColumnCount",
                    Invariant(snapshot.KeyColumns.Count)),
                Facet(
                    "mysqlForeignKeyCount",
                    Invariant(snapshot.ForeignKeys.Count)),
                Facet(
                    "mysqlForeignKeyColumnCount",
                    Invariant(snapshot.ForeignKeyColumns.Count)),
                Facet("mysqlCheckCount", Invariant(snapshot.Checks.Count)),
                Facet("mysqlIndexCount", Invariant(snapshot.Indexes.Count)),
                Facet(
                    "mysqlIndexPartCount",
                    Invariant(snapshot.IndexParts.Count)),
                Facet("mysqlViewCount", Invariant(snapshot.Database.ViewCount)),
            ],
        });
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = namespaceId,
            Kind = MigrationObjectKind.Namespace,
            ParentObjectId = databaseId,
            SourceNamespace = snapshot.Database.Name,
            SourceName = snapshot.Database.Name,
            Facets =
            [
                Facet("isDefault", "true"),
                Facet(
                    "mysqlDefaultCharacterSet",
                    snapshot.Database.DefaultCharacterSet),
                Facet(
                    "mysqlDefaultCollation",
                    snapshot.Database.DefaultCollation),
            ],
        });

        AddDatabaseDiagnostics(snapshot, databaseId, diagnostics);

        var tablesByIdentity =
            new Dictionary<string, (MySqlTableMetadata Metadata, string Id)>(
                StringComparer.Ordinal);
        Dictionary<string, MySqlTableDefinitionMetadata> definitionsByTable =
            snapshot.TableDefinitions.ToDictionary(
                item => TableIdentity(
                    item.SchemaName,
                    item.TableName,
                    snapshot.Server.LowerCaseTableNames),
                StringComparer.Ordinal);
        foreach (MySqlTableMetadata table in OrderedTables(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableId = ObjectId(
                "table",
                table.SchemaName,
                table.Name);
            tablesByIdentity.Add(
                TableIdentity(
                    table.SchemaName,
                    table.Name,
                    snapshot.Server.LowerCaseTableNames),
                (table, tableId));
            MySqlTableDefinitionMetadata definition = definitionsByTable[
                TableIdentity(
                    table.SchemaName,
                    table.Name,
                    snapshot.Server.LowerCaseTableNames)];
            var tableFacets = new List<MigrationCatalogFacet>
            {
                Facet("mysqlTableType", table.TableType),
                Facet("mysqlEngine", table.Engine),
                Facet("mysqlTableCollation", table.TableCollation),
                Facet("mysqlCreateOptions", table.CreateOptions),
                Facet("mysqlPartitioned", Boolean(table.IsPartitioned)),
            };
            AddDefinitionDigestFacets(
                tableFacets,
                "mysqlShowCreate",
                "csharpdb-mysql-show-create-table/v1",
                definition.DefinitionBytes,
                definition.Definition);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = tableId,
                Kind = MigrationObjectKind.Table,
                ParentObjectId = namespaceId,
                SourceNamespace = table.SchemaName,
                SourceName = table.Name,
                Facets = tableFacets.AsReadOnly(),
            });
            AddTableDiagnostics(table, tableId, diagnostics);
        }

        var columnsByIdentity =
            new Dictionary<string, (MySqlColumnMetadata Metadata, string Id)>(
                StringComparer.Ordinal);
        foreach (MySqlColumnMetadata column in OrderedColumns(snapshot))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string tableIdentity = TableIdentity(
                column.SchemaName,
                column.TableName,
                snapshot.Server.LowerCaseTableNames);
            (MySqlTableMetadata table, string tableId) =
                tablesByIdentity[tableIdentity];
            string columnId = ObjectId(
                "column",
                table.SchemaName,
                table.Name,
                column.Name);
            columnsByIdentity.Add(
                ColumnIdentity(
                    column.SchemaName,
                    column.TableName,
                    column.Name,
                    snapshot.Server.LowerCaseTableNames),
                (column, columnId));
            string logicalType = LogicalType(column);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("logicalType", logicalType),
                Facet("nullable", Boolean(column.IsNullable)),
                Facet("identity", Boolean(column.IsAutoIncrement)),
                Facet("mysqlOrdinalPosition", Invariant(column.OrdinalPosition)),
                Facet("mysqlDataType", column.DataType),
                Facet(
                    "mysqlColumnTypeBytes",
                    Invariant(column.ColumnTypeBytes)),
                Facet(
                    "mysqlColumnTypeDigest",
                    "sha256:" + MySqlStableDigest.Text(
                        "csharpdb-mysql-column-type/v1",
                        column.ColumnType)),
                Facet("mysqlUnsigned", Boolean(column.IsUnsigned)),
                Facet("mysqlZerofill", Boolean(column.IsZerofill)),
                Facet("mysqlTinyIntOne", Boolean(column.IsTinyIntOne)),
                Facet("mysqlAutoIncrement", Boolean(column.IsAutoIncrement)),
                Facet("mysqlCharacterSet", column.CharacterSetName),
                Facet("mysqlCollation", column.CollationName),
                Facet("mysqlGenerated", Boolean(column.IsGenerated)),
                Facet("mysqlGenerationKind", column.GenerationKind),
                Facet("mysqlInvisible", Boolean(column.IsInvisible)),
            };
            AddLogicalFacets(facets, column);
            AddGenerationFacets(facets, column);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = columnId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = tableId,
                SourceNamespace = column.SchemaName,
                SourceName = column.Name,
                NativeType = FormatNativeType(column),
                Facets = facets,
            });
            AddColumnDiagnostics(
                column,
                columnId,
                logicalType,
                diagnostics);
        }

        AddRelationalObjects(
            snapshot,
            tablesByIdentity,
            columnsByIdentity,
            objects,
            diagnostics,
            cancellationToken);

        string fingerprint = "sha256:" + ComputeSnapshotDigest(snapshot);
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = request.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.MySql,
                Identity = "mysql-database:" + MySqlStableDigest.Text(
                    "csharpdb-mysql-source-identity/v1",
                    snapshot.EndpointDigest,
                    snapshot.Database.Name),
                Fingerprint = fingerprint,
                ProviderVersion = snapshot.ProviderVersion,
                SourceVersion = snapshot.Server.Version,
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.BestEffort,
                    Description =
                        "One non-pooled MySQL connection executed fixed server-variable and INFORMATION_SCHEMA catalog queries; concurrent DDL is not excluded.",
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

    private static void ValidateRequest(MigrationInspectionRequest request)
    {
        if (!string.Equals(
                request.TargetCSharpDbVersion,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"The MySQL analyzer targets CSharpDB {CSharpDbCapabilityCatalogLoader.CurrentTargetVersion}.");
        }
        if (request.ProfileSampleSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(request),
                "Profile sample size must be positive.");
        }
        if (request.IncludeProfile)
        {
            throw new NotSupportedException(
                "The Phase 7B MySQL checkpoint performs schema analysis only; data profiling is not supported.");
        }
    }

    private static void ValidateSnapshot(
        MySqlCatalogSnapshot snapshot,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        if (snapshot.Tables.Count > limits.MaxTables)
            throw LimitExceeded("table count");
        if (snapshot.Columns.Count > limits.MaxColumns)
            throw LimitExceeded("column count");
        ValidateRelationalCounts(snapshot, limits);
        if (snapshot.Database.ViewCount < 0 ||
            snapshot.Database.ViewCount > limits.MaxViews)
        {
            throw LimitExceeded("view count");
        }
        if (!IsSha256(snapshot.EndpointDigest))
        {
            throw new MySqlMigrationException(
                "The MySQL endpoint digest is invalid.");
        }
        if (snapshot.Server.LowerCaseTableNames is < 0 or > 2)
        {
            throw new MySqlMigrationException(
                "MySQL returned an invalid lower_case_table_names value.");
        }

        var budget = new MetadataBudget(limits);
        budget.Add(snapshot.EndpointDigest);
        budget.AddRequired(snapshot.ProviderVersion);
        budget.AddRequired(snapshot.Server.Version);
        budget.AddRequired(snapshot.Server.VersionComment);
        budget.AddRequired(snapshot.Server.CharacterSetServer);
        budget.AddRequired(snapshot.Server.CollationServer);
        budget.AddRequired(snapshot.Server.SystemTimeZone);
        budget.Add(snapshot.Session.SqlMode);
        budget.AddRequired(snapshot.Session.CharacterSetConnection);
        budget.AddRequired(snapshot.Session.CollationConnection);
        budget.AddRequired(snapshot.Session.TimeZone);
        budget.AddRequired(snapshot.Database.Name, isName: true);
        budget.AddRequired(snapshot.Database.DefaultCharacterSet);
        budget.AddRequired(snapshot.Database.DefaultCollation);

        int lowerCaseTableNames = snapshot.Server.LowerCaseTableNames;
        var tableIdentities = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlTableMetadata table in snapshot.Tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(table.SchemaName, isName: true);
            budget.AddRequired(table.Name, isName: true);
            budget.AddRequired(table.TableType);
            budget.Add(table.Engine);
            budget.Add(table.TableCollation);
            budget.Add(table.CreateOptions);
            if (!DatabaseNamesEqual(
                    table.SchemaName,
                    snapshot.Database.Name,
                    lowerCaseTableNames) ||
                !string.Equals(
                    table.TableType,
                    "BASE TABLE",
                    StringComparison.Ordinal) ||
                !tableIdentities.Add(
                    TableIdentity(
                        table.SchemaName,
                        table.Name,
                        lowerCaseTableNames)))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or out-of-scope table metadata");
            }
        }

        var columnOrdinals = new HashSet<string>(StringComparer.Ordinal);
        var columnNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlColumnMetadata column in snapshot.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddRequired(column.SchemaName, isName: true);
            budget.AddRequired(column.TableName, isName: true);
            budget.AddRequired(column.Name, isName: true);
            budget.AddRequired(column.DataType);
            budget.AddBoundedText(
                column.ColumnType,
                column.ColumnTypeBytes,
                limits.MaxColumnTypeBytes,
                "column type byte");
            budget.Add(column.CharacterSetName);
            budget.Add(column.CollationName);
            budget.AddRequired(column.GenerationKind);

            string tableIdentity = TableIdentity(
                column.SchemaName,
                column.TableName,
                lowerCaseTableNames);
            string ordinalIdentity = string.Concat(
                tableIdentity,
                "\0",
                Invariant(column.OrdinalPosition));
            string nameIdentity = string.Concat(
                tableIdentity,
                "\0",
                column.Name.ToUpperInvariant());
            if (!tableIdentities.Contains(tableIdentity) ||
                column.OrdinalPosition <= 0 ||
                !columnOrdinals.Add(ordinalIdentity) ||
                !columnNames.Add(nameIdentity))
            {
                throw InvalidSnapshot(
                    "duplicate, invalid, or unowned column metadata");
            }
            ValidateNonNegative(column.CharacterMaximumLength);
            ValidateNonNegative(column.NumericPrecision);
            ValidateNonNegative(column.NumericScale);
            ValidateNonNegative(column.DateTimePrecision);
            if (column.NumericPrecision is int precision &&
                column.NumericScale is int scale &&
                scale > precision)
            {
                throw InvalidSnapshot("invalid numeric column metadata");
            }
            ValidateGeneration(column, budget);
        }

        ValidateRelationalSnapshot(
            snapshot,
            tableIdentities,
            columnNames,
            budget,
            cancellationToken);
    }

    private static void ValidateGeneration(
        MySqlColumnMetadata column,
        MetadataBudget budget)
    {
        if (!column.IsGenerated)
        {
            if (!string.Equals(
                    column.GenerationKind,
                    "NEVER",
                    StringComparison.Ordinal) ||
                column.GenerationExpression is not null ||
                column.GenerationExpressionBytes is not null)
            {
                throw InvalidSnapshot("inconsistent generated-column metadata");
            }
            return;
        }
        if (column.GenerationKind is not ("STORED GENERATED" or
            "VIRTUAL GENERATED"))
        {
            throw InvalidSnapshot("invalid generated-column kind");
        }
        bool hasExpression = column.GenerationExpression is not null;
        if (hasExpression != (column.GenerationExpressionBytes is not null))
        {
            throw InvalidSnapshot("inconsistent generated-column expression metadata");
        }
        if (column.GenerationExpression is not null)
        {
            budget.AddExpression(
                column.GenerationExpression,
                column.GenerationExpressionBytes!.Value);
        }
    }

    private static void AddDatabaseDiagnostics(
        MySqlCatalogSnapshot snapshot,
        string databaseId,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        string variant = ServerVariant(snapshot.Server);
        if (!string.Equals(variant, "oracle-mysql", StringComparison.Ordinal))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-SERVER-VARIANT-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The connected MySQL-compatible server variant is not qualified.",
                "The detected server is not an Oracle MySQL Community or Enterprise lane. MariaDB, Aurora MySQL, Percona, HeatWave services, TiDB, Vitess, and other compatible products require independent metadata and semantic qualification.",
                "Use an Oracle MySQL 8.0 or 8.4 source, or add a separately tested provider lane.",
                canOverride: false,
                occurrenceKey: variant));
        }
        if (!IsCandidateOracleVersion(snapshot.Server.Version))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-VERSION-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The MySQL server version is not qualified.",
                "This checkpoint recognizes only the Oracle MySQL 8.0 and 8.4 version families as candidate qualification lanes.",
                "Use Oracle MySQL 8.0 or 8.4, or add an independently tested version lane.",
                canOverride: false));
        }
        if (snapshot.Server.LowerCaseTableNames != 0)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-IDENTIFIER-CASE-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL table-name case folding requires target proof.",
                "The nonzero lower_case_table_names setting changes how table identifiers are stored or compared and has not been proven equivalent to CSharpDB naming behavior.",
                "Review case-colliding identifiers and validate the mapped target names.",
                canOverride: false,
                occurrenceKey: Invariant(snapshot.Server.LowerCaseTableNames)));
        }
        if (snapshot.Server.ShowGeneratedInvisiblePrimaryKey == false ||
            snapshot.Server.ShowGeneratedInvisiblePrimaryKey is null &&
            RequiresGeneratedInvisiblePrimaryKeyVisibilityEvidence(
                snapshot.Server))
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-GIPK-VISIBILITY-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "Generated invisible primary-key visibility is not complete.",
                "The server can synthesize generated invisible primary-key columns, but the session did not prove that INFORMATION_SCHEMA exposes them.",
                "Enable show_gipk_in_create_table_and_information_schema for the inspection session and inspect again.",
                canOverride: false));
        }
        if (snapshot.Session.SqlQuoteShowCreate != true)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-SHOW-CREATE-QUOTING-UNPROVEN-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "Stable SHOW CREATE identifier quoting was not proven.",
                "The inspection session did not prove sql_quote_show_create=ON, so retained table-definition evidence may not use a stable quoted-identifier form.",
                "Enable sql_quote_show_create for the inspection session and inspect again.",
                canOverride: false,
                occurrenceKey: NullableBoolean(
                    snapshot.Session.SqlQuoteShowCreate)));
        }
        if (snapshot.Database.ViewCount > 0)
        {
            diagnostics.Add(Diagnostic(
                databaseId,
                "MIG-MYSQL-VIEW-INVENTORY-DEFERRED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL views exist but are not inventoried by this checkpoint.",
                "The bounded table scan counted views without retaining their definitions or dependencies.",
                "Inventory and analyze each view before migration approval.",
                canOverride: false,
                occurrenceKey: Invariant(snapshot.Database.ViewCount)));
        }
        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-MYSQL-INVENTORY-PARTIAL-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "This checkpoint is an intentionally partial MySQL inventory.",
            "Phase 7B.2 inventories bounded base tables, columns, keys, foreign keys, checks, indexes, and digest-only SHOW CREATE evidence. Defaults, partitions beyond detection, views, triggers, routines, query semantics, and source rows are not yet complete.",
            "Complete the programmable-object and row-semantics checkpoints before using this catalog for migration approval.",
            canOverride: false));
        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "Complete MySQL metadata visibility has not been established.",
            "INFORMATION_SCHEMA can reflect the connected account's visibility, and this checkpoint has not yet proven completeness with a restricted read-only account.",
            "Run the deferred restricted-account qualification and reconcile the visible object inventory.",
            canOverride: false));
        diagnostics.Add(Diagnostic(
            databaseId,
            "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001",
            MigrationDiagnosticSeverity.Error,
            MigrationCompatibilityStatus.Unknown,
            "The detected MySQL lane has not passed live qualification.",
            "Offline deterministic fixtures do not prove behavior against exact Oracle MySQL 8.0 and 8.4 server tags.",
            "Run the repository-authored schema fixture against exact serviced server tags with a restricted account.",
            canOverride: false));
    }

    private static void AddTableDiagnostics(
        MySqlTableMetadata table,
        string tableId,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (!string.Equals(table.Engine, "InnoDB", StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(Diagnostic(
                tableId,
                "MIG-MYSQL-STORAGE-ENGINE-UNQUALIFIED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The table storage engine is not qualified.",
                "Only ordinary InnoDB base tables are in the intended MySQL qualification lane.",
                "Convert the table to InnoDB or provide a reviewed engine-specific migration design.",
                canOverride: false,
                occurrenceKey: table.Engine));
        }
        if (table.IsPartitioned)
        {
            diagnostics.Add(Diagnostic(
                tableId,
                "MIG-MYSQL-PARTITIONING-DEFERRED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "MySQL table partitioning is not analyzed.",
                "The table is marked as partitioned, but partition definitions, routing semantics, and target lowering are outside this checkpoint.",
                "Inventory the partition definition and choose an explicit target design.",
                canOverride: false));
        }
        if (table.TableCollation is not null)
        {
            diagnostics.Add(Diagnostic(
                tableId,
                "MIG-MYSQL-COLLATION-UNANALYZED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL table collation semantics have not been proven equivalent.",
                "The source collation is retained without treating its comparison, ordering, or uniqueness behavior as a CSharpDB collation.",
                "Select and test an explicit target collation policy.",
                canOverride: false,
                occurrenceKey: table.TableCollation));
        }
    }

    private static void AddColumnDiagnostics(
        MySqlColumnMetadata column,
        string columnId,
        string logicalType,
        ICollection<MigrationDiagnostic> diagnostics)
    {
        if (logicalType is "native" or "json")
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-TYPE-UNSUPPORTED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The MySQL column type needs provider-specific handling.",
                "ENUM, SET, JSON, spatial, and other unregistered native types are inventoried but are not lowered by this checkpoint.",
                "Choose and validate an explicit target representation.",
                canOverride: false,
                occurrenceKey: column.DataType.ToLowerInvariant()));
        }
        if (column.IsGenerated)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-GENERATED-COLUMN-DEFERRED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "The generated-column expression is not target-ready.",
                "Only a bounded digest and byte length are retained. The expression has not been parsed, bound, lowered, or scratch-executed.",
                "Materialize the value or complete bounded expression analysis and target validation.",
                canOverride: false));
        }
        if (column.IsInvisible)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-INVISIBLE-COLUMN-DEFERRED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "Invisible-column behavior is not supported.",
                "The source visibility facet affects implicit projections and cannot be silently discarded.",
                "Make the column visible or provide an explicit reviewed target design.",
                canOverride: false));
        }
        if (column.IsZerofill)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-ZEROFILL-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "ZEROFILL display semantics are not supported.",
                "The numeric value can be mapped, but MySQL ZEROFILL presentation behavior is not a CSharpDB storage type.",
                "Move presentation formatting to the application before migration.",
                canOverride: false));
        }
        if (column.IsTinyIntOne)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-TINYINT-BOOLEAN-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "TINYINT(1) boolean semantics require value proof.",
                "TINYINT(1) is only a display-width convention and can contain values outside zero and one.",
                "Profile every value or select an explicit integer mapping.",
                canOverride: false));
        }
        if (column.IsAutoIncrement)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-AUTO-INCREMENT-DEFERRED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "AUTO_INCREMENT semantics are not yet target-ready.",
                "Keys, current sequence state, increment settings, and insert behavior are outside this checkpoint.",
                "Complete key inventory and validate an explicit target identity policy.",
                canOverride: false));
        }
        if (column.CollationName is not null)
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-COLLATION-UNANALYZED-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL column collation semantics have not been proven equivalent.",
                "The source collation is retained without treating its comparison, ordering, or uniqueness behavior as a CSharpDB collation.",
                "Select and test an explicit target collation policy.",
                canOverride: false,
                occurrenceKey: column.CollationName));
        }
        if (column.DataType.Equals("bit", StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-BIT-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "MySQL BIT values do not use the generic binary mapping.",
                "BIT has a declared bit width and bit-string conversion behavior that cannot be inferred safely from a generic BLOB mapping.",
                "Choose and validate an explicit integer or fixed-bit-string representation.",
                canOverride: false));
        }
        if (column.DataType.Equals("time", StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-TIME-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unsupported,
                "MySQL TIME values do not use the generic time-of-day mapping.",
                "MySQL TIME is a signed duration with values up to 838:59:59, which is not equivalent to the shared time-of-day codec.",
                "Choose and validate an explicit duration or canonical text representation.",
                canOverride: false));
        }
        if (column.DataType.Equals(
                "timestamp",
                StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-TIMESTAMP-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL TIMESTAMP time-zone semantics require conversion proof.",
                "Session time-zone conversion and automatic initialization/update behavior are not captured completely by this checkpoint.",
                "Choose a canonical time-zone policy and validate every converted value.",
                canOverride: false));
        }
        if (column.DataType.Equals("year", StringComparison.OrdinalIgnoreCase))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-YEAR-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "MySQL YEAR semantics require target proof.",
                "YEAR has source-specific range, display, and conversion behavior beyond an ordinary integer.",
                "Choose and validate an explicit integer or text representation.",
                canOverride: false));
        }
        if (column.IsUnsigned &&
            !IsIntegerType(column.DataType))
        {
            diagnostics.Add(Diagnostic(
                columnId,
                "MIG-MYSQL-UNSIGNED-NONINTEGER-SEMANTICS-001",
                MigrationDiagnosticSeverity.Error,
                MigrationCompatibilityStatus.Unknown,
                "Unsigned non-integer semantics require target proof.",
                "The unsigned constraint on a decimal or floating-point source type is not represented by the generic logical type.",
                "Validate the range and add an explicit target constraint policy.",
                canOverride: false));
        }
    }

    private static void AddLogicalFacets(
        ICollection<MigrationCatalogFacet> facets,
        MySqlColumnMetadata column)
    {
        string type = column.DataType.ToLowerInvariant();
        if (type is "decimal" or "numeric")
        {
            if (column.NumericPrecision is int precision)
                facets.Add(Facet("precision", Invariant(precision)));
            if (column.NumericScale is int scale)
                facets.Add(Facet("scale", Invariant(scale)));
        }
        if (type is "time" or "datetime" or "timestamp")
        {
            if (column.DateTimePrecision is int precision)
            {
                facets.Add(Facet(
                    "fractionalSeconds",
                    Invariant(precision)));
            }
        }
        if (IsLengthType(type) &&
            column.CharacterMaximumLength is long maxLength)
        {
            facets.Add(Facet("maxLength", Invariant(maxLength)));
        }
    }

    private static void AddGenerationFacets(
        ICollection<MigrationCatalogFacet> facets,
        MySqlColumnMetadata column)
    {
        if (!column.IsGenerated)
            return;
        facets.Add(Facet(
            "mysqlGenerationExpressionBytes",
            column.GenerationExpressionBytes is long bytes
                ? Invariant(bytes)
                : "unknown"));
        if (column.GenerationExpression is null)
            return;
        facets.Add(Facet(
            "mysqlGenerationExpressionDigest",
            "sha256:" + MySqlStableDigest.Text(
                "csharpdb-mysql-generation-expression/v1",
                column.GenerationExpression)));
    }

    private static string LogicalType(MySqlColumnMetadata column)
    {
        string type = column.DataType.ToLowerInvariant();
        if (IsIntegerType(type))
        {
            if (column.IsTinyIntOne &&
                !column.IsUnsigned &&
                !column.IsZerofill)
            {
                return "boolean";
            }
            return column.IsUnsigned
                ? "unsignedInteger"
                : "signedInteger";
        }
        return type switch
        {
            "decimal" or "numeric" => "decimal",
            "float" or "double" or "real" => "floatingPoint",
            "char" or "varchar" or "tinytext" or "text" or "mediumtext" or
                "longtext" => "text",
            "binary" or "varbinary" or "tinyblob" or "blob" or "mediumblob" or
                "longblob" => "binary",
            "date" => "date",
            "datetime" or "timestamp" => "dateTime",
            "year" => "signedInteger",
            "json" => "json",
            _ => "native",
        };
    }

    private static string FormatNativeType(MySqlColumnMetadata column)
    {
        string type = column.DataType.ToLowerInvariant();
        string formatted = type;
        if (column.IsTinyIntOne)
        {
            formatted = "tinyint(1)";
        }
        else if (type is "decimal" or "numeric" &&
                 column.NumericPrecision is int precision &&
                 column.NumericScale is int scale)
        {
            formatted = string.Concat(
                type,
                "(",
                Invariant(precision),
                ",",
                Invariant(scale),
                ")");
        }
        else if (IsLengthType(type) &&
                 column.CharacterMaximumLength is long maxLength)
        {
            formatted = string.Concat(
                type,
                "(",
                Invariant(maxLength),
                ")");
        }
        else if (type is "time" or "datetime" or "timestamp" &&
                 column.DateTimePrecision is int dateTimePrecision)
        {
            formatted = string.Concat(
                type,
                "(",
                Invariant(dateTimePrecision),
                ")");
        }
        if (column.IsUnsigned)
            formatted += " unsigned";
        if (column.IsZerofill)
            formatted += " zerofill";
        return formatted;
    }

    private static string ComputeSnapshotDigest(MySqlCatalogSnapshot snapshot)
    {
        IEnumerable<string?> Fields()
        {
            yield return CatalogContract;
            yield return snapshot.EndpointDigest;
            yield return snapshot.ProviderVersion;
            yield return snapshot.Server.Version;
            yield return snapshot.Server.VersionComment;
            yield return snapshot.Server.CharacterSetServer;
            yield return snapshot.Server.CollationServer;
            yield return snapshot.Server.SystemTimeZone;
            yield return Invariant(snapshot.Server.LowerCaseTableNames);
            yield return NullableBoolean(
                snapshot.Server.ShowGeneratedInvisiblePrimaryKey);
            yield return snapshot.Session.SqlMode;
            yield return NullableBoolean(snapshot.Session.SqlQuoteShowCreate);
            yield return snapshot.Session.CharacterSetConnection;
            yield return snapshot.Session.CollationConnection;
            yield return snapshot.Session.TimeZone;
            yield return snapshot.Database.Name;
            yield return snapshot.Database.DefaultCharacterSet;
            yield return snapshot.Database.DefaultCollation;
            yield return Invariant(snapshot.Database.ViewCount);
            foreach (MySqlTableMetadata table in OrderedTables(snapshot))
            {
                yield return "table";
                yield return table.SchemaName;
                yield return table.Name;
                yield return table.TableType;
                yield return table.Engine;
                yield return table.TableCollation;
                yield return table.CreateOptions;
                yield return Boolean(table.IsPartitioned);
            }
            foreach (MySqlColumnMetadata column in OrderedColumns(snapshot))
            {
                yield return "column";
                yield return column.SchemaName;
                yield return column.TableName;
                yield return Invariant(column.OrdinalPosition);
                yield return column.Name;
                yield return column.DataType;
                yield return Invariant(column.ColumnTypeBytes);
                yield return MySqlStableDigest.Text(
                    "csharpdb-mysql-column-type/v1",
                    column.ColumnType);
                yield return Boolean(column.IsNullable);
                yield return column.CharacterSetName;
                yield return column.CollationName;
                yield return NullableInvariant(column.CharacterMaximumLength);
                yield return NullableInvariant(column.NumericPrecision);
                yield return NullableInvariant(column.NumericScale);
                yield return NullableInvariant(column.DateTimePrecision);
                yield return Boolean(column.IsUnsigned);
                yield return Boolean(column.IsZerofill);
                yield return Boolean(column.IsTinyIntOne);
                yield return Boolean(column.IsAutoIncrement);
                yield return Boolean(column.IsGenerated);
                yield return column.GenerationKind;
                yield return NullableInvariant(column.GenerationExpressionBytes);
                yield return column.GenerationExpression is null
                    ? null
                    : MySqlStableDigest.Text(
                        "csharpdb-mysql-generation-expression/v1",
                        column.GenerationExpression);
                yield return Boolean(column.IsInvisible);
            }
            foreach (string? field in RelationalSnapshotFields(snapshot))
                yield return field;
        }

        return MySqlStableDigest.Sequence(
            "csharpdb-mysql-snapshot/v2",
            Fields());
    }

    private static IOrderedEnumerable<MySqlTableMetadata> OrderedTables(
        MySqlCatalogSnapshot snapshot) =>
        snapshot.Tables
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static IOrderedEnumerable<MySqlColumnMetadata> OrderedColumns(
        MySqlCatalogSnapshot snapshot) =>
        snapshot.Columns
            .OrderBy(static item => item.SchemaName, StringComparer.Ordinal)
            .ThenBy(static item => item.TableName, StringComparer.Ordinal)
            .ThenBy(static item => item.OrdinalPosition)
            .ThenBy(static item => item.Name, StringComparer.Ordinal);

    private static string ServerVariant(MySqlServerMetadata server)
    {
        string identity = string.Concat(
            server.Version,
            " ",
            server.VersionComment).ToLowerInvariant();
        if (identity.Contains("mariadb", StringComparison.Ordinal))
            return "mariadb";
        if (identity.Contains("aurora", StringComparison.Ordinal))
            return "aurora-mysql";
        if (identity.Contains("percona", StringComparison.Ordinal))
            return "percona";
        if (identity.Contains("tidb", StringComparison.Ordinal))
            return "tidb";
        if (identity.Contains("vitess", StringComparison.Ordinal))
            return "vitess";
        if (identity.Contains("heatwave", StringComparison.Ordinal))
            return "heatwave";
        if (identity.Contains("mysql", StringComparison.Ordinal))
            return "oracle-mysql";
        return "unknown";
    }

    private static bool IsCandidateOracleVersion(string value)
    {
        string numeric = value.Split('-', 2)[0];
        return Version.TryParse(numeric, out Version? version) &&
               version.Major == 8 &&
               version.Minor is 0 or 4;
    }

    private static bool RequiresGeneratedInvisiblePrimaryKeyVisibilityEvidence(
        MySqlServerMetadata server)
    {
        if (!string.Equals(
                ServerVariant(server),
                "oracle-mysql",
                StringComparison.Ordinal))
        {
            return false;
        }
        string numeric = server.Version.Split('-', 2)[0];
        return Version.TryParse(numeric, out Version? version) &&
               (version.Major > 8 ||
                version.Major == 8 &&
                (version.Minor > 0 ||
                 version.Minor == 0 && version.Build >= 30));
    }

    private static bool DatabaseNamesEqual(
        string left,
        string right,
        int lowerCaseTableNames) =>
        string.Equals(
            left,
            right,
            lowerCaseTableNames == 0
                ? StringComparison.Ordinal
                : StringComparison.OrdinalIgnoreCase);

    private static string TableIdentity(
        string schema,
        string table,
        int lowerCaseTableNames)
    {
        if (lowerCaseTableNames == 0)
            return string.Concat(schema, "\0", table);
        return string.Concat(
            schema.ToUpperInvariant(),
            "\0",
            table.ToUpperInvariant());
    }

    private static bool IsIntegerType(string value) =>
        value.Equals("tinyint", StringComparison.OrdinalIgnoreCase) ||
        value.Equals("smallint", StringComparison.OrdinalIgnoreCase) ||
        value.Equals("mediumint", StringComparison.OrdinalIgnoreCase) ||
        value.Equals("int", StringComparison.OrdinalIgnoreCase) ||
        value.Equals("integer", StringComparison.OrdinalIgnoreCase) ||
        value.Equals("bigint", StringComparison.OrdinalIgnoreCase);

    private static bool IsLengthType(string value) =>
        value is "char" or "varchar" or "binary" or "varbinary";

    private static void ValidateNonNegative(long? value)
    {
        if (value < 0)
            throw InvalidSnapshot("negative column facet metadata");
    }

    private static MigrationDiagnostic Diagnostic(
        string objectId,
        string ruleId,
        MigrationDiagnosticSeverity severity,
        MigrationCompatibilityStatus status,
        string summary,
        string explanation,
        string? remediation,
        bool canOverride,
        string? occurrenceKey = null) =>
        new()
        {
            DiagnosticId = string.Concat(
                "mysql:diag:",
                ruleId.ToLowerInvariant(),
                ":",
                MySqlStableDigest.Text(
                    "csharpdb-mysql-diagnostic/v1",
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
            "mysql:",
            kind,
            ":",
            MySqlStableDigest.Text(
                "csharpdb-mysql-object-id/v1",
                names));

    private static string Boolean(bool value) => value ? "true" : "false";

    private static string NullableBoolean(bool? value) =>
        value is null ? "unknown" : Boolean(value.Value);

    private static string Invariant<T>(T value)
        where T : IFormattable =>
        value.ToString(null, CultureInfo.InvariantCulture);

    private static string? NullableInvariant<T>(T? value)
        where T : struct, IFormattable =>
        value is null ? null : Invariant(value.Value);

    private static bool IsSha256(string value) =>
        value.Length == 71 &&
        value.StartsWith("sha256:", StringComparison.Ordinal) &&
        value.AsSpan(7).IndexOfAnyExcept(
            "0123456789abcdef".AsSpan()) < 0;

    private static MySqlMigrationException LimitExceeded(string category) =>
        new($"MySQL inspection exceeded the fixed {category} limit.");

    private static MySqlMigrationException InvalidSnapshot(string category) =>
        new($"MySQL returned {category}.");

    private sealed class MetadataBudget
    {
        private long definitionBytes;
        private readonly MySqlInspectionLimits limits;
        private long expressionBytes;
        private long metadataBytes;

        public MetadataBudget(MySqlInspectionLimits limits)
        {
            this.limits = limits;
        }

        public void AddRequired(string? value, bool isName = false)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw InvalidSnapshot("empty required catalog metadata");
            Add(value, isName);
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
                throw new MySqlMigrationException(
                    "MySQL metadata contains invalid Unicode.");
            }
            if (isName && bytes > limits.MaxNameBytes)
                throw LimitExceeded("identifier byte");
            metadataBytes = checked(metadataBytes + bytes);
            if (metadataBytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }

        public void AddExpression(string value, long sourceBytes)
        {
            int before = checked((int)Math.Min(metadataBytes, int.MaxValue));
            Add(value);
            int bytes = checked((int)(metadataBytes - before));
            if (sourceBytes != bytes)
                throw InvalidSnapshot("inconsistent expression byte metadata");
            if (bytes > limits.MaxExpressionBytes)
                throw LimitExceeded("expression byte");
            expressionBytes = checked(expressionBytes + bytes);
            if (expressionBytes > limits.MaxExpressionBytesTotal)
                throw LimitExceeded("aggregate expression byte");
        }

        public void AddDefinition(string value, long sourceBytes)
        {
            long before = metadataBytes;
            Add(value);
            long bytes = checked(metadataBytes - before);
            if (sourceBytes != bytes)
                throw InvalidSnapshot("inconsistent definition byte metadata");
            if (bytes > limits.MaxDefinitionBytes)
                throw LimitExceeded("definition byte");
            definitionBytes = checked(definitionBytes + bytes);
            if (definitionBytes > limits.MaxDefinitionBytesTotal)
                throw LimitExceeded("aggregate definition byte");
        }

        public void AddBoundedText(
            string? value,
            long sourceBytes,
            int maximumBytes,
            string category)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw InvalidSnapshot("empty required catalog metadata");
            int before = checked((int)Math.Min(metadataBytes, int.MaxValue));
            Add(value);
            int bytes = checked((int)(metadataBytes - before));
            if (sourceBytes != bytes)
                throw InvalidSnapshot("inconsistent text byte metadata");
            if (bytes > maximumBytes)
                throw LimitExceeded(category);
        }
    }
}
