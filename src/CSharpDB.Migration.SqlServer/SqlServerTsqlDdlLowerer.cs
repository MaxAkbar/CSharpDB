using System.Globalization;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnDefinition =
    Microsoft.SqlServer.TransactSql.ScriptDom.ColumnDefinition;

namespace CSharpDB.Migration.SqlServer;

internal static class TsqlDdlLowerer
{
    private const int MaxSqlServerIdentifierCharacters = 128;
    private const int MaxSqlServerTableColumns = 1024;
    private const int MaxSqlServerKeyColumns = 32;

    internal static TsqlDdlLoweringResult Lower(
        IReadOnlyList<TSqlStatement> statements,
        string scriptDigest,
        CSharpDbCapabilityCatalog capabilities,
        int maximumCatalogObjects,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(statements);
        ArgumentException.ThrowIfNullOrWhiteSpace(scriptDigest);
        ArgumentNullException.ThrowIfNull(capabilities);
        cancellationToken.ThrowIfCancellationRequested();

        var contexts = statements.Select((statement, index) =>
            new StatementContext(statement, index)).ToArray();
        var tables = new Dictionary<string, TableModel>(
            StringComparer.OrdinalIgnoreCase);
        var indexStatements = new List<StatementContext>();

        foreach (StatementContext context in contexts)
        {
            cancellationToken.ThrowIfCancellationRequested();
            switch (context.Statement)
            {
                case CreateTableStatement create:
                    context.Kind = "create-table";
                    TableModel? table = ReadTable(
                        context,
                        create,
                        cancellationToken);
                    if (table is not null)
                    {
                        if (!tables.TryAdd(table.Name, table))
                        {
                            context.Reject(
                                SqlServerTsqlDdlCompatibilityAnalyzer
                                    .DuplicateObjectRuleId,
                                "The script declares a duplicate table.");
                        }
                    }
                    break;
                case CreateIndexStatement:
                    context.Kind = "create-index";
                    indexStatements.Add(context);
                    break;
                default:
                    context.Kind = "unsupported";
                    context.Reject(
                        SqlServerTsqlDdlCompatibilityAnalyzer
                            .UnsupportedStatementRuleId,
                        "The statement kind is outside the T-SQL DDL allowlist.");
                    break;
            }
        }

        foreach (TableModel table in tables.Values
                     .OrderBy(static item => item.Context.Index))
        {
            cancellationToken.ThrowIfCancellationRequested();
            ResolveTable(
                table,
                tables,
                cancellationToken);
        }
        var schemaObjectNames = new HashSet<string>(
            tables.Values.Select(static table => table.Name),
            StringComparer.OrdinalIgnoreCase);
        foreach (TableModel table in tables.Values
                     .OrderBy(static item => item.Context.Index))
        {
            foreach (string name in table.Keys
                         .Where(static key => key.HasExplicitName)
                         .Select(static key => key.SourceName)
                         .Concat(table.ForeignKeys
                             .Where(static foreignKey =>
                                 foreignKey.HasExplicitName)
                             .Select(static foreignKey =>
                                 foreignKey.SourceName)))
            {
                if (!schemaObjectNames.Add(name))
                {
                    table.Context.Reject(
                        SqlServerTsqlDdlCompatibilityAnalyzer
                            .DuplicateObjectRuleId,
                        "The script declares a duplicate schema-scoped name.");
                    break;
                }
            }
            foreach (KeyModel key in table.Keys
                         .Where(static key => key.HasExplicitName))
            {
                table.IndexNames.Add(key.SourceName);
            }
        }

        var indexes = new List<IndexModel>();
        foreach (StatementContext context in indexStatements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IndexModel? index = ReadIndex(
                context,
                (CreateIndexStatement)context.Statement,
                tables);
            if (index is not null)
                indexes.Add(index);
        }

        CSharpDbDdlCompatibilityDiagnostic[] invalidDiagnostics =
            contexts
                .Where(static context => context.Diagnostic is not null)
                .OrderBy(static context => context.Index)
                .Select((context, ordinal) =>
                    context.Diagnostic! with
                    {
                        Ordinal = ordinal,
                        DiagnosticId = string.Concat(
                            "tsql-ddl/",
                            ordinal.ToString("D6", CultureInfo.InvariantCulture),
                            "/",
                            context.Diagnostic!.RuleId),
                    })
                .ToArray();
        CSharpDbDdlCompatibilityStatement[] statementResults =
            contexts.Select(static context =>
                SqlServerTsqlDdlCompatibilityAnalyzer.Statement(
                    context.Statement,
                    context.Index,
                    context.Kind,
                    context.Diagnostic is null
                        ? MigrationCompatibilityStatus.Conditional
                        : MigrationCompatibilityStatus.Unsupported,
                    context.Diagnostic?.RuleId ??
                    CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId))
                .ToArray();
        if (invalidDiagnostics.Length > 0)
        {
            return new(
                Catalog: null,
                invalidDiagnostics[0].RuleId,
                HasUnresolvedTextCollation: false,
                statementResults,
                invalidDiagnostics);
        }

        long catalogObjectCount = indexes.Count;
        foreach (TableModel table in tables.Values)
        {
            catalogObjectCount = checked(
                catalogObjectCount +
                1L +
                table.Columns.Count +
                table.Keys.Count +
                table.ForeignKeys.Count);
        }
        if (catalogObjectCount > maximumCatalogObjects)
        {
            CSharpDbDdlCompatibilityStatement[] limitedStatements =
                contexts.Select(static context =>
                    SqlServerTsqlDdlCompatibilityAnalyzer.Statement(
                        context.Statement,
                        context.Index,
                        context.Kind,
                        MigrationCompatibilityStatus.Unknown,
                        SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId))
                    .ToArray();
            return new(
                Catalog: null,
                SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
                HasUnresolvedTextCollation: false,
                limitedStatements,
                [
                    SqlServerTsqlDdlCompatibilityAnalyzer.Diagnostic(
                        0,
                        SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
                        MigrationCompatibilityStatus.Unknown,
                        MigrationEvidenceLevel.Parsed,
                        statementIndex: null,
                        span: null,
                        "The lowered catalog exceeded the production object limit."),
                ],
                LimitExceeded: true);
        }

        var objects = new List<MigrationCatalogObject>(
            checked((int)catalogObjectCount));
        bool hasText = false;
        foreach (TableModel table in tables.Values
                     .OrderBy(static item => item.Context.Index))
        {
            cancellationToken.ThrowIfCancellationRequested();
            table.AppendObjects(objects);
            hasText |= table.Columns.Any(static column =>
                string.Equals(
                    column.LogicalType,
                    "text",
                    StringComparison.Ordinal));
        }
        foreach (IndexModel index in indexes
                     .OrderBy(static item => item.Context.Index))
        {
            objects.Add(index.ToObject());
        }

        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion = capabilities.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.SqlServer,
                Identity = "tsql-ddl:sha256:" + scriptDigest,
                Fingerprint = scriptDigest,
                ProviderVersion =
                    SqlServerTsqlDdlCompatibilityAnalyzer.SourceGrammar,
                SourceVersion = "tsql-ddl/v1",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description =
                        "Immutable content-addressed standalone T-SQL DDL script.",
                },
            },
            Objects = objects
                .OrderBy(static item => item.ObjectId, StringComparer.Ordinal)
                .ToArray(),
        };
        MigrationContractValidator.ValidateCatalog(catalog);

        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics =
            hasText
                ?
                [
                    SqlServerTsqlDdlCompatibilityAnalyzer.Diagnostic(
                        0,
                        SqlServerTsqlDdlCompatibilityAnalyzer
                            .TextCollationRuleId,
                        MigrationCompatibilityStatus.Conditional,
                        MigrationEvidenceLevel.Parsed,
                        statementIndex: null,
                        span: null,
                        "SQL Server text collation semantics remain unresolved."),
                ]
                : [];
        return new(
            catalog,
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId,
            hasText,
            statementResults,
            diagnostics);
    }

    private static TableModel? ReadTable(
        StatementContext context,
        CreateTableStatement statement,
        CancellationToken cancellationToken)
    {
        if (!TryDboName(statement.SchemaObjectName, out string? name) ||
            statement.AsEdge ||
            statement.AsFileTable ||
            statement.AsNode ||
            statement.ClonePointInTime is not null ||
            statement.CloneSource is not null ||
            statement.CtasColumns.Count != 0 ||
            statement.FederationScheme is not null ||
            statement.FileStreamOn is not null ||
            statement.OnFileGroupOrPartitionScheme is not null ||
            statement.Options.Count != 0 ||
            statement.SelectStatement is not null ||
            statement.TextImageOn is not null ||
            statement.Definition is null ||
            statement.Definition.ColumnDefinitions.Count == 0 ||
            statement.Definition.ColumnDefinitions.Count >
                MaxSqlServerTableColumns ||
            statement.Definition.Indexes.Count != 0 ||
            statement.Definition.SystemTimePeriod is not null)
        {
            context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "CREATE TABLE contains a name, storage, temporal, graph, or derived-table feature outside the allowlist.");
            return null;
        }

        var table = new TableModel(context, name!);
        var columnNames = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0;
             ordinal < statement.Definition.ColumnDefinitions.Count;
             ordinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TSqlColumnDefinition definition =
                statement.Definition.ColumnDefinitions[ordinal];
            string columnName = definition.ColumnIdentifier?.Value ??
                string.Empty;
            if (!ValidIdentifier(columnName) ||
                !columnNames.Add(columnName) ||
                !TryReadType(
                    definition.DataType,
                    out TypeShape? type) ||
                definition.Collation is not null &&
                !string.Equals(
                    type?.LogicalType,
                    "text",
                    StringComparison.Ordinal) ||
                HasUnsupportedColumnFeature(definition))
            {
                context.Reject(
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "A column contains an unsupported name, type, generation, default, storage, or security feature.");
                continue;
            }

            table.Columns.Add(new ColumnModel(
                definition,
                columnName,
                Id(context.Index, "column", ordinal),
                ordinal,
                type!));
        }

        int constraintOrdinal = 0;
        foreach (ColumnModel column in table.Columns)
        {
            foreach (ConstraintDefinition constraint in
                     column.Definition.Constraints)
            {
                cancellationToken.ThrowIfCancellationRequested();
                switch (constraint)
                {
                    case NullableConstraintDefinition:
                        break;
                    case UniqueConstraintDefinition unique:
                        ReadKey(
                            table,
                            unique,
                            inlineColumn: column,
                            constraintOrdinal++);
                        break;
                    case ForeignKeyConstraintDefinition foreignKey:
                        ReadForeignKey(
                            table,
                            foreignKey,
                            inlineColumn: column,
                            constraintOrdinal++);
                        break;
                    default:
                        context.Reject(
                            SqlServerTsqlDdlCompatibilityAnalyzer
                                .UnsupportedFeatureRuleId,
                            "A column constraint is outside the allowlist.");
                        break;
                }
            }
        }
        foreach (ConstraintDefinition constraint in
                 statement.Definition.TableConstraints)
        {
            cancellationToken.ThrowIfCancellationRequested();
            switch (constraint)
            {
                case UniqueConstraintDefinition unique:
                    ReadKey(
                        table,
                        unique,
                        inlineColumn: null,
                        constraintOrdinal++);
                    break;
                case ForeignKeyConstraintDefinition foreignKey:
                    ReadForeignKey(
                        table,
                        foreignKey,
                        inlineColumn: null,
                        constraintOrdinal++);
                    break;
                default:
                    context.Reject(
                        SqlServerTsqlDdlCompatibilityAnalyzer
                            .UnsupportedFeatureRuleId,
                        "A table constraint is outside the allowlist.");
                    break;
            }
        }
        return table;
    }

    private static void ResolveTable(
        TableModel table,
        IReadOnlyDictionary<string, TableModel> tables,
        CancellationToken cancellationToken)
    {
        if (table.Context.Diagnostic is not null)
            return;
        if (table.Keys.Count(static key => key.Primary) > 1)
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A table may declare only one primary key.");
            return;
        }

        var primaryColumns = table.Keys
            .Where(static key => key.Primary)
            .SelectMany(static key => key.Columns)
            .ToHashSet();
        foreach (ColumnModel column in table.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            NullableConstraintDefinition[] nullability =
                column.Definition.Constraints
                    .OfType<NullableConstraintDefinition>()
                    .ToArray();
            bool primary = primaryColumns.Contains(column);
            if (nullability.Length > 1 ||
                primary && nullability.Any(static item => item.Nullable) ||
                column.IsRowVersion &&
                    nullability.Any(static item => item.Nullable) ||
                !primary && !column.IsRowVersion && nullability.Length != 1)
            {
                table.Context.Reject(
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "Every ordinary non-primary-key column must declare exactly one NULL or NOT NULL constraint, and rowversion columns cannot be nullable.");
                return;
            }
            column.Nullable = !primary &&
                !column.IsRowVersion &&
                nullability[0].Nullable;
            column.BindMapping();
        }

        foreach (KeyModel key in table.Keys)
        {
            if (key.Columns.Count == 0 ||
                key.Columns.Any(static column =>
                    column.LogicalType != "signedInteger") ||
                key.Columns.Any(static column => column.Nullable))
            {
                table.Context.Reject(
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "Primary and unique keys require non-null integral members in this T-SQL subset.");
                return;
            }
        }

        foreach (PendingForeignKey pending in table.PendingForeignKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!tables.TryGetValue(
                    pending.ReferencedTableName,
                    out TableModel? referenced) ||
                !string.Equals(
                    referenced.Name,
                    pending.ReferencedTableName,
                    StringComparison.Ordinal) ||
                referenced.Context.Diagnostic is not null ||
                referenced.Context.Index > table.Context.Index ||
                pending.ReferencedColumns.Count == 0)
            {
                table.Context.Reject(
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .InvalidReferenceRuleId,
                    "A foreign key contains an unresolved or forward table dependency.");
                return;
            }
            KeyModel[] keys = referenced.Keys
                .Where(key => key.Columns
                    .Select(static column => column.Name)
                    .SequenceEqual(
                        pending.ReferencedColumns,
                        StringComparer.Ordinal))
                .ToArray();
            if (keys.Length != 1 ||
                pending.SourceColumns.Count != keys[0].Columns.Count)
            {
                table.Context.Reject(
                    SqlServerTsqlDdlCompatibilityAnalyzer
                        .InvalidReferenceRuleId,
                    "A foreign key must resolve to exactly one ordered primary or unique key.");
                return;
            }
            for (int ordinal = 0;
                 ordinal < pending.SourceColumns.Count;
                 ordinal++)
            {
                if (!pending.SourceColumns[ordinal].ExactSignatureEquals(
                        keys[0].Columns[ordinal]))
                {
                    table.Context.Reject(
                        SqlServerTsqlDdlCompatibilityAnalyzer
                            .UnsupportedFeatureRuleId,
                        "Foreign-key source and referenced columns require exact type, facet, and codec matches.");
                    return;
                }
            }
            table.ForeignKeys.Add(new ForeignKeyModel(
                Id(table.Context.Index, "foreign-key", pending.Ordinal),
                pending.SourceName,
                pending.HasExplicitName,
                pending.SourceColumns,
                keys[0],
                pending.DeleteAction));
        }
    }

    private static void ReadKey(
        TableModel table,
        UniqueConstraintDefinition definition,
        ColumnModel? inlineColumn,
        int ordinal)
    {
        if (definition.Clustered == true ||
            definition.FileStreamOn is not null ||
            definition.IndexOptions.Count != 0 ||
            definition.OnFileGroupOrPartitionScheme is not null ||
            definition.IsEnforced == false ||
            definition.IndexType?.IndexTypeKind is not (
                null or IndexTypeKind.NonClustered))
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A primary or unique constraint uses an unsupported physical index feature.");
            return;
        }

        IReadOnlyList<ColumnModel>? columns = inlineColumn is not null
            ? [inlineColumn]
            : ResolveColumns(table, definition.Columns);
        if (columns is null || columns.Count == 0)
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .InvalidReferenceRuleId,
                "A primary or unique constraint contains an invalid column reference.");
            return;
        }
        Identifier? constraintIdentifier =
            definition.ConstraintIdentifier;
        if (constraintIdentifier is not null &&
            !ValidIdentifier(constraintIdentifier.Value))
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A primary or unique constraint name is outside the bounded identifier contract.");
            return;
        }
        if (columns.Count > MaxSqlServerKeyColumns)
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A primary or unique constraint exceeds the bounded SQL Server key-column limit.");
            return;
        }
        table.Keys.Add(new KeyModel(
            Id(table.Context.Index, "key", ordinal),
            constraintIdentifier?.Value ??
            SyntheticName(
                definition.IsPrimaryKey ? "pk" : "uq",
                table.Context.Index,
                ordinal),
            constraintIdentifier is not null,
            definition.IsPrimaryKey,
            columns));
    }

    private static void ReadForeignKey(
        TableModel table,
        ForeignKeyConstraintDefinition definition,
        ColumnModel? inlineColumn,
        int ordinal)
    {
        if (definition.NotForReplication ||
            definition.IsEnforced == false ||
            definition.DeleteAction is DeleteUpdateAction.SetDefault or
                DeleteUpdateAction.SetNull ||
            definition.UpdateAction is not (
                DeleteUpdateAction.NotSpecified or
                DeleteUpdateAction.NoAction) ||
            !TryDboName(
                definition.ReferenceTableName,
                out string? referencedName))
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A foreign key uses an unsupported enforcement, action, or table-name feature.");
            return;
        }
        IReadOnlyList<ColumnModel>? sourceColumns = inlineColumn is not null
            ? [inlineColumn]
            : ResolveColumns(table, definition.Columns);
        Identifier? constraintIdentifier =
            definition.ConstraintIdentifier;
        string[] referencedColumns = definition.ReferencedTableColumns
            .Select(static identifier => identifier.Value)
            .ToArray();
        if (sourceColumns is null ||
            sourceColumns.Count == 0 ||
            sourceColumns.Count > MaxSqlServerKeyColumns ||
            referencedColumns.Length != sourceColumns.Count ||
            referencedColumns.Any(static name =>
                !ValidIdentifier(name)) ||
            referencedColumns.Distinct(
                StringComparer.OrdinalIgnoreCase).Count() !=
            referencedColumns.Length ||
            constraintIdentifier is not null &&
            !ValidIdentifier(constraintIdentifier.Value))
        {
            table.Context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .InvalidReferenceRuleId,
                "A foreign key contains invalid source or referenced columns.");
            return;
        }
        table.PendingForeignKeys.Add(new PendingForeignKey(
            ordinal,
            constraintIdentifier?.Value ??
            SyntheticName("fk", table.Context.Index, ordinal),
            constraintIdentifier is not null,
            sourceColumns,
            referencedName!,
            referencedColumns,
            definition.DeleteAction));
    }

    private static IndexModel? ReadIndex(
        StatementContext context,
        CreateIndexStatement statement,
        IReadOnlyDictionary<string, TableModel> tables)
    {
        string name = statement.Name?.Value ?? string.Empty;
        if (statement.Clustered == true ||
            statement.FileStreamOn is not null ||
            statement.FilterPredicate is not null ||
            statement.IncludeColumns.Count != 0 ||
            statement.IndexOptions.Count != 0 ||
            statement.OnFileGroupOrPartitionScheme is not null ||
            statement.Translated80SyntaxTo90 ||
            statement.Columns.Count == 0 ||
            statement.Columns.Count > MaxSqlServerKeyColumns ||
            !ValidIdentifier(name) ||
            !TryDboName(statement.OnName, out string? tableName) ||
            !tables.TryGetValue(tableName!, out TableModel? table) ||
            !string.Equals(
                table.Name,
                tableName,
                StringComparison.Ordinal) ||
            table.Context.Diagnostic is not null ||
            table.Context.Index >= context.Index)
        {
            context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "CREATE INDEX contains an unsupported feature, duplicate name, or unresolved source-order dependency.");
            return null;
        }
        if (!table.IndexNames.Add(name))
        {
            context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .DuplicateObjectRuleId,
                "CREATE INDEX duplicates an index name within its table.");
            return null;
        }
        IReadOnlyList<ColumnModel>? columns =
            ResolveColumns(table, statement.Columns);
        if (columns is null ||
            columns.Any(static column =>
                column.LogicalType != "signedInteger") ||
            statement.Unique &&
            columns.Any(static column => column.Nullable))
        {
            context.Reject(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "Indexes require ascending integral members, and unique index members must be non-null.");
            return null;
        }
        return new IndexModel(
            context,
            Id(context.Index, "index", 0),
            name,
            table,
            columns,
            statement.Unique);
    }

    private static IReadOnlyList<ColumnModel>? ResolveColumns(
        TableModel table,
        IEnumerable<ColumnWithSortOrder> references)
    {
        var columns = new List<ColumnModel>();
        var seen = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        foreach (ColumnWithSortOrder reference in references)
        {
            if (reference.SortOrder == SortOrder.Descending ||
                !TrySimpleColumn(reference.Column, out string? name) ||
                !seen.Add(name!) ||
                !table.ColumnsByName.TryGetValue(
                    name!,
                    out ColumnModel? column) ||
                !string.Equals(
                    column.Name,
                    name,
                    StringComparison.Ordinal))
            {
                return null;
            }
            columns.Add(column);
        }
        return columns;
    }

    private static IReadOnlyList<ColumnModel>? ResolveColumns(
        TableModel table,
        IEnumerable<Identifier> references)
    {
        var columns = new List<ColumnModel>();
        var seen = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);
        foreach (Identifier reference in references)
        {
            if (!ValidIdentifier(reference.Value) ||
                !seen.Add(reference.Value) ||
                !table.ColumnsByName.TryGetValue(
                    reference.Value,
                    out ColumnModel? column) ||
                !string.Equals(
                    column.Name,
                    reference.Value,
                    StringComparison.Ordinal))
            {
                return null;
            }
            columns.Add(column);
        }
        return columns;
    }

    private static bool TrySimpleColumn(
        ColumnReferenceExpression reference,
        out string? name)
    {
        name = null;
        if (reference.MultiPartIdentifier?.Identifiers.Count != 1)
            return false;
        name = reference.MultiPartIdentifier.Identifiers[0].Value;
        return ValidIdentifier(name);
    }

    private static bool TryDboName(
        SchemaObjectName? name,
        out string? baseName)
    {
        baseName = null;
        string? candidate = name?.BaseIdentifier?.Value;
        if (name?.Identifiers.Count != 2 ||
            !string.Equals(
                name.SchemaIdentifier?.Value,
                "dbo",
                StringComparison.Ordinal) ||
            !ValidIdentifier(candidate) ||
            candidate![0] == '#')
        {
            return false;
        }
        baseName = candidate;
        return true;
    }

    private static bool ValidIdentifier(string? value) =>
        !string.IsNullOrEmpty(value) &&
        value.Length <= MaxSqlServerIdentifierCharacters;

    private static bool HasUnsupportedColumnFeature(
        TSqlColumnDefinition definition) =>
        definition.ComputedColumnExpression is not null ||
        definition.DefaultConstraint is not null ||
        definition.Encryption is not null ||
        definition.GeneratedAlways is not null ||
        definition.IdentityOptions is not null ||
        definition.Index is not null ||
        definition.IsHidden ||
        definition.IsMasked ||
        definition.IsPersisted ||
        definition.IsRowGuidCol ||
        definition.MaskingFunction is not null ||
        definition.StorageOptions is not null;

    private static bool TryReadType(
        DataTypeReference? reference,
        out TypeShape? shape)
    {
        shape = null;
        if (reference is not SqlDataTypeReference sql ||
            sql.Name?.Identifiers.Count != 1)
        {
            return false;
        }
        string systemType = sql.Name.BaseIdentifier.Value.ToLowerInvariant();
        string logicalType =
            SqlServerTypeSemantics.LogicalType(systemType);
        if (logicalType == "native")
            return false;

        string[] parameters = sql.Parameters
            .Select(static parameter => parameter.Value)
            .ToArray();
        var facets = new List<MigrationCatalogFacet>
        {
            Facet("sqlServerSystemTypeName", systemType),
        };
        bool valid = sql.SqlDataTypeOption switch
        {
            SqlDataTypeOption.BigInt or
            SqlDataTypeOption.Int or
            SqlDataTypeOption.SmallInt or
            SqlDataTypeOption.TinyInt or
            SqlDataTypeOption.Bit or
            SqlDataTypeOption.Real or
            SqlDataTypeOption.DateTime or
            SqlDataTypeOption.SmallDateTime or
            SqlDataTypeOption.Timestamp or
            SqlDataTypeOption.Rowversion or
            SqlDataTypeOption.Text or
            SqlDataTypeOption.NText or
            SqlDataTypeOption.Image or
            SqlDataTypeOption.UniqueIdentifier =>
                parameters.Length == 0,
            SqlDataTypeOption.Money =>
                AddDecimalFacets(facets, parameters, 19, 4),
            SqlDataTypeOption.SmallMoney =>
                AddDecimalFacets(facets, parameters, 10, 4),
            SqlDataTypeOption.Decimal or SqlDataTypeOption.Numeric =>
                ReadDecimalFacets(facets, parameters),
            SqlDataTypeOption.Float =>
                ReadSingleValue(
                    parameters,
                    defaultValue: 53,
                    minimum: 1,
                    maximum: 53),
            SqlDataTypeOption.Char or SqlDataTypeOption.VarChar or
            SqlDataTypeOption.Binary or SqlDataTypeOption.VarBinary =>
                ReadLengthFacet(
                    facets,
                    parameters,
                    permitsMax: sql.SqlDataTypeOption is
                        SqlDataTypeOption.VarChar or
                        SqlDataTypeOption.VarBinary,
                    maximum: 8000),
            SqlDataTypeOption.NChar or SqlDataTypeOption.NVarChar =>
                ReadLengthFacet(
                    facets,
                    parameters,
                    permitsMax:
                        sql.SqlDataTypeOption ==
                        SqlDataTypeOption.NVarChar,
                    maximum: 4000),
            SqlDataTypeOption.Date or
            SqlDataTypeOption.Time or
            SqlDataTypeOption.DateTime2 or
            SqlDataTypeOption.DateTimeOffset =>
                sql.SqlDataTypeOption == SqlDataTypeOption.Date
                    ? parameters.Length == 0
                    : ReadSingleFacet(
                        facets,
                        "fractionalSeconds",
                        parameters,
                        defaultValue: 7,
                        minimum: 0,
                        maximum: 7),
            _ => false,
        };
        if (!valid)
            return false;
        if (Value(facets, "fractionalSeconds") is string fractionalSeconds)
            facets.Add(Facet("sqlServerScale", fractionalSeconds));
        string native = NativeType(
            systemType,
            sql.SqlDataTypeOption,
            facets);
        shape = new TypeShape(native, logicalType, facets);
        return true;
    }

    private static string NativeType(
        string systemType,
        SqlDataTypeOption option,
        IReadOnlyList<MigrationCatalogFacet> facets)
    {
        string? parameter = option switch
        {
            SqlDataTypeOption.Decimal or SqlDataTypeOption.Numeric =>
                string.Concat(
                    Value(facets, "precision"),
                    ",",
                    Value(facets, "scale")),
            SqlDataTypeOption.Char or SqlDataTypeOption.VarChar or
            SqlDataTypeOption.NChar or SqlDataTypeOption.NVarChar or
            SqlDataTypeOption.Binary or SqlDataTypeOption.VarBinary =>
                Value(facets, "maxLength"),
            SqlDataTypeOption.Time or SqlDataTypeOption.DateTime2 or
            SqlDataTypeOption.DateTimeOffset =>
                Value(facets, "fractionalSeconds"),
            _ => null,
        };
        return parameter is null
            ? "sys." + systemType
            : string.Concat("sys.", systemType, "(", parameter, ")");
    }

    private static string? Value(
        IEnumerable<MigrationCatalogFacet> facets,
        string name) =>
        facets.FirstOrDefault(facet => string.Equals(
            facet.Name,
            name,
            StringComparison.Ordinal))?.Value;

    private static bool AddDecimalFacets(
        ICollection<MigrationCatalogFacet> facets,
        IReadOnlyList<string> parameters,
        int precision,
        int scale)
    {
        if (parameters.Count != 0)
            return false;
        facets.Add(Facet("precision", precision));
        facets.Add(Facet("scale", scale));
        return true;
    }

    private static bool ReadDecimalFacets(
        ICollection<MigrationCatalogFacet> facets,
        IReadOnlyList<string> parameters)
    {
        if (parameters.Count > 2 ||
            parameters.Any(static value =>
                !int.TryParse(
                    value,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out _)))
        {
            return false;
        }
        int precision = parameters.Count == 0
            ? 18
            : int.Parse(parameters[0], CultureInfo.InvariantCulture);
        int scale = parameters.Count < 2
            ? 0
            : int.Parse(parameters[1], CultureInfo.InvariantCulture);
        if (precision is < 1 or > 38 || scale < 0 || scale > precision)
            return false;
        facets.Add(Facet("precision", precision));
        facets.Add(Facet("scale", scale));
        return true;
    }

    private static bool ReadSingleFacet(
        ICollection<MigrationCatalogFacet> facets,
        string name,
        IReadOnlyList<string> parameters,
        int defaultValue,
        int minimum,
        int maximum)
    {
        if (parameters.Count > 1)
            return false;
        int value = defaultValue;
        if (parameters.Count == 1 &&
            !int.TryParse(
                parameters[0],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out value))
        {
            return false;
        }
        if (value < minimum || value > maximum)
            return false;
        facets.Add(Facet(name, value));
        return true;
    }

    private static bool ReadSingleValue(
        IReadOnlyList<string> parameters,
        int defaultValue,
        int minimum,
        int maximum)
    {
        if (parameters.Count > 1)
            return false;
        int value = defaultValue;
        if (parameters.Count == 1 &&
            !int.TryParse(
                parameters[0],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out value))
        {
            return false;
        }
        return value >= minimum && value <= maximum;
    }

    private static bool ReadLengthFacet(
        ICollection<MigrationCatalogFacet> facets,
        IReadOnlyList<string> parameters,
        bool permitsMax,
        int maximum)
    {
        if (parameters.Count > 1)
            return false;
        string length = parameters.Count == 0 ? "1" : parameters[0];
        if (string.Equals(length, "max", StringComparison.OrdinalIgnoreCase))
        {
            if (!permitsMax)
                return false;
            facets.Add(new MigrationCatalogFacet
            {
                Name = "maxLength",
                Value = "max",
            });
            return true;
        }
        if (!int.TryParse(
                length,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed) ||
            parsed < 1 ||
            parsed > maximum)
        {
            return false;
        }
        facets.Add(Facet("maxLength", parsed));
        return true;
    }

    private static MigrationCatalogFacet Facet(string name, int value) =>
        new()
        {
            Name = name,
            Value = value.ToString(CultureInfo.InvariantCulture),
        };

    private static MigrationCatalogFacet Facet(
        string name,
        string value) =>
        new() { Name = name, Value = value };

    private static string SyntheticName(
        string kind,
        int statement,
        int ordinal) =>
        string.Concat(
            "tsql_",
            kind,
            "_",
            statement.ToString("D6", CultureInfo.InvariantCulture),
            "_",
            ordinal.ToString("D6", CultureInfo.InvariantCulture));

    private static string Id(int statement, string kind, int ordinal) =>
        string.Concat(
            "tsql-ddl/",
            statement.ToString("D6", CultureInfo.InvariantCulture),
            "/",
            kind,
            "/",
            ordinal.ToString("D6", CultureInfo.InvariantCulture));

    private static MigrationObjectReference Member(
        string objectId,
        string role,
        int ordinal) =>
        new()
        {
            ObjectId = objectId,
            Role = role,
            Ordinal = ordinal,
        };

    private sealed class StatementContext
    {
        internal StatementContext(TSqlStatement statement, int index)
        {
            Statement = statement;
            Index = index;
        }

        internal TSqlStatement Statement { get; }
        internal int Index { get; }
        internal string Kind { get; set; } = "unsupported";
        internal CSharpDbDdlCompatibilityDiagnostic? Diagnostic { get; private set; }

        internal void Reject(string ruleId, string summary)
        {
            Diagnostic ??=
                SqlServerTsqlDdlCompatibilityAnalyzer.Diagnostic(
                    ordinal: 0,
                    ruleId,
                    MigrationCompatibilityStatus.Unsupported,
                    MigrationEvidenceLevel.Parsed,
                    Index,
                    SqlServerTsqlDdlCompatibilityAnalyzer.Span(Statement),
                    summary);
        }
    }

    private sealed class TableModel
    {
        internal TableModel(StatementContext context, string name)
        {
            Context = context;
            Name = name;
            ObjectId = Id(context.Index, "table", 0);
        }

        internal StatementContext Context { get; }
        internal string Name { get; }
        internal string ObjectId { get; }
        internal List<ColumnModel> Columns { get; } = [];
        private Dictionary<string, ColumnModel>? columnsByName;
        internal IReadOnlyDictionary<string, ColumnModel> ColumnsByName =>
            columnsByName ??= Columns.ToDictionary(
                static column => column.Name,
                StringComparer.OrdinalIgnoreCase);
        internal List<KeyModel> Keys { get; } = [];
        internal List<PendingForeignKey> PendingForeignKeys { get; } = [];
        internal List<ForeignKeyModel> ForeignKeys { get; } = [];
        internal HashSet<string> IndexNames { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        internal void AppendObjects(
            ICollection<MigrationCatalogObject> objects)
        {
            MigrationSourceSpan span =
                SqlServerTsqlDdlCompatibilityAnalyzer.Span(
                    Context.Statement);
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = ObjectId,
                Kind = MigrationObjectKind.Table,
                SourceNamespace = "dbo",
                SourceName = Name,
                SourceSpan = span,
            });
            foreach (ColumnModel column in Columns)
            {
                var facets = new List<MigrationCatalogFacet>
                {
                    Facet("logicalType", column.LogicalType),
                    Facet(
                        "nullable",
                        column.Nullable
                            .ToString()
                            .ToLowerInvariant()),
                    Facet("identity", "false"),
                    Facet(
                        "rowVersion",
                        column.IsRowVersion
                            .ToString()
                            .ToLowerInvariant()),
                };
                facets.AddRange(column.Type.Facets);
                objects.Add(new MigrationCatalogObject
                {
                    ObjectId = column.ObjectId,
                    Kind = MigrationObjectKind.Column,
                    ParentObjectId = ObjectId,
                    SourceNamespace = "dbo",
                    SourceName = column.Name,
                    NativeType = column.Type.NativeType,
                    SourceSpan = span,
                    Facets = facets,
                });
            }
            foreach (KeyModel key in Keys)
                objects.Add(key.ToObject(this, span));
            foreach (ForeignKeyModel foreignKey in ForeignKeys)
                objects.Add(foreignKey.ToObject(this, span));
        }
    }

    private sealed class ColumnModel
    {
        internal ColumnModel(
            TSqlColumnDefinition definition,
            string name,
            string objectId,
            int ordinal,
            TypeShape type)
        {
            Definition = definition;
            Name = name;
            ObjectId = objectId;
            Ordinal = ordinal;
            Type = type;
        }

        internal TSqlColumnDefinition Definition { get; }
        internal string Name { get; }
        internal string ObjectId { get; }
        internal int Ordinal { get; }
        internal TypeShape Type { get; }
        internal string LogicalType => Type.LogicalType;
        internal bool IsRowVersion => string.Equals(
            LogicalType,
            "rowVersion",
            StringComparison.Ordinal);
        internal bool Nullable { get; set; }
        internal DbType? TargetType { get; private set; }
        internal MigrationTypeMapping? Mapping { get; private set; }

        internal void BindMapping()
        {
            var source = new MigrationCatalogObject
            {
                ObjectId = ObjectId,
                Kind = MigrationObjectKind.Column,
                SourceName = Name,
                NativeType = Type.NativeType,
                Facets =
                [
                    Facet("logicalType", LogicalType),
                    .. Type.Facets,
                ],
            };
            MigrationTypeMappingDecision decision =
                new StandardDataTypeMappingProvider().Map(
                    new MigrationTypeMappingRequest
                    {
                        SourceObject = source,
                        Profile = MigrationMappingProfile.Preserve,
                        Coverage = new MigrationProfileCoverage
                        {
                            Kind = MigrationCoverageKind.None,
                            RequiresFullStreamValidation = true,
                        },
                    });
            Mapping = decision.Mapping;
            TargetType = decision.Mapping.TargetType;
        }

        internal bool ExactSignatureEquals(ColumnModel other)
        {
            if (!string.Equals(
                    Type.NativeType,
                    other.Type.NativeType,
                    StringComparison.OrdinalIgnoreCase) ||
                TargetType != other.TargetType ||
                Mapping?.Classification != other.Mapping?.Classification ||
                !string.Equals(
                    Mapping?.Conversion?.ConversionId,
                    other.Mapping?.Conversion?.ConversionId,
                    StringComparison.Ordinal))
            {
                return false;
            }
            return Type.Facets
                .OrderBy(static facet => facet.Name, StringComparer.Ordinal)
                .SequenceEqual(
                    other.Type.Facets.OrderBy(
                        static facet => facet.Name,
                        StringComparer.Ordinal));
        }
    }

    private sealed record TypeShape(
        string NativeType,
        string LogicalType,
        IReadOnlyList<MigrationCatalogFacet> Facets);

    private sealed record KeyModel(
        string ObjectId,
        string SourceName,
        bool HasExplicitName,
        bool Primary,
        IReadOnlyList<ColumnModel> Columns)
    {
        internal MigrationCatalogObject ToObject(
            TableModel table,
            MigrationSourceSpan span) =>
            new()
            {
                ObjectId = ObjectId,
                Kind = MigrationObjectKind.Key,
                ParentObjectId = table.ObjectId,
                SourceNamespace = "dbo",
                SourceName = SourceName,
                SourceSpan = span,
                Facets =
                [
                    Facet("kind", Primary ? "primary" : "unique"),
                ],
                Members = Columns.Select((column, ordinal) =>
                    Member(
                        column.ObjectId,
                        MigrationObjectReferenceRoles.Column,
                        ordinal)).ToArray(),
                DependsOn = Columns.Select(static column =>
                    column.ObjectId).ToArray(),
            };
    }

    private sealed record PendingForeignKey(
        int Ordinal,
        string SourceName,
        bool HasExplicitName,
        IReadOnlyList<ColumnModel> SourceColumns,
        string ReferencedTableName,
        IReadOnlyList<string> ReferencedColumns,
        DeleteUpdateAction DeleteAction);

    private sealed record ForeignKeyModel(
        string ObjectId,
        string SourceName,
        bool HasExplicitName,
        IReadOnlyList<ColumnModel> SourceColumns,
        KeyModel ReferencedKey,
        DeleteUpdateAction DeleteAction)
    {
        internal MigrationCatalogObject ToObject(
            TableModel table,
            MigrationSourceSpan span)
        {
            List<MigrationObjectReference> members = SourceColumns
                .Select((column, ordinal) =>
                    Member(
                        column.ObjectId,
                        MigrationObjectReferenceRoles.SourceColumn,
                        ordinal))
                .ToList();
            members.Add(Member(
                ReferencedKey.ObjectId,
                MigrationObjectReferenceRoles.ReferencedKey,
                0));
            return new MigrationCatalogObject
            {
                ObjectId = ObjectId,
                Kind = MigrationObjectKind.ForeignKey,
                ParentObjectId = table.ObjectId,
                SourceNamespace = "dbo",
                SourceName = SourceName,
                SourceSpan = span,
                Facets =
                [
                    Facet(
                        "onDelete",
                        DeleteAction == DeleteUpdateAction.Cascade
                            ? "cascade"
                            : "restrict"),
                ],
                Members = members,
                DependsOn = SourceColumns
                    .Select(static column => column.ObjectId)
                    .Append(ReferencedKey.ObjectId)
                    .ToArray(),
            };
        }
    }

    private sealed record IndexModel(
        StatementContext Context,
        string ObjectId,
        string SourceName,
        TableModel Table,
        IReadOnlyList<ColumnModel> Columns,
        bool Unique)
    {
        internal MigrationCatalogObject ToObject() =>
            new()
            {
                ObjectId = ObjectId,
                Kind = MigrationObjectKind.Index,
                ParentObjectId = Table.ObjectId,
                SourceNamespace = "dbo",
                SourceName = SourceName,
                SourceSpan =
                    SqlServerTsqlDdlCompatibilityAnalyzer.Span(
                        Context.Statement),
                Facets =
                [
                    Facet(
                        "unique",
                        Unique.ToString().ToLowerInvariant()),
                ],
                Members = Columns.Select((column, ordinal) =>
                    Member(
                        column.ObjectId,
                        MigrationObjectReferenceRoles.Column,
                        ordinal)).ToArray(),
                DependsOn = Columns.Select(static column =>
                    column.ObjectId).ToArray(),
            };
    }
}
