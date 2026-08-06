using System.Globalization;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Migration.CSharpDb;

internal static class DdlLowerer
{
    internal static CSharpDbDdlCompatibilityAnalyzer.LoweringResult Lower(
        IReadOnlyList<SqlScriptStatement> statements,
        CSharpDbCapabilityCatalog capabilities,
        string scriptDigest,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(statements);
        ArgumentNullException.ThrowIfNull(capabilities);
        ArgumentException.ThrowIfNullOrWhiteSpace(scriptDigest);
        cancellationToken.ThrowIfCancellationRequested();

        var diagnostics =
            new List<CSharpDbDdlCompatibilityDiagnostic>();
        var invalidStatements = new HashSet<int>();
        var tableStatements = new List<SqlScriptStatement>();
        var indexStatements = new List<SqlScriptStatement>();
        var results =
            new List<CSharpDbDdlCompatibilityStatement>(statements.Count);

        foreach (SqlScriptStatement item in statements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string kind = StatementKind(item.Statement);
            results.Add(new CSharpDbDdlCompatibilityStatement
            {
                Index = item.Index,
                Kind = kind,
                Span =
                    CSharpDbDdlCompatibilityAnalyzer.SourceSpan(item.Span),
                Status = MigrationCompatibilityStatus.Conditional,
                Evidence = MigrationEvidenceLevel.Parsed,
                RuleId =
                    CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId,
            });

            switch (item.Statement)
            {
                case CreateTableStatement:
                    tableStatements.Add(item);
                    break;
                case CreateIndexStatement:
                    indexStatements.Add(item);
                    break;
                default:
                    AddDiagnostic(
                        item,
                        CSharpDbDdlCompatibilityAnalyzer
                            .UnsupportedStatementRuleId,
                        "The statement kind is outside the initial additive DDL allowlist.",
                        "Use a persistent CREATE TABLE or simple CREATE INDEX statement.",
                        diagnostics,
                        invalidStatements);
                    break;
            }
        }

        var objects = new List<MigrationCatalogObject>();
        var tables =
            new Dictionary<string, TableModel>(
                StringComparer.OrdinalIgnoreCase);
        bool requiresRewrite = false;

        foreach (SqlScriptStatement item in tableStatements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var table = (CreateTableStatement)item.Statement;
            if (!tables.TryAdd(
                    table.TableName,
                    new TableModel(item, table)))
            {
                AddDiagnostic(
                    item,
                    CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId,
                    "The script declares a duplicate table name.",
                    "Use one unique table name per script.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            TableModel model = tables[table.TableName];
            ValidateAndCreateTable(
                model,
                objects,
                diagnostics,
                invalidStatements,
                ref requiresRewrite,
                cancellationToken);
        }

        foreach (TableModel model in tables.Values
                     .OrderBy(item => item.Statement.Index))
        {
            cancellationToken.ThrowIfCancellationRequested();
            ResolveForeignKeys(
                model,
                tables,
                objects,
                diagnostics,
                invalidStatements,
                ref requiresRewrite,
                cancellationToken);
        }

        var indexNames =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (SqlScriptStatement item in indexStatements)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ValidateAndCreateIndex(
                item,
                tables,
                indexNames,
                objects,
                diagnostics,
                invalidStatements,
                ref requiresRewrite);
        }

        CSharpDbDdlCompatibilityStatement[] finalStatements = results
            .Select(item => invalidStatements.Contains(item.Index)
                ? item with
                {
                    Status = MigrationCompatibilityStatus.Unsupported,
                    RuleId = diagnostics
                        .First(diagnostic =>
                            diagnostic.StatementIndex == item.Index)
                        .RuleId,
                }
                : item)
            .ToArray();
        if (diagnostics.Count > 0)
        {
            return new CSharpDbDdlCompatibilityAnalyzer.LoweringResult(
                Catalog: null,
                MigrationCompatibilityStatus.Unsupported,
                diagnostics[0].RuleId,
                RequiresRewrite: false,
                finalStatements,
                diagnostics);
        }

        cancellationToken.ThrowIfCancellationRequested();
        var catalog = new MigrationCatalog
        {
            TargetCSharpDbVersion =
                capabilities.TargetCSharpDbVersion,
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.CSharpDb,
                Identity = string.Concat(
                    "csharpdb-ddl:sha256:",
                    scriptDigest),
                Fingerprint = scriptDigest,
                ProviderVersion = "1",
                SourceVersion = "csharpdb-ddl/v1",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Immutable,
                    Description =
                        "Immutable content-addressed CSharpDB DDL script.",
                },
            },
            Objects = objects
                .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
                .ToArray(),
        };
        MigrationContractValidator.ValidateCatalog(catalog);
        return new CSharpDbDdlCompatibilityAnalyzer.LoweringResult(
            catalog,
            MigrationCompatibilityStatus.Conditional,
            CSharpDbDdlCompatibilityAnalyzer.CapabilityRuleId,
            requiresRewrite,
            finalStatements,
            diagnostics);
    }

    private static void ValidateAndCreateTable(
        TableModel model,
        ICollection<MigrationCatalogObject> objects,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements,
        ref bool requiresRewrite,
        CancellationToken cancellationToken)
    {
        SqlScriptStatement statement = model.Statement;
        CreateTableStatement table = model.Table;
        bool valid = true;
        if (table.IsTemporary ||
            table.IfNotExists ||
            table.CheckConstraints.Count > 0)
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "The CREATE TABLE statement contains a feature outside the initial proof subset.",
                "Remove temporary, conditional, or CHECK behavior before retrying.",
                diagnostics,
                invalidStatements);
            valid = false;
        }
        if (table.Columns.Count == 0)
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "A proven table must declare at least one supported column.",
                "Add a supported INTEGER, REAL, TEXT, or BLOB column.",
                diagnostics,
                invalidStatements);
            valid = false;
        }

        var columnNames =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int ordinal = 0; ordinal < table.Columns.Count; ordinal++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ColumnDef column = table.Columns[ordinal];
            if (!columnNames.Add(column.Name))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .DuplicateObjectRuleId,
                    "The table declares a duplicate column name.",
                    "Use unique column names within the table.",
                    diagnostics,
                    invalidStatements);
                valid = false;
                continue;
            }

            if (!TryMapType(
                    column.DeclaredType,
                    out string nativeType,
                    out string logicalType,
                    out DbType targetType))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "The column type cannot be represented by the target CSharpDB type model.",
                    "Use a supported logical SQL type and valid type facets.",
                    diagnostics,
                    invalidStatements);
                valid = false;
                continue;
            }
            CSharpDbLiteralDefaultDescriptor? literalDefault = null;
            if (column.DefaultExpression is not null &&
                !TryLowerLiteralDefault(
                    column.DefaultExpression,
                    column.DeclaredType,
                    out literalDefault,
                    out string defaultReason))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    $"The column default is outside the safe literal proof subset: {defaultReason}",
                    "Use a compatible INTEGER, REAL, TEXT, BLOB, or NULL literal default.",
                    diagnostics,
                    invalidStatements);
                valid = false;
            }
            if (column.IsIdentity ||
                column.IsRowVersion ||
                column.CheckConstraints.Count > 0)
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "The column contains an unproven identity, rowversion, or CHECK feature.",
                    "Remove the unproven column feature before retrying.",
                    diagnostics,
                    invalidStatements);
                valid = false;
            }
            if (!TryNormalizeCollation(
                    column.Collation,
                    targetType,
                    out string? normalizedCollation))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "The column collation cannot be preserved by the initial proof subset.",
                    "Use a safe collation token on a TEXT column or remove the collation.",
                    diagnostics,
                    invalidStatements);
                valid = false;
            }

            string columnId = Id(
                statement.Index,
                "column",
                ordinal);
            model.Columns.Add(new ColumnModel(
                column,
                columnId,
                ordinal,
                nativeType,
                logicalType,
                targetType,
                normalizedCollation,
                literalDefault));
            model.ColumnsByName.TryAdd(column.Name, model.Columns[^1]);
        }

        CreateKeys(
            model,
            diagnostics,
            invalidStatements,
            ref requiresRewrite);
        if (invalidStatements.Contains(statement.Index))
            valid = false;

        if (!valid)
            return;

        MigrationSourceSpan span =
            CSharpDbDdlCompatibilityAnalyzer.SourceSpan(statement.Span);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = model.TableId,
            Kind = MigrationObjectKind.Table,
            SourceName = table.TableName,
            SourceSpan = span,
        });
        HashSet<string> primaryColumnIds = model.Keys
            .Where(key => key.Kind == KeyConstraintKind.PrimaryKey)
            .SelectMany(key => key.Columns)
            .Select(column => column.ObjectId)
            .ToHashSet(StringComparer.Ordinal);
        foreach (ColumnModel column in model.Columns
                     .OrderBy(item => item.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool primary = primaryColumnIds.Contains(column.ObjectId);
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("logicalType", column.LogicalType),
                Facet(
                    "nullable",
                    (!primary && column.Column.IsNullable)
                        .ToString()
                        .ToLowerInvariant()),
                Facet(
                    "hasDefault",
                    (column.LiteralDefault is not null)
                        .ToString()
                        .ToLowerInvariant()),
            };
            CSharpDbDeclaredTypeContract.AddFacets(
                facets,
                column.Column.DeclaredType);
            if (column.Collation is not null)
            {
                facets.Add(
                    Facet("collation", column.Collation));
            }
            if (column.LiteralDefault is
                CSharpDbLiteralDefaultDescriptor literalDefault)
            {
                facets.Add(
                    Facet("defaultKind", literalDefault.Kind));
                if (literalDefault.LiteralType is not null)
                {
                    facets.Add(
                        Facet(
                            "defaultType",
                            literalDefault.LiteralType));
                }
                if (literalDefault.Value is not null)
                {
                    facets.Add(
                        Facet(
                            "defaultValue",
                            literalDefault.Value));
                }
                facets.Add(
                    Facet(
                        "defaultExpression",
                        literalDefault.Expression));
            }

            objects.Add(new MigrationCatalogObject
            {
                ObjectId = column.ObjectId,
                Kind = MigrationObjectKind.Column,
                ParentObjectId = model.TableId,
                SourceName = column.Column.Name,
                NativeType = column.NativeType,
                SourceSpan = span,
                Facets = facets,
            });
        }

        foreach (KeyModel key in model.Keys)
        {
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = key.ObjectId,
                Kind = MigrationObjectKind.Key,
                ParentObjectId = model.TableId,
                SourceName = key.SourceName,
                SourceSpan = span,
                Facets =
                [
                    Facet(
                        "kind",
                        key.Kind == KeyConstraintKind.PrimaryKey
                            ? "primary"
                            : "unique"),
                ],
                Members = key.Columns
                    .Select((column, ordinal) =>
                        Member(
                            column.ObjectId,
                            MigrationObjectReferenceRoles.Column,
                            ordinal))
                    .ToArray(),
                DependsOn = key.Columns
                    .Select(column => column.ObjectId)
                    .ToArray(),
            });
        }
    }

    private static void CreateKeys(
        TableModel model,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements,
        ref bool requiresRewrite)
    {
        SqlScriptStatement statement = model.Statement;
        int keyOrdinal = 0;
        var constraintNames =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (ColumnModel column in model.Columns)
        {
            if (!column.Column.IsPrimaryKey)
                continue;
            AddKey(
                model,
                constraintName: null,
                KeyConstraintKind.PrimaryKey,
                [column.Column.Name],
                keyOrdinal++,
                constraintNames,
                diagnostics,
                invalidStatements);
            requiresRewrite = true;
        }

        foreach (KeyConstraintClause key in model.Table.KeyConstraints)
        {
            AddKey(
                model,
                key.ConstraintName,
                key.Kind,
                key.Columns,
                keyOrdinal++,
                constraintNames,
                diagnostics,
                invalidStatements);
            requiresRewrite = true;
        }

        int primaryCount = model.Keys.Count(key =>
            key.Kind == KeyConstraintKind.PrimaryKey);
        if (primaryCount > 1)
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "The table declares more than one primary key.",
                "Retain one primary key declaration.",
                diagnostics,
                invalidStatements);
        }
    }

    private static void AddKey(
        TableModel model,
        string? constraintName,
        KeyConstraintKind kind,
        IReadOnlyList<string> columnNames,
        int ordinal,
        ISet<string> constraintNames,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements)
    {
        SqlScriptStatement statement = model.Statement;
        if (kind is not (
                KeyConstraintKind.PrimaryKey or
                KeyConstraintKind.Unique) ||
            columnNames.Count == 0)
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "The key constraint shape is outside the initial proof subset.",
                "Use a non-empty PRIMARY KEY or UNIQUE column list.",
                diagnostics,
                invalidStatements);
            return;
        }
        if (constraintName is not null &&
            !constraintNames.Add(constraintName))
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId,
                "The table declares a duplicate constraint name.",
                "Use unique constraint names within the table.",
                diagnostics,
                invalidStatements);
            return;
        }

        var seenColumns =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var columns = new List<ColumnModel>(columnNames.Count);
        foreach (string name in columnNames)
        {
            if (!seenColumns.Add(name) ||
                !model.ColumnsByName.TryGetValue(
                    name,
                    out ColumnModel? column))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .InvalidReferenceRuleId,
                    "The key constraint contains a duplicate or unknown column reference.",
                    "Reference each existing key column exactly once.",
                    diagnostics,
                    invalidStatements);
                return;
            }
            if (column.TargetType is not (
                    DbType.Integer or DbType.Text))
            {
                AddDiagnostic(
                    statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "CSharpDB keys in this proof subset require INTEGER or TEXT columns.",
                    "Use INTEGER or TEXT key columns.",
                    diagnostics,
                    invalidStatements);
                return;
            }

            columns.Add(column);
        }

        string sourceName = constraintName ??
            string.Concat(
                kind == KeyConstraintKind.PrimaryKey
                    ? "ddl_pk_"
                    : "ddl_uq_",
                statement.Index.ToString(
                    "D6",
                    System.Globalization.CultureInfo.InvariantCulture),
                "_",
                ordinal.ToString(
                    "D6",
                    System.Globalization.CultureInfo.InvariantCulture));
        model.Keys.Add(new KeyModel(
            Id(statement.Index, "key", ordinal),
            sourceName,
            constraintName is not null,
            kind,
            columns));
    }

    private static void ResolveForeignKeys(
        TableModel model,
        IReadOnlyDictionary<string, TableModel> tables,
        ICollection<MigrationCatalogObject> objects,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements,
        ref bool requiresRewrite,
        CancellationToken cancellationToken)
    {
        if (invalidStatements.Contains(model.Statement.Index))
            return;

        var pending = new List<PendingForeignKey>();
        int ordinal = 0;
        foreach (ColumnModel column in model.Columns)
        {
            if (column.Column.ForeignKey is not ForeignKeyClause foreignKey)
                continue;
            pending.Add(new PendingForeignKey(
                ConstraintName: null,
                SourceColumnNames: [column.Column.Name],
                foreignKey.ReferencedTableName,
                foreignKey.ReferencedColumnName is null
                    ? null
                    : [foreignKey.ReferencedColumnName],
                foreignKey.OnDelete,
                foreignKey.OnUpdate,
                ordinal++));
        }
        foreach (ForeignKeyConstraintClause foreignKey in
                 model.Table.ForeignKeys)
        {
            pending.Add(new PendingForeignKey(
                foreignKey.ConstraintName,
                foreignKey.Columns,
                foreignKey.ReferencedTableName,
                foreignKey.ReferencedColumns,
                foreignKey.OnDelete,
                foreignKey.OnUpdate,
                ordinal++));
        }
        if (pending.Count == 0)
            return;

        requiresRewrite = true;
        var names =
            new HashSet<string>(
                model.Keys
                    .Where(key => key.HasExplicitSourceName)
                    .Select(key => key.SourceName),
                StringComparer.OrdinalIgnoreCase);
        foreach (PendingForeignKey foreignKey in pending)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (foreignKey.ConstraintName is not null &&
                !names.Add(foreignKey.ConstraintName))
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .DuplicateObjectRuleId,
                    "The table declares a duplicate constraint name.",
                    "Use unique key and foreign-key constraint names.",
                    diagnostics,
                    invalidStatements);
                continue;
            }
            if (!TryResolveColumns(
                    model,
                    foreignKey.SourceColumnNames,
                    out IReadOnlyList<ColumnModel> sourceColumns) ||
                !tables.TryGetValue(
                    foreignKey.ReferencedTableName,
                    out TableModel? referencedTable) ||
                invalidStatements.Contains(
                    referencedTable.Statement.Index))
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .InvalidReferenceRuleId,
                    "The foreign key contains an unknown or duplicate source or table reference.",
                    "Reference tables and columns declared exactly once in this bounded script.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            bool supportedDeleteAction =
                foreignKey.OnDelete is
                    ForeignKeyOnDeleteAction.Restrict or
                    ForeignKeyOnDeleteAction.NoAction or
                    ForeignKeyOnDeleteAction.Cascade or
                    ForeignKeyOnDeleteAction.SetNull or
                    ForeignKeyOnDeleteAction.SetDefault;
            bool supportedUpdateAction =
                foreignKey.OnUpdate is
                    ForeignKeyOnDeleteAction.Restrict or
                    ForeignKeyOnDeleteAction.NoAction or
                    ForeignKeyOnDeleteAction.Cascade or
                    ForeignKeyOnDeleteAction.SetNull or
                    ForeignKeyOnDeleteAction.SetDefault;
            if (!supportedDeleteAction || !supportedUpdateAction)
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "The foreign key uses an unknown referential action.",
                    "Use RESTRICT, NO ACTION, CASCADE, SET NULL, or SET DEFAULT.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            bool requiresSetNull =
                foreignKey.OnDelete ==
                    ForeignKeyOnDeleteAction.SetNull ||
                foreignKey.OnUpdate ==
                    ForeignKeyOnDeleteAction.SetNull;
            if (requiresSetNull &&
                sourceColumns.Any(column =>
                    !column.Column.IsNullable ||
                    model.Keys.Any(key =>
                        key.Kind == KeyConstraintKind.PrimaryKey &&
                        key.Columns.Contains(column))))
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "SET NULL requires every child column to be nullable and outside the primary key.",
                    "Make every child column nullable or use a restrictive or cascading delete action.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            bool requiresSetDefault =
                foreignKey.OnDelete ==
                    ForeignKeyOnDeleteAction.SetDefault ||
                foreignKey.OnUpdate ==
                    ForeignKeyOnDeleteAction.SetDefault;
            if (requiresSetDefault &&
                sourceColumns.Any(column =>
                {
                    bool primary = model.Keys.Any(key =>
                        key.Kind == KeyConstraintKind.PrimaryKey &&
                        key.Columns.Contains(column));
                    bool effectiveNullable =
                        column.Column.IsNullable && !primary;
                    return (column.LiteralDefault is null ||
                            column.LiteralDefault.Value.ProducesNull) &&
                        !effectiveNullable;
                }))
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "SET DEFAULT requires every child column to have a compatible non-NULL literal default or be nullable when its default resolves to NULL.",
                    "Add compatible literal defaults, make NULL-defaulted child columns nullable, or choose another referential action.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            KeyModel[] matchingKeys =
                foreignKey.ReferencedColumnNames is null
                    ? referencedTable.PrimaryKeys(
                        sourceColumns.Count)
                    : referencedTable.KeysForColumns(
                        foreignKey.ReferencedColumnNames);
            KeyModel? referencedKey = matchingKeys.Length == 1
                ? matchingKeys[0]
                : null;
            if (referencedKey is null ||
                referencedKey.Columns.Count != sourceColumns.Count)
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .InvalidReferenceRuleId,
                    "The foreign key does not resolve to one ordered primary or unique key.",
                    "Reference an explicitly declared primary or unique key with matching arity.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            bool signaturesMatch = sourceColumns
                .Zip(
                    referencedKey.Columns,
                    (source, target) =>
                        source.TargetType == target.TargetType &&
                        string.Equals(
                            source.Collation ?? "default",
                            target.Collation ?? "default",
                            StringComparison.OrdinalIgnoreCase))
                .All(value => value);
            if (!signaturesMatch)
            {
                AddDiagnostic(
                    model.Statement,
                    CSharpDbDdlCompatibilityAnalyzer
                        .UnsupportedFeatureRuleId,
                    "The foreign-key column types or collations do not match the referenced key.",
                    "Use matching INTEGER or TEXT types and collations in source and referenced columns.",
                    diagnostics,
                    invalidStatements);
                continue;
            }

            string sourceName = foreignKey.ConstraintName ??
                string.Concat(
                    "ddl_fk_",
                    model.Statement.Index.ToString(
                        "D6",
                        System.Globalization.CultureInfo.InvariantCulture),
                    "_",
                    foreignKey.Ordinal.ToString(
                        "D6",
                        System.Globalization.CultureInfo.InvariantCulture));
            string foreignKeyId = Id(
                model.Statement.Index,
                "foreign-key",
                foreignKey.Ordinal);
            List<MigrationObjectReference> members = sourceColumns
                .Select((column, memberOrdinal) =>
                    Member(
                        column.ObjectId,
                        MigrationObjectReferenceRoles.SourceColumn,
                        memberOrdinal))
                .ToList();
            members.Add(Member(
                referencedKey.ObjectId,
                MigrationObjectReferenceRoles.ReferencedKey,
                0));
            string[] dependencies = sourceColumns
                .Select(column => column.ObjectId)
                .Append(referencedKey.ObjectId)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            var facets = new List<MigrationCatalogFacet>
            {
                Facet("onDelete", FormatReferentialAction(foreignKey.OnDelete)),
            };
            if (foreignKey.OnUpdate != ForeignKeyOnDeleteAction.Restrict)
            {
                facets.Add(
                    Facet(
                        "onUpdate",
                        FormatReferentialAction(foreignKey.OnUpdate)));
            }
            objects.Add(new MigrationCatalogObject
            {
                ObjectId = foreignKeyId,
                Kind = MigrationObjectKind.ForeignKey,
                ParentObjectId = model.TableId,
                SourceName = sourceName,
                SourceSpan =
                    CSharpDbDdlCompatibilityAnalyzer.SourceSpan(
                        model.Statement.Span),
                Facets = facets,
                Members = members,
                DependsOn = dependencies,
            });
        }
    }

    private static void ValidateAndCreateIndex(
        SqlScriptStatement statement,
        IReadOnlyDictionary<string, TableModel> tables,
        ISet<string> indexNames,
        ICollection<MigrationCatalogObject> objects,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements,
        ref bool requiresRewrite)
    {
        var index = (CreateIndexStatement)statement.Statement;
        if (index.IfNotExists ||
            index.Columns.Count == 0 ||
            index.ColumnCollations.Any(item => item is not null))
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "The index contains a conditional, empty, or per-column collation feature outside the initial proof subset.",
                "Use a non-conditional column-only index without per-index collation overrides.",
                diagnostics,
                invalidStatements);
            return;
        }
        if (!indexNames.Add(index.IndexName))
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer.DuplicateObjectRuleId,
                "The script declares a duplicate index name.",
                "Use unique index names within the script.",
                diagnostics,
                invalidStatements);
            return;
        }
        if (!tables.TryGetValue(
                index.TableName,
                out TableModel? table) ||
            invalidStatements.Contains(table.Statement.Index) ||
            !TryResolveColumns(
                table,
                index.Columns,
                out IReadOnlyList<ColumnModel> columns))
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .InvalidReferenceRuleId,
                "The index contains an unknown or duplicate table or column reference.",
                "Reference each existing INTEGER, REAL, or TEXT column exactly once.",
                diagnostics,
                invalidStatements);
            return;
        }
        if (columns.Any(column =>
                column.TargetType is not (
                    DbType.Integer or DbType.Real or DbType.Text)))
        {
            AddDiagnostic(
                statement,
                CSharpDbDdlCompatibilityAnalyzer
                    .UnsupportedFeatureRuleId,
                "CSharpDB indexes in this proof subset require INTEGER, REAL, or TEXT columns.",
                "Use INTEGER, REAL, or TEXT index columns.",
                diagnostics,
                invalidStatements);
            return;
        }
        if (statement.Index < table.Statement.Index)
            requiresRewrite = true;

        string objectId = Id(statement.Index, "index", 0);
        objects.Add(new MigrationCatalogObject
        {
            ObjectId = objectId,
            Kind = MigrationObjectKind.Index,
            ParentObjectId = table.TableId,
            SourceName = index.IndexName,
            SourceSpan =
                CSharpDbDdlCompatibilityAnalyzer.SourceSpan(
                    statement.Span),
            Facets =
            [
                Facet(
                    "unique",
                    index.IsUnique.ToString().ToLowerInvariant()),
            ],
            Members = columns
                .Select((column, ordinal) =>
                    Member(
                        column.ObjectId,
                        MigrationObjectReferenceRoles.Column,
                        ordinal))
                .ToArray(),
            DependsOn = columns
                .Select(column => column.ObjectId)
                .ToArray(),
        });
    }

    private static bool TryResolveColumns(
        TableModel table,
        IReadOnlyList<string> names,
        out IReadOnlyList<ColumnModel> columns)
    {
        var result = new List<ColumnModel>(names.Count);
        var seen =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string name in names)
        {
            if (!seen.Add(name) ||
                !table.ColumnsByName.TryGetValue(
                    name,
                    out ColumnModel? column))
            {
                columns = [];
                return false;
            }

            result.Add(column);
        }

        columns = result;
        return result.Count > 0;
    }

    private static bool TryNormalizeCollation(
        string? collation,
        DbType type,
        out string? normalizedCollation)
    {
        if (string.IsNullOrWhiteSpace(collation))
        {
            normalizedCollation = null;
            return true;
        }
        if (type != DbType.Text ||
            collation.Any(character =>
                !(char.IsLetterOrDigit(character) ||
                  character is '_' or '-' or ':')))
        {
            normalizedCollation = null;
            return false;
        }

        string normalized = collation.ToUpperInvariant();
        if (normalized is "BINARY" or "NOCASE" or "NOCASE_AI")
        {
            normalizedCollation = normalized;
            return true;
        }
        if (!normalized.StartsWith("ICU:", StringComparison.Ordinal))
        {
            normalizedCollation = null;
            return false;
        }

        string locale = collation["ICU:".Length..];
        if (string.IsNullOrWhiteSpace(locale))
        {
            normalizedCollation = null;
            return false;
        }
        try
        {
            CultureInfo culture = CultureInfo.GetCultureInfo(locale);
            normalizedCollation = string.Concat("ICU:", culture.Name);
            return true;
        }
        catch (CultureNotFoundException)
        {
            normalizedCollation = null;
            return false;
        }
    }

    private static bool TryMapType(
        SqlTypeDescriptor descriptor,
        out string nativeType,
        out string logicalType,
        out DbType targetType)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        nativeType = descriptor.ToSql();
        logicalType = descriptor.Kind switch
        {
            SqlTypeKind.Boolean => "boolean",
            SqlTypeKind.TinyInt or
            SqlTypeKind.SmallInt or
            SqlTypeKind.Integer or
            SqlTypeKind.BigInt => "signedInteger",
            SqlTypeKind.Real or
            SqlTypeKind.Double => "floatingPoint",
            SqlTypeKind.Decimal => "decimal",
            SqlTypeKind.Char or
            SqlTypeKind.VarChar or
            SqlTypeKind.Text => "text",
            SqlTypeKind.Binary or
            SqlTypeKind.VarBinary or
            SqlTypeKind.Blob => "binary",
            SqlTypeKind.Uuid => "uuid",
            SqlTypeKind.Date => "date",
            SqlTypeKind.Time => "time",
            SqlTypeKind.Timestamp => "datetime",
            SqlTypeKind.TimestampWithTimeZone => "datetimeOffset",
            SqlTypeKind.IntervalYearToMonth => "intervalYearToMonth",
            SqlTypeKind.IntervalDayToSecond => "intervalDayToSecond",
            SqlTypeKind.Json => "json",
            SqlTypeKind.Xml => "xml",
            SqlTypeKind.Bit or SqlTypeKind.VarBit => "bitString",
            _ => string.Empty,
        };
        targetType = descriptor.StorageType;
        return logicalType.Length != 0;
    }

    private static bool TryLowerLiteralDefault(
        Expression expression,
        SqlTypeDescriptor columnType,
        out CSharpDbLiteralDefaultDescriptor? descriptor,
        out string reason)
    {
        bool negative = false;
        LiteralExpression? literal = expression as LiteralExpression;
        if (expression is UnaryExpression
            {
                Op: TokenType.Minus,
                Operand: LiteralExpression operand
            })
        {
            negative = true;
            literal = operand;
        }
        if (literal is null)
        {
            descriptor = null;
            reason =
                "only typed literals, NULL, and unary-negative numeric literals are accepted.";
            return false;
        }

        string kind;
        string? literalType;
        string? value;
        switch (literal.LiteralType, literal.Value)
        {
            case (TokenType.Null, null) when !negative:
                kind = "null";
                literalType = null;
                value = null;
                break;

            case (TokenType.IntegerLiteral, long integer)
                when columnType.Kind == SqlTypeKind.Decimal &&
                     (!negative || integer >= 0):
                kind = "typed-literal";
                literalType = "decimal";
                value = string.Concat(
                    negative ? "-" : string.Empty,
                    literal.RawText ??
                    integer.ToString(CultureInfo.InvariantCulture));
                break;

            case (TokenType.RealLiteral, double real)
                when columnType.Kind == SqlTypeKind.Decimal &&
                     (!negative || real >= 0) &&
                     double.IsFinite(real):
                kind = "typed-literal";
                literalType = "decimal";
                value = string.Concat(
                    negative ? "-" : string.Empty,
                    literal.RawText ?? SqlLiteralRules.FormatReal(real));
                break;

            case (TokenType.IntegerLiteral, long integer)
                when !negative || integer >= 0:
                kind = "typed-literal";
                literalType = "integer";
                value = string.Concat(
                    negative ? "-" : string.Empty,
                    integer.ToString(CultureInfo.InvariantCulture));
                break;

            case (TokenType.RealLiteral, double real)
                when (!negative || real >= 0) &&
                     double.IsFinite(real):
                kind = "typed-literal";
                literalType = "real";
                value = string.Concat(
                    negative ? "-" : string.Empty,
                    SqlLiteralRules.FormatReal(real));
                break;

            case (TokenType.StringLiteral, string text)
                when !negative:
                kind = "typed-literal";
                literalType = "text";
                value = text;
                break;

            case (TokenType.BlobLiteral, byte[] blob)
                when !negative:
                kind = "typed-literal";
                literalType = "blob";
                value = Convert.ToHexString(blob);
                break;

            default:
                descriptor = null;
                reason =
                    "the parsed literal has an invalid value or unary sign.";
                return false;
        }

        if (!CSharpDbLiteralDefaultContract.TryCreate(
                columnType.StorageType,
                kind,
                literalType,
                value,
                out CSharpDbLiteralDefaultDescriptor lowered,
                out reason))
        {
            descriptor = null;
            return false;
        }

        if (columnType.Kind == SqlTypeKind.Decimal &&
            lowered.Value is string decimalText)
        {
            try
            {
                decimal decimalValue = decimal.Parse(
                    decimalText,
                    NumberStyles.AllowLeadingSign |
                    NumberStyles.AllowDecimalPoint,
                    CultureInfo.InvariantCulture);
                (int precision, int scale) =
                    CSharpDbDecimalCodec.ResolveFacets(
                        columnType.Precision,
                        columnType.Scale);
                _ = CSharpDbDecimalCodec.ToScaledInt64(
                    decimalValue,
                    precision,
                    scale);
            }
            catch (Exception error) when (error is
                FormatException or
                OverflowException or
                InvalidOperationException or
                NotSupportedException)
            {
                descriptor = null;
                reason =
                    $"the DECIMAL literal is outside {columnType.ToSql()}.";
                return false;
            }
        }

        descriptor = lowered;
        return true;
    }

    private static string StatementKind(Statement statement) =>
        statement switch
        {
            CreateTableStatement => "create-table",
            CreateIndexStatement => "create-index",
            _ => "unsupported",
        };

    private static void AddDiagnostic(
        SqlScriptStatement statement,
        string ruleId,
        string summary,
        string remediation,
        ICollection<CSharpDbDdlCompatibilityDiagnostic> diagnostics,
        ISet<int> invalidStatements)
    {
        if (!invalidStatements.Add(statement.Index))
            return;
        int ordinal = diagnostics.Count;
        diagnostics.Add(
            CSharpDbDdlCompatibilityAnalyzer.Diagnostic(
                ordinal,
                ruleId,
                MigrationCompatibilityStatus.Unsupported,
                MigrationEvidenceLevel.Parsed,
                statement.Index,
                CSharpDbDdlCompatibilityAnalyzer.SourceSpan(
                    statement.Span),
                summary,
                remediation));
    }

    private static MigrationCatalogFacet Facet(
        string name,
        string value) =>
        new()
        {
            Name = name,
            Value = value,
        };

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

    private static string Id(
        int statementIndex,
        string role,
        int ordinal) =>
        string.Concat(
            "csharpdb-ddl:s",
            statementIndex.ToString(
                "D6",
                System.Globalization.CultureInfo.InvariantCulture),
            ":",
            role,
            ":",
            ordinal.ToString(
                "D6",
                System.Globalization.CultureInfo.InvariantCulture));

    private sealed class TableModel(
        SqlScriptStatement statement,
        CreateTableStatement table)
    {
        private Dictionary<IReadOnlyList<string>, List<KeyModel>>?
            keysByColumns;
        private Dictionary<int, List<KeyModel>>? primaryKeysByArity;

        internal SqlScriptStatement Statement { get; } = statement;

        internal CreateTableStatement Table { get; } = table;

        internal string TableId { get; } =
            Id(statement.Index, "table", 0);

        internal List<ColumnModel> Columns { get; } = [];

        internal Dictionary<string, ColumnModel> ColumnsByName { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        internal List<KeyModel> Keys { get; } = [];

        internal KeyModel[] KeysForColumns(
            IReadOnlyList<string> columns)
        {
            EnsureKeyLookups();
            return keysByColumns!.TryGetValue(
                columns,
                out List<KeyModel>? keys)
                    ? keys.ToArray()
                    : [];
        }

        internal KeyModel[] PrimaryKeys(int arity)
        {
            EnsureKeyLookups();
            return primaryKeysByArity!.TryGetValue(
                arity,
                out List<KeyModel>? keys)
                    ? keys.ToArray()
                    : [];
        }

        private void EnsureKeyLookups()
        {
            if (keysByColumns is not null)
                return;

            keysByColumns =
                new Dictionary<IReadOnlyList<string>, List<KeyModel>>(
                    ColumnNameSequenceComparer.Instance);
            primaryKeysByArity = [];
            foreach (KeyModel key in Keys)
            {
                string[] names = key.Columns
                    .Select(column => column.Column.Name)
                    .ToArray();
                if (!keysByColumns.TryGetValue(
                        names,
                        out List<KeyModel>? matching))
                {
                    matching = [];
                    keysByColumns.Add(names, matching);
                }

                matching.Add(key);
                if (key.Kind != KeyConstraintKind.PrimaryKey)
                    continue;
                if (!primaryKeysByArity.TryGetValue(
                        names.Length,
                        out List<KeyModel>? primary))
                {
                    primary = [];
                    primaryKeysByArity.Add(
                        names.Length,
                        primary);
                }

                primary.Add(key);
            }
        }
    }

    private sealed record ColumnModel(
        ColumnDef Column,
        string ObjectId,
        int Ordinal,
        string NativeType,
        string LogicalType,
        DbType TargetType,
        string? Collation,
        CSharpDbLiteralDefaultDescriptor? LiteralDefault);

    private sealed record KeyModel(
        string ObjectId,
        string SourceName,
        bool HasExplicitSourceName,
        KeyConstraintKind Kind,
        IReadOnlyList<ColumnModel> Columns);

    private sealed record PendingForeignKey(
        string? ConstraintName,
        IReadOnlyList<string> SourceColumnNames,
        string ReferencedTableName,
        IReadOnlyList<string>? ReferencedColumnNames,
        ForeignKeyOnDeleteAction OnDelete,
        ForeignKeyOnDeleteAction OnUpdate,
        int Ordinal);

    private static string FormatReferentialAction(
        ForeignKeyOnDeleteAction action) =>
        action switch
        {
            ForeignKeyOnDeleteAction.Restrict => "restrict",
            ForeignKeyOnDeleteAction.Cascade => "cascade",
            ForeignKeyOnDeleteAction.NoAction => "no-action",
            ForeignKeyOnDeleteAction.SetNull => "set-null",
            ForeignKeyOnDeleteAction.SetDefault => "set-default",
            _ => throw new InvalidDataException(
                $"Unsupported foreign key referential action '{action}'."),
        };

    private sealed class ColumnNameSequenceComparer
        : IEqualityComparer<IReadOnlyList<string>>
    {
        internal static ColumnNameSequenceComparer Instance { get; } =
            new();

        public bool Equals(
            IReadOnlyList<string>? left,
            IReadOnlyList<string>? right)
        {
            if (ReferenceEquals(left, right))
                return true;
            if (left is null ||
                right is null ||
                left.Count != right.Count)
            {
                return false;
            }

            for (int index = 0; index < left.Count; index++)
            {
                if (!StringComparer.OrdinalIgnoreCase.Equals(
                        left[index],
                        right[index]))
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode(IReadOnlyList<string> columns)
        {
            var hash = new HashCode();
            foreach (string column in columns)
            {
                hash.Add(
                    column,
                    StringComparer.OrdinalIgnoreCase);
            }

            return hash.ToHashCode();
        }
    }
}
