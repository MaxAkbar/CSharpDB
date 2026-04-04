using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Execution;

/// <summary>
/// Takes a parsed AST statement and produces an executable QueryResult.
/// Handles DDL (CREATE/DROP TABLE/INDEX/VIEW) and DML (INSERT/UPDATE/DELETE/SELECT).
/// </summary>
public sealed class QueryPlanner
{
    private const string InternalSavedQueriesTableName = "__saved_queries";

    private readonly record struct ReorderableJoinLeaf(
        SimpleTableRef TableRef,
        TableSchema Schema,
        long RowCount,
        int OriginalIndex,
        string Identifier,
        string[] ReferenceNames);

    private sealed class ReorderableJoinPredicate
    {
        public required Expression Expression { get; init; }
        public required HashSet<string> ReferencedTables { get; init; }
        public required int OriginalIndex { get; init; }
    }

    private sealed class ForeignKeyMutationContext
    {
        public HashSet<ForeignKeyDeleteKey> VisitedDeletes { get; } = [];
        public HashSet<string> TouchedTables { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> StaleTables { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private readonly record struct ForeignKeyDeleteKey(string TableName, long RowId)
    {
        public bool Equals(ForeignKeyDeleteKey other) =>
            RowId == other.RowId &&
            string.Equals(TableName, other.TableName, StringComparison.OrdinalIgnoreCase);

        public override int GetHashCode() =>
            HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(TableName), RowId);
    }

    private static readonly ColumnDefinition[] SystemTablesColumns =
    [
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_count", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "primary_key_column", Type = DbType.Text, Nullable = true },
    ];

    private static readonly ColumnDefinition[] SystemColumnsColumns =
    [
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "ordinal_position", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "data_type", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "is_nullable", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "is_primary_key", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "is_identity", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "collation", Type = DbType.Text, Nullable = true },
    ];

    private static readonly ColumnDefinition[] SystemIndexesColumns =
    [
        new ColumnDefinition { Name = "index_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "ordinal_position", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "is_unique", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "collation", Type = DbType.Text, Nullable = true },
    ];

    private static readonly ColumnDefinition[] SystemForeignKeysColumns =
    [
        new ColumnDefinition { Name = "constraint_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "referenced_table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "referenced_column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "on_delete", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "supporting_index_name", Type = DbType.Text, Nullable = false },
    ];

    private static readonly ColumnDefinition[] SystemViewsColumns =
    [
        new ColumnDefinition { Name = "view_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "sql", Type = DbType.Text, Nullable = false },
    ];

    private static readonly ColumnDefinition[] SystemTriggersColumns =
    [
        new ColumnDefinition { Name = "trigger_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "timing", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "event", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "body_sql", Type = DbType.Text, Nullable = false },
    ];

    private static readonly ColumnDefinition[] SystemObjectsColumns =
    [
        new ColumnDefinition { Name = "object_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "object_type", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "parent_table_name", Type = DbType.Text, Nullable = true },
    ];

    private static readonly ColumnDefinition[] SystemTableStatsColumns =
    [
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "row_count", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "has_stale_columns", Type = DbType.Integer, Nullable = false },
    ];

    private static readonly ColumnDefinition[] SystemColumnStatsColumns =
    [
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "ordinal_position", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "distinct_count", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "non_null_count", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "min_value", Type = DbType.Null, Nullable = true },
        new ColumnDefinition { Name = "max_value", Type = DbType.Null, Nullable = true },
        new ColumnDefinition { Name = "is_stale", Type = DbType.Integer, Nullable = false },
    ];

    private static readonly ColumnDefinition[] SystemSavedQueriesColumns =
    [
        new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true, IsIdentity = true },
        new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "sql_text", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "created_utc", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "updated_utc", Type = DbType.Text, Nullable = false },
    ];

    private static readonly ColumnDefinition[] DefaultCountStarOutputSchema =
    [
        new ColumnDefinition
        {
            Name = "COUNT(*)",
            Type = DbType.Integer,
            Nullable = false,
        },
    ];

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly IRecordSerializer _recordSerializer;
    private readonly IRecordSerializer? _collectionReadSerializer;
    private readonly Func<string, long?>? _tableRowCountProvider;

    /// <summary>
    /// CTE materialized results, scoped to the current WITH query execution.
    /// Maps CTE name -> (rows, schema).
    /// </summary>
    private Dictionary<string, (List<DbValue[]> Rows, TableSchema Schema)>? _cteData;

    /// <summary>Recursion guard for trigger execution.</summary>
    private int _triggerDepth;
    private const int MaxTriggerDepth = 16;
    private const int MaxForeignKeyCascadeDepth = 64;

    /// <summary>Cache of parsed trigger bodies to avoid re-parsing on every row.</summary>
    private readonly Dictionary<string, List<Statement>> _triggerBodyCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _nextRowIdCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<CompiledExpressionCacheKey, Func<DbValue[], DbValue>> _compiledExpressionCache = new();
    private readonly Dictionary<CompiledExpressionCacheKey, SpanExpressionEvaluator> _compiledSpanExpressionCache = new();
    private readonly Dictionary<TableSchema, string> _qualifiedMappingFingerprintCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<TableSchema, ColumnDefinition[]> _tableSchemaArrayCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<(TableSchema Schema, int ColumnIndex), ColumnDefinition[]> _singleColumnOutputSchemaCache = new();
    private readonly Dictionary<Expression, bool> _requiresQualifiedMappingCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<TableRef, TableSchema> _correlationTableRefSchemaCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<QueryStatement, ColumnDefinition[]> _correlationQueryOutputSchemaCache = new(ReferenceEqualityComparer.Instance);
    private List<DbValue[]>? _systemTablesRowsCache;
    private List<DbValue[]>? _systemColumnsRowsCache;
    private List<DbValue[]>? _systemIndexesRowsCache;
    private List<DbValue[]>? _systemForeignKeysRowsCache;
    private List<DbValue[]>? _systemViewsRowsCache;
    private List<DbValue[]>? _systemTriggersRowsCache;
    private List<DbValue[]>? _systemObjectsRowsCache;
    private long? _systemColumnsCountCache;
    private long? _systemIndexesCountCache;
    private long? _systemForeignKeysCountCache;
    private readonly Dictionary<string, TableSchema> _systemCatalogSchemaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<SelectStatement, SelectPlanKind> _selectPlanCache = new(ReferenceEqualityComparer.Instance);
    private readonly Queue<SelectStatement> _selectPlanInsertionOrder = new();
    private long _selectPlanCacheHitCount;
    private long _selectPlanCacheMissCount;
    private long _selectPlanCacheReclassificationCount;
    private long _selectPlanCacheStoreCount;

    private readonly record struct CorrelationScope(DbValue[] Row, TableSchema Schema);
    private long _observedSchemaVersion;

    private const int MaxCompiledExpressionCacheEntries = 4096;
    private const int MaxSelectPlanCacheEntries = 1024;

    private enum SelectPlanKind
    {
        FastPrimaryKeyLookup,
        FastIndexedLookup,
        FastSimpleTableScan,
        SimpleSystemCatalogCountStar,
        SimpleCountStar,
        SimpleScalarAggregateColumn,
        SimpleLookupScalarAggregateColumn,
        SimpleGroupedIndexAggregate,
        SimpleConstantGroupAggregateColumn,
        General,
    }

    internal readonly record struct SelectPlanCacheDiagnostics(
        long HitCount,
        long MissCount,
        long ReclassificationCount,
        long StoreCount,
        int EntryCount);

    /// <summary>
    /// When true, simple PK equality lookups (SELECT * / PK-only projection WHERE pk = N) will try a synchronous
    /// cache-only path first, bypassing the async operator pipeline. Falls back to async on cache miss.
    /// </summary>
    public bool PreferSyncPointLookups { get; set; } = true;

    public QueryPlanner(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer? recordSerializer = null,
        Func<string, long?>? tableRowCountProvider = null)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _collectionReadSerializer = _recordSerializer is DefaultRecordSerializer
            ? new CollectionAwareRecordSerializer(_recordSerializer)
            : null;
        _tableRowCountProvider = tableRowCountProvider;
        _observedSchemaVersion = catalog.SchemaVersion;
    }

    public ValueTask<QueryResult> ExecuteAsync(Statement stmt, CancellationToken ct = default)
    {
        InvalidateSchemaSensitiveCachesIfNeeded();

        return stmt switch
        {
            CreateTableStatement create => ExecuteCreateTableAsync(create, ct),
            DropTableStatement drop => ExecuteDropTableAsync(drop, ct),
            InsertStatement insert => ExecuteInsertAsync(insert, persistRootChanges: true, ct),
            QueryStatement query => ExecuteQueryAsync(query, ct),
            DeleteStatement delete => ExecuteDeleteAsync(delete, ct),
            UpdateStatement update => ExecuteUpdateAsync(update, ct),
            AlterTableStatement alter => ExecuteAlterTableAsync(alter, ct),
            CreateIndexStatement createIdx => ExecuteCreateIndexAsync(createIdx, ct),
            DropIndexStatement dropIdx => ExecuteDropIndexAsync(dropIdx, ct),
            CreateViewStatement createView => ExecuteCreateViewAsync(createView, ct),
            DropViewStatement dropView => ExecuteDropViewAsync(dropView, ct),
            WithStatement with => ExecuteWithAsync(with, ct),
            CreateTriggerStatement createTrig => ExecuteCreateTriggerAsync(createTrig, ct),
            DropTriggerStatement dropTrig => ExecuteDropTriggerAsync(dropTrig, ct),
            AnalyzeStatement analyze => ExecuteAnalyzeAsync(analyze, ct),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown statement type: {stmt.GetType().Name}"),
        };
    }

    private ValueTask<QueryResult> ExecuteQueryAsync(QueryStatement stmt, CancellationToken ct)
    {
        if (ContainsSubqueries(stmt))
            return ExecuteQueryWithSubqueriesAsync(stmt, ct);

        return stmt switch
        {
            SelectStatement select => ValueTask.FromResult(ExecuteSelect(select)),
            CompoundSelectStatement compound => ExecuteCompoundSelectAsync(compound, ct),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {stmt.GetType().Name}"),
        };
    }

    private async ValueTask<QueryResult> ExecuteQueryWithSubqueriesAsync(QueryStatement stmt, CancellationToken ct)
    {
        var lowered = await RewriteSubqueriesInQueryAsync(stmt, ct);
        if (!ContainsSubqueries(lowered))
        {
            return lowered switch
            {
                SelectStatement select => ExecuteSelect(select),
                CompoundSelectStatement compound => await ExecuteCompoundSelectAsync(compound, ct),
                _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {lowered.GetType().Name}"),
            };
        }

        return lowered switch
        {
            SelectStatement select => await ExecuteSelectWithCorrelatedSubqueriesAsync(
                select,
                Array.Empty<CorrelationScope>(),
                ct),
            CompoundSelectStatement compound => await ExecuteCompoundSelectWithSubqueriesAsync(compound, ct),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {lowered.GetType().Name}"),
        };
    }

    internal SelectPlanCacheDiagnostics GetSelectPlanCacheDiagnostics()
        => new(
            _selectPlanCacheHitCount,
            _selectPlanCacheMissCount,
            _selectPlanCacheReclassificationCount,
            _selectPlanCacheStoreCount,
            _selectPlanCache.Count);

    internal void ResetSelectPlanCacheDiagnostics()
    {
        _selectPlanCacheHitCount = 0;
        _selectPlanCacheMissCount = 0;
        _selectPlanCacheReclassificationCount = 0;
        _selectPlanCacheStoreCount = 0;
    }

    private void InvalidateSchemaSensitiveCachesIfNeeded()
    {
        long currentVersion = _catalog.SchemaVersion;
        if (currentVersion == _observedSchemaVersion)
            return;

        _triggerBodyCache.Clear();
        _nextRowIdCache.Clear();
        _compiledExpressionCache.Clear();
        _compiledSpanExpressionCache.Clear();
        _qualifiedMappingFingerprintCache.Clear();
        _tableSchemaArrayCache.Clear();
        _singleColumnOutputSchemaCache.Clear();
        _requiresQualifiedMappingCache.Clear();
        _systemTablesRowsCache = null;
        _systemColumnsRowsCache = null;
        _systemIndexesRowsCache = null;
        _systemForeignKeysRowsCache = null;
        _systemViewsRowsCache = null;
        _systemTriggersRowsCache = null;
        _systemObjectsRowsCache = null;
        _systemColumnsCountCache = null;
        _systemIndexesCountCache = null;
        _systemForeignKeysCountCache = null;
        _selectPlanCache.Clear();
        _selectPlanInsertionOrder.Clear();

        _observedSchemaVersion = currentVersion;
    }

    private bool TryGetTableRowCount(string tableName, out long rowCount)
    {
        if (_tableRowCountProvider is not null)
        {
            long? provided = _tableRowCountProvider(tableName);
            if (provided.HasValue)
            {
                rowCount = provided.Value;
                return true;
            }
        }

        return _catalog.TryGetEstimatedTableRowCount(tableName, out rowCount);
    }

    private bool TryGetExactTableRowCount(string tableName, out long rowCount)
    {
        if (_tableRowCountProvider is not null)
        {
            long? provided = _tableRowCountProvider(tableName);
            if (provided.HasValue)
            {
                rowCount = provided.Value;
                return true;
            }
        }

        return _catalog.TryGetExactTableRowCount(tableName, out rowCount);
    }

    #region DDL — Tables

    private async ValueTask<QueryResult> ExecuteCreateTableAsync(CreateTableStatement stmt, CancellationToken ct)
    {
        if (stmt.IfNotExists && _catalog.GetTable(stmt.TableName) != null)
            return new QueryResult(0);

        var columns = stmt.Columns.Select(c => new ColumnDefinition
        {
            Name = c.Name,
            Type = MapType(c.TypeToken),
            IsPrimaryKey = c.IsPrimaryKey,
            IsIdentity = c.IsIdentity || (c.IsPrimaryKey && c.TypeToken == TokenType.Integer),
            Nullable = c.IsNullable,
            Collation = ValidateAndNormalizeColumnCollation(c.Name, c.TypeToken, c.Collation),
        }).ToArray();

        ForeignKeyDefinition[] foreignKeys = await BuildForeignKeysAsync(
            stmt.TableName,
            columns,
            stmt.Columns,
            Array.Empty<ForeignKeyDefinition>(),
            ct);

        var schema = new TableSchema
        {
            TableName = stmt.TableName,
            Columns = columns,
            ForeignKeys = foreignKeys,
            NextRowId = 1,
        };
        await _catalog.CreateTableAsync(schema, ct);
        for (int i = 0; i < foreignKeys.Length; i++)
            await CreateForeignKeySupportIndexAsync(schema, foreignKeys[i], ct);
        _nextRowIdCache.Remove(stmt.TableName);
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropTableAsync(DropTableStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetTable(stmt.TableName) == null)
            return new QueryResult(0);

        TableForeignKeyReference[] inboundReferences = _catalog.GetReferencingForeignKeys(stmt.TableName)
            .Where(reference => !string.Equals(reference.TableName, stmt.TableName, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        if (inboundReferences.Length > 0)
        {
            string constraintList = string.Join(", ", inboundReferences.Select(static reference => reference.ForeignKey.ConstraintName));
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Cannot drop table '{stmt.TableName}' because it is referenced by foreign key(s): {constraintList}.");
        }

        await _catalog.DropTableAsync(stmt.TableName, ct);
        _nextRowIdCache.Remove(stmt.TableName);
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteAlterTableAsync(AlterTableStatement stmt, CancellationToken ct)
    {
        var schema = GetSchema(stmt.TableName);

        switch (stmt.Action)
        {
            case AddColumnAction add:
            {
                // Check for duplicate column name
                if (schema.GetColumnIndex(add.Column.Name) >= 0)
                    throw new CSharpDbException(ErrorCode.SyntaxError, $"Column '{add.Column.Name}' already exists in table '{stmt.TableName}'.");

                var newCols = new List<ColumnDefinition>(schema.Columns);
                newCols.Add(new ColumnDefinition
                {
                    Name = add.Column.Name,
                    Type = MapType(add.Column.TypeToken),
                    IsPrimaryKey = add.Column.IsPrimaryKey,
                    IsIdentity = add.Column.IsIdentity || (add.Column.IsPrimaryKey && add.Column.TypeToken == TokenType.Integer),
                    Nullable = add.Column.IsNullable,
                    Collation = ValidateAndNormalizeColumnCollation(add.Column.Name, add.Column.TypeToken, add.Column.Collation),
                });

                ColumnDefinition[] newColumns = newCols.ToArray();
                ForeignKeyDefinition[] newForeignKeys = await BuildForeignKeysAsync(
                    stmt.TableName,
                    newColumns,
                    new[] { add.Column },
                    schema.ForeignKeys,
                    ct);

                var newSchema = new TableSchema
                {
                    TableName = stmt.TableName,
                    Columns = newColumns,
                    ForeignKeys = newForeignKeys,
                    NextRowId = schema.NextRowId,
                };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
                if (add.Column.ForeignKey is not null)
                    await CreateForeignKeySupportIndexAsync(newSchema, newForeignKeys[^1], ct);
                break;
            }

            case DropColumnAction drop:
            {
                int colIdx = schema.GetColumnIndex(drop.ColumnName);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{drop.ColumnName}' not found in table '{stmt.TableName}'.");

                if (schema.Columns[colIdx].IsPrimaryKey)
                    throw new CSharpDbException(ErrorCode.SyntaxError, "Cannot drop primary key column.");

                if (schema.ForeignKeys.Any(fk => string.Equals(fk.ColumnName, drop.ColumnName, StringComparison.OrdinalIgnoreCase)))
                    throw new CSharpDbException(ErrorCode.ConstraintViolation, $"Cannot drop column '{drop.ColumnName}' because it has a foreign key constraint.");

                if (_catalog.GetReferencingForeignKeys(stmt.TableName)
                    .Any(reference => string.Equals(reference.ForeignKey.ReferencedColumnName, drop.ColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    throw new CSharpDbException(
                        ErrorCode.ConstraintViolation,
                        $"Cannot drop column '{drop.ColumnName}' because it is referenced by a foreign key.");
                }

                var newCols = schema.Columns.Where((_, i) => i != colIdx).ToArray();
                if (newCols.Length == 0)
                    throw new CSharpDbException(ErrorCode.SyntaxError, "Cannot drop the last column of a table.");

                // Rewrite all rows without the dropped column
                var tree = _catalog.GetTableTree(stmt.TableName, _pager);
                int? rewriteCapacityHint = TryGetCachedTreeRowCountCapacityHint(tree);
                var scan = new TableScanOperator(tree, schema, GetReadSerializer(schema), rewriteCapacityHint);
                await scan.OpenAsync(ct);
                var rowsToRewrite = rewriteCapacityHint.HasValue
                    ? new List<(long rowId, DbValue[] newRow)>(rewriteCapacityHint.Value)
                    : new List<(long rowId, DbValue[] newRow)>();
                while (await scan.MoveNextAsync(ct))
                {
                    var oldRow = scan.Current;
                    var newRow = new DbValue[newCols.Length];
                    int dest = 0;
                    for (int i = 0; i < oldRow.Length && i < schema.Columns.Count; i++)
                    {
                        if (i == colIdx) continue;
                        if (dest < newRow.Length) newRow[dest++] = oldRow[i];
                    }
                    // Fill remaining with NULL (in case old row was short)
                    for (; dest < newRow.Length; dest++)
                        newRow[dest] = DbValue.Null;
                    rowsToRewrite.Add((scan.CurrentRowId, newRow));
                }

                foreach (var (rowId, newRow) in rowsToRewrite)
                {
                    await tree.DeleteAsync(rowId, ct);
                    await tree.InsertAsync(rowId, _recordSerializer.Encode(newRow), ct);
                }

                await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

                var newSchema = new TableSchema
                {
                    TableName = stmt.TableName,
                    Columns = newCols,
                    ForeignKeys = schema.ForeignKeys,
                    NextRowId = schema.NextRowId,
                };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
                break;
            }

            case DropConstraintAction dropConstraint:
            {
                ForeignKeyDefinition? foreignKey = schema.ForeignKeys.FirstOrDefault(fk =>
                    string.Equals(fk.ConstraintName, dropConstraint.ConstraintName, StringComparison.OrdinalIgnoreCase));
                if (foreignKey is null)
                {
                    throw new CSharpDbException(
                        ErrorCode.ConstraintViolation,
                        $"Constraint '{dropConstraint.ConstraintName}' not found on table '{stmt.TableName}'.");
                }

                ForeignKeyDefinition[] remainingForeignKeys = schema.ForeignKeys
                    .Where(fk => !string.Equals(fk.ConstraintName, dropConstraint.ConstraintName, StringComparison.OrdinalIgnoreCase))
                    .ToArray();

                var newSchema = new TableSchema
                {
                    TableName = stmt.TableName,
                    Columns = schema.Columns,
                    ForeignKeys = remainingForeignKeys,
                    NextRowId = schema.NextRowId,
                };

                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
                await _catalog.DropForeignKeyOwnedIndexAsync(foreignKey.SupportingIndexName, ct);
                break;
            }

            case RenameTableAction rename:
            {
                // Check new name doesn't already exist
                if (_catalog.GetTable(rename.NewTableName) != null)
                    throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Table '{rename.NewTableName}' already exists.");

                await RenameTableWithDependenciesAsync(stmt.TableName, rename.NewTableName, schema, ct);
                if (_nextRowIdCache.Remove(stmt.TableName, out long nextRowId))
                    _nextRowIdCache[rename.NewTableName] = nextRowId;
                break;
            }

            case RenameColumnAction renameCol:
            {
                int colIdx = schema.GetColumnIndex(renameCol.OldColumnName);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{renameCol.OldColumnName}' not found in table '{stmt.TableName}'.");

                // Check new column name doesn't already exist
                if (schema.GetColumnIndex(renameCol.NewColumnName) >= 0)
                    throw new CSharpDbException(ErrorCode.SyntaxError, $"Column '{renameCol.NewColumnName}' already exists in table '{stmt.TableName}'.");

                await RenameColumnWithDependenciesAsync(stmt.TableName, renameCol.OldColumnName, renameCol.NewColumnName, schema, ct);
                break;
            }

            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown alter action: {stmt.Action.GetType().Name}");
        }

        return new QueryResult(0);
    }

    #endregion

    #region DDL — Indexes

    private async ValueTask<QueryResult> ExecuteCreateIndexAsync(CreateIndexStatement stmt, CancellationToken ct)
    {
        if (stmt.IfNotExists && _catalog.GetIndex(stmt.IndexName) != null)
            return new QueryResult(0);

        var tableSchema = GetSchema(stmt.TableName);

        if (stmt.Columns.Count == 0)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Index must reference at least one column.");

        if (stmt.ColumnCollations.Count != 0 && stmt.ColumnCollations.Count != stmt.Columns.Count)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Index collation metadata must align with index columns.");

        // Validate columns exist, are supported index types, and are not duplicated.
        var indexColumnIndices = new int[stmt.Columns.Count];
        var indexColumnCollations = new string?[stmt.Columns.Count];
        for (int i = 0; i < stmt.Columns.Count; i++)
        {
            string columnName = stmt.Columns[i];
            int colIdx = tableSchema.GetColumnIndex(columnName);
            if (colIdx < 0)
                throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{columnName}' not found in table '{stmt.TableName}'.");

            DbType columnType = tableSchema.Columns[colIdx].Type;
            if (columnType is not (DbType.Integer or DbType.Text))
                throw new CSharpDbException(ErrorCode.TypeMismatch, "Only INTEGER and TEXT column indexes are supported.");

            string? columnCollation = i < stmt.ColumnCollations.Count
                ? NormalizeCollationName(stmt.ColumnCollations[i])
                : null;
            if (columnCollation != null && columnType != DbType.Text)
                throw new CSharpDbException(ErrorCode.TypeMismatch, $"COLLATE is only supported for TEXT index columns. Column '{columnName}' uses type '{columnType}'.");

            for (int j = 0; j < i; j++)
            {
                if (indexColumnIndices[j] == colIdx)
                    throw new CSharpDbException(ErrorCode.SyntaxError, $"Duplicate column '{columnName}' in index definition.");
            }

            indexColumnIndices[i] = colIdx;
            indexColumnCollations[i] = columnCollation;
        }

        var indexSchema = new IndexSchema
        {
            IndexName = stmt.IndexName,
            TableName = stmt.TableName,
            Columns = stmt.Columns,
            ColumnCollations = indexColumnCollations,
            IsUnique = stmt.IsUnique,
            Kind = IndexKind.Sql,
            OptionsJson = SqlIndexOptionsCodec.CreateDefaultOptionsJson(tableSchema, indexColumnIndices, indexColumnCollations),
        };

        await CreateAndBackfillIndexWithOrderedTextFallbackAsync(indexSchema, tableSchema, ct);

        await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropIndexAsync(DropIndexStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetIndex(stmt.IndexName) == null)
            return new QueryResult(0);

        ValidateParentIndexCanBeDropped(stmt.IndexName);
        await _catalog.DropIndexAsync(stmt.IndexName, ct);
        return new QueryResult(0);
    }

    #endregion

    private async ValueTask CreateAndBackfillIndexWithOrderedTextFallbackAsync(
        IndexSchema indexSchema,
        TableSchema tableSchema,
        CancellationToken ct)
    {
        await _catalog.CreateIndexAsync(indexSchema, ct);
        try
        {
            await IndexMaintenanceHelper.BackfillIndexAsync(
                _catalog,
                tableSchema,
                indexSchema,
                GetReadSerializer(tableSchema),
                ct);
        }
        catch (OrderedTextIndexOverflowException) when (CanFallbackToHashedSqlIndex(indexSchema, tableSchema))
        {
            await _catalog.DropIndexAsync(indexSchema.IndexName, ct);
            var fallbackSchema = CreateHashedSqlIndexSchema(indexSchema);
            await _catalog.CreateIndexAsync(fallbackSchema, ct);
            await IndexMaintenanceHelper.BackfillIndexAsync(
                _catalog,
                tableSchema,
                fallbackSchema,
                GetReadSerializer(tableSchema),
                ct);
        }
    }

    private static bool CanFallbackToHashedSqlIndex(IndexSchema indexSchema, TableSchema tableSchema)
        => indexSchema.Kind == IndexKind.Sql &&
           indexSchema.Columns.Count == 1 &&
           tableSchema.GetColumnIndex(indexSchema.Columns[0]) is int columnIndex &&
           columnIndex >= 0 &&
           columnIndex < tableSchema.Columns.Count &&
           tableSchema.Columns[columnIndex].Type == DbType.Text;

    private static IndexSchema CreateHashedSqlIndexSchema(IndexSchema indexSchema) =>
        new()
        {
            IndexName = indexSchema.IndexName,
            TableName = indexSchema.TableName,
            Columns = indexSchema.Columns,
            ColumnCollations = indexSchema.ColumnCollations,
            IsUnique = indexSchema.IsUnique,
            Kind = indexSchema.Kind,
            State = indexSchema.State,
            OwnerIndexName = indexSchema.OwnerIndexName,
            OptionsJson = null,
        };

    #region DDL — Views

    private async ValueTask<QueryResult> ExecuteCreateViewAsync(CreateViewStatement stmt, CancellationToken ct)
    {
        if (stmt.IfNotExists && _catalog.GetViewSql(stmt.ViewName) != null)
            return new QueryResult(0);

        string viewSql = QueryToSql(stmt.Query);

        await _catalog.CreateViewAsync(stmt.ViewName, viewSql, ct);
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropViewAsync(DropViewStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetViewSql(stmt.ViewName) == null)
            return new QueryResult(0);

        await _catalog.DropViewAsync(stmt.ViewName, ct);
        return new QueryResult(0);
    }

    private static string QueryToSql(QueryStatement stmt) => stmt switch
    {
        SelectStatement select => SelectToSql(select),
        CompoundSelectStatement compound => CompoundSelectToSql(compound),
        _ => throw new InvalidOperationException($"Cannot serialize query type: {stmt.GetType().Name}"),
    };

    private static string SelectToSql(SelectStatement stmt)
    {
        var parts = new List<string>();
        parts.Add("SELECT");
        if (stmt.IsDistinct)
            parts.Add("DISTINCT");

        // Columns
        var colParts = new List<string>();
        foreach (var col in stmt.Columns)
        {
            if (col.IsStar)
                colParts.Add("*");
            else
            {
                string expr = ExprToSql(col.Expression!);
                if (col.Alias != null)
                    expr += $" AS {col.Alias}";
                colParts.Add(expr);
            }
        }
        parts.Add(string.Join(", ", colParts));

        // FROM
        parts.Add("FROM");
        parts.Add(TableRefToSql(stmt.From));

        // WHERE
        if (stmt.Where != null)
        {
            parts.Add("WHERE");
            parts.Add(ExprToSql(stmt.Where));
        }

        // GROUP BY
        if (stmt.GroupBy != null)
        {
            parts.Add("GROUP BY");
            parts.Add(string.Join(", ", stmt.GroupBy.Select(ExprToSql)));
        }

        // HAVING
        if (stmt.Having != null)
        {
            parts.Add("HAVING");
            parts.Add(ExprToSql(stmt.Having));
        }

        AppendOrderingAndPagination(parts, stmt.OrderBy, stmt.Limit, stmt.Offset);

        return string.Join(" ", parts);
    }

    private static string CompoundSelectToSql(CompoundSelectStatement stmt)
    {
        var parts = new List<string>
        {
            QueryToSql(stmt.Left),
            SetOperationToSql(stmt.Operation),
            QueryToSql(stmt.Right),
        };

        AppendOrderingAndPagination(parts, stmt.OrderBy, stmt.Limit, stmt.Offset);
        return string.Join(" ", parts);
    }

    private static void AppendOrderingAndPagination(
        List<string> parts,
        List<OrderByClause>? orderBy,
        int? limit,
        int? offset)
    {
        if (orderBy != null)
        {
            parts.Add("ORDER BY");
            var orderParts = orderBy.Select(o =>
                ExprToSql(o.Expression) + (o.Descending ? " DESC" : ""));
            parts.Add(string.Join(", ", orderParts));
        }

        if (limit.HasValue)
            parts.Add($"LIMIT {limit.Value}");

        if (offset.HasValue)
            parts.Add($"OFFSET {offset.Value}");
    }

    private static string TableRefToSql(TableRef tableRef) => tableRef switch
    {
        SimpleTableRef s => s.Alias != null ? $"{s.TableName} AS {s.Alias}" : s.TableName,
        JoinTableRef j => $"{TableRefToSql(j.Left)} {JoinTypeToSql(j.JoinType)} {TableRefToSql(j.Right)}"
                          + (j.Condition != null ? $" ON {ExprToSql(j.Condition)}" : ""),
        _ => throw new InvalidOperationException(),
    };

    private static string JoinTypeToSql(JoinType jt) => jt switch
    {
        JoinType.Inner => "JOIN",
        JoinType.LeftOuter => "LEFT JOIN",
        JoinType.RightOuter => "RIGHT JOIN",
        JoinType.Cross => "CROSS JOIN",
        _ => "JOIN",
    };

    private static string SetOperationToSql(SetOperationKind operation) => operation switch
    {
        SetOperationKind.Union => "UNION",
        SetOperationKind.Intersect => "INTERSECT",
        SetOperationKind.Except => "EXCEPT",
        _ => throw new InvalidOperationException(),
    };

    private static string ExprToSql(Expression expr) => expr switch
    {
        LiteralExpression lit => lit.Value == null ? "NULL"
            : lit.LiteralType == TokenType.StringLiteral ? $"'{lit.Value.ToString()!.Replace("'", "''")}'"
            : lit.Value.ToString()!,
        ParameterExpression param => $"@{param.Name}",
        ColumnRefExpression col => col.TableAlias != null ? $"{col.TableAlias}.{col.ColumnName}" : col.ColumnName,
        BinaryExpression bin => $"({ExprToSql(bin.Left)} {BinaryOpToSql(bin.Op)} {ExprToSql(bin.Right)})",
        UnaryExpression un => un.Op == TokenType.Not ? $"NOT {ExprToSql(un.Operand)}" : $"-{ExprToSql(un.Operand)}",
        CollateExpression collate => $"{ExprToSql(collate.Operand)} COLLATE {collate.Collation}",
        FunctionCallExpression func => func.IsStarArg ? $"{func.FunctionName}(*)"
            : $"{func.FunctionName}({(func.IsDistinct ? "DISTINCT " : "")}{string.Join(", ", func.Arguments.Select(ExprToSql))})",
        LikeExpression like => $"{ExprToSql(like.Operand)}{(like.Negated ? " NOT" : "")} LIKE {ExprToSql(like.Pattern)}",
        InExpression inE => $"{ExprToSql(inE.Operand)}{(inE.Negated ? " NOT" : "")} IN ({string.Join(", ", inE.Values.Select(ExprToSql))})",
        InSubqueryExpression inSubquery => $"{ExprToSql(inSubquery.Operand)}{(inSubquery.Negated ? " NOT" : "")} IN ({QueryToSql(inSubquery.Query)})",
        ScalarSubqueryExpression scalarSubquery => $"({QueryToSql(scalarSubquery.Query)})",
        ExistsExpression exists => $"EXISTS ({QueryToSql(exists.Query)})",
        BetweenExpression bet => $"{ExprToSql(bet.Operand)}{(bet.Negated ? " NOT" : "")} BETWEEN {ExprToSql(bet.Low)} AND {ExprToSql(bet.High)}",
        IsNullExpression isn => $"{ExprToSql(isn.Operand)} IS{(isn.Negated ? " NOT" : "")} NULL",
        _ => throw new InvalidOperationException($"Cannot serialize expression: {expr.GetType().Name}"),
    };

    private static string BinaryOpToSql(BinaryOp op) => op switch
    {
        BinaryOp.Equals => "=",
        BinaryOp.NotEquals => "<>",
        BinaryOp.LessThan => "<",
        BinaryOp.GreaterThan => ">",
        BinaryOp.LessOrEqual => "<=",
        BinaryOp.GreaterOrEqual => ">=",
        BinaryOp.And => "AND",
        BinaryOp.Or => "OR",
        BinaryOp.Plus => "+",
        BinaryOp.Minus => "-",
        BinaryOp.Multiply => "*",
        BinaryOp.Divide => "/",
        _ => throw new InvalidOperationException(),
    };

    #endregion

    #region CTEs

    private async ValueTask<QueryResult> ExecuteWithAsync(WithStatement stmt, CancellationToken ct)
    {
        // Save previous CTE data (in case of nested WITH, though not expected)
        var previousCteData = _cteData;
        _cteData = new Dictionary<string, (List<DbValue[]> Rows, TableSchema Schema)>(StringComparer.OrdinalIgnoreCase);

        try
        {
            // Materialize each CTE
            foreach (var cte in stmt.Ctes)
            {
                await using var result = await ExecuteQueryAsync(cte.Query, ct);
                var rows = await result.ToListAsync(ct);

                // Build schema for this CTE
                ColumnDefinition[] cols;
                if (cte.ColumnNames != null)
                {
                    // Use explicit column names
                    cols = new ColumnDefinition[cte.ColumnNames.Count];
                    for (int i = 0; i < cols.Length; i++)
                    {
                        cols[i] = new ColumnDefinition
                        {
                            Name = cte.ColumnNames[i],
                            Type = i < result.Schema.Length ? result.Schema[i].Type : DbType.Null,
                            Nullable = true,
                        };
                    }
                }
                else
                {
                    // Use the output schema from the query
                    cols = result.Schema.ToArray();
                }

                var schema = new TableSchema { TableName = cte.Name, Columns = cols };
                _cteData[cte.Name] = (rows, schema);
            }

            // Execute the main query with CTE data available
            return await ExecuteQueryAsync(stmt.MainQuery, ct);
        }
        finally
        {
            _cteData = previousCteData;
        }
    }

    #endregion

    #region Subquery Lowering

    private async ValueTask<InsertStatement> RewriteSubqueriesInInsertAsync(InsertStatement stmt, CancellationToken ct)
    {
        var valueRows = new List<List<Expression>>(stmt.ValueRows.Count);
        for (int i = 0; i < stmt.ValueRows.Count; i++)
            valueRows.Add(await RewriteSubqueriesInExpressionListAsync(stmt.ValueRows[i], ct));

        return new InsertStatement
        {
            TableName = stmt.TableName,
            ColumnNames = stmt.ColumnNames,
            ValueRows = valueRows,
        };
    }

    private async ValueTask<DeleteStatement> RewriteSubqueriesInDeleteAsync(DeleteStatement stmt, CancellationToken ct)
    {
        return new DeleteStatement
        {
            TableName = stmt.TableName,
            Where = stmt.Where != null ? await RewriteSubqueriesInExpressionAsync(stmt.Where, ct) : null,
        };
    }

    private async ValueTask<UpdateStatement> RewriteSubqueriesInUpdateAsync(UpdateStatement stmt, CancellationToken ct)
    {
        var setClauses = new List<SetClause>(stmt.SetClauses.Count);
        for (int i = 0; i < stmt.SetClauses.Count; i++)
        {
            var setClause = stmt.SetClauses[i];
            setClauses.Add(new SetClause
            {
                ColumnName = setClause.ColumnName,
                Value = await RewriteSubqueriesInExpressionAsync(setClause.Value, ct),
            });
        }

        return new UpdateStatement
        {
            TableName = stmt.TableName,
            SetClauses = setClauses,
            Where = stmt.Where != null ? await RewriteSubqueriesInExpressionAsync(stmt.Where, ct) : null,
        };
    }

    private async ValueTask<QueryStatement> RewriteSubqueriesInQueryAsync(QueryStatement stmt, CancellationToken ct)
    {
        switch (stmt)
        {
            case SelectStatement select:
                return new SelectStatement
                {
                    IsDistinct = select.IsDistinct,
                    Columns = await RewriteSubqueriesInSelectColumnsAsync(select.Columns, ct),
                    From = await RewriteSubqueriesInTableRefAsync(select.From, ct),
                    Where = select.Where != null ? await RewriteSubqueriesInExpressionAsync(select.Where, ct) : null,
                    GroupBy = select.GroupBy != null ? await RewriteSubqueriesInExpressionListAsync(select.GroupBy, ct) : null,
                    Having = select.Having != null ? await RewriteSubqueriesInExpressionAsync(select.Having, ct) : null,
                    OrderBy = select.OrderBy != null ? await RewriteSubqueriesInOrderByClausesAsync(select.OrderBy, ct) : null,
                    Limit = select.Limit,
                    Offset = select.Offset,
                };
            case CompoundSelectStatement compound:
                return new CompoundSelectStatement
                {
                    Left = await RewriteSubqueriesInQueryAsync(compound.Left, ct),
                    Right = await RewriteSubqueriesInQueryAsync(compound.Right, ct),
                    Operation = compound.Operation,
                    OrderBy = compound.OrderBy != null ? await RewriteSubqueriesInOrderByClausesAsync(compound.OrderBy, ct) : null,
                    Limit = compound.Limit,
                    Offset = compound.Offset,
                };
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {stmt.GetType().Name}");
        }
    }

    private async ValueTask<TableRef> RewriteSubqueriesInTableRefAsync(TableRef tableRef, CancellationToken ct)
    {
        if (tableRef is not JoinTableRef join)
            return tableRef;

        return new JoinTableRef
        {
            Left = await RewriteSubqueriesInTableRefAsync(join.Left, ct),
            Right = await RewriteSubqueriesInTableRefAsync(join.Right, ct),
            JoinType = join.JoinType,
            Condition = join.Condition != null ? await RewriteSubqueriesInExpressionAsync(join.Condition, ct) : null,
        };
    }

    private async ValueTask<List<SelectColumn>> RewriteSubqueriesInSelectColumnsAsync(List<SelectColumn> columns, CancellationToken ct)
    {
        var rewritten = new List<SelectColumn>(columns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            rewritten.Add(new SelectColumn
            {
                IsStar = column.IsStar,
                Alias = column.Alias,
                Expression = column.Expression != null ? await RewriteSubqueriesInExpressionAsync(column.Expression, ct) : null,
            });
        }

        return rewritten;
    }

    private async ValueTask<List<OrderByClause>> RewriteSubqueriesInOrderByClausesAsync(List<OrderByClause> clauses, CancellationToken ct)
    {
        var rewritten = new List<OrderByClause>(clauses.Count);
        for (int i = 0; i < clauses.Count; i++)
        {
            rewritten.Add(new OrderByClause
            {
                Expression = await RewriteSubqueriesInExpressionAsync(clauses[i].Expression, ct),
                Descending = clauses[i].Descending,
            });
        }

        return rewritten;
    }

    private async ValueTask<List<Expression>> RewriteSubqueriesInExpressionListAsync(List<Expression> expressions, CancellationToken ct)
    {
        var rewritten = new List<Expression>(expressions.Count);
        for (int i = 0; i < expressions.Count; i++)
            rewritten.Add(await RewriteSubqueriesInExpressionAsync(expressions[i], ct));

        return rewritten;
    }

    private async ValueTask<Expression> RewriteSubqueriesInExpressionAsync(Expression expression, CancellationToken ct)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
            case ColumnRefExpression:
                return expression;
            case BinaryExpression binary:
                return new BinaryExpression
                {
                    Op = binary.Op,
                    Left = await RewriteSubqueriesInExpressionAsync(binary.Left, ct),
                    Right = await RewriteSubqueriesInExpressionAsync(binary.Right, ct),
                };
            case UnaryExpression unary:
                return new UnaryExpression
                {
                    Op = unary.Op,
                    Operand = await RewriteSubqueriesInExpressionAsync(unary.Operand, ct),
                };
            case CollateExpression collate:
                return new CollateExpression
                {
                    Operand = await RewriteSubqueriesInExpressionAsync(collate.Operand, ct),
                    Collation = collate.Collation,
                };
            case LikeExpression like:
                return new LikeExpression
                {
                    Operand = await RewriteSubqueriesInExpressionAsync(like.Operand, ct),
                    Pattern = await RewriteSubqueriesInExpressionAsync(like.Pattern, ct),
                    EscapeChar = like.EscapeChar != null ? await RewriteSubqueriesInExpressionAsync(like.EscapeChar, ct) : null,
                    Negated = like.Negated,
                };
            case InExpression inExpression:
                return new InExpression
                {
                    Operand = await RewriteSubqueriesInExpressionAsync(inExpression.Operand, ct),
                    Values = await RewriteSubqueriesInExpressionListAsync(inExpression.Values, ct),
                    Negated = inExpression.Negated,
                };
            case InSubqueryExpression inSubquery:
            {
                var operand = await RewriteSubqueriesInExpressionAsync(inSubquery.Operand, ct);
                var query = await RewriteSubqueriesInQueryAsync(inSubquery.Query, ct);
                if (!CanExecuteStandalone(query))
                {
                    return new InSubqueryExpression
                    {
                        Operand = operand,
                        Query = query,
                        Negated = inSubquery.Negated,
                    };
                }

                var values = await MaterializeSingleColumnSubqueryAsExpressionsAsync(query, ct);
                return new InExpression
                {
                    Operand = operand,
                    Values = values,
                    Negated = inSubquery.Negated,
                };
            }
            case ScalarSubqueryExpression scalarSubquery:
            {
                var query = await RewriteSubqueriesInQueryAsync(scalarSubquery.Query, ct);
                if (!CanExecuteStandalone(query))
                    return new ScalarSubqueryExpression { Query = query };

                var value = await ExecuteScalarSubqueryAsync(query, ct);
                return CreateLiteralExpression(value);
            }
            case ExistsExpression exists:
            {
                var query = await RewriteSubqueriesInQueryAsync(exists.Query, ct);
                if (!CanExecuteStandalone(query))
                    return new ExistsExpression { Query = query };

                bool subqueryHasRows = await ExecuteExistsSubqueryAsync(query, ct);
                return new LiteralExpression
                {
                    Value = subqueryHasRows ? 1L : 0L,
                    LiteralType = TokenType.IntegerLiteral,
                };
            }
            case BetweenExpression between:
                return new BetweenExpression
                {
                    Operand = await RewriteSubqueriesInExpressionAsync(between.Operand, ct),
                    Low = await RewriteSubqueriesInExpressionAsync(between.Low, ct),
                    High = await RewriteSubqueriesInExpressionAsync(between.High, ct),
                    Negated = between.Negated,
                };
            case IsNullExpression isNull:
                return new IsNullExpression
                {
                    Operand = await RewriteSubqueriesInExpressionAsync(isNull.Operand, ct),
                    Negated = isNull.Negated,
                };
            case FunctionCallExpression functionCall:
            {
                var args = new List<Expression>(functionCall.Arguments.Count);
                for (int i = 0; i < functionCall.Arguments.Count; i++)
                    args.Add(await RewriteSubqueriesInExpressionAsync(functionCall.Arguments[i], ct));

                return new FunctionCallExpression
                {
                    FunctionName = functionCall.FunctionName,
                    Arguments = args,
                    IsDistinct = functionCall.IsDistinct,
                    IsStarArg = functionCall.IsStarArg,
                };
            }
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown expression type: {expression.GetType().Name}");
        }
    }

    private async ValueTask<DbValue> ExecuteScalarSubqueryAsync(QueryStatement query, CancellationToken ct)
    {
        await using var result = await ExecuteQueryAsync(query, ct);
        EnsureSingleColumnSubquery(result.Schema, "Scalar subquery");

        if (!await result.MoveNextAsync(ct))
            return DbValue.Null;

        var value = result.Current[0];
        if (await result.MoveNextAsync(ct))
            throw new CSharpDbException(ErrorCode.SyntaxError, "Scalar subquery returned more than one row.");

        return value;
    }

    private async ValueTask<bool> ExecuteExistsSubqueryAsync(QueryStatement query, CancellationToken ct)
    {
        await using var result = await ExecuteQueryAsync(query, ct);
        return await result.MoveNextAsync(ct);
    }

    private async ValueTask<List<Expression>> MaterializeSingleColumnSubqueryAsExpressionsAsync(QueryStatement query, CancellationToken ct)
    {
        await using var result = await ExecuteQueryAsync(query, ct);
        EnsureSingleColumnSubquery(result.Schema, "IN subquery");

        var values = new List<Expression>();
        while (await result.MoveNextAsync(ct))
            values.Add(CreateLiteralExpression(result.Current[0]));

        return values;
    }

    private static void EnsureSingleColumnSubquery(ColumnDefinition[] schema, string description)
    {
        if (schema.Length != 1)
            throw new CSharpDbException(ErrorCode.SyntaxError, $"{description} must return exactly one column.");
    }

    private static LiteralExpression CreateLiteralExpression(DbValue value)
    {
        return value.Type switch
        {
            DbType.Null => new LiteralExpression { Value = null, LiteralType = TokenType.Null },
            DbType.Integer => new LiteralExpression { Value = value.AsInteger, LiteralType = TokenType.IntegerLiteral },
            DbType.Real => new LiteralExpression { Value = value.AsReal, LiteralType = TokenType.RealLiteral },
            DbType.Text => new LiteralExpression { Value = value.AsText, LiteralType = TokenType.StringLiteral },
            DbType.Blob => throw new CSharpDbException(ErrorCode.TypeMismatch, "Blob-valued subqueries are not supported in expressions."),
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported subquery value type '{value.Type}'."),
        };
    }

    private bool CanExecuteStandalone(QueryStatement query)
        => !QueryHasExternalReferences(query, Array.Empty<TableSchema>());

    private bool QueryHasExternalReferences(QueryStatement query, IReadOnlyList<TableSchema> ancestorQueryScopes)
    {
        switch (query)
        {
            case SelectStatement select:
            {
                var currentScope = ResolveCorrelationTableRefSchema(select.From);
                var visibleScopes = PrependVisibleScope(currentScope, ancestorQueryScopes);
                return TableRefHasExternalReferences(select.From, visibleScopes)
                    || select.Columns.Any(column => column.Expression != null && ExpressionHasExternalReferences(column.Expression, visibleScopes))
                    || (select.Where != null && ExpressionHasExternalReferences(select.Where, visibleScopes))
                    || (select.GroupBy != null && select.GroupBy.Any(expr => ExpressionHasExternalReferences(expr, visibleScopes)))
                    || (select.Having != null && ExpressionHasExternalReferences(select.Having, visibleScopes))
                    || (select.OrderBy != null && select.OrderBy.Any(orderBy => ExpressionHasExternalReferences(orderBy.Expression, visibleScopes)));
            }
            case CompoundSelectStatement compound:
            {
                var outputScope = CreateQueryOutputScope(compound);
                var visibleScopes = PrependVisibleScope(outputScope, ancestorQueryScopes);
                return QueryHasExternalReferences(compound.Left, ancestorQueryScopes)
                    || QueryHasExternalReferences(compound.Right, ancestorQueryScopes)
                    || (compound.OrderBy != null && compound.OrderBy.Any(orderBy => ExpressionHasExternalReferences(orderBy.Expression, visibleScopes)));
            }
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {query.GetType().Name}");
        }
    }

    private bool TableRefHasExternalReferences(TableRef tableRef, IReadOnlyList<TableSchema> visibleScopes)
    {
        if (tableRef is not JoinTableRef join)
            return false;

        return TableRefHasExternalReferences(join.Left, visibleScopes)
            || TableRefHasExternalReferences(join.Right, visibleScopes)
            || (join.Condition != null && ExpressionHasExternalReferences(join.Condition, visibleScopes));
    }

    private bool ExpressionHasExternalReferences(Expression expression, IReadOnlyList<TableSchema> visibleScopes)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return false;
            case ColumnRefExpression columnRef:
                return !CanResolveInVisibleScopes(columnRef, visibleScopes);
            case BinaryExpression binary:
                return ExpressionHasExternalReferences(binary.Left, visibleScopes)
                    || ExpressionHasExternalReferences(binary.Right, visibleScopes);
            case UnaryExpression unary:
                return ExpressionHasExternalReferences(unary.Operand, visibleScopes);
            case CollateExpression collate:
                return ExpressionHasExternalReferences(collate.Operand, visibleScopes);
            case LikeExpression like:
                return ExpressionHasExternalReferences(like.Operand, visibleScopes)
                    || ExpressionHasExternalReferences(like.Pattern, visibleScopes)
                    || (like.EscapeChar != null && ExpressionHasExternalReferences(like.EscapeChar, visibleScopes));
            case InExpression inExpression:
                return ExpressionHasExternalReferences(inExpression.Operand, visibleScopes)
                    || inExpression.Values.Any(value => ExpressionHasExternalReferences(value, visibleScopes));
            case InSubqueryExpression inSubquery:
                return ExpressionHasExternalReferences(inSubquery.Operand, visibleScopes)
                    || QueryHasExternalReferences(inSubquery.Query, visibleScopes);
            case ScalarSubqueryExpression scalarSubquery:
                return QueryHasExternalReferences(scalarSubquery.Query, visibleScopes);
            case ExistsExpression exists:
                return QueryHasExternalReferences(exists.Query, visibleScopes);
            case BetweenExpression between:
                return ExpressionHasExternalReferences(between.Operand, visibleScopes)
                    || ExpressionHasExternalReferences(between.Low, visibleScopes)
                    || ExpressionHasExternalReferences(between.High, visibleScopes);
            case IsNullExpression isNull:
                return ExpressionHasExternalReferences(isNull.Operand, visibleScopes);
            case FunctionCallExpression functionCall:
                return functionCall.Arguments.Any(argument => ExpressionHasExternalReferences(argument, visibleScopes));
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown expression type: {expression.GetType().Name}");
        }
    }

    private QueryStatement BindOuterScopesInQuery(QueryStatement query, IReadOnlyList<CorrelationScope> outerScopes)
        => BindOuterScopesInQuery(query, Array.Empty<TableSchema>(), outerScopes);

    private QueryStatement BindOuterScopesInQuery(
        QueryStatement query,
        IReadOnlyList<TableSchema> ancestorQueryScopes,
        IReadOnlyList<CorrelationScope> outerScopes)
    {
        switch (query)
        {
            case SelectStatement select:
            {
                var currentScope = ResolveCorrelationTableRefSchema(select.From);
                var visibleScopes = PrependVisibleScope(currentScope, ancestorQueryScopes);
                return new SelectStatement
                {
                    IsDistinct = select.IsDistinct,
                    Columns = select.Columns.Select(column => new SelectColumn
                    {
                        IsStar = column.IsStar,
                        Alias = column.Alias,
                        Expression = column.Expression != null
                            ? BindOuterScopesInExpression(column.Expression, visibleScopes, outerScopes)
                            : null,
                    }).ToList(),
                    From = BindOuterScopesInTableRef(select.From, visibleScopes, outerScopes),
                    Where = select.Where != null ? BindOuterScopesInExpression(select.Where, visibleScopes, outerScopes) : null,
                    GroupBy = select.GroupBy?.Select(expr => BindOuterScopesInExpression(expr, visibleScopes, outerScopes)).ToList(),
                    Having = select.Having != null ? BindOuterScopesInExpression(select.Having, visibleScopes, outerScopes) : null,
                    OrderBy = select.OrderBy?.Select(orderBy => new OrderByClause
                    {
                        Expression = BindOuterScopesInExpression(orderBy.Expression, visibleScopes, outerScopes),
                        Descending = orderBy.Descending,
                    }).ToList(),
                    Limit = select.Limit,
                    Offset = select.Offset,
                };
            }
            case CompoundSelectStatement compound:
            {
                var outputScope = CreateQueryOutputScope(compound);
                var visibleScopes = PrependVisibleScope(outputScope, ancestorQueryScopes);
                return new CompoundSelectStatement
                {
                    Left = BindOuterScopesInQuery(compound.Left, ancestorQueryScopes, outerScopes),
                    Right = BindOuterScopesInQuery(compound.Right, ancestorQueryScopes, outerScopes),
                    Operation = compound.Operation,
                    OrderBy = compound.OrderBy?.Select(orderBy => new OrderByClause
                    {
                        Expression = BindOuterScopesInExpression(orderBy.Expression, visibleScopes, outerScopes),
                        Descending = orderBy.Descending,
                    }).ToList(),
                    Limit = compound.Limit,
                    Offset = compound.Offset,
                };
            }
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {query.GetType().Name}");
        }
    }

    private TableRef BindOuterScopesInTableRef(
        TableRef tableRef,
        IReadOnlyList<TableSchema> visibleScopes,
        IReadOnlyList<CorrelationScope> outerScopes)
    {
        if (tableRef is not JoinTableRef join)
            return tableRef;

        return new JoinTableRef
        {
            Left = BindOuterScopesInTableRef(join.Left, visibleScopes, outerScopes),
            Right = BindOuterScopesInTableRef(join.Right, visibleScopes, outerScopes),
            JoinType = join.JoinType,
            Condition = join.Condition != null ? BindOuterScopesInExpression(join.Condition, visibleScopes, outerScopes) : null,
        };
    }

    private Expression BindOuterScopesInExpression(
        Expression expression,
        IReadOnlyList<TableSchema> visibleScopes,
        IReadOnlyList<CorrelationScope> outerScopes)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
                return expression;
            case ColumnRefExpression columnRef:
                return CanResolveInVisibleScopes(columnRef, visibleScopes)
                    ? expression
                    : TryResolveOuterScopeValue(columnRef, outerScopes, out var outerValue)
                        ? CreateLiteralExpression(outerValue)
                        : expression;
            case BinaryExpression binary:
                return new BinaryExpression
                {
                    Op = binary.Op,
                    Left = BindOuterScopesInExpression(binary.Left, visibleScopes, outerScopes),
                    Right = BindOuterScopesInExpression(binary.Right, visibleScopes, outerScopes),
                };
            case UnaryExpression unary:
                return new UnaryExpression
                {
                    Op = unary.Op,
                    Operand = BindOuterScopesInExpression(unary.Operand, visibleScopes, outerScopes),
                };
            case CollateExpression collate:
                return new CollateExpression
                {
                    Operand = BindOuterScopesInExpression(collate.Operand, visibleScopes, outerScopes),
                    Collation = collate.Collation,
                };
            case LikeExpression like:
                return new LikeExpression
                {
                    Operand = BindOuterScopesInExpression(like.Operand, visibleScopes, outerScopes),
                    Pattern = BindOuterScopesInExpression(like.Pattern, visibleScopes, outerScopes),
                    EscapeChar = like.EscapeChar != null
                        ? BindOuterScopesInExpression(like.EscapeChar, visibleScopes, outerScopes)
                        : null,
                    Negated = like.Negated,
                };
            case InExpression inExpression:
                return new InExpression
                {
                    Operand = BindOuterScopesInExpression(inExpression.Operand, visibleScopes, outerScopes),
                    Values = inExpression.Values
                        .Select(value => BindOuterScopesInExpression(value, visibleScopes, outerScopes))
                        .ToList(),
                    Negated = inExpression.Negated,
                };
            case InSubqueryExpression inSubquery:
                return new InSubqueryExpression
                {
                    Operand = BindOuterScopesInExpression(inSubquery.Operand, visibleScopes, outerScopes),
                    Query = BindOuterScopesInQuery(inSubquery.Query, visibleScopes, outerScopes),
                    Negated = inSubquery.Negated,
                };
            case ScalarSubqueryExpression scalarSubquery:
                return new ScalarSubqueryExpression
                {
                    Query = BindOuterScopesInQuery(scalarSubquery.Query, visibleScopes, outerScopes),
                };
            case ExistsExpression exists:
                return new ExistsExpression
                {
                    Query = BindOuterScopesInQuery(exists.Query, visibleScopes, outerScopes),
                };
            case BetweenExpression between:
                return new BetweenExpression
                {
                    Operand = BindOuterScopesInExpression(between.Operand, visibleScopes, outerScopes),
                    Low = BindOuterScopesInExpression(between.Low, visibleScopes, outerScopes),
                    High = BindOuterScopesInExpression(between.High, visibleScopes, outerScopes),
                    Negated = between.Negated,
                };
            case IsNullExpression isNull:
                return new IsNullExpression
                {
                    Operand = BindOuterScopesInExpression(isNull.Operand, visibleScopes, outerScopes),
                    Negated = isNull.Negated,
                };
            case FunctionCallExpression functionCall:
                return new FunctionCallExpression
                {
                    FunctionName = functionCall.FunctionName,
                    Arguments = functionCall.Arguments
                        .Select(argument => BindOuterScopesInExpression(argument, visibleScopes, outerScopes))
                        .ToList(),
                    IsDistinct = functionCall.IsDistinct,
                    IsStarArg = functionCall.IsStarArg,
                };
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown expression type: {expression.GetType().Name}");
        }
    }

    private TableSchema ResolveCorrelationTableRefSchema(TableRef tableRef)
    {
        if (_correlationTableRefSchemaCache.TryGetValue(tableRef, out var cached))
            return cached;

        TableSchema resolved;
        switch (tableRef)
        {
            case SimpleTableRef simple:
                resolved = ResolveCorrelationSimpleTableSchema(simple);
                break;
            case JoinTableRef join:
                var left = ResolveCorrelationTableRefSchema(join.Left);
                var right = ResolveCorrelationTableRefSchema(join.Right);
                resolved = TableSchema.CreateJoinSchema(left, right);
                break;
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown table ref type: {tableRef.GetType().Name}");
        }

        _correlationTableRefSchemaCache[tableRef] = resolved;
        return resolved;
    }

    private TableSchema ResolveCorrelationSimpleTableSchema(SimpleTableRef tableRef)
    {
        if (_cteData != null && _cteData.TryGetValue(tableRef.TableName, out var cteInfo))
            return CreateQualifiedSchema(cteInfo.Schema.TableName, cteInfo.Schema.Columns, tableRef.Alias ?? tableRef.TableName);

        if (TryBuildSystemCatalogSource(tableRef, out var systemSource))
            return systemSource.schema;

        var viewSql = _catalog.GetViewSql(tableRef.TableName);
        if (viewSql != null)
        {
            var viewQuery = Parser.Parse(viewSql) as QueryStatement
                ?? throw new CSharpDbException(ErrorCode.SyntaxError, $"View '{tableRef.TableName}' does not contain a query definition.");
            var outputColumns = ResolveCorrelationQueryOutputSchema(viewQuery);
            return CreateQualifiedSchema(tableRef.TableName, outputColumns, tableRef.Alias ?? tableRef.TableName);
        }

        var baseSchema = GetSchema(tableRef.TableName);
        return CreateQualifiedSchema(baseSchema.TableName, baseSchema.Columns, tableRef.Alias ?? tableRef.TableName);
    }

    private ColumnDefinition[] ResolveCorrelationQueryOutputSchema(QueryStatement query)
    {
        if (_correlationQueryOutputSchemaCache.TryGetValue(query, out var cached))
            return cached;

        ColumnDefinition[] output;
        switch (query)
        {
            case SelectStatement select:
            {
                var sourceSchema = ResolveCorrelationTableRefSchema(select.From);
                bool hasAggregates = select.GroupBy != null ||
                                     select.Having != null ||
                                     select.Columns.Any(column => column.Expression != null && ContainsAggregate(column.Expression));
                if (hasAggregates)
                {
                    output = BuildAggregateOutputSchema(select.Columns, sourceSchema);
                }
                else if (select.Columns.Any(column => column.IsStar))
                {
                    output = GetSchemaColumnsArray(sourceSchema);
                }
                else if (TryBuildColumnProjection(select.Columns, sourceSchema, out _, out var projectedColumns))
                {
                    output = projectedColumns;
                }
                else
                {
                    var expressions = select.Columns.Select(column => column.Expression!).ToArray();
                    output = new ColumnDefinition[expressions.Length];
                    for (int i = 0; i < expressions.Length; i++)
                        output[i] = InferColumnDef(expressions[i], select.Columns[i].Alias, sourceSchema, i);
                }

                break;
            }
            case CompoundSelectStatement compound:
                output = MergeCompoundSchemas(
                    ResolveCorrelationQueryOutputSchema(compound.Left),
                    ResolveCorrelationQueryOutputSchema(compound.Right));
                break;
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {query.GetType().Name}");
        }

        _correlationQueryOutputSchemaCache[query] = output;
        return output;
    }

    private static TableSchema CreateQualifiedSchema(
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        string qualifier)
    {
        var qualifiedMappings = new Dictionary<string, int>(columns.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Count; i++)
            qualifiedMappings[$"{qualifier}.{columns[i].Name}"] = i;

        return new TableSchema
        {
            TableName = tableName,
            Columns = columns,
            QualifiedMappings = qualifiedMappings,
        };
    }

    private TableSchema CreateQueryOutputScope(QueryStatement query)
        => new()
        {
            TableName = "query",
            Columns = ResolveCorrelationQueryOutputSchema(query),
        };

    private static TableSchema[] PrependVisibleScope(TableSchema currentScope, IReadOnlyList<TableSchema> ancestorScopes)
    {
        var visibleScopes = new TableSchema[ancestorScopes.Count + 1];
        visibleScopes[0] = currentScope;
        for (int i = 0; i < ancestorScopes.Count; i++)
            visibleScopes[i + 1] = ancestorScopes[i];

        return visibleScopes;
    }

    private static bool CanResolveInVisibleScopes(ColumnRefExpression columnRef, IReadOnlyList<TableSchema> visibleScopes)
    {
        for (int i = 0; i < visibleScopes.Count; i++)
        {
            if (TryResolveColumnIndex(columnRef, visibleScopes[i], out _))
                return true;
        }

        return false;
    }

    private static bool TryResolveOuterScopeValue(
        ColumnRefExpression columnRef,
        IReadOnlyList<CorrelationScope> outerScopes,
        out DbValue value)
    {
        for (int i = 0; i < outerScopes.Count; i++)
        {
            var scope = outerScopes[i];
            if (!TryResolveColumnIndex(columnRef, scope.Schema, out int index))
                continue;

            value = index < scope.Row.Length ? scope.Row[index] : DbValue.Null;
            return true;
        }

        value = DbValue.Null;
        return false;
    }

    private static bool TryResolveColumnIndex(ColumnRefExpression columnRef, TableSchema schema, out int index)
    {
        index = columnRef.TableAlias != null
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);
        return index >= 0;
    }

    private static bool ContainsSubqueries(Statement statement) => statement switch
    {
        QueryStatement query => ContainsSubqueries(query),
        InsertStatement insert => insert.ValueRows.Any(row => row.Any(ContainsSubqueries)),
        DeleteStatement delete => delete.Where != null && ContainsSubqueries(delete.Where),
        UpdateStatement update => update.SetClauses.Any(set => ContainsSubqueries(set.Value))
            || (update.Where != null && ContainsSubqueries(update.Where)),
        _ => false,
    };

    private static bool ContainsSubqueries(QueryStatement statement) => statement switch
    {
        SelectStatement select => select.Columns.Any(c => c.Expression != null && ContainsSubqueries(c.Expression))
            || ContainsSubqueries(select.From)
            || (select.Where != null && ContainsSubqueries(select.Where))
            || (select.GroupBy != null && select.GroupBy.Any(ContainsSubqueries))
            || (select.Having != null && ContainsSubqueries(select.Having))
            || (select.OrderBy != null && select.OrderBy.Any(o => ContainsSubqueries(o.Expression))),
        CompoundSelectStatement compound => ContainsSubqueries(compound.Left)
            || ContainsSubqueries(compound.Right)
            || (compound.OrderBy != null && compound.OrderBy.Any(o => ContainsSubqueries(o.Expression))),
        _ => false,
    };

    private static bool ContainsSubqueries(TableRef tableRef) => tableRef switch
    {
        JoinTableRef join => ContainsSubqueries(join.Left)
            || ContainsSubqueries(join.Right)
            || (join.Condition != null && ContainsSubqueries(join.Condition)),
        _ => false,
    };

    private static bool ContainsSubqueries(Expression expression) => expression switch
    {
        InSubqueryExpression => true,
        ScalarSubqueryExpression => true,
        ExistsExpression => true,
        BinaryExpression binary => ContainsSubqueries(binary.Left) || ContainsSubqueries(binary.Right),
        UnaryExpression unary => ContainsSubqueries(unary.Operand),
        CollateExpression collate => ContainsSubqueries(collate.Operand),
        LikeExpression like => ContainsSubqueries(like.Operand)
            || ContainsSubqueries(like.Pattern)
            || (like.EscapeChar != null && ContainsSubqueries(like.EscapeChar)),
        InExpression inExpression => ContainsSubqueries(inExpression.Operand)
            || inExpression.Values.Any(ContainsSubqueries),
        BetweenExpression between => ContainsSubqueries(between.Operand)
            || ContainsSubqueries(between.Low)
            || ContainsSubqueries(between.High),
        IsNullExpression isNull => ContainsSubqueries(isNull.Operand),
        FunctionCallExpression functionCall => functionCall.Arguments.Any(ContainsSubqueries),
        _ => false,
    };

    #endregion

    #region DDL — Triggers

    private async ValueTask<QueryResult> ExecuteCreateTriggerAsync(CreateTriggerStatement stmt, CancellationToken ct)
    {
        if (stmt.IfNotExists && _catalog.GetTrigger(stmt.TriggerName) != null)
            return new QueryResult(0);

        // Validate the target table exists
        GetSchema(stmt.TableName);

        // Serialize the trigger body statements back to SQL text for storage
        string bodySql = SerializeTriggerBody(stmt);

        var schema = new TriggerSchema
        {
            TriggerName = stmt.TriggerName,
            TableName = stmt.TableName,
            Timing = stmt.Timing,
            Event = stmt.Event,
            BodySql = bodySql,
        };

        await _catalog.CreateTriggerAsync(schema, ct);
        _triggerBodyCache.Remove(stmt.TriggerName); // invalidate cache
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropTriggerAsync(DropTriggerStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetTrigger(stmt.TriggerName) == null)
            return new QueryResult(0);

        await _catalog.DropTriggerAsync(stmt.TriggerName, ct);
        _triggerBodyCache.Remove(stmt.TriggerName);
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteAnalyzeAsync(AnalyzeStatement stmt, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(stmt.TableName))
        {
            await AnalyzeTableAsync(stmt.TableName, ct);
            return new QueryResult(0);
        }

        foreach (string tableName in _catalog.GetTableNames().ToArray())
            await AnalyzeTableAsync(tableName, ct);

        return new QueryResult(0);
    }

    private async ValueTask AnalyzeTableAsync(string tableName, CancellationToken ct)
    {
        if (_catalog.IsView(tableName) || IsSystemCatalogTable(tableName))
            throw new CSharpDbException(ErrorCode.SyntaxError, $"ANALYZE does not support '{tableName}'.");

        var schema = GetSchema(tableName);
        var tree = _catalog.GetTableTree(tableName, _pager);
        var columnStats = await CollectColumnStatisticsAsync(tableName, schema, tree, ct);
        long rowCount = columnStats.RowCount;
        await _catalog.SetTableRowCountAsync(tableName, rowCount, ct);
        await _catalog.ReplaceColumnStatisticsAsync(tableName, columnStats.Columns, ct);
        await _catalog.PersistDirtyAdvisoryStatisticsAsync(ct);
    }

    private async ValueTask<(long RowCount, ColumnStatistics[] Columns)> CollectColumnStatisticsAsync(
        string tableName,
        TableSchema schema,
        BTree tree,
        CancellationToken ct)
    {
        int columnCount = schema.Columns.Count;
        var distinctSets = new HashSet<DbValue>[columnCount];
        var nonNullCounts = new long[columnCount];
        var minValues = new DbValue[columnCount];
        var maxValues = new DbValue[columnCount];
        var hasValues = new bool[columnCount];

        int? scanCapacityHint = TryGetCachedTreeRowCountCapacityHint(tree);
        var scan = new TableScanOperator(tree, schema, GetReadSerializer(schema), scanCapacityHint);
        await scan.OpenAsync(ct);

        long rowCount = 0;
        while (await scan.MoveNextAsync(ct))
        {
            rowCount++;
            var row = scan.Current;
            for (int i = 0; i < columnCount; i++)
            {
                DbValue value = row[i];
                if (value.IsNull)
                    continue;

                nonNullCounts[i]++;
                (distinctSets[i] ??= new HashSet<DbValue>()).Add(value);

                if (!hasValues[i])
                {
                    minValues[i] = value;
                    maxValues[i] = value;
                    hasValues[i] = true;
                    continue;
                }

                if (DbValue.Compare(value, minValues[i]) < 0)
                    minValues[i] = value;
                if (DbValue.Compare(value, maxValues[i]) > 0)
                    maxValues[i] = value;
            }
        }

        var result = new ColumnStatistics[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            result[i] = new ColumnStatistics
            {
                TableName = tableName,
                ColumnName = schema.Columns[i].Name,
                DistinctCount = distinctSets[i]?.Count ?? 0,
                NonNullCount = nonNullCounts[i],
                MinValue = hasValues[i] ? minValues[i] : DbValue.Null,
                MaxValue = hasValues[i] ? maxValues[i] : DbValue.Null,
                IsStale = false,
            };
        }

        return (rowCount, result);
    }

    /// <summary>
    /// Serialize a trigger's body statements to a SQL string for catalog storage.
    /// </summary>
    private static string SerializeTriggerBody(CreateTriggerStatement stmt)
    {
        var parts = new List<string>();
        foreach (var bodyStmt in stmt.Body)
        {
            parts.Add(bodyStmt switch
            {
                InsertStatement ins => SerializeInsertToSql(ins),
                UpdateStatement upd => SerializeUpdateToSql(upd),
                DeleteStatement del => SerializeDeleteToSql(del),
                _ => throw new CSharpDbException(ErrorCode.SyntaxError, $"Unsupported statement type in trigger body: {bodyStmt.GetType().Name}"),
            });
        }
        return string.Join("; ", parts);
    }

    private static string SerializeInsertToSql(InsertStatement ins)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append($"INSERT INTO {ins.TableName}");
        if (ins.ColumnNames != null)
            sb.Append($" ({string.Join(", ", ins.ColumnNames)})");
        sb.Append(" VALUES ");
        var rowParts = ins.ValueRows.Select(row => $"({string.Join(", ", row.Select(ExprToSql))})");
        sb.Append(string.Join(", ", rowParts));
        return sb.ToString();
    }

    private static string SerializeUpdateToSql(UpdateStatement upd)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append($"UPDATE {upd.TableName} SET ");
        sb.Append(string.Join(", ", upd.SetClauses.Select(s => $"{s.ColumnName} = {ExprToSql(s.Value)}")));
        if (upd.Where != null) sb.Append($" WHERE {ExprToSql(upd.Where)}");
        return sb.ToString();
    }

    private static string SerializeDeleteToSql(DeleteStatement del)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append($"DELETE FROM {del.TableName}");
        if (del.Where != null) sb.Append($" WHERE {ExprToSql(del.Where)}");
        return sb.ToString();
    }

    /// <summary>
    /// Fires matching triggers for a given event timing.
    /// </summary>
    private async ValueTask FireTriggersAsync(
        string tableName, TriggerTiming timing, TriggerEvent evt,
        DbValue[]? oldRow, DbValue[]? newRow, TableSchema schema, CancellationToken ct)
    {
        var triggers = _catalog.GetTriggersForTable(tableName);
        foreach (var trigger in triggers)
        {
            if (trigger.Timing != timing || trigger.Event != evt) continue;
            await ExecuteTriggerBodyAsync(trigger, oldRow, newRow, schema, ct);
        }
    }

    /// <summary>
    /// Executes a trigger body with NEW/OLD row bindings.
    /// </summary>
    private async ValueTask ExecuteTriggerBodyAsync(
        TriggerSchema trigger, DbValue[]? oldRow, DbValue[]? newRow, TableSchema tableSchema, CancellationToken ct)
    {
        _triggerDepth++;
        if (_triggerDepth > MaxTriggerDepth)
        {
            _triggerDepth--;
            throw new CSharpDbException(ErrorCode.SyntaxError, "Maximum trigger recursion depth exceeded.");
        }

        try
        {
            // Parse the trigger body (cache it)
            if (!_triggerBodyCache.TryGetValue(trigger.TriggerName, out var bodyStatements))
            {
                bodyStatements = ParseTriggerBody(trigger.BodySql);
                _triggerBodyCache[trigger.TriggerName] = bodyStatements;
            }

            // Check WHEN condition if present in the original trigger definition
            // (For MVP, WHEN is evaluated at creation time and stored as part of the body check)
            // We can add WHEN support later if needed

            // Build a composite schema that can resolve NEW.col and OLD.col
            var compositeSchema = BuildTriggerSchema(tableSchema, oldRow != null, newRow != null);
            var compositeRow = BuildTriggerRow(tableSchema, oldRow, newRow);

            // Execute each statement, substituting NEW/OLD references
            foreach (var stmt in bodyStatements)
            {
                var resolved = ResolveNewOldReferences(stmt, compositeRow, compositeSchema, tableSchema);
                await ExecuteAsync(resolved, ct);
            }
        }
        finally
        {
            _triggerDepth--;
        }
    }

    private static List<Statement> ParseTriggerBody(string bodySql)
    {
        // Body is stored as "stmt1; stmt2; ..."
        // Split and parse each individually
        var statements = new List<Statement>();
        var parts = bodySql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var part in parts)
        {
            if (string.IsNullOrWhiteSpace(part)) continue;
            statements.Add(Parser.Parse(part));
        }
        return statements;
    }

    /// <summary>
    /// Builds a composite schema with qualified NEW.col and OLD.col mappings.
    /// </summary>
    private static TableSchema BuildTriggerSchema(TableSchema tableSchema, bool hasOld, bool hasNew)
    {
        var qualified = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        int offset = 0;

        // NEW columns (indices 0..N-1)
        if (hasNew)
        {
            for (int i = 0; i < tableSchema.Columns.Count; i++)
                qualified[$"NEW.{tableSchema.Columns[i].Name}"] = offset + i;
            offset += tableSchema.Columns.Count;
        }

        // OLD columns (indices N..2N-1)
        if (hasOld)
        {
            for (int i = 0; i < tableSchema.Columns.Count; i++)
                qualified[$"OLD.{tableSchema.Columns[i].Name}"] = offset + i;
        }

        return new TableSchema
        {
            TableName = tableSchema.TableName,
            Columns = tableSchema.Columns,
            QualifiedMappings = qualified,
        };
    }

    /// <summary>
    /// Builds a composite row with [NEW values...] [OLD values...] for expression evaluation.
    /// </summary>
    private static DbValue[] BuildTriggerRow(TableSchema tableSchema, DbValue[]? oldRow, DbValue[]? newRow)
    {
        int colCount = tableSchema.Columns.Count;
        int totalCols = (newRow != null ? colCount : 0) + (oldRow != null ? colCount : 0);
        var row = new DbValue[totalCols];
        int offset = 0;

        if (newRow != null)
        {
            Array.Copy(newRow, 0, row, offset, colCount);
            offset += colCount;
        }
        if (oldRow != null)
        {
            Array.Copy(oldRow, 0, row, offset, colCount);
        }

        return row;
    }

    /// <summary>
    /// Resolves NEW.col and OLD.col references in a trigger body statement.
    /// For INSERT statements, evaluates expressions with the trigger's composite row/schema.
    /// </summary>
    private static Statement ResolveNewOldReferences(
        Statement stmt, DbValue[] compositeRow, TableSchema compositeSchema, TableSchema tableSchema)
    {
        switch (stmt)
        {
            case InsertStatement ins:
                var resolvedRows = ins.ValueRows.Select(row =>
                    row.Select(expr => ResolveNewOldRefsInExpression(expr, compositeRow, compositeSchema)).ToList()
                ).ToList();
                return new InsertStatement
                {
                    TableName = ins.TableName,
                    ColumnNames = ins.ColumnNames,
                    ValueRows = resolvedRows,
                };

            case UpdateStatement upd:
                var resolvedSets = upd.SetClauses.Select(s => new SetClause
                {
                    ColumnName = s.ColumnName,
                    Value = ResolveNewOldRefsInExpression(s.Value, compositeRow, compositeSchema),
                }).ToList();
                return new UpdateStatement
                {
                    TableName = upd.TableName,
                    SetClauses = resolvedSets,
                    Where = upd.Where != null ? ResolveNewOldRefsInExpression(upd.Where, compositeRow, compositeSchema) : null,
                };

            case DeleteStatement del:
                return new DeleteStatement
                {
                    TableName = del.TableName,
                    Where = del.Where != null ? ResolveNewOldRefsInExpression(del.Where, compositeRow, compositeSchema) : null,
                };

            default:
                return stmt;
        }
    }

    private static Expression ResolveNewOldRefsInExpression(Expression expr, DbValue[] compositeRow, TableSchema compositeSchema)
    {
        switch (expr)
        {
            case ColumnRefExpression col when IsNewOldColumnRef(col):
                return CreateLiteralExpression(ExpressionEvaluator.Evaluate(col, compositeRow, compositeSchema));
            case BinaryExpression binary:
                return new BinaryExpression
                {
                    Op = binary.Op,
                    Left = ResolveNewOldRefsInExpression(binary.Left, compositeRow, compositeSchema),
                    Right = ResolveNewOldRefsInExpression(binary.Right, compositeRow, compositeSchema),
                };
            case UnaryExpression unary:
                return new UnaryExpression
                {
                    Op = unary.Op,
                    Operand = ResolveNewOldRefsInExpression(unary.Operand, compositeRow, compositeSchema),
                };
            case CollateExpression collate:
                return new CollateExpression
                {
                    Operand = ResolveNewOldRefsInExpression(collate.Operand, compositeRow, compositeSchema),
                    Collation = collate.Collation,
                };
            case LikeExpression like:
                return new LikeExpression
                {
                    Operand = ResolveNewOldRefsInExpression(like.Operand, compositeRow, compositeSchema),
                    Pattern = ResolveNewOldRefsInExpression(like.Pattern, compositeRow, compositeSchema),
                    EscapeChar = like.EscapeChar != null
                        ? ResolveNewOldRefsInExpression(like.EscapeChar, compositeRow, compositeSchema)
                        : null,
                    Negated = like.Negated,
                };
            case InExpression inExpression:
                return new InExpression
                {
                    Operand = ResolveNewOldRefsInExpression(inExpression.Operand, compositeRow, compositeSchema),
                    Values = inExpression.Values
                        .Select(value => ResolveNewOldRefsInExpression(value, compositeRow, compositeSchema))
                        .ToList(),
                    Negated = inExpression.Negated,
                };
            case InSubqueryExpression inSubquery:
                return new InSubqueryExpression
                {
                    Operand = ResolveNewOldRefsInExpression(inSubquery.Operand, compositeRow, compositeSchema),
                    Query = ResolveNewOldRefsInQuery(inSubquery.Query, compositeRow, compositeSchema),
                    Negated = inSubquery.Negated,
                };
            case ScalarSubqueryExpression scalarSubquery:
                return new ScalarSubqueryExpression
                {
                    Query = ResolveNewOldRefsInQuery(scalarSubquery.Query, compositeRow, compositeSchema),
                };
            case ExistsExpression exists:
                return new ExistsExpression
                {
                    Query = ResolveNewOldRefsInQuery(exists.Query, compositeRow, compositeSchema),
                };
            case BetweenExpression between:
                return new BetweenExpression
                {
                    Operand = ResolveNewOldRefsInExpression(between.Operand, compositeRow, compositeSchema),
                    Low = ResolveNewOldRefsInExpression(between.Low, compositeRow, compositeSchema),
                    High = ResolveNewOldRefsInExpression(between.High, compositeRow, compositeSchema),
                    Negated = between.Negated,
                };
            case IsNullExpression isNull:
                return new IsNullExpression
                {
                    Operand = ResolveNewOldRefsInExpression(isNull.Operand, compositeRow, compositeSchema),
                    Negated = isNull.Negated,
                };
            case FunctionCallExpression functionCall:
                return new FunctionCallExpression
                {
                    FunctionName = functionCall.FunctionName,
                    Arguments = functionCall.Arguments
                        .Select(arg => ResolveNewOldRefsInExpression(arg, compositeRow, compositeSchema))
                        .ToList(),
                    IsDistinct = functionCall.IsDistinct,
                    IsStarArg = functionCall.IsStarArg,
                };
            default:
                return expr;
        }
    }

    private static QueryStatement ResolveNewOldRefsInQuery(QueryStatement query, DbValue[] compositeRow, TableSchema compositeSchema)
    {
        switch (query)
        {
            case SelectStatement select:
                return new SelectStatement
                {
                    IsDistinct = select.IsDistinct,
                    Columns = select.Columns.Select(column => new SelectColumn
                    {
                        IsStar = column.IsStar,
                        Alias = column.Alias,
                        Expression = column.Expression != null
                            ? ResolveNewOldRefsInExpression(column.Expression, compositeRow, compositeSchema)
                            : null,
                    }).ToList(),
                    From = ResolveNewOldRefsInTableRef(select.From, compositeRow, compositeSchema),
                    Where = select.Where != null ? ResolveNewOldRefsInExpression(select.Where, compositeRow, compositeSchema) : null,
                    GroupBy = select.GroupBy?.Select(expr => ResolveNewOldRefsInExpression(expr, compositeRow, compositeSchema)).ToList(),
                    Having = select.Having != null ? ResolveNewOldRefsInExpression(select.Having, compositeRow, compositeSchema) : null,
                    OrderBy = select.OrderBy?.Select(orderBy => new OrderByClause
                    {
                        Expression = ResolveNewOldRefsInExpression(orderBy.Expression, compositeRow, compositeSchema),
                        Descending = orderBy.Descending,
                    }).ToList(),
                    Limit = select.Limit,
                    Offset = select.Offset,
                };
            case CompoundSelectStatement compound:
                return new CompoundSelectStatement
                {
                    Left = ResolveNewOldRefsInQuery(compound.Left, compositeRow, compositeSchema),
                    Right = ResolveNewOldRefsInQuery(compound.Right, compositeRow, compositeSchema),
                    Operation = compound.Operation,
                    OrderBy = compound.OrderBy?.Select(orderBy => new OrderByClause
                    {
                        Expression = ResolveNewOldRefsInExpression(orderBy.Expression, compositeRow, compositeSchema),
                        Descending = orderBy.Descending,
                    }).ToList(),
                    Limit = compound.Limit,
                    Offset = compound.Offset,
                };
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown query type: {query.GetType().Name}");
        }
    }

    private static TableRef ResolveNewOldRefsInTableRef(TableRef tableRef, DbValue[] compositeRow, TableSchema compositeSchema)
    {
        if (tableRef is not JoinTableRef join)
            return tableRef;

        return new JoinTableRef
        {
            Left = ResolveNewOldRefsInTableRef(join.Left, compositeRow, compositeSchema),
            Right = ResolveNewOldRefsInTableRef(join.Right, compositeRow, compositeSchema),
            JoinType = join.JoinType,
            Condition = join.Condition != null ? ResolveNewOldRefsInExpression(join.Condition, compositeRow, compositeSchema) : null,
        };
    }

    private static bool IsNewOldColumnRef(ColumnRefExpression col) => col.TableAlias != null &&
        (col.TableAlias.Equals("NEW", StringComparison.OrdinalIgnoreCase) ||
         col.TableAlias.Equals("OLD", StringComparison.OrdinalIgnoreCase));

    #endregion

    #region DML

    internal async ValueTask<QueryResult> ExecuteInsertAsync(
        InsertStatement stmt,
        bool persistRootChanges,
        CancellationToken ct = default)
    {
        if (ContainsSubqueries(stmt))
            stmt = await RewriteSubqueriesInInsertAsync(stmt, ct);
        if (ContainsSubqueries(stmt))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Correlated subqueries are not supported in INSERT VALUES.");
        }

        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName, _pager);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);

        int inserted = 0;
        ForeignKeyMutationContext? mutationContext =
            _catalog.GetForeignKeysForTable(stmt.TableName).Count > 0
                ? new ForeignKeyMutationContext()
                : null;
        try
        {
            foreach (var valueRow in stmt.ValueRows)
            {
                var row = ResolveInsertRow(schema, stmt.ColumnNames, valueRow);
                await ExecuteResolvedInsertRowAsync(
                    stmt.TableName,
                    schema,
                    tree,
                    indexes,
                    row,
                    mutationContext,
                    adjustTableRowCount: false,
                    ct);
                inserted++;
            }
        }
        catch
        {
            await FinalizeInsertStatementAsync(
                mutationContext,
                stmt.TableName,
                inserted,
                persistRootChanges: false,
                ct);
            throw;
        }

        await FinalizeInsertStatementAsync(mutationContext, stmt.TableName, inserted, persistRootChanges, ct);

        return QueryResult.FromRowsAffected(inserted);
    }

    private async ValueTask<QueryResult> ExecuteCompoundSelectAsync(CompoundSelectStatement stmt, CancellationToken ct)
    {
        await using var leftResult = await ExecuteQueryAsync(stmt.Left, ct);
        await using var rightResult = await ExecuteQueryAsync(stmt.Right, ct);

        var outputSchema = MergeCompoundSchemas(leftResult.Schema, rightResult.Schema);
        var leftRows = await leftResult.ToListAsync(ct);
        var rightRows = await rightResult.ToListAsync(ct);
        var rows = stmt.Operation switch
        {
            SetOperationKind.Union => ExecuteUnion(leftRows, rightRows),
            SetOperationKind.Intersect => ExecuteIntersect(leftRows, rightRows),
            SetOperationKind.Except => ExecuteExcept(leftRows, rightRows),
            _ => throw new InvalidOperationException($"Unknown set operation: {stmt.Operation}"),
        };

        if (stmt.OrderBy is not { Count: > 0 } && !stmt.Offset.HasValue && !stmt.Limit.HasValue)
            return QueryResult.FromMaterializedRows(outputSchema, rows);

        IOperator op = new MaterializedOperator(rows, outputSchema);
        var schema = new TableSchema
        {
            TableName = "compound",
            Columns = outputSchema,
        };

        op = ApplyOrdering(op, stmt.OrderBy, schema, GetOrderByTopN(stmt.OrderBy, stmt.Limit, stmt.Offset));
        if (stmt.Offset.HasValue)
            op = new OffsetOperator(op, stmt.Offset.Value);
        if (stmt.Limit.HasValue)
            op = new LimitOperator(op, stmt.Limit.Value);

        return CreateQueryResult(op);
    }

    private async ValueTask<QueryResult> ExecuteCompoundSelectWithSubqueriesAsync(CompoundSelectStatement stmt, CancellationToken ct)
    {
        if (stmt.OrderBy != null && stmt.OrderBy.Any(orderBy => ContainsSubqueries(orderBy.Expression)))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in compound ORDER BY are not supported.");
        }

        return await ExecuteCompoundSelectAsync(stmt, ct);
    }

    private async ValueTask<QueryResult> ExecuteSelectWithCorrelatedSubqueriesAsync(
        SelectStatement stmt,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        ValidateCorrelatedSelectSupport(stmt);

        var (sourceOp, sourceSchema) = BuildFromOperator(
            stmt.From,
            stmt.Where,
            pushDownOuterLocalPredicates: stmt.From is JoinTableRef);
        bool hasAggregates = stmt.GroupBy != null ||
                             stmt.Having != null ||
                             stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));
        bool useOrderedDistinctSingleColumnFastPath =
            !hasAggregates && ShouldUseOrderedSingleColumnDistinctFastPath(stmt);
        int? orderByTopN = stmt.IsDistinct ? null : GetOrderByTopN(stmt);
        List<DbValue[]> filteredRows;
        await using (sourceOp)
        {
            filteredRows = await MaterializeFilteredRowsAsync(sourceOp, stmt.Where, sourceSchema, outerScopes, ct);
        }

        IOperator op = new MaterializedOperator(filteredRows, GetSchemaColumnsArray(sourceSchema));
        if (hasAggregates)
        {
            var outputCols = BuildAggregateOutputSchema(stmt.Columns, sourceSchema);
            if (stmt.GroupBy is { Count: > 0 })
            {
                op = new HashAggregateOperator(op, stmt.Columns, stmt.GroupBy, stmt.Having, sourceSchema, outputCols);
            }
            else
            {
                op = new ScalarAggregateOperator(op, stmt.Columns, stmt.Having, sourceSchema, outputCols);
            }

            var aggregateSchema = new TableSchema
            {
                TableName = sourceSchema.TableName,
                Columns = outputCols,
            };

            if (stmt.IsDistinct)
                op = new DistinctOperator(op, useOrderedDistinctSingleColumnFastPath);

            op = ApplyOrdering(op, stmt.OrderBy, aggregateSchema, orderByTopN);
        }
        else
        {
            op = ApplyOrdering(op, stmt.OrderBy, sourceSchema, orderByTopN);

            if (!stmt.Columns.Any(c => c.IsStar))
            {
                if (TryBuildColumnProjection(stmt.Columns, sourceSchema, out var columnIndices, out var outputCols))
                {
                    op = new ProjectionOperator(op, columnIndices, outputCols, sourceSchema);
                }
                else
                {
                    var expressions = stmt.Columns.Select(c => c.Expression!).ToArray();
                    outputCols = new ColumnDefinition[expressions.Length];
                    for (int i = 0; i < expressions.Length; i++)
                        outputCols[i] = InferColumnDef(expressions[i], stmt.Columns[i].Alias, sourceSchema, i);

                    List<DbValue[]> projectedRows;
                    await using (op)
                    {
                        projectedRows = await MaterializeProjectedRowsAsync(op, expressions, sourceSchema, outerScopes, ct);
                    }

                    op = new MaterializedOperator(projectedRows, outputCols);
                }
            }

            if (stmt.IsDistinct)
                op = new DistinctOperator(op, useOrderedDistinctSingleColumnFastPath);
        }

        if (stmt.Offset.HasValue)
            op = new OffsetOperator(op, stmt.Offset.Value);

        if (stmt.Limit.HasValue)
            op = new LimitOperator(op, stmt.Limit.Value);

        return CreateQueryResult(op);
    }

    private void ValidateCorrelatedSelectSupport(SelectStatement stmt)
    {
        if (ContainsSubqueries(stmt.From))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in JOIN conditions are not supported.");
        }

        if (stmt.GroupBy != null && stmt.GroupBy.Any(ContainsSubqueries))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in GROUP BY are not supported.");
        }

        if (stmt.Having != null && ContainsSubqueries(stmt.Having))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in HAVING are not supported.");
        }

        if (stmt.OrderBy != null && stmt.OrderBy.Any(orderBy => ContainsSubqueries(orderBy.Expression)))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in ORDER BY are not supported.");
        }

        bool hasAggregates = stmt.GroupBy != null ||
                             stmt.Having != null ||
                             stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));
        if (hasAggregates &&
            stmt.Columns.Any(c => c.Expression != null && ContainsSubqueries(c.Expression)))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Subqueries in aggregate projections are not supported.");
        }
    }

    private async ValueTask<List<DbValue[]>> MaterializeFilteredRowsAsync(
        IOperator source,
        Expression? predicate,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        int initialCapacity = source is IEstimatedRowCountProvider estimated &&
                              estimated.EstimatedRowCount is int rowCount &&
                              rowCount > 0
            ? rowCount
            : 0;
        var rows = initialCapacity > 0
            ? new List<DbValue[]>(initialCapacity)
            : new List<DbValue[]>();

        await source.OpenAsync(ct);
        while (await source.MoveNextAsync(ct))
        {
            if (predicate != null)
            {
                bool predicatePassed = await EvaluateWherePredicateWithSubqueriesAsync(
                    predicate,
                    source.Current,
                    schema,
                    outerScopes,
                    ct);
                if (!predicatePassed)
                    continue;
            }

            rows.Add((DbValue[])source.Current.Clone());
        }

        return rows;
    }

    private async ValueTask<bool> EvaluateWherePredicateWithSubqueriesAsync(
        Expression predicate,
        DbValue[] row,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        switch (predicate)
        {
            case BinaryExpression { Op: BinaryOp.And } andExpression:
                if (!await EvaluateWherePredicateWithSubqueriesAsync(andExpression.Left, row, schema, outerScopes, ct))
                    return false;

                return await EvaluateWherePredicateWithSubqueriesAsync(andExpression.Right, row, schema, outerScopes, ct);
            case BinaryExpression { Op: BinaryOp.Or } orExpression:
                if (await EvaluateWherePredicateWithSubqueriesAsync(orExpression.Left, row, schema, outerScopes, ct))
                    return true;

                return await EvaluateWherePredicateWithSubqueriesAsync(orExpression.Right, row, schema, outerScopes, ct);
            case UnaryExpression { Op: TokenType.Not, Operand: ExistsExpression exists }:
            {
                bool? existsResult = await TryEvaluateExistsFilterFastAsync(exists, row, schema, outerScopes, ct);
                if (existsResult.HasValue)
                    return !existsResult.Value;
                break;
            }
            case ExistsExpression exists:
            {
                bool? existsResult = await TryEvaluateExistsFilterFastAsync(exists, row, schema, outerScopes, ct);
                if (existsResult.HasValue)
                    return existsResult.Value;
                break;
            }
            case InSubqueryExpression inSubquery:
            {
                bool? inResult = await TryEvaluateInSubqueryFilterFastAsync(inSubquery, row, schema, outerScopes, ct);
                if (inResult.HasValue)
                    return inResult.Value;
                break;
            }
        }

        var predicateResult = await EvaluateExpressionWithSubqueriesAsync(predicate, row, schema, outerScopes, ct);
        return predicateResult.IsTruthy;
    }

    private async ValueTask<List<DbValue[]>> MaterializeProjectedRowsAsync(
        IOperator source,
        IReadOnlyList<Expression> expressions,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        int initialCapacity = source is IEstimatedRowCountProvider estimated &&
                              estimated.EstimatedRowCount is int rowCount &&
                              rowCount > 0
            ? rowCount
            : 0;
        var rows = initialCapacity > 0
            ? new List<DbValue[]>(initialCapacity)
            : new List<DbValue[]>();

        await source.OpenAsync(ct);
        while (await source.MoveNextAsync(ct))
        {
            var row = source.Current;
            var projectedRow = new DbValue[expressions.Count];
            for (int i = 0; i < expressions.Count; i++)
            {
                projectedRow[i] = await EvaluateExpressionWithSubqueriesAsync(
                    expressions[i],
                    row,
                    schema,
                    outerScopes,
                    ct);
            }

            rows.Add(projectedRow);
        }

        return rows;
    }

    private async ValueTask<DbValue> EvaluateExpressionWithSubqueriesAsync(
        Expression expression,
        DbValue[] row,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        if (!ContainsSubqueries(expression))
            return ExpressionEvaluator.Evaluate(expression, row, schema);

        var correlationScopes = CreateCorrelationScopes(row, schema, outerScopes);
        var rewritten = await RewriteCorrelatedExpressionAsync(expression, correlationScopes, ct);
        return ExpressionEvaluator.Evaluate(rewritten, row, schema);
    }

    private async ValueTask<bool?> TryEvaluateExistsFilterFastAsync(
        ExistsExpression exists,
        DbValue[] row,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        var correlationScopes = CreateCorrelationScopes(row, schema, outerScopes);
        var boundQuery = BindOuterScopesInQuery(exists.Query, correlationScopes);
        return await TryExecuteSimpleExistsProbeAsync(boundQuery, ct);
    }

    private async ValueTask<bool?> TryEvaluateInSubqueryFilterFastAsync(
        InSubqueryExpression inSubquery,
        DbValue[] row,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        var visibleScopes = new[] { schema };
        var boundOperand = BindOuterScopesInExpression(inSubquery.Operand, visibleScopes, outerScopes);
        if (ContainsSubqueries(boundOperand))
            return null;

        var operandValue = ExpressionEvaluator.Evaluate(boundOperand, row, schema);
        if (operandValue.IsNull)
            return false;

        var correlationScopes = CreateCorrelationScopes(row, schema, outerScopes);
        var boundQuery = BindOuterScopesInQuery(inSubquery.Query, correlationScopes);
        return inSubquery.Negated
            ? await TryExecuteSimpleNotInFilterProbeAsync(boundQuery, operandValue, ct)
            : await TryExecuteSimpleInFilterProbeAsync(boundQuery, operandValue, ct);
    }

    private async ValueTask<bool?> TryExecuteSimpleExistsProbeAsync(QueryStatement query, CancellationToken ct)
    {
        if (!TryCreateSimpleBoundSubquerySource(query, out var source, out var sourceSchema, out var residualPredicate))
            return null;

        await using (source)
        {
            ApplySimpleSubqueryDecodeHints(source, sourceSchema, residualPredicate, projectedColumnIndex: null);

            await source.OpenAsync(ct);
            while (await source.MoveNextAsync(ct))
            {
                if (residualPredicate == null ||
                    ExpressionEvaluator.Evaluate(residualPredicate, source.Current, sourceSchema).IsTruthy)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private async ValueTask<bool?> TryExecuteSimpleInFilterProbeAsync(
        QueryStatement query,
        DbValue operandValue,
        CancellationToken ct)
    {
        if (query is not SelectStatement select)
            return null;

        if (select.Columns.Count != 1 ||
            select.Columns[0].IsStar ||
            select.Columns[0].Expression is not ColumnRefExpression projectedColumn)
        {
            return null;
        }

        if (!TryCreateSimpleBoundSubquerySource(query, out var source, out var sourceSchema, out var residualPredicate))
            return null;

        int projectedColumnIndex = projectedColumn.TableAlias != null
            ? sourceSchema.GetQualifiedColumnIndex(projectedColumn.TableAlias, projectedColumn.ColumnName)
            : sourceSchema.GetColumnIndex(projectedColumn.ColumnName);
        if (projectedColumnIndex < 0 || projectedColumnIndex >= sourceSchema.Columns.Count)
            return null;

        await using (source)
        {
            ApplySimpleSubqueryDecodeHints(source, sourceSchema, residualPredicate, projectedColumnIndex);

            await source.OpenAsync(ct);
            while (await source.MoveNextAsync(ct))
            {
                if (residualPredicate != null &&
                    !ExpressionEvaluator.Evaluate(residualPredicate, source.Current, sourceSchema).IsTruthy)
                {
                    continue;
                }

                var candidateValue = projectedColumnIndex < source.Current.Length
                    ? source.Current[projectedColumnIndex]
                    : DbValue.Null;
                if (candidateValue.IsNull)
                    continue;

                if (DbValue.Compare(candidateValue, operandValue) == 0)
                    return true;
            }
        }

        return false;
    }

    private async ValueTask<bool?> TryExecuteSimpleNotInFilterProbeAsync(
        QueryStatement query,
        DbValue operandValue,
        CancellationToken ct)
    {
        if (query is not SelectStatement select)
            return null;

        if (select.Columns.Count != 1 ||
            select.Columns[0].IsStar ||
            select.Columns[0].Expression is not ColumnRefExpression projectedColumn)
        {
            return null;
        }

        if (!TryCreateSimpleBoundSubquerySource(query, out var source, out var sourceSchema, out var residualPredicate))
            return null;

        int projectedColumnIndex = projectedColumn.TableAlias != null
            ? sourceSchema.GetQualifiedColumnIndex(projectedColumn.TableAlias, projectedColumn.ColumnName)
            : sourceSchema.GetColumnIndex(projectedColumn.ColumnName);
        if (projectedColumnIndex < 0 || projectedColumnIndex >= sourceSchema.Columns.Count)
            return null;

        bool sawNullCandidate = false;

        await using (source)
        {
            ApplySimpleSubqueryDecodeHints(source, sourceSchema, residualPredicate, projectedColumnIndex);

            await source.OpenAsync(ct);
            while (await source.MoveNextAsync(ct))
            {
                if (residualPredicate != null &&
                    !ExpressionEvaluator.Evaluate(residualPredicate, source.Current, sourceSchema).IsTruthy)
                {
                    continue;
                }

                var candidateValue = projectedColumnIndex < source.Current.Length
                    ? source.Current[projectedColumnIndex]
                    : DbValue.Null;
                if (candidateValue.IsNull)
                {
                    sawNullCandidate = true;
                    continue;
                }

                if (DbValue.Compare(candidateValue, operandValue) == 0)
                    return false;
            }
        }

        return !sawNullCandidate;
    }

    private bool TryCreateSimpleBoundSubquerySource(
        QueryStatement query,
        out IOperator source,
        out TableSchema sourceSchema,
        out Expression? residualPredicate)
    {
        source = null!;
        sourceSchema = null!;
        residualPredicate = null;

        if (query is not SelectStatement select ||
            select.IsDistinct ||
            select.GroupBy is { Count: > 0 } ||
            select.Having != null ||
            select.OrderBy is { Count: > 0 } ||
            select.Limit.HasValue ||
            select.Offset.HasValue ||
            select.From is not SimpleTableRef simpleRef)
        {
            return false;
        }

        if ((_cteData != null && _cteData.ContainsKey(simpleRef.TableName)) ||
            TryBuildSystemCatalogSource(simpleRef, out _) ||
            _catalog.GetViewSql(simpleRef.TableName) != null ||
            _catalog.GetTable(simpleRef.TableName) == null)
        {
            return false;
        }

        sourceSchema = ResolveCorrelationSimpleTableSchema(simpleRef);
        if (select.Where != null && ContainsSubqueries(select.Where))
            return false;

        source = select.Where != null
            ? TryBuildIndexScan(simpleRef.TableName, select.Where, sourceSchema, out residualPredicate)
                ?? new TableScanOperator(_catalog.GetTableTree(simpleRef.TableName, _pager), sourceSchema, GetReadSerializer(sourceSchema))
            : new TableScanOperator(_catalog.GetTableTree(simpleRef.TableName, _pager), sourceSchema, GetReadSerializer(sourceSchema));

        if (select.Where == null)
            residualPredicate = null;
        else if (source is TableScanOperator)
            residualPredicate = select.Where;

        return true;
    }

    private static void ApplySimpleSubqueryDecodeHints(
        IOperator source,
        TableSchema sourceSchema,
        Expression? residualPredicate,
        int? projectedColumnIndex)
    {
        if (residualPredicate == null && !projectedColumnIndex.HasValue)
        {
            TrySetDecodedColumnIndices(source, Array.Empty<int>());
            return;
        }

        if (projectedColumnIndex.HasValue && residualPredicate == null)
        {
            TrySetDecodedColumnIndices(source, [projectedColumnIndex.Value]);
            return;
        }

        int maxDecodedColumnIndex = projectedColumnIndex ?? -1;
        if (residualPredicate != null &&
            !TryAccumulateMaxReferencedColumn(residualPredicate, sourceSchema, ref maxDecodedColumnIndex))
        {
            return;
        }

        if (maxDecodedColumnIndex >= 0)
            TrySetDecodedColumnUpperBound(source, maxDecodedColumnIndex);
    }

    private async ValueTask<Expression> RewriteCorrelatedExpressionAsync(
        Expression expression,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        switch (expression)
        {
            case LiteralExpression:
            case ParameterExpression:
            case ColumnRefExpression:
                return expression;
            case BinaryExpression binary:
                return new BinaryExpression
                {
                    Op = binary.Op,
                    Left = await RewriteCorrelatedExpressionAsync(binary.Left, outerScopes, ct),
                    Right = await RewriteCorrelatedExpressionAsync(binary.Right, outerScopes, ct),
                };
            case UnaryExpression unary:
                return new UnaryExpression
                {
                    Op = unary.Op,
                    Operand = await RewriteCorrelatedExpressionAsync(unary.Operand, outerScopes, ct),
                };
            case CollateExpression collate:
                return new CollateExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(collate.Operand, outerScopes, ct),
                    Collation = collate.Collation,
                };
            case LikeExpression like:
                return new LikeExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(like.Operand, outerScopes, ct),
                    Pattern = await RewriteCorrelatedExpressionAsync(like.Pattern, outerScopes, ct),
                    EscapeChar = like.EscapeChar != null
                        ? await RewriteCorrelatedExpressionAsync(like.EscapeChar, outerScopes, ct)
                        : null,
                    Negated = like.Negated,
                };
            case InExpression inExpression:
                return new InExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(inExpression.Operand, outerScopes, ct),
                    Values = await RewriteCorrelatedExpressionListAsync(inExpression.Values, outerScopes, ct),
                    Negated = inExpression.Negated,
                };
            case InSubqueryExpression inSubquery:
            {
                var boundQuery = BindOuterScopesInQuery(inSubquery.Query, outerScopes);
                return new InExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(inSubquery.Operand, outerScopes, ct),
                    Values = await MaterializeSingleColumnSubqueryAsExpressionsAsync(boundQuery, ct),
                    Negated = inSubquery.Negated,
                };
            }
            case ScalarSubqueryExpression scalarSubquery:
            {
                var boundQuery = BindOuterScopesInQuery(scalarSubquery.Query, outerScopes);
                return CreateLiteralExpression(await ExecuteScalarSubqueryAsync(boundQuery, ct));
            }
            case ExistsExpression exists:
            {
                var boundQuery = BindOuterScopesInQuery(exists.Query, outerScopes);
                return new LiteralExpression
                {
                    Value = await ExecuteExistsSubqueryAsync(boundQuery, ct) ? 1L : 0L,
                    LiteralType = TokenType.IntegerLiteral,
                };
            }
            case BetweenExpression between:
                return new BetweenExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(between.Operand, outerScopes, ct),
                    Low = await RewriteCorrelatedExpressionAsync(between.Low, outerScopes, ct),
                    High = await RewriteCorrelatedExpressionAsync(between.High, outerScopes, ct),
                    Negated = between.Negated,
                };
            case IsNullExpression isNull:
                return new IsNullExpression
                {
                    Operand = await RewriteCorrelatedExpressionAsync(isNull.Operand, outerScopes, ct),
                    Negated = isNull.Negated,
                };
            case FunctionCallExpression functionCall:
                return new FunctionCallExpression
                {
                    FunctionName = functionCall.FunctionName,
                    Arguments = await RewriteCorrelatedExpressionListAsync(functionCall.Arguments, outerScopes, ct),
                    IsDistinct = functionCall.IsDistinct,
                    IsStarArg = functionCall.IsStarArg,
                };
            default:
                throw new CSharpDbException(ErrorCode.Unknown, $"Unknown expression type: {expression.GetType().Name}");
        }
    }

    private async ValueTask<List<Expression>> RewriteCorrelatedExpressionListAsync(
        IReadOnlyList<Expression> expressions,
        IReadOnlyList<CorrelationScope> outerScopes,
        CancellationToken ct)
    {
        var rewritten = new List<Expression>(expressions.Count);
        for (int i = 0; i < expressions.Count; i++)
            rewritten.Add(await RewriteCorrelatedExpressionAsync(expressions[i], outerScopes, ct));

        return rewritten;
    }

    private static CorrelationScope[] CreateCorrelationScopes(
        DbValue[] row,
        TableSchema schema,
        IReadOnlyList<CorrelationScope> outerScopes)
    {
        var scopes = new CorrelationScope[outerScopes.Count + 1];
        scopes[0] = new CorrelationScope(row, schema);
        for (int i = 0; i < outerScopes.Count; i++)
            scopes[i + 1] = outerScopes[i];

        return scopes;
    }

    private QueryResult ExecuteSelect(SelectStatement stmt)
    {
        if (_cteData != null)
            return ExecuteSelectGeneral(stmt);

        if (_selectPlanCache.TryGetValue(stmt, out var cachedPlan))
        {
            _selectPlanCacheHitCount++;
            return ExecuteSelectWithCachedPlan(stmt, cachedPlan);
        }

        _selectPlanCacheMissCount++;

        var result = ClassifyAndExecuteSelect(stmt, out var selectedPlan);
        CacheSelectPlan(stmt, selectedPlan);
        return result;
    }

    private QueryResult ExecuteSelectWithCachedPlan(SelectStatement stmt, SelectPlanKind cachedPlan)
    {
        switch (cachedPlan)
        {
            case SelectPlanKind.FastPrimaryKeyLookup:
                if (TryFastPkLookup(stmt, out var fastPkResult))
                    return fastPkResult;
                break;
            case SelectPlanKind.FastIndexedLookup:
                if (TryFastIndexedLookup(stmt, out var fastIndexedResult))
                    return fastIndexedResult;
                break;
            case SelectPlanKind.FastSimpleTableScan:
                if (TryFastSimpleTableScan(stmt, out var fastTableScanResult))
                    return fastTableScanResult;
                break;
            case SelectPlanKind.SimpleSystemCatalogCountStar:
                if (TryBuildSimpleSystemCatalogCountStarQuery(stmt, out var systemCountResult))
                    return systemCountResult;
                break;
            case SelectPlanKind.SimpleCountStar:
                if (TryBuildSimpleCountStarQuery(stmt, out var countResult))
                    return countResult;
                break;
            case SelectPlanKind.SimpleScalarAggregateColumn:
                if (TryBuildSimpleScalarAggregateColumnQuery(stmt, out var scalarAggResult))
                    return scalarAggResult;
                break;
            case SelectPlanKind.SimpleLookupScalarAggregateColumn:
                if (TryBuildSimpleLookupScalarAggregateColumnQuery(stmt, out var lookupScalarAggResult))
                    return lookupScalarAggResult;
                break;
            case SelectPlanKind.SimpleGroupedIndexAggregate:
                if (TryBuildSimpleGroupedIndexAggregateQuery(stmt, out var groupedIndexAggResult) ||
                    TryBuildCompositeGroupedIndexAggregateQuery(stmt, out groupedIndexAggResult))
                    return groupedIndexAggResult;
                break;
            case SelectPlanKind.SimpleConstantGroupAggregateColumn:
                if (TryBuildSimpleConstantGroupAggregateColumnQuery(stmt, out var constantGroupAggResult))
                    return constantGroupAggResult;
                break;
            case SelectPlanKind.General:
                return ExecuteSelectGeneral(stmt);
            default:
                throw new InvalidOperationException($"Unknown select plan kind: {cachedPlan}");
        }

        // Plan assumptions no longer hold (typically after cache invalidation edge cases).
        // Reclassify and refresh the cache entry.
        _selectPlanCacheReclassificationCount++;
        var result = ClassifyAndExecuteSelect(stmt, out var updatedPlan);
        CacheSelectPlan(stmt, updatedPlan);
        return result;
    }

    private QueryResult ClassifyAndExecuteSelect(SelectStatement stmt, out SelectPlanKind selectedPlan)
    {
        if (!stmt.IsDistinct)
        {
            // Fast-path for simple PK equality lookups — bypasses aggregate checks, BuildFromOperator, and TryBuildIndexScan
            if (TryFastPkLookup(stmt, out var fastResult))
            {
                selectedPlan = SelectPlanKind.FastPrimaryKeyLookup;
                return fastResult;
            }
            // Fast-path for simple indexed equality lookups — bypasses BuildFromOperator and planner setup.
            if (TryFastIndexedLookup(stmt, out var fastIndexedResult))
            {
                selectedPlan = SelectPlanKind.FastIndexedLookup;
                return fastIndexedResult;
            }
            // Fast-path for simple table scans with optional WHERE filter — bypasses BuildFromOperator and planner setup.
            if (TryFastSimpleTableScan(stmt, out var fastTableScanResult))
            {
                selectedPlan = SelectPlanKind.FastSimpleTableScan;
                return fastTableScanResult;
            }

            if (TryBuildSimpleSystemCatalogCountStarQuery(stmt, out var systemCountResult))
            {
                selectedPlan = SelectPlanKind.SimpleSystemCatalogCountStar;
                return systemCountResult;
            }
            if (TryBuildSimpleCountStarQuery(stmt, out var countResult))
            {
                selectedPlan = SelectPlanKind.SimpleCountStar;
                return countResult;
            }
            if (TryBuildSimpleScalarAggregateColumnQuery(stmt, out var scalarAggResult))
            {
                selectedPlan = SelectPlanKind.SimpleScalarAggregateColumn;
                return scalarAggResult;
            }
            if (TryBuildSimpleLookupScalarAggregateColumnQuery(stmt, out var lookupScalarAggResult))
            {
                selectedPlan = SelectPlanKind.SimpleLookupScalarAggregateColumn;
                return lookupScalarAggResult;
            }
            if (TryBuildSimpleGroupedIndexAggregateQuery(stmt, out var groupedIndexAggResult) ||
                TryBuildCompositeGroupedIndexAggregateQuery(stmt, out groupedIndexAggResult))
            {
                selectedPlan = SelectPlanKind.SimpleGroupedIndexAggregate;
                return groupedIndexAggResult;
            }
            if (TryBuildSimpleConstantGroupAggregateColumnQuery(stmt, out var constantGroupAggResult))
            {
                selectedPlan = SelectPlanKind.SimpleConstantGroupAggregateColumn;
                return constantGroupAggResult;
            }
        }

        selectedPlan = SelectPlanKind.General;
        return ExecuteSelectGeneral(stmt);
    }

    private void CacheSelectPlan(SelectStatement stmt, SelectPlanKind kind)
    {
        _selectPlanCacheStoreCount++;
        if (_selectPlanCache.TryAdd(stmt, kind))
        {
            _selectPlanInsertionOrder.Enqueue(stmt);
        }
        else
        {
            _selectPlanCache[stmt] = kind;
        }

        while (_selectPlanCache.Count > MaxSelectPlanCacheEntries &&
               _selectPlanInsertionOrder.Count > 0)
        {
            var evicted = _selectPlanInsertionOrder.Dequeue();
            _selectPlanCache.Remove(evicted);
        }
    }

    private QueryResult ExecuteSelectGeneral(SelectStatement stmt)
    {
        // Build the FROM operator (single table scan, join tree, or view expansion)
        var (op, schema) = BuildFromOperator(
            stmt.From,
            stmt.Where,
            pushDownOuterLocalPredicates: stmt.From is JoinTableRef);
        bool hasAggregates = stmt.GroupBy != null ||
                             stmt.Having != null ||
                             stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));
        bool useOrderedDistinctSingleColumnFastPath =
            !hasAggregates && ShouldUseOrderedSingleColumnDistinctFastPath(stmt);
        int? orderByTopN = stmt.IsDistinct ? null : GetOrderByTopN(stmt);
        bool sourceProvidesRequestedOrder = false;

        // Try index-based scan for simple equality WHERE on a single table
        Expression? remainingWhere = stmt.Where;
        if (stmt.From is SimpleTableRef simpleRef &&
            !_catalog.IsView(simpleRef.TableName) &&
            !IsSystemCatalogTable(simpleRef.TableName))
        {
            if (stmt.Where != null)
            {
                var indexOp = TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out remainingWhere);
                if (indexOp != null)
                    op = indexOp;
            }

            if (!hasAggregates &&
                TryBuildIndexOrderedScan(stmt, simpleRef, schema, op, remainingWhere, out var orderedSource, out var orderedRemainingWhere))
            {
                if (orderedSource != null)
                    op = orderedSource;
                remainingWhere = orderedRemainingWhere;

                sourceProvidesRequestedOrder = stmt.OrderBy is { Count: > 0 };
            }
        }

        // Push simple comparison predicates down to payload-level filtering so
        // non-matching rows can skip full row decode.
        if (remainingWhere != null && TryPushDownSimplePreDecodeFilter(op, remainingWhere, schema, out var pushedWhere))
            remainingWhere = pushedWhere;

        // Aggregate optimization: avoid decoding trailing columns that are never referenced.
        // This applies to both scalar aggregates and GROUP BY aggregates.
        if (hasAggregates)
        {
            if (TryGetAggregateDecodeColumnIndices(stmt, schema, remainingWhere, out var decodeColumns) &&
                TrySetDecodedColumnIndices(op, decodeColumns))
            {
                // Sparse decode hint applied.
            }
            else if (TryGetAggregateDecodeUpperBound(stmt, schema, remainingWhere, out int maxColumnIndex))
                TrySetDecodedColumnUpperBound(op, maxColumnIndex);
        }
        else if (TryGetProjectionDecodeColumnIndices(
                     stmt,
                     schema,
                     remainingWhere,
                     includeOrderBy: !sourceProvidesRequestedOrder,
                     out var decodeColumns) &&
                 TrySetDecodedColumnIndices(op, decodeColumns))
        {
            // Sparse decode hint applied.
        }
        else if (TryGetProjectionDecodeUpperBound(
                     stmt,
                     schema,
                     remainingWhere,
                     includeOrderBy: !sourceProvidesRequestedOrder,
                     out int maxColumnIndex))
        {
            TrySetDecodedColumnUpperBound(op, maxColumnIndex);
        }

        bool delayFilterUntilProjection = !hasAggregates && !stmt.Columns.Any(c => c.IsStar);
        SpanExpressionEvaluator? remainingWhereEvaluator =
            remainingWhere != null && delayFilterUntilProjection
                ? GetOrCompileSpanExpression(remainingWhere, schema)
                : null;

        if (remainingWhere != null && remainingWhereEvaluator == null)
            op = new FilterOperator(
                op,
                GetOrCompileSpanExpression(remainingWhere, schema),
                TryCreateFilterBatchPlan(op, remainingWhere, schema));

        if (hasAggregates)
        {
            // Build output schema for aggregate operator
            var outputCols = BuildAggregateOutputSchema(stmt.Columns, schema);

            bool hasGroupBy = stmt.GroupBy is { Count: > 0 };
            if (hasGroupBy)
            {
                op = new HashAggregateOperator(
                    op, stmt.Columns, stmt.GroupBy, stmt.Having, schema, outputCols);
            }
            else
            {
                op = new ScalarAggregateOperator(
                    op, stmt.Columns, stmt.Having, schema, outputCols);
            }

            // After aggregate, we need a synthetic schema for Sort to work with
            var aggSchema = new TableSchema
            {
                TableName = schema.TableName,
                Columns = outputCols,
            };

            if (stmt.IsDistinct)
                op = new DistinctOperator(op, useOrderedDistinctSingleColumnFastPath);

            op = ApplyOrdering(op, stmt.OrderBy, aggSchema, orderByTopN);
        }
        else
        {
            if (!sourceProvidesRequestedOrder)
                op = ApplyOrdering(op, stmt.OrderBy, schema, orderByTopN);

            // Projection (if not SELECT *)
            if (!stmt.Columns.Any(c => c.IsStar))
            {
                if (TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out var outputCols))
                {
                    // Fast path: PK equality lookup with projection that only references the PK column.
                    // We can return the key directly once row existence is confirmed and skip row decode.
                    if (remainingWhere == null &&
                        stmt.OrderBy is not { Count: > 0 } &&
                        op is PrimaryKeyLookupOperator pkLookup &&
                        IsPrimaryKeyOnlyProjection(columnIndices, schema.PrimaryKeyColumnIndex))
                    {
                        op = new PrimaryKeyProjectionLookupOperator(pkLookup.TableTree, pkLookup.SeekKey, outputCols);
                    }
                    else if (remainingWhere == null &&
                             op is IndexOrderedScanOperator orderedScan &&
                             TryBuildCoveredOrderedIndexProjectionOperator(
                                 orderedScan,
                                 schema,
                                 columnIndices,
                                 outputCols,
                                 out var coveredOrderedProjection))
                    {
                        op = coveredOrderedProjection;
                    }
                    else if (remainingWhere == null &&
                             op is IndexScanOperator hashedLookup &&
                             TryBuildCoveredHashedIndexProjectionOperator(
                                 hashedLookup,
                                 schema,
                                 columnIndices,
                                 outputCols,
                                 out var coveredHashedProjection))
                    {
                        op = coveredHashedProjection;
                    }
                    else if (remainingWhere == null &&
                             TryPushDownColumnProjection(op, columnIndices, outputCols))
                    {
                        // Join operators can project directly, avoiding full composite row materialization.
                    }
                    else
                    {
                        var projectionExpressions = stmt.Columns.Select(c => c.Expression!).ToArray();
                        var batchPlan = remainingWhereEvaluator != null
                            ? TryCreateBatchPlan(op, remainingWhere, projectionExpressions, schema)
                            : null;

                        if (remainingWhereEvaluator != null)
                            op = batchPlan != null
                                ? new FilterProjectionOperator(op, remainingWhereEvaluator, columnIndices, outputCols, batchPlan, useSpanEvaluator: true)
                                : new FilterOperator(op, remainingWhereEvaluator);

                        op = batchPlan != null
                            ? op
                            : new ProjectionOperator(op, columnIndices, outputCols, schema);
                    }
                }
                else
                {
                    var expressions = stmt.Columns.Select(c => c.Expression!).ToArray();
                    outputCols = new ColumnDefinition[expressions.Length];
                    for (int i = 0; i < expressions.Length; i++)
                    {
                        outputCols[i] = InferColumnDef(expressions[i], stmt.Columns[i].Alias, schema, i);
                    }
                    var expressionEvaluators = GetOrCompileSpanExpressions(expressions, schema);
                    var batchPlan = TryCreateBatchPlan(op, remainingWhere, expressions, schema);
                    op = remainingWhereEvaluator != null
                        ? new FilterProjectionOperator(op, remainingWhereEvaluator, outputCols, expressionEvaluators, batchPlan, useSpanEvaluator: true)
                        : batchPlan != null
                            ? new ProjectionOperator(
                                op,
                                Array.Empty<int>(),
                                outputCols,
                                expressionEvaluators,
                                batchPlan,
                                useSpanEvaluators: true)
                            : new ProjectionOperator(
                                op,
                                Array.Empty<int>(),
                                outputCols,
                                expressionEvaluators,
                                batchPlan: null,
                                useSpanEvaluators: true);
                }
            }

            if (stmt.IsDistinct)
                op = new DistinctOperator(op, useOrderedDistinctSingleColumnFastPath);
        }

        if (stmt.Offset.HasValue)
            op = new OffsetOperator(op, stmt.Offset.Value);

        if (stmt.Limit.HasValue)
            op = new LimitOperator(op, stmt.Limit.Value);

        return CreateQueryResult(op);
    }

    private static QueryResult CreateQueryResult(IOperator op)
        => op is IBatchOperator batchOperator && ShouldUseBatchResultBoundary(op)
            ? QueryResult.FromBatchOperator(batchOperator)
            : new QueryResult(op);

    private static bool ShouldUseBatchResultBoundary(IOperator _) => true;

    private static int? GetOrderByTopN(SelectStatement stmt)
        => GetOrderByTopN(stmt.OrderBy, stmt.Limit, stmt.Offset);

    private static bool ShouldUseOrderedSingleColumnDistinctFastPath(SelectStatement stmt)
    {
        if (!stmt.IsDistinct ||
            stmt.Columns.Count != 1 ||
            stmt.OrderBy is not { Count: 1 })
        {
            return false;
        }

        var column = stmt.Columns[0];
        if (column.IsStar ||
            column.Expression is not ColumnRefExpression projectedColumn ||
            stmt.OrderBy[0].Expression is not ColumnRefExpression orderedColumn)
        {
            return false;
        }

        return string.Equals(projectedColumn.ColumnName, orderedColumn.ColumnName, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(projectedColumn.TableAlias, orderedColumn.TableAlias, StringComparison.OrdinalIgnoreCase);
    }

    private static int? GetOrderByTopN(
        List<OrderByClause>? orderBy,
        int? limit,
        int? offset)
    {
        if (orderBy is not { Count: > 0 } || !limit.HasValue)
            return null;

        long topN = limit.Value;
        if (offset.HasValue)
            topN += offset.Value;

        if (topN <= 0)
            return 0;

        return topN >= int.MaxValue ? int.MaxValue : (int)topN;
    }

    private static IOperator ApplyOrdering(
        IOperator source,
        List<OrderByClause>? orderBy,
        TableSchema schema,
        int? topN)
    {
        if (orderBy is not { Count: > 0 })
            return source;

        if (topN.HasValue)
            return new TopNSortOperator(source, orderBy, schema, topN.Value);

        return new SortOperator(source, orderBy, schema);
    }

    private static bool TryPushDownColumnProjection(
        IOperator op,
        int[] columnIndices,
        ColumnDefinition[] outputCols)
    {
        if (op is IProjectionPushdownTarget pushdownTarget)
            return pushdownTarget.TrySetOutputProjection(columnIndices, outputCols);

        return false;
    }

    private static ColumnDefinition[] MergeCompoundSchemas(
        IReadOnlyList<ColumnDefinition> leftSchema,
        IReadOnlyList<ColumnDefinition> rightSchema)
    {
        if (leftSchema.Count != rightSchema.Count)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Set operations require matching column counts. Left query has {leftSchema.Count} column(s); right query has {rightSchema.Count}.");
        }

        var output = new ColumnDefinition[leftSchema.Count];
        for (int i = 0; i < output.Length; i++)
        {
            var left = leftSchema[i];
            var right = rightSchema[i];
            output[i] = new ColumnDefinition
            {
                Name = string.IsNullOrWhiteSpace(left.Name) ? right.Name : left.Name,
                Type = MergeCompoundType(left.Type, right.Type),
                Nullable = left.Nullable || right.Nullable || left.Type != right.Type,
            };
        }

        return output;
    }

    private static DbType MergeCompoundType(DbType left, DbType right)
    {
        if (left == right)
            return left;
        if (left == DbType.Null)
            return right;
        if (right == DbType.Null)
            return left;
        if (left is DbType.Integer or DbType.Real && right is DbType.Integer or DbType.Real)
            return DbType.Real;

        return DbType.Null;
    }

    private static List<DbValue[]> ExecuteUnion(List<DbValue[]> leftRows, List<DbValue[]> rightRows)
    {
        var seen = new HashSet<RowSetKey>(new RowSetKeyComparer());
        var output = new List<DbValue[]>(leftRows.Count + rightRows.Count);
        AddDistinctRows(leftRows, output, seen);
        AddDistinctRows(rightRows, output, seen);
        return output;
    }

    private static List<DbValue[]> ExecuteIntersect(List<DbValue[]> leftRows, List<DbValue[]> rightRows)
    {
        var rightSet = CreateRowSet(rightRows);
        var emitted = new HashSet<RowSetKey>(new RowSetKeyComparer());
        var output = new List<DbValue[]>();

        for (int i = 0; i < leftRows.Count; i++)
        {
            var row = leftRows[i];
            var key = new RowSetKey(row, ComputeRowHashCode(row));
            if (rightSet.Contains(key) && emitted.Add(key))
                output.Add(row);
        }

        return output;
    }

    private static List<DbValue[]> ExecuteExcept(List<DbValue[]> leftRows, List<DbValue[]> rightRows)
    {
        var rightSet = CreateRowSet(rightRows);
        var emitted = new HashSet<RowSetKey>(new RowSetKeyComparer());
        var output = new List<DbValue[]>();

        for (int i = 0; i < leftRows.Count; i++)
        {
            var row = leftRows[i];
            var key = new RowSetKey(row, ComputeRowHashCode(row));
            if (!rightSet.Contains(key) && emitted.Add(key))
                output.Add(row);
        }

        return output;
    }

    private static HashSet<RowSetKey> CreateRowSet(List<DbValue[]> rows)
    {
        var set = new HashSet<RowSetKey>(new RowSetKeyComparer());
        for (int i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            set.Add(new RowSetKey(row, ComputeRowHashCode(row)));
        }

        return set;
    }

    private static void AddDistinctRows(
        List<DbValue[]> sourceRows,
        List<DbValue[]> output,
        HashSet<RowSetKey> seen)
    {
        for (int i = 0; i < sourceRows.Count; i++)
        {
            var row = sourceRows[i];
            var key = new RowSetKey(row, ComputeRowHashCode(row));
            if (seen.Add(key))
                output.Add(row);
        }
    }

    private static int ComputeRowHashCode(DbValue[] row)
    {
        var hash = new HashCode();
        for (int i = 0; i < row.Length; i++)
            hash.Add(row[i]);
        return hash.ToHashCode();
    }

    private readonly record struct RowSetKey(DbValue[] Values, int HashCode);

    private sealed class RowSetKeyComparer : IEqualityComparer<RowSetKey>
    {
        public bool Equals(RowSetKey x, RowSetKey y)
        {
            if (x.HashCode != y.HashCode || x.Values.Length != y.Values.Length)
                return false;

            for (int i = 0; i < x.Values.Length; i++)
            {
                if (!x.Values[i].Equals(y.Values[i]))
                    return false;
            }

            return true;
        }

        public int GetHashCode(RowSetKey obj) => obj.HashCode;
    }

    internal async ValueTask<QueryResult?> TryExecuteSimplePrimaryKeyLookupDirectAsync(
        SimplePrimaryKeyLookupSql lookup,
        CancellationToken ct = default)
    {
        if (_catalog.IsView(lookup.TableName))
            return null;
        if (IsSystemCatalogTable(lookup.TableName))
            return null;
        if (_cteData != null && _cteData.ContainsKey(lookup.TableName))
            return null;

        var schema = _catalog.GetTable(lookup.TableName);
        if (schema == null)
            return null;

        int predicateColumnIndex = schema.GetColumnIndex(lookup.PredicateColumn);
        if (predicateColumnIndex < 0 || predicateColumnIndex >= schema.Columns.Count)
            return null;

        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (pkIdx < 0 ||
            pkIdx >= schema.Columns.Count ||
            predicateColumnIndex != pkIdx ||
            schema.Columns[pkIdx].Type != DbType.Integer)
        {
            return null;
        }

        DbValue predicateLiteral = lookup.PredicateLiteral;
        if (predicateLiteral.Type == DbType.Null)
            predicateLiteral = DbValue.FromInteger(lookup.LookupValue);
        if (predicateLiteral.Type != DbType.Integer)
            return null;

        ColumnDefinition[] outputColumns;
        int[] projectionColumnIndices;
        if (lookup.SelectStar)
        {
            outputColumns = GetSchemaColumnsArray(schema);
            projectionColumnIndices = Array.Empty<int>();
        }
        else if (!TryBuildSimpleLookupProjection(lookup.ProjectionColumns, schema, out projectionColumnIndices, out outputColumns))
        {
            return null;
        }

        long lookupValue = predicateLiteral.AsInteger;
        var tableTree = _catalog.GetTableTree(lookup.TableName, _pager);
        ReadOnlyMemory<byte>? payload = tableTree.TryFindCachedMemory(lookupValue, out var cachedPayload)
            ? cachedPayload
            : await tableTree.FindMemoryAsync(lookupValue, ct);

        if (payload is not { } payloadMemory)
            return QueryResult.FromSyncLookup(null, outputColumns);

        var serializer = GetReadSerializer(schema);
        if (lookup.HasResidualPredicate)
        {
            int residualColumnIndex = schema.GetColumnIndex(lookup.ResidualPredicateColumn);
            if (residualColumnIndex < 0 || residualColumnIndex >= schema.Columns.Count)
                return null;

            byte[]? residualTextBytes = lookup.ResidualPredicateLiteral.Type == DbType.Text &&
                                        CanPushDownPredicate(schema, residualColumnIndex, lookup.ResidualPredicateLiteral)
                ? Encoding.UTF8.GetBytes(lookup.ResidualPredicateLiteral.AsText)
                : null;

            if (!BoundColumnAccessHelper.EvaluatePreDecodeFilter(
                    payloadMemory.Span,
                    serializer,
                    BoundColumnAccessHelper.TryCreate(serializer, residualColumnIndex),
                    residualColumnIndex,
                    residualTextBytes,
                    BinaryOp.Equals,
                    lookup.ResidualPredicateLiteral))
            {
                return QueryResult.FromSyncLookup(null, outputColumns);
            }
        }

        if (lookup.SelectStar)
            return QueryResult.FromSyncLookup(serializer.Decode(payloadMemory.Span), outputColumns);

        if (IsPrimaryKeyOnlyProjection(projectionColumnIndices, pkIdx))
        {
            var row = outputColumns.Length == 0 ? Array.Empty<DbValue>() : new DbValue[outputColumns.Length];
            if (row.Length > 0)
            {
                var keyValue = DbValue.FromInteger(lookupValue);
                Array.Fill(row, keyValue);
            }

            return QueryResult.FromSyncLookup(row, outputColumns);
        }

        var projectedRow = new DbValue[projectionColumnIndices.Length];
        for (int i = 0; i < projectionColumnIndices.Length; i++)
            projectedRow[i] = serializer.DecodeColumn(payloadMemory.Span, projectionColumnIndices[i]);

        return QueryResult.FromSyncLookup(projectedRow, outputColumns);
    }

    public bool TryExecuteSimplePrimaryKeyLookup(SimplePrimaryKeyLookupSql lookup, out QueryResult result)
    {
        result = null!;

        if (_catalog.IsView(lookup.TableName))
            return false;
        if (IsSystemCatalogTable(lookup.TableName))
            return false;
        if (_cteData != null && _cteData.ContainsKey(lookup.TableName))
            return false;

        var schema = _catalog.GetTable(lookup.TableName);
        if (schema == null)
            return false;

        int predicateColumnIndex = schema.GetColumnIndex(lookup.PredicateColumn);
        if (predicateColumnIndex < 0 || predicateColumnIndex >= schema.Columns.Count)
            return false;
        if (schema.Columns[predicateColumnIndex].Type != DbType.Integer &&
            schema.Columns[predicateColumnIndex].Type != DbType.Text)
        {
            return false;
        }

        int pkIdx = schema.PrimaryKeyColumnIndex;
        bool hasIntegerPk = pkIdx >= 0 &&
            pkIdx < schema.Columns.Count &&
            schema.Columns[pkIdx].Type == DbType.Integer;
        DbValue predicateLiteral = lookup.PredicateLiteral;
        if (predicateLiteral.Type == DbType.Null &&
            schema.Columns[predicateColumnIndex].Type == DbType.Integer)
        {
            // Backward compatibility for older callers that only set LookupValue.
            predicateLiteral = DbValue.FromInteger(lookup.LookupValue);
        }

        bool predicateUsesDirectIntegerKey =
            predicateLiteral.Type == DbType.Integer &&
            schema.Columns[predicateColumnIndex].Type == DbType.Integer;
        bool isPrimaryKeyLookup = hasIntegerPk &&
            predicateColumnIndex == pkIdx &&
            predicateUsesDirectIntegerKey;

        long lookupValue;
        var tableTree = _catalog.GetTableTree(lookup.TableName, _pager);
        IOperator lookupOp;
        IndexSchema? matchedIndex = null;
        IIndexStore? indexStore = null;
        bool predicateRequiresResidual = false;
        DbValue normalizedPredicateLiteral = predicateLiteral;

        if (isPrimaryKeyLookup)
        {
            lookupValue = predicateLiteral.AsInteger;
            lookupOp = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, GetReadSerializer(schema));
        }
        else
        {
            var indexes = _catalog.GetSqlIndexesForTable(lookup.TableName);
            matchedIndex = FindLookupIndexForColumn(indexes, schema, predicateColumnIndex);
            if (matchedIndex == null)
                return false;

            if (predicateLiteral.Type != DbType.Integer && predicateLiteral.Type != DbType.Text)
                return false;

            if (!predicateUsesDirectIntegerKey &&
                !(predicateLiteral.Type == DbType.Text && schema.Columns[predicateColumnIndex].Type == DbType.Text))
            {
                return false;
            }

            normalizedPredicateLiteral = predicateUsesDirectIntegerKey
                ? predicateLiteral
                : NormalizeLookupLiteralForIndex(predicateLiteral, matchedIndex, schema, 0, predicateColumnIndex);
            lookupValue = predicateUsesDirectIntegerKey
                ? predicateLiteral.AsInteger
                : ComputeLookupKeyForIndex(matchedIndex, schema, 0, predicateColumnIndex, normalizedPredicateLiteral);
            indexStore = _catalog.GetIndexStore(matchedIndex.IndexName, _pager);
            string?[]? expectedKeyCollations = predicateUsesDirectIntegerKey
                ? null
                : [CollationSupport.GetEffectiveIndexColumnCollation(matchedIndex, schema, 0, predicateColumnIndex)];
            lookupOp = matchedIndex.IsUnique && predicateUsesDirectIntegerKey
                ? new UniqueIndexLookupOperator(indexStore, tableTree, schema, lookupValue, GetReadSerializer(schema))
                : new IndexScanOperator(
                    indexStore,
                    tableTree,
                    schema,
                    lookupValue,
                    GetReadSerializer(schema),
                    predicateUsesDirectIntegerKey ? null : [predicateColumnIndex],
                    predicateUsesDirectIntegerKey ? null : [normalizedPredicateLiteral],
                    expectedKeyCollations,
                    IndexMaintenanceHelper.UsesOrderedTextIndexKey(matchedIndex, schema));
        }

        if (predicateRequiresResidual && lookup.HasResidualPredicate)
            return false;

        bool hasResidual = predicateRequiresResidual || lookup.HasResidualPredicate;
        int residualColumnIndex = -1;
        if (hasResidual)
        {
            DbValue residualLiteral;
            if (predicateRequiresResidual)
            {
                residualColumnIndex = predicateColumnIndex;
                residualLiteral = predicateLiteral;
            }
            else
            {
                residualColumnIndex = schema.GetColumnIndex(lookup.ResidualPredicateColumn);
                if (residualColumnIndex < 0 || residualColumnIndex >= schema.Columns.Count)
                    return false;

                residualLiteral = lookup.ResidualPredicateLiteral;
            }

            if (lookupOp is not IPreDecodeFilterSupport preDecodeFilterTarget)
                return false;

            if (!CanPushDownPredicate(schema, residualColumnIndex, residualLiteral))
                return false;

            preDecodeFilterTarget.SetPreDecodeFilter(
                residualColumnIndex,
                BinaryOp.Equals,
                residualLiteral);
        }

        if (lookup.SelectStar)
        {
            if (isPrimaryKeyLookup &&
                !hasResidual &&
                PreferSyncPointLookups &&
                tableTree.TryFindCachedMemory(lookupValue, out var payload))
            {
                var row = payload is { } payloadMemory ? GetReadSerializer(schema).Decode(payloadMemory.Span) : null;
                result = QueryResult.FromSyncLookup(row, GetSchemaColumnsArray(schema));
                return true;
            }

            result = new QueryResult(lookupOp);
            return true;
        }

        if (!TryBuildSimpleLookupProjection(
                lookup.ProjectionColumns,
                schema,
                out var projectionColumnIndices,
                out var outputColumns))
        {
            return false;
        }

        if (isPrimaryKeyLookup && !hasResidual && IsPrimaryKeyOnlyProjection(projectionColumnIndices, pkIdx))
        {
            if (PreferSyncPointLookups && tableTree.TryFindCachedMemory(lookupValue, out var payload))
            {
                DbValue[]? row = null;
                if (payload != null)
                {
                    var keyValue = DbValue.FromInteger(lookupValue);
                    row = new DbValue[outputColumns.Length];
                    Array.Fill(row, keyValue);
                }

                result = QueryResult.FromSyncLookup(row, outputColumns);
                return true;
            }

            result = new QueryResult(new PrimaryKeyProjectionLookupOperator(tableTree, lookupValue, outputColumns));
            return true;
        }

        if (!isPrimaryKeyLookup &&
            !hasResidual &&
            matchedIndex != null &&
            indexStore != null)
        {
            if (predicateUsesDirectIntegerKey &&
                IsCoveredLookupProjection(
                    projectionColumnIndices,
                    pkIdx,
                    predicateColumnIndex,
                    canProjectPrimaryKey: hasIntegerPk))
            {
                IOperator projectionLookup = matchedIndex.IsUnique
                    ? new UniqueIndexProjectionLookupOperator(
                        indexStore,
                        lookupValue,
                        outputColumns,
                        projectionColumnIndices,
                        pkIdx,
                        predicateColumnIndex)
                    : new IndexScanProjectionOperator(
                        indexStore,
                        lookupValue,
                        outputColumns,
                        projectionColumnIndices,
                        pkIdx,
                        predicateColumnIndex);
                result = new QueryResult(projectionLookup);
                return true;
            }

            if (!predicateUsesDirectIntegerKey &&
                matchedIndex != null &&
                !IndexMaintenanceHelper.UsesOrderedTextIndexKey(matchedIndex, schema) &&
                CanProjectPrimaryKeyOrKeyColumns(
                    projectionColumnIndices,
                    schema,
                    [predicateColumnIndex]))
            {
                result = new QueryResult(
                    new HashedIndexProjectionLookupOperator(
                        indexStore,
                        tableTree,
                        schema,
                        lookupValue,
                        outputColumns,
                        projectionColumnIndices,
                        [predicateColumnIndex],
                        [normalizedPredicateLiteral],
                        GetReadSerializer(schema)));
                return true;
            }
        }

        int[] decodeColumnIndices = ToSortedColumnIndices(new HashSet<int>(projectionColumnIndices));
        if (!TrySetDecodedColumnIndices(lookupOp, decodeColumnIndices))
        {
            int maxDecodedColumn = -1;
            for (int i = 0; i < projectionColumnIndices.Length; i++)
            {
                if (projectionColumnIndices[i] > maxDecodedColumn)
                    maxDecodedColumn = projectionColumnIndices[i];
            }

            if (maxDecodedColumn >= 0)
                TrySetDecodedColumnUpperBound(lookupOp, maxDecodedColumn);
        }

        IOperator op = new ProjectionOperator(lookupOp, projectionColumnIndices, outputColumns, schema);
        result = new QueryResult(op);
        return true;
    }

    public ValueTask<QueryResult> ExecuteSimpleInsertAsync(SimpleInsertSql insert, CancellationToken ct = default) =>
        ExecuteSimpleInsertAsync(insert, persistRootChanges: true, ct);

    internal async ValueTask<QueryResult> ExecuteSimpleInsertAsync(
        SimpleInsertSql insert,
        bool persistRootChanges,
        CancellationToken ct = default)
    {
        var schema = GetSchema(insert.TableName);
        var tree = _catalog.GetTableTree(insert.TableName);
        var indexes = _catalog.GetIndexesForTable(insert.TableName);
        IReadOnlyList<ForeignKeyDefinition> foreignKeys = _catalog.GetForeignKeysForTable(insert.TableName);
        IReadOnlyList<TriggerSchema> triggers = _catalog.GetTriggersForTable(insert.TableName);

        if (indexes.Count == 0 && foreignKeys.Count == 0 && triggers.Count == 0)
            return await ExecuteBareSimpleInsertAsync(insert, schema, tree, persistRootChanges, ct);

        int inserted = 0;
        ForeignKeyMutationContext? mutationContext =
            foreignKeys.Count > 0
                ? new ForeignKeyMutationContext()
                : null;
        try
        {
            for (int i = 0; i < insert.RowCount; i++)
            {
                var row = insert.ValueRows[i];
                if (row.Length != schema.Columns.Count)
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"Expected {schema.Columns.Count} values, got {row.Length}.");
                }

                await ExecuteResolvedInsertRowAsync(
                    insert.TableName,
                    schema,
                    tree,
                    indexes,
                    row,
                    mutationContext,
                    adjustTableRowCount: false,
                    ct);
                inserted++;
            }
        }
        catch
        {
            await FinalizeInsertStatementAsync(
                mutationContext,
                insert.TableName,
                inserted,
                persistRootChanges: false,
                ct);
            throw;
        }

        await FinalizeInsertStatementAsync(mutationContext, insert.TableName, inserted, persistRootChanges, ct);
        return QueryResult.FromRowsAffected(inserted);
    }

    private async ValueTask<QueryResult> ExecuteBareSimpleInsertAsync(
        SimpleInsertSql insert,
        TableSchema schema,
        BTree tree,
        bool persistRootChanges,
        CancellationToken ct)
    {
        int inserted = 0;
        var insertTraversalPath = new List<uint>(capacity: 8);
        var insertTraversalSet = new HashSet<uint>();
        try
        {
            for (int i = 0; i < insert.RowCount; i++)
            {
                DbValue[] row = insert.ValueRows[i];
                if (row.Length != schema.Columns.Count)
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"Expected {schema.Columns.Count} values, got {row.Length}.");
                }

                await ExecuteBareInsertRowAsync(
                    insert.TableName,
                    schema,
                    tree,
                    row,
                    insertTraversalPath,
                    insertTraversalSet,
                    ct);
                inserted++;
            }
        }
        catch
        {
            await FinalizeInsertStatementAsync(
                mutationContext: null,
                insert.TableName,
                inserted,
                persistRootChanges: false,
                ct);
            throw;
        }

        await FinalizeInsertStatementAsync(
            mutationContext: null,
            insert.TableName,
            inserted,
            persistRootChanges,
            ct);
        return QueryResult.FromRowsAffected(inserted);
    }

    private async ValueTask FinalizeInsertStatementAsync(
        ForeignKeyMutationContext? mutationContext,
        string tableName,
        int inserted,
        bool persistRootChanges,
        CancellationToken ct)
    {
        if (inserted <= 0)
            return;

        await _catalog.AdjustTableRowCountKnownExactAsync(tableName, inserted, ct);
        await PersistForeignKeyMutationContextAsync(
            mutationContext,
            tableName,
            hasMutations: true,
            persistRootChanges,
            ct);
    }

    private async ValueTask<long> ExecuteBareInsertRowAsync(
        string tableName,
        TableSchema schema,
        BTree tree,
        DbValue[] row,
        List<uint>? traversalPath,
        HashSet<uint>? traversalSet,
        CancellationToken ct)
    {
        var (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(tableName, schema, tree, row, ct);
        while (true)
        {
            try
            {
                ReadOnlyMemory<byte> encodedRow = _recordSerializer.Encode(row);
                if (traversalPath != null && traversalSet != null)
                    await tree.InsertAsync(rowId, encodedRow, traversalPath, traversalSet, ct);
                else
                    await tree.InsertAsync(rowId, encodedRow, ct);
                return rowId;
            }
            catch (CSharpDbException ex) when (autoGeneratedRowId && ex.Code == ErrorCode.DuplicateKey)
            {
                InvalidateRowIdCache(tableName);
                (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(tableName, schema, tree, row, ct);
            }
        }
    }

    /// <summary>
    /// Fast path for simple PK lookups: SELECT * / columns FROM table WHERE pk = literal [AND ...].
    /// Bypasses BuildFromOperator, TryBuildIndexScan, and aggregate checks entirely.
    /// </summary>
    private bool TryFastPkLookup(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        // Must be a simple table reference (not a join, subquery, or view)
        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (_catalog.IsView(simpleRef.TableName))
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        // Must have a WHERE clause
        if (stmt.Where == null)
            return false;

        // Must not have GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
        if (stmt.GroupBy != null || stmt.Having != null)
            return false;
        if (stmt.OrderBy is { Count: > 0 })
            return false;
        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        // Resolve schema and verify integer PK
        var schema = _catalog.GetTable(simpleRef.TableName);
        if (schema == null)
            return false;
        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (pkIdx < 0 || pkIdx >= schema.Columns.Count || schema.Columns[pkIdx].Type != DbType.Integer)
            return false;

        if (!TryExtractPrimaryKeyLookupWithResidual(stmt.Where, schema, pkIdx, out long lookupValue, out var residualWhere))
            return false;

        var tableTree = _catalog.GetTableTree(simpleRef.TableName, _pager);
        bool selectStar = stmt.Columns.Any(c => c.IsStar);

        // Sync fast path: try cache-only lookup to bypass the async operator pipeline
        if (PreferSyncPointLookups && residualWhere == null && selectStar)
        {
            if (tableTree.TryFindCachedMemory(lookupValue, out var payload))
            {
                var row = payload is { } payloadMemory ? GetReadSerializer(schema).Decode(payloadMemory.Span) : null;
                var schemaArray = GetSchemaColumnsArray(schema);
                result = QueryResult.FromSyncLookup(row, schemaArray);
                return true;
            }
            // Cache miss — fall through to create operator (existing async path)
        }

        // SELECT * — just PrimaryKeyLookupOperator
        if (selectStar)
        {
            IOperator op = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, GetReadSerializer(schema));
            if (residualWhere != null && TryPushDownSimplePreDecodeFilter(op, residualWhere, schema, out var pushedWhere))
                residualWhere = pushedWhere;
            if (residualWhere != null)
                op = new FilterOperator(
                    op,
                    GetOrCompileSpanExpression(residualWhere, schema),
                    TryCreateFilterBatchPlan(op, residualWhere, schema));
            result = CreateQueryResult(op);
            return true;
        }

        if (residualWhere == null &&
            TryResolveUnaliasedPrimaryKeyProjectionCount(stmt.Columns, schema, pkIdx, out int projectedPkCount))
        {
            ColumnDefinition[] pkOutputCols = projectedPkCount == 1
                ? GetSingleColumnOutputSchema(schema, pkIdx)
                : BuildRepeatedColumnOutputSchema(schema.Columns[pkIdx], projectedPkCount);

            if (PreferSyncPointLookups && tableTree.TryFindCachedMemory(lookupValue, out var payload))
            {
                DbValue[]? row = null;
                if (payload != null)
                {
                    var keyValue = DbValue.FromInteger(lookupValue);
                    if (projectedPkCount == 1)
                    {
                        row = [keyValue];
                    }
                    else
                    {
                        row = new DbValue[projectedPkCount];
                        Array.Fill(row, keyValue);
                    }
                }

                result = QueryResult.FromSyncLookup(row, pkOutputCols);
                return true;
            }

            IOperator projectedOp = new PrimaryKeyProjectionLookupOperator(tableTree, lookupValue, pkOutputCols);
            result = new QueryResult(projectedOp);
            return true;
        }

        // Column projection — check if all columns are simple column references
        if (TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out var outputCols))
        {
            // PK-only projection with no residual filter: skip row decode entirely.
            if (residualWhere == null && IsPrimaryKeyOnlyProjection(columnIndices, pkIdx))
            {
                if (PreferSyncPointLookups && tableTree.TryFindCachedMemory(lookupValue, out var payload))
                {
                    DbValue[]? row = null;
                    if (payload != null)
                    {
                        row = new DbValue[outputCols.Length];
                        var keyValue = DbValue.FromInteger(lookupValue);
                        for (int i = 0; i < row.Length; i++)
                            row[i] = keyValue;
                    }

                    result = QueryResult.FromSyncLookup(row, outputCols);
                    return true;
                }

                IOperator op = new PrimaryKeyProjectionLookupOperator(tableTree, lookupValue, outputCols);
                result = new QueryResult(op);
                return true;
            }

            // Column projection with row decode (and optional residual filter).
            var pkOp = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, GetReadSerializer(schema));
            var remainingResidual = residualWhere;
            if (remainingResidual != null &&
                TryPushDownSimplePreDecodeFilter(pkOp, remainingResidual, schema, out var pushedWhere))
            {
                remainingResidual = pushedWhere;
            }

            int maxCol = -1;
            for (int i = 0; i < columnIndices.Length; i++)
                if (columnIndices[i] > maxCol) maxCol = columnIndices[i];
            if (remainingResidual != null &&
                !TryAccumulateMaxReferencedColumn(remainingResidual, schema, ref maxCol))
            {
                return false;
            }
            if (maxCol >= 0)
                pkOp.SetDecodedColumnUpperBound(maxCol);

            IOperator projOp = pkOp;
            if (remainingResidual != null)
                projOp = new FilterOperator(
                    projOp,
                    GetOrCompileSpanExpression(remainingResidual, schema),
                    TryCreateFilterBatchPlan(projOp, remainingResidual, schema));

            projOp = new ProjectionOperator(projOp, columnIndices, outputCols, schema);
            result = new QueryResult(projOp);
            return true;
        }

        // Expression columns — fall through to general path
        return false;
    }

    private static bool TryExtractPrimaryKeyLookupWithResidual(
        Expression where,
        TableSchema schema,
        int pkIndex,
        out long lookupValue,
        out Expression? residualWhere)
    {
        lookupValue = 0;
        residualWhere = null;

        if (TryExtractIntegerEqualityLookupTerm(where, schema, out int columnIndex, out long singleLookup))
        {
            if (columnIndex != pkIndex)
                return false;

            lookupValue = singleLookup;
            return true;
        }

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        int selectedConjunctIndex = -1;
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (!TryExtractIntegerEqualityLookupTerm(conjuncts[i], schema, out int conjunctColumnIndex, out long conjunctLookup))
                continue;

            if (conjunctColumnIndex != pkIndex)
                continue;

            selectedConjunctIndex = i;
            lookupValue = conjunctLookup;
            break;
        }

        if (selectedConjunctIndex < 0)
            return false;

        if (conjuncts.Count == 1)
            return true;

        var residualTerms = new List<Expression>(conjuncts.Count - 1);
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (i == selectedConjunctIndex)
                continue;

            residualTerms.Add(conjuncts[i]);
        }

        residualWhere = CombineConjuncts(residualTerms);
        return true;
    }

    private bool TryBuildSimpleCountStarQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.Where != null || stmt.GroupBy != null || stmt.Having != null)
            return false;

        if (stmt.OrderBy is { Count: > 0 })
            return false;

        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
            return false;

        if (stmt.Columns[0].Expression is not FunctionCallExpression func)
            return false;

        if (!func.IsStarArg || func.IsDistinct || func.Arguments.Count != 0)
            return false;

        if (!string.Equals(func.FunctionName, "COUNT", StringComparison.OrdinalIgnoreCase))
            return false;

        if (_catalog.IsView(simpleRef.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        // Validate table exists and build the direct count operator.
        GetSchema(simpleRef.TableName);
        string outputName = stmt.Columns[0].Alias ?? "COUNT(*)";
        var outputSchema = new[]
        {
            new ColumnDefinition
            {
                Name = outputName,
                Type = DbType.Integer,
                Nullable = false,
            },
        };

        return TryBuildTableRowCountQuery(simpleRef.TableName, outputSchema, out result);
    }

    private bool TryBuildSimpleSystemCatalogCountStarQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (!TryNormalizeSystemCatalogTableName(simpleRef.TableName, out string normalized))
            return false;

        // Backed by row data, not static metadata; use the normal aggregate pipeline for correctness.
        if (string.Equals(normalized, "sys.saved_queries", StringComparison.Ordinal))
            return false;

        if (stmt.Where != null || stmt.GroupBy != null || stmt.Having != null)
            return false;

        if (stmt.OrderBy is { Count: > 0 })
            return false;

        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
            return false;

        if (stmt.Columns[0].Expression is not FunctionCallExpression func)
            return false;

        if (!func.IsStarArg || func.IsDistinct || func.Arguments.Count != 0)
            return false;

        if (!string.Equals(func.FunctionName, "COUNT", StringComparison.OrdinalIgnoreCase))
            return false;

        long count = normalized switch
        {
            "sys.tables" => _catalog.GetTableNames().Count,
            "sys.columns" => CountSystemColumns(),
            "sys.indexes" => CountSystemIndexes(),
            "sys.foreign_keys" => CountSystemForeignKeys(),
            "sys.views" => _catalog.GetViewNames().Count,
            "sys.triggers" => _catalog.GetTriggers().Count,
            "sys.objects" => CountSystemObjects(),
            "sys.table_stats" => _catalog.GetTableStatistics().Count,
            "sys.column_stats" => _catalog.GetColumnStatistics().Count,
            _ => 0,
        };

        var row = new[] { DbValue.FromInteger(count) };
        var outputSchema = stmt.Columns[0].Alias is { Length: > 0 } alias
            ? new[]
            {
                new ColumnDefinition
                {
                    Name = alias,
                    Type = DbType.Integer,
                    Nullable = false,
                },
            }
            : DefaultCountStarOutputSchema;

        result = QueryResult.FromSyncLookup(row, outputSchema);
        return true;
    }

    private long CountSystemColumns()
    {
        if (_systemColumnsCountCache.HasValue)
            return _systemColumnsCountCache.Value;

        long count = 0;
        foreach (string tableName in _catalog.GetTableNames())
        {
            var schema = _catalog.GetTable(tableName);
            if (schema != null)
                count += schema.Columns.Count;
        }

        _systemColumnsCountCache = count;
        return count;
    }

    private long CountSystemIndexes()
    {
        if (_systemIndexesCountCache.HasValue)
            return _systemIndexesCountCache.Value;

        long count = 0;
        foreach (var index in _catalog.GetIndexes())
        {
            if (index.Kind == IndexKind.ForeignKeyInternal)
                continue;

            count += index.Columns.Count;
        }

        _systemIndexesCountCache = count;
        return count;
    }

    private long CountSystemForeignKeys()
    {
        if (_systemForeignKeysCountCache.HasValue)
            return _systemForeignKeysCountCache.Value;

        long count = 0;
        foreach (string tableName in _catalog.GetTableNames())
        {
            TableSchema? schema = _catalog.GetTable(tableName);
            if (schema is not null)
                count += schema.ForeignKeys.Count;
        }

        _systemForeignKeysCountCache = count;
        return count;
    }

    private long CountSystemObjects() =>
        _catalog.GetTableNames().Count
        + _catalog.GetIndexes().Count(index => index.Kind != IndexKind.ForeignKeyInternal)
        + CountSystemForeignKeys()
        + _catalog.GetViewNames().Count
        + _catalog.GetTriggers().Count;

    private bool TryBuildSimpleScalarAggregateColumnQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.Where != null || stmt.GroupBy != null || stmt.Having != null)
            return false;

        if (stmt.OrderBy is { Count: > 0 })
            return false;

        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
            return false;

        if (stmt.Columns[0].Expression is not FunctionCallExpression func)
            return false;

        if (func.IsStarArg || func.Arguments.Count != 1)
            return false;

        if (func.FunctionName is not ("COUNT" or "SUM" or "AVG" or "MIN" or "MAX"))
            return false;

        if (func.Arguments[0] is not ColumnRefExpression col)
            return false;

        if (_catalog.IsView(simpleRef.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        if (col.TableAlias != null)
        {
            string expectedAlias = simpleRef.Alias ?? simpleRef.TableName;
            if (!string.Equals(col.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        var schema = GetSchema(simpleRef.TableName);
        int columnIndex = schema.GetColumnIndex(col.ColumnName);
        if (columnIndex < 0)
            return false;

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        if (TryBuildPrimaryKeyCountQuery(simpleRef.TableName, schema, columnIndex, func, outputSchema, out result))
            return true;

        if (TryBuildKeyAggregateQuery(
                simpleRef.TableName,
                schema,
                where: null,
                columnIndex,
                func.FunctionName,
                outputSchema,
                isDistinct: func.IsDistinct,
                isCountStar: false,
                out result))
        {
            return true;
        }

        var tableTree = _catalog.GetTableTree(simpleRef.TableName, _pager);
        result = new QueryResult(new ScalarAggregateTableOperator(
            tableTree,
            columnIndex,
            func.FunctionName,
            outputSchema,
            isDistinct: func.IsDistinct,
            recordSerializer: GetReadSerializer(schema)));
        return true;
    }

    private bool TryBuildSimpleLookupScalarAggregateColumnQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.Where == null || stmt.GroupBy != null || stmt.Having != null)
            return false;

        if (stmt.OrderBy is { Count: > 0 })
            return false;

        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
            return false;

        if (stmt.Columns[0].Expression is not FunctionCallExpression func)
            return false;

        bool isCountStar = false;
        ColumnRefExpression? aggregateColumnRef = null;
        Expression? aggregateExpression = null;

        if (func.IsStarArg)
        {
            if (func.FunctionName != "COUNT" || func.IsDistinct || func.Arguments.Count != 0)
                return false;

            isCountStar = true;
        }
        else
        {
            if (func.Arguments.Count != 1)
                return false;

            if (func.FunctionName is not ("COUNT" or "SUM" or "AVG" or "MIN" or "MAX"))
                return false;

            aggregateExpression = func.Arguments[0];
            aggregateColumnRef = aggregateExpression as ColumnRefExpression;
        }

        if (_catalog.IsView(simpleRef.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        if (aggregateColumnRef?.TableAlias != null)
        {
            string expectedAlias = simpleRef.Alias ?? simpleRef.TableName;
            if (!string.Equals(aggregateColumnRef.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        var schema = GetSchema(simpleRef.TableName);
        int columnIndex = -1;
        if (aggregateColumnRef != null)
        {
            columnIndex = aggregateColumnRef.TableAlias != null
                ? schema.GetQualifiedColumnIndex(aggregateColumnRef.TableAlias, aggregateColumnRef.ColumnName)
                : schema.GetColumnIndex(aggregateColumnRef.ColumnName);
            if (columnIndex < 0)
                return false;
        }

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        if ((isCountStar || columnIndex >= 0) &&
            TryBuildKeyAggregateQuery(
                simpleRef.TableName,
                schema,
                stmt.Where,
                columnIndex,
                func.FunctionName,
                outputSchema,
                isDistinct: func.IsDistinct,
                isCountStar,
                out result))
        {
            return true;
        }

        IOperator? indexedSource = null;
        Expression? indexedRemainingWhere = null;

        if (!isCountStar && columnIndex >= 0)
        {
            indexedSource = TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out indexedRemainingWhere);
            if (indexedSource != null && indexedRemainingWhere == null)
            {
                result = indexedSource switch
                {
                    PrimaryKeyLookupOperator pk => new QueryResult(new ScalarAggregateLookupOperator(
                        pk.TableTree,
                        pk.SeekKey,
                        columnIndex,
                        func.FunctionName,
                        outputSchema,
                        isDistinct: func.IsDistinct,
                        recordSerializer: GetReadSerializer(schema))),
                    IndexScanOperator idx => new QueryResult(new ScalarAggregateLookupOperator(
                        idx.IndexStore,
                        idx.TableTree,
                        idx.SeekValue,
                        columnIndex,
                        func.FunctionName,
                        outputSchema,
                        isDistinct: func.IsDistinct,
                        recordSerializer: GetReadSerializer(schema))),
                    UniqueIndexLookupOperator uniq => new QueryResult(new ScalarAggregateLookupOperator(
                        uniq.IndexStore,
                        uniq.TableTree,
                        uniq.SeekValue,
                        columnIndex,
                        func.FunctionName,
                        outputSchema,
                        isDistinct: func.IsDistinct,
                        recordSerializer: GetReadSerializer(schema))),
                    _ => null!,
                };

                if (result != null)
                    return true;
            }
        }

        indexedSource ??= TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out indexedRemainingWhere);
        if (indexedSource == null)
        {
            indexedSource = TryBuildIntegerIndexRangeScan(simpleRef.TableName, stmt.Where, schema, out indexedRemainingWhere)
                ?? TryBuildOrderedTextIndexRangeScan(simpleRef.TableName, stmt.Where, schema, out indexedRemainingWhere);
        }

        if (indexedSource is IEncodedPayloadSource &&
            ShouldUseIndexedPayloadAggregateFastPath(simpleRef.TableName, schema, indexedSource))
        {
            TrySetDecodedColumnIndices(indexedSource, Array.Empty<int>());

            var indexedDecodeColumns = new HashSet<int>();
            if (indexedRemainingWhere != null &&
                !TryAccumulateReferencedColumns(indexedRemainingWhere, schema, indexedDecodeColumns))
            {
                return false;
            }

            if (!isCountStar)
            {
                if (!TryAccumulateReferencedColumns(aggregateExpression!, schema, indexedDecodeColumns))
                    return false;
            }

            var compactIndexedDecodeColumns = ToSortedColumnIndices(indexedDecodeColumns);
            var compactIndexedSchema = CreateCompactProjectionSchema(schema, compactIndexedDecodeColumns);
            int compactIndexedAggregateColumnIndex = -1;
            if (columnIndex >= 0)
            {
                compactIndexedAggregateColumnIndex = Array.BinarySearch(compactIndexedDecodeColumns, columnIndex);
                if (compactIndexedAggregateColumnIndex < 0)
                    return false;
            }

            Func<DbValue[], DbValue>? compactIndexedPredicateEvaluator = indexedRemainingWhere != null
                ? GetOrCompileExpression(indexedRemainingWhere, compactIndexedSchema)
                : null;
            Func<DbValue[], DbValue>? compactIndexedAggregateEvaluator = !isCountStar && compactIndexedAggregateColumnIndex < 0
                ? GetOrCompileExpression(aggregateExpression!, compactIndexedSchema)
                : null;
            var indexedScalarBatchPlan = BatchPlanCompiler.TryCreateScalarAggregate(
                indexedRemainingWhere,
                func.FunctionName,
                aggregateExpression,
                isCountStar,
                func.IsDistinct,
                compactIndexedSchema);

            result = new QueryResult(new FilteredScalarAggregatePayloadOperator(
                indexedSource,
                GetReadSerializer(schema),
                compactIndexedDecodeColumns,
                compactIndexedAggregateColumnIndex,
                func.FunctionName,
                outputSchema,
                predicateEvaluator: compactIndexedPredicateEvaluator,
                aggregateArgumentEvaluator: compactIndexedAggregateEvaluator,
                isDistinct: func.IsDistinct,
                isCountStar,
                batchPlan: indexedScalarBatchPlan));
            return true;
        }

        var decodeColumns = new HashSet<int>();
        if (!TryAccumulateReferencedColumns(stmt.Where, schema, decodeColumns))
            return false;

        if (!isCountStar)
        {
            if (!TryAccumulateReferencedColumns(aggregateExpression!, schema, decodeColumns))
                return false;
        }

        var filteredDecodeColumns = ToSortedColumnIndices(decodeColumns);
        var compactSchema = CreateCompactProjectionSchema(schema, filteredDecodeColumns);
        int compactAggregateColumnIndex = -1;
        if (columnIndex >= 0)
        {
            compactAggregateColumnIndex = Array.BinarySearch(filteredDecodeColumns, columnIndex);
            if (compactAggregateColumnIndex < 0)
                return false;
        }

        var compactPredicateEvaluator = GetOrCompileExpression(stmt.Where, compactSchema);
        Func<DbValue[], DbValue>? compactAggregateEvaluator = !isCountStar && compactAggregateColumnIndex < 0
            ? GetOrCompileExpression(aggregateExpression!, compactSchema)
            : null;
        var scalarBatchPlan = BatchPlanCompiler.TryCreateScalarAggregate(
            stmt.Where,
            func.FunctionName,
            aggregateExpression,
            isCountStar,
            func.IsDistinct,
            compactSchema);

        result = new QueryResult(new FilteredScalarAggregateTableOperator(
            _catalog.GetTableTree(simpleRef.TableName, _pager),
            compactAggregateColumnIndex,
            func.FunctionName,
            outputSchema,
            compactPredicateEvaluator,
            filteredDecodeColumns,
            aggregateArgumentEvaluator: compactAggregateEvaluator,
            isDistinct: func.IsDistinct,
            isCountStar,
            recordSerializer: GetReadSerializer(schema),
            batchPlan: scalarBatchPlan));
        return true;
    }

    private bool ShouldUseIndexedPayloadAggregateFastPath(string tableName, TableSchema schema, IOperator source)
    {
        if (source is not IndexOrderedScanOperator)
            return true;

        if (TryEstimateOrderedPayloadAggregateRangeSelectivity(tableName, schema, (IndexOrderedScanOperator)source, out double estimatedFraction))
        {
            const double maxSelectiveRangeFractionForSmallTablePayloadAggregate = 0.25;
            if (estimatedFraction <= maxSelectiveRangeFractionForSmallTablePayloadAggregate)
                return true;
        }

        int? tableRowCount = TryGetIndexedPayloadAggregateTableRowCountCapacityHint(tableName);
        const int minTableRowCountForOrderedPayloadAggregate = 20_000;
        return !tableRowCount.HasValue || tableRowCount.Value >= minTableRowCountForOrderedPayloadAggregate;
    }

    private int? TryGetIndexedPayloadAggregateTableRowCountCapacityHint(string tableName)
    {
        if (_catalog.TryGetEstimatedTableRowCount(tableName, out long rowCount))
            return ToCapacityHint(rowCount);

        return TryGetCachedTreeRowCountCapacityHint(_catalog.GetTableTree(tableName, _pager));
    }

    private bool TryEstimateOrderedPayloadAggregateRangeSelectivity(
        string tableName,
        TableSchema schema,
        IndexOrderedScanOperator source,
        out double estimatedFraction)
    {
        estimatedFraction = 0;

        if ((uint)source.KeyColumnIndex >= (uint)schema.Columns.Count)
            return false;

        ColumnDefinition keyColumn = schema.Columns[source.KeyColumnIndex];
        if (keyColumn.Type != DbType.Integer ||
            !_catalog.TryGetFreshColumnStatistics(tableName, keyColumn.Name, out var stats) ||
            stats.NonNullCount <= 0 ||
            stats.MinValue.IsNull ||
            stats.MaxValue.IsNull ||
            stats.MinValue.Type != DbType.Integer ||
            stats.MaxValue.Type != DbType.Integer)
        {
            return false;
        }

        long statsMin = stats.MinValue.AsInteger;
        long statsMax = stats.MaxValue.AsInteger;
        if (statsMax < statsMin)
            return false;

        if (!TryNormalizeInclusiveIntegerRange(source.ScanRange, statsMin, statsMax, out long effectiveLower, out long effectiveUpper))
        {
            estimatedFraction = 0;
            return true;
        }

        double domainWidth = (double)statsMax - statsMin + 1d;
        if (domainWidth <= 0)
            return false;

        double coveredWidth = (double)effectiveUpper - effectiveLower + 1d;
        estimatedFraction = Math.Clamp(coveredWidth / domainWidth, 0d, 1d);
        return true;
    }

    private static bool TryNormalizeInclusiveIntegerRange(
        IndexScanRange range,
        long statsMin,
        long statsMax,
        out long effectiveLower,
        out long effectiveUpper)
    {
        effectiveLower = range.LowerBound ?? statsMin;
        effectiveUpper = range.UpperBound ?? statsMax;

        if (range.LowerBound.HasValue && !range.LowerInclusive)
        {
            if (effectiveLower == long.MaxValue)
                return false;

            effectiveLower++;
        }

        if (range.UpperBound.HasValue && !range.UpperInclusive)
        {
            if (effectiveUpper == long.MinValue)
                return false;

            effectiveUpper--;
        }

        if (effectiveLower < statsMin)
            effectiveLower = statsMin;
        if (effectiveUpper > statsMax)
            effectiveUpper = statsMax;

        return effectiveLower <= effectiveUpper;
    }

    private bool TryBuildKeyAggregateQuery(
        string tableName,
        TableSchema schema,
        Expression? where,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct,
        bool isCountStar,
        out QueryResult result)
    {
        result = null!;

        if (TryBuildTableKeyAggregateQuery(
                tableName,
                schema,
                where,
                columnIndex,
                functionName,
                outputSchema,
                isDistinct,
                isCountStar,
                out result))
        {
            return true;
        }

        if (where == null)
        {
            if (isCountStar)
                return false;

            if (!TryFindDirectIntegerIndexForColumn(tableName, schema, columnIndex, out var directIndex))
                return false;

            result = new QueryResult(new IndexKeyAggregateOperator(
                _catalog.GetIndexStore(directIndex.IndexName, _pager),
                IndexScanRange.All,
                functionName,
                outputSchema,
                isDistinct));
            return true;
        }

        if (TryBuildIntegerIndexRangeScan(tableName, where, schema, out var remainingWhere) is not IndexOrderedScanOperator orderedScan ||
            remainingWhere != null)
        {
            return false;
        }

        if (!isCountStar && orderedScan.KeyColumnIndex != columnIndex)
            return false;

        result = new QueryResult(new IndexKeyAggregateOperator(
            orderedScan.IndexStore,
            orderedScan.ScanRange,
            functionName,
            outputSchema,
            isDistinct));
        return true;
    }

    private bool TryBuildPrimaryKeyCountQuery(
        string tableName,
        TableSchema schema,
        int columnIndex,
        FunctionCallExpression func,
        ColumnDefinition[] outputSchema,
        out QueryResult result)
    {
        result = null!;

        if (!string.Equals(func.FunctionName, "COUNT", StringComparison.OrdinalIgnoreCase))
            return false;

        int pkIndex = schema.PrimaryKeyColumnIndex;
        if (pkIndex < 0 || columnIndex != pkIndex)
            return false;

        if (schema.Columns[pkIndex].Type != DbType.Integer)
            return false;

        return TryBuildTableRowCountQuery(tableName, outputSchema, out result);
    }

    private bool TryBuildTableRowCountQuery(string tableName, ColumnDefinition[] outputSchema, out QueryResult result)
    {
        if (TryGetExactTableRowCount(tableName, out long rowCount))
        {
            result = QueryResult.FromSyncLookup([DbValue.FromInteger(rowCount)], outputSchema);
            return true;
        }

        var tree = _catalog.GetTableTree(tableName, _pager);
        result = new QueryResult(new CountStarTableOperator(tree, outputSchema, ignoreCachedCount: true));
        return true;
    }

    private bool TryBuildTableKeyAggregateQuery(
        string tableName,
        TableSchema schema,
        Expression? where,
        int columnIndex,
        string functionName,
        ColumnDefinition[] outputSchema,
        bool isDistinct,
        bool isCountStar,
        out QueryResult result)
    {
        result = null!;

        int pkIndex = schema.PrimaryKeyColumnIndex;
        if (pkIndex < 0 || schema.Columns[pkIndex].Type != DbType.Integer)
            return false;

        if (!isCountStar && columnIndex != pkIndex)
            return false;

        if (where == null)
        {
            var tableTree = _catalog.GetTableTree(tableName, _pager);
            result = new QueryResult(new TableKeyAggregateOperator(
                tableTree,
                IndexScanRange.All,
                functionName,
                outputSchema));
            return true;
        }

        ExtractOrderedIndexRange(where, schema, pkIndex, out var scanRange, out var remainingWhere, out int consumedTermCount);
        if (consumedTermCount == 0 || remainingWhere != null)
            return false;

        var rangedTableTree = _catalog.GetTableTree(tableName, _pager);
        result = new QueryResult(new TableKeyAggregateOperator(
            rangedTableTree,
            scanRange,
            functionName,
            outputSchema));
        return true;
    }

    private bool TryBuildSimpleConstantGroupAggregateColumnQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.Where != null || stmt.Having != null)
            return false;

        if (stmt.GroupBy is not { Count: > 0 } || !stmt.GroupBy.All(e => e is LiteralExpression))
            return false;

        if (stmt.OrderBy is { Count: > 0 })
            return false;

        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        if (stmt.Columns.Count != 1 || stmt.Columns[0].IsStar)
            return false;

        if (stmt.Columns[0].Expression is not FunctionCallExpression func)
            return false;

        if (func.IsStarArg || func.Arguments.Count != 1)
            return false;

        if (func.FunctionName is not ("COUNT" or "SUM" or "AVG" or "MIN" or "MAX"))
            return false;

        if (func.Arguments[0] is not ColumnRefExpression col)
            return false;

        if (_catalog.IsView(simpleRef.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        if (col.TableAlias != null)
        {
            string expectedAlias = simpleRef.Alias ?? simpleRef.TableName;
            if (!string.Equals(col.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        var schema = GetSchema(simpleRef.TableName);
        int columnIndex = schema.GetColumnIndex(col.ColumnName);
        if (columnIndex < 0)
            return false;

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        var tableTree = _catalog.GetTableTree(simpleRef.TableName, _pager);
        result = new QueryResult(new ScalarAggregateTableOperator(
            tableTree,
            columnIndex,
            func.FunctionName,
            outputSchema,
            isDistinct: func.IsDistinct,
            emitOnEmptyInput: false,
            recordSerializer: GetReadSerializer(schema)));
        return true;
    }

    private bool TryBuildSimpleGroupedIndexAggregateQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.GroupBy is not { Count: 1 })
            return false;

        if (stmt.Columns.Count == 0 || stmt.Columns.Any(c => c.IsStar))
            return false;

        if (stmt.GroupBy[0] is not ColumnRefExpression groupByColumn)
            return false;

        if (_catalog.IsView(simpleRef.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        string expectedAlias = simpleRef.Alias ?? simpleRef.TableName;
        var schema = GetSchema(simpleRef.TableName);
        if (!TryResolveSimpleColumnRefIndex(groupByColumn, schema, expectedAlias, out int groupColumnIndex))
            return false;

        var groupColumn = schema.Columns[groupColumnIndex];
        if (groupColumn.Type != DbType.Integer || groupColumn.Nullable)
            return false;

        if (!TryFindDirectIntegerIndexForColumn(simpleRef.TableName, schema, groupColumnIndex, out var directIndex))
            return false;

        if (!TryBuildGroupedIndexAggregateProjection(stmt.Columns, schema, expectedAlias, groupColumnIndex, out var projectionKinds))
            return false;

        if (!CanUseNaturalGroupedIndexOrder(stmt.OrderBy, schema, expectedAlias, groupColumnIndex))
            return false;

        if (!TryBuildGroupedIndexAggregateCountPredicate(
                stmt.Having,
                out var countPredicateKind,
                out long countPredicateValue))
        {
            return false;
        }

        IndexScanRange scanRange = IndexScanRange.All;
        if (stmt.Where != null)
        {
            ExtractOrderedIndexRange(
                stmt.Where,
                schema,
                groupColumnIndex,
                out scanRange,
                out var residualWhere,
                out int consumedTermCount);

            if (consumedTermCount == 0 || residualWhere != null)
                return false;
        }

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        var projectionKindIds = Array.ConvertAll(projectionKinds, static kind => (int)kind);
        IOperator op = new IndexGroupedAggregateOperator(
            _catalog.GetIndexStore(directIndex.IndexName, _pager),
            scanRange,
            outputSchema,
            projectionKindIds,
            countPredicateKind,
            countPredicateValue);

        if (stmt.Offset.HasValue)
            op = new OffsetOperator(op, stmt.Offset.Value);
        if (stmt.Limit.HasValue)
            op = new LimitOperator(op, stmt.Limit.Value);

        result = new QueryResult(op);
        return true;
    }

    private bool TryBuildCompositeGroupedIndexAggregateQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;
        if (stmt.Where != null || stmt.Having != null || stmt.OrderBy is { Count: > 0 } || stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;
        if (stmt.GroupBy is not { Count: > 0 })
            return false;
        if (stmt.Columns.Count == 0 || stmt.Columns.Any(c => c.IsStar))
            return false;
        if (_catalog.IsView(simpleRef.TableName))
            return false;
        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        string expectedAlias = simpleRef.Alias ?? simpleRef.TableName;
        var schema = GetSchema(simpleRef.TableName);
        if (!TryResolveCompositeGroupByColumns(stmt.GroupBy, schema, expectedAlias, out var groupColumnIndices))
            return false;

        var matchingIndex = FindCompositeGroupedIndex(simpleRef.TableName, schema, groupColumnIndices);
        if (matchingIndex == null)
            return false;

        if (!TryBuildCompositeGroupedAggregateProjection(
                stmt.Columns,
                schema,
                expectedAlias,
                groupColumnIndices,
                out var projectionKinds))
        {
            return false;
        }

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        result = new QueryResult(new CompositeIndexGroupedAggregateOperator(
            _catalog.GetIndexStore(matchingIndex.IndexName, _pager),
            _catalog.GetTableTree(simpleRef.TableName, _pager),
            GetReadSerializer(schema),
            groupColumnIndices,
            outputSchema,
            projectionKinds));
        return true;
    }

    private static bool TryBuildGroupedIndexAggregateCountPredicate(
        Expression? having,
        out GroupedIndexAggregateCountPredicateKind predicateKind,
        out long predicateValue)
    {
        predicateKind = GroupedIndexAggregateCountPredicateKind.None;
        predicateValue = 0;

        if (having == null)
            return true;

        if (having is not BinaryExpression binary)
            return false;

        if (TryMatchCountStarComparison(binary.Left, binary.Right, binary.Op, out predicateKind, out predicateValue))
            return true;

        return TryMatchCountStarComparison(binary.Right, binary.Left, InvertBinaryComparison(binary.Op), out predicateKind, out predicateValue);
    }

    private static bool CanUseNaturalGroupedIndexOrder(
        List<OrderByClause>? orderBy,
        TableSchema schema,
        string expectedAlias,
        int groupColumnIndex)
    {
        if (orderBy is not { Count: > 0 })
            return true;

        if (orderBy.Count != 1)
            return false;

        var clause = orderBy[0];
        if (clause.Descending)
            return false;

        if (clause.Expression is not ColumnRefExpression columnRef)
            return false;

        return TryResolveSimpleColumnRefIndex(columnRef, schema, expectedAlias, out int orderColumnIndex) &&
               orderColumnIndex == groupColumnIndex;
    }

    private static bool TryMatchCountStarComparison(
        Expression aggregateExpr,
        Expression literalExpr,
        BinaryOp op,
        out GroupedIndexAggregateCountPredicateKind predicateKind,
        out long predicateValue)
    {
        predicateKind = GroupedIndexAggregateCountPredicateKind.None;
        predicateValue = 0;

        if (!IsCountStarExpression(aggregateExpr) ||
            literalExpr is not LiteralExpression literal ||
            literal.LiteralType != TokenType.IntegerLiteral ||
            literal.Value is not long longValue)
        {
            return false;
        }

        predicateKind = op switch
        {
            BinaryOp.Equals => GroupedIndexAggregateCountPredicateKind.Equals,
            BinaryOp.NotEquals => GroupedIndexAggregateCountPredicateKind.NotEquals,
            BinaryOp.LessThan => GroupedIndexAggregateCountPredicateKind.LessThan,
            BinaryOp.GreaterThan => GroupedIndexAggregateCountPredicateKind.GreaterThan,
            BinaryOp.LessOrEqual => GroupedIndexAggregateCountPredicateKind.LessOrEqual,
            BinaryOp.GreaterOrEqual => GroupedIndexAggregateCountPredicateKind.GreaterOrEqual,
            _ => GroupedIndexAggregateCountPredicateKind.None,
        };

        if (predicateKind == GroupedIndexAggregateCountPredicateKind.None)
            return false;

        predicateValue = longValue;
        return true;
    }

    private static BinaryOp InvertBinaryComparison(BinaryOp op)
    {
        return op switch
        {
            BinaryOp.LessThan => BinaryOp.GreaterThan,
            BinaryOp.GreaterThan => BinaryOp.LessThan,
            BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
            BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual,
            _ => op,
        };
    }

    private static bool IsCountStarExpression(Expression expression)
    {
        return expression is FunctionCallExpression
        {
            FunctionName: "COUNT",
            IsDistinct: false,
            IsStarArg: true,
            Arguments.Count: 0,
        };
    }

    private static bool TryResolveCompositeGroupByColumns(
        List<Expression> groupByExpressions,
        TableSchema schema,
        string expectedAlias,
        out int[] groupColumnIndices)
    {
        groupColumnIndices = Array.Empty<int>();
        var resolved = new int[groupByExpressions.Count];

        for (int i = 0; i < groupByExpressions.Count; i++)
        {
            if (groupByExpressions[i] is not ColumnRefExpression columnRef ||
                !TryResolveSimpleColumnRefIndex(columnRef, schema, expectedAlias, out int columnIndex))
            {
                return false;
            }

            var column = schema.Columns[columnIndex];
            if (column.Nullable || column.Type is not (DbType.Integer or DbType.Text))
                return false;

            for (int j = 0; j < i; j++)
            {
                if (resolved[j] == columnIndex)
                    return false;
            }

            resolved[i] = columnIndex;
        }

        groupColumnIndices = resolved;
        return true;
    }

    private IndexSchema? FindCompositeGroupedIndex(string tableName, TableSchema schema, ReadOnlySpan<int> groupColumnIndices)
    {
        IndexSchema? best = null;
        foreach (var index in _catalog.GetSqlIndexesForTable(tableName))
        {
            if (index.Columns.Count < groupColumnIndices.Length || index.Columns.Count < 2)
                continue;

            bool matches = true;
            for (int i = 0; i < groupColumnIndices.Length; i++)
            {
                if (!string.Equals(
                        index.Columns[i],
                        schema.Columns[groupColumnIndices[i]].Name,
                        StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (!matches)
                continue;

            if (best == null ||
                index.Columns.Count < best.Columns.Count ||
                (index.Columns.Count == best.Columns.Count && index.IsUnique && !best.IsUnique))
            {
                best = index;
            }
        }

        return best;
    }

    private static bool TryBuildCompositeGroupedAggregateProjection(
        List<SelectColumn> columns,
        TableSchema schema,
        string expectedAlias,
        ReadOnlySpan<int> groupColumnIndices,
        out int[] projectionKinds)
    {
        projectionKinds = Array.Empty<int>();
        var kinds = new int[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            var expression = columns[i].Expression;
            if (expression is null)
                return false;

            if (expression is ColumnRefExpression columnRef)
            {
                if (!TryResolveSimpleColumnRefIndex(columnRef, schema, expectedAlias, out int columnIndex))
                    return false;

                int groupOrdinal = -1;
                for (int j = 0; j < groupColumnIndices.Length; j++)
                {
                    if (groupColumnIndices[j] == columnIndex)
                    {
                        groupOrdinal = j;
                        break;
                    }
                }

                if (groupOrdinal < 0)
                    return false;

                kinds[i] = groupOrdinal;
                continue;
            }

            if (expression is FunctionCallExpression
                {
                    FunctionName: "COUNT",
                    IsDistinct: false,
                    IsStarArg: true,
                    Arguments.Count: 0,
                })
            {
                kinds[i] = -1;
                continue;
            }

            return false;
        }

        projectionKinds = kinds;
        return true;
    }

    private static bool TryBuildGroupedIndexAggregateProjection(
        List<SelectColumn> columns,
        TableSchema schema,
        string expectedAlias,
        int groupColumnIndex,
        out GroupedIndexAggregateProjectionKind[] projectionKinds)
    {
        projectionKinds = Array.Empty<GroupedIndexAggregateProjectionKind>();
        var kinds = new GroupedIndexAggregateProjectionKind[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            var expression = columns[i].Expression;
            if (expression is null)
                return false;

            if (expression is ColumnRefExpression columnRef)
            {
                if (!TryResolveSimpleColumnRefIndex(columnRef, schema, expectedAlias, out int columnIndex) ||
                    columnIndex != groupColumnIndex)
                {
                    return false;
                }

                kinds[i] = GroupedIndexAggregateProjectionKind.GroupKey;
                continue;
            }

            if (expression is not FunctionCallExpression func || func.IsDistinct)
                return false;

            if (func.IsStarArg)
            {
                if (func.FunctionName != "COUNT" || func.Arguments.Count != 0)
                    return false;

                kinds[i] = GroupedIndexAggregateProjectionKind.Count;
                continue;
            }

            if (func.Arguments.Count != 1 || func.Arguments[0] is not ColumnRefExpression aggregateColumn)
                return false;

            if (!TryResolveSimpleColumnRefIndex(aggregateColumn, schema, expectedAlias, out int aggregateColumnIndex) ||
                aggregateColumnIndex != groupColumnIndex)
            {
                return false;
            }

            if (func.FunctionName is not ("COUNT" or "SUM" or "AVG" or "MIN" or "MAX"))
                return false;

            kinds[i] = func.FunctionName switch
            {
                "COUNT" => GroupedIndexAggregateProjectionKind.Count,
                "SUM" => GroupedIndexAggregateProjectionKind.Sum,
                "AVG" => GroupedIndexAggregateProjectionKind.Avg,
                "MIN" => GroupedIndexAggregateProjectionKind.Min,
                "MAX" => GroupedIndexAggregateProjectionKind.Max,
                _ => GroupedIndexAggregateProjectionKind.GroupKey,
            };
        }

        projectionKinds = kinds;
        return true;
    }

    private static bool TryResolveSimpleColumnRefIndex(
        ColumnRefExpression columnRef,
        TableSchema schema,
        string expectedAlias,
        out int columnIndex)
    {
        columnIndex = -1;
        if (columnRef.TableAlias != null &&
            !string.Equals(columnRef.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        columnIndex = schema.GetColumnIndex(columnRef.ColumnName);
        return columnIndex >= 0;
    }

    private async ValueTask<QueryResult> ExecuteDeleteAsync(DeleteStatement stmt, CancellationToken ct)
    {
        if (ContainsSubqueries(stmt))
            stmt = await RewriteSubqueriesInDeleteAsync(stmt, ct);
        bool hasRemainingSubqueries = ContainsSubqueries(stmt);

        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName, _pager);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);

        // Collect rows to delete (can't modify tree while iterating)
        int? deleteCapacityHint = TryGetCachedTreeRowCountCapacityHint(tree);
        var rowsToDelete = deleteCapacityHint.HasValue
            ? new List<(long rowId, DbValue[] row)>(deleteCapacityHint.Value)
            : new List<(long rowId, DbValue[] row)>();
        var scan = new TableScanOperator(tree, schema, GetReadSerializer(schema), deleteCapacityHint);
        await scan.OpenAsync(ct);
        while (await scan.MoveNextAsync(ct))
        {
            if (stmt.Where != null)
            {
                var result = hasRemainingSubqueries && ContainsSubqueries(stmt.Where)
                    ? await EvaluateExpressionWithSubqueriesAsync(
                        stmt.Where,
                        scan.Current,
                        schema,
                        Array.Empty<CorrelationScope>(),
                        ct)
                    : ExpressionEvaluator.Evaluate(stmt.Where, scan.Current, schema);
                if (!result.IsTruthy) continue;
            }
            rowsToDelete.Add((scan.CurrentRowId, (DbValue[])scan.Current.Clone()));
        }

        bool hasIncomingForeignKeys = _catalog.GetReferencingForeignKeys(stmt.TableName).Count > 0;
        if (!hasIncomingForeignKeys)
        {
            foreach (var (rowId, row) in rowsToDelete)
            {
                // BEFORE DELETE triggers
                await FireTriggersAsync(stmt.TableName, TriggerTiming.Before, TriggerEvent.Delete, row, null, schema, ct);

                await tree.DeleteAsync(rowId, ct);

                // Maintain indexes
                await DeleteFromAllIndexesAsync(indexes, schema, row, rowId, ct);
                await _catalog.AdjustTableRowCountAsync(stmt.TableName, -1, ct);

                // AFTER DELETE triggers
                await FireTriggersAsync(stmt.TableName, TriggerTiming.After, TriggerEvent.Delete, row, null, schema, ct);
            }

            if (rowsToDelete.Count > 0)
                await _catalog.MarkTableColumnStatisticsStaleAsync(stmt.TableName, ct);

            await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

            return new QueryResult(rowsToDelete.Count);
        }

        int deleted = 0;
        var mutationContext = new ForeignKeyMutationContext();
        foreach (var (rowId, _) in rowsToDelete)
        {
            if (await DeleteRowWithForeignKeysAsync(
                    stmt.TableName,
                    schema,
                    tree,
                    indexes,
                    rowId,
                    mutationContext,
                    depth: 0,
                    ct))
            {
                deleted++;
            }
        }

        await PersistForeignKeyMutationContextAsync(mutationContext, stmt.TableName, deleted > 0, persistRootChanges: true, ct);

        return new QueryResult(deleted);
    }

    private async ValueTask<QueryResult> ExecuteUpdateAsync(UpdateStatement stmt, CancellationToken ct)
    {
        if (ContainsSubqueries(stmt))
            stmt = await RewriteSubqueriesInUpdateAsync(stmt, ct);
        bool hasRemainingSubqueries = ContainsSubqueries(stmt);

        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName, _pager);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);
        int pkIdx = schema.PrimaryKeyColumnIndex;
        bool hasIntegerPrimaryKey = pkIdx >= 0 && schema.Columns[pkIdx].Type == DbType.Integer;
        bool hasOutgoingForeignKeys = _catalog.GetForeignKeysForTable(stmt.TableName).Count > 0;
        bool hasIncomingForeignKeys = _catalog.GetReferencingForeignKeys(stmt.TableName).Count > 0;

        // Collect rows to update
        int? updateCapacityHint = TryGetCachedTreeRowCountCapacityHint(tree);
        var updates = updateCapacityHint.HasValue
            ? new List<(long rowId, DbValue[] oldRow, DbValue[] newRow)>(updateCapacityHint.Value)
            : new List<(long rowId, DbValue[] oldRow, DbValue[] newRow)>();
        var scan = new TableScanOperator(tree, schema, GetReadSerializer(schema), updateCapacityHint);
        await scan.OpenAsync(ct);
        while (await scan.MoveNextAsync(ct))
        {
            if (stmt.Where != null)
            {
                var result = hasRemainingSubqueries && ContainsSubqueries(stmt.Where)
                    ? await EvaluateExpressionWithSubqueriesAsync(
                        stmt.Where,
                        scan.Current,
                        schema,
                        Array.Empty<CorrelationScope>(),
                        ct)
                    : ExpressionEvaluator.Evaluate(stmt.Where, scan.Current, schema);
                if (!result.IsTruthy) continue;
            }

            var oldRow = (DbValue[])scan.Current.Clone();
            var newRow = (DbValue[])scan.Current.Clone();
            foreach (var set in stmt.SetClauses)
            {
                int colIdx = schema.GetColumnIndex(set.ColumnName);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{set.ColumnName}' not found.");
                newRow[colIdx] = hasRemainingSubqueries && ContainsSubqueries(set.Value)
                    ? await EvaluateExpressionWithSubqueriesAsync(
                        set.Value,
                        scan.Current,
                        schema,
                        Array.Empty<CorrelationScope>(),
                        ct)
                    : ExpressionEvaluator.Evaluate(set.Value, scan.Current, schema);
            }
            updates.Add((scan.CurrentRowId, oldRow, newRow));
        }

        if (!hasOutgoingForeignKeys && !hasIncomingForeignKeys)
        {
            foreach (var (rowId, oldRow, newRow) in updates)
            {
                // BEFORE UPDATE triggers
                await FireTriggersAsync(stmt.TableName, TriggerTiming.Before, TriggerEvent.Update, oldRow, newRow, schema, ct);

                long newRowId = rowId;
                if (hasIntegerPrimaryKey)
                {
                    if (newRow[pkIdx].IsNull)
                    {
                        // INTEGER PRIMARY KEY aliases the physical row key.
                        newRow[pkIdx] = DbValue.FromInteger(rowId);
                    }

                    if (newRow[pkIdx].Type != DbType.Integer)
                        throw new CSharpDbException(ErrorCode.TypeMismatch, "INTEGER PRIMARY KEY must remain an integer value.");

                    newRowId = newRow[pkIdx].AsInteger;
                }

                await tree.DeleteAsync(rowId, ct);
                await tree.InsertAsync(newRowId, _recordSerializer.Encode(newRow), ct);

                // Maintain indexes: remove old entries, add new entries, and update rowid payloads.
                await UpdateAllIndexesAsync(indexes, schema, oldRow, newRow, rowId, newRowId, ct);

                // AFTER UPDATE triggers
                await FireTriggersAsync(stmt.TableName, TriggerTiming.After, TriggerEvent.Update, oldRow, newRow, schema, ct);
            }

            if (updates.Count > 0)
                await _catalog.MarkTableColumnStatisticsStaleAsync(stmt.TableName, ct);

            await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

            return new QueryResult(updates.Count);
        }

        var mutationContext = new ForeignKeyMutationContext();
        foreach (var (rowId, oldRow, newRow) in updates)
        {
            // BEFORE UPDATE triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.Before, TriggerEvent.Update, oldRow, newRow, schema, ct);

            long newRowId = rowId;
            if (hasIntegerPrimaryKey)
            {
                if (newRow[pkIdx].IsNull)
                {
                    // INTEGER PRIMARY KEY aliases the physical row key.
                    newRow[pkIdx] = DbValue.FromInteger(rowId);
                }

                if (newRow[pkIdx].Type != DbType.Integer)
                    throw new CSharpDbException(ErrorCode.TypeMismatch, "INTEGER PRIMARY KEY must remain an integer value.");

                newRowId = newRow[pkIdx].AsInteger;
            }

            await ValidateIncomingForeignKeyUpdatesAsync(stmt.TableName, schema, rowId, oldRow, newRow, ct);
            await ValidateOutgoingForeignKeysAsync(stmt.TableName, schema, oldRow, newRow, ct);

            await tree.DeleteAsync(rowId, ct);
            await tree.InsertAsync(newRowId, _recordSerializer.Encode(newRow), ct);

            // Maintain indexes: remove old entries, add new entries, and update rowid payloads.
            await UpdateAllIndexesAsync(indexes, schema, oldRow, newRow, rowId, newRowId, ct);
            mutationContext.TouchedTables.Add(stmt.TableName);
            mutationContext.StaleTables.Add(stmt.TableName);

            // AFTER UPDATE triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.After, TriggerEvent.Update, oldRow, newRow, schema, ct);
        }

        await PersistForeignKeyMutationContextAsync(mutationContext, stmt.TableName, updates.Count > 0, persistRootChanges: true, ct);

        return new QueryResult(updates.Count);
    }

    #endregion

    #region FROM Clause / JOIN / View Expansion

    /// <summary>
    /// Recursively builds an operator tree from a TableRef AST node.
    /// Returns the operator and the schema (with qualified column mappings for JOINs).
    /// If the table name references a view, expands the view.
    /// </summary>
    private (IOperator op, TableSchema schema) BuildFromOperator(
        TableRef tableRef,
        Expression? outerWhere = null,
        bool pushDownOuterLocalPredicates = false)
    {
        if (tableRef is SimpleTableRef simple)
        {
            // Check if this is a CTE reference
            if (_cteData != null && _cteData.TryGetValue(simple.TableName, out var cteInfo))
            {
                var cteCols = cteInfo.Schema.Columns;
                // Make a copy of rows so each reference gets its own iteration
                var rowsCopy = cteInfo.Rows.Select(r => (DbValue[])r.Clone()).ToList();
                IOperator cteOp = new MaterializedOperator(rowsCopy, cteCols.ToArray());

                string cteAlias = simple.Alias ?? simple.TableName;
                var cteQualified = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < cteCols.Count; i++)
                    cteQualified[$"{cteAlias}.{cteCols[i].Name}"] = i;

                var cteSchema = new TableSchema
                {
                    TableName = cteInfo.Schema.TableName,
                    Columns = cteCols,
                    QualifiedMappings = cteQualified,
                };

                return (cteOp, cteSchema);
            }

            if (TryBuildSystemCatalogSource(simple, out var systemSource))
                return systemSource;

            // Check if this is a view
            var viewSql = _catalog.GetViewSql(simple.TableName);
            if (viewSql != null)
            {
                var viewQuery = Parser.Parse(viewSql) as QueryStatement
                    ?? throw new CSharpDbException(ErrorCode.SyntaxError, $"View '{simple.TableName}' does not contain a query definition.");

                IOperator viewOp;
                TableSchema viewSchema;

                if (viewQuery is SelectStatement viewStmt && !ContainsSubqueries(viewStmt))
                {
                    (viewOp, viewSchema) = BuildFromOperator(viewStmt.From);

                    bool hasAggregates = viewStmt.GroupBy != null ||
                                         viewStmt.Having != null ||
                                         viewStmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));

                    // Aggregate optimization for simple view pipelines.
                    if (hasAggregates)
                    {
                        if (TryGetAggregateDecodeUpperBound(viewStmt, viewSchema, viewStmt.Where, out int maxColumnIndex))
                            TrySetDecodedColumnUpperBound(viewOp, maxColumnIndex);
                    }

                    if (viewStmt.Where != null)
                        viewOp = new FilterOperator(
                            viewOp,
                            GetOrCompileSpanExpression(viewStmt.Where, viewSchema),
                            TryCreateFilterBatchPlan(viewOp, viewStmt.Where, viewSchema));

                    if (hasAggregates)
                    {
                        var outputCols = BuildAggregateOutputSchema(viewStmt.Columns, viewSchema);
                        bool hasGroupBy = viewStmt.GroupBy is { Count: > 0 };
                        if (hasGroupBy)
                        {
                            viewOp = new HashAggregateOperator(
                                viewOp, viewStmt.Columns, viewStmt.GroupBy, viewStmt.Having, viewSchema, outputCols);
                        }
                        else
                        {
                            viewOp = new ScalarAggregateOperator(
                                viewOp, viewStmt.Columns, viewStmt.Having, viewSchema, outputCols);
                        }

                        viewSchema = new TableSchema
                        {
                            TableName = simple.TableName,
                            Columns = outputCols,
                        };
                    }
                    else if (!viewStmt.Columns.Any(c => c.IsStar))
                    {
                        var expressions = viewStmt.Columns.Select(c => c.Expression!).ToArray();
                        var outputCols = new ColumnDefinition[expressions.Length];
                        for (int i = 0; i < expressions.Length; i++)
                            outputCols[i] = InferColumnDef(expressions[i], viewStmt.Columns[i].Alias, viewSchema, i);
                        viewOp = new ProjectionOperator(
                            viewOp,
                            Array.Empty<int>(),
                            outputCols,
                            GetOrCompileSpanExpressions(expressions, viewSchema),
                            batchPlan: null,
                            useSpanEvaluators: true);

                        viewSchema = new TableSchema
                        {
                            TableName = simple.TableName,
                            Columns = outputCols,
                        };
                    }
                    else
                    {
                        viewSchema = new TableSchema
                        {
                            TableName = simple.TableName,
                            Columns = viewSchema.Columns,
                        };
                    }
                }
                else
                {
                    var materialized = ExecuteQueryAsync(viewQuery, CancellationToken.None).AsTask().GetAwaiter().GetResult();
                    try
                    {
                        var rows = materialized.ToListAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
                        var outputCols = materialized.Schema.ToArray();
                        viewOp = new MaterializedOperator(rows, outputCols);
                        viewSchema = new TableSchema
                        {
                            TableName = simple.TableName,
                            Columns = outputCols,
                        };
                    }
                    finally
                    {
                        materialized.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    }
                }

                // Create qualified mappings
                string alias = simple.Alias ?? simple.TableName;
                var qualified = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < viewSchema.Columns.Count; i++)
                    qualified[$"{alias}.{viewSchema.Columns[i].Name}"] = i;

                var qualifiedSchema = new TableSchema
                {
                    TableName = viewSchema.TableName,
                    Columns = viewSchema.Columns,
                    QualifiedMappings = qualified,
                };

                return (viewOp, qualifiedSchema);
            }

            // Normal table
            var schema = GetSchema(simple.TableName);
            var tree = _catalog.GetTableTree(simple.TableName, _pager);
            IOperator op = new TableScanOperator(
                tree,
                schema,
                GetReadSerializer(schema),
                TryGetCachedTreeRowCountCapacityHint(tree));

            // Create schema with qualified mappings for this table
            string tableAlias = simple.Alias ?? simple.TableName;
            var qualifiedMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < schema.Columns.Count; i++)
                qualifiedMappings[$"{tableAlias}.{schema.Columns[i].Name}"] = i;

            var qualifiedTableSchema = new TableSchema
            {
                TableName = schema.TableName,
                Columns = schema.Columns,
                QualifiedMappings = qualifiedMappings,
            };

            if (pushDownOuterLocalPredicates &&
                TryExtractLocalJoinLeafPredicate(outerWhere, simple, qualifiedTableSchema, out var localPredicate) &&
                localPredicate != null)
            {
                var indexedLocalOp = TryBuildIndexScan(simple.TableName, localPredicate, qualifiedTableSchema, out var localResidualPredicate);
                if (indexedLocalOp != null)
                    op = indexedLocalOp;

                if (localResidualPredicate != null)
                    TryPushDownSimplePreDecodeFilter(op, localResidualPredicate, qualifiedTableSchema, out _);
            }

            return (op, qualifiedTableSchema);
        }

        if (tableRef is JoinTableRef join)
        {
            if (TryReorderInnerJoinChain(join, outerWhere, out var reordered))
                return BuildFromOperator(reordered, outerWhere, pushDownOuterLocalPredicates);

            var (leftOp, leftSchema) = BuildFromOperator(join.Left, outerWhere, pushDownOuterLocalPredicates);
            var (rightOp, rightSchema) = BuildFromOperator(join.Right, outerWhere, pushDownOuterLocalPredicates);

            // Build composite schema that inherits all qualified mappings
            var compositeSchema = TableSchema.CreateJoinSchema(leftSchema, rightSchema);

            // Rewrite RIGHT OUTER JOIN to swapped LEFT OUTER JOIN so it can reuse
            // hash/index join paths that are implemented for left-driven probing.
            if (join.JoinType == JoinType.RightOuter)
            {
                var swappedCompositeSchema = TableSchema.CreateJoinSchema(rightSchema, leftSchema);
                var rewrittenJoin = new JoinTableRef
                {
                    Left = join.Right,
                    Right = join.Left,
                    JoinType = JoinType.LeftOuter,
                    Condition = join.Condition,
                };

                IOperator swappedJoinOp;
                if (TryBuildIndexNestedLoopJoinOperator(
                    rewrittenJoin,
                    rightOp,
                    leftOp,
                    rightSchema,
                    leftSchema,
                    swappedCompositeSchema,
                    out var swappedIndexNestedJoinOp))
                {
                    swappedJoinOp = swappedIndexNestedJoinOp!;
                }
                else if (TryBuildHashJoinOperator(
                    rewrittenJoin,
                    rightOp,
                    leftOp,
                    rightSchema,
                    leftSchema,
                    swappedCompositeSchema,
                    out var swappedHashJoinOp))
                {
                    swappedJoinOp = swappedHashJoinOp!;
                }
                else
                {
                    int? swappedEstimatedOutputRowCount = TryEstimateJoinOutputRowCount(rewrittenJoin, rightSchema, leftSchema);
                    int? swappedRightRowCapacityHint = TryEstimateTableRefRowCountCapacityHint(rewrittenJoin.Right);
                    swappedJoinOp = new NestedLoopJoinOperator(
                        rightOp,
                        leftOp,
                        JoinType.LeftOuter,
                        join.Condition,
                        swappedCompositeSchema,
                        rightSchema.Columns.Count,
                        leftSchema.Columns.Count,
                        swappedEstimatedOutputRowCount,
                        swappedRightRowCapacityHint);
                }

                // Swapped execution produces [original right | original left];
                // reorder to SQL-visible [original left | original right].
                var projectionMap = BuildSwappedJoinProjectionMap(
                    leftSchema.Columns.Count,
                    rightSchema.Columns.Count);
                var projected = new ProjectionOperator(
                    swappedJoinOp,
                    projectionMap,
                    compositeSchema.Columns.ToArray(),
                    compositeSchema);

                return (projected, compositeSchema);
            }

            if (TryBuildIndexNestedLoopJoinOperator(
                join,
                leftOp,
                rightOp,
                leftSchema,
                rightSchema,
                compositeSchema,
                out var indexNestedJoinOp))
            {
                return (indexNestedJoinOp!, compositeSchema);
            }

            if (TryBuildHashJoinOperator(
                join,
                leftOp,
                rightOp,
                leftSchema,
                rightSchema,
                compositeSchema,
                out var hashJoinOp))
            {
                return (hashJoinOp!, compositeSchema);
            }

            int? estimatedOutputRowCount = TryEstimateJoinOutputRowCount(join, leftSchema, rightSchema);
            int? rightRowCapacityHint = TryEstimateTableRefRowCountCapacityHint(join.Right);
            var joinOp = new NestedLoopJoinOperator(
                leftOp, rightOp, join.JoinType, join.Condition,
                compositeSchema, leftSchema.Columns.Count, rightSchema.Columns.Count,
                estimatedOutputRowCount, rightRowCapacityHint);

            return (joinOp, compositeSchema);
        }

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown table ref type: {tableRef.GetType().Name}");
    }

    private bool TryReorderInnerJoinChain(JoinTableRef join, Expression? outerWhere, out TableRef reordered)
    {
        reordered = join;

        var leaves = new List<ReorderableJoinLeaf>();
        var predicates = new List<ReorderableJoinPredicate>();
        int leafIndex = 0;
        int predicateIndex = 0;
        if (!TryCollectReorderableInnerJoinChain(join, leaves, predicates, ref leafIndex, ref predicateIndex))
            return false;

        if (leaves.Count < 3)
            return false;

        ApplyLocalPredicateRowEstimates(leaves, predicates, outerWhere);

        var originalOrder = leaves.OrderBy(static l => l.OriginalIndex).Select(static l => l.Identifier).ToArray();
        if (!TryChooseGreedyInnerJoinOrder(leaves, predicates, out var orderedLeaves))
            return false;

        var reorderedOrder = orderedLeaves.Select(static l => l.Identifier).ToArray();
        if (originalOrder.SequenceEqual(reorderedOrder, StringComparer.OrdinalIgnoreCase))
            return false;

        var orderedPredicates = predicates
            .OrderBy(static p => p.OriginalIndex)
            .ToList();
        var attachedPredicateIndexes = new HashSet<int>();

        var selectedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            orderedLeaves[0].Identifier
        };

        TableRef current = orderedLeaves[0].TableRef;
        for (int i = 1; i < orderedLeaves.Count; i++)
        {
            var candidate = orderedLeaves[i];
            var nextSelectedIds = new HashSet<string>(selectedIds, StringComparer.OrdinalIgnoreCase)
            {
                candidate.Identifier
            };

            var attachPredicates = orderedPredicates
                .Where(p =>
                    !attachedPredicateIndexes.Contains(p.OriginalIndex) &&
                    ShouldAttachInnerJoinPredicate(p, selectedIds, nextSelectedIds))
                .OrderBy(static p => p.OriginalIndex)
                .ToList();

            if (attachPredicates.Count == 0)
                return false;

            current = new JoinTableRef
            {
                Left = current,
                Right = candidate.TableRef,
                JoinType = JoinType.Inner,
                Condition = CombineConjuncts(attachPredicates.Select(static p => p.Expression).ToList()),
            };

            foreach (var predicate in attachPredicates)
                attachedPredicateIndexes.Add(predicate.OriginalIndex);

            selectedIds = nextSelectedIds;
        }

        if (orderedPredicates.Any(p => !attachedPredicateIndexes.Contains(p.OriginalIndex)))
            return false;

        reordered = current;
        return true;
    }

    private static bool TryExtractLocalJoinLeafPredicate(
        Expression? outerWhere,
        SimpleTableRef simple,
        TableSchema qualifiedSchema,
        out Expression? localPredicate)
    {
        localPredicate = null;

        if (outerWhere == null)
            return false;

        string identifier = simple.Alias ?? simple.TableName;
        string[] referenceNames = simple.Alias is { Length: > 0 }
            ? [simple.Alias, simple.TableName]
            : [simple.TableName];

        var leaves = new[]
        {
            new ReorderableJoinLeaf(simple, qualifiedSchema, 1, 0, identifier, referenceNames)
        };

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(outerWhere, conjuncts);

        var localConjuncts = new List<Expression>();
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (!TryResolveReferencedJoinTables(conjuncts[i], leaves, out var referencedTables) ||
                referencedTables.Count != 1 ||
                !referencedTables.Contains(identifier))
            {
                continue;
            }

            localConjuncts.Add(conjuncts[i]);
        }

        if (localConjuncts.Count == 0)
            return false;

        localPredicate = CombineConjuncts(localConjuncts);
        return localPredicate != null;
    }

    private void ApplyLocalPredicateRowEstimates(
        List<ReorderableJoinLeaf> leaves,
        IReadOnlyList<ReorderableJoinPredicate> predicates,
        Expression? outerWhere)
    {
        for (int i = 0; i < leaves.Count; i++)
        {
            var leaf = leaves[i];
            var localPredicates = predicates
                .Where(p => p.ReferencedTables.Count == 1 && p.ReferencedTables.Contains(leaf.Identifier))
                .OrderBy(static p => p.OriginalIndex)
                .Select(static p => p.Expression)
                .ToList();

            if (outerWhere != null)
            {
                var outerConjuncts = new List<Expression>();
                CollectAndConjuncts(outerWhere, outerConjuncts);
                for (int conjunctIndex = 0; conjunctIndex < outerConjuncts.Count; conjunctIndex++)
                {
                    if (!TryResolveReferencedJoinTables(outerConjuncts[conjunctIndex], leaves, out var referencedTables) ||
                        referencedTables.Count != 1 ||
                        !referencedTables.Contains(leaf.Identifier))
                    {
                        continue;
                    }

                    localPredicates.Add(outerConjuncts[conjunctIndex]);
                }
            }

            if (localPredicates.Count == 0)
                continue;

            if (!CardinalityEstimator.TryEstimateFilteredRowCount(
                    _catalog,
                    leaf.Schema,
                    leaf.RowCount,
                    localPredicates,
                    out long estimatedRows))
            {
                continue;
            }

            leaves[i] = leaf with { RowCount = estimatedRows };
        }
    }

    private bool TryCollectReorderableInnerJoinChain(
        TableRef tableRef,
        List<ReorderableJoinLeaf> leaves,
        List<ReorderableJoinPredicate> predicates,
        ref int leafIndex,
        ref int predicateIndex)
    {
        if (tableRef is SimpleTableRef simple)
        {
            if (!TryCreateReorderableJoinLeaf(simple, leafIndex++, out var leaf))
                return false;

            leaves.Add(leaf);
            return true;
        }

        if (tableRef is not JoinTableRef join ||
            join.JoinType != JoinType.Inner ||
            join.Condition == null)
        {
            return false;
        }

        if (!TryCollectReorderableInnerJoinChain(join.Left, leaves, predicates, ref leafIndex, ref predicateIndex) ||
            !TryCollectReorderableInnerJoinChain(join.Right, leaves, predicates, ref leafIndex, ref predicateIndex))
        {
            return false;
        }

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(join.Condition, conjuncts);
        foreach (var conjunct in conjuncts)
        {
            if (!TryResolveReferencedJoinTables(conjunct, leaves, out var referencedTables) ||
                referencedTables.Count == 0)
            {
                return false;
            }

            predicates.Add(new ReorderableJoinPredicate
            {
                Expression = conjunct,
                ReferencedTables = referencedTables,
                OriginalIndex = predicateIndex++,
            });
        }

        return true;
    }

    private bool TryCreateReorderableJoinLeaf(SimpleTableRef simple, int originalIndex, out ReorderableJoinLeaf leaf)
    {
        leaf = default;

        if (_cteData != null && _cteData.ContainsKey(simple.TableName))
            return false;

        if (IsSystemCatalogTable(simple.TableName) || _catalog.GetViewSql(simple.TableName) != null)
            return false;

        var schema = GetSchema(simple.TableName);
        if (!TryEstimateTableRefRowCount(simple, out long rowCount) || rowCount <= 0)
            return false;

        string identifier = simple.Alias ?? simple.TableName;
        string[] referenceNames = simple.Alias is { Length: > 0 }
            ? [simple.Alias, simple.TableName]
            : [simple.TableName];

        var qualifiedMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (string referenceName in referenceNames)
        {
            for (int i = 0; i < schema.Columns.Count; i++)
                qualifiedMappings[$"{referenceName}.{schema.Columns[i].Name}"] = i;
        }

        var qualifiedSchema = new TableSchema
        {
            TableName = schema.TableName,
            Columns = schema.Columns,
            QualifiedMappings = qualifiedMappings,
        };

        leaf = new ReorderableJoinLeaf(simple, qualifiedSchema, rowCount, originalIndex, identifier, referenceNames);
        return true;
    }

    private bool TryChooseGreedyInnerJoinOrder(
        List<ReorderableJoinLeaf> leaves,
        List<ReorderableJoinPredicate> predicates,
        out List<ReorderableJoinLeaf> orderedLeaves)
    {
        orderedLeaves = new List<ReorderableJoinLeaf>(leaves.Count);

        var remaining = leaves
            .OrderBy(static l => l.OriginalIndex)
            .ToList();

        var start = remaining
            .OrderBy(static l => l.RowCount)
            .ThenBy(static l => l.OriginalIndex)
            .First();

        orderedLeaves.Add(start);
        remaining.Remove(start);

        long currentEstimate = start.RowCount;
        var selectedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            start.Identifier
        };

        while (remaining.Count > 0)
        {
            ReorderableJoinLeaf? bestCandidate = null;
            long bestEstimate = long.MaxValue;
            long bestRowCount = long.MaxValue;

            for (int i = 0; i < remaining.Count; i++)
            {
                var candidate = remaining[i];
                if (!TryEstimateNextJoinStep(leaves, predicates, orderedLeaves, selectedIds, currentEstimate, candidate, out long stepEstimate))
                    continue;

                if (stepEstimate < bestEstimate ||
                    (stepEstimate == bestEstimate && candidate.RowCount < bestRowCount) ||
                    (stepEstimate == bestEstimate && candidate.RowCount == bestRowCount && candidate.OriginalIndex < bestCandidate?.OriginalIndex))
                {
                    bestCandidate = candidate;
                    bestEstimate = stepEstimate;
                    bestRowCount = candidate.RowCount;
                }
            }

            if (bestCandidate is not ReorderableJoinLeaf chosen)
                return false;

            orderedLeaves.Add(chosen);
            remaining.Remove(chosen);
            selectedIds.Add(chosen.Identifier);
            currentEstimate = bestEstimate;
        }

        return true;
    }

    private bool TryEstimateNextJoinStep(
        IReadOnlyList<ReorderableJoinLeaf> allLeaves,
        IReadOnlyList<ReorderableJoinPredicate> predicates,
        IReadOnlyList<ReorderableJoinLeaf> selectedLeaves,
        HashSet<string> selectedIds,
        long currentEstimate,
        ReorderableJoinLeaf candidate,
        out long estimatedRows)
    {
        estimatedRows = 0;

        var nextSelectedIds = new HashSet<string>(selectedIds, StringComparer.OrdinalIgnoreCase)
        {
            candidate.Identifier
        };

        var attachablePredicates = predicates
            .Where(p => ShouldAttachInnerJoinPredicate(p, selectedIds, nextSelectedIds))
            .ToList();

        if (attachablePredicates.Count == 0)
            return false;

        long bestStepEstimate = long.MaxValue;
        foreach (var selectedLeaf in selectedLeaves)
        {
            var pairPredicates = attachablePredicates
                .Where(p =>
                    p.ReferencedTables.Count == 2 &&
                    p.ReferencedTables.Contains(selectedLeaf.Identifier) &&
                    p.ReferencedTables.Contains(candidate.Identifier))
                .OrderBy(static p => p.OriginalIndex)
                .ToList();

            if (pairPredicates.Count == 0)
                continue;

            if (!TryEstimatePairwiseJoinRows(selectedLeaf, candidate, pairPredicates, out long pairwiseRows))
                continue;

            long scaledEstimate = Math.Max(
                1,
                DivideRoundUp(
                    SafeMultiply(currentEstimate, Math.Max(pairwiseRows, 1)),
                    Math.Max(selectedLeaf.RowCount, 1)));

            if (scaledEstimate < bestStepEstimate)
                bestStepEstimate = scaledEstimate;
        }

        if (bestStepEstimate != long.MaxValue)
        {
            estimatedRows = bestStepEstimate;
            return true;
        }

        estimatedRows = CardinalityEstimator.EstimateFallbackJoinRowCount(
            JoinType.Inner,
            hasLeftEstimate: true,
            currentEstimate,
            hasRightEstimate: true,
            candidate.RowCount);
        return true;
    }

    private bool TryEstimatePairwiseJoinRows(
        ReorderableJoinLeaf left,
        ReorderableJoinLeaf right,
        IReadOnlyList<ReorderableJoinPredicate> predicates,
        out long estimatedRows)
    {
        estimatedRows = 0;

        var condition = CombineConjuncts(predicates.Select(static p => p.Expression).ToList());
        if (condition == null)
            return false;

        var compositeSchema = TableSchema.CreateJoinSchema(left.Schema, right.Schema);
        if (!TryAnalyzeHashJoinCondition(
                condition,
                compositeSchema,
                left.Schema.Columns.Count,
                out var leftKeyIndices,
                out var rightKeyIndices,
                out _))
        {
            return false;
        }

        if (leftKeyIndices.Length == 0 || leftKeyIndices.Length != rightKeyIndices.Length)
            return false;

        return CardinalityEstimator.TryEstimateEqualityJoinRowCount(
            _catalog,
            JoinType.Inner,
            left.Schema,
            right.Schema,
            leftKeyIndices,
            rightKeyIndices,
            left.RowCount,
            right.RowCount,
            out estimatedRows);
    }

    private static bool ShouldAttachInnerJoinPredicate(
        ReorderableJoinPredicate predicate,
        HashSet<string> selectedIds,
        HashSet<string> nextSelectedIds)
    {
        if (!predicate.ReferencedTables.IsSubsetOf(nextSelectedIds))
            return false;

        return predicate.ReferencedTables.Count == 1 ||
               !predicate.ReferencedTables.IsSubsetOf(selectedIds);
    }

    private static bool TryResolveReferencedJoinTables(
        Expression expression,
        IReadOnlyList<ReorderableJoinLeaf> leaves,
        out HashSet<string> referencedTables)
    {
        referencedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        return TryCollectReferencedJoinTables(expression, leaves, referencedTables);
    }

    private static bool TryCollectReferencedJoinTables(
        Expression expression,
        IReadOnlyList<ReorderableJoinLeaf> leaves,
        HashSet<string> referencedTables)
    {
        switch (expression)
        {
            case ColumnRefExpression columnRef:
                if (!TryResolveColumnReferenceTable(columnRef, leaves, out string identifier))
                    return false;

                referencedTables.Add(identifier);
                return true;

            case BinaryExpression binary:
                return TryCollectReferencedJoinTables(binary.Left, leaves, referencedTables) &&
                       TryCollectReferencedJoinTables(binary.Right, leaves, referencedTables);

            case UnaryExpression unary:
                return TryCollectReferencedJoinTables(unary.Operand, leaves, referencedTables);
            case CollateExpression collate:
                return TryCollectReferencedJoinTables(collate.Operand, leaves, referencedTables);

            case LikeExpression like:
                return TryCollectReferencedJoinTables(like.Operand, leaves, referencedTables) &&
                       TryCollectReferencedJoinTables(like.Pattern, leaves, referencedTables) &&
                       (like.EscapeChar == null || TryCollectReferencedJoinTables(like.EscapeChar, leaves, referencedTables));

            case InExpression inExpr:
                if (!TryCollectReferencedJoinTables(inExpr.Operand, leaves, referencedTables))
                    return false;
                for (int i = 0; i < inExpr.Values.Count; i++)
                {
                    if (!TryCollectReferencedJoinTables(inExpr.Values[i], leaves, referencedTables))
                        return false;
                }
                return true;

            case BetweenExpression between:
                return TryCollectReferencedJoinTables(between.Operand, leaves, referencedTables) &&
                       TryCollectReferencedJoinTables(between.Low, leaves, referencedTables) &&
                       TryCollectReferencedJoinTables(between.High, leaves, referencedTables);

            case IsNullExpression isNull:
                return TryCollectReferencedJoinTables(isNull.Operand, leaves, referencedTables);

            case FunctionCallExpression functionCall:
                for (int i = 0; i < functionCall.Arguments.Count; i++)
                {
                    if (!TryCollectReferencedJoinTables(functionCall.Arguments[i], leaves, referencedTables))
                        return false;
                }
                return true;

            case LiteralExpression:
            case ParameterExpression:
                return true;

            default:
                return false;
        }
    }

    private static bool TryResolveColumnReferenceTable(
        ColumnRefExpression columnRef,
        IReadOnlyList<ReorderableJoinLeaf> leaves,
        out string identifier)
    {
        identifier = string.Empty;

        if (!string.IsNullOrEmpty(columnRef.TableAlias))
        {
            for (int i = 0; i < leaves.Count; i++)
            {
                if (leaves[i].ReferenceNames.Any(name => string.Equals(name, columnRef.TableAlias, StringComparison.OrdinalIgnoreCase)))
                {
                    identifier = leaves[i].Identifier;
                    return true;
                }
            }

            return false;
        }

        ReorderableJoinLeaf? match = null;
        for (int i = 0; i < leaves.Count; i++)
        {
            if (!leaves[i].Schema.Columns.Any(c => string.Equals(c.Name, columnRef.ColumnName, StringComparison.OrdinalIgnoreCase)))
                continue;

            if (match.HasValue)
                return false;

            match = leaves[i];
        }

        if (match.HasValue)
        {
            identifier = match.Value.Identifier;
            return true;
        }

        return false;
    }

    private static int[] BuildSwappedJoinProjectionMap(int leftColumnCount, int rightColumnCount)
    {
        int total = leftColumnCount + rightColumnCount;
        var projectionMap = new int[total];

        // Original left columns start after original right columns in swapped rows.
        for (int i = 0; i < leftColumnCount; i++)
            projectionMap[i] = rightColumnCount + i;

        // Original right columns are at the start of swapped rows.
        for (int i = 0; i < rightColumnCount; i++)
            projectionMap[leftColumnCount + i] = i;

        return projectionMap;
    }

    private bool TryBuildIndexNestedLoopJoinOperator(
        JoinTableRef join,
        IOperator leftOp,
        IOperator rightOp,
        TableSchema leftSchema,
        TableSchema rightSchema,
        TableSchema compositeSchema,
        out IOperator? indexNestedJoinOp)
    {
        indexNestedJoinOp = null;

        if (join.JoinType is not (JoinType.Inner or JoinType.LeftOuter))
            return false;

        if (join.Right is not SimpleTableRef rightSimple)
            return false;

        if (_catalog.IsView(rightSimple.TableName))
            return false;
        if (IsSystemCatalogTable(rightSimple.TableName))
            return false;

        if (_cteData != null && _cteData.ContainsKey(rightSimple.TableName))
            return false;

        if (join.Condition == null)
            return false;

        if (!TryAnalyzeHashJoinCondition(
                join.Condition,
                compositeSchema,
                leftSchema.Columns.Count,
                out var leftKeyIndices,
                out var rightKeyIndices,
                out var residualCondition))
        {
            return false;
        }

        if (leftKeyIndices.Length == 0 || leftKeyIndices.Length != rightKeyIndices.Length)
            return false;

        var rightTableTree = _catalog.GetTableTree(rightSimple.TableName, _pager);
        IIndexStore? rightIndexStore = null;
        bool lookupIsUnique = false;
        bool lookupIsPrimaryKey = false;
        bool usesDirectIntegerLookup = false;
        bool usesOrderedTextLookupPayload = false;
        int[]? orderedLeftKeyIndices = null;
        int[]? orderedRightKeyIndices = null;
        string?[]? orderedRightKeyCollations = null;

        int rightPkIndex = rightSchema.PrimaryKeyColumnIndex;
        bool usesPrimaryKeyLookup =
            rightKeyIndices.Length == 1 &&
            rightPkIndex == rightKeyIndices[0] &&
            rightSchema.Columns[rightPkIndex].Type == DbType.Integer;

        if (usesPrimaryKeyLookup)
        {
            lookupIsUnique = true;
            lookupIsPrimaryKey = true;
            usesDirectIntegerLookup = true;
            orderedLeftKeyIndices = [leftKeyIndices[0]];
            orderedRightKeyIndices = [rightKeyIndices[0]];
        }
        else
        {
            for (int i = 0; i < rightKeyIndices.Length; i++)
            {
                int rightKeyIndex = rightKeyIndices[i];
                if (rightKeyIndex < 0 || rightKeyIndex >= rightSchema.Columns.Count)
                    return false;

                var rightKeyColumn = rightSchema.Columns[rightKeyIndex];
                if (rightKeyColumn.Type is not (DbType.Integer or DbType.Text))
                    return false;
            }

            var indexes = _catalog.GetSqlIndexesForTable(rightSimple.TableName);
            IndexSchema? selected = null;
            int[]? selectedLeftKeyIndices = null;
            int[]? selectedRightKeyIndices = null;
            bool selectedUsesDirectIntegerKey = false;
            for (int i = 0; i < indexes.Count; i++)
            {
                var idx = indexes[i];
                if (!TryMatchJoinLookupIndex(
                        idx,
                        leftSchema,
                        rightSchema,
                        leftKeyIndices,
                        rightKeyIndices,
                        out var candidateLeftKeyIndices,
                        out var candidateRightKeyIndices,
                        out var candidateRightKeyCollations))
                {
                    continue;
                }

                bool candidateUsesDirectIntegerKey =
                    candidateRightKeyIndices.Length == 1 &&
                    rightSchema.Columns[candidateRightKeyIndices[0]].Type == DbType.Integer;
                bool candidateUsesOrderedTextPayload =
                    !candidateUsesDirectIntegerKey &&
                    IndexMaintenanceHelper.UsesOrderedTextIndexKey(idx, rightSchema);

                if (selected == null ||
                    (idx.IsUnique && !selected.IsUnique) ||
                    (candidateUsesDirectIntegerKey && !selectedUsesDirectIntegerKey && idx.IsUnique == selected.IsUnique))
                {
                    selected = idx;
                    selectedLeftKeyIndices = candidateLeftKeyIndices;
                    selectedRightKeyIndices = candidateRightKeyIndices;
                    orderedRightKeyCollations = candidateRightKeyCollations;
                    selectedUsesDirectIntegerKey = candidateUsesDirectIntegerKey;
                    usesOrderedTextLookupPayload = candidateUsesOrderedTextPayload;
                }
            }

            if (selected == null || selectedLeftKeyIndices == null || selectedRightKeyIndices == null)
                return false;

            rightIndexStore = _catalog.GetIndexStore(selected.IndexName, _pager);
            lookupIsUnique = selected.IsUnique;
            usesDirectIntegerLookup = selectedUsesDirectIntegerKey;
            orderedLeftKeyIndices = selectedLeftKeyIndices;
            orderedRightKeyIndices = selectedRightKeyIndices;
        }

        bool hasOuterEstimate = TryEstimateJoinInputRowCount(leftOp, join.Left, out long outerRows);
        bool hasInnerEstimate = TryEstimateJoinInputRowCount(rightOp, join.Right, out long innerRows);
        bool hasEstimatedOutputRows = TryEstimateJoinOutputRows(
            join,
            leftSchema,
            rightSchema,
            orderedLeftKeyIndices,
            orderedRightKeyIndices,
            hasOuterEstimate,
            outerRows,
            hasInnerEstimate,
            innerRows,
            out long estimatedOutputRows);

        if (hasOuterEstimate && hasInnerEstimate)
        {
            bool shouldUseLookup = lookupIsPrimaryKey || lookupIsUnique
                ? ShouldPreferIndexNestedLoop(outerRows, innerRows, lookupIsPrimaryKey, lookupIsUnique)
                : hasEstimatedOutputRows
                    ? ShouldPreferNonUniqueIndexNestedLoop(outerRows, innerRows, estimatedOutputRows, usesDirectIntegerLookup)
                    : ShouldPreferIndexNestedLoop(outerRows, innerRows, lookupIsPrimaryKey, lookupIsUnique);

            if (!shouldUseLookup)
                return false;
        }
        else if (!lookupIsUnique)
        {
            // Without any cardinality hint, keep non-unique lookups on hash join.
            return false;
        }

        int? estimatedOutputRowCount = lookupIsUnique
            ? ToCapacityHint(outerRows)
            : hasEstimatedOutputRows
                ? ToCapacityHint(estimatedOutputRows)
                : EstimateJoinOutputRowCount(join.JoinType, hasOuterEstimate, outerRows, hasInnerEstimate, innerRows);

        if (orderedLeftKeyIndices == null || orderedRightKeyIndices == null)
            return false;

        if (usesDirectIntegerLookup)
        {
            indexNestedJoinOp = new IndexNestedLoopJoinOperator(
                leftOp,
                rightTableTree,
                rightIndexStore,
                join.JoinType,
                orderedLeftKeyIndices[0],
                leftSchema.Columns.Count,
                rightSchema.Columns.Count,
                residualCondition,
                compositeSchema,
                GetReadSerializer(rightSchema),
                estimatedOutputRowCount);
        }
        else
        {
            indexNestedJoinOp = new HashedIndexNestedLoopJoinOperator(
                leftOp,
                rightTableTree,
                rightIndexStore!,
                join.JoinType,
                orderedLeftKeyIndices,
                orderedRightKeyIndices,
                orderedRightKeyCollations ?? Array.Empty<string?>(),
                leftSchema.Columns.Count,
                rightSchema.Columns.Count,
                rightSchema.PrimaryKeyColumnIndex,
                residualCondition,
                compositeSchema,
                usesOrderedTextPayload: usesOrderedTextLookupPayload,
                recordSerializer: GetReadSerializer(rightSchema),
                estimatedOutputRowCount: estimatedOutputRowCount);
        }

        return true;
    }

    private bool TryEstimateJoinInputRowCount(IOperator op, TableRef tableRef, out long count)
    {
        count = 0;

        if (op is IEstimatedRowCountProvider estimated &&
            estimated.EstimatedRowCount is int estimatedRowCount &&
            estimatedRowCount >= 0)
        {
            count = estimatedRowCount;
            return true;
        }

        return TryEstimateTableRefRowCount(tableRef, out count);
    }

    private static bool TryMatchJoinLookupIndex(
        IndexSchema indexSchema,
        TableSchema leftSchema,
        TableSchema rightSchema,
        ReadOnlySpan<int> leftKeyIndices,
        ReadOnlySpan<int> rightKeyIndices,
        out int[] orderedLeftKeyIndices,
        out int[] orderedRightKeyIndices,
        out string?[] orderedRightKeyCollations)
    {
        orderedLeftKeyIndices = Array.Empty<int>();
        orderedRightKeyIndices = Array.Empty<int>();
        orderedRightKeyCollations = Array.Empty<string?>();

        if (indexSchema.Columns.Count != rightKeyIndices.Length)
            return false;

        var consumed = new bool[rightKeyIndices.Length];
        orderedLeftKeyIndices = new int[indexSchema.Columns.Count];
        orderedRightKeyIndices = new int[indexSchema.Columns.Count];
        orderedRightKeyCollations = new string?[indexSchema.Columns.Count];

        for (int indexColumn = 0; indexColumn < indexSchema.Columns.Count; indexColumn++)
        {
            string indexColumnName = indexSchema.Columns[indexColumn];
            int match = -1;
            for (int candidate = 0; candidate < rightKeyIndices.Length; candidate++)
            {
                if (consumed[candidate])
                    continue;

                int rightKeyIndex = rightKeyIndices[candidate];
                if (rightKeyIndex < 0 || rightKeyIndex >= rightSchema.Columns.Count)
                    return false;
                int leftKeyIndex = leftKeyIndices[candidate];
                if (leftKeyIndex < 0 || leftKeyIndex >= leftSchema.Columns.Count)
                    return false;

                if (!string.Equals(
                        rightSchema.Columns[rightKeyIndex].Name,
                        indexColumnName,
                        StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                DbType rightType = rightSchema.Columns[rightKeyIndex].Type;
                DbType leftType = leftSchema.Columns[leftKeyIndex].Type;
                if (leftType != rightType)
                    return false;
                if (rightType is not (DbType.Integer or DbType.Text))
                    return false;

                string? effectiveIndexCollation = rightType == DbType.Text
                    ? CollationSupport.GetEffectiveIndexColumnCollation(indexSchema, rightSchema, indexColumn, rightKeyIndex)
                    : null;
                string? leftCollation = leftType == DbType.Text
                    ? CollationSupport.NormalizeMetadataName(leftSchema.Columns[leftKeyIndex].Collation)
                    : null;
                if (!CollationSupport.SemanticallyEquals(leftCollation, effectiveIndexCollation))
                    return false;

                match = candidate;
                break;
            }

            if (match < 0)
                return false;

            consumed[match] = true;
            orderedLeftKeyIndices[indexColumn] = leftKeyIndices[match];
            orderedRightKeyIndices[indexColumn] = rightKeyIndices[match];
            orderedRightKeyCollations[indexColumn] = rightSchema.Columns[rightKeyIndices[match]].Type == DbType.Text
                ? CollationSupport.GetEffectiveIndexColumnCollation(indexSchema, rightSchema, indexColumn, rightKeyIndices[match])
                : null;
        }

        return true;
    }

    private bool TryEstimateTableRefRowCount(TableRef tableRef, out long count)
    {
        count = 0;

        if (tableRef is SimpleTableRef simple)
        {
            if (_catalog.IsView(simple.TableName))
                return false;
            if (IsSystemCatalogTable(simple.TableName))
                return false;
            if (_cteData != null && _cteData.ContainsKey(simple.TableName))
                return false;

            if (TryGetTableRowCount(simple.TableName, out count))
                return true;

            try
            {
                var tree = _catalog.GetTableTree(simple.TableName, _pager);
                count = tree.CountEntriesAsync().AsTask().GetAwaiter().GetResult();
                return true;
            }
            catch
            {
                return false;
            }
        }

        if (tableRef is not JoinTableRef join)
            return false;

        if (!TryEstimateTableRefRowCount(join.Left, out long leftCount) ||
            !TryEstimateTableRefRowCount(join.Right, out long rightCount))
        {
            return false;
        }

        count = join.JoinType switch
        {
            JoinType.Cross => SafeMultiply(leftCount, rightCount),
            JoinType.Inner => Math.Min(leftCount, rightCount),
            JoinType.LeftOuter => leftCount,
            JoinType.RightOuter => rightCount,
            _ => Math.Max(leftCount, rightCount),
        };

        return true;
    }

    private static bool ShouldPreferIndexNestedLoop(
        long outerRows,
        long innerRows,
        bool lookupIsPrimaryKey,
        bool lookupIsUnique)
    {
        outerRows = Math.Max(outerRows, 1);
        innerRows = Math.Max(innerRows, 1);

        if (lookupIsPrimaryKey)
            return outerRows <= SafeMultiply(innerRows, 8);

        if (lookupIsUnique)
            return outerRows <= SafeMultiply(innerRows, 2);

        // Non-unique index lookups can fan out to many rowids.
        return outerRows <= Math.Max(1, innerRows / 4);
    }

    private static bool ShouldPreferNonUniqueIndexNestedLoop(
        long outerRows,
        long innerRows,
        long estimatedOutputRows,
        bool usesDirectIntegerLookup)
    {
        outerRows = Math.Max(outerRows, 1);
        innerRows = Math.Max(innerRows, 1);
        estimatedOutputRows = Math.Clamp(estimatedOutputRows, 1, SafeMultiply(outerRows, innerRows));

        long hashJoinCost = SafeMultiply(innerRows, 1) + outerRows;
        long lookupProbeCost = SafeMultiply(outerRows, usesDirectIntegerLookup ? 1 : 2);
        long lookupJoinCost = lookupProbeCost + estimatedOutputRows;
        return lookupJoinCost <= hashJoinCost;
    }

    private static long SafeMultiply(long a, long b)
    {
        if (a <= 0 || b <= 0) return 0;
        if (a > long.MaxValue / b) return long.MaxValue;
        return a * b;
    }

    private bool TryBuildHashJoinOperator(
        JoinTableRef join,
        IOperator leftOp,
        IOperator rightOp,
        TableSchema leftSchema,
        TableSchema rightSchema,
        TableSchema compositeSchema,
        out IOperator? hashJoinOp)
    {
        hashJoinOp = null;

        if (join.JoinType is JoinType.Cross || join.Condition == null)
            return false;

        if (!TryAnalyzeHashJoinCondition(
                join.Condition,
                compositeSchema,
                leftSchema.Columns.Count,
                out var leftKeyIndices,
                out var rightKeyIndices,
                out var residualCondition))
        {
            return false;
        }

        bool hasLeftEstimate = TryEstimateJoinInputRowCount(leftOp, join.Left, out long leftRows);
        bool hasRightEstimate = TryEstimateJoinInputRowCount(rightOp, join.Right, out long rightRows);

        // Build the smaller estimated side for INNER hash joins to reduce hash table
        // memory and probe work.
        bool buildRightSide = true;
        if (join.JoinType == JoinType.Inner &&
            hasLeftEstimate &&
            hasRightEstimate &&
            !ShouldBuildHashRightSide(leftRows, rightRows))
        {
            buildRightSide = false;
        }

        int? buildRowCapacityHint = null;
        if (buildRightSide ? hasRightEstimate : hasLeftEstimate)
        {
            long buildRows = buildRightSide ? rightRows : leftRows;
            buildRowCapacityHint = ToCapacityHint(buildRows);
        }

        int? estimatedOutputRowCount = TryEstimateJoinOutputRowCount(
            join,
            leftSchema,
            rightSchema,
            leftKeyIndices,
            rightKeyIndices,
            hasLeftEstimate,
            leftRows,
            hasRightEstimate,
            rightRows);

        hashJoinOp = new HashJoinOperator(
            leftOp,
            rightOp,
            join.JoinType,
            residualCondition,
            compositeSchema,
            leftSchema.Columns.Count,
            rightSchema.Columns.Count,
            leftKeyIndices,
            rightKeyIndices,
            buildRightSide,
            buildRowCapacityHint,
            estimatedOutputRowCount);

        return true;
    }

    private int? TryEstimateTableRefRowCountCapacityHint(TableRef tableRef)
    {
        return TryEstimateTableRefRowCount(tableRef, out long count)
            ? ToCapacityHint(count)
            : null;
    }

    private static int? TryGetCachedTreeRowCountCapacityHint(BTree tree)
    {
        return tree.TryGetCachedEntryCount(out long count)
            ? ToCapacityHint(count)
            : null;
    }

    private bool TryEstimateJoinOutputRows(
        JoinTableRef join,
        TableSchema leftSchema,
        TableSchema rightSchema,
        ReadOnlySpan<int> leftKeyIndices,
        ReadOnlySpan<int> rightKeyIndices,
        bool hasLeftEstimate,
        long leftRows,
        bool hasRightEstimate,
        long rightRows,
        out long estimatedRows)
    {
        estimatedRows = 0;
        if (!hasLeftEstimate || !hasRightEstimate)
            return false;

        if (leftKeyIndices.Length == 0 &&
            rightKeyIndices.Length == 0 &&
            join.Condition != null)
        {
            var compositeSchema = TableSchema.CreateJoinSchema(leftSchema, rightSchema);
            if (TryAnalyzeHashJoinCondition(
                    join.Condition,
                    compositeSchema,
                    leftSchema.Columns.Count,
                    out var analyzedLeftKeyIndices,
                    out var analyzedRightKeyIndices,
                    out _))
            {
                leftKeyIndices = analyzedLeftKeyIndices;
                rightKeyIndices = analyzedRightKeyIndices;
            }
        }

        if (leftKeyIndices.Length == 0 || leftKeyIndices.Length != rightKeyIndices.Length)
            return false;

        return CardinalityEstimator.TryEstimateEqualityJoinRowCount(
            _catalog,
            join.JoinType,
            leftSchema,
            rightSchema,
            leftKeyIndices,
            rightKeyIndices,
            leftRows,
            rightRows,
            out estimatedRows);
    }

    private int? TryEstimateJoinOutputRowCount(
        JoinTableRef join,
        TableSchema leftSchema,
        TableSchema rightSchema,
        ReadOnlySpan<int> leftKeyIndices = default,
        ReadOnlySpan<int> rightKeyIndices = default,
        bool? hasLeftEstimateOverride = null,
        long leftRowsOverride = 0,
        bool? hasRightEstimateOverride = null,
        long rightRowsOverride = 0)
    {
        bool hasLeftEstimate = hasLeftEstimateOverride ?? TryEstimateTableRefRowCount(join.Left, out leftRowsOverride);
        long leftRows = leftRowsOverride;
        bool hasRightEstimate = hasRightEstimateOverride ?? TryEstimateTableRefRowCount(join.Right, out rightRowsOverride);
        long rightRows = rightRowsOverride;

        if (TryEstimateJoinOutputRows(
                join,
                leftSchema,
                rightSchema,
                leftKeyIndices,
                rightKeyIndices,
                hasLeftEstimate,
                leftRows,
                hasRightEstimate,
                rightRows,
                out long estimatedRows))
        {
            return ToCapacityHint(estimatedRows);
        }

        return EstimateJoinOutputRowCount(
            join.JoinType,
            hasLeftEstimate,
            leftRows,
            hasRightEstimate,
            rightRows);
    }

    private static int? EstimateJoinOutputRowCount(
        JoinType joinType,
        bool hasLeftEstimate,
        long leftRows,
        bool hasRightEstimate,
        long rightRows)
    {
        long estimate = CardinalityEstimator.EstimateFallbackJoinRowCount(
            joinType,
            hasLeftEstimate,
            leftRows,
            hasRightEstimate,
            rightRows);
        if (estimate <= 0)
            return null;
        return ToCapacityHint(estimate);
    }

    private static int? ToCapacityHint(long count)
    {
        if (count <= 0)
            return null;

        // Guard against large one-shot preallocations while still reducing growth churn.
        const int maxCapacityHint = 1_000_000;
        return (int)Math.Min(count, maxCapacityHint);
    }

    private static bool ShouldBuildHashRightSide(long leftRows, long rightRows)
    {
        leftRows = Math.Max(leftRows, 1);
        rightRows = Math.Max(rightRows, 1);
        return rightRows <= leftRows;
    }

    private static bool TryAnalyzeHashJoinCondition(
        Expression condition,
        TableSchema compositeSchema,
        int leftColumnCount,
        out int[] leftKeyIndices,
        out int[] rightKeyIndices,
        out Expression? residualCondition)
    {
        leftKeyIndices = Array.Empty<int>();
        rightKeyIndices = Array.Empty<int>();
        residualCondition = null;

        var keyPairs = new List<(int Left, int Right)>();
        var residualTerms = new List<Expression>();
        var conjuncts = new List<Expression>();
        CollectAndConjuncts(condition, conjuncts);

        foreach (var conjunct in conjuncts)
        {
            if (TryExtractHashJoinKeyPair(conjunct, compositeSchema, leftColumnCount, out int leftKey, out int rightKey))
            {
                keyPairs.Add((leftKey, rightKey));
                continue;
            }

            residualTerms.Add(conjunct);
        }

        if (keyPairs.Count == 0)
            return false;

        leftKeyIndices = new int[keyPairs.Count];
        rightKeyIndices = new int[keyPairs.Count];
        for (int i = 0; i < keyPairs.Count; i++)
        {
            leftKeyIndices[i] = keyPairs[i].Left;
            rightKeyIndices[i] = keyPairs[i].Right;
        }

        residualCondition = CombineConjuncts(residualTerms);
        return true;
    }

    private static void CollectAndConjuncts(Expression expression, List<Expression> output)
    {
        if (expression is BinaryExpression { Op: BinaryOp.And } andExpr)
        {
            CollectAndConjuncts(andExpr.Left, output);
            CollectAndConjuncts(andExpr.Right, output);
            return;
        }

        output.Add(expression);
    }

    private static bool TryExtractHashJoinKeyPair(
        Expression expression,
        TableSchema compositeSchema,
        int leftColumnCount,
        out int leftKeyIndex,
        out int rightKeyIndex)
    {
        leftKeyIndex = -1;
        rightKeyIndex = -1;

        if (expression is not BinaryExpression { Op: BinaryOp.Equals } equalsExpr)
            return false;

        if (equalsExpr.Left is not ColumnRefExpression leftColumn ||
            equalsExpr.Right is not ColumnRefExpression rightColumn)
        {
            return false;
        }

        int leftAbs = ResolveJoinColumnIndex(leftColumn, compositeSchema);
        int rightAbs = ResolveJoinColumnIndex(rightColumn, compositeSchema);
        if (leftAbs < 0 || rightAbs < 0)
            return false;

        bool leftIsLeftInput = leftAbs < leftColumnCount;
        bool rightIsLeftInput = rightAbs < leftColumnCount;

        if (leftIsLeftInput == rightIsLeftInput)
            return false;

        if (leftIsLeftInput)
        {
            leftKeyIndex = leftAbs;
            rightKeyIndex = rightAbs - leftColumnCount;
        }
        else
        {
            leftKeyIndex = rightAbs;
            rightKeyIndex = leftAbs - leftColumnCount;
        }

        return leftKeyIndex >= 0 && rightKeyIndex >= 0;
    }

    private static int ResolveJoinColumnIndex(ColumnRefExpression columnRef, TableSchema compositeSchema)
    {
        return columnRef.TableAlias != null
            ? compositeSchema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : compositeSchema.GetColumnIndex(columnRef.ColumnName);
    }

    private static Expression? CombineConjuncts(List<Expression> terms)
    {
        Expression? combined = null;
        for (int i = 0; i < terms.Count; i++)
        {
            combined = combined == null
                ? terms[i]
                : new BinaryExpression
                {
                    Op = BinaryOp.And,
                    Left = combined,
                    Right = terms[i],
                };
        }

        return combined;
    }

    #endregion

    #region Index Scan Selection

    /// <summary>
    /// Attempts to use a point/equality lookup for a WHERE clause.
    /// Supports extracting an integer equality term from AND-conjunct predicates.
    /// remaining is set to residual terms that were not consumed by the lookup.
    /// </summary>
    private IOperator? TryBuildIndexScan(string tableName, Expression where, TableSchema schema, out Expression? remaining)
    {
        int pkIdx = schema.PrimaryKeyColumnIndex;
        bool hasIntegerPk = pkIdx >= 0 &&
            pkIdx < schema.Columns.Count &&
            schema.Columns[pkIdx].Type == DbType.Integer;
        var indexes = _catalog.GetSqlIndexesForTable(tableName);

        remaining = where;
        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        int selectedConjunctIndex = -1;
        LookupCandidate selectedCandidate = default;
        bool hasSelectedCandidate = false;

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (!TryPickLookupCandidate(tableName, conjuncts[i], schema, indexes, hasIntegerPk, pkIdx, out var candidate))
                continue;

            if (!ShouldUseLookupCandidate(candidate))
                continue;

            if (!hasSelectedCandidate || IsBetterLookupCandidate(candidate, selectedCandidate))
            {
                selectedConjunctIndex = i;
                selectedCandidate = candidate;
                hasSelectedCandidate = true;

                if (candidate.Rank == 0)
                    break;
            }
        }

        bool hasCompositeCandidate = TryPickCompositeLookupCandidate(
            conjuncts,
            schema,
            indexes,
            out var compositeIndex,
            out long compositeLookupKey,
            out var compositeColumnIndices,
            out var compositeKeyComponents);

        if (hasCompositeCandidate &&
            (!hasSelectedCandidate || (compositeIndex!.IsUnique ? 1 : 2) < selectedCandidate.Rank))
        {
            remaining = BuildResidualTermsExcludingLookupKeyTerms(
                conjuncts,
                schema,
                compositeColumnIndices!,
                compositeKeyComponents!);
            return BuildLookupOperator(
                tableName,
                schema,
                isPrimaryKey: false,
                compositeIndex,
                compositeLookupKey,
                compositeColumnIndices,
                compositeKeyComponents);
        }

        if (!hasSelectedCandidate || selectedConjunctIndex < 0)
            return null;
        IOperator lookupOp = BuildLookupOperator(
            tableName,
            schema,
            selectedCandidate.IsPrimaryKey,
            selectedCandidate.Index,
            selectedCandidate.LookupValue,
            selectedCandidate.KeyColumnIndices,
            selectedCandidate.KeyComponents);

        if (selectedCandidate.RequiresResidualPredicate)
        {
            remaining = where;
            return lookupOp;
        }

        if (conjuncts.Count == 1)
        {
            remaining = null;
            return lookupOp;
        }

        var residualTerms = new List<Expression>(conjuncts.Count - 1);
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (i == selectedConjunctIndex)
                continue;

            residualTerms.Add(conjuncts[i]);
        }

        remaining = CombineConjuncts(residualTerms);
        return lookupOp;
    }

    private readonly record struct LookupCandidate(
        long LookupValue,
        bool IsPrimaryKey,
        IndexSchema? Index,
        int Rank,
        bool RequiresResidualPredicate,
        long? EstimatedRows,
        long? TableRowCount,
        int[]? KeyColumnIndices,
        DbValue[]? KeyComponents);

    private bool TryPickLookupCandidate(
        string tableName,
        Expression expression,
        TableSchema schema,
        IReadOnlyList<IndexSchema> indexes,
        bool hasIntegerPk,
        int pkIdx,
        out LookupCandidate candidate)
    {
        candidate = default;

        if (!TryExtractIndexEqualityLookupTerm(
                expression,
                schema,
                out int columnIndex,
                out DbValue lookupLiteral,
                out string? queryCollation))
        {
            return false;
        }

        if (hasIntegerPk &&
            columnIndex == pkIdx &&
            lookupLiteral.Type == DbType.Integer)
        {
            candidate = new LookupCandidate(
                LookupValue: lookupLiteral.AsInteger,
                IsPrimaryKey: true,
                Index: null,
                Rank: 0,
                RequiresResidualPredicate: false,
                EstimatedRows: 1,
                TableRowCount: 1,
                KeyColumnIndices: null,
                KeyComponents: null);
            return true;
        }

        string columnName = schema.Columns[columnIndex].Name;
        var matchedIndex = FindLookupIndexForColumn(indexes, schema, columnIndex, queryCollation);
        if (matchedIndex == null)
            return false;

        bool usesDirectIntegerKey =
            lookupLiteral.Type == DbType.Integer &&
            schema.Columns[columnIndex].Type == DbType.Integer;
        DbValue normalizedLookupLiteral = usesDirectIntegerKey
            ? lookupLiteral
            : NormalizeLookupLiteralForIndex(lookupLiteral, matchedIndex, schema, 0, columnIndex);
        long lookupValue = usesDirectIntegerKey
            ? lookupLiteral.AsInteger
            : ComputeLookupKeyForIndex(matchedIndex, schema, 0, columnIndex, normalizedLookupLiteral);
        long estimatedRows = 0;
        long tableRowCount = 0;
        bool hasEstimatedRows = !matchedIndex.IsUnique &&
            TryEstimateLookupRowCount(tableName, columnName, out estimatedRows, out tableRowCount);

        candidate = new LookupCandidate(
            lookupValue,
            IsPrimaryKey: false,
            Index: matchedIndex,
            Rank: matchedIndex.IsUnique ? 1 : 2,
            RequiresResidualPredicate: false,
            EstimatedRows: matchedIndex.IsUnique
                ? 1
                : hasEstimatedRows ? estimatedRows : null,
            TableRowCount: matchedIndex.IsUnique
                ? 1
                : hasEstimatedRows ? tableRowCount : null,
            KeyColumnIndices: usesDirectIntegerKey ? null : [columnIndex],
            KeyComponents: usesDirectIntegerKey ? null : [normalizedLookupLiteral]);
        return true;
    }

    private bool TryEstimateLookupRowCount(string tableName, string columnName, out long estimatedRows, out long tableRowCount)
    {
        return CardinalityEstimator.TryEstimateLookupRowCount(
            _tableRowCountProvider,
            _catalog,
            tableName,
            columnName,
            out estimatedRows,
            out tableRowCount);
    }

    private static bool ShouldUseLookupCandidate(LookupCandidate candidate)
    {
        if (candidate.IsPrimaryKey || candidate.Index?.IsUnique == true)
            return true;

        if (!candidate.EstimatedRows.HasValue)
            return true;

        if (!candidate.TableRowCount.HasValue)
            return true;

        return ShouldPreferNonUniqueLookup(candidate.TableRowCount.Value, candidate.EstimatedRows.Value, candidate.RequiresResidualPredicate);
    }

    private static bool ShouldPreferNonUniqueLookup(long tableRowCount, long estimatedRows, bool requiresResidualPredicate)
    {
        tableRowCount = Math.Max(tableRowCount, 1);
        estimatedRows = Math.Clamp(estimatedRows, 1, tableRowCount);

        if (tableRowCount <= 64)
            return true;

        long divisor = requiresResidualPredicate ? 8 : 4;
        return estimatedRows <= Math.Max(1, tableRowCount / divisor);
    }

    private static bool IsBetterLookupCandidate(LookupCandidate candidate, LookupCandidate currentBest)
    {
        if (candidate.Rank != currentBest.Rank)
            return candidate.Rank < currentBest.Rank;

        if (candidate.EstimatedRows.HasValue && currentBest.EstimatedRows.HasValue &&
            candidate.EstimatedRows.Value != currentBest.EstimatedRows.Value)
        {
            return candidate.EstimatedRows.Value < currentBest.EstimatedRows.Value;
        }

        if (candidate.EstimatedRows.HasValue != currentBest.EstimatedRows.HasValue)
            return candidate.EstimatedRows.HasValue;

        if (candidate.RequiresResidualPredicate != currentBest.RequiresResidualPredicate)
            return !candidate.RequiresResidualPredicate;

        return false;
    }

    private static long DivideRoundUp(long dividend, long divisor)
    {
        if (divisor <= 0)
            throw new ArgumentOutOfRangeException(nameof(divisor));

        return dividend / divisor + (dividend % divisor == 0 ? 0 : 1);
    }

    private static bool TryPickCompositeLookupCandidate(
        IReadOnlyList<Expression> conjuncts,
        TableSchema schema,
        IReadOnlyList<IndexSchema> indexes,
        out IndexSchema? selectedIndex,
        out long lookupKey,
        out int[]? keyColumnIndices,
        out DbValue[]? keyComponents)
    {
        selectedIndex = null;
        lookupKey = 0;
        keyColumnIndices = null;
        keyComponents = null;

        if (conjuncts.Count == 0)
            return false;

        Dictionary<int, DbValue>? equalityLiteralsByColumn = null;
        Dictionary<int, string?>? equalityCollationsByColumn = null;
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (!TryExtractIndexEqualityLookupTerm(
                    conjuncts[i],
                    schema,
                    out int columnIndex,
                    out DbValue literal,
                    out string? queryCollation))
            {
                continue;
            }

            equalityLiteralsByColumn ??= new Dictionary<int, DbValue>();
            equalityCollationsByColumn ??= new Dictionary<int, string?>();
            if (equalityLiteralsByColumn.TryGetValue(columnIndex, out DbValue existing))
            {
                string? existingCollation = equalityCollationsByColumn[columnIndex];
                if (!CollationSupport.SemanticallyEquals(existingCollation, queryCollation) ||
                    CollationSupport.Compare(existing, literal, queryCollation) != 0)
                {
                    return false;
                }

                continue;
            }

            equalityLiteralsByColumn[columnIndex] = literal;
            equalityCollationsByColumn[columnIndex] = queryCollation;
        }

        if (equalityLiteralsByColumn == null || equalityLiteralsByColumn.Count < 2)
            return false;

        IndexSchema? bestUnique = null;
        long bestUniqueKey = 0;
        int bestUniqueColumnCount = -1;
        int[]? bestUniqueColumnIndices = null;
        DbValue[]? bestUniqueKeyComponents = null;

        IndexSchema? bestNonUnique = null;
        long bestNonUniqueKey = 0;
        int bestNonUniqueColumnCount = -1;
        int[]? bestNonUniqueColumnIndices = null;
        DbValue[]? bestNonUniqueKeyComponents = null;

        foreach (var idx in indexes)
        {
            if (idx.Columns.Count <= 1)
                continue;

            var candidateKeyComponents = new DbValue[idx.Columns.Count];
            var candidateColumnIndices = new int[idx.Columns.Count];
            bool matches = true;

            for (int i = 0; i < idx.Columns.Count; i++)
            {
                int colIndex = schema.GetColumnIndex(idx.Columns[i]);
                if (colIndex < 0 || colIndex >= schema.Columns.Count)
                {
                    matches = false;
                    break;
                }

                DbType colType = schema.Columns[colIndex].Type;
                if (colType is not (DbType.Integer or DbType.Text))
                {
                    matches = false;
                    break;
                }

                if (!equalityLiteralsByColumn.TryGetValue(colIndex, out DbValue literal))
                {
                    matches = false;
                    break;
                }

                if (literal.Type != colType)
                {
                    matches = false;
                    break;
                }

                candidateColumnIndices[i] = colIndex;
                candidateKeyComponents[i] = NormalizeLookupLiteralForIndex(literal, idx, schema, i, colIndex);
            }

            if (!matches)
                continue;

            string?[] candidateQueryCollations = new string?[idx.Columns.Count];
            for (int i = 0; i < idx.Columns.Count; i++)
                candidateQueryCollations[i] = equalityCollationsByColumn![candidateColumnIndices[i]];

            if (!CollationSupport.CanUseIndexForLookup(idx, schema, candidateColumnIndices, candidateQueryCollations))
                continue;

            long key = ComputeIndexKey(candidateKeyComponents);

            if (idx.IsUnique)
            {
                if (idx.Columns.Count > bestUniqueColumnCount)
                {
                    bestUnique = idx;
                    bestUniqueKey = key;
                    bestUniqueColumnCount = idx.Columns.Count;
                    bestUniqueColumnIndices = candidateColumnIndices;
                    bestUniqueKeyComponents = candidateKeyComponents;
                }
            }
            else if (idx.Columns.Count > bestNonUniqueColumnCount)
            {
                bestNonUnique = idx;
                bestNonUniqueKey = key;
                bestNonUniqueColumnCount = idx.Columns.Count;
                bestNonUniqueColumnIndices = candidateColumnIndices;
                bestNonUniqueKeyComponents = candidateKeyComponents;
            }
        }

        if (bestUnique != null)
        {
            selectedIndex = bestUnique;
            lookupKey = bestUniqueKey;
            keyColumnIndices = bestUniqueColumnIndices;
            keyComponents = bestUniqueKeyComponents;
            return true;
        }

        if (bestNonUnique != null)
        {
            selectedIndex = bestNonUnique;
            lookupKey = bestNonUniqueKey;
            keyColumnIndices = bestNonUniqueColumnIndices;
            keyComponents = bestNonUniqueKeyComponents;
            return true;
        }

        return false;
    }

    private IOperator BuildLookupOperator(
        string tableName,
        TableSchema schema,
        bool isPrimaryKey,
        IndexSchema? index,
        long lookupValue,
        int[]? expectedKeyColumnIndices = null,
        DbValue[]? expectedKeyComponents = null)
    {
        var tableTree = _catalog.GetTableTree(tableName, _pager);
        if (isPrimaryKey)
            return new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, GetReadSerializer(schema));

        var indexStore = _catalog.GetIndexStore(index!.IndexName, _pager);
        string?[]? expectedKeyCollations = expectedKeyColumnIndices is { Length: > 0 } && expectedKeyComponents is { Length: > 0 }
            ? CollationSupport.GetEffectiveIndexColumnCollations(index, schema, expectedKeyColumnIndices)
            : null;
        bool usesOrderedTextPayload = IndexMaintenanceHelper.UsesOrderedTextIndexKey(index, schema);
        return index.IsUnique && UsesDirectIntegerIndexKey(index, schema)
            ? new UniqueIndexLookupOperator(indexStore, tableTree, schema, lookupValue, GetReadSerializer(schema))
            : new IndexScanOperator(
                indexStore,
                tableTree,
                schema,
                lookupValue,
                GetReadSerializer(schema),
                expectedKeyColumnIndices,
                expectedKeyComponents,
                expectedKeyCollations,
                usesOrderedTextPayload);
    }

    private static bool UsesDirectIntegerIndexKey(IndexSchema index, TableSchema schema)
    {
        if (index.Columns.Count != 1)
            return false;

        int colIdx = schema.GetColumnIndex(index.Columns[0]);
        return colIdx >= 0 &&
            colIdx < schema.Columns.Count &&
            schema.Columns[colIdx].Type == DbType.Integer;
    }

    private static bool TryExtractIntegerEqualityLookupTerm(
        Expression expression,
        TableSchema schema,
        out int columnIndex,
        out long lookupValue)
    {
        columnIndex = -1;
        lookupValue = 0;

        if (expression is not BinaryExpression { Op: BinaryOp.Equals } eq)
            return false;

        if (TryExtractColumnIntegerLiteralPair(eq.Left, eq.Right, schema, out columnIndex, out lookupValue))
            return true;

        return TryExtractColumnIntegerLiteralPair(eq.Right, eq.Left, schema, out columnIndex, out lookupValue);
    }

    private static bool TryExtractColumnIntegerLiteralPair(
        Expression columnSide,
        Expression literalSide,
        TableSchema schema,
        out int columnIndex,
        out long lookupValue)
    {
        columnIndex = -1;
        lookupValue = 0;

        if (columnSide is not ColumnRefExpression col || literalSide is not LiteralExpression lit)
            return false;

        if (lit.LiteralType != TokenType.IntegerLiteral || lit.Value is not long literalValue)
            return false;

        int resolvedIndex = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);
        if (resolvedIndex < 0 || resolvedIndex >= schema.Columns.Count)
            return false;

        columnIndex = resolvedIndex;
        lookupValue = literalValue;
        return true;
    }

    private static bool TryExtractIndexEqualityLookupTerm(
        Expression expression,
        TableSchema schema,
        out int columnIndex,
        out DbValue lookupLiteral,
        out string? queryCollation)
    {
        columnIndex = -1;
        lookupLiteral = DbValue.Null;
        queryCollation = null;

        if (expression is not BinaryExpression { Op: BinaryOp.Equals } eq)
            return false;

        if (TryExtractColumnIndexLiteralPair(
                eq.Left,
                eq.Right,
                schema,
                out columnIndex,
                out lookupLiteral,
                out queryCollation))
        {
            return true;
        }

        return TryExtractColumnIndexLiteralPair(
            eq.Right,
            eq.Left,
            schema,
            out columnIndex,
            out lookupLiteral,
            out queryCollation);
    }

    private static bool TryExtractColumnIndexLiteralPair(
        Expression columnSide,
        Expression literalSide,
        TableSchema schema,
        out int columnIndex,
        out DbValue lookupLiteral,
        out string? queryCollation)
    {
        columnIndex = -1;
        lookupLiteral = DbValue.Null;
        queryCollation = null;

        queryCollation = CollationSupport.ResolveComparisonCollation(columnSide, literalSide, schema);

        columnSide = CollationSupport.StripCollation(columnSide);
        literalSide = CollationSupport.StripCollation(literalSide);

        if (columnSide is not ColumnRefExpression col || literalSide is not LiteralExpression lit)
            return false;
        if (!TryConvertLiteral(lit, out var literal) || literal.IsNull)
            return false;

        int resolvedIndex = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);
        if (resolvedIndex < 0 || resolvedIndex >= schema.Columns.Count)
            return false;

        DbType columnType = schema.Columns[resolvedIndex].Type;
        if (columnType is not (DbType.Integer or DbType.Text))
            return false;

        if (literal.Type != columnType)
            return false;

        columnIndex = resolvedIndex;
        lookupLiteral = literal;
        return true;
    }

    private static IndexSchema? FindLookupIndexForColumn(
        IReadOnlyList<IndexSchema> indexes,
        TableSchema schema,
        int columnIndex,
        string? queryCollation = null)
    {
        string columnName = schema.Columns[columnIndex].Name;
        IndexSchema? firstNonUnique = null;

        for (int i = 0; i < indexes.Count; i++)
        {
            var idx = indexes[i];
            if (idx.Columns.Count != 1 ||
                !string.Equals(idx.Columns[0], columnName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!CollationSupport.CanUseIndexForLookup(idx, schema, [columnIndex], [queryCollation]))
                continue;

            if (idx.IsUnique)
                return idx;

            firstNonUnique ??= idx;
        }

        return firstNonUnique;
    }

    private static DbValue NormalizeLookupLiteralForIndex(
        DbValue literal,
        IndexSchema index,
        TableSchema schema,
        int indexColumnPosition,
        int schemaColumnIndex)
    {
        string? effectiveCollation = CollationSupport.GetEffectiveIndexColumnCollation(
            index,
            schema,
            indexColumnPosition,
            schemaColumnIndex);

        return CollationSupport.NormalizeIndexValue(literal, effectiveCollation);
    }

    private bool TryFindDirectIntegerIndexForColumn(
        string tableName,
        TableSchema schema,
        int columnIndex,
        out IndexSchema index)
    {
        index = null!;

        if (columnIndex < 0 ||
            columnIndex >= schema.Columns.Count ||
            schema.Columns[columnIndex].Type != DbType.Integer)
        {
            return false;
        }

        var matchedIndex = FindLookupIndexForColumn(
            _catalog.GetSqlIndexesForTable(tableName),
            schema,
            columnIndex);
        if (matchedIndex == null || !UsesDirectIntegerIndexKey(matchedIndex, schema))
            return false;

        index = matchedIndex;
        return true;
    }

    private static long ComputeIndexKey(ReadOnlySpan<DbValue> keyComponents)
        => IndexMaintenanceHelper.ComputeIndexKey(keyComponents);

    private static long ComputeLookupKeyForIndex(
        IndexSchema index,
        TableSchema schema,
        int indexColumnPosition,
        int schemaColumnIndex,
        DbValue normalizedLiteral)
    {
        if (normalizedLiteral.Type == DbType.Text &&
            index.Columns.Count == 1 &&
            indexColumnPosition == 0 &&
            IndexMaintenanceHelper.UsesOrderedTextIndexKey(index, schema))
        {
            return OrderedTextIndexKeyCodec.ComputeKey(normalizedLiteral.AsText);
        }

        return ComputeIndexKey([normalizedLiteral]);
    }

    /// <summary>
    /// Attempts to satisfy ORDER BY with natural/index order for a simple single-table query.
    /// Returns true when ORDER BY is fully provided by the source operator.
    /// replacementSource is non-null when the source should be replaced with an ordered index scan.
    /// </summary>
    private bool TryBuildIndexOrderedScan(
        SelectStatement stmt,
        SimpleTableRef tableRef,
        TableSchema schema,
        IOperator currentSource,
        Expression? where,
        out IOperator? replacementSource,
        out Expression? remainingWhere)
    {
        replacementSource = null;
        remainingWhere = where;

        if (stmt.OrderBy is not { Count: 1 })
            return false;

        if (currentSource is not TableScanOperator)
            return false;

        if (_cteData != null && _cteData.ContainsKey(tableRef.TableName))
            return false;

        var orderBy = stmt.OrderBy[0];
        if (orderBy.Descending)
            return false;

        string? orderQueryCollation = CollationSupport.ResolveExpressionCollation(orderBy.Expression, schema);
        Expression orderExpression = CollationSupport.StripCollation(orderBy.Expression);
        if (orderExpression is not ColumnRefExpression columnRef)
            return false;

        if (columnRef.TableAlias != null)
        {
            string expectedAlias = tableRef.Alias ?? tableRef.TableName;
            if (!string.Equals(columnRef.TableAlias, expectedAlias, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        int orderColumnIndex = columnRef.TableAlias != null
            ? schema.GetQualifiedColumnIndex(columnRef.TableAlias, columnRef.ColumnName)
            : schema.GetColumnIndex(columnRef.ColumnName);
        if (orderColumnIndex < 0 || orderColumnIndex >= schema.Columns.Count)
            return false;

        var orderColumn = schema.Columns[orderColumnIndex];
        if (orderColumn.Type is not (DbType.Integer or DbType.Text))
            return false;

        // INTEGER PRIMARY KEY is physically the table B+tree key, and table scan is key-ordered.
        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (orderColumn.Type == DbType.Integer &&
            pkIdx == orderColumnIndex &&
            currentSource is TableScanOperator)
        {
            return true;
        }

        if (!stmt.Limit.HasValue &&
            !CanUseCoveredOrderedIndexScanWithoutLimit(stmt, schema, orderColumnIndex))
        {
            return false;
        }

        // Secondary indexes currently skip NULL values.
        // To preserve ORDER BY semantics, only use index order for non-nullable columns.
        if (orderColumn.Nullable)
            return false;

        IndexScanRange scanRange;
        string? orderedTextLowerBound = null;
        string? orderedTextUpperBound = null;
        if (orderColumn.Type == DbType.Integer)
        {
            ExtractOrderedIndexRange(where, schema, orderColumnIndex, out scanRange, out remainingWhere, out _);
        }
        else
        {
            ExtractOrderedTextIndexRange(
                where,
                schema,
                orderColumnIndex,
                orderQueryCollation ?? orderColumn.Collation,
                out scanRange,
                out orderedTextLowerBound,
                out orderedTextUpperBound,
                out remainingWhere,
                out _);
        }

        var indexes = _catalog.GetSqlIndexesForTable(tableRef.TableName);
        foreach (var idx in indexes)
        {
            if (idx.Columns.Count != 1 ||
                !string.Equals(idx.Columns[0], orderColumn.Name, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            bool isSupportedOrderedIndex = orderColumn.Type == DbType.Integer
                ? UsesDirectIntegerIndexKey(idx, schema)
                : IndexMaintenanceHelper.UsesOrderedTextIndexKey(idx, schema) &&
                  CollationSupport.CanUseIndexForLookup(idx, schema, [orderColumnIndex], [orderQueryCollation]);
            if (!isSupportedOrderedIndex)
                continue;

            var indexStore = _catalog.GetIndexStore(idx.IndexName, _pager);
            var tableTree = _catalog.GetTableTree(tableRef.TableName, _pager);
            replacementSource = new IndexOrderedScanOperator(
                indexStore,
                tableTree,
                schema,
                orderColumnIndex,
                scanRange,
                usesOrderedTextPayload: orderColumn.Type == DbType.Text,
                orderedTextLowerBound: orderedTextLowerBound,
                orderedTextLowerInclusive: scanRange.LowerInclusive,
                orderedTextUpperBound: orderedTextUpperBound,
                orderedTextUpperInclusive: scanRange.UpperInclusive,
                recordSerializer: GetReadSerializer(schema));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Fast path for simple secondary-index lookups:
    /// SELECT * / columns FROM table WHERE indexed_col = literal [AND ...],
    /// or integer range predicates on indexed INTEGER columns.
    /// Bypasses BuildFromOperator and broad planner setup.
    /// </summary>
    private bool TryFastIndexedLookup(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (_catalog.IsView(simpleRef.TableName))
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;

        if (stmt.Where == null)
            return false;

        if (stmt.GroupBy != null || stmt.Having != null)
            return false;
        if (stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression)))
            return false;
        if (stmt.OrderBy is { Count: > 0 })
            return false;
        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        var baseSchema = _catalog.GetTable(simpleRef.TableName);
        if (baseSchema == null)
            return false;
        var schema = CreateSimpleTableQuerySchema(baseSchema, simpleRef.Alias);
        var serializer = GetReadSerializer(baseSchema);

        var indexOp = TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out var remainingWhere)
            ?? TryBuildIntegerIndexRangeScan(simpleRef.TableName, stmt.Where, schema, out remainingWhere)
            ?? TryBuildOrderedTextIndexRangeScan(simpleRef.TableName, stmt.Where, schema, out remainingWhere);
        if (indexOp == null)
            return false;

        IOperator op = indexOp;
        List<PushdownPredicateSpec>? pushedPredicates = null;
        if (remainingWhere != null &&
            TryExtractPushdownPredicates(remainingWhere, schema, out var extractedPredicates, out var residualWhere))
        {
            pushedPredicates = extractedPredicates;
            remainingWhere = residualWhere;

            for (int i = 0; i < pushedPredicates.Count; i++)
            {
                var predicate = pushedPredicates[i];
                if (op is IPreDecodeFilterSupport preDecodeFilterTarget)
                    preDecodeFilterTarget.SetPreDecodeFilter(predicate.ColumnIndex, predicate.Op, predicate.Literal);
            }
        }

        if (stmt.Columns.Any(c => c.IsStar))
        {
            if (remainingWhere != null)
                op = new FilterOperator(
                    op,
                    GetOrCompileSpanExpression(remainingWhere, schema),
                    TryCreateFilterBatchPlan(op, remainingWhere, schema));
            result = CreateQueryResult(op);
            return true;
        }

        if (TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out var outputCols))
        {
            if (remainingWhere == null &&
                op is IndexOrderedScanOperator orderedScan &&
                TryBuildCoveredOrderedIndexProjectionOperator(
                    orderedScan,
                    schema,
                    columnIndices,
                    outputCols,
                    out var coveredOrderedProjection))
            {
                result = new QueryResult(coveredOrderedProjection);
                return true;
            }

            if (remainingWhere == null &&
                op is IndexScanOperator hashedLookup &&
                TryBuildCoveredHashedIndexProjectionOperator(
                    hashedLookup,
                    schema,
                    columnIndices,
                    outputCols,
                    out var coveredHashedProjection))
            {
                result = new QueryResult(coveredHashedProjection);
                return true;
            }

            if (indexOp is IEncodedPayloadSource &&
                TryGetProjectionDecodeColumnIndices(stmt, schema, remainingWhere, includeOrderBy: false, out var decodeColumnIndices))
            {
                var compactSchema = CreateCompactProjectionSchema(schema, decodeColumnIndices);
                if (TryBuildColumnProjection(stmt.Columns, compactSchema, out var compactProjectionIndices, out var compactOutputCols))
                {
                    var projectionExpressions = stmt.Columns.Select(c => c.Expression!).ToArray();
                    var batchPlan = remainingWhere != null
                        ? TryCreateCompactBatchPlan(remainingWhere, projectionExpressions, compactSchema)
                        : null;

                    var compactOp = new CompactPayloadProjectionOperator(
                        indexOp,
                        serializer,
                        decodeColumnIndices,
                        compactProjectionIndices,
                        compactOutputCols);
                    if (remainingWhere != null)
                        compactOp.SetPredicateEvaluator(GetOrCompileExpression(remainingWhere, compactSchema));
                    if (batchPlan != null)
                        compactOp.SetBatchPlan(batchPlan);

                    result = CreateQueryResult(compactOp);
                    return true;
                }
            }

            int maxCol = -1;
            for (int i = 0; i < columnIndices.Length; i++)
                if (columnIndices[i] > maxCol) maxCol = columnIndices[i];

            if (remainingWhere != null &&
                !TryAccumulateMaxReferencedColumn(remainingWhere, schema, ref maxCol))
            {
                return false;
            }

            if (maxCol >= 0)
                TrySetDecodedColumnUpperBound(op, maxCol);

            if (remainingWhere != null)
                op = new FilterOperator(
                    op,
                    GetOrCompileSpanExpression(remainingWhere, schema),
                    TryCreateFilterBatchPlan(op, remainingWhere, schema));

            op = new ProjectionOperator(op, columnIndices, outputCols, schema);
            result = CreateQueryResult(op);
            return true;
        }

        if (indexOp is IEncodedPayloadSource &&
            TryGetProjectionDecodeColumnIndices(stmt, schema, remainingWhere, includeOrderBy: false, out var expressionDecodeColumnIndices))
        {
            var compactSchema = CreateCompactProjectionSchema(schema, expressionDecodeColumnIndices);
            var expressions = stmt.Columns.Select(c => c.Expression!).ToArray();
            var expressionOutputCols = new ColumnDefinition[expressions.Length];
            for (int i = 0; i < expressions.Length; i++)
                expressionOutputCols[i] = InferColumnDef(expressions[i], stmt.Columns[i].Alias, compactSchema, i);

            var compactExpressionOp = new CompactPayloadProjectionOperator(
                indexOp,
                serializer,
                expressionDecodeColumnIndices,
                expressionOutputCols,
                GetOrCompileExpressions(expressions, compactSchema));
            if (remainingWhere != null)
                compactExpressionOp.SetPredicateEvaluator(GetOrCompileExpression(remainingWhere, compactSchema));
            var compactBatchPlan = TryCreateCompactBatchPlan(remainingWhere, expressions, compactSchema);
            if (compactBatchPlan != null)
                compactExpressionOp.SetBatchPlan(compactBatchPlan);

            result = CreateQueryResult(compactExpressionOp);
            return true;
        }

        // Expression projections fall back to the general path.
        return false;
    }

    private bool TryMaterializeSyncIndexedLookupResult(
        IOperator lookupOp,
        TableSchema schema,
        IRecordSerializer serializer,
        ColumnDefinition[] outputSchema,
        int[]? projectionColumnIndices,
        out QueryResult result)
    {
        result = null!;

        if (!PreferSyncPointLookups ||
            !TryGetSingleCachedLookupRowPayload(lookupOp, out var rowPayload))
        {
            return false;
        }

        if (projectionColumnIndices == null)
        {
            DbValue[]? row = rowPayload is { } rowPayloadMemory
                ? serializer.Decode(rowPayloadMemory.Span)
                : null;
            result = QueryResult.FromSyncLookup(row, outputSchema);
            return true;
        }

        DbValue[]? projectedRow = null;
        if (rowPayload is { } projectedPayloadMemory)
        {
            projectedRow = projectionColumnIndices.Length == 0
                ? Array.Empty<DbValue>()
                : new DbValue[projectionColumnIndices.Length];

            if (projectionColumnIndices.Length > 0)
            {
                if (AreStrictlyAscendingUnique(projectionColumnIndices))
                {
                    serializer.DecodeSelectedCompactInto(
                        projectedPayloadMemory.Span,
                        projectedRow,
                        projectionColumnIndices);
                }
                else
                {
                    var decodedRow = serializer.Decode(projectedPayloadMemory.Span);
                    for (int i = 0; i < projectionColumnIndices.Length; i++)
                    {
                        int columnIndex = projectionColumnIndices[i];
                        projectedRow[i] = columnIndex >= 0 && columnIndex < decodedRow.Length
                            ? decodedRow[columnIndex]
                            : DbValue.Null;
                    }
                }
            }
        }

        result = QueryResult.FromSyncLookup(projectedRow, outputSchema);
        return true;
    }

    private static bool TryGetSingleCachedLookupRowPayload(IOperator lookupOp, out ReadOnlyMemory<byte>? rowPayload)
    {
        rowPayload = null;
        return lookupOp switch
        {
            UniqueIndexLookupOperator uniqueLookup => TryGetSingleCachedLookupRowPayload(uniqueLookup, out rowPayload),
            IndexScanOperator indexScan => TryGetSingleCachedLookupRowPayload(indexScan, out rowPayload),
            _ => false,
        };
    }

    private static bool TryGetSingleCachedLookupRowPayload(
        UniqueIndexLookupOperator uniqueLookup,
        out ReadOnlyMemory<byte>? rowPayload)
    {
        rowPayload = null;

        if (uniqueLookup.IndexStore is not ICacheAwareIndexStore cacheAware ||
            !cacheAware.TryFindCached(uniqueLookup.SeekValue, out var cachedIndexPayload))
        {
            return false;
        }

        if (cachedIndexPayload is not { Length: >= RowIdPayloadCodec.RowIdSize })
            return true;

        long rowId = BinaryPrimitives.ReadInt64LittleEndian(cachedIndexPayload.AsSpan(0, RowIdPayloadCodec.RowIdSize));
        if (!uniqueLookup.TableTree.TryFindCachedMemory(rowId, out var cachedRowPayload))
            return false;

        rowPayload = cachedRowPayload;
        return true;
    }

    private static bool TryGetSingleCachedLookupRowPayload(
        IndexScanOperator indexScan,
        out ReadOnlyMemory<byte>? rowPayload)
    {
        rowPayload = null;

        if (!TryResolveSingleCachedIndexRowId(indexScan, out bool foundRow, out long rowId))
            return false;

        if (!foundRow)
            return true;

        if (!indexScan.TableTree.TryFindCachedMemory(rowId, out var cachedRowPayload))
            return false;

        rowPayload = cachedRowPayload;
        return true;
    }

    private static bool TryResolveSingleCachedIndexRowId(
        IndexScanOperator indexScan,
        out bool foundRow,
        out long rowId)
    {
        foundRow = false;
        rowId = 0;

        if (indexScan.IndexStore is not ICacheAwareIndexStore cacheAware ||
            !cacheAware.TryFindCached(indexScan.SeekValue, out var cachedIndexPayload))
        {
            return false;
        }

        if (cachedIndexPayload is { Length: > 0 } &&
            indexScan.ExpectedKeyComponents is { Length: > 0 } keyComponents &&
            HashedIndexPayloadCodec.TryGetSingleMatchingRowId(
                cachedIndexPayload,
                keyComponents,
                indexScan.ExpectedKeyTextBytes,
                out foundRow,
                out rowId))
        {
            return true;
        }

        if (!TryGetCachedIndexRowIdPayload(indexScan, cachedIndexPayload, out var rowIdPayload))
            return false;

        if (rowIdPayload.IsEmpty)
            return true;

        if (RowIdPayloadCodec.GetCount(rowIdPayload.Span) != 1)
            return false;

        rowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, 0);
        foundRow = true;
        return true;
    }

    private static bool TryGetCachedIndexRowIdPayload(
        IndexScanOperator indexScan,
        byte[]? payload,
        out ReadOnlyMemory<byte> rowIdPayload)
    {
        rowIdPayload = ReadOnlyMemory<byte>.Empty;

        if (payload is not { Length: > 0 })
            return true;

        if (indexScan.UsesOrderedTextPayload)
        {
            if (indexScan.ExpectedKeyComponents is [var keyComponent] &&
                keyComponent.Type == DbType.Text &&
                OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(
                    payload,
                    keyComponent.AsText,
                    out var orderedRowIds))
            {
                rowIdPayload = orderedRowIds;
                return true;
            }

            return false;
        }

        if (indexScan.ExpectedKeyComponents is { Length: > 0 } keyComponents)
        {
            rowIdPayload = HashedIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(
                payload,
                keyComponents,
                indexScan.ExpectedKeyTextBytes,
                out var hashedRowIds)
                ? hashedRowIds
                : ReadOnlyMemory<byte>.Empty;
            return rowIdPayload.Length > 0 || HashedIndexPayloadCodec.IsEncoded(payload);
        }

        rowIdPayload = payload;
        return true;
    }

    private static bool AreStrictlyAscendingUnique(ReadOnlySpan<int> values)
    {
        for (int i = 1; i < values.Length; i++)
        {
            if (values[i] <= values[i - 1])
                return false;
        }

        return true;
    }

    /// <summary>
    /// Fast path for simple single-table scans:
    /// SELECT * / columns FROM table [WHERE ...].
    /// Bypasses BuildFromOperator and broad planner setup when no ordering/grouping/windowing clauses are present.
    /// </summary>
    private bool TryFastSimpleTableScan(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (_catalog.IsView(simpleRef.TableName))
            return false;
        if (IsSystemCatalogTable(simpleRef.TableName))
            return false;
        if (_cteData != null && _cteData.ContainsKey(simpleRef.TableName))
            return false;

        if (stmt.GroupBy != null || stmt.Having != null)
            return false;
        if (stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression)))
            return false;
        if (stmt.OrderBy is { Count: > 0 })
            return false;
        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        var baseSchema = _catalog.GetTable(simpleRef.TableName);
        if (baseSchema == null)
            return false;
        var schema = CreateSimpleTableQuerySchema(baseSchema, simpleRef.Alias);

        var tableTree = _catalog.GetTableTree(simpleRef.TableName, _pager);
        var serializer = GetReadSerializer(baseSchema);
        int? estimatedRowCount = TryGetCachedTreeRowCountCapacityHint(tableTree);
        var remainingWhere = stmt.Where;

        if (stmt.Columns.Any(c => c.IsStar))
        {
            var scanOp = new TableScanOperator(
                tableTree,
                schema,
                serializer,
                estimatedRowCount);
            IOperator op = scanOp;

            if (remainingWhere != null &&
                TryPushDownSimplePreDecodeFilter(scanOp, remainingWhere, schema, out var pushedWhere))
            {
                remainingWhere = pushedWhere;
            }

            if (remainingWhere != null)
                op = new FilterOperator(
                    op,
                    GetOrCompileSpanExpression(remainingWhere, schema),
                    TryCreateFilterBatchPlan(op, remainingWhere, schema));

            result = CreateQueryResult(op);
            return true;
        }

        List<PushdownPredicateSpec>? pushedPredicates = null;
        if (remainingWhere != null &&
            TryExtractPushdownPredicates(remainingWhere, schema, out var extractedPredicates, out var residualWhere))
        {
            pushedPredicates = extractedPredicates;
            remainingWhere = residualWhere;
        }

        if (!TryGetProjectionDecodeColumnIndices(stmt, schema, remainingWhere, includeOrderBy: false, out var decodeColumnIndices))
            return false;

        var compactSchema = CreateCompactProjectionSchema(schema, decodeColumnIndices);

        if (TryBuildColumnProjection(stmt.Columns, compactSchema, out var compactProjectionIndices, out var outputCols))
        {
            var projectionExpressions = stmt.Columns.Select(c => c.Expression!).ToArray();
            var batchPlan = remainingWhere != null
                ? TryCreateCompactBatchPlan(remainingWhere, projectionExpressions, compactSchema)
                : null;

            var compactOp = new CompactTableScanProjectionOperator(
                tableTree,
                decodeColumnIndices,
                compactProjectionIndices,
                outputCols,
                serializer,
                estimatedRowCount);

            if (pushedPredicates is { Count: > 0 })
            {
                for (int i = 0; i < pushedPredicates.Count; i++)
                {
                    var predicate = pushedPredicates[i];
                    compactOp.SetPreDecodeFilter(predicate.ColumnIndex, predicate.Op, predicate.Literal);
                }
            }

            if (remainingWhere != null)
                compactOp.SetPredicateEvaluator(GetOrCompileExpression(remainingWhere, compactSchema));
            if (batchPlan != null)
                compactOp.SetBatchPlan(batchPlan);

            result = CreateQueryResult(compactOp);
            return true;
        }

        var expressions = stmt.Columns.Select(c => c.Expression!).ToArray();
        var expressionOutputCols = new ColumnDefinition[expressions.Length];
        for (int i = 0; i < expressions.Length; i++)
            expressionOutputCols[i] = InferColumnDef(expressions[i], stmt.Columns[i].Alias, compactSchema, i);

        var compactExpressionOp = new CompactTableScanProjectionOperator(
            tableTree,
            decodeColumnIndices,
            expressionOutputCols,
            GetOrCompileExpressions(expressions, compactSchema),
            serializer,
            estimatedRowCount);

        if (pushedPredicates is { Count: > 0 })
        {
            for (int i = 0; i < pushedPredicates.Count; i++)
            {
                var predicate = pushedPredicates[i];
                compactExpressionOp.SetPreDecodeFilter(predicate.ColumnIndex, predicate.Op, predicate.Literal);
            }
        }

        if (remainingWhere != null)
            compactExpressionOp.SetPredicateEvaluator(GetOrCompileExpression(remainingWhere, compactSchema));
        var compactBatchPlan = TryCreateCompactBatchPlan(remainingWhere, expressions, compactSchema);
        if (compactBatchPlan != null)
            compactExpressionOp.SetBatchPlan(compactBatchPlan);

        result = CreateQueryResult(compactExpressionOp);
        return true;
    }

    private IOperator? TryBuildIntegerIndexRangeScan(
        string tableName,
        Expression where,
        TableSchema schema,
        out Expression? remaining)
    {
        remaining = where;
        var indexes = _catalog.GetSqlIndexesForTable(tableName);
        if (indexes.Count == 0)
            return null;

        IndexSchema? selectedIndex = null;
        IndexScanRange selectedRange = default;
        Expression? selectedRemaining = where;
        int selectedScore = int.MinValue;

        for (int i = 0; i < indexes.Count; i++)
        {
            var idx = indexes[i];
            if (!UsesDirectIntegerIndexKey(idx, schema))
                continue;

            int columnIndex = schema.GetColumnIndex(idx.Columns[0]);
            if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
                continue;

            ExtractOrderedIndexRange(
                where,
                schema,
                columnIndex,
                out var scanRange,
                out var residualWhere,
                out int consumedTermCount);

            if (consumedTermCount == 0)
                continue;

            int score = consumedTermCount * 10 + (idx.IsUnique ? 1 : 0);
            if (score <= selectedScore)
                continue;

            selectedScore = score;
            selectedIndex = idx;
            selectedRange = scanRange;
            selectedRemaining = residualWhere;
        }

        if (selectedIndex == null)
            return null;

        remaining = selectedRemaining;
        var indexStore = _catalog.GetIndexStore(selectedIndex.IndexName, _pager);
        var tableTree = _catalog.GetTableTree(tableName, _pager);
        int keyColumnIndex = schema.GetColumnIndex(selectedIndex.Columns[0]);
        return new IndexOrderedScanOperator(
            indexStore,
            tableTree,
            schema,
            keyColumnIndex,
            selectedRange,
            usesOrderedTextPayload: false,
            recordSerializer: GetReadSerializer(schema));
    }

    private IOperator? TryBuildOrderedTextIndexRangeScan(
        string tableName,
        Expression where,
        TableSchema schema,
        out Expression? remaining)
    {
        remaining = where;
        var indexes = _catalog.GetSqlIndexesForTable(tableName);
        if (indexes.Count == 0)
            return null;

        IndexSchema? selectedIndex = null;
        IndexScanRange selectedRange = default;
        string? selectedLowerBound = null;
        string? selectedUpperBound = null;
        Expression? selectedRemaining = where;
        int selectedScore = int.MinValue;
        int selectedColumnIndex = -1;

        for (int i = 0; i < indexes.Count; i++)
        {
            var idx = indexes[i];
            if (!IndexMaintenanceHelper.UsesOrderedTextIndexKey(idx, schema))
                continue;

            int columnIndex = schema.GetColumnIndex(idx.Columns[0]);
            if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
                continue;

            string? effectiveCollation = CollationSupport.GetEffectiveIndexColumnCollation(idx, schema, 0, columnIndex);
            ExtractOrderedTextIndexRange(
                where,
                schema,
                columnIndex,
                effectiveCollation,
                out var scanRange,
                out var normalizedLowerBound,
                out var normalizedUpperBound,
                out var residualWhere,
                out int consumedTermCount);

            if (consumedTermCount == 0)
                continue;

            int score = consumedTermCount * 10 + (idx.IsUnique ? 1 : 0);
            if (score <= selectedScore)
                continue;

            selectedScore = score;
            selectedIndex = idx;
            selectedRange = scanRange;
            selectedLowerBound = normalizedLowerBound;
            selectedUpperBound = normalizedUpperBound;
            selectedRemaining = residualWhere;
            selectedColumnIndex = columnIndex;
        }

        if (selectedIndex == null || selectedColumnIndex < 0)
            return null;

        remaining = selectedRemaining;
        var indexStore = _catalog.GetIndexStore(selectedIndex.IndexName, _pager);
        var tableTree = _catalog.GetTableTree(tableName, _pager);
        return new IndexOrderedScanOperator(
            indexStore,
            tableTree,
            schema,
            selectedColumnIndex,
            selectedRange,
            usesOrderedTextPayload: true,
            orderedTextLowerBound: selectedLowerBound,
            orderedTextUpperBound: selectedUpperBound,
            orderedTextLowerInclusive: selectedRange.LowerInclusive,
            orderedTextUpperInclusive: selectedRange.UpperInclusive,
            recordSerializer: GetReadSerializer(schema));
    }

    private static void ExtractOrderedIndexRange(
        Expression? where,
        TableSchema schema,
        int orderColumnIndex,
        out IndexScanRange range,
        out Expression? remaining,
        out int consumedTermCount)
    {
        consumedTermCount = 0;

        if (where == null)
        {
            range = IndexScanRange.All;
            remaining = null;
            return;
        }

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        var residualTerms = new List<Expression>(conjuncts.Count);
        long? lowerBound = null;
        bool lowerInclusive = true;
        long? upperBound = null;
        bool upperInclusive = true;

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (TryExtractOrderedIndexBetweenRange(
                    conjuncts[i],
                    schema,
                    orderColumnIndex,
                    out long betweenLow,
                    out long betweenHigh))
            {
                consumedTermCount++;
                ApplyLowerBound(ref lowerBound, ref lowerInclusive, betweenLow, inclusive: true);
                ApplyUpperBound(ref upperBound, ref upperInclusive, betweenHigh, inclusive: true);
                continue;
            }

            if (!TryExtractOrderedIndexRangeTerm(conjuncts[i], schema, orderColumnIndex, out var term))
            {
                residualTerms.Add(conjuncts[i]);
                continue;
            }

            consumedTermCount++;
            switch (term.Kind)
            {
                case RangeTermKind.Equal:
                    ApplyLowerBound(ref lowerBound, ref lowerInclusive, term.Value, inclusive: true);
                    ApplyUpperBound(ref upperBound, ref upperInclusive, term.Value, inclusive: true);
                    break;
                case RangeTermKind.Lower:
                    ApplyLowerBound(ref lowerBound, ref lowerInclusive, term.Value, term.Inclusive);
                    break;
                case RangeTermKind.Upper:
                    ApplyUpperBound(ref upperBound, ref upperInclusive, term.Value, term.Inclusive);
                    break;
            }
        }

        if (!IsRangeSatisfiable(lowerBound, lowerInclusive, upperBound, upperInclusive))
        {
            range = new IndexScanRange(long.MaxValue, true, long.MinValue, true);
            remaining = null;
            return;
        }

        range = new IndexScanRange(lowerBound, lowerInclusive, upperBound, upperInclusive);
        remaining = CombineConjuncts(residualTerms);
    }

    private static bool TryExtractOrderedIndexBetweenRange(
        Expression expression,
        TableSchema schema,
        int orderColumnIndex,
        out long lowValue,
        out long highValue)
    {
        lowValue = 0;
        highValue = 0;

        if (expression is not BetweenExpression { Negated: false } between)
            return false;

        if (!TryExtractColumnIntegerLiteralPair(
                between.Operand,
                between.Low,
                schema,
                out int lowColumnIndex,
                out lowValue))
        {
            return false;
        }

        if (lowColumnIndex != orderColumnIndex)
            return false;

        if (!TryExtractColumnIntegerLiteralPair(
                between.Operand,
                between.High,
                schema,
                out int highColumnIndex,
                out highValue))
        {
            return false;
        }

        return highColumnIndex == orderColumnIndex;
    }

    private enum RangeTermKind
    {
        Lower,
        Upper,
        Equal,
    }

    private readonly record struct IndexRangeTerm(
        RangeTermKind Kind,
        long Value,
        bool Inclusive);

    private static bool TryExtractOrderedIndexRangeTerm(
        Expression expression,
        TableSchema schema,
        int orderColumnIndex,
        out IndexRangeTerm term)
    {
        term = default;

        if (expression is not BinaryExpression bin)
            return false;

        BinaryOp op = bin.Op;
        if (op is not BinaryOp.Equals and
            not BinaryOp.LessThan and
            not BinaryOp.LessOrEqual and
            not BinaryOp.GreaterThan and
            not BinaryOp.GreaterOrEqual)
        {
            return false;
        }

        if (TryExtractColumnIntegerLiteralPair(bin.Left, bin.Right, schema, out int leftColumnIndex, out long leftValue))
        {
            if (leftColumnIndex != orderColumnIndex)
                return false;

            return TryClassifyRangeTerm(op, leftValue, out term);
        }

        if (TryExtractColumnIntegerLiteralPair(bin.Right, bin.Left, schema, out int rightColumnIndex, out long rightValue))
        {
            if (rightColumnIndex != orderColumnIndex)
                return false;

            return TryClassifyRangeTerm(ReverseComparison(op), rightValue, out term);
        }

        return false;
    }

    private static bool TryClassifyRangeTerm(BinaryOp op, long value, out IndexRangeTerm term)
    {
        term = op switch
        {
            BinaryOp.Equals => new IndexRangeTerm(RangeTermKind.Equal, value, Inclusive: true),
            BinaryOp.GreaterThan => new IndexRangeTerm(RangeTermKind.Lower, value, Inclusive: false),
            BinaryOp.GreaterOrEqual => new IndexRangeTerm(RangeTermKind.Lower, value, Inclusive: true),
            BinaryOp.LessThan => new IndexRangeTerm(RangeTermKind.Upper, value, Inclusive: false),
            BinaryOp.LessOrEqual => new IndexRangeTerm(RangeTermKind.Upper, value, Inclusive: true),
            _ => default,
        };

        return op is BinaryOp.Equals or
            BinaryOp.GreaterThan or
            BinaryOp.GreaterOrEqual or
            BinaryOp.LessThan or
            BinaryOp.LessOrEqual;
    }

    private static void ApplyLowerBound(ref long? existingBound, ref bool existingInclusive, long candidateValue, bool inclusive)
    {
        if (!existingBound.HasValue || candidateValue > existingBound.Value)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        if (candidateValue == existingBound.Value)
            existingInclusive &= inclusive;
    }

    private static void ApplyUpperBound(ref long? existingBound, ref bool existingInclusive, long candidateValue, bool inclusive)
    {
        if (!existingBound.HasValue || candidateValue < existingBound.Value)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        if (candidateValue == existingBound.Value)
            existingInclusive &= inclusive;
    }

    private static bool IsRangeSatisfiable(
        long? lowerBound,
        bool lowerInclusive,
        long? upperBound,
        bool upperInclusive)
    {
        if (!lowerBound.HasValue || !upperBound.HasValue)
            return true;

        if (lowerBound.Value < upperBound.Value)
            return true;

        if (lowerBound.Value > upperBound.Value)
            return false;

        return lowerInclusive && upperInclusive;
    }

    private static void ExtractOrderedTextIndexRange(
        Expression? where,
        TableSchema schema,
        int orderColumnIndex,
        string? expectedCollation,
        out IndexScanRange range,
        out string? normalizedLowerBound,
        out string? normalizedUpperBound,
        out Expression? remaining,
        out int consumedTermCount)
    {
        consumedTermCount = 0;
        normalizedLowerBound = null;
        normalizedUpperBound = null;

        if (where == null)
        {
            range = IndexScanRange.All;
            remaining = null;
            return;
        }

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        var residualTerms = new List<Expression>(conjuncts.Count);
        bool lowerInclusive = true;
        bool upperInclusive = true;

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (TryExtractOrderedTextIndexBetweenRange(
                    conjuncts[i],
                    schema,
                    orderColumnIndex,
                    expectedCollation,
                    out string betweenLow,
                    out string betweenHigh))
            {
                consumedTermCount++;
                ApplyLowerTextBound(ref normalizedLowerBound, ref lowerInclusive, betweenLow, inclusive: true);
                ApplyUpperTextBound(ref normalizedUpperBound, ref upperInclusive, betweenHigh, inclusive: true);
                continue;
            }

            if (!TryExtractOrderedTextIndexRangeTerm(
                    conjuncts[i],
                    schema,
                    orderColumnIndex,
                    expectedCollation,
                    out var term))
            {
                residualTerms.Add(conjuncts[i]);
                continue;
            }

            consumedTermCount++;
            switch (term.Kind)
            {
                case TextRangeTermKind.Equal:
                    ApplyLowerTextBound(ref normalizedLowerBound, ref lowerInclusive, term.Value, inclusive: true);
                    ApplyUpperTextBound(ref normalizedUpperBound, ref upperInclusive, term.Value, inclusive: true);
                    break;
                case TextRangeTermKind.Lower:
                    ApplyLowerTextBound(ref normalizedLowerBound, ref lowerInclusive, term.Value, term.Inclusive);
                    break;
                case TextRangeTermKind.Upper:
                    ApplyUpperTextBound(ref normalizedUpperBound, ref upperInclusive, term.Value, term.Inclusive);
                    break;
            }
        }

        if (!IsTextRangeSatisfiable(normalizedLowerBound, lowerInclusive, normalizedUpperBound, upperInclusive))
        {
            range = new IndexScanRange(long.MaxValue, true, long.MinValue, true);
            normalizedLowerBound = null;
            normalizedUpperBound = null;
            remaining = null;
            return;
        }

        long? lowerKey = normalizedLowerBound != null
            ? OrderedTextIndexKeyCodec.ComputeKey(normalizedLowerBound)
            : null;
        long? upperKey = normalizedUpperBound != null
            ? OrderedTextIndexKeyCodec.ComputeKey(normalizedUpperBound)
            : null;
        range = new IndexScanRange(lowerKey, lowerInclusive, upperKey, upperInclusive);
        remaining = CombineConjuncts(residualTerms);
    }

    private static bool TryExtractOrderedTextIndexBetweenRange(
        Expression expression,
        TableSchema schema,
        int orderColumnIndex,
        string? expectedCollation,
        out string lowValue,
        out string highValue)
    {
        lowValue = string.Empty;
        highValue = string.Empty;

        if (expression is not BetweenExpression { Negated: false } between)
            return false;

        if (!TryExtractColumnTextLiteralPair(
                between.Operand,
                between.Low,
                schema,
                expectedCollation,
                out int lowColumnIndex,
                out lowValue))
        {
            return false;
        }

        if (lowColumnIndex != orderColumnIndex)
            return false;

        if (!TryExtractColumnTextLiteralPair(
                between.Operand,
                between.High,
                schema,
                expectedCollation,
                out int highColumnIndex,
                out highValue))
        {
            return false;
        }

        return highColumnIndex == orderColumnIndex;
    }

    private enum TextRangeTermKind
    {
        Lower,
        Upper,
        Equal,
    }

    private readonly record struct TextIndexRangeTerm(
        TextRangeTermKind Kind,
        string Value,
        bool Inclusive);

    private static bool TryExtractOrderedTextIndexRangeTerm(
        Expression expression,
        TableSchema schema,
        int orderColumnIndex,
        string? expectedCollation,
        out TextIndexRangeTerm term)
    {
        term = default;

        if (expression is not BinaryExpression bin)
            return false;

        BinaryOp op = bin.Op;
        if (op is not BinaryOp.Equals and
            not BinaryOp.LessThan and
            not BinaryOp.LessOrEqual and
            not BinaryOp.GreaterThan and
            not BinaryOp.GreaterOrEqual)
        {
            return false;
        }

        if (TryExtractColumnTextLiteralPair(
                bin.Left,
                bin.Right,
                schema,
                expectedCollation,
                out int leftColumnIndex,
                out string leftValue))
        {
            if (leftColumnIndex != orderColumnIndex)
                return false;

            return TryClassifyTextRangeTerm(op, leftValue, out term);
        }

        if (TryExtractColumnTextLiteralPair(
                bin.Right,
                bin.Left,
                schema,
                expectedCollation,
                out int rightColumnIndex,
                out string rightValue))
        {
            if (rightColumnIndex != orderColumnIndex)
                return false;

            return TryClassifyTextRangeTerm(ReverseComparison(op), rightValue, out term);
        }

        return false;
    }

    private static bool TryClassifyTextRangeTerm(BinaryOp op, string value, out TextIndexRangeTerm term)
    {
        term = op switch
        {
            BinaryOp.Equals => new TextIndexRangeTerm(TextRangeTermKind.Equal, value, Inclusive: true),
            BinaryOp.GreaterThan => new TextIndexRangeTerm(TextRangeTermKind.Lower, value, Inclusive: false),
            BinaryOp.GreaterOrEqual => new TextIndexRangeTerm(TextRangeTermKind.Lower, value, Inclusive: true),
            BinaryOp.LessThan => new TextIndexRangeTerm(TextRangeTermKind.Upper, value, Inclusive: false),
            BinaryOp.LessOrEqual => new TextIndexRangeTerm(TextRangeTermKind.Upper, value, Inclusive: true),
            _ => default,
        };

        return op is BinaryOp.Equals or
            BinaryOp.GreaterThan or
            BinaryOp.GreaterOrEqual or
            BinaryOp.LessThan or
            BinaryOp.LessOrEqual;
    }

    private static bool TryExtractColumnTextLiteralPair(
        Expression columnSide,
        Expression literalSide,
        TableSchema schema,
        string? expectedCollation,
        out int columnIndex,
        out string normalizedText)
    {
        columnIndex = -1;
        normalizedText = string.Empty;

        string? queryCollation = CollationSupport.ResolveComparisonCollation(columnSide, literalSide, schema);
        if (!CollationSupport.SemanticallyEquals(queryCollation, expectedCollation))
            return false;

        columnSide = CollationSupport.StripCollation(columnSide);
        literalSide = CollationSupport.StripCollation(literalSide);

        if (columnSide is not ColumnRefExpression col || literalSide is not LiteralExpression lit)
            return false;

        if (!TryConvertLiteral(lit, out DbValue literal) ||
            literal.IsNull ||
            literal.Type != DbType.Text)
        {
            return false;
        }

        int resolvedIndex = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);
        if (resolvedIndex < 0 || resolvedIndex >= schema.Columns.Count)
            return false;

        if (schema.Columns[resolvedIndex].Type != DbType.Text)
            return false;

        columnIndex = resolvedIndex;
        normalizedText = CollationSupport.NormalizeText(literal.AsText, expectedCollation);
        return true;
    }

    private static void ApplyLowerTextBound(ref string? existingBound, ref bool existingInclusive, string candidateValue, bool inclusive)
    {
        if (existingBound == null)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        int comparison = string.Compare(existingBound, candidateValue, StringComparison.Ordinal);
        if (comparison < 0)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        if (comparison == 0)
            existingInclusive &= inclusive;
    }

    private static void ApplyUpperTextBound(ref string? existingBound, ref bool existingInclusive, string candidateValue, bool inclusive)
    {
        if (existingBound == null)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        int comparison = string.Compare(existingBound, candidateValue, StringComparison.Ordinal);
        if (comparison > 0)
        {
            existingBound = candidateValue;
            existingInclusive = inclusive;
            return;
        }

        if (comparison == 0)
            existingInclusive &= inclusive;
    }

    private static bool IsTextRangeSatisfiable(
        string? lowerBound,
        bool lowerInclusive,
        string? upperBound,
        bool upperInclusive)
    {
        if (lowerBound == null || upperBound == null)
            return true;

        int comparison = string.Compare(lowerBound, upperBound, StringComparison.Ordinal);
        if (comparison < 0)
            return true;

        if (comparison > 0)
            return false;

        return lowerInclusive && upperInclusive;
    }

    private static bool TryPushDownSimplePreDecodeFilter(
        IOperator op,
        Expression where,
        TableSchema schema,
        out Expression? remaining)
    {
        remaining = where;

        if (op is not IPreDecodeFilterSupport preDecodeFilterTarget)
            return false;

        if (TryExtractPushdownPredicates(where, schema, out var predicates, out var residual))
        {
            for (int i = 0; i < predicates.Count; i++)
            {
                var predicate = predicates[i];
                preDecodeFilterTarget.SetPreDecodeFilter(predicate.ColumnIndex, predicate.Op, predicate.Literal);
            }

            remaining = residual;
            return true;
        }

        return false;
    }

    private static bool TryExtractPushdownPredicates(
        Expression where,
        TableSchema schema,
        out List<PushdownPredicateSpec> predicates,
        out Expression? remaining)
    {
        predicates = [];
        remaining = where;

        if (where is BinaryExpression singleBin &&
            IsPushdownComparison(singleBin.Op) &&
            TryGetPushdownOperands(singleBin, schema, out int columnIndex, out BinaryOp opToApply, out DbValue literal))
        {
            predicates.Add(new PushdownPredicateSpec(columnIndex, opToApply, literal));
            remaining = null;
            return true;
        }

        if (where is not BinaryExpression { Op: BinaryOp.And })
            return false;

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        var residualTerms = new List<Expression>(conjuncts.Count);

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (conjuncts[i] is BinaryExpression bin &&
                IsPushdownComparison(bin.Op) &&
                TryGetPushdownOperands(bin, schema, out int candidateColumnIndex, out BinaryOp candidateOp, out DbValue candidateLiteral))
            {
                predicates.Add(new PushdownPredicateSpec(candidateColumnIndex, candidateOp, candidateLiteral));
            }
            else
            {
                residualTerms.Add(conjuncts[i]);
            }
        }

        if (predicates.Count == 0)
            return false;

        predicates.Sort(static (left, right) =>
            GetPushdownComparisonRank(left.Op).CompareTo(GetPushdownComparisonRank(right.Op)));
        remaining = CombineConjuncts(residualTerms);
        return true;
    }

    private readonly struct PushdownPredicateSpec
    {
        public PushdownPredicateSpec(int columnIndex, BinaryOp op, DbValue literal)
        {
            ColumnIndex = columnIndex;
            Op = op;
            Literal = literal;
        }

        public int ColumnIndex { get; }
        public BinaryOp Op { get; }
        public DbValue Literal { get; }
    }

    private static int GetPushdownComparisonRank(BinaryOp op)
    {
        return op switch
        {
            BinaryOp.Equals => 0,
            BinaryOp.LessThan or BinaryOp.GreaterThan or BinaryOp.LessOrEqual or BinaryOp.GreaterOrEqual => 1,
            BinaryOp.NotEquals => 2,
            _ => 3,
        };
    }

    private static bool IsPushdownComparison(BinaryOp op)
    {
        return op is BinaryOp.Equals
            or BinaryOp.NotEquals
            or BinaryOp.LessThan
            or BinaryOp.GreaterThan
            or BinaryOp.LessOrEqual
            or BinaryOp.GreaterOrEqual;
    }

    private static bool TryGetPushdownOperands(
        BinaryExpression bin,
        TableSchema schema,
        out int columnIndex,
        out BinaryOp op,
        out DbValue literal)
    {
        columnIndex = -1;
        op = bin.Op;
        literal = DbValue.Null;

        if (TryResolvePushdownOperand(bin.Left, bin.Right, schema, out columnIndex, out literal))
            return true;

        if (TryResolvePushdownOperand(bin.Right, bin.Left, schema, out columnIndex, out literal))
        {
            op = ReverseComparison(bin.Op);
            return true;
        }

        return false;
    }

    private static bool TryResolvePushdownOperand(
        Expression columnExpression,
        Expression literalExpression,
        TableSchema schema,
        out int columnIndex,
        out DbValue literal)
    {
        string? queryCollation = CollationSupport.ResolveComparisonCollation(columnExpression, literalExpression, schema);
        columnExpression = CollationSupport.StripCollation(columnExpression);
        literalExpression = CollationSupport.StripCollation(literalExpression);

        if (columnExpression is not ColumnRefExpression col || literalExpression is not LiteralExpression lit)
        {
            columnIndex = -1;
            literal = DbValue.Null;
            return false;
        }

        columnIndex = col.TableAlias != null
            ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
            : schema.GetColumnIndex(col.ColumnName);

        if (columnIndex < 0)
        {
            literal = DbValue.Null;
            return false;
        }

        if (!TryConvertLiteral(lit, out literal))
            return false;

        if (!CanPushDownPredicate(schema, columnIndex, literal, queryCollation))
            return false;

        return true;
    }

    private static bool CanPushDownPredicate(TableSchema schema, int columnIndex, DbValue literal, string? queryCollation = null)
    {
        if (columnIndex < 0 || columnIndex >= schema.Columns.Count)
            return false;

        if (schema.Columns[columnIndex].Type != DbType.Text || literal.Type != DbType.Text)
            return true;

        string? effectiveCollation = queryCollation ?? schema.Columns[columnIndex].Collation;
        return CollationSupport.IsBinaryOrDefault(effectiveCollation);
    }

    private static BinaryOp ReverseComparison(BinaryOp op)
    {
        return op switch
        {
            BinaryOp.LessThan => BinaryOp.GreaterThan,
            BinaryOp.GreaterThan => BinaryOp.LessThan,
            BinaryOp.LessOrEqual => BinaryOp.GreaterOrEqual,
            BinaryOp.GreaterOrEqual => BinaryOp.LessOrEqual,
            _ => op,
        };
    }

    private static bool TryConvertLiteral(LiteralExpression lit, out DbValue value)
    {
        switch (lit.LiteralType)
        {
            case TokenType.Null:
                value = DbValue.Null;
                return true;
            case TokenType.IntegerLiteral when lit.Value is long longValue:
                value = DbValue.FromInteger(longValue);
                return true;
            case TokenType.RealLiteral when lit.Value is double doubleValue:
                value = DbValue.FromReal(doubleValue);
                return true;
            case TokenType.StringLiteral when lit.Value is string stringValue:
                value = DbValue.FromText(stringValue);
                return true;
            default:
                value = DbValue.Null;
                return false;
        }
    }

    #endregion

    #region Index Maintenance Helpers

    private async ValueTask InsertIntoAllIndexesAsync(
        IReadOnlyList<IndexSchema> indexes, TableSchema schema, DbValue[] row, long rowId, CancellationToken ct)
    {
        var tableTree = _catalog.GetTableTree(schema.TableName, _pager);

        foreach (var idx in indexes)
        {
            if (idx.Kind == IndexKind.FullText)
            {
                await FullTextIndexMaintenance.InsertAsync(_catalog, schema, idx, row, rowId, ct);
                continue;
            }

            if (idx.Kind is not (IndexKind.Sql or IndexKind.ForeignKeyInternal))
                continue;

            if (!IndexMaintenanceHelper.TryResolveIndexColumnIndices(idx, schema, out var columnIndices, out bool usesDirectIntegerKey))
                continue;
            string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(idx, schema, columnIndices);
            SqlIndexStorageMode storageMode = IndexMaintenanceHelper.ResolveSqlIndexStorageMode(idx, schema);
            if (!IndexMaintenanceHelper.TryBuildIndexKey(
                    row,
                    columnIndices,
                    indexColumnCollations,
                    usesDirectIntegerKey,
                    storageMode,
                    out long indexKey,
                    out DbValue[]? keyComponents))
            {
                continue; // Don't index entries that include NULL.
            }

            var indexStore = _catalog.GetIndexStore(idx.IndexName);

            if (idx.IsUnique)
            {
                if (usesDirectIntegerKey)
                {
                    var existing = await indexStore.FindAsync(indexKey, ct);
                    if (existing != null)
                        throw new CSharpDbException(ErrorCode.ConstraintViolation,
                            $"Duplicate key value in unique index '{idx.IndexName}'.");

                    var payload = new byte[8];
                    BitConverter.TryWriteBytes(payload, rowId);
                    await indexStore.InsertAsync(indexKey, payload, ct);
                }
                else
                {
                    await IndexMaintenanceHelper.EnsureUniqueConstraintAsync(
                        indexStore,
                        tableTree,
                        schema,
                        GetReadSerializer(schema),
                        columnIndices,
                        indexColumnCollations,
                        keyComponents!,
                        indexKey,
                        storageMode,
                        idx.IndexName,
                        ct);

                    await IndexMaintenanceHelper.InsertRowIdAsync(indexStore, indexKey, rowId, keyComponents, storageMode, ct);
                }
            }
            else
            {
                await IndexMaintenanceHelper.InsertRowIdAsync(indexStore, indexKey, rowId, keyComponents, storageMode, ct);
            }
        }
    }

    private async ValueTask DeleteFromAllIndexesAsync(
        IReadOnlyList<IndexSchema> indexes, TableSchema schema, DbValue[] row, long rowId, CancellationToken ct)
    {
        foreach (var idx in indexes)
        {
            if (idx.Kind == IndexKind.FullText)
            {
                await FullTextIndexMaintenance.DeleteAsync(_catalog, schema, idx, row, rowId, ct);
                continue;
            }

            if (idx.Kind is not (IndexKind.Sql or IndexKind.ForeignKeyInternal))
                continue;

            if (!IndexMaintenanceHelper.TryResolveIndexColumnIndices(idx, schema, out var columnIndices, out bool usesDirectIntegerKey))
                continue;
            string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(idx, schema, columnIndices);
            SqlIndexStorageMode storageMode = IndexMaintenanceHelper.ResolveSqlIndexStorageMode(idx, schema);
            if (!IndexMaintenanceHelper.TryBuildIndexKey(
                    row,
                    columnIndices,
                    indexColumnCollations,
                    usesDirectIntegerKey,
                    storageMode,
                    out long indexKey,
                    out DbValue[]? keyComponents))
            {
                continue;
            }

            var indexStore = _catalog.GetIndexStore(idx.IndexName);
            await IndexMaintenanceHelper.DeleteRowIdAsync(indexStore, indexKey, rowId, keyComponents, storageMode, ct);
        }
    }

    private async ValueTask UpdateAllIndexesAsync(
        IReadOnlyList<IndexSchema> indexes, TableSchema schema,
        DbValue[] oldRow, DbValue[] newRow, long oldRowId, long newRowId, CancellationToken ct)
    {
        var tableTree = _catalog.GetTableTree(schema.TableName, _pager);

        foreach (var idx in indexes)
        {
            if (idx.Kind == IndexKind.FullText)
            {
                await FullTextIndexMaintenance.UpdateAsync(_catalog, schema, idx, oldRow, newRow, oldRowId, newRowId, ct);
                continue;
            }

            if (idx.Kind is not (IndexKind.Sql or IndexKind.ForeignKeyInternal))
                continue;

            if (!IndexMaintenanceHelper.TryResolveIndexColumnIndices(idx, schema, out var columnIndices, out bool usesDirectIntegerKey))
                continue;
            string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(idx, schema, columnIndices);
            SqlIndexStorageMode storageMode = IndexMaintenanceHelper.ResolveSqlIndexStorageMode(idx, schema);

            bool hasOldKey = IndexMaintenanceHelper.TryBuildIndexKey(
                oldRow,
                columnIndices,
                indexColumnCollations,
                usesDirectIntegerKey,
                storageMode,
                out long oldKey,
                out DbValue[]? oldComponents);
            bool hasNewKey = IndexMaintenanceHelper.TryBuildIndexKey(
                newRow,
                columnIndices,
                indexColumnCollations,
                usesDirectIntegerKey,
                storageMode,
                out long newKey,
                out DbValue[]? newComponents);

            // If neither index presence, key value, nor rowid changed, no maintenance needed.
            if (hasOldKey == hasNewKey &&
                oldRowId == newRowId &&
                (!hasOldKey || (oldKey == newKey && IndexMaintenanceHelper.IndexKeyComponentsEqual(oldComponents, newComponents))))
            {
                continue;
            }

            var indexStore = _catalog.GetIndexStore(idx.IndexName);

            // Remove old entry.
            if (hasOldKey)
            {
                await IndexMaintenanceHelper.DeleteRowIdAsync(indexStore, oldKey, oldRowId, oldComponents, storageMode, ct);
            }

            // Add new entry.
            if (hasNewKey)
            {
                if (idx.IsUnique)
                {
                    if (usesDirectIntegerKey)
                    {
                        var existing = await indexStore.FindAsync(newKey, ct);
                        if (existing != null)
                            throw new CSharpDbException(ErrorCode.ConstraintViolation,
                                $"Duplicate key value in unique index '{idx.IndexName}'.");

                        var payload = new byte[8];
                        BitConverter.TryWriteBytes(payload, newRowId);
                        await indexStore.InsertAsync(newKey, payload, ct);
                    }
                    else
                    {
                        await IndexMaintenanceHelper.EnsureUniqueConstraintAsync(
                            indexStore,
                            tableTree,
                            schema,
                            GetReadSerializer(schema),
                            columnIndices,
                            indexColumnCollations,
                            newComponents!,
                            newKey,
                            storageMode,
                            idx.IndexName,
                            ct);

                        await IndexMaintenanceHelper.InsertRowIdAsync(indexStore, newKey, newRowId, newComponents, storageMode, ct);
                    }
                }
                else
                {
                    await IndexMaintenanceHelper.InsertRowIdAsync(indexStore, newKey, newRowId, newComponents, storageMode, ct);
                }
            }
        }
    }

    /// <summary>
    /// Inserts a rowid into a non-unique index entry. The index stores a list of rowids as the payload.
    /// </summary>
    private static async ValueTask InsertIntoIndexAsync(IIndexStore indexStore, long indexKey, long rowId, CancellationToken ct)
    {
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing != null)
        {
            // Append rowId to existing list
            var newPayload = new byte[existing.Length + 8];
            existing.CopyTo(newPayload, 0);
            BitConverter.TryWriteBytes(newPayload.AsSpan(existing.Length), rowId);
            await indexStore.ReplaceAsync(indexKey, newPayload, ct);
        }
        else
        {
            var payload = new byte[8];
            BitConverter.TryWriteBytes(payload, rowId);
            await indexStore.InsertAsync(indexKey, payload, ct);
        }
    }

    /// <summary>
    /// Removes a rowid from an index entry. If it was the last rowid, deletes the entire entry.
    /// </summary>
    private static async ValueTask DeleteFromIndexAsync(IIndexStore indexStore, long indexKey, long rowId, CancellationToken ct)
    {
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null) return;

        int count = existing.Length / 8;
        if (count <= 1)
        {
            await indexStore.DeleteAsync(indexKey, ct);
            return;
        }

        // Remove the specific rowId from the list
        var ms = new MemoryStream();
        for (int i = 0; i < count; i++)
        {
            long id = BitConverter.ToInt64(existing, i * 8);
            if (id != rowId)
                ms.Write(BitConverter.GetBytes(id));
        }

        if (ms.Length > 0)
            await indexStore.ReplaceAsync(indexKey, ms.ToArray(), ct);
        else
            await indexStore.DeleteAsync(indexKey, ct);
    }

    #endregion

    #region Helpers

    private static bool ContainsAggregate(Expression expr)
    {
        return expr switch
        {
            FunctionCallExpression func => ScalarFunctionEvaluator.IsAggregateFunction(func.FunctionName)
                || func.Arguments.Any(ContainsAggregate),
            BinaryExpression bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
            UnaryExpression un => ContainsAggregate(un.Operand),
            CollateExpression collate => ContainsAggregate(collate.Operand),
            _ => false,
        };
    }

    private static void TrySetDecodedColumnUpperBound(IOperator op, int maxColumnIndex)
    {
        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case PrimaryKeyLookupOperator pkLookup:
                pkLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
            case IndexOrderedScanOperator indexOrderedScan:
                indexOrderedScan.SetDecodedColumnUpperBound(maxColumnIndex);
                break;
        }
    }

    private static bool TrySetDecodedColumnIndices(IOperator op, int[] columnIndices)
    {
        switch (op)
        {
            case TableScanOperator tableScan:
                tableScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexScanOperator indexScan:
                indexScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case UniqueIndexLookupOperator uniqueIndexLookup:
                uniqueIndexLookup.SetDecodedColumnIndices(columnIndices);
                return true;
            case IndexOrderedScanOperator indexOrderedScan:
                indexOrderedScan.SetDecodedColumnIndices(columnIndices);
                return true;
            case PrimaryKeyLookupOperator primaryKeyLookup:
                primaryKeyLookup.SetDecodedColumnIndices(columnIndices);
                return true;
        }

        return false;
    }

    private static int[] ToSortedColumnIndices(HashSet<int> columnSet)
    {
        if (columnSet.Count == 0)
            return Array.Empty<int>();

        var columns = new int[columnSet.Count];
        columnSet.CopyTo(columns);
        Array.Sort(columns);
        return columns;
    }

    private static TableSchema CreateSimpleTableQuerySchema(TableSchema schema, string? alias)
    {
        if (string.IsNullOrWhiteSpace(alias))
            return schema;

        var qualifiedMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (schema.QualifiedMappings != null)
        {
            foreach (var (key, index) in schema.QualifiedMappings)
                qualifiedMappings[key] = index;
        }

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            string columnName = schema.Columns[i].Name;
            qualifiedMappings[$"{alias}.{columnName}"] = i;
            qualifiedMappings[$"{schema.TableName}.{columnName}"] = i;
        }

        return new TableSchema
        {
            TableName = schema.TableName,
            Columns = schema.Columns,
            NextRowId = schema.NextRowId,
            QualifiedMappings = qualifiedMappings,
        };
    }

    private static TableSchema CreateCompactProjectionSchema(TableSchema schema, ReadOnlySpan<int> columnIndices)
    {
        if (columnIndices.Length == 0)
        {
            return new TableSchema
            {
                TableName = schema.TableName,
                Columns = Array.Empty<ColumnDefinition>(),
                NextRowId = schema.NextRowId,
            };
        }

        var columns = new ColumnDefinition[columnIndices.Length];
        var remappedIndices = new Dictionary<int, int>(columnIndices.Length);
        for (int i = 0; i < columnIndices.Length; i++)
        {
            columns[i] = schema.Columns[columnIndices[i]];
            remappedIndices[columnIndices[i]] = i;
        }

        Dictionary<string, int>? qualifiedMappings = null;
        if (schema.QualifiedMappings != null)
        {
            qualifiedMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var (key, originalIndex) in schema.QualifiedMappings)
            {
                if (remappedIndices.TryGetValue(originalIndex, out int compactIndex))
                    qualifiedMappings[key] = compactIndex;
            }
        }

        return new TableSchema
        {
            TableName = schema.TableName,
            Columns = columns,
            NextRowId = schema.NextRowId,
            QualifiedMappings = qualifiedMappings,
        };
    }

    private static bool TryGetAggregateDecodeColumnIndices(
        SelectStatement stmt,
        TableSchema schema,
        Expression? whereExpr,
        out int[] columnIndices)
    {
        var referencedColumns = new HashSet<int>();

        if (whereExpr != null && !TryAccumulateReferencedColumns(whereExpr, schema, referencedColumns))
        {
            columnIndices = Array.Empty<int>();
            return false;
        }

        if (stmt.Having != null && !TryAccumulateReferencedColumns(stmt.Having, schema, referencedColumns))
        {
            columnIndices = Array.Empty<int>();
            return false;
        }

        if (stmt.GroupBy != null)
        {
            for (int i = 0; i < stmt.GroupBy.Count; i++)
            {
                if (!TryAccumulateReferencedColumns(stmt.GroupBy[i], schema, referencedColumns))
                {
                    columnIndices = Array.Empty<int>();
                    return false;
                }
            }
        }

        for (int i = 0; i < stmt.Columns.Count; i++)
        {
            var col = stmt.Columns[i];
            if (col.IsStar || col.Expression == null)
                continue;

            if (!TryAccumulateReferencedColumns(col.Expression, schema, referencedColumns))
            {
                columnIndices = Array.Empty<int>();
                return false;
            }
        }

        columnIndices = ToSortedColumnIndices(referencedColumns);
        return true;
    }

    private static bool TryGetProjectionDecodeColumnIndices(
        SelectStatement stmt,
        TableSchema schema,
        Expression? whereExpr,
        bool includeOrderBy,
        out int[] columnIndices)
    {
        if (stmt.Columns.Any(c => c.IsStar))
        {
            columnIndices = Array.Empty<int>();
            return false;
        }

        var referencedColumns = new HashSet<int>();

        if (whereExpr != null && !TryAccumulateReferencedColumns(whereExpr, schema, referencedColumns))
        {
            columnIndices = Array.Empty<int>();
            return false;
        }

        if (includeOrderBy && stmt.OrderBy != null)
        {
            for (int i = 0; i < stmt.OrderBy.Count; i++)
            {
                if (!TryAccumulateReferencedColumns(stmt.OrderBy[i].Expression, schema, referencedColumns))
                {
                    columnIndices = Array.Empty<int>();
                    return false;
                }
            }
        }

        for (int i = 0; i < stmt.Columns.Count; i++)
        {
            var expression = stmt.Columns[i].Expression;
            if (expression == null)
                continue;

            if (!TryAccumulateReferencedColumns(expression, schema, referencedColumns))
            {
                columnIndices = Array.Empty<int>();
                return false;
            }
        }

        columnIndices = ToSortedColumnIndices(referencedColumns);
        return true;
    }

    private static bool TryGetAggregateDecodeUpperBound(
        SelectStatement stmt,
        TableSchema schema,
        Expression? whereExpr,
        out int maxColumnIndex)
    {
        maxColumnIndex = -1;

        if (whereExpr != null && !TryAccumulateMaxReferencedColumn(whereExpr, schema, ref maxColumnIndex))
            return false;

        if (stmt.Having != null && !TryAccumulateMaxReferencedColumn(stmt.Having, schema, ref maxColumnIndex))
            return false;

        if (stmt.GroupBy != null)
        {
            for (int i = 0; i < stmt.GroupBy.Count; i++)
            {
                if (!TryAccumulateMaxReferencedColumn(stmt.GroupBy[i], schema, ref maxColumnIndex))
                    return false;
            }
        }

        for (int i = 0; i < stmt.Columns.Count; i++)
        {
            var col = stmt.Columns[i];
            if (col.IsStar || col.Expression == null) continue;
            if (!TryAccumulateMaxReferencedColumn(col.Expression, schema, ref maxColumnIndex))
                return false;
        }

        return true;
    }

    private static bool TryGetProjectionDecodeUpperBound(
        SelectStatement stmt,
        TableSchema schema,
        Expression? whereExpr,
        bool includeOrderBy,
        out int maxColumnIndex)
    {
        maxColumnIndex = -1;

        // SELECT * needs full row materialization.
        if (stmt.Columns.Any(c => c.IsStar))
            return false;

        if (whereExpr != null && !TryAccumulateMaxReferencedColumn(whereExpr, schema, ref maxColumnIndex))
            return false;

        if (includeOrderBy && stmt.OrderBy != null)
        {
            for (int i = 0; i < stmt.OrderBy.Count; i++)
            {
                if (!TryAccumulateMaxReferencedColumn(stmt.OrderBy[i].Expression, schema, ref maxColumnIndex))
                    return false;
            }
        }

        for (int i = 0; i < stmt.Columns.Count; i++)
        {
            var expression = stmt.Columns[i].Expression;
            if (expression == null)
                continue;

            if (!TryAccumulateMaxReferencedColumn(expression, schema, ref maxColumnIndex))
                return false;
        }

        return true;
    }

    private static bool TryAccumulateMaxReferencedColumn(Expression expr, TableSchema schema, ref int maxColumnIndex)
    {
        switch (expr)
        {
            case LiteralExpression:
                return true;
            case ColumnRefExpression col:
            {
                int idx = col.TableAlias != null
                    ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                    : schema.GetColumnIndex(col.ColumnName);

                if (idx < 0)
                    return false;

                if (idx > maxColumnIndex)
                    maxColumnIndex = idx;
                return true;
            }
            case BinaryExpression bin:
                return TryAccumulateMaxReferencedColumn(bin.Left, schema, ref maxColumnIndex)
                    && TryAccumulateMaxReferencedColumn(bin.Right, schema, ref maxColumnIndex);
            case UnaryExpression un:
                return TryAccumulateMaxReferencedColumn(un.Operand, schema, ref maxColumnIndex);
            case CollateExpression collate:
                return TryAccumulateMaxReferencedColumn(collate.Operand, schema, ref maxColumnIndex);
            case LikeExpression like:
                return TryAccumulateMaxReferencedColumn(like.Operand, schema, ref maxColumnIndex)
                    && TryAccumulateMaxReferencedColumn(like.Pattern, schema, ref maxColumnIndex)
                    && (like.EscapeChar == null || TryAccumulateMaxReferencedColumn(like.EscapeChar, schema, ref maxColumnIndex));
            case InExpression inExpr:
            {
                if (!TryAccumulateMaxReferencedColumn(inExpr.Operand, schema, ref maxColumnIndex))
                    return false;
                foreach (var value in inExpr.Values)
                {
                    if (!TryAccumulateMaxReferencedColumn(value, schema, ref maxColumnIndex))
                        return false;
                }
                return true;
            }
            case BetweenExpression between:
                return TryAccumulateMaxReferencedColumn(between.Operand, schema, ref maxColumnIndex)
                    && TryAccumulateMaxReferencedColumn(between.Low, schema, ref maxColumnIndex)
                    && TryAccumulateMaxReferencedColumn(between.High, schema, ref maxColumnIndex);
            case IsNullExpression isNull:
                return TryAccumulateMaxReferencedColumn(isNull.Operand, schema, ref maxColumnIndex);
            case FunctionCallExpression func:
            {
                if (func.IsStarArg) return true;
                foreach (var arg in func.Arguments)
                {
                    if (!TryAccumulateMaxReferencedColumn(arg, schema, ref maxColumnIndex))
                        return false;
                }
                return true;
            }
            default:
                return false;
        }
    }

    private static ColumnDefinition InferColumnDef(Expression expr, string? alias, TableSchema schema, int index)
    {
        if (alias != null)
            return new ColumnDefinition { Name = alias, Type = DbType.Null, Nullable = true };

        if (expr is CollateExpression collate)
            return InferColumnDef(collate.Operand, alias, schema, index);

        if (expr is ColumnRefExpression colRef)
        {
            int idx = schema.GetColumnIndex(colRef.ColumnName);
            if (idx >= 0) return schema.Columns[idx];
            return new ColumnDefinition { Name = colRef.ColumnName, Type = DbType.Null, Nullable = true };
        }

        if (expr is FunctionCallExpression func)
        {
            string name = func.IsStarArg
                ? $"{func.FunctionName}(*)"
                : $"{func.FunctionName}({(func.IsDistinct ? "DISTINCT " : "")}{func.Arguments[0]})";
            return new ColumnDefinition { Name = name, Type = DbType.Null, Nullable = true };
        }

        return new ColumnDefinition { Name = $"expr{index}", Type = DbType.Null, Nullable = true };
    }

    private static bool TryBuildColumnProjection(
        IReadOnlyList<SelectColumn> columns,
        TableSchema schema,
        out int[] columnIndices,
        out ColumnDefinition[] outputColumns)
    {
        columnIndices = new int[columns.Count];
        outputColumns = new ColumnDefinition[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            if (column.IsStar || column.Expression is not ColumnRefExpression colRef)
                return false;

            int sourceIndex = colRef.TableAlias != null
                ? schema.GetQualifiedColumnIndex(colRef.TableAlias, colRef.ColumnName)
                : schema.GetColumnIndex(colRef.ColumnName);
            if (sourceIndex < 0 || sourceIndex >= schema.Columns.Count)
                return false;

            columnIndices[i] = sourceIndex;
            var sourceColumn = schema.Columns[sourceIndex];
            outputColumns[i] = column.Alias != null
                ? new ColumnDefinition
                {
                    Name = column.Alias,
                    Type = sourceColumn.Type,
                    Nullable = sourceColumn.Nullable,
                    IsPrimaryKey = sourceColumn.IsPrimaryKey,
                    IsIdentity = sourceColumn.IsIdentity,
                }
                : sourceColumn;
        }

        return true;
    }

    private static bool TryBuildSimpleLookupProjection(
        IReadOnlyList<string> projectionColumns,
        TableSchema schema,
        out int[] columnIndices,
        out ColumnDefinition[] outputColumns)
    {
        columnIndices = new int[projectionColumns.Count];
        outputColumns = new ColumnDefinition[projectionColumns.Count];

        for (int i = 0; i < projectionColumns.Count; i++)
        {
            string columnName = projectionColumns[i];
            int sourceIndex = schema.GetColumnIndex(columnName);
            if (sourceIndex < 0 || sourceIndex >= schema.Columns.Count)
                return false;

            columnIndices[i] = sourceIndex;
            outputColumns[i] = schema.Columns[sourceIndex];
        }

        return true;
    }

    private static bool TryAccumulateReferencedColumns(
        Expression expr,
        TableSchema schema,
        HashSet<int> referencedColumns)
    {
        switch (expr)
        {
            case LiteralExpression:
                return true;
            case ColumnRefExpression col:
            {
                int idx = col.TableAlias != null
                    ? schema.GetQualifiedColumnIndex(col.TableAlias, col.ColumnName)
                    : schema.GetColumnIndex(col.ColumnName);

                if (idx < 0)
                    return false;

                referencedColumns.Add(idx);
                return true;
            }
            case BinaryExpression bin:
                return TryAccumulateReferencedColumns(bin.Left, schema, referencedColumns)
                    && TryAccumulateReferencedColumns(bin.Right, schema, referencedColumns);
            case UnaryExpression un:
                return TryAccumulateReferencedColumns(un.Operand, schema, referencedColumns);
            case CollateExpression collate:
                return TryAccumulateReferencedColumns(collate.Operand, schema, referencedColumns);
            case LikeExpression like:
                return TryAccumulateReferencedColumns(like.Operand, schema, referencedColumns)
                    && TryAccumulateReferencedColumns(like.Pattern, schema, referencedColumns)
                    && (like.EscapeChar == null || TryAccumulateReferencedColumns(like.EscapeChar, schema, referencedColumns));
            case InExpression inExpr:
            {
                if (!TryAccumulateReferencedColumns(inExpr.Operand, schema, referencedColumns))
                    return false;
                foreach (var value in inExpr.Values)
                {
                    if (!TryAccumulateReferencedColumns(value, schema, referencedColumns))
                        return false;
                }

                return true;
            }
            case BetweenExpression between:
                return TryAccumulateReferencedColumns(between.Operand, schema, referencedColumns)
                    && TryAccumulateReferencedColumns(between.Low, schema, referencedColumns)
                    && TryAccumulateReferencedColumns(between.High, schema, referencedColumns);
            case IsNullExpression isNull:
                return TryAccumulateReferencedColumns(isNull.Operand, schema, referencedColumns);
            case FunctionCallExpression func:
            {
                if (func.IsStarArg)
                    return true;

                foreach (var arg in func.Arguments)
                {
                    if (!TryAccumulateReferencedColumns(arg, schema, referencedColumns))
                        return false;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private static bool TryResolveUnaliasedPrimaryKeyProjectionCount(
        IReadOnlyList<SelectColumn> columns,
        TableSchema schema,
        int primaryKeyIndex,
        out int projectedColumnCount)
    {
        projectedColumnCount = 0;

        if (primaryKeyIndex < 0 || columns.Count == 0)
            return false;

        for (int i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            if (column.IsStar || column.Alias != null || column.Expression is not ColumnRefExpression colRef)
                return false;

            int sourceIndex = colRef.TableAlias != null
                ? schema.GetQualifiedColumnIndex(colRef.TableAlias, colRef.ColumnName)
                : schema.GetColumnIndex(colRef.ColumnName);
            if (sourceIndex != primaryKeyIndex)
                return false;
        }

        projectedColumnCount = columns.Count;
        return true;
    }

    private ColumnDefinition[] GetSchemaColumnsArray(TableSchema schema)
    {
        if (schema.Columns is ColumnDefinition[] columnsArray)
            return columnsArray;

        if (_tableSchemaArrayCache.TryGetValue(schema, out var cached))
            return cached;

        var created = schema.Columns.ToArray();
        _tableSchemaArrayCache[schema] = created;
        return created;
    }

    private ColumnDefinition[] GetSingleColumnOutputSchema(TableSchema schema, int columnIndex)
    {
        var key = (schema, columnIndex);
        if (_singleColumnOutputSchemaCache.TryGetValue(key, out var cached))
            return cached;

        var created = new[] { schema.Columns[columnIndex] };
        _singleColumnOutputSchemaCache[key] = created;
        return created;
    }

    private static ColumnDefinition[] BuildRepeatedColumnOutputSchema(ColumnDefinition column, int count)
    {
        var output = new ColumnDefinition[count];
        Array.Fill(output, column);
        return output;
    }

    private static bool IsPrimaryKeyOnlyProjection(int[] columnIndices, int primaryKeyIndex)
    {
        if (primaryKeyIndex < 0)
            return false;

        for (int i = 0; i < columnIndices.Length; i++)
        {
            if (columnIndices[i] != primaryKeyIndex)
                return false;
        }

        return true;
    }

    private static bool IsCoveredLookupProjection(
        int[] columnIndices,
        int primaryKeyIndex,
        int predicateColumnIndex,
        bool canProjectPrimaryKey)
    {
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex == predicateColumnIndex)
                continue;

            if (canProjectPrimaryKey && columnIndex == primaryKeyIndex)
                continue;

            return false;
        }

        return true;
    }

    private static Expression? BuildResidualTermsExcludingLookupKeyTerms(
        IReadOnlyList<Expression> conjuncts,
        TableSchema schema,
        ReadOnlySpan<int> keyColumnIndices,
        ReadOnlySpan<DbValue> keyComponents)
    {
        if (conjuncts.Count == 0)
            return null;

        var residualTerms = new List<Expression>(conjuncts.Count);
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (TryExtractIndexEqualityLookupTerm(
                    conjuncts[i],
                    schema,
                    out int columnIndex,
                    out DbValue literal,
                    out string? queryCollation))
            {
                bool matchesLookupKey = false;
                for (int componentIndex = 0; componentIndex < keyColumnIndices.Length; componentIndex++)
                {
                    if (keyColumnIndices[componentIndex] != columnIndex)
                        continue;

                    if (CollationSupport.Compare(keyComponents[componentIndex], literal, queryCollation) == 0)
                    {
                        matchesLookupKey = true;
                        break;
                    }
                }

                if (matchesLookupKey)
                    continue;
            }

            residualTerms.Add(conjuncts[i]);
        }

        return CombineConjuncts(residualTerms);
    }

    private static bool CanUseCoveredOrderedIndexScanWithoutLimit(
        SelectStatement stmt,
        TableSchema schema,
        int keyColumnIndex)
    {
        if (stmt.Columns.Any(c => c.IsStar))
            return false;

        if (!TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out _))
            return false;

        return CanProjectIntegerPrimaryKeyOrKeyColumn(columnIndices, schema, keyColumnIndex);
    }

    private static bool TryBuildCoveredOrderedIndexProjectionOperator(
        IndexOrderedScanOperator orderedScan,
        TableSchema schema,
        int[] columnIndices,
        ColumnDefinition[] outputColumns,
        out IOperator projectionOperator)
    {
        projectionOperator = null!;

        if (!CanProjectIntegerPrimaryKeyOrKeyColumn(columnIndices, schema, orderedScan.KeyColumnIndex))
            return false;

        projectionOperator = new IndexOrderedProjectionScanOperator(
            orderedScan.IndexStore,
            orderedScan.ScanRange,
            outputColumns,
            columnIndices,
            schema.PrimaryKeyColumnIndex,
            orderedScan.KeyColumnIndex);
        return true;
    }

    private bool TryBuildCoveredHashedIndexProjectionOperator(
        IndexScanOperator indexScan,
        TableSchema schema,
        int[] columnIndices,
        ColumnDefinition[] outputColumns,
        out IOperator projectionOperator)
    {
        projectionOperator = null!;

        if (indexScan.UsesOrderedTextPayload)
            return false;

        if (indexScan.ExpectedKeyColumnIndices is not { Length: > 0 } keyColumnIndices ||
            indexScan.ExpectedKeyComponents is not { Length: > 0 } keyComponents)
        {
            return false;
        }

        if (!CanProjectPrimaryKeyOrKeyColumns(columnIndices, schema, keyColumnIndices))
            return false;

        projectionOperator = new HashedIndexProjectionLookupOperator(
            indexScan.IndexStore,
            indexScan.TableTree,
            schema,
            indexScan.SeekValue,
            outputColumns,
            columnIndices,
            keyColumnIndices,
            keyComponents,
            GetReadSerializer(schema));
        return true;
    }

    private static bool CanProjectIntegerPrimaryKeyOrKeyColumn(
        int[] columnIndices,
        TableSchema schema,
        int keyColumnIndex)
    {
        if (keyColumnIndex < 0 ||
            keyColumnIndex >= schema.Columns.Count ||
            schema.Columns[keyColumnIndex].Type != DbType.Integer)
        {
            return false;
        }

        int primaryKeyIndex = schema.PrimaryKeyColumnIndex;
        bool canProjectPrimaryKey = primaryKeyIndex >= 0 &&
            primaryKeyIndex < schema.Columns.Count &&
            schema.Columns[primaryKeyIndex].Type == DbType.Integer;

        return IsCoveredLookupProjection(
            columnIndices,
            primaryKeyIndex,
            keyColumnIndex,
            canProjectPrimaryKey);
    }

    private static bool CanProjectPrimaryKeyOrKeyColumns(
        int[] columnIndices,
        TableSchema schema,
        ReadOnlySpan<int> keyColumnIndices)
    {
        int primaryKeyIndex = schema.PrimaryKeyColumnIndex;
        bool canProjectPrimaryKey = primaryKeyIndex >= 0 &&
            primaryKeyIndex < schema.Columns.Count &&
            schema.Columns[primaryKeyIndex].Type == DbType.Integer;

        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (canProjectPrimaryKey && columnIndex == primaryKeyIndex)
                continue;

            bool matchesIndexedColumn = false;
            for (int j = 0; j < keyColumnIndices.Length; j++)
            {
                if (keyColumnIndices[j] == columnIndex)
                {
                    matchesIndexedColumn = true;
                    break;
                }
            }

            if (!matchesIndexedColumn)
                return false;
        }

        return true;
    }

    private Func<DbValue[], DbValue> GetOrCompileExpression(Expression expression, TableSchema schema)
    {
        bool requiresQualifiedMappings = RequiresQualifiedMappings(expression);
        var key = new CompiledExpressionCacheKey(
            expression,
            schema.Columns,
            requiresQualifiedMappings ? GetQualifiedMappingsFingerprint(schema) : null);

        if (_compiledExpressionCache.TryGetValue(key, out var evaluator))
            return evaluator;

        if (_compiledExpressionCache.Count >= MaxCompiledExpressionCacheEntries)
        {
            _compiledExpressionCache.Clear();
            _compiledSpanExpressionCache.Clear();
            _qualifiedMappingFingerprintCache.Clear();
            _requiresQualifiedMappingCache.Clear();
        }

        evaluator = ExpressionCompiler.Compile(expression, schema);
        _compiledExpressionCache[key] = evaluator;
        return evaluator;
    }

    private Func<DbValue[], DbValue>[] GetOrCompileExpressions(Expression[] expressions, TableSchema schema)
    {
        var evaluators = new Func<DbValue[], DbValue>[expressions.Length];
        for (int i = 0; i < expressions.Length; i++)
            evaluators[i] = GetOrCompileExpression(expressions[i], schema);
        return evaluators;
    }

    private SpanExpressionEvaluator GetOrCompileSpanExpression(Expression expression, TableSchema schema)
    {
        bool requiresQualifiedMappings = RequiresQualifiedMappings(expression);
        var key = new CompiledExpressionCacheKey(
            expression,
            schema.Columns,
            requiresQualifiedMappings ? GetQualifiedMappingsFingerprint(schema) : null);

        if (_compiledSpanExpressionCache.TryGetValue(key, out var evaluator))
            return evaluator;

        if (_compiledSpanExpressionCache.Count >= MaxCompiledExpressionCacheEntries)
        {
            _compiledExpressionCache.Clear();
            _compiledSpanExpressionCache.Clear();
            _qualifiedMappingFingerprintCache.Clear();
            _requiresQualifiedMappingCache.Clear();
        }

        evaluator = ExpressionCompiler.CompileSpan(expression, schema);
        _compiledSpanExpressionCache[key] = evaluator;
        return evaluator;
    }

    private SpanExpressionEvaluator[] GetOrCompileSpanExpressions(Expression[] expressions, TableSchema schema)
    {
        var evaluators = new SpanExpressionEvaluator[expressions.Length];
        for (int i = 0; i < expressions.Length; i++)
            evaluators[i] = GetOrCompileSpanExpression(expressions[i], schema);
        return evaluators;
    }

    private IFilterProjectionBatchPlan? TryCreateBatchPlan(
        IOperator source,
        Expression? predicate,
        Expression[] projections,
        TableSchema schema)
    {
        if (!IsBatchPlanEligibleSource(source))
            return null;

        var batchPlan = BatchPlanCompiler.TryCreate(predicate, projections, schema);
        ApplyBatchPlanPreDecodeFilters(source, batchPlan);
        return batchPlan;
    }

    private IFilterProjectionBatchPlan? TryCreateFilterBatchPlan(
        IOperator source,
        Expression? predicate,
        TableSchema schema)
    {
        if (predicate == null || !IsBatchPlanEligibleSource(source))
            return null;

        var batchPlan = BatchPlanCompiler.TryCreateFilter(predicate, schema);
        ApplyBatchPlanPreDecodeFilters(source, batchPlan);
        return batchPlan;
    }

    private static IFilterProjectionBatchPlan? TryCreateCompactBatchPlan(
        Expression? predicate,
        Expression[] projections,
        TableSchema schema)
        => BatchPlanCompiler.TryCreate(predicate, projections, schema);

    private static bool IsBatchPlanEligibleSource(IOperator source)
        => source is
            TableScanOperator or
            IndexScanOperator or
            IndexOrderedScanOperator or
            HashJoinOperator or
            IndexNestedLoopJoinOperator or
            HashedIndexNestedLoopJoinOperator or
            NestedLoopJoinOperator;

    private void ApplyBatchPlanPreDecodeFilters(IOperator source, IFilterProjectionBatchPlan? batchPlan)
    {
        if (source is not IPreDecodeFilterSupport preDecodeFilterTarget ||
            batchPlan?.PushdownFilters is not { Length: > 0 } pushdownFilters)
        {
            return;
        }

        for (int i = 0; i < pushdownFilters.Length; i++)
        {
            var filter = pushdownFilters[i];
            preDecodeFilterTarget.SetPreDecodeFilter(new PreDecodeFilterSpec(_recordSerializer, filter.ColumnIndex, filter));
        }
    }

    private string GetQualifiedMappingsFingerprint(TableSchema schema)
    {
        if (_qualifiedMappingFingerprintCache.TryGetValue(schema, out var fingerprint))
            return fingerprint;

        if (schema.QualifiedMappings is not { Count: > 0 } qualified)
        {
            fingerprint = string.Empty;
            _qualifiedMappingFingerprintCache[schema] = fingerprint;
            return fingerprint;
        }

        var sb = new StringBuilder();
        foreach (var pair in qualified)
            sb.Append(pair.Key).Append('=').Append(pair.Value).Append('|');

        fingerprint = sb.ToString();
        _qualifiedMappingFingerprintCache[schema] = fingerprint;
        return fingerprint;
    }

    private bool RequiresQualifiedMappings(Expression expression)
    {
        if (_requiresQualifiedMappingCache.TryGetValue(expression, out bool cached))
            return cached;

        bool computed = ComputeRequiresQualifiedMappings(expression);
        _requiresQualifiedMappingCache[expression] = computed;
        return computed;
    }

    private static bool ComputeRequiresQualifiedMappings(Expression expression)
    {
        return expression switch
        {
            ColumnRefExpression col => col.TableAlias != null,
            BinaryExpression bin => ComputeRequiresQualifiedMappings(bin.Left) || ComputeRequiresQualifiedMappings(bin.Right),
            UnaryExpression un => ComputeRequiresQualifiedMappings(un.Operand),
            CollateExpression collate => ComputeRequiresQualifiedMappings(collate.Operand),
            LikeExpression like => ComputeRequiresQualifiedMappings(like.Operand)
                || ComputeRequiresQualifiedMappings(like.Pattern)
                || (like.EscapeChar != null && ComputeRequiresQualifiedMappings(like.EscapeChar)),
            InExpression inExpr => ComputeRequiresQualifiedMappings(inExpr.Operand)
                || inExpr.Values.Any(ComputeRequiresQualifiedMappings),
            BetweenExpression between => ComputeRequiresQualifiedMappings(between.Operand)
                || ComputeRequiresQualifiedMappings(between.Low)
                || ComputeRequiresQualifiedMappings(between.High),
            IsNullExpression isNull => ComputeRequiresQualifiedMappings(isNull.Operand),
            FunctionCallExpression call => call.Arguments.Any(ComputeRequiresQualifiedMappings),
            _ => false,
        };
    }

    private readonly record struct CompiledExpressionCacheKey(
        Expression Expression,
        IReadOnlyList<ColumnDefinition> Columns,
        string? QualifiedMappingsFingerprint)
    {
        public bool Equals(CompiledExpressionCacheKey other) =>
            ReferenceEquals(Expression, other.Expression) &&
            ReferenceEquals(Columns, other.Columns) &&
            string.Equals(QualifiedMappingsFingerprint, other.QualifiedMappingsFingerprint, StringComparison.Ordinal);

        public override int GetHashCode() =>
            HashCode.Combine(
                RuntimeHelpers.GetHashCode(Expression),
                RuntimeHelpers.GetHashCode(Columns),
                QualifiedMappingsFingerprint != null
                    ? StringComparer.Ordinal.GetHashCode(QualifiedMappingsFingerprint)
                    : 0);
    }

    private static bool IsSystemCatalogTable(string tableName) =>
        TryNormalizeSystemCatalogTableName(tableName, out _);

    private static bool TryNormalizeSystemCatalogTableName(string tableName, out string normalized)
    {
        if (string.Equals(tableName, "sys.tables", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_tables", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.tables";
            return true;
        }

        if (string.Equals(tableName, "sys.columns", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_columns", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.columns";
            return true;
        }

        if (string.Equals(tableName, "sys.indexes", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_indexes", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.indexes";
            return true;
        }

        if (string.Equals(tableName, "sys.foreign_keys", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_foreign_keys", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.foreign_keys";
            return true;
        }

        if (string.Equals(tableName, "sys.views", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_views", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.views";
            return true;
        }

        if (string.Equals(tableName, "sys.triggers", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_triggers", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.triggers";
            return true;
        }

        if (string.Equals(tableName, "sys.objects", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_objects", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.objects";
            return true;
        }

        if (string.Equals(tableName, "sys.saved_queries", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_saved_queries", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.saved_queries";
            return true;
        }

        if (string.Equals(tableName, "sys.table_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_table_stats", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.table_stats";
            return true;
        }

        if (string.Equals(tableName, "sys.column_stats", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(tableName, "sys_column_stats", StringComparison.OrdinalIgnoreCase))
        {
            normalized = "sys.column_stats";
            return true;
        }

        normalized = string.Empty;
        return false;
    }

    private bool TryBuildSystemCatalogSource(SimpleTableRef tableRef, out (IOperator op, TableSchema schema) source)
    {
        source = default;
        if (!TryNormalizeSystemCatalogTableName(tableRef.TableName, out string normalized))
            return false;

        ColumnDefinition[] columns;
        List<DbValue[]> rows;

        switch (normalized)
        {
            case "sys.tables":
                columns = SystemTablesColumns;
                rows = BuildSystemTablesRows();
                break;

            case "sys.columns":
                columns = SystemColumnsColumns;
                rows = BuildSystemColumnsRows();
                break;

            case "sys.indexes":
                columns = SystemIndexesColumns;
                rows = BuildSystemIndexesRows();
                break;

            case "sys.foreign_keys":
                columns = SystemForeignKeysColumns;
                rows = BuildSystemForeignKeysRows();
                break;

            case "sys.views":
                columns = SystemViewsColumns;
                rows = BuildSystemViewsRows();
                break;

            case "sys.triggers":
                columns = SystemTriggersColumns;
                rows = BuildSystemTriggersRows();
                break;

            case "sys.objects":
                columns = SystemObjectsColumns;
                rows = BuildSystemObjectsRows();
                break;

            case "sys.saved_queries":
                if (_catalog.GetTable(InternalSavedQueriesTableName) is TableSchema savedQueriesSchema)
                {
                    var tableTree = _catalog.GetTableTree(InternalSavedQueriesTableName, _pager);
                    var scanOp = new TableScanOperator(
                        tableTree,
                        savedQueriesSchema,
                        _recordSerializer,
                        TryGetCachedTreeRowCountCapacityHint(tableTree));
                    var scanSchema = GetOrCreateSystemCatalogSchema(
                        normalized,
                        tableRef.TableName,
                        tableRef.Alias,
                        savedQueriesSchema.Columns.ToArray());
                    source = (scanOp, scanSchema);
                    return true;
                }

                columns = SystemSavedQueriesColumns;
                rows = new List<DbValue[]>();
                break;

            case "sys.table_stats":
                columns = SystemTableStatsColumns;
                rows = BuildSystemTableStatsRows();
                break;

            case "sys.column_stats":
                columns = SystemColumnStatsColumns;
                rows = BuildSystemColumnStatsRows();
                break;

            default:
                return false;
        }

        var op = new MaterializedOperator(rows, columns);
        var schema = GetOrCreateSystemCatalogSchema(normalized, tableRef.TableName, tableRef.Alias, columns);
        source = (op, schema);
        return true;
    }

    private TableSchema GetOrCreateSystemCatalogSchema(
        string normalizedName,
        string tableNameToken,
        string? alias,
        ColumnDefinition[] columns)
    {
        if (alias is null && _systemCatalogSchemaCache.TryGetValue(tableNameToken, out var cached))
            return cached;

        string qualifier = alias ?? tableNameToken;
        var qualified = new Dictionary<string, int>(columns.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Length; i++)
            qualified[$"{qualifier}.{columns[i].Name}"] = i;

        var schema = new TableSchema
        {
            TableName = normalizedName,
            Columns = columns,
            QualifiedMappings = qualified,
        };

        if (alias is null)
            _systemCatalogSchemaCache[tableNameToken] = schema;

        return schema;
    }

    private List<DbValue[]> BuildSystemTablesRows()
    {
        if (_systemTablesRowsCache != null)
            return _systemTablesRowsCache;

        var tableNames = _catalog.GetTableNames();
        var rows = new List<DbValue[]>(tableNames.Count);
        foreach (string tableName in tableNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            var schema = _catalog.GetTable(tableName);
            if (schema == null)
                continue;

            string? pkName = schema.PrimaryKeyColumnIndex >= 0
                ? schema.Columns[schema.PrimaryKeyColumnIndex].Name
                : null;

            rows.Add(
            [
                DbValue.FromText(tableName),
                DbValue.FromInteger(schema.Columns.Count),
                pkName is null ? DbValue.Null : DbValue.FromText(pkName),
            ]);
        }

        _systemTablesRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemColumnsRows()
    {
        if (_systemColumnsRowsCache != null)
            return _systemColumnsRowsCache;

        var tableNames = _catalog.GetTableNames();
        var rows = new List<DbValue[]>(tableNames.Count * 4);
        foreach (string tableName in tableNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            var schema = _catalog.GetTable(tableName);
            if (schema == null)
                continue;

            for (int i = 0; i < schema.Columns.Count; i++)
            {
                var col = schema.Columns[i];
                rows.Add(
                [
                    DbValue.FromText(tableName),
                    DbValue.FromText(col.Name),
                    DbValue.FromInteger(i + 1),
                    DbValue.FromText(col.Type.ToString().ToUpperInvariant()),
                    DbValue.FromInteger(col.Nullable ? 1 : 0),
                    DbValue.FromInteger(col.IsPrimaryKey ? 1 : 0),
                    DbValue.FromInteger(col.IsIdentity ? 1 : 0),
                    col.Collation is null ? DbValue.Null : DbValue.FromText(col.Collation),
                ]);
            }
        }

        _systemColumnsRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemIndexesRows()
    {
        if (_systemIndexesRowsCache != null)
            return _systemIndexesRowsCache;

        var indexes = _catalog.GetIndexes();
        var rows = new List<DbValue[]>(indexes.Count * 2);
        foreach (var index in indexes
                     .OrderBy(i => i.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (index.Kind == IndexKind.ForeignKeyInternal)
                continue;

            var tableSchema = _catalog.GetTable(index.TableName);
            for (int i = 0; i < index.Columns.Count; i++)
            {
                string? collation = index.ColumnCollations.Count > i
                    ? index.ColumnCollations[i]
                    : null;

                if (collation is null && tableSchema != null)
                {
                    int columnIndex = tableSchema.GetColumnIndex(index.Columns[i]);
                    if (columnIndex >= 0 && columnIndex < tableSchema.Columns.Count)
                        collation = tableSchema.Columns[columnIndex].Collation;
                }

                rows.Add(
                [
                    DbValue.FromText(index.IndexName),
                    DbValue.FromText(index.TableName),
                    DbValue.FromText(index.Columns[i]),
                    DbValue.FromInteger(i + 1),
                    DbValue.FromInteger(index.IsUnique ? 1 : 0),
                    collation is null ? DbValue.Null : DbValue.FromText(collation),
                ]);
            }
        }

        _systemIndexesRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemForeignKeysRows()
    {
        if (_systemForeignKeysRowsCache != null)
            return _systemForeignKeysRowsCache;

        var rows = new List<DbValue[]>((int)Math.Min(CountSystemForeignKeys(), int.MaxValue));
        foreach (string tableName in _catalog.GetTableNames().OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            TableSchema? schema = _catalog.GetTable(tableName);
            if (schema is null || schema.ForeignKeys.Count == 0)
                continue;

            foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys.OrderBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase))
            {
                rows.Add(
                [
                    DbValue.FromText(foreignKey.ConstraintName),
                    DbValue.FromText(tableName),
                    DbValue.FromText(foreignKey.ColumnName),
                    DbValue.FromText(foreignKey.ReferencedTableName),
                    DbValue.FromText(foreignKey.ReferencedColumnName),
                    DbValue.FromText(foreignKey.OnDelete.ToString().ToUpperInvariant()),
                    DbValue.FromText(foreignKey.SupportingIndexName),
                ]);
            }
        }

        _systemForeignKeysRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemViewsRows()
    {
        if (_systemViewsRowsCache != null)
            return _systemViewsRowsCache;

        var viewNames = _catalog.GetViewNames();
        var rows = new List<DbValue[]>(viewNames.Count);
        foreach (string viewName in viewNames.OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(viewName),
                DbValue.FromText(_catalog.GetViewSql(viewName) ?? string.Empty),
            ]);
        }

        _systemViewsRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemTriggersRows()
    {
        if (_systemTriggersRowsCache != null)
            return _systemTriggersRowsCache;

        var triggers = _catalog.GetTriggers();
        var rows = new List<DbValue[]>(triggers.Count);
        foreach (var trigger in triggers.OrderBy(t => t.TriggerName, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(trigger.TriggerName),
                DbValue.FromText(trigger.TableName),
                DbValue.FromText(trigger.Timing.ToString().ToUpperInvariant()),
                DbValue.FromText(trigger.Event.ToString().ToUpperInvariant()),
                DbValue.FromText(trigger.BodySql),
            ]);
        }

        _systemTriggersRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemObjectsRows()
    {
        if (_systemObjectsRowsCache != null)
            return _systemObjectsRowsCache;

        int capacity = _catalog.GetTableNames().Count
            + _catalog.GetIndexes().Count(index => index.Kind != IndexKind.ForeignKeyInternal)
            + (int)Math.Min(CountSystemForeignKeys(), int.MaxValue)
            + _catalog.GetViewNames().Count
            + _catalog.GetTriggers().Count;

        var rows = new List<DbValue[]>(capacity);

        foreach (string tableName in _catalog.GetTableNames().OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(tableName),
                DbValue.FromText("TABLE"),
                DbValue.Null,
            ]);
        }

        foreach (var index in _catalog.GetIndexes()
                     .OrderBy(i => i.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (index.Kind == IndexKind.ForeignKeyInternal)
                continue;

            rows.Add(
            [
                DbValue.FromText(index.IndexName),
                DbValue.FromText("INDEX"),
                DbValue.FromText(index.TableName),
            ]);
        }

        foreach (string tableName in _catalog.GetTableNames().OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            TableSchema? schema = _catalog.GetTable(tableName);
            if (schema is null || schema.ForeignKeys.Count == 0)
                continue;

            foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys.OrderBy(fk => fk.ConstraintName, StringComparer.OrdinalIgnoreCase))
            {
                rows.Add(
                [
                    DbValue.FromText(foreignKey.ConstraintName),
                    DbValue.FromText("FOREIGN KEY"),
                    DbValue.FromText(tableName),
                ]);
            }
        }

        foreach (string viewName in _catalog.GetViewNames().OrderBy(n => n, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(viewName),
                DbValue.FromText("VIEW"),
                DbValue.Null,
            ]);
        }

        foreach (var trigger in _catalog.GetTriggers().OrderBy(t => t.TriggerName, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(trigger.TriggerName),
                DbValue.FromText("TRIGGER"),
                DbValue.FromText(trigger.TableName),
            ]);
        }

        _systemObjectsRowsCache = rows;
        return rows;
    }

    private List<DbValue[]> BuildSystemTableStatsRows()
    {
        var tableStats = _catalog.GetTableStatistics();
        var rows = new List<DbValue[]>(tableStats.Count);
        foreach (var stats in tableStats.OrderBy(item => item.TableName, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(stats.TableName),
                DbValue.FromInteger(stats.RowCount),
                DbValue.FromInteger(stats.HasStaleColumns ? 1 : 0),
            ]);
        }

        return rows;
    }

    private List<DbValue[]> BuildSystemColumnStatsRows()
    {
        var columnStats = _catalog.GetColumnStatistics();
        var rows = new List<DbValue[]>(columnStats.Count);
        foreach (var stats in columnStats
                     .OrderBy(item => item.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => GetColumnOrdinal(item.TableName, item.ColumnName))
                     .ThenBy(item => item.ColumnName, StringComparer.OrdinalIgnoreCase))
        {
            rows.Add(
            [
                DbValue.FromText(stats.TableName),
                DbValue.FromText(stats.ColumnName),
                DbValue.FromInteger(GetColumnOrdinal(stats.TableName, stats.ColumnName)),
                DbValue.FromInteger(stats.DistinctCount),
                DbValue.FromInteger(stats.NonNullCount),
                stats.MinValue,
                stats.MaxValue,
                DbValue.FromInteger(stats.IsStale ? 1 : 0),
            ]);
        }

        return rows;
    }

    private int GetColumnOrdinal(string tableName, string columnName)
    {
        var schema = _catalog.GetTable(tableName);
        if (schema == null)
            return 0;

        int columnIndex = schema.GetColumnIndex(columnName);
        return columnIndex >= 0 ? columnIndex + 1 : 0;
    }

    private ColumnDefinition[] BuildAggregateOutputSchema(List<SelectColumn> columns, TableSchema schema)
    {
        var outputCols = new ColumnDefinition[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].IsStar)
            {
                outputCols[i] = new ColumnDefinition { Name = "*", Type = DbType.Null, Nullable = true };
            }
            else
            {
                outputCols[i] = InferColumnDef(columns[i].Expression!, columns[i].Alias, schema, i);
            }
        }
        return outputCols;
    }

    private TableSchema GetSchema(string tableName) =>
        _catalog.GetTable(tableName)
        ?? throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

    private IRecordSerializer GetReadSerializer(TableSchema schema)
        => _collectionReadSerializer != null && IsCollectionBackingSchema(schema)
            ? _collectionReadSerializer
            : _recordSerializer;

    private static bool IsCollectionBackingSchema(TableSchema schema)
    {
        if (!schema.TableName.StartsWith("_col_", StringComparison.Ordinal))
            return false;

        return schema.Columns.Count == 2
            && string.Equals(schema.Columns[0].Name, "_key", StringComparison.Ordinal)
            && schema.Columns[0].Type == DbType.Text
            && string.Equals(schema.Columns[1].Name, "_doc", StringComparison.Ordinal)
            && schema.Columns[1].Type == DbType.Text;
    }

    private static DbType MapType(TokenType token) => token switch
    {
        TokenType.Integer => DbType.Integer,
        TokenType.Real => DbType.Real,
        TokenType.Text => DbType.Text,
        TokenType.Blob => DbType.Blob,
        _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown type token: {token}"),
    };

    private static string? NormalizeCollationName(string? collation)
    {
        string? normalized = CollationSupport.NormalizeMetadataName(collation);
        if (!CollationSupport.IsSupported(normalized))
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Unsupported collation '{collation}'. Supported collations are {CollationSupport.DescribeSupportedCollations()}.");

        return normalized;
    }

    private static string? ValidateAndNormalizeColumnCollation(string columnName, TokenType typeToken, string? collation)
    {
        string? normalized = NormalizeCollationName(collation);
        if (normalized == null)
            return null;

        if (typeToken != TokenType.Text)
            throw new CSharpDbException(ErrorCode.TypeMismatch, $"COLLATE is only supported for TEXT columns. Column '{columnName}' uses type '{typeToken}'.");

        return normalized;
    }

    private async ValueTask<ForeignKeyDefinition[]> BuildForeignKeysAsync(
        string tableName,
        ColumnDefinition[] columns,
        IReadOnlyList<ColumnDef> columnDefs,
        IReadOnlyList<ForeignKeyDefinition> existingForeignKeys,
        CancellationToken ct)
    {
        var foreignKeys = new List<ForeignKeyDefinition>(existingForeignKeys.Count + columnDefs.Count);
        foreignKeys.AddRange(existingForeignKeys);

        var currentSchema = new TableSchema
        {
            TableName = tableName,
            Columns = columns,
            ForeignKeys = foreignKeys,
            NextRowId = 1,
        };

        for (int i = 0; i < columnDefs.Count; i++)
        {
            if (columnDefs[i].ForeignKey is null)
                continue;

            foreignKeys.Add(await ValidateAndMaterializeForeignKeyAsync(
                tableName,
                columns,
                columnDefs[i],
                currentSchema,
                ct));
        }

        return foreignKeys.ToArray();
    }

    private async ValueTask<ForeignKeyDefinition> ValidateAndMaterializeForeignKeyAsync(
        string tableName,
        IReadOnlyList<ColumnDefinition> columns,
        ColumnDef columnDef,
        TableSchema currentTableSchema,
        CancellationToken ct)
    {
        if (columnDef.ForeignKey is null)
            throw new InvalidOperationException($"Column '{columnDef.Name}' does not define a foreign key.");

        int childColumnIndex = currentTableSchema.GetColumnIndex(columnDef.Name);
        if (childColumnIndex < 0)
            throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{columnDef.Name}' not found in table '{tableName}'.");

        ColumnDefinition childColumn = columns[childColumnIndex];
        if (childColumn.Type is not (DbType.Integer or DbType.Text))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Foreign key column '{columnDef.Name}' must use INTEGER or TEXT.");
        }

        TableSchema parentSchema;
        if (string.Equals(columnDef.ForeignKey.ReferencedTableName, tableName, StringComparison.OrdinalIgnoreCase))
        {
            parentSchema = currentTableSchema;
        }
        else
        {
            parentSchema = GetSchema(columnDef.ForeignKey.ReferencedTableName);
        }

        string referencedColumnName = columnDef.ForeignKey.ReferencedColumnName ?? ResolvePrimaryKeyColumnName(parentSchema, columnDef.Name);
        int parentColumnIndex = parentSchema.GetColumnIndex(referencedColumnName);
        if (parentColumnIndex < 0)
        {
            throw new CSharpDbException(
                ErrorCode.ColumnNotFound,
                $"Referenced column '{referencedColumnName}' was not found on table '{parentSchema.TableName}'.");
        }

        ColumnDefinition parentColumn = parentSchema.Columns[parentColumnIndex];
        if (parentColumn.Type != childColumn.Type)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Foreign key column '{columnDef.Name}' type '{childColumn.Type}' does not match referenced column '{parentSchema.TableName}.{referencedColumnName}' type '{parentColumn.Type}'.");
        }

        string? childCollation = childColumn.Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(childColumn.Collation)
            : null;
        if (!CanUseParentColumnForForeignKey(parentSchema, parentColumnIndex, childCollation, excludedIndexName: null))
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Referenced column '{parentSchema.TableName}.{referencedColumnName}' must be a single-column PRIMARY KEY or UNIQUE index with matching collation.");
        }

        string constraintName = GenerateForeignKeyConstraintName(
            tableName,
            columnDef.Name,
            parentSchema.TableName,
            referencedColumnName);

        return new ForeignKeyDefinition
        {
            ConstraintName = constraintName,
            ColumnName = columnDef.Name,
            ReferencedTableName = parentSchema.TableName,
            ReferencedColumnName = referencedColumnName,
            OnDelete = columnDef.ForeignKey.OnDelete,
            SupportingIndexName = GenerateForeignKeySupportIndexName(constraintName, tableName, columnDef.Name),
        };
    }

    private string ResolvePrimaryKeyColumnName(TableSchema parentSchema, string childColumnName)
    {
        int parentPrimaryKeyIndex = parentSchema.PrimaryKeyColumnIndex;
        if (parentPrimaryKeyIndex < 0)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Foreign key column '{childColumnName}' references table '{parentSchema.TableName}', which does not have a primary key.");
        }

        return parentSchema.Columns[parentPrimaryKeyIndex].Name;
    }

    private bool CanUseParentColumnForForeignKey(
        TableSchema parentSchema,
        int parentColumnIndex,
        string? expectedTextCollation,
        string? excludedIndexName)
    {
        ColumnDefinition parentColumn = parentSchema.Columns[parentColumnIndex];
        if (parentColumn.Type == DbType.Text &&
            !CollationSupport.SemanticallyEquals(
                expectedTextCollation,
                CollationSupport.NormalizeMetadataName(parentColumn.Collation)) &&
            !HasCompatibleUniqueParentIndex(parentSchema, parentColumnIndex, expectedTextCollation, excludedIndexName))
        {
            return false;
        }

        if (parentColumn.IsPrimaryKey)
            return true;

        return HasCompatibleUniqueParentIndex(parentSchema, parentColumnIndex, expectedTextCollation, excludedIndexName);
    }

    private bool HasCompatibleUniqueParentIndex(
        TableSchema parentSchema,
        int parentColumnIndex,
        string? expectedTextCollation,
        string? excludedIndexName)
    {
        foreach (IndexSchema index in _catalog.GetSqlIndexesForTable(parentSchema.TableName))
        {
            if (!index.IsUnique ||
                index.Columns.Count != 1 ||
                !string.Equals(index.Columns[0], parentSchema.Columns[parentColumnIndex].Name, StringComparison.OrdinalIgnoreCase) ||
                (excludedIndexName != null && string.Equals(index.IndexName, excludedIndexName, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            string?[] effectiveCollations = CollationSupport.GetEffectiveIndexColumnCollations(index, parentSchema, new[] { parentColumnIndex });
            string? effectiveCollation = effectiveCollations.Length > 0
                ? CollationSupport.NormalizeMetadataName(effectiveCollations[0])
                : null;
            if (parentSchema.Columns[parentColumnIndex].Type == DbType.Text &&
                !CollationSupport.SemanticallyEquals(expectedTextCollation, effectiveCollation))
            {
                continue;
            }

            return true;
        }

        return false;
    }

    private async ValueTask CreateForeignKeySupportIndexAsync(
        TableSchema tableSchema,
        ForeignKeyDefinition foreignKey,
        CancellationToken ct)
    {
        ColumnDefinition column = tableSchema.Columns[tableSchema.GetColumnIndex(foreignKey.ColumnName)];
        string? columnCollation = column.Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(column.Collation)
            : null;

        var indexSchema = new IndexSchema
        {
            IndexName = foreignKey.SupportingIndexName,
            TableName = tableSchema.TableName,
            Columns = new[] { foreignKey.ColumnName },
            ColumnCollations = new string?[] { columnCollation },
            IsUnique = false,
            Kind = IndexKind.ForeignKeyInternal,
            OwnerIndexName = foreignKey.ConstraintName,
        };

        await CreateAndBackfillIndexWithOrderedTextFallbackAsync(indexSchema, tableSchema, ct);
    }

    private static string GenerateForeignKeyConstraintName(
        string tableName,
        string columnName,
        string referencedTableName,
        string referencedColumnName)
        => $"fk_{SanitizeNameSegment(tableName)}_{SanitizeNameSegment(columnName)}_{ComputeStableNameSuffix($"{tableName}|{columnName}|{referencedTableName}|{referencedColumnName}")}";

    private static string GenerateForeignKeySupportIndexName(
        string constraintName,
        string tableName,
        string columnName)
        => $"__fk_{SanitizeNameSegment(tableName)}_{SanitizeNameSegment(columnName)}_{ComputeStableNameSuffix(constraintName)}";

    private void ValidateParentIndexCanBeDropped(string indexName)
    {
        IndexSchema? index = _catalog.GetIndex(indexName);
        if (index is null ||
            index.Kind != IndexKind.Sql ||
            !index.IsUnique ||
            index.Columns.Count != 1)
        {
            return;
        }

        TableSchema? parentSchema = _catalog.GetTable(index.TableName);
        if (parentSchema is null)
            return;

        int parentColumnIndex = parentSchema.GetColumnIndex(index.Columns[0]);
        if (parentColumnIndex < 0)
            return;

        foreach (TableForeignKeyReference reference in _catalog.GetReferencingForeignKeys(index.TableName))
        {
            if (!string.Equals(reference.ForeignKey.ReferencedColumnName, index.Columns[0], StringComparison.OrdinalIgnoreCase))
                continue;

            TableSchema? childSchema = _catalog.GetTable(reference.TableName);
            if (childSchema is null)
                continue;

            int childColumnIndex = childSchema.GetColumnIndex(reference.ForeignKey.ColumnName);
            if (childColumnIndex < 0)
                continue;

            string? expectedCollation = childSchema.Columns[childColumnIndex].Type == DbType.Text
                ? CollationSupport.NormalizeMetadataName(childSchema.Columns[childColumnIndex].Collation)
                : null;

            if (!CanUseParentColumnForForeignKey(parentSchema, parentColumnIndex, expectedCollation, excludedIndexName: indexName))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Cannot drop index '{indexName}' because foreign key '{reference.ForeignKey.ConstraintName}' depends on it.");
            }
        }
    }

    private async ValueTask RenameTableWithDependenciesAsync(
        string oldTableName,
        string newTableName,
        TableSchema schema,
        CancellationToken ct)
    {
        IndexSchema[] originalIndexes = _catalog.GetIndexesForTable(oldTableName).ToArray();
        ForeignKeyDefinition[] renamedForeignKeys = schema.ForeignKeys
            .Select(foreignKey => RenameForeignKeyForTable(foreignKey, oldTableName, newTableName))
            .ToArray();

        var renamedSchema = new TableSchema
        {
            TableName = newTableName,
            Columns = schema.Columns,
            ForeignKeys = renamedForeignKeys,
            NextRowId = schema.NextRowId,
        };

        await _catalog.UpdateTableSchemaAsync(oldTableName, renamedSchema, ct);

        for (int i = 0; i < originalIndexes.Length; i++)
        {
            IndexSchema index = originalIndexes[i];
            string newIndexName = index.IndexName;
            if (index.Kind == IndexKind.ForeignKeyInternal && index.OwnerIndexName is { Length: > 0 })
            {
                ForeignKeyDefinition? foreignKey = renamedForeignKeys.FirstOrDefault(fk =>
                    string.Equals(fk.ConstraintName, index.OwnerIndexName, StringComparison.OrdinalIgnoreCase));
                if (foreignKey != null)
                    newIndexName = foreignKey.SupportingIndexName;
            }

            await _catalog.UpdateIndexSchemaAsync(
                index.IndexName,
                CloneIndexSchema(index, newIndexName, newTableName, index.Columns),
                ct);
        }

        foreach (string tableName in _catalog.GetTableNames().ToArray())
        {
            if (string.Equals(tableName, newTableName, StringComparison.OrdinalIgnoreCase))
                continue;

            TableSchema? tableSchema = _catalog.GetTable(tableName);
            if (tableSchema is null || tableSchema.ForeignKeys.Count == 0)
                continue;

            ForeignKeyDefinition[] updatedForeignKeys = tableSchema.ForeignKeys
                .Select(foreignKey => string.Equals(foreignKey.ReferencedTableName, oldTableName, StringComparison.OrdinalIgnoreCase)
                    ? new ForeignKeyDefinition
                    {
                        ConstraintName = foreignKey.ConstraintName,
                        ColumnName = foreignKey.ColumnName,
                        ReferencedTableName = newTableName,
                        ReferencedColumnName = foreignKey.ReferencedColumnName,
                        OnDelete = foreignKey.OnDelete,
                        SupportingIndexName = foreignKey.SupportingIndexName,
                    }
                    : foreignKey)
                .ToArray();

            if (updatedForeignKeys.SequenceEqual(tableSchema.ForeignKeys))
                continue;

            await _catalog.UpdateTableSchemaAsync(
                tableSchema.TableName,
                new TableSchema
                {
                    TableName = tableSchema.TableName,
                    Columns = tableSchema.Columns,
                    ForeignKeys = updatedForeignKeys,
                    NextRowId = tableSchema.NextRowId,
                },
                ct);
        }
    }

    private async ValueTask RenameColumnWithDependenciesAsync(
        string tableName,
        string oldColumnName,
        string newColumnName,
        TableSchema schema,
        CancellationToken ct)
    {
        IndexSchema[] originalIndexes = _catalog.GetIndexesForTable(tableName).ToArray();
        ColumnDefinition[] renamedColumns = schema.Columns.Select(column =>
            string.Equals(column.Name, oldColumnName, StringComparison.OrdinalIgnoreCase)
                ? new ColumnDefinition
                {
                    Name = newColumnName,
                    Type = column.Type,
                    Nullable = column.Nullable,
                    IsPrimaryKey = column.IsPrimaryKey,
                    IsIdentity = column.IsIdentity,
                    Collation = column.Collation,
                }
                : column).ToArray();

        ForeignKeyDefinition[] renamedForeignKeys = schema.ForeignKeys
            .Select(foreignKey => RenameForeignKeyForColumn(foreignKey, tableName, oldColumnName, newColumnName))
            .ToArray();

        await _catalog.UpdateTableSchemaAsync(
            tableName,
            new TableSchema
            {
                TableName = tableName,
                Columns = renamedColumns,
                ForeignKeys = renamedForeignKeys,
                NextRowId = schema.NextRowId,
            },
            ct);

        for (int i = 0; i < originalIndexes.Length; i++)
        {
            IndexSchema index = originalIndexes[i];
            IReadOnlyList<string> updatedColumns = index.Columns
                .Select(column => string.Equals(column, oldColumnName, StringComparison.OrdinalIgnoreCase) ? newColumnName : column)
                .ToArray();

            string newIndexName = index.IndexName;
            if (index.Kind == IndexKind.ForeignKeyInternal && index.OwnerIndexName is { Length: > 0 })
            {
                ForeignKeyDefinition? foreignKey = renamedForeignKeys.FirstOrDefault(fk =>
                    string.Equals(fk.ConstraintName, index.OwnerIndexName, StringComparison.OrdinalIgnoreCase));
                if (foreignKey != null)
                    newIndexName = foreignKey.SupportingIndexName;
            }

            await _catalog.UpdateIndexSchemaAsync(
                index.IndexName,
                CloneIndexSchema(index, newIndexName, tableName, updatedColumns),
                ct);
        }

        foreach (string otherTableName in _catalog.GetTableNames().ToArray())
        {
            if (string.Equals(otherTableName, tableName, StringComparison.OrdinalIgnoreCase))
                continue;

            TableSchema? otherSchema = _catalog.GetTable(otherTableName);
            if (otherSchema is null || otherSchema.ForeignKeys.Count == 0)
                continue;

            bool changed = false;
            ForeignKeyDefinition[] updatedForeignKeys = otherSchema.ForeignKeys
                .Select(foreignKey =>
                {
                    if (!string.Equals(foreignKey.ReferencedTableName, tableName, StringComparison.OrdinalIgnoreCase) ||
                        !string.Equals(foreignKey.ReferencedColumnName, oldColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        return foreignKey;
                    }

                    changed = true;
                    return new ForeignKeyDefinition
                    {
                        ConstraintName = foreignKey.ConstraintName,
                        ColumnName = foreignKey.ColumnName,
                        ReferencedTableName = foreignKey.ReferencedTableName,
                        ReferencedColumnName = newColumnName,
                        OnDelete = foreignKey.OnDelete,
                        SupportingIndexName = foreignKey.SupportingIndexName,
                    };
                })
                .ToArray();

            if (!changed)
                continue;

            await _catalog.UpdateTableSchemaAsync(
                otherSchema.TableName,
                new TableSchema
                {
                    TableName = otherSchema.TableName,
                    Columns = otherSchema.Columns,
                    ForeignKeys = updatedForeignKeys,
                    NextRowId = otherSchema.NextRowId,
                },
                ct);
        }
    }

    private static ForeignKeyDefinition RenameForeignKeyForTable(ForeignKeyDefinition foreignKey, string oldTableName, string newTableName)
    {
        bool childTableRenamed = true;
        bool referencedTableRenamed = string.Equals(foreignKey.ReferencedTableName, oldTableName, StringComparison.OrdinalIgnoreCase);

        return new ForeignKeyDefinition
        {
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = referencedTableRenamed ? newTableName : foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            OnDelete = foreignKey.OnDelete,
            SupportingIndexName = childTableRenamed
                ? GenerateForeignKeySupportIndexName(foreignKey.ConstraintName, newTableName, foreignKey.ColumnName)
                : foreignKey.SupportingIndexName,
        };
    }

    private static ForeignKeyDefinition RenameForeignKeyForColumn(
        ForeignKeyDefinition foreignKey,
        string tableName,
        string oldColumnName,
        string newColumnName)
    {
        bool childColumnRenamed = string.Equals(foreignKey.ColumnName, oldColumnName, StringComparison.OrdinalIgnoreCase);
        bool referencedColumnRenamed =
            string.Equals(foreignKey.ReferencedTableName, tableName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(foreignKey.ReferencedColumnName, oldColumnName, StringComparison.OrdinalIgnoreCase);

        string childColumnName = childColumnRenamed ? newColumnName : foreignKey.ColumnName;
        return new ForeignKeyDefinition
        {
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = childColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = referencedColumnRenamed ? newColumnName : foreignKey.ReferencedColumnName,
            OnDelete = foreignKey.OnDelete,
            SupportingIndexName = childColumnRenamed
                ? GenerateForeignKeySupportIndexName(foreignKey.ConstraintName, tableName, newColumnName)
                : foreignKey.SupportingIndexName,
        };
    }

    private static IndexSchema CloneIndexSchema(
        IndexSchema index,
        string newIndexName,
        string newTableName,
        IReadOnlyList<string> newColumns)
        => new()
        {
            IndexName = newIndexName,
            TableName = newTableName,
            Columns = newColumns.ToArray(),
            ColumnCollations = index.ColumnCollations.ToArray(),
            IsUnique = index.IsUnique,
            Kind = index.Kind,
            State = index.State,
            OwnerIndexName = index.OwnerIndexName,
            OptionsJson = index.OptionsJson,
        };

    private static string SanitizeNameSegment(string value)
    {
        var builder = new StringBuilder(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            char ch = value[i];
            builder.Append(char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_');
        }

        return builder.ToString();
    }

    private static string ComputeStableNameSuffix(string value)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash, 0, 4).ToLowerInvariant();
    }

    private DbValue[] ResolveInsertRow(TableSchema schema, List<string>? columnNames, List<Expression> values)
    {
        var row = new DbValue[schema.Columns.Count];

        if (columnNames != null)
        {
            if (columnNames.Count != values.Count)
                throw new CSharpDbException(ErrorCode.SyntaxError,
                    $"Column count ({columnNames.Count}) doesn't match value count ({values.Count}).");

            for (int i = 0; i < columnNames.Count; i++)
            {
                int colIdx = schema.GetColumnIndex(columnNames[i]);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{columnNames[i]}' not found.");
                row[colIdx] = ResolveInsertValue(values[i], schema);
            }
        }
        else
        {
            if (values.Count != schema.Columns.Count)
                throw new CSharpDbException(ErrorCode.SyntaxError,
                    $"Expected {schema.Columns.Count} values, got {values.Count}.");

            for (int i = 0; i < values.Count; i++)
                row[i] = ResolveInsertValue(values[i], schema);
        }

        return row;
    }

    private static DbValue ResolveInsertValue(Expression valueExpression, TableSchema schema)
    {
        if (valueExpression is LiteralExpression literal)
            return ResolveInsertLiteral(literal);

        return ExpressionEvaluator.Evaluate(valueExpression, Array.Empty<DbValue>(), schema);
    }

    private static DbValue ResolveInsertLiteral(LiteralExpression literal)
    {
        if (literal.Value == null)
            return DbValue.Null;

        return literal.LiteralType switch
        {
            TokenType.IntegerLiteral => DbValue.FromInteger((long)literal.Value),
            TokenType.RealLiteral => DbValue.FromReal((double)literal.Value),
            TokenType.StringLiteral => DbValue.FromText((string)literal.Value),
            TokenType.Null => DbValue.Null,
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown literal type: {literal.LiteralType}"),
        };
    }

    private async ValueTask PersistForeignKeyMutationContextAsync(
        ForeignKeyMutationContext? mutationContext,
        string tableName,
        bool hasMutations,
        bool persistRootChanges,
        CancellationToken ct)
    {
        if (!hasMutations)
            return;

        if (mutationContext is null)
        {
            await _catalog.MarkTableColumnStatisticsStaleAsync(tableName, ct);
            if (persistRootChanges)
                await _catalog.PersistRootPageChangesAsync(tableName, ct);

            return;
        }

        if (mutationContext.TouchedTables.Count == 0)
            mutationContext.TouchedTables.Add(tableName);

        if (mutationContext.StaleTables.Count == 0)
            mutationContext.StaleTables.Add(tableName);

        foreach (string staleTableName in mutationContext.StaleTables)
            await _catalog.MarkTableColumnStatisticsStaleAsync(staleTableName, ct);

        if (!persistRootChanges)
            return;

        foreach (string touchedTableName in mutationContext.TouchedTables)
            await _catalog.PersistRootPageChangesAsync(touchedTableName, ct);
    }

    private async ValueTask ValidateOutgoingForeignKeysAsync(
        string tableName,
        TableSchema schema,
        DbValue[]? oldRow,
        DbValue[] newRow,
        CancellationToken ct)
    {
        IReadOnlyList<ForeignKeyDefinition> foreignKeys = _catalog.GetForeignKeysForTable(tableName);
        if (foreignKeys.Count == 0)
            return;

        for (int i = 0; i < foreignKeys.Count; i++)
        {
            ForeignKeyDefinition foreignKey = foreignKeys[i];
            int childColumnIndex = schema.GetColumnIndex(foreignKey.ColumnName);
            if (childColumnIndex < 0)
                throw new InvalidOperationException($"Foreign key '{foreignKey.ConstraintName}' references missing child column '{foreignKey.ColumnName}'.");

            DbValue childValue = newRow[childColumnIndex];
            if (childValue.IsNull)
                continue;

            if (oldRow is not null && DbValue.Compare(oldRow[childColumnIndex], childValue) == 0)
                continue;

            TableSchema parentSchema = string.Equals(foreignKey.ReferencedTableName, tableName, StringComparison.OrdinalIgnoreCase)
                ? schema
                : GetSchema(foreignKey.ReferencedTableName);
            int parentColumnIndex = parentSchema.GetColumnIndex(foreignKey.ReferencedColumnName);
            if (parentColumnIndex < 0)
            {
                throw new InvalidOperationException(
                    $"Foreign key '{foreignKey.ConstraintName}' references missing parent column '{foreignKey.ReferencedColumnName}'.");
            }

            if (string.Equals(foreignKey.ReferencedTableName, tableName, StringComparison.OrdinalIgnoreCase) &&
                DbValue.Compare(newRow[parentColumnIndex], childValue) == 0)
            {
                continue;
            }

            if (!await ParentRowExistsAsync(parentSchema, parentColumnIndex, childValue, ct))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Foreign key '{foreignKey.ConstraintName}' on table '{tableName}' requires a matching row in '{foreignKey.ReferencedTableName}'.");
            }
        }
    }

    private async ValueTask ValidateIncomingForeignKeyUpdatesAsync(
        string tableName,
        TableSchema schema,
        long rowId,
        DbValue[] oldRow,
        DbValue[] newRow,
        CancellationToken ct)
    {
        IReadOnlyList<TableForeignKeyReference> references = _catalog.GetReferencingForeignKeys(tableName);
        if (references.Count == 0)
            return;

        for (int i = 0; i < references.Count; i++)
        {
            TableForeignKeyReference reference = references[i];
            int parentColumnIndex = schema.GetColumnIndex(reference.ForeignKey.ReferencedColumnName);
            if (parentColumnIndex < 0)
                throw new InvalidOperationException($"Foreign key '{reference.ForeignKey.ConstraintName}' references missing parent column '{reference.ForeignKey.ReferencedColumnName}'.");

            DbValue oldParentValue = oldRow[parentColumnIndex];
            DbValue newParentValue = newRow[parentColumnIndex];
            if (oldParentValue.IsNull || DbValue.Compare(oldParentValue, newParentValue) == 0)
                continue;

            List<(long RowId, DbValue[] Row)> dependents = await LoadReferencingRowsAsync(reference, oldParentValue, ct);
            if (dependents.Count == 0)
                continue;

            bool onlyCurrentRowSelfReference = dependents.Count == 1 &&
                string.Equals(reference.TableName, tableName, StringComparison.OrdinalIgnoreCase) &&
                dependents[0].RowId == rowId;
            if (onlyCurrentRowSelfReference)
                continue;

            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Cannot update referenced value '{reference.ForeignKey.ReferencedColumnName}' on table '{tableName}' because foreign key '{reference.ForeignKey.ConstraintName}' has dependent rows.");
        }
    }

    private async ValueTask<bool> DeleteRowWithForeignKeysAsync(
        string tableName,
        TableSchema schema,
        BTree tree,
        IReadOnlyList<IndexSchema> indexes,
        long rowId,
        ForeignKeyMutationContext mutationContext,
        int depth,
        CancellationToken ct)
    {
        if (depth > MaxForeignKeyCascadeDepth)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Maximum foreign key cascade depth of {MaxForeignKeyCascadeDepth} was exceeded.");
        }

        if (!mutationContext.VisitedDeletes.Add(new ForeignKeyDeleteKey(tableName, rowId)))
            return false;

        DbValue[]? currentRow = await TryLoadRowAsync(tableName, schema, rowId, ct);
        if (currentRow is null)
            return false;

        await FireTriggersAsync(tableName, TriggerTiming.Before, TriggerEvent.Delete, currentRow, null, schema, ct);

        currentRow = await TryLoadRowAsync(tableName, schema, rowId, ct);
        if (currentRow is null)
            return false;

        IReadOnlyList<TableForeignKeyReference> references = _catalog.GetReferencingForeignKeys(tableName);
        for (int i = 0; i < references.Count; i++)
        {
            TableForeignKeyReference reference = references[i];
            int parentColumnIndex = schema.GetColumnIndex(reference.ForeignKey.ReferencedColumnName);
            if (parentColumnIndex < 0)
                throw new InvalidOperationException($"Foreign key '{reference.ForeignKey.ConstraintName}' references missing parent column '{reference.ForeignKey.ReferencedColumnName}'.");

            DbValue parentValue = currentRow[parentColumnIndex];
            if (parentValue.IsNull)
                continue;

            List<(long RowId, DbValue[] Row)> dependentRows = await LoadReferencingRowsAsync(reference, parentValue, ct);
            if (dependentRows.Count == 0)
                continue;

            if (reference.ForeignKey.OnDelete != ForeignKeyOnDeleteAction.Cascade)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Cannot delete row from '{tableName}' because foreign key '{reference.ForeignKey.ConstraintName}' has dependent rows in '{reference.TableName}'.");
            }

            TableSchema childSchema = GetSchema(reference.TableName);
            BTree childTree = _catalog.GetTableTree(reference.TableName, _pager);
            IReadOnlyList<IndexSchema> childIndexes = _catalog.GetIndexesForTable(reference.TableName);
            for (int dependentIndex = 0; dependentIndex < dependentRows.Count; dependentIndex++)
            {
                (long dependentRowId, _) = dependentRows[dependentIndex];
                if (string.Equals(reference.TableName, tableName, StringComparison.OrdinalIgnoreCase) &&
                    dependentRowId == rowId)
                {
                    continue;
                }

                await DeleteRowWithForeignKeysAsync(
                    reference.TableName,
                    childSchema,
                    childTree,
                    childIndexes,
                    dependentRowId,
                    mutationContext,
                    depth + 1,
                    ct);
            }
        }

        await tree.DeleteAsync(rowId, ct);
        await DeleteFromAllIndexesAsync(indexes, schema, currentRow, rowId, ct);
        await _catalog.AdjustTableRowCountAsync(tableName, -1, ct);
        mutationContext.TouchedTables.Add(tableName);
        mutationContext.StaleTables.Add(tableName);

        await FireTriggersAsync(tableName, TriggerTiming.After, TriggerEvent.Delete, currentRow, null, schema, ct);
        return true;
    }

    private async ValueTask<List<(long RowId, DbValue[] Row)>> LoadReferencingRowsAsync(
        TableForeignKeyReference reference,
        DbValue referencedValue,
        CancellationToken ct)
    {
        var rows = new List<(long RowId, DbValue[] Row)>();
        TableSchema childSchema = GetSchema(reference.TableName);
        int childColumnIndex = childSchema.GetColumnIndex(reference.ForeignKey.ColumnName);
        if (childColumnIndex < 0 || referencedValue.IsNull)
            return rows;

        BTree childTree = _catalog.GetTableTree(reference.TableName, _pager);
        IRecordSerializer serializer = GetReadSerializer(childSchema);
        IndexSchema? supportIndex = _catalog.GetIndex(reference.ForeignKey.SupportingIndexName);
        if (supportIndex is not null &&
            TryBuildForeignKeyLookup(supportIndex, childSchema, childColumnIndex, referencedValue, out long lookupKey, out DbValue[]? keyComponents, out SqlIndexStorageMode storageMode, out bool usesDirectIntegerKey))
        {
            byte[]? payload = await _catalog.GetIndexStore(supportIndex.IndexName).FindAsync(lookupKey, ct);
            ReadOnlyMemory<byte> rowIdPayload = GetMatchingIndexRowIds(payload, keyComponents, storageMode, usesDirectIntegerKey);
            if (!rowIdPayload.IsEmpty)
            {
                int rowIdCount = RowIdPayloadCodec.GetCount(rowIdPayload.Span);
                for (int i = 0; i < rowIdCount; i++)
                {
                    long dependentRowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, i);
                    ReadOnlyMemory<byte>? rowPayload = await childTree.FindMemoryAsync(dependentRowId, ct);
                    if (rowPayload is not { } rowPayloadMemory)
                        continue;

                    rows.Add((dependentRowId, serializer.Decode(rowPayloadMemory.Span)));
                }

                return rows;
            }
        }

        string? childCollation = childSchema.Columns[childColumnIndex].Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(childSchema.Columns[childColumnIndex].Collation)
            : null;
        var cursor = childTree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            DbValue[] childRow = serializer.Decode(cursor.CurrentValue.Span);
            if (childColumnIndex >= childRow.Length)
                continue;

            if (!childRow[childColumnIndex].IsNull &&
                CollationSupport.Compare(childRow[childColumnIndex], referencedValue, childCollation) == 0)
            {
                rows.Add((cursor.CurrentKey, childRow));
            }
        }

        return rows;
    }

    private async ValueTask<bool> ParentRowExistsAsync(
        TableSchema parentSchema,
        int parentColumnIndex,
        DbValue expectedValue,
        CancellationToken ct)
    {
        if (expectedValue.IsNull)
            return true;

        if (parentColumnIndex == parentSchema.PrimaryKeyColumnIndex &&
            parentSchema.Columns[parentColumnIndex].Type == DbType.Integer &&
            expectedValue.Type == DbType.Integer)
        {
            return await _catalog.GetTableTree(parentSchema.TableName, _pager).FindMemoryAsync(expectedValue.AsInteger, ct) is not null;
        }

        IndexSchema? lookupIndex = FindSingleColumnForeignKeyLookupIndex(parentSchema, parentColumnIndex);
        if (lookupIndex is not null &&
            TryBuildForeignKeyLookup(lookupIndex, parentSchema, parentColumnIndex, expectedValue, out long lookupKey, out DbValue[]? keyComponents, out SqlIndexStorageMode storageMode, out bool usesDirectIntegerKey))
        {
            byte[]? payload = await _catalog.GetIndexStore(lookupIndex.IndexName).FindAsync(lookupKey, ct);
            ReadOnlyMemory<byte> rowIdPayload = GetMatchingIndexRowIds(payload, keyComponents, storageMode, usesDirectIntegerKey);
            if (!rowIdPayload.IsEmpty)
            {
                int rowIdCount = RowIdPayloadCodec.GetCount(rowIdPayload.Span);
                BTree tableTree = _catalog.GetTableTree(parentSchema.TableName, _pager);
                for (int i = 0; i < rowIdCount; i++)
                {
                    long rowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, i);
                    if (await tableTree.FindMemoryAsync(rowId, ct) is not null)
                        return true;
                }
            }
        }

        string? parentCollation = parentSchema.Columns[parentColumnIndex].Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(parentSchema.Columns[parentColumnIndex].Collation)
            : null;
        BTree scanTree = _catalog.GetTableTree(parentSchema.TableName, _pager);
        IRecordSerializer serializer = GetReadSerializer(parentSchema);
        var cursor = scanTree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            DbValue[] row = serializer.Decode(cursor.CurrentValue.Span);
            if (parentColumnIndex < row.Length &&
                !row[parentColumnIndex].IsNull &&
                CollationSupport.Compare(row[parentColumnIndex], expectedValue, parentCollation) == 0)
            {
                return true;
            }
        }

        return false;
    }

    private IndexSchema? FindSingleColumnForeignKeyLookupIndex(TableSchema schema, int columnIndex)
    {
        string columnName = schema.Columns[columnIndex].Name;
        string? expectedCollation = schema.Columns[columnIndex].Type == DbType.Text
            ? CollationSupport.NormalizeMetadataName(schema.Columns[columnIndex].Collation)
            : null;

        IndexSchema? firstMatch = null;
        foreach (IndexSchema index in _catalog.GetSqlIndexesForTable(schema.TableName))
        {
            if (index.Columns.Count != 1 ||
                !string.Equals(index.Columns[0], columnName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            string? indexCollation = index.ColumnCollations.Count > 0
                ? CollationSupport.NormalizeMetadataName(index.ColumnCollations[0])
                : expectedCollation;
            if (!CollationSupport.SemanticallyEquals(indexCollation, expectedCollation))
                continue;

            if (index.IsUnique)
                return index;

            firstMatch ??= index;
        }

        return firstMatch;
    }

    private static bool TryBuildForeignKeyLookup(
        IndexSchema index,
        TableSchema schema,
        int columnIndex,
        DbValue value,
        out long lookupKey,
        out DbValue[]? keyComponents,
        out SqlIndexStorageMode storageMode,
        out bool usesDirectIntegerKey)
    {
        lookupKey = 0;
        keyComponents = null;
        storageMode = SqlIndexStorageMode.Hashed;
        usesDirectIntegerKey = false;

        if (index.Columns.Count != 1 || value.IsNull)
            return false;

        int[] columnIndices = [columnIndex];
        string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(index, schema, columnIndices);
        storageMode = IndexMaintenanceHelper.ResolveSqlIndexStorageMode(index, schema);
        usesDirectIntegerKey = schema.Columns[columnIndex].Type == DbType.Integer;
        if (usesDirectIntegerKey)
        {
            if (value.Type != DbType.Integer)
                return false;

            lookupKey = value.AsInteger;
            return true;
        }

        if (value.Type is not (DbType.Integer or DbType.Text))
            return false;

        DbValue normalizedValue = CollationSupport.NormalizeIndexValue(
            value,
            indexColumnCollations.Length > 0 ? indexColumnCollations[0] : null);
        keyComponents = [normalizedValue];
        lookupKey = storageMode == SqlIndexStorageMode.OrderedText
            ? OrderedTextIndexKeyCodec.ComputeKey(normalizedValue.AsText)
            : IndexMaintenanceHelper.ComputeIndexKey(keyComponents);
        return true;
    }

    private static ReadOnlyMemory<byte> GetMatchingIndexRowIds(
        byte[]? payload,
        DbValue[]? keyComponents,
        SqlIndexStorageMode storageMode,
        bool usesDirectIntegerKey)
    {
        if (payload is null || payload.Length == 0)
            return ReadOnlyMemory<byte>.Empty;

        if (usesDirectIntegerKey)
            return payload;

        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            return keyComponents is [var keyComponent] &&
                   keyComponent.Type == DbType.Text &&
                   OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, keyComponent.AsText, out ReadOnlyMemory<byte> orderedRowIds)
                ? orderedRowIds
                : ReadOnlyMemory<byte>.Empty;
        }

        if (keyComponents is null)
            return ReadOnlyMemory<byte>.Empty;

        if (!HashedIndexPayloadCodec.TryGetMatchingRowIds(payload, keyComponents, out byte[]? hashedRowIds))
            return ReadOnlyMemory<byte>.Empty;

        return hashedRowIds ?? ReadOnlyMemory<byte>.Empty;
    }

    private async ValueTask<DbValue[]?> TryLoadRowAsync(
        string tableName,
        TableSchema schema,
        long rowId,
        CancellationToken ct)
    {
        ReadOnlyMemory<byte>? payload = await _catalog.GetTableTree(tableName, _pager).FindMemoryAsync(rowId, ct);
        return payload is { } rowPayload ? GetReadSerializer(schema).Decode(rowPayload.Span) : null;
    }

    private async ValueTask<long> ExecuteResolvedInsertRowAsync(
        string tableName,
        TableSchema schema,
        BTree tree,
        IReadOnlyList<IndexSchema> indexes,
        DbValue[] row,
        ForeignKeyMutationContext? mutationContext,
        bool adjustTableRowCount,
        CancellationToken ct)
    {
        // BEFORE INSERT triggers
        await FireTriggersAsync(tableName, TriggerTiming.Before, TriggerEvent.Insert, null, row, schema, ct);

        var (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(tableName, schema, tree, row, ct);
        if (mutationContext is not null)
            await ValidateOutgoingForeignKeysAsync(tableName, schema, oldRow: null, row, ct);
        while (true)
        {
            try
            {
                await tree.InsertAsync(rowId, _recordSerializer.Encode(row), ct);
                break;
            }
            catch (CSharpDbException ex) when (autoGeneratedRowId && ex.Code == ErrorCode.DuplicateKey)
            {
                // Another writer may have advanced rowids; reload the high-water mark once and retry.
                InvalidateRowIdCache(tableName);
                (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(tableName, schema, tree, row, ct);
            }
        }

        // Maintain indexes
        await InsertIntoAllIndexesAsync(indexes, schema, row, rowId, ct);
        if (adjustTableRowCount)
            await _catalog.AdjustTableRowCountAsync(tableName, 1, ct);
        if (mutationContext is not null)
        {
            mutationContext.TouchedTables.Add(tableName);
            mutationContext.StaleTables.Add(tableName);
        }

        // AFTER INSERT triggers
        await FireTriggersAsync(tableName, TriggerTiming.After, TriggerEvent.Insert, null, row, schema, ct);

        return rowId;
    }

    private async ValueTask<(long RowId, bool AutoGenerated)> ResolveRowIdForInsertAsync(
        string tableName, TableSchema schema, BTree tree, DbValue[] row, CancellationToken ct)
    {
        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (pkIdx >= 0 &&
            schema.Columns[pkIdx].Type == DbType.Integer &&
            schema.Columns[pkIdx].IsIdentity)
        {
            if (!row[pkIdx].IsNull)
            {
                long explicitRowId = row[pkIdx].AsInteger;
                if (explicitRowId >= 0 && explicitRowId < long.MaxValue)
                    ObserveExplicitRowId(tableName, schema, checked(explicitRowId + 1));
                return (explicitRowId, false);
            }

            long rowId = await AllocateRowIdAsync(tableName, schema, tree, ct);
            row[pkIdx] = DbValue.FromInteger(rowId);
            return (rowId, true);
        }

        if (pkIdx >= 0 && schema.Columns[pkIdx].Type == DbType.Integer)
        {
            if (row[pkIdx].IsNull)
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Primary key column '{schema.Columns[pkIdx].Name}' requires an explicit value.");

            long rowId = row[pkIdx].AsInteger;
            if (rowId >= 0 && rowId < long.MaxValue)
                UpdateNextRowIdState(tableName, schema, checked(rowId + 1));
            return (rowId, false);
        }

        return (await AllocateRowIdAsync(tableName, schema, tree, ct), true);
    }

    private async ValueTask<long> AllocateRowIdAsync(string tableName, TableSchema schema, BTree tree, CancellationToken ct)
    {
        if (!_nextRowIdCache.TryGetValue(tableName, out long nextRowId))
            nextRowId = await LoadNextRowIdAsync(tableName, schema, tree, ct);

        UpdateNextRowIdState(tableName, schema, checked(nextRowId + 1));
        return nextRowId;
    }

    private void InvalidateRowIdCache(string tableName)
    {
        _nextRowIdCache.Remove(tableName);
    }

    private async ValueTask<long> LoadNextRowIdAsync(string tableName, TableSchema schema, BTree tree, CancellationToken ct)
    {
        if (schema.NextRowId > 0)
        {
            _nextRowIdCache[tableName] = schema.NextRowId;
            return schema.NextRowId;
        }

        long loadedNextRowId = await ScanNextRowIdAsync(tree, ct);
        UpdateNextRowIdState(tableName, schema, loadedNextRowId);
        return loadedNextRowId;
    }

    private static async ValueTask<long> ScanNextRowIdAsync(BTree tree, CancellationToken ct)
    {
        long maxId = 0;
        var cursor = tree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            if (cursor.CurrentKey > maxId)
                maxId = cursor.CurrentKey;
        }
        return maxId + 1;
    }

    private void UpdateNextRowIdState(string tableName, TableSchema schema, long nextRowId)
    {
        if (nextRowId <= 0)
            return;

        if (_nextRowIdCache.TryGetValue(tableName, out long cached))
            _nextRowIdCache[tableName] = Math.Max(cached, nextRowId);
        else
            _nextRowIdCache[tableName] = nextRowId;

        if (schema.NextRowId < nextRowId)
            schema.NextRowId = nextRowId;
    }

    private void ObserveExplicitRowId(string tableName, TableSchema schema, long nextRowId)
    {
        if (nextRowId <= 0)
            return;

        if (_nextRowIdCache.TryGetValue(tableName, out long cached))
            _nextRowIdCache[tableName] = Math.Max(cached, nextRowId);
        else if (schema.NextRowId > 0)
            _nextRowIdCache[tableName] = Math.Max(schema.NextRowId, nextRowId);
        else
            _nextRowIdCache[tableName] = nextRowId;

        // Explicit rowid inserts can push the durable high-water mark forward, but persisting
        // every bump rewrites the schema catalog row on each commit. Mark the persisted hint as
        // unknown instead; the current session still uses the in-memory cache, and a future
        // reopened session can recompute max(rowid)+1 once if needed.
        if (schema.NextRowId > 0 && nextRowId > schema.NextRowId)
            schema.NextRowId = 0;
    }

    #endregion
}
