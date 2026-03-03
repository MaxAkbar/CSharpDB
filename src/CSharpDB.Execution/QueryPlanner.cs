using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Core;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Takes a parsed AST statement and produces an executable QueryResult.
/// Handles DDL (CREATE/DROP TABLE/INDEX/VIEW) and DML (INSERT/UPDATE/DELETE/SELECT).
/// </summary>
public sealed class QueryPlanner
{
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
    ];

    private static readonly ColumnDefinition[] SystemIndexesColumns =
    [
        new ColumnDefinition { Name = "index_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "table_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "column_name", Type = DbType.Text, Nullable = false },
        new ColumnDefinition { Name = "ordinal_position", Type = DbType.Integer, Nullable = false },
        new ColumnDefinition { Name = "is_unique", Type = DbType.Integer, Nullable = false },
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

    /// <summary>
    /// CTE materialized results, scoped to the current WITH query execution.
    /// Maps CTE name -> (rows, schema).
    /// </summary>
    private Dictionary<string, (List<DbValue[]> Rows, TableSchema Schema)>? _cteData;

    /// <summary>Recursion guard for trigger execution.</summary>
    private int _triggerDepth;
    private const int MaxTriggerDepth = 16;

    /// <summary>Cache of parsed trigger bodies to avoid re-parsing on every row.</summary>
    private readonly Dictionary<string, List<Statement>> _triggerBodyCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _nextRowIdCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<CompiledExpressionCacheKey, Func<DbValue[], DbValue>> _compiledExpressionCache = new();
    private readonly Dictionary<TableSchema, string> _qualifiedMappingFingerprintCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<TableSchema, ColumnDefinition[]> _tableSchemaArrayCache = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<(TableSchema Schema, int ColumnIndex), ColumnDefinition[]> _singleColumnOutputSchemaCache = new();
    private readonly Dictionary<Expression, bool> _requiresQualifiedMappingCache = new(ReferenceEqualityComparer.Instance);
    private List<DbValue[]>? _systemTablesRowsCache;
    private List<DbValue[]>? _systemColumnsRowsCache;
    private List<DbValue[]>? _systemIndexesRowsCache;
    private List<DbValue[]>? _systemViewsRowsCache;
    private List<DbValue[]>? _systemTriggersRowsCache;
    private long? _systemColumnsCountCache;
    private long? _systemIndexesCountCache;
    private readonly Dictionary<string, TableSchema> _systemCatalogSchemaCache = new(StringComparer.OrdinalIgnoreCase);
    private long _observedSchemaVersion;

    private const int MaxCompiledExpressionCacheEntries = 4096;

    /// <summary>
    /// When true, simple PK equality lookups (SELECT * / PK-only projection WHERE pk = N) will try a synchronous
    /// cache-only path first, bypassing the async operator pipeline. Falls back to async on cache miss.
    /// </summary>
    public bool PreferSyncPointLookups { get; set; } = true;

    public QueryPlanner(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer? recordSerializer = null)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer ?? new DefaultRecordSerializer();
        _observedSchemaVersion = catalog.SchemaVersion;
    }

    public ValueTask<QueryResult> ExecuteAsync(Statement stmt, CancellationToken ct = default)
    {
        InvalidateSchemaSensitiveCachesIfNeeded();

        return stmt switch
        {
            CreateTableStatement create => ExecuteCreateTableAsync(create, ct),
            DropTableStatement drop => ExecuteDropTableAsync(drop, ct),
            InsertStatement insert => ExecuteInsertAsync(insert, ct),
            SelectStatement select => ValueTask.FromResult(ExecuteSelect(select)),
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
            _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown statement type: {stmt.GetType().Name}"),
        };
    }

    private void InvalidateSchemaSensitiveCachesIfNeeded()
    {
        long currentVersion = _catalog.SchemaVersion;
        if (currentVersion == _observedSchemaVersion)
            return;

        _triggerBodyCache.Clear();
        _nextRowIdCache.Clear();
        _compiledExpressionCache.Clear();
        _qualifiedMappingFingerprintCache.Clear();
        _tableSchemaArrayCache.Clear();
        _singleColumnOutputSchemaCache.Clear();
        _requiresQualifiedMappingCache.Clear();
        _systemTablesRowsCache = null;
        _systemColumnsRowsCache = null;
        _systemIndexesRowsCache = null;
        _systemViewsRowsCache = null;
        _systemTriggersRowsCache = null;
        _systemColumnsCountCache = null;
        _systemIndexesCountCache = null;

        _observedSchemaVersion = currentVersion;
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
            Nullable = c.IsNullable,
        }).ToArray();

        var schema = new TableSchema { TableName = stmt.TableName, Columns = columns };
        await _catalog.CreateTableAsync(schema, ct);
        _nextRowIdCache.Remove(stmt.TableName);
        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropTableAsync(DropTableStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetTable(stmt.TableName) == null)
            return new QueryResult(0);

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
                    Nullable = add.Column.IsNullable,
                });

                var newSchema = new TableSchema { TableName = stmt.TableName, Columns = newCols.ToArray() };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
                break;
            }

            case DropColumnAction drop:
            {
                int colIdx = schema.GetColumnIndex(drop.ColumnName);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{drop.ColumnName}' not found in table '{stmt.TableName}'.");

                if (schema.Columns[colIdx].IsPrimaryKey)
                    throw new CSharpDbException(ErrorCode.SyntaxError, "Cannot drop primary key column.");

                var newCols = schema.Columns.Where((_, i) => i != colIdx).ToArray();
                if (newCols.Length == 0)
                    throw new CSharpDbException(ErrorCode.SyntaxError, "Cannot drop the last column of a table.");

                // Rewrite all rows without the dropped column
                var tree = _catalog.GetTableTree(stmt.TableName);
                var scan = new TableScanOperator(tree, schema, _recordSerializer);
                await scan.OpenAsync(ct);
                var rowsToRewrite = new List<(long rowId, DbValue[] newRow)>();
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

                var newSchema = new TableSchema { TableName = stmt.TableName, Columns = newCols };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
                break;
            }

            case RenameTableAction rename:
            {
                // Check new name doesn't already exist
                if (_catalog.GetTable(rename.NewTableName) != null)
                    throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Table '{rename.NewTableName}' already exists.");

                var newSchema = new TableSchema { TableName = rename.NewTableName, Columns = schema.Columns };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
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

                var newCols = schema.Columns.Select((col, i) =>
                    i == colIdx
                        ? new ColumnDefinition { Name = renameCol.NewColumnName, Type = col.Type, IsPrimaryKey = col.IsPrimaryKey, Nullable = col.Nullable }
                        : col
                ).ToArray();

                var newSchema = new TableSchema { TableName = stmt.TableName, Columns = newCols };
                await _catalog.UpdateTableSchemaAsync(stmt.TableName, newSchema, ct);
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

        // Validate columns exist and are INTEGER type (MVP: single-column integer indexes)
        if (stmt.Columns.Count != 1)
            throw new CSharpDbException(ErrorCode.SyntaxError, "Only single-column indexes are supported.");

        int colIdx = tableSchema.GetColumnIndex(stmt.Columns[0]);
        if (colIdx < 0)
            throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{stmt.Columns[0]}' not found in table '{stmt.TableName}'.");

        if (tableSchema.Columns[colIdx].Type != DbType.Integer)
            throw new CSharpDbException(ErrorCode.TypeMismatch, "Only INTEGER column indexes are supported.");

        var indexSchema = new IndexSchema
        {
            IndexName = stmt.IndexName,
            TableName = stmt.TableName,
            Columns = stmt.Columns,
            IsUnique = stmt.IsUnique,
        };

        await _catalog.CreateIndexAsync(indexSchema, ct);

        // Populate the index from existing rows
        var tableTree = _catalog.GetTableTree(stmt.TableName);
        var indexStore = _catalog.GetIndexStore(stmt.IndexName);
        var scan = new TableScanOperator(tableTree, tableSchema, _recordSerializer);
        await scan.OpenAsync(ct);

        while (await scan.MoveNextAsync(ct))
        {
            var value = scan.Current[colIdx];
            if (value.IsNull) continue; // Don't index NULL values

            long indexKey = value.AsInteger;

            if (stmt.IsUnique)
            {
                // Check for duplicate
                var existing = await indexStore.FindAsync(indexKey, ct);
                if (existing != null)
                    throw new CSharpDbException(ErrorCode.ConstraintViolation,
                        $"Duplicate key value in unique index '{stmt.IndexName}'.");

                var payload = new byte[8];
                BitConverter.TryWriteBytes(payload, scan.CurrentRowId);
                await indexStore.InsertAsync(indexKey, payload, ct);
            }
            else
            {
                await InsertIntoIndexAsync(indexStore, indexKey, scan.CurrentRowId, ct);
            }
        }

        await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

        return new QueryResult(0);
    }

    private async ValueTask<QueryResult> ExecuteDropIndexAsync(DropIndexStatement stmt, CancellationToken ct)
    {
        if (stmt.IfExists && _catalog.GetIndex(stmt.IndexName) == null)
            return new QueryResult(0);

        await _catalog.DropIndexAsync(stmt.IndexName, ct);
        return new QueryResult(0);
    }

    #endregion

    #region DDL — Views

    private async ValueTask<QueryResult> ExecuteCreateViewAsync(CreateViewStatement stmt, CancellationToken ct)
    {
        if (stmt.IfNotExists && _catalog.GetViewSql(stmt.ViewName) != null)
            return new QueryResult(0);

        // Store the original SQL for the view query.
        // We reconstruct the SQL from the parsed AST to normalize it.
        // For simplicity, we store the SELECT portion as-is by re-serializing from AST.
        // Actually, let's just store the SQL that was parsed. We need to reconstruct it.
        // Simplest approach: serialize the SelectStatement back to SQL text.
        string viewSql = SelectToSql(stmt.Query);

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

    /// <summary>
    /// Reconstructs a SQL SELECT string from a SelectStatement AST node.
    /// Used to store view definitions.
    /// </summary>
    private static string SelectToSql(SelectStatement stmt)
    {
        var parts = new List<string>();
        parts.Add("SELECT");

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

        // ORDER BY
        if (stmt.OrderBy != null)
        {
            parts.Add("ORDER BY");
            var orderParts = stmt.OrderBy.Select(o =>
                ExprToSql(o.Expression) + (o.Descending ? " DESC" : ""));
            parts.Add(string.Join(", ", orderParts));
        }

        // LIMIT
        if (stmt.Limit.HasValue)
            parts.Add($"LIMIT {stmt.Limit.Value}");

        // OFFSET
        if (stmt.Offset.HasValue)
            parts.Add($"OFFSET {stmt.Offset.Value}");

        return string.Join(" ", parts);
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

    private static string ExprToSql(Expression expr) => expr switch
    {
        LiteralExpression lit => lit.Value == null ? "NULL"
            : lit.LiteralType == TokenType.StringLiteral ? $"'{lit.Value.ToString()!.Replace("'", "''")}'"
            : lit.Value.ToString()!,
        ParameterExpression param => $"@{param.Name}",
        ColumnRefExpression col => col.TableAlias != null ? $"{col.TableAlias}.{col.ColumnName}" : col.ColumnName,
        BinaryExpression bin => $"({ExprToSql(bin.Left)} {BinaryOpToSql(bin.Op)} {ExprToSql(bin.Right)})",
        UnaryExpression un => un.Op == TokenType.Not ? $"NOT {ExprToSql(un.Operand)}" : $"-{ExprToSql(un.Operand)}",
        FunctionCallExpression func => func.IsStarArg ? $"{func.FunctionName}(*)"
            : $"{func.FunctionName}({(func.IsDistinct ? "DISTINCT " : "")}{string.Join(", ", func.Arguments.Select(ExprToSql))})",
        LikeExpression like => $"{ExprToSql(like.Operand)}{(like.Negated ? " NOT" : "")} LIKE {ExprToSql(like.Pattern)}",
        InExpression inE => $"{ExprToSql(inE.Operand)}{(inE.Negated ? " NOT" : "")} IN ({string.Join(", ", inE.Values.Select(ExprToSql))})",
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
                await using var result = ExecuteSelect(cte.Query);
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
            return ExecuteSelect(stmt.MainQuery);
        }
        finally
        {
            _cteData = previousCteData;
        }
    }

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
        // For each statement, evaluate expressions that reference NEW/OLD and replace with literal values
        switch (stmt)
        {
            case InsertStatement ins:
                var resolvedRows = ins.ValueRows.Select(row =>
                    row.Select(expr => ResolveExprToLiteral(expr, compositeRow, compositeSchema)).ToList()
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
                    Value = ResolveExprToLiteral(s.Value, compositeRow, compositeSchema),
                }).ToList();
                return new UpdateStatement
                {
                    TableName = upd.TableName,
                    SetClauses = resolvedSets,
                    Where = upd.Where != null ? ResolveExprToLiteral(upd.Where, compositeRow, compositeSchema) : null,
                };

            case DeleteStatement del:
                return new DeleteStatement
                {
                    TableName = del.TableName,
                    Where = del.Where != null ? ResolveExprToLiteral(del.Where, compositeRow, compositeSchema) : null,
                };

            default:
                return stmt;
        }
    }

    /// <summary>
    /// If an expression contains NEW.col or OLD.col references, evaluate them and return a literal.
    /// Otherwise, return the expression unchanged.
    /// </summary>
    private static Expression ResolveExprToLiteral(Expression expr, DbValue[] compositeRow, TableSchema compositeSchema)
    {
        if (!ContainsNewOldRef(expr)) return expr;

        var value = ExpressionEvaluator.Evaluate(expr, compositeRow, compositeSchema);
        return value.Type switch
        {
            DbType.Integer => new LiteralExpression { Value = value.AsInteger, LiteralType = TokenType.IntegerLiteral },
            DbType.Real => new LiteralExpression { Value = value.AsReal, LiteralType = TokenType.RealLiteral },
            DbType.Text => new LiteralExpression { Value = value.AsText, LiteralType = TokenType.StringLiteral },
            _ => new LiteralExpression { Value = null, LiteralType = TokenType.Null },
        };
    }

    private static bool ContainsNewOldRef(Expression expr)
    {
        return expr switch
        {
            ColumnRefExpression col => col.TableAlias != null &&
                (col.TableAlias.Equals("NEW", StringComparison.OrdinalIgnoreCase) ||
                 col.TableAlias.Equals("OLD", StringComparison.OrdinalIgnoreCase)),
            BinaryExpression bin => ContainsNewOldRef(bin.Left) || ContainsNewOldRef(bin.Right),
            UnaryExpression un => ContainsNewOldRef(un.Operand),
            _ => false,
        };
    }

    #endregion

    #region DML

    private async ValueTask<QueryResult> ExecuteInsertAsync(InsertStatement stmt, CancellationToken ct)
    {
        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);

        int inserted = 0;
        foreach (var valueRow in stmt.ValueRows)
        {
            var row = ResolveInsertRow(schema, stmt.ColumnNames, valueRow);

            // BEFORE INSERT triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.Before, TriggerEvent.Insert, null, row, schema, ct);

            var (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(stmt.TableName, schema, tree, row, ct);
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
                    InvalidateRowIdCache(stmt.TableName);
                    (rowId, autoGeneratedRowId) = await ResolveRowIdForInsertAsync(stmt.TableName, schema, tree, row, ct);
                }
            }

            // Maintain indexes
            await InsertIntoAllIndexesAsync(indexes, schema, row, rowId, ct);

            // AFTER INSERT triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.After, TriggerEvent.Insert, null, row, schema, ct);

            inserted++;
        }

        await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

        return new QueryResult(inserted);
    }

    private QueryResult ExecuteSelect(SelectStatement stmt)
    {
        // Fast-path for simple PK equality lookups — bypasses aggregate checks, BuildFromOperator, and TryBuildIndexScan
        if (TryFastPkLookup(stmt, out var fastResult))
            return fastResult;
        // Fast-path for simple indexed equality lookups — bypasses BuildFromOperator and planner setup.
        if (TryFastIndexedLookup(stmt, out var fastIndexedResult))
            return fastIndexedResult;
        // Fast-path for simple table scans with optional WHERE filter — bypasses BuildFromOperator and planner setup.
        if (TryFastSimpleTableScan(stmt, out var fastTableScanResult))
            return fastTableScanResult;

        if (TryBuildSimpleSystemCatalogCountStarQuery(stmt, out var systemCountResult))
            return systemCountResult;
        if (TryBuildSimpleCountStarQuery(stmt, out var countResult))
            return countResult;
        if (TryBuildSimpleScalarAggregateColumnQuery(stmt, out var scalarAggResult))
            return scalarAggResult;
        if (TryBuildSimpleLookupScalarAggregateColumnQuery(stmt, out var lookupScalarAggResult))
            return lookupScalarAggResult;
        if (TryBuildSimpleConstantGroupAggregateColumnQuery(stmt, out var constantGroupAggResult))
            return constantGroupAggResult;

        // Build the FROM operator (single table scan, join tree, or view expansion)
        var (op, schema) = BuildFromOperator(stmt.From);
        bool hasAggregates = stmt.GroupBy != null ||
                             stmt.Having != null ||
                             stmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));
        int? orderByTopN = GetOrderByTopN(stmt);
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
            if (TryGetAggregateDecodeUpperBound(stmt, schema, remainingWhere, out int maxColumnIndex))
                TrySetDecodedColumnUpperBound(op, maxColumnIndex);
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

        if (remainingWhere != null)
            op = new FilterOperator(op, GetOrCompileExpression(remainingWhere, schema));

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
                    else if (TryPushDownColumnProjection(op, columnIndices, outputCols))
                    {
                        // Join operators can project directly, avoiding full composite row materialization.
                    }
                    else
                    {
                        op = new ProjectionOperator(op, columnIndices, outputCols, schema);
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
                    op = new ProjectionOperator(
                        op,
                        Array.Empty<int>(),
                        outputCols,
                        GetOrCompileExpressions(expressions, schema));
                }
            }
        }

        if (stmt.Offset.HasValue)
            op = new OffsetOperator(op, stmt.Offset.Value);

        if (stmt.Limit.HasValue)
            op = new LimitOperator(op, stmt.Limit.Value);

        return new QueryResult(op);
    }

    private static int? GetOrderByTopN(SelectStatement stmt)
    {
        if (stmt.OrderBy is not { Count: > 0 } || !stmt.Limit.HasValue)
            return null;

        long topN = stmt.Limit.Value;
        if (stmt.Offset.HasValue)
            topN += stmt.Offset.Value;

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
        if (schema.Columns[predicateColumnIndex].Type != DbType.Integer)
            return false;

        int pkIdx = schema.PrimaryKeyColumnIndex;
        bool hasIntegerPk = pkIdx >= 0 &&
            pkIdx < schema.Columns.Count &&
            schema.Columns[pkIdx].Type == DbType.Integer;
        bool isPrimaryKeyLookup = hasIntegerPk && predicateColumnIndex == pkIdx;

        long lookupValue = lookup.LookupValue;
        var tableTree = _catalog.GetTableTree(lookup.TableName, _pager);
        IOperator lookupOp;

        if (isPrimaryKeyLookup)
        {
            lookupOp = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, _recordSerializer);
        }
        else
        {
            var indexes = _catalog.GetIndexesForTable(lookup.TableName);
            var matchedIndex = FindLookupIndexForColumn(indexes, schema.Columns[predicateColumnIndex].Name);
            if (matchedIndex == null)
                return false;

            var indexStore = _catalog.GetIndexStore(matchedIndex.IndexName, _pager);
            lookupOp = matchedIndex.IsUnique
                ? new UniqueIndexLookupOperator(indexStore, tableTree, schema, lookupValue, _recordSerializer)
                : new IndexScanOperator(indexStore, tableTree, schema, lookupValue, _recordSerializer);
        }

        bool hasResidual = lookup.HasResidualPredicate;
        int residualColumnIndex = -1;
        if (hasResidual)
        {
            residualColumnIndex = schema.GetColumnIndex(lookup.ResidualPredicateColumn);
            if (residualColumnIndex < 0 || residualColumnIndex >= schema.Columns.Count)
                return false;

            if (lookupOp is not IPreDecodeFilterSupport preDecodeFilterTarget)
                return false;

            preDecodeFilterTarget.SetPreDecodeFilter(
                residualColumnIndex,
                BinaryOp.Equals,
                lookup.ResidualPredicateLiteral);
        }

        if (lookup.SelectStar)
        {
            if (isPrimaryKeyLookup &&
                !hasResidual &&
                PreferSyncPointLookups &&
                tableTree.TryFindCached(lookupValue, out var payload))
            {
                var row = payload != null ? _recordSerializer.Decode(payload) : null;
                result = QueryResult.FromSyncLookup(row, GetSchemaColumnsArray(schema));
                return true;
            }

            result = new QueryResult(lookupOp);
            return true;
        }

        int projectionColumnIndex = schema.GetColumnIndex(lookup.ProjectionColumn);
        if (projectionColumnIndex < 0)
            return false;

        if (isPrimaryKeyLookup && !hasResidual && projectionColumnIndex == pkIdx)
        {
            var outputCols = GetSingleColumnOutputSchema(schema, pkIdx);
            if (PreferSyncPointLookups && tableTree.TryFindCached(lookupValue, out var payload))
            {
                DbValue[]? row = null;
                if (payload != null)
                    row = new[] { DbValue.FromInteger(lookupValue) };

                result = QueryResult.FromSyncLookup(row, outputCols);
                return true;
            }

            result = new QueryResult(new PrimaryKeyProjectionLookupOperator(tableTree, lookupValue, outputCols));
            return true;
        }

        int maxDecodedColumn = projectionColumnIndex;
        if (residualColumnIndex > maxDecodedColumn)
            maxDecodedColumn = residualColumnIndex;
        if (maxDecodedColumn >= 0)
            TrySetDecodedColumnUpperBound(lookupOp, maxDecodedColumn);

        var output = GetSingleColumnOutputSchema(schema, projectionColumnIndex);
        IOperator op = new ProjectionOperator(lookupOp, new[] { projectionColumnIndex }, output, schema);
        result = new QueryResult(op);
        return true;
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
            if (tableTree.TryFindCached(lookupValue, out var payload))
            {
                var row = payload != null ? _recordSerializer.Decode(payload) : null;
                var schemaArray = GetSchemaColumnsArray(schema);
                result = QueryResult.FromSyncLookup(row, schemaArray);
                return true;
            }
            // Cache miss — fall through to create operator (existing async path)
        }

        // SELECT * — just PrimaryKeyLookupOperator
        if (selectStar)
        {
            IOperator op = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, _recordSerializer);
            if (residualWhere != null && TryPushDownSimplePreDecodeFilter(op, residualWhere, schema, out var pushedWhere))
                residualWhere = pushedWhere;
            if (residualWhere != null)
                op = new FilterOperator(op, GetOrCompileExpression(residualWhere, schema));
            result = new QueryResult(op);
            return true;
        }

        if (residualWhere == null &&
            TryResolveUnaliasedPrimaryKeyProjectionCount(stmt.Columns, schema, pkIdx, out int projectedPkCount))
        {
            ColumnDefinition[] pkOutputCols = projectedPkCount == 1
                ? GetSingleColumnOutputSchema(schema, pkIdx)
                : BuildRepeatedColumnOutputSchema(schema.Columns[pkIdx], projectedPkCount);

            if (PreferSyncPointLookups && tableTree.TryFindCached(lookupValue, out var payload))
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
                if (PreferSyncPointLookups && tableTree.TryFindCached(lookupValue, out var payload))
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
            var pkOp = new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, _recordSerializer);
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
                projOp = new FilterOperator(projOp, GetOrCompileExpression(remainingResidual, schema));
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
        var tree = _catalog.GetTableTree(simpleRef.TableName);
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

        result = new QueryResult(new CountStarTableOperator(tree, outputSchema));
        return true;
    }

    private bool TryBuildSimpleSystemCatalogCountStarQuery(SelectStatement stmt, out QueryResult result)
    {
        result = null!;

        if (stmt.From is not SimpleTableRef simpleRef)
            return false;
        if (!TryNormalizeSystemCatalogTableName(simpleRef.TableName, out string normalized))
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
            "sys.views" => _catalog.GetViewNames().Count,
            "sys.triggers" => _catalog.GetTriggers().Count,
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
            count += index.Columns.Count;

        _systemIndexesCountCache = count;
        return count;
    }

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
        var tableTree = _catalog.GetTableTree(simpleRef.TableName);
        result = new QueryResult(new ScalarAggregateTableOperator(
            tableTree,
            columnIndex,
            func.FunctionName,
            outputSchema,
            isDistinct: func.IsDistinct,
            recordSerializer: _recordSerializer));
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

        var lookupOp = TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out var remainingWhere);
        if (lookupOp == null || remainingWhere != null)
            return false;

        var outputSchema = BuildAggregateOutputSchema(stmt.Columns, schema);
        result = lookupOp switch
        {
            PrimaryKeyLookupOperator pk => new QueryResult(new ScalarAggregateLookupOperator(
                pk.TableTree,
                pk.SeekKey,
                columnIndex,
                func.FunctionName,
                outputSchema,
                isDistinct: func.IsDistinct,
                recordSerializer: _recordSerializer)),
            IndexScanOperator idx => new QueryResult(new ScalarAggregateLookupOperator(
                idx.IndexStore,
                idx.TableTree,
                idx.SeekValue,
                columnIndex,
                func.FunctionName,
                outputSchema,
                isDistinct: func.IsDistinct,
                recordSerializer: _recordSerializer)),
            UniqueIndexLookupOperator uniq => new QueryResult(new ScalarAggregateLookupOperator(
                uniq.IndexStore,
                uniq.TableTree,
                uniq.SeekValue,
                columnIndex,
                func.FunctionName,
                outputSchema,
                isDistinct: func.IsDistinct,
                recordSerializer: _recordSerializer)),
            _ => null!,
        };

        return result != null;
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
        var tableTree = _catalog.GetTableTree(simpleRef.TableName);
        result = new QueryResult(new ScalarAggregateTableOperator(
            tableTree,
            columnIndex,
            func.FunctionName,
            outputSchema,
            isDistinct: func.IsDistinct,
            emitOnEmptyInput: false,
            recordSerializer: _recordSerializer));
        return true;
    }

    private async ValueTask<QueryResult> ExecuteDeleteAsync(DeleteStatement stmt, CancellationToken ct)
    {
        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);

        // Collect rows to delete (can't modify tree while iterating)
        var rowsToDelete = new List<(long rowId, DbValue[] row)>();
        var scan = new TableScanOperator(tree, schema, _recordSerializer);
        await scan.OpenAsync(ct);
        while (await scan.MoveNextAsync(ct))
        {
            if (stmt.Where != null)
            {
                var result = ExpressionEvaluator.Evaluate(stmt.Where, scan.Current, schema);
                if (!result.IsTruthy) continue;
            }
            rowsToDelete.Add((scan.CurrentRowId, (DbValue[])scan.Current.Clone()));
        }

        foreach (var (rowId, row) in rowsToDelete)
        {
            // BEFORE DELETE triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.Before, TriggerEvent.Delete, row, null, schema, ct);

            await tree.DeleteAsync(rowId, ct);

            // Maintain indexes
            await DeleteFromAllIndexesAsync(indexes, schema, row, rowId, ct);

            // AFTER DELETE triggers
            await FireTriggersAsync(stmt.TableName, TriggerTiming.After, TriggerEvent.Delete, row, null, schema, ct);
        }

        await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

        return new QueryResult(rowsToDelete.Count);
    }

    private async ValueTask<QueryResult> ExecuteUpdateAsync(UpdateStatement stmt, CancellationToken ct)
    {
        var schema = GetSchema(stmt.TableName);
        var tree = _catalog.GetTableTree(stmt.TableName);
        var indexes = _catalog.GetIndexesForTable(stmt.TableName);
        int pkIdx = schema.PrimaryKeyColumnIndex;
        bool hasIntegerPrimaryKey = pkIdx >= 0 && schema.Columns[pkIdx].Type == DbType.Integer;

        // Collect rows to update
        var updates = new List<(long rowId, DbValue[] oldRow, DbValue[] newRow)>();
        var scan = new TableScanOperator(tree, schema, _recordSerializer);
        await scan.OpenAsync(ct);
        while (await scan.MoveNextAsync(ct))
        {
            if (stmt.Where != null)
            {
                var result = ExpressionEvaluator.Evaluate(stmt.Where, scan.Current, schema);
                if (!result.IsTruthy) continue;
            }

            var oldRow = (DbValue[])scan.Current.Clone();
            var newRow = (DbValue[])scan.Current.Clone();
            foreach (var set in stmt.SetClauses)
            {
                int colIdx = schema.GetColumnIndex(set.ColumnName);
                if (colIdx < 0)
                    throw new CSharpDbException(ErrorCode.ColumnNotFound, $"Column '{set.ColumnName}' not found.");
                newRow[colIdx] = ExpressionEvaluator.Evaluate(set.Value, scan.Current, schema);
            }
            updates.Add((scan.CurrentRowId, oldRow, newRow));
        }

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

        await _catalog.PersistRootPageChangesAsync(stmt.TableName, ct);

        return new QueryResult(updates.Count);
    }

    #endregion

    #region FROM Clause / JOIN / View Expansion

    /// <summary>
    /// Recursively builds an operator tree from a TableRef AST node.
    /// Returns the operator and the schema (with qualified column mappings for JOINs).
    /// If the table name references a view, expands the view.
    /// </summary>
    private (IOperator op, TableSchema schema) BuildFromOperator(TableRef tableRef)
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
                // Re-parse the stored SQL and build operator pipeline
                var viewStmt = (SelectStatement)Parser.Parse(viewSql);
                var (viewOp, viewSchema) = BuildFromOperator(viewStmt.From);

                bool hasAggregates = viewStmt.GroupBy != null ||
                                     viewStmt.Having != null ||
                                     viewStmt.Columns.Any(c => c.Expression != null && ContainsAggregate(c.Expression));

                // Aggregate optimization for simple view pipelines.
                if (hasAggregates)
                {
                    if (TryGetAggregateDecodeUpperBound(viewStmt, viewSchema, viewStmt.Where, out int maxColumnIndex))
                        TrySetDecodedColumnUpperBound(viewOp, maxColumnIndex);
                }

                // Apply view's WHERE
                if (viewStmt.Where != null)
                    viewOp = new FilterOperator(viewOp, GetOrCompileExpression(viewStmt.Where, viewSchema));

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
                    // Apply view's projection if not SELECT *
                    var expressions = viewStmt.Columns.Select(c => c.Expression!).ToArray();
                    var outputCols = new ColumnDefinition[expressions.Length];
                    for (int i = 0; i < expressions.Length; i++)
                        outputCols[i] = InferColumnDef(expressions[i], viewStmt.Columns[i].Alias, viewSchema, i);
                    viewOp = new ProjectionOperator(
                        viewOp,
                        Array.Empty<int>(),
                        outputCols,
                        GetOrCompileExpressions(expressions, viewSchema));

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
            IOperator op = new TableScanOperator(tree, schema, _recordSerializer);

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

            return (op, qualifiedTableSchema);
        }

        if (tableRef is JoinTableRef join)
        {
            var (leftOp, leftSchema) = BuildFromOperator(join.Left);
            var (rightOp, rightSchema) = BuildFromOperator(join.Right);

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
                    rightSchema.Columns.Count,
                    leftSchema.Columns.Count,
                    swappedCompositeSchema,
                    out var swappedHashJoinOp))
                {
                    swappedJoinOp = swappedHashJoinOp!;
                }
                else
                {
                    int? swappedEstimatedOutputRowCount = TryEstimateJoinOutputRowCount(rewrittenJoin);
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
                leftSchema.Columns.Count,
                rightSchema.Columns.Count,
                compositeSchema,
                out var hashJoinOp))
            {
                return (hashJoinOp!, compositeSchema);
            }

            int? estimatedOutputRowCount = TryEstimateJoinOutputRowCount(join);
            int? rightRowCapacityHint = TryEstimateTableRefRowCountCapacityHint(join.Right);
            var joinOp = new NestedLoopJoinOperator(
                leftOp, rightOp, join.JoinType, join.Condition,
                compositeSchema, leftSchema.Columns.Count, rightSchema.Columns.Count,
                estimatedOutputRowCount, rightRowCapacityHint);

            return (joinOp, compositeSchema);
        }

        throw new CSharpDbException(ErrorCode.Unknown, $"Unknown table ref type: {tableRef.GetType().Name}");
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

        // Index nested-loop currently supports single key lookups only.
        if (leftKeyIndices.Length != 1 || rightKeyIndices.Length != 1)
            return false;

        int leftKeyIndex = leftKeyIndices[0];
        int rightKeyIndex = rightKeyIndices[0];
        if (rightKeyIndex < 0 || rightKeyIndex >= rightSchema.Columns.Count)
            return false;

        var rightKeyColumn = rightSchema.Columns[rightKeyIndex];
        if (rightKeyColumn.Type != DbType.Integer)
            return false;

        var rightTableTree = _catalog.GetTableTree(rightSimple.TableName, _pager);
        IIndexStore? rightIndexStore = null;
        bool lookupIsUnique = false;
        bool lookupIsPrimaryKey = false;

        int rightPkIndex = rightSchema.PrimaryKeyColumnIndex;
        bool usesPrimaryKeyLookup =
            rightPkIndex == rightKeyIndex &&
            rightSchema.Columns[rightPkIndex].Type == DbType.Integer;

        if (!usesPrimaryKeyLookup)
        {
            // Secondary indexes in this engine omit NULL keys.
            // Preserve current expression semantics by using index lookup only
            // when the join key column is non-nullable.
            if (rightKeyColumn.Nullable)
                return false;

            var indexes = _catalog.GetIndexesForTable(rightSimple.TableName);
            IndexSchema? selected = null;
            foreach (var idx in indexes)
            {
                if (idx.Columns.Count != 1 ||
                    !string.Equals(idx.Columns[0], rightKeyColumn.Name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (idx.IsUnique)
                {
                    selected = idx;
                    break;
                }

                selected ??= idx;
            }

            if (selected == null)
                return false;

            rightIndexStore = _catalog.GetIndexStore(selected.IndexName, _pager);
            lookupIsUnique = selected.IsUnique;
        }
        else
        {
            lookupIsUnique = true;
            lookupIsPrimaryKey = true;
        }

        bool hasOuterEstimate = TryEstimateTableRefRowCount(join.Left, out long outerRows);
        bool hasInnerEstimate = TryEstimateTableRefRowCount(join.Right, out long innerRows);
        if (hasOuterEstimate && hasInnerEstimate)
        {
            if (!ShouldPreferIndexNestedLoop(outerRows, innerRows, lookupIsPrimaryKey, lookupIsUnique))
                return false;
        }
        else if (!lookupIsUnique)
        {
            // Without any cardinality hint, keep non-unique lookups on hash join.
            return false;
        }

        int? estimatedOutputRowCount = lookupIsUnique
            ? ToCapacityHint(outerRows)
            : EstimateJoinOutputRowCount(join.JoinType, hasOuterEstimate, outerRows, hasInnerEstimate, innerRows);

        indexNestedJoinOp = new IndexNestedLoopJoinOperator(
            leftOp,
            rightTableTree,
            rightIndexStore,
            join.JoinType,
            leftKeyIndex,
            leftSchema.Columns.Count,
            rightSchema.Columns.Count,
            residualCondition,
            compositeSchema,
            _recordSerializer,
            estimatedOutputRowCount);

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
        int leftColumnCount,
        int rightColumnCount,
        TableSchema compositeSchema,
        out IOperator? hashJoinOp)
    {
        hashJoinOp = null;

        if (join.JoinType is JoinType.Cross || join.Condition == null)
            return false;

        if (!TryAnalyzeHashJoinCondition(
                join.Condition,
                compositeSchema,
                leftColumnCount,
                out var leftKeyIndices,
                out var rightKeyIndices,
                out var residualCondition))
        {
            return false;
        }

        bool hasLeftEstimate = TryEstimateTableRefRowCount(join.Left, out long leftRows);
        bool hasRightEstimate = TryEstimateTableRefRowCount(join.Right, out long rightRows);

        // HashJoinOperator defaults to building the right input side. For INNER joins,
        // flip the build side when left is much smaller.
        bool buildRightSide = true;
        if (join.JoinType == JoinType.Inner &&
            hasLeftEstimate &&
            hasRightEstimate &&
            ShouldSwapInnerHashJoinBuild(leftRows, rightRows))
        {
            buildRightSide = false;
        }

        int? buildRowCapacityHint = null;
        if (buildRightSide ? hasRightEstimate : hasLeftEstimate)
        {
            long buildRows = buildRightSide ? rightRows : leftRows;
            buildRowCapacityHint = ToCapacityHint(buildRows);
        }

        int? estimatedOutputRowCount = EstimateJoinOutputRowCount(
            join.JoinType,
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
            leftColumnCount,
            rightColumnCount,
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

    private int? TryEstimateJoinOutputRowCount(JoinTableRef join)
    {
        bool hasLeftEstimate = TryEstimateTableRefRowCount(join.Left, out long leftRows);
        bool hasRightEstimate = TryEstimateTableRefRowCount(join.Right, out long rightRows);
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
        if (!hasLeftEstimate && !hasRightEstimate)
            return null;

        long estimate = joinType switch
        {
            JoinType.Cross => hasLeftEstimate && hasRightEstimate
                ? SafeMultiply(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
            JoinType.LeftOuter => hasLeftEstimate
                ? leftRows
                : rightRows,
            JoinType.RightOuter => hasRightEstimate
                ? rightRows
                : leftRows,
            JoinType.Inner => hasLeftEstimate && hasRightEstimate
                ? Math.Max(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
            _ => hasLeftEstimate && hasRightEstimate
                ? Math.Max(leftRows, rightRows)
                : hasLeftEstimate
                    ? leftRows
                    : rightRows,
        };

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

    private static bool ShouldSwapInnerHashJoinBuild(long leftRows, long rightRows)
    {
        leftRows = Math.Max(leftRows, 1);
        rightRows = Math.Max(rightRows, 1);

        // Keep a conservative threshold to avoid churn when sizes are near-equal.
        return SafeMultiply(leftRows, 2) < rightRows;
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
        var indexes = _catalog.GetIndexesForTable(tableName);

        // Fast path: single equality lookup term.
        if (TryPickLookupCandidate(where, schema, indexes, hasIntegerPk, pkIdx, out var singleCandidate))
        {
            remaining = null;
            return BuildLookupOperator(tableName, schema, singleCandidate.IsPrimaryKey, singleCandidate.Index, singleCandidate.LookupValue);
        }

        // Fast path: two-term AND where one side can be consumed as the lookup and the other stays as residual.
        if (where is BinaryExpression { Op: BinaryOp.And } andExpr)
        {
            bool hasLeft = TryPickLookupCandidate(andExpr.Left, schema, indexes, hasIntegerPk, pkIdx, out var leftCandidate);
            bool hasRight = TryPickLookupCandidate(andExpr.Right, schema, indexes, hasIntegerPk, pkIdx, out var rightCandidate);

            if (hasLeft || hasRight)
            {
                bool useLeft = hasLeft && (!hasRight || leftCandidate.Rank <= rightCandidate.Rank);
                var selected = useLeft ? leftCandidate : rightCandidate;
                remaining = useLeft ? andExpr.Right : andExpr.Left;
                return BuildLookupOperator(tableName, schema, selected.IsPrimaryKey, selected.Index, selected.LookupValue);
            }
        }

        remaining = where;
        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        int selectedConjunctIndex = -1;
        int selectedRank = int.MaxValue;
        LookupCandidate selectedCandidate = default;

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (!TryPickLookupCandidate(conjuncts[i], schema, indexes, hasIntegerPk, pkIdx, out var candidate))
                continue;

            if (candidate.Rank < selectedRank)
            {
                selectedRank = candidate.Rank;
                selectedConjunctIndex = i;
                selectedCandidate = candidate;

                if (candidate.Rank == 0)
                    break;
            }
        }

        if (selectedConjunctIndex < 0)
            return null;
        IOperator lookupOp = BuildLookupOperator(tableName, schema, selectedCandidate.IsPrimaryKey, selectedCandidate.Index, selectedCandidate.LookupValue);

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
        int Rank);

    private static bool TryPickLookupCandidate(
        Expression expression,
        TableSchema schema,
        IReadOnlyList<IndexSchema> indexes,
        bool hasIntegerPk,
        int pkIdx,
        out LookupCandidate candidate)
    {
        candidate = default;

        if (!TryExtractIntegerEqualityLookupTerm(expression, schema, out int columnIndex, out long lookupValue))
            return false;

        if (hasIntegerPk && columnIndex == pkIdx)
        {
            candidate = new LookupCandidate(lookupValue, IsPrimaryKey: true, Index: null, Rank: 0);
            return true;
        }

        string columnName = schema.Columns[columnIndex].Name;
        var matchedIndex = FindLookupIndexForColumn(indexes, columnName);
        if (matchedIndex == null)
            return false;

        candidate = new LookupCandidate(
            lookupValue,
            IsPrimaryKey: false,
            Index: matchedIndex,
            Rank: matchedIndex.IsUnique ? 1 : 2);
        return true;
    }

    private IOperator BuildLookupOperator(
        string tableName,
        TableSchema schema,
        bool isPrimaryKey,
        IndexSchema? index,
        long lookupValue)
    {
        var tableTree = _catalog.GetTableTree(tableName, _pager);
        if (isPrimaryKey)
            return new PrimaryKeyLookupOperator(tableTree, schema, lookupValue, _recordSerializer);

        var indexStore = _catalog.GetIndexStore(index!.IndexName, _pager);
        return index.IsUnique
            ? new UniqueIndexLookupOperator(indexStore, tableTree, schema, lookupValue, _recordSerializer)
            : new IndexScanOperator(indexStore, tableTree, schema, lookupValue, _recordSerializer);
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

    private static IndexSchema? FindLookupIndexForColumn(
        IReadOnlyList<IndexSchema> indexes,
        string columnName)
    {
        IndexSchema? firstNonUnique = null;

        for (int i = 0; i < indexes.Count; i++)
        {
            var idx = indexes[i];
            if (idx.Columns.Count != 1 ||
                !string.Equals(idx.Columns[0], columnName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (idx.IsUnique)
                return idx;

            firstNonUnique ??= idx;
        }

        return firstNonUnique;
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

        if (orderBy.Expression is not ColumnRefExpression columnRef)
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
        if (orderColumn.Type != DbType.Integer)
            return false;

        // INTEGER PRIMARY KEY is physically the table B+tree key, and table scan is key-ordered.
        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (pkIdx == orderColumnIndex && currentSource is TableScanOperator)
            return true;

        if (!stmt.Limit.HasValue)
            return false;

        // Secondary indexes currently skip NULL values.
        // To preserve ORDER BY semantics, only use index order for non-nullable columns.
        if (orderColumn.Nullable)
            return false;

        ExtractOrderedIndexRange(where, schema, orderColumnIndex, out var scanRange, out remainingWhere);

        var indexes = _catalog.GetIndexesForTable(tableRef.TableName);
        foreach (var idx in indexes)
        {
            if (idx.Columns.Count != 1 ||
                !string.Equals(idx.Columns[0], orderColumn.Name, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var indexStore = _catalog.GetIndexStore(idx.IndexName, _pager);
            var tableTree = _catalog.GetTableTree(tableRef.TableName, _pager);
            replacementSource = new IndexOrderedScanOperator(indexStore, tableTree, schema, scanRange, _recordSerializer);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Fast path for simple secondary-index equality lookups:
    /// SELECT * / columns FROM table WHERE indexed_col = literal [AND ...].
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
        if (stmt.OrderBy is { Count: > 0 })
            return false;
        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        var schema = _catalog.GetTable(simpleRef.TableName);
        if (schema == null)
            return false;

        var indexOp = TryBuildIndexScan(simpleRef.TableName, stmt.Where, schema, out var remainingWhere);
        if (indexOp == null)
            return false;

        IOperator op = indexOp;
        if (remainingWhere != null && TryPushDownSimplePreDecodeFilter(op, remainingWhere, schema, out var pushedWhere))
            remainingWhere = pushedWhere;

        if (stmt.Columns.Any(c => c.IsStar))
        {
            if (remainingWhere != null)
                op = new FilterOperator(op, GetOrCompileExpression(remainingWhere, schema));
            result = new QueryResult(op);
            return true;
        }

        if (TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out var outputCols))
        {
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
                op = new FilterOperator(op, GetOrCompileExpression(remainingWhere, schema));

            op = new ProjectionOperator(op, columnIndices, outputCols, schema);
            result = new QueryResult(op);
            return true;
        }

        // Expression projections fall back to the general path.
        return false;
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
        if (stmt.OrderBy is { Count: > 0 })
            return false;
        if (stmt.Limit.HasValue || stmt.Offset.HasValue)
            return false;

        var schema = _catalog.GetTable(simpleRef.TableName);
        if (schema == null)
            return false;

        var tableTree = _catalog.GetTableTree(simpleRef.TableName, _pager);
        var scanOp = new TableScanOperator(tableTree, schema, _recordSerializer);
        IOperator op = scanOp;
        var remainingWhere = stmt.Where;

        if (remainingWhere != null &&
            TryPushDownSimplePreDecodeFilter(scanOp, remainingWhere, schema, out var pushedWhere))
        {
            remainingWhere = pushedWhere;
        }

        if (stmt.Columns.Any(c => c.IsStar))
        {
            if (remainingWhere != null)
                op = new FilterOperator(op, GetOrCompileExpression(remainingWhere, schema));

            result = new QueryResult(op);
            return true;
        }

        if (TryBuildColumnProjection(stmt.Columns, schema, out var columnIndices, out var outputCols))
        {
            int maxCol = -1;
            for (int i = 0; i < columnIndices.Length; i++)
                if (columnIndices[i] > maxCol) maxCol = columnIndices[i];

            if (remainingWhere != null &&
                !TryAccumulateMaxReferencedColumn(remainingWhere, schema, ref maxCol))
            {
                return false;
            }

            if (maxCol >= 0)
                scanOp.SetDecodedColumnUpperBound(maxCol);

            if (remainingWhere != null)
                op = new FilterOperator(op, GetOrCompileExpression(remainingWhere, schema));

            op = new ProjectionOperator(op, columnIndices, outputCols, schema);
            result = new QueryResult(op);
            return true;
        }

        // Expression projections fall back to the general path.
        return false;
    }

    private static void ExtractOrderedIndexRange(
        Expression? where,
        TableSchema schema,
        int orderColumnIndex,
        out IndexScanRange range,
        out Expression? remaining)
    {
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
            if (!TryExtractOrderedIndexRangeTerm(conjuncts[i], schema, orderColumnIndex, out var term))
            {
                residualTerms.Add(conjuncts[i]);
                continue;
            }

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

    private static bool TryPushDownSimplePreDecodeFilter(
        IOperator op,
        Expression where,
        TableSchema schema,
        out Expression? remaining)
    {
        remaining = where;

        if (op is not IPreDecodeFilterSupport preDecodeFilterTarget)
            return false;

        if (TryExtractPushdownPredicate(where, schema, out int columnIndex, out BinaryOp opToApply, out DbValue literal, out var residual))
        {
            preDecodeFilterTarget.SetPreDecodeFilter(columnIndex, opToApply, literal);
            remaining = residual;
            return true;
        }

        return false;
    }

    private static bool TryExtractPushdownPredicate(
        Expression where,
        TableSchema schema,
        out int columnIndex,
        out BinaryOp opToApply,
        out DbValue literal,
        out Expression? remaining)
    {
        columnIndex = -1;
        opToApply = BinaryOp.Equals;
        literal = DbValue.Null;
        remaining = where;

        if (where is BinaryExpression singleBin &&
            IsPushdownComparison(singleBin.Op) &&
            TryGetPushdownOperands(singleBin, schema, out columnIndex, out opToApply, out literal))
        {
            remaining = null;
            return true;
        }

        if (where is not BinaryExpression { Op: BinaryOp.And })
            return false;

        var conjuncts = new List<Expression>();
        CollectAndConjuncts(where, conjuncts);

        int selectedConjunctIndex = -1;
        int selectedRank = int.MaxValue;

        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (conjuncts[i] is not BinaryExpression bin || !IsPushdownComparison(bin.Op))
                continue;

            if (!TryGetPushdownOperands(bin, schema, out int candidateColumnIndex, out BinaryOp candidateOp, out DbValue candidateLiteral))
                continue;

            int candidateRank = GetPushdownComparisonRank(candidateOp);
            if (candidateRank >= selectedRank)
                continue;

            selectedConjunctIndex = i;
            selectedRank = candidateRank;
            columnIndex = candidateColumnIndex;
            opToApply = candidateOp;
            literal = candidateLiteral;

            if (candidateRank == 0)
                break;
        }

        if (selectedConjunctIndex < 0)
            return false;

        if (conjuncts.Count == 1)
        {
            remaining = null;
            return true;
        }

        var residualTerms = new List<Expression>(conjuncts.Count - 1);
        for (int i = 0; i < conjuncts.Count; i++)
        {
            if (i == selectedConjunctIndex)
                continue;

            residualTerms.Add(conjuncts[i]);
        }

        remaining = CombineConjuncts(residualTerms);
        return true;
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

        if (bin.Left is ColumnRefExpression leftCol && bin.Right is LiteralExpression rightLit)
            return TryResolvePushdownOperand(leftCol, rightLit, schema, out columnIndex, out literal);

        if (bin.Right is ColumnRefExpression rightCol && bin.Left is LiteralExpression leftLit)
        {
            op = ReverseComparison(bin.Op);
            return TryResolvePushdownOperand(rightCol, leftLit, schema, out columnIndex, out literal);
        }

        return false;
    }

    private static bool TryResolvePushdownOperand(
        ColumnRefExpression col,
        LiteralExpression lit,
        TableSchema schema,
        out int columnIndex,
        out DbValue literal)
    {
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

        return true;
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
        foreach (var idx in indexes)
        {
            int colIdx = schema.GetColumnIndex(idx.Columns[0]);
            if (colIdx < 0) continue;

            var value = row[colIdx];
            if (value.IsNull) continue; // Don't index NULL values

            long indexKey = value.AsInteger;
            var indexStore = _catalog.GetIndexStore(idx.IndexName);

            if (idx.IsUnique)
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
                await InsertIntoIndexAsync(indexStore, indexKey, rowId, ct);
            }
        }
    }

    private async ValueTask DeleteFromAllIndexesAsync(
        IReadOnlyList<IndexSchema> indexes, TableSchema schema, DbValue[] row, long rowId, CancellationToken ct)
    {
        foreach (var idx in indexes)
        {
            int colIdx = schema.GetColumnIndex(idx.Columns[0]);
            if (colIdx < 0) continue;

            var value = row[colIdx];
            if (value.IsNull) continue;

            long indexKey = value.AsInteger;
            var indexStore = _catalog.GetIndexStore(idx.IndexName);
            await DeleteFromIndexAsync(indexStore, indexKey, rowId, ct);
        }
    }

    private async ValueTask UpdateAllIndexesAsync(
        IReadOnlyList<IndexSchema> indexes, TableSchema schema,
        DbValue[] oldRow, DbValue[] newRow, long oldRowId, long newRowId, CancellationToken ct)
    {
        foreach (var idx in indexes)
        {
            int colIdx = schema.GetColumnIndex(idx.Columns[0]);
            if (colIdx < 0) continue;

            var oldValue = oldRow[colIdx];
            var newValue = newRow[colIdx];

            // If neither indexed value nor rowid changed, no maintenance needed.
            if (oldValue.Equals(newValue) && oldRowId == newRowId) continue;

            // Remove old entry
            if (!oldValue.IsNull)
            {
                long oldKey = oldValue.AsInteger;
                var indexStore = _catalog.GetIndexStore(idx.IndexName);
                await DeleteFromIndexAsync(indexStore, oldKey, oldRowId, ct);
            }

            // Add new entry
            if (!newValue.IsNull)
            {
                long newKey = newValue.AsInteger;
                var indexStore = _catalog.GetIndexStore(idx.IndexName);

                if (idx.IsUnique)
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
                    await InsertIntoIndexAsync(indexStore, newKey, newRowId, ct);
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
            await indexStore.DeleteAsync(indexKey, ct);
            await indexStore.InsertAsync(indexKey, newPayload, ct);
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

        await indexStore.DeleteAsync(indexKey, ct);
        if (ms.Length > 0)
            await indexStore.InsertAsync(indexKey, ms.ToArray(), ct);
    }

    #endregion

    #region Helpers

    private static bool ContainsAggregate(Expression expr)
    {
        return expr switch
        {
            FunctionCallExpression => true,
            BinaryExpression bin => ContainsAggregate(bin.Left) || ContainsAggregate(bin.Right),
            UnaryExpression un => ContainsAggregate(un.Operand),
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
                }
                : sourceColumn;
        }

        return true;
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

            case "sys.views":
                columns = SystemViewsColumns;
                rows = BuildSystemViewsRows();
                break;

            case "sys.triggers":
                columns = SystemTriggersColumns;
                rows = BuildSystemTriggersRows();
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
            for (int i = 0; i < index.Columns.Count; i++)
            {
                rows.Add(
                [
                    DbValue.FromText(index.IndexName),
                    DbValue.FromText(index.TableName),
                    DbValue.FromText(index.Columns[i]),
                    DbValue.FromInteger(i + 1),
                    DbValue.FromInteger(index.IsUnique ? 1 : 0),
                ]);
            }
        }

        _systemIndexesRowsCache = rows;
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

    private static DbType MapType(TokenType token) => token switch
    {
        TokenType.Integer => DbType.Integer,
        TokenType.Real => DbType.Real,
        TokenType.Text => DbType.Text,
        TokenType.Blob => DbType.Blob,
        _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unknown type token: {token}"),
    };

    private DbValue[] ResolveInsertRow(TableSchema schema, List<string>? columnNames, List<Expression> values)
    {
        var row = new DbValue[schema.Columns.Count];
        for (int i = 0; i < row.Length; i++)
            row[i] = DbValue.Null;

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
                row[colIdx] = ExpressionEvaluator.Evaluate(values[i], Array.Empty<DbValue>(), schema);
            }
        }
        else
        {
            if (values.Count != schema.Columns.Count)
                throw new CSharpDbException(ErrorCode.SyntaxError,
                    $"Expected {schema.Columns.Count} values, got {values.Count}.");

            for (int i = 0; i < values.Count; i++)
                row[i] = ExpressionEvaluator.Evaluate(values[i], Array.Empty<DbValue>(), schema);
        }

        return row;
    }

    private async ValueTask<(long RowId, bool AutoGenerated)> ResolveRowIdForInsertAsync(
        string tableName, TableSchema schema, BTree tree, DbValue[] row, CancellationToken ct)
    {
        int pkIdx = schema.PrimaryKeyColumnIndex;
        if (pkIdx >= 0 && schema.Columns[pkIdx].Type == DbType.Integer)
        {
            if (!row[pkIdx].IsNull)
            {
                long explicitKey = row[pkIdx].AsInteger;
                if (_nextRowIdCache.TryGetValue(tableName, out long next) && explicitKey >= next)
                    _nextRowIdCache[tableName] = checked(explicitKey + 1);
                return (explicitKey, false);
            }

            long generatedKey = await AllocateRowIdAsync(tableName, tree, ct);
            row[pkIdx] = DbValue.FromInteger(generatedKey);
            return (generatedKey, true);
        }

        return (await AllocateRowIdAsync(tableName, tree, ct), true);
    }

    private async ValueTask<long> AllocateRowIdAsync(string tableName, BTree tree, CancellationToken ct)
    {
        if (!_nextRowIdCache.TryGetValue(tableName, out long nextRowId))
            nextRowId = await LoadNextRowIdAsync(tree, ct);

        _nextRowIdCache[tableName] = checked(nextRowId + 1);
        return nextRowId;
    }

    private void InvalidateRowIdCache(string tableName)
    {
        _nextRowIdCache.Remove(tableName);
    }

    private static async ValueTask<long> LoadNextRowIdAsync(BTree tree, CancellationToken ct)
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

    #endregion
}
