using System.Globalization;
using CSharpDB.Admin.Models;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Helpers;

internal sealed class QueryPagingPlan
{
    private const string ResultCteBaseName = "__admin_query_results";

    private readonly string _originalSql;
    private readonly IReadOnlyList<CteDefinition> _baseCtes;
    private readonly QueryStatement _baseQuery;
    private readonly SelectStatement? _baseSelect;
    private readonly string _resultCteName;
    private static readonly IReadOnlyDictionary<int, DataGridFilterMatchMode> EmptyFilterModes =
        new Dictionary<int, DataGridFilterMatchMode>();

    private QueryPagingPlan(
        string originalSql,
        IReadOnlyList<CteDefinition> baseCtes,
        QueryStatement baseQuery,
        string resultCteName)
    {
        _originalSql = originalSql;
        _baseCtes = baseCtes;
        _baseQuery = baseQuery;
        _baseSelect = baseQuery as SelectStatement;
        _resultCteName = resultCteName;
    }

    public static QueryPagingPlan Parse(string sql)
    {
        string trimmedSql = TrimTrailingSemicolon(sql);
        IReadOnlyList<string> statements;
        try
        {
            statements = SqlScriptSplitter.SplitExecutableStatements(trimmedSql);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse SQL script: {ex.Message}", ex);
        }

        if (statements.Count != 1)
            throw new InvalidOperationException("Only single query statements can use paged query results.");

        Statement statement;
        try
        {
            statement = Parser.Parse(statements[0]);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to parse query: {ex.Message}", ex);
        }

        return statement switch
        {
            QueryStatement query => new QueryPagingPlan(
                trimmedSql,
                [],
                query,
                BuildUniqueResultCteName([])),
            WithStatement with => new QueryPagingPlan(
                trimmedSql,
                with.Ctes.ToList(),
                with.MainQuery,
                BuildUniqueResultCteName(with.Ctes)),
            _ => throw new InvalidOperationException("Only query statements can use paged query results."),
        };
    }

    public string OriginalSql => _originalSql;

    public string BuildPageSql(
        IReadOnlyDictionary<int, string> filters,
        int? sortColumn,
        bool sortAscending,
        int pageSize,
        int page,
        string[] displayColumns)
        => BuildPageSql(filters, EmptyFilterModes, sortColumn, sortAscending, pageSize, page, displayColumns);

    public string BuildPageSql(
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        int? sortColumn,
        bool sortAscending,
        int pageSize,
        int page,
        string[] displayColumns)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(page, 1);

        bool hasInteractiveTransforms = HasActiveFilters(filters) || sortColumn is not null;
        if (hasInteractiveTransforms
            && TryBuildDirectTransforms(displayColumns, filters, filterModes, sortColumn, sortAscending, out var filterExpression, out var orderBy))
        {
            return SerializeStatement(BuildDirectPageStatement(pageSize, page, filterExpression, orderBy));
        }

        if (hasInteractiveTransforms)
            return SerializeStatement(BuildWrappedPageStatement(pageSize, page, filters, filterModes, sortColumn, sortAscending, displayColumns));

        return SerializeStatement(BuildDirectPageStatement(pageSize, page, filterExpression: null, orderBy: null));
    }

    public QueryCountPlan BuildCountPlan(
        IReadOnlyDictionary<int, string> filters,
        string[] displayColumns)
        => BuildCountPlan(filters, EmptyFilterModes, displayColumns);

    public QueryCountPlan BuildCountPlan(
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        string[] displayColumns)
    {
        if (CanUseFastCount(filters, filterModes, displayColumns, out var directFilter))
        {
            return new QueryCountPlan
            {
                Sql = SerializeStatement(BuildFastCountStatement(directFilter)),
                ApplyBasePagination = _baseSelect?.Limit.HasValue == true || _baseSelect?.Offset.HasValue == true,
            };
        }

        return new QueryCountPlan
        {
            Sql = SerializeStatement(BuildWrappedCountStatement(filters, filterModes, displayColumns)),
            ApplyBasePagination = false,
        };
    }

    public int ApplyBasePaginationToCount(int rawCount)
    {
        if (_baseSelect is null)
            return rawCount;

        int remaining = Math.Max(0, rawCount - (_baseSelect.Offset ?? 0));
        if (_baseSelect.Limit.HasValue)
            remaining = Math.Min(remaining, _baseSelect.Limit.Value);

        return remaining;
    }

    private bool CanUseFastCount(
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        string[] displayColumns,
        out Expression? directFilter)
    {
        directFilter = null;
        if (_baseSelect is null
            || _baseSelect.IsDistinct
            || _baseSelect.GroupBy is not null
            || _baseSelect.Having is not null
            || SelectHasAggregateProjection(_baseSelect))
        {
            return false;
        }

        if (!HasActiveFilters(filters))
            return true;

        return TryBuildDirectFilterExpression(displayColumns, filters, filterModes, out directFilter);
    }

    private Statement BuildFastCountStatement(Expression? directFilter)
    {
        if (_baseSelect is null)
            throw new InvalidOperationException("Fast count is only available for SELECT queries.");

        Statement statement = BuildStatement(
            new SelectStatement
            {
                IsDistinct = false,
                Columns =
                [
                    new SelectColumn
                    {
                        Expression = new FunctionCallExpression
                        {
                            FunctionName = "COUNT",
                            Arguments = [],
                            IsStarArg = true,
                        }
                    }
                ],
                From = _baseSelect.From,
                Where = CombineAnd(_baseSelect.Where, directFilter),
                GroupBy = null,
                Having = null,
                OrderBy = null,
                Limit = null,
                Offset = null,
            });

        return statement;
    }

    private Statement BuildDirectPageStatement(
        int pageSize,
        int page,
        Expression? filterExpression,
        List<OrderByClause>? orderBy)
    {
        int offset = (page - 1) * pageSize;

        QueryStatement rewritten = _baseQuery switch
        {
            SelectStatement select => new SelectStatement
            {
                IsDistinct = select.IsDistinct,
                Columns = select.Columns,
                From = select.From,
                Where = CombineAnd(select.Where, filterExpression),
                GroupBy = select.GroupBy,
                Having = select.Having,
                OrderBy = orderBy ?? select.OrderBy,
                Limit = ComputeEffectiveLimit(select.Limit, pageSize, offset),
                Offset = ComputeEffectiveOffset(select.Offset, offset),
            },
            CompoundSelectStatement compound => new CompoundSelectStatement
            {
                Left = compound.Left,
                Right = compound.Right,
                Operation = compound.Operation,
                OrderBy = orderBy ?? compound.OrderBy,
                Limit = ComputeEffectiveLimit(compound.Limit, pageSize, offset),
                Offset = ComputeEffectiveOffset(compound.Offset, offset),
            },
            _ => throw new InvalidOperationException($"Unsupported query type '{_baseQuery.GetType().Name}'."),
        };

        return BuildStatement(rewritten);
    }

    private Statement BuildWrappedPageStatement(
        int pageSize,
        int page,
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        int? sortColumn,
        bool sortAscending,
        string[] displayColumns)
    {
        string[] internalColumns = BuildInternalColumns(displayColumns);
        int offset = (page - 1) * pageSize;

        var outerQuery = new SelectStatement
        {
            IsDistinct = false,
            Columns = [new SelectColumn { IsStar = true }],
            From = new SimpleTableRef { TableName = _resultCteName },
            Where = BuildWrappedFilterExpression(filters, filterModes, internalColumns),
            GroupBy = null,
            Having = null,
            OrderBy = BuildWrappedOrderBy(sortColumn, sortAscending, internalColumns),
            Limit = pageSize,
            Offset = offset,
        };

        return BuildWrappedStatement(outerQuery, internalColumns);
    }

    private Statement BuildWrappedCountStatement(
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        string[] displayColumns)
    {
        string[]? internalColumns = HasActiveFilters(filters) ? BuildInternalColumns(displayColumns) : null;
        var outerQuery = new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    Expression = new FunctionCallExpression
                    {
                        FunctionName = "COUNT",
                        Arguments = [],
                        IsStarArg = true,
                    }
                }
            ],
            From = new SimpleTableRef { TableName = _resultCteName },
            Where = internalColumns is null ? null : BuildWrappedFilterExpression(filters, filterModes, internalColumns),
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };

        return BuildWrappedStatement(outerQuery, internalColumns);
    }

    private Statement BuildWrappedStatement(QueryStatement outerQuery, string[]? internalColumns)
    {
        var ctes = new List<CteDefinition>(_baseCtes.Count + 1);
        ctes.AddRange(_baseCtes);
        ctes.Add(new CteDefinition
        {
            Name = _resultCteName,
            ColumnNames = internalColumns?.ToList(),
            Query = _baseQuery,
        });

        return new WithStatement
        {
            Ctes = ctes,
            MainQuery = outerQuery,
        };
    }

    private Statement BuildStatement(QueryStatement query)
        => _baseCtes.Count == 0
            ? query
            : new WithStatement
            {
                Ctes = _baseCtes.ToList(),
                MainQuery = query,
            };

    private bool TryBuildDirectTransforms(
        string[] displayColumns,
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        int? sortColumn,
        bool sortAscending,
        out Expression? filterExpression,
        out List<OrderByClause>? orderBy)
    {
        filterExpression = null;
        orderBy = null;

        if (_baseSelect is null
            || _baseSelect.IsDistinct
            || _baseSelect.GroupBy is not null
            || _baseSelect.Having is not null
            || _baseSelect.Limit.HasValue
            || _baseSelect.Offset.HasValue
            || SelectHasAggregateProjection(_baseSelect))
        {
            return !HasActiveFilters(filters) && sortColumn is null;
        }

        if (HasActiveFilters(filters)
            && !TryBuildDirectFilterExpression(displayColumns, filters, filterModes, out filterExpression))
        {
            return false;
        }

        if (sortColumn is null)
            return true;

        if (!TryGetDirectColumnExpression(displayColumns, sortColumn.Value, out var sortExpression))
            return false;

        orderBy =
        [
            new OrderByClause
            {
                Expression = sortExpression,
                Descending = !sortAscending,
            }
        ];
        return true;
    }

    private bool TryBuildDirectFilterExpression(
        string[] displayColumns,
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        out Expression? filterExpression)
    {
        filterExpression = null;

        foreach (var filter in filters.OrderBy(pair => pair.Key))
        {
            if (string.IsNullOrWhiteSpace(filter.Value))
                continue;

            if (!TryGetDirectColumnExpression(displayColumns, filter.Key, out var sourceExpression))
                return false;

            var predicate = BuildFilterPredicate(sourceExpression, filter.Value, GetFilterMode(filterModes, filter.Key));
            filterExpression = filterExpression is null
                ? predicate
                : new BinaryExpression
                {
                    Op = BinaryOp.And,
                    Left = filterExpression,
                    Right = predicate,
                };
        }

        return true;
    }

    private bool TryGetDirectColumnExpression(
        string[] displayColumns,
        int columnIndex,
        out Expression expression)
    {
        expression = null!;
        if (_baseSelect is null || columnIndex < 0)
            return false;

        if (_baseSelect.Columns.Count == 1 && _baseSelect.Columns[0].IsStar)
        {
            if (columnIndex >= displayColumns.Length || !IsSimpleIdentifier(displayColumns[columnIndex]))
                return false;

            expression = new ColumnRefExpression { ColumnName = displayColumns[columnIndex] };
            return true;
        }

        if (columnIndex >= _baseSelect.Columns.Count)
            return false;

        var selectedColumn = _baseSelect.Columns[columnIndex];
        if (selectedColumn.IsStar || selectedColumn.Expression is null)
            return false;

        expression = selectedColumn.Expression;
        return true;
    }

    private static Expression BuildFilterPredicate(
        Expression sourceExpression,
        string filterValue,
        DataGridFilterMatchMode mode)
        => mode == DataGridFilterMatchMode.Exact
            ? new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = BuildTextExpression(sourceExpression),
                Right = new LiteralExpression
                {
                    Value = filterValue,
                    LiteralType = TokenType.StringLiteral,
                },
            }
            : new LikeExpression
            {
                Operand = BuildTextExpression(sourceExpression),
                Pattern = new LiteralExpression
                {
                    Value = BuildLikePattern(filterValue, mode),
                    LiteralType = TokenType.StringLiteral,
                },
                EscapeChar = new LiteralExpression
                {
                    Value = "!",
                    LiteralType = TokenType.StringLiteral,
                },
            };

    private static FunctionCallExpression BuildTextExpression(Expression sourceExpression)
        => new()
        {
            FunctionName = "TEXT",
            Arguments = [sourceExpression],
            IsStarArg = false,
        };

    private static Expression? CombineAnd(Expression? left, Expression? right)
    {
        if (left is null)
            return right;

        if (right is null)
            return left;

        return new BinaryExpression
        {
            Op = BinaryOp.And,
            Left = left,
            Right = right,
        };
    }

    private static int ComputeEffectiveOffset(int? baseOffset, int additionalOffset)
        => (baseOffset ?? 0) + additionalOffset;

    private static int? ComputeEffectiveLimit(int? baseLimit, int pageSize, int additionalOffset)
    {
        if (!baseLimit.HasValue)
            return pageSize;

        int remaining = Math.Max(0, baseLimit.Value - additionalOffset);
        return Math.Min(pageSize, remaining);
    }

    private static string[] BuildInternalColumns(string[] displayColumns)
    {
        if (displayColumns.Length == 0)
            throw new InvalidOperationException("Query result columns are not available yet.");

        return Enumerable.Range(0, displayColumns.Length)
            .Select(i => $"__q{i}")
            .ToArray();
    }

    private static Expression? BuildWrappedFilterExpression(
        IReadOnlyDictionary<int, string> filters,
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        string[] internalColumns)
    {
        Expression? filterExpression = null;

        foreach (var filter in filters.OrderBy(pair => pair.Key))
        {
            if (string.IsNullOrWhiteSpace(filter.Value))
                continue;

            if (filter.Key < 0 || filter.Key >= internalColumns.Length)
                continue;

            var predicate = BuildFilterPredicate(
                new ColumnRefExpression { ColumnName = internalColumns[filter.Key] },
                filter.Value,
                GetFilterMode(filterModes, filter.Key));

            filterExpression = filterExpression is null
                ? predicate
                : new BinaryExpression
                {
                    Op = BinaryOp.And,
                    Left = filterExpression,
                    Right = predicate,
                };
        }

        return filterExpression;
    }

    private static List<OrderByClause>? BuildWrappedOrderBy(
        int? sortColumn,
        bool sortAscending,
        string[] internalColumns)
    {
        if (sortColumn is null || sortColumn < 0 || sortColumn >= internalColumns.Length)
            return null;

        return
        [
            new OrderByClause
            {
                Expression = new ColumnRefExpression { ColumnName = internalColumns[sortColumn.Value] },
                Descending = !sortAscending,
            }
        ];
    }

    private static bool SelectHasAggregateProjection(SelectStatement select)
        => select.Columns.Any(column => column.Expression is not null && ContainsAggregateFunction(column.Expression));

    private static bool ContainsAggregateFunction(Expression expression)
        => expression switch
        {
            FunctionCallExpression func when IsAggregateFunction(func.FunctionName) => true,
            FunctionCallExpression func => func.Arguments.Any(ContainsAggregateFunction),
            BinaryExpression binary => ContainsAggregateFunction(binary.Left) || ContainsAggregateFunction(binary.Right),
            UnaryExpression unary => ContainsAggregateFunction(unary.Operand),
            CollateExpression collate => ContainsAggregateFunction(collate.Operand),
            LikeExpression like => ContainsAggregateFunction(like.Operand)
                || ContainsAggregateFunction(like.Pattern)
                || (like.EscapeChar is not null && ContainsAggregateFunction(like.EscapeChar)),
            InExpression inExpression => ContainsAggregateFunction(inExpression.Operand)
                || inExpression.Values.Any(ContainsAggregateFunction),
            InSubqueryExpression inSubquery => ContainsAggregateFunction(inSubquery.Operand),
            BetweenExpression between => ContainsAggregateFunction(between.Operand)
                || ContainsAggregateFunction(between.Low)
                || ContainsAggregateFunction(between.High),
            IsNullExpression isNull => ContainsAggregateFunction(isNull.Operand),
            ScalarSubqueryExpression scalarSubquery => ContainsAggregateFunction(scalarSubquery.Query),
            ExistsExpression exists => ContainsAggregateFunction(exists.Query),
            _ => false,
        };

    private static bool ContainsAggregateFunction(QueryStatement query)
        => query switch
        {
            SelectStatement select => select.Columns.Any(column => column.Expression is not null && ContainsAggregateFunction(column.Expression))
                || (select.Where is not null && ContainsAggregateFunction(select.Where))
                || (select.GroupBy is not null && select.GroupBy.Any(ContainsAggregateFunction))
                || (select.Having is not null && ContainsAggregateFunction(select.Having))
                || (select.OrderBy is not null && select.OrderBy.Any(orderBy => ContainsAggregateFunction(orderBy.Expression))),
            CompoundSelectStatement compound => ContainsAggregateFunction(compound.Left)
                || ContainsAggregateFunction(compound.Right)
                || (compound.OrderBy is not null && compound.OrderBy.Any(orderBy => ContainsAggregateFunction(orderBy.Expression))),
            _ => false,
        };

    private static bool IsAggregateFunction(string name)
        => name.Equals("COUNT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SUM", StringComparison.OrdinalIgnoreCase)
            || name.Equals("AVG", StringComparison.OrdinalIgnoreCase)
            || name.Equals("MIN", StringComparison.OrdinalIgnoreCase)
            || name.Equals("MAX", StringComparison.OrdinalIgnoreCase);

    private static bool HasActiveFilters(IReadOnlyDictionary<int, string> filters)
        => filters.Values.Any(value => !string.IsNullOrWhiteSpace(value));

    private static DataGridFilterMatchMode GetFilterMode(
        IReadOnlyDictionary<int, DataGridFilterMatchMode> filterModes,
        int columnIndex)
        => filterModes.TryGetValue(columnIndex, out var mode)
            ? mode
            : DataGridFilterMatchMode.Contains;

    private static string EscapeLikePattern(string value)
        => value
            .Replace("!", "!!", StringComparison.Ordinal)
            .Replace("%", "!%", StringComparison.Ordinal)
            .Replace("_", "!_", StringComparison.Ordinal);

    private static string BuildLikePattern(string filterValue, DataGridFilterMatchMode mode)
    {
        string escapedFilterValue = EscapeLikePattern(filterValue);
        return mode switch
        {
            DataGridFilterMatchMode.StartsWith => $"{escapedFilterValue}%",
            DataGridFilterMatchMode.EndsWith => $"%{escapedFilterValue}",
            _ => $"%{escapedFilterValue}%",
        };
    }

    private static string BuildUniqueResultCteName(IEnumerable<CteDefinition> ctes)
    {
        var existingNames = new HashSet<string>(
            ctes.Select(cte => cte.Name),
            StringComparer.OrdinalIgnoreCase);

        string name = ResultCteBaseName;
        int suffix = 1;
        while (existingNames.Contains(name))
            name = $"{ResultCteBaseName}_{suffix++}";

        return name;
    }

    private static bool IsSimpleIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        for (int i = 0; i < value.Length; i++)
        {
            char ch = value[i];
            bool isValid = i == 0
                ? char.IsLetter(ch) || ch == '_'
                : char.IsLetterOrDigit(ch) || ch == '_';

            if (!isValid)
                return false;
        }

        return true;
    }

    private static string TrimTrailingSemicolon(string sql)
    {
        int end = sql.Length;
        while (end > 0 && char.IsWhiteSpace(sql[end - 1]))
            end--;

        if (end > 0 && sql[end - 1] == ';')
            end--;

        while (end > 0 && char.IsWhiteSpace(sql[end - 1]))
            end--;

        return sql[..end];
    }

    private static string SerializeStatement(Statement statement)
        => QueryAstSqlWriter.Write(statement);
}

internal sealed class QueryCountPlan
{
    public required string Sql { get; init; }
    public bool ApplyBasePagination { get; init; }
}

internal static class QueryAstSqlWriter
{
    public static string Write(Statement statement)
        => statement switch
        {
            WithStatement with => WriteWith(with),
            QueryStatement query => WriteQuery(query),
            _ => throw new InvalidOperationException($"Cannot serialize statement type: {statement.GetType().Name}"),
        };

    private static string WriteWith(WithStatement statement)
    {
        string ctes = string.Join(", ", statement.Ctes.Select(WriteCte));
        return $"WITH {ctes} {WriteQuery(statement.MainQuery)}";
    }

    private static string WriteCte(CteDefinition cte)
    {
        string columns = cte.ColumnNames is { Count: > 0 }
            ? $"({string.Join(", ", cte.ColumnNames)})"
            : string.Empty;

        return $"{cte.Name}{columns} AS ({WriteQuery(cte.Query)})";
    }

    private static string WriteQuery(QueryStatement query)
        => query switch
        {
            SelectStatement select => WriteSelect(select),
            CompoundSelectStatement compound => WriteCompound(compound),
            _ => throw new InvalidOperationException($"Cannot serialize query type: {query.GetType().Name}"),
        };

    private static string WriteSelect(SelectStatement statement)
    {
        var parts = new List<string> { "SELECT" };
        if (statement.IsDistinct)
            parts.Add("DISTINCT");

        parts.Add(string.Join(", ", statement.Columns.Select(WriteSelectColumn)));
        parts.Add("FROM");
        parts.Add(WriteTableRef(statement.From));

        if (statement.Where is not null)
        {
            parts.Add("WHERE");
            parts.Add(WriteExpression(statement.Where));
        }

        if (statement.GroupBy is { Count: > 0 })
        {
            parts.Add("GROUP BY");
            parts.Add(string.Join(", ", statement.GroupBy.Select(WriteExpression)));
        }

        if (statement.Having is not null)
        {
            parts.Add("HAVING");
            parts.Add(WriteExpression(statement.Having));
        }

        AppendOrderingAndPagination(parts, statement.OrderBy, statement.Limit, statement.Offset);
        return string.Join(" ", parts);
    }

    private static string WriteCompound(CompoundSelectStatement statement)
    {
        var parts = new List<string>
        {
            WriteCompoundBranch(statement.Left),
            WriteSetOperation(statement.Operation),
            WriteCompoundBranch(statement.Right),
        };

        AppendOrderingAndPagination(parts, statement.OrderBy, statement.Limit, statement.Offset);
        return string.Join(" ", parts);
    }

    private static string WriteCompoundBranch(QueryStatement statement)
        => statement is CompoundSelectStatement
            ? $"({WriteQuery(statement)})"
            : WriteQuery(statement);

    private static string WriteSelectColumn(SelectColumn column)
    {
        if (column.IsStar)
            return "*";

        string expression = WriteExpression(column.Expression!);
        return column.Alias is null ? expression : $"{expression} AS {column.Alias}";
    }

    private static void AppendOrderingAndPagination(
        List<string> parts,
        List<OrderByClause>? orderBy,
        int? limit,
        int? offset)
    {
        if (orderBy is { Count: > 0 })
        {
            parts.Add("ORDER BY");
            parts.Add(string.Join(", ", orderBy.Select(clause =>
                $"{WriteExpression(clause.Expression)}{(clause.Descending ? " DESC" : "")}")));
        }

        if (limit.HasValue)
            parts.Add($"LIMIT {limit.Value}");

        if (offset.HasValue)
            parts.Add($"OFFSET {offset.Value}");
    }

    private static string WriteTableRef(TableRef tableRef)
        => tableRef switch
        {
            SimpleTableRef simple => simple.Alias is null
                ? simple.TableName
                : $"{simple.TableName} AS {simple.Alias}",
            JoinTableRef join => $"{WriteTableRef(join.Left)} {WriteJoinType(join.JoinType)} {WriteTableRef(join.Right)}"
                + (join.Condition is null ? string.Empty : $" ON {WriteExpression(join.Condition)}"),
            _ => throw new InvalidOperationException($"Cannot serialize table ref type: {tableRef.GetType().Name}"),
        };

    private static string WriteJoinType(JoinType joinType)
        => joinType switch
        {
            JoinType.Inner => "JOIN",
            JoinType.LeftOuter => "LEFT JOIN",
            JoinType.RightOuter => "RIGHT JOIN",
            JoinType.Cross => "CROSS JOIN",
            _ => throw new InvalidOperationException($"Unknown join type: {joinType}"),
        };

    private static string WriteSetOperation(SetOperationKind operation)
        => operation switch
        {
            SetOperationKind.Union => "UNION",
            SetOperationKind.Intersect => "INTERSECT",
            SetOperationKind.Except => "EXCEPT",
            _ => throw new InvalidOperationException($"Unknown set operation: {operation}"),
        };

    private static string WriteExpression(Expression expression)
        => expression switch
        {
            LiteralExpression literal => WriteLiteral(literal),
            ParameterExpression parameter => $"@{parameter.Name}",
            ColumnRefExpression column => column.TableAlias is null
                ? column.ColumnName
                : $"{column.TableAlias}.{column.ColumnName}",
            BinaryExpression binary => $"({WriteExpression(binary.Left)} {WriteBinaryOperator(binary.Op)} {WriteExpression(binary.Right)})",
            UnaryExpression unary => unary.Op == TokenType.Not
                ? $"NOT {WriteExpression(unary.Operand)}"
                : $"-{WriteExpression(unary.Operand)}",
            CollateExpression collate => $"{WriteExpression(collate.Operand)} COLLATE {collate.Collation}",
            FunctionCallExpression function => function.IsStarArg
                ? $"{function.FunctionName}(*)"
                : $"{function.FunctionName}({(function.IsDistinct ? "DISTINCT " : string.Empty)}{string.Join(", ", function.Arguments.Select(WriteExpression))})",
            LikeExpression like => $"{WriteExpression(like.Operand)}{(like.Negated ? " NOT" : string.Empty)} LIKE {WriteExpression(like.Pattern)}"
                + (like.EscapeChar is null ? string.Empty : $" ESCAPE {WriteExpression(like.EscapeChar)}"),
            InExpression inExpression => $"{WriteExpression(inExpression.Operand)}{(inExpression.Negated ? " NOT" : string.Empty)} IN ({string.Join(", ", inExpression.Values.Select(WriteExpression))})",
            InSubqueryExpression inSubquery => $"{WriteExpression(inSubquery.Operand)}{(inSubquery.Negated ? " NOT" : string.Empty)} IN ({WriteQuery(inSubquery.Query)})",
            ScalarSubqueryExpression scalarSubquery => $"({WriteQuery(scalarSubquery.Query)})",
            ExistsExpression exists => $"EXISTS ({WriteQuery(exists.Query)})",
            BetweenExpression between => $"{WriteExpression(between.Operand)}{(between.Negated ? " NOT" : string.Empty)} BETWEEN {WriteExpression(between.Low)} AND {WriteExpression(between.High)}",
            IsNullExpression isNull => $"{WriteExpression(isNull.Operand)} IS{(isNull.Negated ? " NOT" : string.Empty)} NULL",
            _ => throw new InvalidOperationException($"Cannot serialize expression type: {expression.GetType().Name}"),
        };

    private static string WriteLiteral(LiteralExpression literal)
    {
        if (literal.Value is null)
            return "NULL";

        return literal.LiteralType switch
        {
            TokenType.StringLiteral => $"'{literal.Value.ToString()!.Replace("'", "''", StringComparison.Ordinal)}'",
            TokenType.RealLiteral => Convert.ToDouble(literal.Value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
            TokenType.IntegerLiteral => Convert.ToInt64(literal.Value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
            _ => literal.Value.ToString() ?? "NULL",
        };
    }

    private static string WriteBinaryOperator(BinaryOp operation)
        => operation switch
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
            _ => throw new InvalidOperationException($"Unknown binary operator: {operation}"),
        };
}
