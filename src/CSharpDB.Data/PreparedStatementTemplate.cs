using System.Globalization;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal sealed class PreparedStatementTemplate
{
    private const int DefaultBoundStatementCacheCapacity = 64;
    private readonly Statement _template;
    private readonly string[] _parameterNames;
    private readonly bool _containsParameters;
    private readonly BoundStatementCache? _boundStatementCache;

    private PreparedStatementTemplate(Statement template, string[] parameterNames)
    {
        _template = template;
        _parameterNames = parameterNames;
        _containsParameters = parameterNames.Length > 0;
        _boundStatementCache = _containsParameters
            ? new BoundStatementCache(DefaultBoundStatementCacheCapacity)
            : null;
    }

    internal static PreparedStatementTemplate Create(string sql)
    {
        var template = Parser.Parse(sql);
        var parameterNames = CollectParameterNames(template);
        return new PreparedStatementTemplate(template, parameterNames.ToArray());
    }

    internal Statement Bind(CSharpDbParameterCollection parameters)
    {
        if (!_containsParameters)
            return _template;

        var cache = _boundStatementCache;
        if (cache != null)
            return cache.GetOrAdd(_parameterNames, parameters, () => BindStatement(_template, parameters));

        return BindStatement(_template, parameters);
    }

    private static Statement BindStatement(Statement statement, CSharpDbParameterCollection parameters)
    {
        switch (statement)
        {
            case InsertStatement insert:
            {
                var valueRows = BindExpressionRows(insert.ValueRows, parameters, out bool rowsChanged);
                if (!rowsChanged)
                    return insert;

                return new InsertStatement
                {
                    TableName = insert.TableName,
                    ColumnNames = insert.ColumnNames,
                    ValueRows = valueRows,
                };
            }

            case SelectStatement select:
            {
                var columns = BindSelectColumns(select.Columns, parameters, out bool columnsChanged);
                var from = BindTableRef(select.From, parameters, out bool fromChanged);
                Expression? where = BindOptionalExpression(select.Where, parameters, out bool whereChanged);
                var groupBy = BindOptionalExpressionList(select.GroupBy, parameters, out bool groupByChanged);
                Expression? having = BindOptionalExpression(select.Having, parameters, out bool havingChanged);
                var orderBy = BindOrderByClauses(select.OrderBy, parameters, out bool orderByChanged);

                if (!columnsChanged && !fromChanged && !whereChanged && !groupByChanged && !havingChanged && !orderByChanged)
                    return select;

                return new SelectStatement
                {
                    IsDistinct = select.IsDistinct,
                    Columns = columns,
                    From = from,
                    Where = where,
                    GroupBy = groupBy,
                    Having = having,
                    OrderBy = orderBy,
                    Limit = select.Limit,
                    Offset = select.Offset,
                };
            }

            case DeleteStatement delete:
            {
                Expression? where = BindOptionalExpression(delete.Where, parameters, out bool whereChanged);
                if (!whereChanged)
                    return delete;

                return new DeleteStatement
                {
                    TableName = delete.TableName,
                    Where = where,
                };
            }

            case UpdateStatement update:
            {
                var setClauses = BindSetClauses(update.SetClauses, parameters, out bool setChanged);
                Expression? where = BindOptionalExpression(update.Where, parameters, out bool whereChanged);
                if (!setChanged && !whereChanged)
                    return update;

                return new UpdateStatement
                {
                    TableName = update.TableName,
                    SetClauses = setClauses,
                    Where = where,
                };
            }

            case CreateViewStatement view:
            {
                var query = (SelectStatement)BindStatement(view.Query, parameters);
                if (ReferenceEquals(query, view.Query))
                    return view;

                return new CreateViewStatement
                {
                    ViewName = view.ViewName,
                    Query = query,
                    IfNotExists = view.IfNotExists,
                };
            }

            case WithStatement with:
            {
                var ctes = BindCtes(with.Ctes, parameters, out bool ctesChanged);
                var mainQuery = (SelectStatement)BindStatement(with.MainQuery, parameters);
                if (!ctesChanged && ReferenceEquals(mainQuery, with.MainQuery))
                    return with;

                return new WithStatement
                {
                    Ctes = ctes,
                    MainQuery = mainQuery,
                };
            }

            case CreateTriggerStatement trigger:
            {
                Expression? whenCondition = BindOptionalExpression(trigger.WhenCondition, parameters, out bool whenChanged);
                var body = BindStatements(trigger.Body, parameters, out bool bodyChanged);
                if (!whenChanged && !bodyChanged)
                    return trigger;

                return new CreateTriggerStatement
                {
                    TriggerName = trigger.TriggerName,
                    TableName = trigger.TableName,
                    Timing = trigger.Timing,
                    Event = trigger.Event,
                    WhenCondition = whenCondition,
                    Body = body,
                    IfNotExists = trigger.IfNotExists,
                };
            }

            case CreateTableStatement
                or DropTableStatement
                or AlterTableStatement
                or CreateIndexStatement
                or DropIndexStatement
                or DropViewStatement
                or DropTriggerStatement:
                return statement;
            default:
                throw new NotSupportedException($"Unsupported statement type '{statement.GetType().Name}' in prepared execution.");
        }
    }

    private static TableRef BindTableRef(TableRef tableRef, CSharpDbParameterCollection parameters, out bool changed)
    {
        switch (tableRef)
        {
            case SimpleTableRef:
                changed = false;
                return tableRef;
            case JoinTableRef join:
            {
                var left = BindTableRef(join.Left, parameters, out bool leftChanged);
                var right = BindTableRef(join.Right, parameters, out bool rightChanged);
                Expression? condition = BindOptionalExpression(join.Condition, parameters, out bool condChanged);
                if (!leftChanged && !rightChanged && !condChanged)
                {
                    changed = false;
                    return join;
                }

                changed = true;
                return new JoinTableRef
                {
                    Left = left,
                    Right = right,
                    JoinType = join.JoinType,
                    Condition = condition,
                };
            }
            default:
                throw new NotSupportedException($"Unsupported table ref type '{tableRef.GetType().Name}' in prepared execution.");
        }
    }

    private static Expression BindExpression(Expression expression, CSharpDbParameterCollection parameters, out bool changed)
    {
        switch (expression)
        {
            case ParameterExpression parameter:
                changed = true;
                return BindParameter(parameter, parameters);
            case LiteralExpression:
            case ColumnRefExpression:
                changed = false;
                return expression;
            case BinaryExpression binary:
            {
                var left = BindExpression(binary.Left, parameters, out bool leftChanged);
                var right = BindExpression(binary.Right, parameters, out bool rightChanged);
                if (!leftChanged && !rightChanged)
                {
                    changed = false;
                    return binary;
                }

                changed = true;
                return new BinaryExpression
                {
                    Op = binary.Op,
                    Left = left,
                    Right = right,
                };
            }
            case UnaryExpression unary:
            {
                var operand = BindExpression(unary.Operand, parameters, out bool operandChanged);
                if (!operandChanged)
                {
                    changed = false;
                    return unary;
                }

                changed = true;
                return new UnaryExpression
                {
                    Op = unary.Op,
                    Operand = operand,
                };
            }
            case LikeExpression like:
            {
                var operand = BindExpression(like.Operand, parameters, out bool operandChanged);
                var pattern = BindExpression(like.Pattern, parameters, out bool patternChanged);
                Expression? escape = BindOptionalExpression(like.EscapeChar, parameters, out bool escapeChanged);
                if (!operandChanged && !patternChanged && !escapeChanged)
                {
                    changed = false;
                    return like;
                }

                changed = true;
                return new LikeExpression
                {
                    Operand = operand,
                    Pattern = pattern,
                    EscapeChar = escape,
                    Negated = like.Negated,
                };
            }
            case InExpression inExpression:
            {
                var operand = BindExpression(inExpression.Operand, parameters, out bool operandChanged);
                var values = BindExpressionList(inExpression.Values, parameters, out bool valuesChanged);
                if (!operandChanged && !valuesChanged)
                {
                    changed = false;
                    return inExpression;
                }

                changed = true;
                return new InExpression
                {
                    Operand = operand,
                    Values = values,
                    Negated = inExpression.Negated,
                };
            }
            case BetweenExpression between:
            {
                var operand = BindExpression(between.Operand, parameters, out bool operandChanged);
                var low = BindExpression(between.Low, parameters, out bool lowChanged);
                var high = BindExpression(between.High, parameters, out bool highChanged);
                if (!operandChanged && !lowChanged && !highChanged)
                {
                    changed = false;
                    return between;
                }

                changed = true;
                return new BetweenExpression
                {
                    Operand = operand,
                    Low = low,
                    High = high,
                    Negated = between.Negated,
                };
            }
            case IsNullExpression isNull:
            {
                var operand = BindExpression(isNull.Operand, parameters, out bool operandChanged);
                if (!operandChanged)
                {
                    changed = false;
                    return isNull;
                }

                changed = true;
                return new IsNullExpression
                {
                    Operand = operand,
                    Negated = isNull.Negated,
                };
            }
            case FunctionCallExpression functionCall:
            {
                var args = BindExpressionList(functionCall.Arguments, parameters, out bool argsChanged);
                if (!argsChanged)
                {
                    changed = false;
                    return functionCall;
                }

                changed = true;
                return new FunctionCallExpression
                {
                    FunctionName = functionCall.FunctionName,
                    IsDistinct = functionCall.IsDistinct,
                    IsStarArg = functionCall.IsStarArg,
                    Arguments = args,
                };
            }
            default:
                throw new NotSupportedException($"Unsupported expression type '{expression.GetType().Name}' in prepared execution.");
        }
    }

    private static Expression? BindOptionalExpression(Expression? expression, CSharpDbParameterCollection parameters, out bool changed)
    {
        if (expression == null)
        {
            changed = false;
            return null;
        }

        return BindExpression(expression, parameters, out changed);
    }

    private static List<Expression> BindExpressionList(List<Expression> expressions, CSharpDbParameterCollection parameters, out bool changed)
    {
        Expression[]? rewritten = null;
        for (int i = 0; i < expressions.Count; i++)
        {
            var bound = BindExpression(expressions[i], parameters, out bool exprChanged);
            if (!exprChanged)
                continue;

            rewritten ??= expressions.ToArray();
            rewritten[i] = bound;
        }

        if (rewritten == null)
        {
            changed = false;
            return expressions;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static List<List<Expression>> BindExpressionRows(
        List<List<Expression>> valueRows,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        List<List<Expression>>? rewrittenRows = null;
        for (int i = 0; i < valueRows.Count; i++)
        {
            var boundRow = BindExpressionList(valueRows[i], parameters, out bool rowChanged);
            if (!rowChanged)
                continue;

            rewrittenRows ??= new List<List<Expression>>(valueRows);
            rewrittenRows[i] = boundRow;
        }

        if (rewrittenRows == null)
        {
            changed = false;
            return valueRows;
        }

        changed = true;
        return rewrittenRows;
    }

    private static List<SelectColumn> BindSelectColumns(
        List<SelectColumn> columns,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        SelectColumn[]? rewritten = null;
        for (int i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            var expression = BindOptionalExpression(column.Expression, parameters, out bool exprChanged);
            if (!exprChanged)
                continue;

            rewritten ??= columns.ToArray();
            rewritten[i] = new SelectColumn
            {
                IsStar = column.IsStar,
                Alias = column.Alias,
                Expression = expression,
            };
        }

        if (rewritten == null)
        {
            changed = false;
            return columns;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static List<OrderByClause>? BindOrderByClauses(
        List<OrderByClause>? clauses,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        if (clauses == null)
        {
            changed = false;
            return null;
        }

        OrderByClause[]? rewritten = null;
        for (int i = 0; i < clauses.Count; i++)
        {
            var clause = clauses[i];
            var expression = BindExpression(clause.Expression, parameters, out bool exprChanged);
            if (!exprChanged)
                continue;

            rewritten ??= clauses.ToArray();
            rewritten[i] = new OrderByClause
            {
                Expression = expression,
                Descending = clause.Descending,
            };
        }

        if (rewritten == null)
        {
            changed = false;
            return clauses;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static List<Expression>? BindOptionalExpressionList(
        List<Expression>? expressions,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        if (expressions == null)
        {
            changed = false;
            return null;
        }

        return BindExpressionList(expressions, parameters, out changed);
    }

    private static List<SetClause> BindSetClauses(
        List<SetClause> setClauses,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        SetClause[]? rewritten = null;
        for (int i = 0; i < setClauses.Count; i++)
        {
            var set = setClauses[i];
            var value = BindExpression(set.Value, parameters, out bool valueChanged);
            if (!valueChanged)
                continue;

            rewritten ??= setClauses.ToArray();
            rewritten[i] = new SetClause
            {
                ColumnName = set.ColumnName,
                Value = value,
            };
        }

        if (rewritten == null)
        {
            changed = false;
            return setClauses;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static List<CteDefinition> BindCtes(
        List<CteDefinition> ctes,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        CteDefinition[]? rewritten = null;
        for (int i = 0; i < ctes.Count; i++)
        {
            var cte = ctes[i];
            var query = (SelectStatement)BindStatement(cte.Query, parameters);
            if (ReferenceEquals(query, cte.Query))
                continue;

            rewritten ??= ctes.ToArray();
            rewritten[i] = new CteDefinition
            {
                Name = cte.Name,
                ColumnNames = cte.ColumnNames,
                Query = query,
            };
        }

        if (rewritten == null)
        {
            changed = false;
            return ctes;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static List<Statement> BindStatements(
        List<Statement> statements,
        CSharpDbParameterCollection parameters,
        out bool changed)
    {
        Statement[]? rewritten = null;
        for (int i = 0; i < statements.Count; i++)
        {
            var bound = BindStatement(statements[i], parameters);
            if (ReferenceEquals(bound, statements[i]))
                continue;

            rewritten ??= statements.ToArray();
            rewritten[i] = bound;
        }

        if (rewritten == null)
        {
            changed = false;
            return statements;
        }

        changed = true;
        return rewritten.ToList();
    }

    private static LiteralExpression BindParameter(ParameterExpression parameter, CSharpDbParameterCollection parameters)
    {
        if (!parameters.TryGetValue(parameter.Name.AsSpan(), out var value))
            throw new InvalidOperationException($"Parameter '@{parameter.Name}' was not supplied.");

        return ToLiteral(value);
    }

    private static LiteralExpression ToLiteral(object? value)
    {
        if (value is null or DBNull)
            return new LiteralExpression { Value = null, LiteralType = TokenType.Null };

        return value switch
        {
            long l => new LiteralExpression { Value = l, LiteralType = TokenType.IntegerLiteral },
            int iv => new LiteralExpression { Value = (long)iv, LiteralType = TokenType.IntegerLiteral },
            short s => new LiteralExpression { Value = (long)s, LiteralType = TokenType.IntegerLiteral },
            byte b => new LiteralExpression { Value = (long)b, LiteralType = TokenType.IntegerLiteral },
            sbyte sb => new LiteralExpression { Value = (long)sb, LiteralType = TokenType.IntegerLiteral },
            uint ui => new LiteralExpression { Value = (long)ui, LiteralType = TokenType.IntegerLiteral },
            ushort us => new LiteralExpression { Value = (long)us, LiteralType = TokenType.IntegerLiteral },
            ulong ul => new LiteralExpression { Value = checked((long)ul), LiteralType = TokenType.IntegerLiteral },
            bool bv => new LiteralExpression { Value = bv ? 1L : 0L, LiteralType = TokenType.IntegerLiteral },
            double d => new LiteralExpression { Value = d, LiteralType = TokenType.RealLiteral },
            float f => new LiteralExpression { Value = (double)f, LiteralType = TokenType.RealLiteral },
            decimal m => new LiteralExpression { Value = (double)m, LiteralType = TokenType.RealLiteral },
            string sv => new LiteralExpression { Value = sv, LiteralType = TokenType.StringLiteral },
            DateTime dt => new LiteralExpression { Value = dt.ToString("O", CultureInfo.InvariantCulture), LiteralType = TokenType.StringLiteral },
            Guid g => new LiteralExpression { Value = g.ToString(), LiteralType = TokenType.StringLiteral },
            byte[] => throw new NotSupportedException("Blob parameters are not supported."),
            _ => new LiteralExpression { Value = value.ToString()!, LiteralType = TokenType.StringLiteral },
        };
    }

    private static List<string> CollectParameterNames(Statement statement)
    {
        var names = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        CollectParameterNames(statement, names, seen);
        return names;
    }

    private static void CollectParameterNames(Statement statement, List<string> names, HashSet<string> seen)
    {
        switch (statement)
        {
            case InsertStatement insert:
                for (int i = 0; i < insert.ValueRows.Count; i++)
                    for (int j = 0; j < insert.ValueRows[i].Count; j++)
                        CollectParameterNames(insert.ValueRows[i][j], names, seen);
                return;
            case SelectStatement select:
                for (int i = 0; i < select.Columns.Count; i++)
                {
                    var expression = select.Columns[i].Expression;
                    if (expression != null)
                        CollectParameterNames(expression, names, seen);
                }

                CollectParameterNames(select.From, names, seen);
                if (select.Where != null)
                    CollectParameterNames(select.Where, names, seen);
                if (select.GroupBy != null)
                    for (int i = 0; i < select.GroupBy.Count; i++)
                        CollectParameterNames(select.GroupBy[i], names, seen);
                if (select.Having != null)
                    CollectParameterNames(select.Having, names, seen);
                if (select.OrderBy != null)
                    for (int i = 0; i < select.OrderBy.Count; i++)
                        CollectParameterNames(select.OrderBy[i].Expression, names, seen);
                return;
            case DeleteStatement delete:
                if (delete.Where != null)
                    CollectParameterNames(delete.Where, names, seen);
                return;
            case UpdateStatement update:
                for (int i = 0; i < update.SetClauses.Count; i++)
                    CollectParameterNames(update.SetClauses[i].Value, names, seen);
                if (update.Where != null)
                    CollectParameterNames(update.Where, names, seen);
                return;
            case CreateViewStatement view:
                CollectParameterNames(view.Query, names, seen);
                return;
            case WithStatement with:
                for (int i = 0; i < with.Ctes.Count; i++)
                    CollectParameterNames(with.Ctes[i].Query, names, seen);
                CollectParameterNames(with.MainQuery, names, seen);
                return;
            case CreateTriggerStatement trigger:
                if (trigger.WhenCondition != null)
                    CollectParameterNames(trigger.WhenCondition, names, seen);
                for (int i = 0; i < trigger.Body.Count; i++)
                    CollectParameterNames(trigger.Body[i], names, seen);
                return;
        }
    }

    private static void CollectParameterNames(TableRef tableRef, List<string> names, HashSet<string> seen)
    {
        if (tableRef is not JoinTableRef join)
            return;

        CollectParameterNames(join.Left, names, seen);
        CollectParameterNames(join.Right, names, seen);
        if (join.Condition != null)
            CollectParameterNames(join.Condition, names, seen);
    }

    private static void CollectParameterNames(Expression expression, List<string> names, HashSet<string> seen)
    {
        switch (expression)
        {
            case ParameterExpression parameter:
                if (seen.Add(parameter.Name))
                    names.Add(parameter.Name);
                return;
            case BinaryExpression binary:
                CollectParameterNames(binary.Left, names, seen);
                CollectParameterNames(binary.Right, names, seen);
                return;
            case UnaryExpression unary:
                CollectParameterNames(unary.Operand, names, seen);
                return;
            case LikeExpression like:
                CollectParameterNames(like.Operand, names, seen);
                CollectParameterNames(like.Pattern, names, seen);
                if (like.EscapeChar != null)
                    CollectParameterNames(like.EscapeChar, names, seen);
                return;
            case InExpression inExpression:
                CollectParameterNames(inExpression.Operand, names, seen);
                for (int i = 0; i < inExpression.Values.Count; i++)
                    CollectParameterNames(inExpression.Values[i], names, seen);
                return;
            case BetweenExpression between:
                CollectParameterNames(between.Operand, names, seen);
                CollectParameterNames(between.Low, names, seen);
                CollectParameterNames(between.High, names, seen);
                return;
            case IsNullExpression isNull:
                CollectParameterNames(isNull.Operand, names, seen);
                return;
            case FunctionCallExpression functionCall:
                for (int i = 0; i < functionCall.Arguments.Count; i++)
                    CollectParameterNames(functionCall.Arguments[i], names, seen);
                return;
        }
    }

    private sealed class BoundStatementCache
    {
        private readonly int _capacity;
        private readonly List<CacheEntry> _lru = new();
        private readonly object _gate = new();

        internal BoundStatementCache(int capacity)
        {
            _capacity = capacity > 0 ? capacity : 0;
        }

        internal Statement GetOrAdd(
            string[] parameterNames,
            CSharpDbParameterCollection parameters,
            Func<Statement> factory)
        {
            if (_capacity == 0)
                return factory();

            lock (_gate)
            {
                for (int i = 0; i < _lru.Count; i++)
                {
                    var entry = _lru[i];
                    if (!Matches(entry.Values, parameterNames, parameters))
                        continue;

                    if (i != 0)
                    {
                        _lru.RemoveAt(i);
                        _lru.Insert(0, entry);
                    }

                    return entry.Statement;
                }
            }

            var created = factory();
            var capturedValues = CaptureValues(parameterNames, parameters);

            lock (_gate)
            {
                for (int i = 0; i < _lru.Count; i++)
                {
                    var existing = _lru[i];
                    if (!Matches(existing.Values, parameterNames, parameters))
                        continue;

                    if (i != 0)
                    {
                        _lru.RemoveAt(i);
                        _lru.Insert(0, existing);
                    }

                    return existing.Statement;
                }

                _lru.Insert(0, new CacheEntry(capturedValues, created));

                if (_lru.Count > _capacity)
                    _lru.RemoveAt(_lru.Count - 1);
            }

            return created;
        }

        private static object?[] CaptureValues(string[] parameterNames, CSharpDbParameterCollection parameters)
        {
            var values = new object?[parameterNames.Length];
            for (int i = 0; i < parameterNames.Length; i++)
            {
                string name = parameterNames[i];
                if (!parameters.TryGetValue(name.AsSpan(), out object? value))
                    throw new InvalidOperationException($"Parameter '@{name}' was not supplied.");

                values[i] = NormalizeNull(value);
            }

            return values;
        }

        private static bool Matches(object?[] cachedValues, string[] parameterNames, CSharpDbParameterCollection parameters)
        {
            if (cachedValues.Length != parameterNames.Length)
                return false;

            for (int i = 0; i < parameterNames.Length; i++)
            {
                string name = parameterNames[i];
                if (!parameters.TryGetValue(name.AsSpan(), out object? value))
                    throw new InvalidOperationException($"Parameter '@{name}' was not supplied.");

                if (!Equals(cachedValues[i], NormalizeNull(value)))
                    return false;
            }

            return true;
        }

        private static object? NormalizeNull(object? value)
            => value is DBNull ? null : value;

        private readonly record struct CacheEntry(object?[] Values, Statement Statement);
    }
}
