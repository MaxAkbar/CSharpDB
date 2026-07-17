using System.Globalization;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

internal static class WindowExpressionSupport
{
    public static void ValidateQuery(QueryStatement query)
    {
        switch (query)
        {
            case SelectStatement select:
                ValidateSelect(select);
                break;
            case CompoundSelectStatement compound:
                ValidateQuery(compound.Left);
                ValidateQuery(compound.Right);
                if (compound.OrderBy != null)
                {
                    foreach (OrderByClause clause in compound.OrderBy)
                    {
                        var windows = new List<WindowFunctionExpression>();
                        bool containsSubquery = false;
                        ValidateExpression(
                            clause.Expression,
                            allowWindow: false,
                            insideWindow: false,
                            windows,
                            ref containsSubquery);
                    }
                }
                break;
        }
    }

    public static bool ContainsWindowFunctions(SelectStatement select) =>
        select.Columns.Any(column =>
            column.Expression != null && ContainsWindowFunction(column.Expression)) ||
        (select.OrderBy != null &&
         select.OrderBy.Any(clause => ContainsWindowFunction(clause.Expression)));

    public static List<WindowFunctionExpression> CollectWindowFunctions(SelectStatement select)
    {
        var collected = new List<WindowFunctionExpression>();
        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (SelectColumn column in select.Columns)
        {
            if (column.Expression != null)
                CollectWindowFunctions(column.Expression, collected, seen);
        }

        if (select.OrderBy != null)
        {
            foreach (OrderByClause clause in select.OrderBy)
                CollectWindowFunctions(clause.Expression, collected, seen);
        }

        return collected;
    }

    public static string GetExpressionKey(Expression expression)
    {
        var builder = new StringBuilder();
        AppendExpressionKey(builder, expression);
        return builder.ToString();
    }

    public static Expression RewriteWindowFunctions(
        Expression expression,
        IReadOnlyDictionary<string, string> slotNames)
    {
        if (expression is WindowFunctionExpression window)
        {
            string key = GetExpressionKey(window);
            if (!slotNames.TryGetValue(key, out string? slotName))
            {
                throw new CSharpDbException(
                    ErrorCode.Unknown,
                    "A planned window expression did not have a bound output slot.");
            }

            return new ColumnRefExpression { ColumnName = slotName };
        }

        return expression switch
        {
            BinaryExpression binary => new BinaryExpression
            {
                Op = binary.Op,
                Left = RewriteWindowFunctions(binary.Left, slotNames),
                Right = RewriteWindowFunctions(binary.Right, slotNames),
            },
            UnaryExpression unary => new UnaryExpression
            {
                Op = unary.Op,
                Operand = RewriteWindowFunctions(unary.Operand, slotNames),
            },
            CollateExpression collate => new CollateExpression
            {
                Operand = RewriteWindowFunctions(collate.Operand, slotNames),
                Collation = collate.Collation,
            },
            FunctionCallExpression function => new FunctionCallExpression
            {
                FunctionName = function.FunctionName,
                Arguments = function.Arguments
                    .Select(argument => RewriteWindowFunctions(argument, slotNames))
                    .ToList(),
                IsDistinct = function.IsDistinct,
                IsStarArg = function.IsStarArg,
            },
            LikeExpression like => new LikeExpression
            {
                Operand = RewriteWindowFunctions(like.Operand, slotNames),
                Pattern = RewriteWindowFunctions(like.Pattern, slotNames),
                EscapeChar = like.EscapeChar == null
                    ? null
                    : RewriteWindowFunctions(like.EscapeChar, slotNames),
                Negated = like.Negated,
            },
            InExpression inExpression => new InExpression
            {
                Operand = RewriteWindowFunctions(inExpression.Operand, slotNames),
                Values = inExpression.Values
                    .Select(value => RewriteWindowFunctions(value, slotNames))
                    .ToList(),
                Negated = inExpression.Negated,
            },
            InSubqueryExpression inSubquery => new InSubqueryExpression
            {
                Operand = RewriteWindowFunctions(inSubquery.Operand, slotNames),
                Query = inSubquery.Query,
                Negated = inSubquery.Negated,
            },
            BetweenExpression between => new BetweenExpression
            {
                Operand = RewriteWindowFunctions(between.Operand, slotNames),
                Low = RewriteWindowFunctions(between.Low, slotNames),
                High = RewriteWindowFunctions(between.High, slotNames),
                Negated = between.Negated,
            },
            IsNullExpression isNull => new IsNullExpression
            {
                Operand = RewriteWindowFunctions(isNull.Operand, slotNames),
                Negated = isNull.Negated,
            },
            _ => expression,
        };
    }

    private static void ValidateSelect(SelectStatement select)
    {
        var windows = new List<WindowFunctionExpression>();
        bool containsSubquery = false;

        if (select.Where != null)
        {
            ValidateExpression(
                select.Where,
                allowWindow: false,
                insideWindow: false,
                windows,
                ref containsSubquery);
        }

        if (select.GroupBy != null)
        {
            foreach (Expression expression in select.GroupBy)
            {
                ValidateExpression(
                    expression,
                    allowWindow: false,
                    insideWindow: false,
                    windows,
                    ref containsSubquery);
            }
        }

        if (select.Having != null)
        {
            ValidateExpression(
                select.Having,
                allowWindow: false,
                insideWindow: false,
                windows,
                ref containsSubquery);
        }

        ValidateTableRef(select.From, windows, ref containsSubquery);

        foreach (SelectColumn column in select.Columns)
        {
            if (column.Expression != null)
            {
                ValidateExpression(
                    column.Expression,
                    allowWindow: true,
                    insideWindow: false,
                    windows,
                    ref containsSubquery);
            }
        }

        if (select.OrderBy != null)
        {
            foreach (OrderByClause clause in select.OrderBy)
            {
                ValidateExpression(
                    clause.Expression,
                    allowWindow: true,
                    insideWindow: false,
                    windows,
                    ref containsSubquery);
            }
        }

        if (windows.Count == 0)
            return;

        if (select.GroupBy != null || select.Having != null)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window functions cannot be combined with GROUP BY or HAVING in the experimental window-function tier.");
        }

        bool hasOrdinaryAggregate = select.Columns.Any(column =>
                column.Expression != null && ContainsOrdinaryAggregate(column.Expression)) ||
            (select.OrderBy != null &&
             select.OrderBy.Any(clause => ContainsOrdinaryAggregate(clause.Expression)));
        if (hasOrdinaryAggregate)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window functions cannot be combined with ordinary aggregate expressions in the experimental window-function tier.");
        }

        if (containsSubquery)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Window functions cannot be combined with subqueries in the experimental window-function tier.");
        }

        string specificationKey = GetWindowSpecificationKey(windows[0].Window);
        for (int i = 1; i < windows.Count; i++)
        {
            if (!string.Equals(
                    specificationKey,
                    GetWindowSpecificationKey(windows[i].Window),
                    StringComparison.Ordinal))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    "Multiple incompatible window specifications are not supported in the experimental window-function tier.");
            }
        }
    }

    private static void ValidateTableRef(
        TableRef tableRef,
        List<WindowFunctionExpression> windows,
        ref bool containsSubquery)
    {
        if (tableRef is not JoinTableRef join)
            return;

        ValidateTableRef(join.Left, windows, ref containsSubquery);
        ValidateTableRef(join.Right, windows, ref containsSubquery);
        if (join.Condition != null)
        {
            ValidateExpression(
                join.Condition,
                allowWindow: false,
                insideWindow: false,
                windows,
                ref containsSubquery);
        }
    }

    private static void ValidateExpression(
        Expression expression,
        bool allowWindow,
        bool insideWindow,
        List<WindowFunctionExpression> windows,
        ref bool containsSubquery)
    {
        if (expression is WindowFunctionExpression window)
        {
            if (insideWindow)
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    "Nested window functions are not supported.");
            }

            if (!allowWindow)
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    "Window functions are only allowed in the SELECT list and query ORDER BY.");
            }

            ValidateWindowFunction(window);
            windows.Add(window);

            foreach (Expression argument in window.Function.Arguments)
            {
                ValidateExpression(
                    argument,
                    allowWindow: false,
                    insideWindow: true,
                    windows,
                    ref containsSubquery);
            }

            foreach (Expression partition in window.Window.PartitionBy)
            {
                ValidateExpression(
                    partition,
                    allowWindow: false,
                    insideWindow: true,
                    windows,
                    ref containsSubquery);
            }

            foreach (OrderByClause clause in window.Window.OrderBy)
            {
                ValidateExpression(
                    clause.Expression,
                    allowWindow: false,
                    insideWindow: true,
                    windows,
                    ref containsSubquery);
            }

            return;
        }

        switch (expression)
        {
            case BinaryExpression binary:
                ValidateExpression(binary.Left, allowWindow, insideWindow, windows, ref containsSubquery);
                ValidateExpression(binary.Right, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case UnaryExpression unary:
                ValidateExpression(unary.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case CollateExpression collate:
                ValidateExpression(collate.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case FunctionCallExpression function:
                foreach (Expression argument in function.Arguments)
                    ValidateExpression(argument, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case LikeExpression like:
                ValidateExpression(like.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                ValidateExpression(like.Pattern, allowWindow, insideWindow, windows, ref containsSubquery);
                if (like.EscapeChar != null)
                    ValidateExpression(like.EscapeChar, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case InExpression inExpression:
                ValidateExpression(inExpression.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                foreach (Expression value in inExpression.Values)
                    ValidateExpression(value, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case InSubqueryExpression inSubquery:
                containsSubquery = true;
                ValidateExpression(inSubquery.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                ValidateQuery(inSubquery.Query);
                break;
            case ScalarSubqueryExpression scalarSubquery:
                containsSubquery = true;
                ValidateQuery(scalarSubquery.Query);
                break;
            case ExistsExpression exists:
                containsSubquery = true;
                ValidateQuery(exists.Query);
                break;
            case BetweenExpression between:
                ValidateExpression(between.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                ValidateExpression(between.Low, allowWindow, insideWindow, windows, ref containsSubquery);
                ValidateExpression(between.High, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
            case IsNullExpression isNull:
                ValidateExpression(isNull.Operand, allowWindow, insideWindow, windows, ref containsSubquery);
                break;
        }
    }

    private static void ValidateWindowFunction(WindowFunctionExpression window)
    {
        FunctionCallExpression function = window.Function;
        string functionName = function.FunctionName.ToUpperInvariant();
        if (function.IsDistinct)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"DISTINCT is not supported for window function '{functionName}' in the experimental tier.");
        }

        switch (functionName)
        {
            case "ROW_NUMBER":
            case "RANK":
            case "DENSE_RANK":
                if (function.IsStarArg || function.Arguments.Count != 0)
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"{functionName} window function requires zero arguments.");
                }
                break;

            case "COUNT":
                if (function.IsStarArg == (function.Arguments.Count == 1))
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        "COUNT window function requires either one expression or '*'.");
                }
                break;

            case "SUM":
            case "AVG":
            case "MIN":
            case "MAX":
                if (function.IsStarArg || function.Arguments.Count != 1)
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"{functionName} window function requires exactly one expression argument.");
                }
                break;

            default:
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Window function '{function.FunctionName}' is not supported in the experimental window-function tier.");
        }

        if (function.Arguments.Any(ContainsOrdinaryAggregate) ||
            window.Window.PartitionBy.Any(ContainsOrdinaryAggregate) ||
            window.Window.OrderBy.Any(clause => ContainsOrdinaryAggregate(clause.Expression)))
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                "Ordinary aggregate expressions inside a window definition are not supported in the experimental tier.");
        }
    }

    private static bool ContainsWindowFunction(Expression expression)
    {
        return expression switch
        {
            WindowFunctionExpression => true,
            BinaryExpression binary => ContainsWindowFunction(binary.Left) ||
                ContainsWindowFunction(binary.Right),
            UnaryExpression unary => ContainsWindowFunction(unary.Operand),
            CollateExpression collate => ContainsWindowFunction(collate.Operand),
            FunctionCallExpression function => function.Arguments.Any(ContainsWindowFunction),
            LikeExpression like => ContainsWindowFunction(like.Operand) ||
                ContainsWindowFunction(like.Pattern) ||
                (like.EscapeChar != null && ContainsWindowFunction(like.EscapeChar)),
            InExpression inExpression => ContainsWindowFunction(inExpression.Operand) ||
                inExpression.Values.Any(ContainsWindowFunction),
            InSubqueryExpression inSubquery => ContainsWindowFunction(inSubquery.Operand),
            BetweenExpression between => ContainsWindowFunction(between.Operand) ||
                ContainsWindowFunction(between.Low) ||
                ContainsWindowFunction(between.High),
            IsNullExpression isNull => ContainsWindowFunction(isNull.Operand),
            _ => false,
        };
    }

    private static bool ContainsOrdinaryAggregate(Expression expression)
    {
        return expression switch
        {
            WindowFunctionExpression => false,
            FunctionCallExpression function =>
                ScalarFunctionEvaluator.IsAggregateFunction(function.FunctionName) ||
                function.Arguments.Any(ContainsOrdinaryAggregate),
            BinaryExpression binary => ContainsOrdinaryAggregate(binary.Left) ||
                ContainsOrdinaryAggregate(binary.Right),
            UnaryExpression unary => ContainsOrdinaryAggregate(unary.Operand),
            CollateExpression collate => ContainsOrdinaryAggregate(collate.Operand),
            LikeExpression like => ContainsOrdinaryAggregate(like.Operand) ||
                ContainsOrdinaryAggregate(like.Pattern) ||
                (like.EscapeChar != null && ContainsOrdinaryAggregate(like.EscapeChar)),
            InExpression inExpression => ContainsOrdinaryAggregate(inExpression.Operand) ||
                inExpression.Values.Any(ContainsOrdinaryAggregate),
            BetweenExpression between => ContainsOrdinaryAggregate(between.Operand) ||
                ContainsOrdinaryAggregate(between.Low) ||
                ContainsOrdinaryAggregate(between.High),
            IsNullExpression isNull => ContainsOrdinaryAggregate(isNull.Operand),
            _ => false,
        };
    }

    private static void CollectWindowFunctions(
        Expression expression,
        List<WindowFunctionExpression> collected,
        HashSet<string> seen)
    {
        if (expression is WindowFunctionExpression window)
        {
            string key = GetExpressionKey(window);
            if (seen.Add(key))
                collected.Add(window);
            return;
        }

        switch (expression)
        {
            case BinaryExpression binary:
                CollectWindowFunctions(binary.Left, collected, seen);
                CollectWindowFunctions(binary.Right, collected, seen);
                break;
            case UnaryExpression unary:
                CollectWindowFunctions(unary.Operand, collected, seen);
                break;
            case CollateExpression collate:
                CollectWindowFunctions(collate.Operand, collected, seen);
                break;
            case FunctionCallExpression function:
                foreach (Expression argument in function.Arguments)
                    CollectWindowFunctions(argument, collected, seen);
                break;
            case LikeExpression like:
                CollectWindowFunctions(like.Operand, collected, seen);
                CollectWindowFunctions(like.Pattern, collected, seen);
                if (like.EscapeChar != null)
                    CollectWindowFunctions(like.EscapeChar, collected, seen);
                break;
            case InExpression inExpression:
                CollectWindowFunctions(inExpression.Operand, collected, seen);
                foreach (Expression value in inExpression.Values)
                    CollectWindowFunctions(value, collected, seen);
                break;
            case InSubqueryExpression inSubquery:
                CollectWindowFunctions(inSubquery.Operand, collected, seen);
                break;
            case BetweenExpression between:
                CollectWindowFunctions(between.Operand, collected, seen);
                CollectWindowFunctions(between.Low, collected, seen);
                CollectWindowFunctions(between.High, collected, seen);
                break;
            case IsNullExpression isNull:
                CollectWindowFunctions(isNull.Operand, collected, seen);
                break;
        }
    }

    private static string GetWindowSpecificationKey(WindowSpecification specification)
    {
        var builder = new StringBuilder("P[");
        foreach (Expression expression in specification.PartitionBy)
        {
            AppendExpressionKey(builder, expression);
            builder.Append(';');
        }

        builder.Append("]O[");
        foreach (OrderByClause clause in specification.OrderBy)
        {
            AppendExpressionKey(builder, clause.Expression);
            builder.Append(clause.Descending ? ":D;" : ":A;");
        }

        return builder.Append(']').ToString();
    }

    private static void AppendExpressionKey(StringBuilder builder, Expression expression)
    {
        switch (expression)
        {
            case DefaultExpression:
                builder.Append("default");
                break;
            case LiteralExpression literal:
                builder.Append("lit:").Append((int)literal.LiteralType).Append(':');
                if (literal.Value is byte[] bytes)
                    builder.Append(Convert.ToHexString(bytes));
                else if (literal.Value is string text)
                    AppendLengthPrefixed(builder, text);
                else
                    builder.Append(Convert.ToString(literal.Value, CultureInfo.InvariantCulture));
                break;
            case ParameterExpression parameter:
                builder.Append("param:");
                AppendLengthPrefixed(builder, parameter.Name.ToUpperInvariant());
                break;
            case ColumnRefExpression column:
                builder.Append("col:");
                AppendLengthPrefixed(builder, column.TableAlias?.ToUpperInvariant() ?? string.Empty);
                AppendLengthPrefixed(builder, column.ColumnName.ToUpperInvariant());
                break;
            case BinaryExpression binary:
                builder.Append("bin:").Append((int)binary.Op).Append('(');
                AppendExpressionKey(builder, binary.Left);
                builder.Append(',');
                AppendExpressionKey(builder, binary.Right);
                builder.Append(')');
                break;
            case UnaryExpression unary:
                builder.Append("un:").Append((int)unary.Op).Append('(');
                AppendExpressionKey(builder, unary.Operand);
                builder.Append(')');
                break;
            case CollateExpression collate:
                builder.Append("collate:");
                AppendLengthPrefixed(builder, collate.Collation.ToUpperInvariant());
                builder.Append('(');
                AppendExpressionKey(builder, collate.Operand);
                builder.Append(')');
                break;
            case FunctionCallExpression function:
                AppendFunctionKey(builder, function);
                break;
            case WindowFunctionExpression window:
                builder.Append("window(");
                AppendFunctionKey(builder, window.Function);
                builder.Append(',').Append(GetWindowSpecificationKey(window.Window)).Append(')');
                break;
            case LikeExpression like:
                builder.Append(like.Negated ? "notlike(" : "like(");
                AppendExpressionKey(builder, like.Operand);
                builder.Append(',');
                AppendExpressionKey(builder, like.Pattern);
                if (like.EscapeChar != null)
                {
                    builder.Append(',');
                    AppendExpressionKey(builder, like.EscapeChar);
                }
                builder.Append(')');
                break;
            case InExpression inExpression:
                builder.Append(inExpression.Negated ? "notin(" : "in(");
                AppendExpressionKey(builder, inExpression.Operand);
                foreach (Expression value in inExpression.Values)
                {
                    builder.Append(',');
                    AppendExpressionKey(builder, value);
                }
                builder.Append(')');
                break;
            case BetweenExpression between:
                builder.Append(between.Negated ? "notbetween(" : "between(");
                AppendExpressionKey(builder, between.Operand);
                builder.Append(',');
                AppendExpressionKey(builder, between.Low);
                builder.Append(',');
                AppendExpressionKey(builder, between.High);
                builder.Append(')');
                break;
            case IsNullExpression isNull:
                builder.Append(isNull.Negated ? "isnotnull(" : "isnull(");
                AppendExpressionKey(builder, isNull.Operand);
                builder.Append(')');
                break;
            default:
                builder.Append("unsupported:");
                AppendLengthPrefixed(builder, expression.GetType().FullName ?? expression.GetType().Name);
                break;
        }
    }

    private static void AppendFunctionKey(StringBuilder builder, FunctionCallExpression function)
    {
        builder.Append("func:");
        AppendLengthPrefixed(builder, function.FunctionName.ToUpperInvariant());
        builder.Append(function.IsDistinct ? ":distinct" : ":all");
        builder.Append(function.IsStarArg ? ":star(" : ":args(");
        foreach (Expression argument in function.Arguments)
        {
            AppendExpressionKey(builder, argument);
            builder.Append(';');
        }
        builder.Append(')');
    }

    private static void AppendLengthPrefixed(StringBuilder builder, string value) =>
        builder.Append(value.Length).Append(':').Append(value);
}
