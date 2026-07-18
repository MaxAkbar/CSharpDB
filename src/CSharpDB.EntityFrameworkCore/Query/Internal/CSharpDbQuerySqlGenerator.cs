using CSharpDB.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQuerySqlGenerator : QuerySqlGenerator
{
    private readonly Dictionary<string, (int Precision, int Scale)>
        _decimalParameterFacets =
            new(StringComparer.Ordinal);

    public CSharpDbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine()
                .Append("LIMIT ");

            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset is not null)
        {
            Sql.AppendLine()
                .Append("OFFSET ");

            Visit(selectExpression.Offset);
        }
    }

    protected override void GenerateRootCommand(
        Expression queryExpression)
    {
        _decimalParameterFacets.Clear();
        base.GenerateRootCommand(queryExpression);
    }

    protected override Expression VisitSqlParameter(
        SqlParameterExpression sqlParameterExpression)
    {
        if (sqlParameterExpression.TypeMapping?.Converter is
            CSharpDbDecimalToInt64Converter converter)
        {
            var facets =
                (converter.Precision, converter.Scale);
            if (_decimalParameterFacets.TryGetValue(
                    sqlParameterExpression.Name,
                    out var previousFacets) &&
                previousFacets != facets)
            {
                throw new InvalidOperationException(
                    CSharpDbQueryTranslationDiagnostics
                        .ForDecimal(
                            $"Parameter '{sqlParameterExpression.Name}' is reused with incompatible decimal facets decimal({previousFacets.Precision}, {previousFacets.Scale}) and decimal({facets.Precision}, {facets.Scale})."));
            }

            _decimalParameterFacets[
                sqlParameterExpression.Name] = facets;
        }

        return base.VisitSqlParameter(
            sqlParameterExpression);
    }

    protected override Expression VisitSelect(SelectExpression selectExpression)
    {
        if (selectExpression.GroupBy.Count > 0 &&
            FindUnsupportedGroupedSqlShape(
                selectExpression) is
                { } groupedShapeReason)
        {
            throw new InvalidOperationException(
                CSharpDbQueryTranslationDiagnostics
                    .ForGroupedAggregate(
                        groupedShapeReason));
        }

        if (TryGenerateTopLevelExistsProjection(selectExpression))
            return selectExpression;

        return base.VisitSelect(selectExpression);
    }

    private static string? FindUnsupportedGroupedSqlShape(
        SelectExpression selectExpression)
    {
        foreach (ProjectionExpression projection in
                 selectExpression.Projection)
        {
            if (!IsQualifiedGroupedProjection(
                    projection.Expression,
                    selectExpression.GroupBy))
            {
                return
                    "Generated grouped projections must contain only direct grouped columns and bare supported aggregates; transformed projections are unsupported.";
            }
        }

        if (selectExpression.Having is not null &&
            !IsQualifiedHavingPredicate(
                selectExpression.Having))
        {
            return
                "Generated HAVING predicates must use basic comparisons or logical composition over direct grouped columns, bare supported aggregates, and constants or parameters.";
        }

        foreach (OrderingExpression ordering in
                 selectExpression.Orderings)
        {
            if (!IsQualifiedGroupedOrdering(
                    ordering.Expression,
                    selectExpression.Projection))
            {
                return
                    "Grouped ordering must use a directly projected key or bare aggregate; transformed or unprojected ordering expressions are unsupported.";
            }
        }

        return null;
    }

    private static bool IsQualifiedGroupedProjection(
        SqlExpression expression,
        IReadOnlyList<SqlExpression> groupBy) =>
        groupBy.Any(grouping =>
            IsSameSqlExpression(
                expression,
                grouping)) ||
        IsBareAggregate(expression);

    private static bool IsQualifiedGroupedOrdering(
        SqlExpression expression,
        IReadOnlyList<ProjectionExpression> projections)
    {
        if (!IsDirectColumnExpression(expression) &&
            !IsBareAggregate(expression))
        {
            return false;
        }

        return projections.Any(projection =>
            IsSameSqlExpression(
                expression,
                projection.Expression));
    }

    private static bool IsQualifiedHavingPredicate(
        SqlExpression expression)
    {
        if (expression is SqlBinaryExpression binary)
        {
            if (binary.OperatorType is
                ExpressionType.AndAlso or
                ExpressionType.OrElse)
            {
                return IsQualifiedHavingPredicate(binary.Left) &&
                    IsQualifiedHavingPredicate(binary.Right);
            }

            if (binary.OperatorType is
                ExpressionType.Equal or
                ExpressionType.NotEqual or
                ExpressionType.LessThan or
                ExpressionType.LessThanOrEqual or
                ExpressionType.GreaterThan or
                ExpressionType.GreaterThanOrEqual)
            {
                return IsQualifiedHavingOperand(binary.Left) &&
                    IsQualifiedHavingOperand(binary.Right);
            }

            return false;
        }

        if (expression is SqlUnaryExpression unary &&
            unary.OperatorType is
                ExpressionType.Equal or
                ExpressionType.NotEqual)
        {
            return IsQualifiedHavingOperand(unary.Operand);
        }

        return expression is ColumnExpression ||
            IsBareAggregate(expression) ||
            IsIndependentValue(expression);
    }

    private static bool IsQualifiedHavingOperand(
        SqlExpression expression) =>
        expression is ColumnExpression ||
        IsBareAggregate(expression) ||
        IsIndependentValue(expression);

    private static bool IsIndependentValue(
        SqlExpression expression) =>
        expression is SqlConstantExpression or
            SqlParameterExpression;

    private static bool IsBareAggregate(
        SqlExpression expression)
    {
        if (expression is SqlFunctionExpression function)
        {
            if (IsAggregateFunction(function))
                return true;

            return string.Equals(
                    function.Name,
                    "COALESCE",
                    StringComparison.OrdinalIgnoreCase) &&
                function.Arguments is
                    { Count: 2 } arguments &&
                IsSumFunction(arguments[0]) &&
                IsIndependentValue(arguments[1]);
        }

        return expression is SqlBinaryExpression
            {
                OperatorType: ExpressionType.Coalesce,
            } coalesce &&
            IsSumFunction(coalesce.Left) &&
            IsIndependentValue(coalesce.Right);
    }

    private static bool IsSumFunction(
        SqlExpression expression) =>
        expression is SqlFunctionExpression function &&
        string.Equals(
            function.Name,
            "SUM",
            StringComparison.OrdinalIgnoreCase) &&
        IsAggregateFunction(function);

    private static bool IsAggregateFunction(
        SqlFunctionExpression function)
    {
        if (function.Name is not (
                "COUNT" or
                "SUM" or
                "AVG" or
                "MIN" or
                "MAX"))
        {
            return false;
        }

        if (function.Arguments is not
            { } arguments)
        {
            return false;
        }

        if (arguments.Count == 0)
            return true;

        if (arguments.Count != 1)
            return false;

        SqlExpression argument = arguments[0];
        return IsDirectColumnExpression(argument) ||
            argument is DistinctExpression
            {
                Operand: var distinctOperand,
            } &&
            IsDirectColumnExpression(distinctOperand) ||
            argument is SqlFragmentExpression
            {
                Sql: "*",
            } ||
            argument.GetType().Name == "StarExpression";
    }

    private static bool IsSameSqlExpression(
        SqlExpression left,
        SqlExpression right) =>
        left.Equals(right) ||
        IsSameDirectColumnExpression(left, right) ||
        string.Equals(
            left.ToString(),
            right.ToString(),
            StringComparison.Ordinal);

    private static bool IsSameDirectColumnExpression(
        SqlExpression left,
        SqlExpression right) =>
        TryGetDirectColumn(left, out ColumnExpression? leftColumn) &&
        TryGetDirectColumn(right, out ColumnExpression? rightColumn) &&
        string.Equals(
            leftColumn.TableAlias,
            rightColumn.TableAlias,
            StringComparison.Ordinal) &&
        string.Equals(
            leftColumn.Name,
            rightColumn.Name,
            StringComparison.Ordinal);

    private static bool IsDirectColumnExpression(
        SqlExpression expression) =>
        TryGetDirectColumn(
            expression,
            out _);

    private static bool TryGetDirectColumn(
        SqlExpression expression,
        out ColumnExpression column)
    {
        while (expression is SqlUnaryExpression
               {
                   OperatorType: ExpressionType.Convert,
               } conversion)
        {
            expression = conversion.Operand;
        }

        column = expression as ColumnExpression ?? null!;
        return column is not null;
    }

    private bool TryGenerateTopLevelExistsProjection(SelectExpression selectExpression)
    {
        if (selectExpression.Tables.Count != 0
            || selectExpression.GroupBy.Count != 0
            || selectExpression.Having is not null
            || selectExpression.Projection.Count != 1
            || selectExpression.Projection[0].Expression is not ExistsExpression existsExpression)
        {
            return false;
        }

        SelectExpression subquery = existsExpression.Subquery;
        if (subquery.Tables.Count == 0
            || subquery.GroupBy.Count != 0
            || subquery.Having is not null
            || subquery.IsDistinct
            || subquery.Offset is not null
            || subquery.Limit is not null)
        {
            return false;
        }

        Sql.Append("SELECT COUNT(*)");
        GenerateFrom(subquery);

        if (subquery.Predicate is not null)
        {
            Sql.AppendLine()
                .Append("WHERE ");

            Visit(subquery.Predicate);
        }

        return true;
    }
}
