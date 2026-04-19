using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Linq.Expressions;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQuerySqlGenerator : QuerySqlGenerator
{
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

    protected override Expression VisitSelect(SelectExpression selectExpression)
    {
        if (TryGenerateTopLevelExistsProjection(selectExpression))
            return selectExpression;

        return base.VisitSelect(selectExpression);
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
