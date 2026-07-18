using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQueryableMethodTranslatingExpressionVisitorFactory
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    private readonly QueryableMethodTranslatingExpressionVisitorDependencies _dependencies;
    private readonly RelationalQueryableMethodTranslatingExpressionVisitorDependencies _relationalDependencies;

    public CSharpDbQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryableMethodTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext)
        => new CSharpDbQueryableMethodTranslatingExpressionVisitor(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext);
}

public sealed class CSharpDbQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    public CSharpDbQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
    }

    private CSharpDbQueryableMethodTranslatingExpressionVisitor(
        CSharpDbQueryableMethodTranslatingExpressionVisitor parent)
        : base(parent)
    {
    }

    protected override QueryableMethodTranslatingExpressionVisitor
        CreateSubqueryVisitor() =>
        new CSharpDbQueryableMethodTranslatingExpressionVisitor(this);

    public override Expression Translate(Expression expression)
    {
        string? operatorName =
            CSharpDbQueryTranslationDiagnostics.FindUnsupportedOperator(
                expression);
        if (operatorName is not null)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics.ForOperator(
                    operatorName));
        }

        return base.Translate(expression);
    }
}
