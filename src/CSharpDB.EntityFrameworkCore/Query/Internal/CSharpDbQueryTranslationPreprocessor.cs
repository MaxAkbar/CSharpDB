using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQueryTranslationPreprocessorFactory
    : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPreprocessorDependencies
        _relationalDependencies;
    private readonly IModel _designTimeModel;

    public CSharpDbQueryTranslationPreprocessorFactory(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies
            relationalDependencies,
        IDesignTimeModel designTimeModel)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
        _designTimeModel = designTimeModel.Model;
    }

    public QueryTranslationPreprocessor Create(
        QueryCompilationContext queryCompilationContext) =>
        new CSharpDbQueryTranslationPreprocessor(
            _dependencies,
            _relationalDependencies,
            queryCompilationContext,
            _designTimeModel);
}

public sealed class CSharpDbQueryTranslationPreprocessor
    : RelationalQueryTranslationPreprocessor
{
    private readonly IModel _model;

    public CSharpDbQueryTranslationPreprocessor(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies
            relationalDependencies,
        QueryCompilationContext queryCompilationContext,
        IModel designTimeModel)
        : base(
            dependencies,
            relationalDependencies,
            queryCompilationContext)
    {
        _model = designTimeModel;
    }

    public override Expression Process(Expression query)
    {
        ValidateDecimalSafety(query);

        string? unsafeDistinctAggregate =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsafeDistinctCardinality(query);
        if (unsafeDistinctAggregate is not null)
        {
            throw new InvalidOperationException(
                unsafeDistinctAggregate);
        }

        string? unsafeGroupBySource =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsafeGroupBySource(query);
        if (unsafeGroupBySource is not null)
        {
            throw new InvalidOperationException(
                unsafeGroupBySource);
        }

        string? unsupportedGroupMaterialization =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsupportedGroupMaterialization(query);
        if (unsupportedGroupMaterialization is not null)
        {
            throw new InvalidOperationException(
                unsupportedGroupMaterialization);
        }

        string? unsafeGroupedAggregate =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsafeGroupedAggregate(query, _model);
        if (unsafeGroupedAggregate is not null)
        {
            throw new InvalidOperationException(
                unsafeGroupedAggregate);
        }

        string? unsupportedLeftJoinShape =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsupportedLeftJoinShape(query);
        if (unsupportedLeftJoinShape is not null)
        {
            throw new InvalidOperationException(
                unsupportedLeftJoinShape);
        }

        string? operatorName =
            CSharpDbQueryTranslationDiagnostics.FindUnsupportedOperator(
                query);
        if (operatorName is
            "Queryable.Concat" or
            "Queryable.Union" or
            "Queryable.Except" or
            "Queryable.Intersect" or
            "Queryable.GroupJoin" or
            "Queryable.SelectMany" or
            "Queryable.DefaultIfEmpty" or
            "Queryable.LeftJoin(comparer)" or
            "Queryable.RightJoin" or
            "Queryable.Join(comparer)" or
            "RelationalQueryableExtensions.ExecuteUpdate")
        {
            throw new InvalidOperationException(
                CSharpDbQueryTranslationDiagnostics.ForOperator(
                    operatorName));
        }

        try
        {
            Expression processed =
                base.Process(query);
            ValidateDecimalSafety(processed);
            return processed;
        }
        catch (InvalidOperationException exception)
            when (operatorName is not null &&
                IsOperatorTranslationFailure(
                    exception,
                    operatorName))
        {
            string details =
                CSharpDbQueryTranslationDiagnostics.ForOperator(
                    operatorName);
            throw new InvalidOperationException(
                $"{exception.Message}{Environment.NewLine}{details}",
                exception);
        }
    }

    private void ValidateDecimalSafety(
        Expression query)
    {
        string? unsafeDecimalExpression =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsafeDecimalExpression(query);
        if (unsafeDecimalExpression is not null)
        {
            throw new InvalidOperationException(
                unsafeDecimalExpression);
        }

        string? unsafeDecimalParameterReuse =
            CSharpDbQueryTranslationDiagnostics
                .FindUnsafeDecimalParameterReuse(
                    query,
                    _model);
        if (unsafeDecimalParameterReuse is not null)
        {
            throw new InvalidOperationException(
                unsafeDecimalParameterReuse);
        }
    }

    private static bool IsOperatorTranslationFailure(
        InvalidOperationException exception,
        string operatorName)
    {
        int separator = operatorName.LastIndexOf('.');
        string methodName = separator < 0
            ? operatorName
            : operatorName[(separator + 1)..];
        return exception.Message.Contains(
                $".{methodName}(",
                StringComparison.Ordinal) &&
            exception.Message.Contains(
                "could not be translated",
                StringComparison.Ordinal);
    }
}
