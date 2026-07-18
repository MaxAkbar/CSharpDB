using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQueryTranslationPreprocessorFactory
    : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPreprocessorDependencies
        _relationalDependencies;

    public CSharpDbQueryTranslationPreprocessorFactory(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies
            relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryTranslationPreprocessor Create(
        QueryCompilationContext queryCompilationContext) =>
        new CSharpDbQueryTranslationPreprocessor(
            _dependencies,
            _relationalDependencies,
            queryCompilationContext);
}

public sealed class CSharpDbQueryTranslationPreprocessor
    : RelationalQueryTranslationPreprocessor
{
    public CSharpDbQueryTranslationPreprocessor(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies
            relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(
            dependencies,
            relationalDependencies,
            queryCompilationContext)
    {
    }

    public override Expression Process(Expression query)
    {
        string? operatorName =
            CSharpDbQueryTranslationDiagnostics.FindUnsupportedOperator(
                query);

        try
        {
            return base.Process(query);
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
