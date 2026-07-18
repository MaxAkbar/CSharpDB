using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbSqlTranslatingExpressionVisitorFactory
    : RelationalSqlTranslatingExpressionVisitorFactory
{
    public CSharpDbSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor) =>
        new CSharpDbSqlTranslatingExpressionVisitor(
            Dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
}

public sealed class CSharpDbSqlTranslatingExpressionVisitor
    : RelationalSqlTranslatingExpressionVisitor
{
    public CSharpDbSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(
            dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor)
    {
    }

    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        string? previousDetails = TranslationErrorDetails;
        Expression translated = base.VisitMethodCall(methodCallExpression);
        if (ReferenceEquals(
                translated,
                QueryCompilationContext.NotTranslatedExpression) &&
            !ProviderDetailWasAdded(
                previousDetails,
                TranslationErrorDetails))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics.ForMethod(
                    methodCallExpression.Method));
        }

        return translated;
    }

    protected override Expression VisitMember(MemberExpression memberExpression)
    {
        string? previousDetails = TranslationErrorDetails;
        Expression translated = base.VisitMember(memberExpression);
        if (ReferenceEquals(
                translated,
                QueryCompilationContext.NotTranslatedExpression) &&
            !ProviderDetailWasAdded(
                previousDetails,
                TranslationErrorDetails))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics.ForMember(
                    memberExpression.Member));
        }

        return translated;
    }

    private static bool ProviderDetailWasAdded(
        string? previousDetails,
        string? currentDetails)
    {
        int previousLength = previousDetails?.Length ?? 0;
        return currentDetails is not null &&
            currentDetails.Length > previousLength &&
            currentDetails.AsSpan(previousLength)
                .Contains("CDBEF", StringComparison.Ordinal);
    }
}

internal static class CSharpDbQueryTranslationDiagnostics
{
    private const string DocumentationUrl =
        "https://csharpdb.com/docs/entity-framework-core.html#linq-translation";

    public static string ForMethod(MethodInfo method)
    {
        string signature = FormatMethod(method);
        string guidance = method.GetParameters().Any(static parameter =>
                parameter.ParameterType == typeof(StringComparison))
            ? " StringComparison overloads are not supported; configure an appropriate CSharpDB collation or use a supported invariant casing method before comparison."
            : string.Empty;

        return
            $"CDBEF1001: The CSharpDB EF Core provider cannot translate LINQ method '{signature}' to SQL.{guidance} " +
            $"Keep supported filters server-side, rewrite this expression, or call AsEnumerable() explicitly before the unsupported portion. See {DocumentationUrl}.";
    }

    public static string ForMember(MemberInfo member)
    {
        string declaringType = member.DeclaringType?.FullName ?? "<unknown>";
        return
            $"CDBEF1002: The CSharpDB EF Core provider cannot translate LINQ member '{declaringType}.{member.Name}' to SQL. " +
            $"Keep supported filters server-side, rewrite this expression, or call AsEnumerable() explicitly before the unsupported portion. See {DocumentationUrl}.";
    }

    public static string ForOperator(string operatorName) =>
        $"CDBEF1003: The CSharpDB EF Core provider cannot translate LINQ operator '{operatorName}' to SQL. " +
        $"Rewrite the query with supported operators, or call AsEnumerable() explicitly after applying selective server-side filters. See {DocumentationUrl}.";

    public static string? FindUnsupportedOperator(Expression expression)
    {
        var visitor = new UnsupportedOperatorFindingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.OperatorName;
    }

    private static string FormatMethod(MethodInfo method)
    {
        string declaringType = method.DeclaringType?.FullName ?? "<unknown>";
        string parameters = string.Join(
            ", ",
            method.GetParameters().Select(static parameter =>
                parameter.ParameterType.FullName ?? parameter.ParameterType.Name));
        return $"{declaringType}.{method.Name}({parameters})";
    }

    private sealed class UnsupportedOperatorFindingExpressionVisitor
        : ExpressionVisitor
    {
        public string? OperatorName { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (OperatorName is null &&
                node.Method.DeclaringType == typeof(Queryable))
            {
                OperatorName = node.Method.Name switch
                {
                    nameof(Queryable.TakeWhile) =>
                        "Queryable.TakeWhile",
                    nameof(Queryable.SkipWhile) =>
                        "Queryable.SkipWhile",
                    _ => null,
                };
            }

            return OperatorName is null
                ? base.VisitMethodCall(node)
                : node;
        }
    }
}
