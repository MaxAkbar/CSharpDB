using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public CSharpDbMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators(
        [
            new CSharpDbMemberTranslator(dependencies.SqlExpressionFactory),
        ]);
    }
}

internal sealed class CSharpDbMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public CSharpDbMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
        => _sqlExpressionFactory = sqlExpressionFactory;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
            return null;

        if (member.DeclaringType == typeof(string) &&
            member.Name == nameof(string.Length))
        {
            return Function("LENGTH", instance, returnType);
        }

        string? temporalFunction = member.DeclaringType switch
        {
            var type when type == typeof(DateTime) || type == typeof(DateOnly) =>
                member.Name switch
                {
                    nameof(DateTime.Year) => "YEAR",
                    nameof(DateTime.Month) => "MONTH",
                    nameof(DateTime.Day) => "DAY",
                    nameof(DateTime.Hour) when type == typeof(DateTime) => "HOUR",
                    nameof(DateTime.Minute) when type == typeof(DateTime) => "MINUTE",
                    nameof(DateTime.Second) when type == typeof(DateTime) => "SECOND",
                    _ => null,
                },
            var type when type == typeof(TimeOnly) =>
                member.Name switch
                {
                    nameof(TimeOnly.Hour) => "HOUR",
                    nameof(TimeOnly.Minute) => "MINUTE",
                    nameof(TimeOnly.Second) => "SECOND",
                    _ => null,
                },
            _ => null,
        };

        return temporalFunction is null
            ? null
            : Function(temporalFunction, instance, returnType);
    }

    private SqlExpression Function(
        string functionName,
        SqlExpression instance,
        Type returnType) =>
        _sqlExpressionFactory.Function(
            functionName,
            [instance],
            nullable: true,
            argumentsPropagateNullability: [true],
            returnType);
}
