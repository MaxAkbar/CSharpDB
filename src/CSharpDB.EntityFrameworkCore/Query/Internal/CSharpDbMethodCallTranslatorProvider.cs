using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public CSharpDbMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators(
        [
            new CSharpDbStringMethodTranslator(dependencies.SqlExpressionFactory),
            new CSharpDbMathMethodTranslator(dependencies.SqlExpressionFactory),
        ]);
    }
}

internal sealed class CSharpDbStringMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public CSharpDbStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
        => _sqlExpressionFactory = sqlExpressionFactory;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || method.DeclaringType != typeof(string))
            return null;

        if (arguments.Count == 0)
        {
            string? functionName = method.Name switch
            {
                nameof(string.ToLower) or nameof(string.ToLowerInvariant) => "LOWER",
                nameof(string.ToUpper) or nameof(string.ToUpperInvariant) => "UPPER",
                nameof(string.Trim) => "TRIM",
                nameof(string.TrimStart) => "LTRIM",
                nameof(string.TrimEnd) => "RTRIM",
                _ => null,
            };

            if (functionName is not null && method.GetParameters().Length == 0)
                return TextFunction(functionName, [instance], [true]);
        }

        ParameterInfo[] parameters = method.GetParameters();
        if (method.Name == nameof(string.Replace) &&
            parameters.Select(static parameter => parameter.ParameterType)
                .SequenceEqual([typeof(string), typeof(string)]))
        {
            SqlExpression replacement = _sqlExpressionFactory.Coalesce(
                arguments[1],
                _sqlExpressionFactory.Constant(
                    string.Empty,
                    instance.TypeMapping),
                instance.TypeMapping);

            return TextFunction(
                "REPLACE",
                [instance, arguments[0], replacement],
                [true, true, true]);
        }

        if (method.Name == nameof(string.Substring) &&
            parameters.Length is 1 or 2 &&
            parameters.All(static parameter => parameter.ParameterType == typeof(int)))
        {
            SqlExpression one = _sqlExpressionFactory.Constant(
                1,
                arguments[0].TypeMapping);
            SqlExpression oneBasedStart = _sqlExpressionFactory.Add(
                arguments[0],
                one,
                arguments[0].TypeMapping);

            return parameters.Length == 1
                ? TextFunction(
                    "SUBSTRING",
                    [instance, oneBasedStart],
                    [true, true])
                : TextFunction(
                    "SUBSTRING",
                    [instance, oneBasedStart, arguments[1]],
                    [true, true, true]);
        }

        return null;
    }

    private SqlExpression TextFunction(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<bool> argumentsPropagateNullability) =>
        _sqlExpressionFactory.Function(
            functionName,
            arguments,
            nullable: true,
            argumentsPropagateNullability,
            typeof(string),
            arguments[0].TypeMapping);
}

internal sealed class CSharpDbMathMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public CSharpDbMathMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
        => _sqlExpressionFactory = sqlExpressionFactory;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is not null ||
            method.DeclaringType != typeof(Math) ||
            method.GetParameters().Any(static parameter => parameter.ParameterType != typeof(double)))
        {
            return null;
        }

        if (arguments.Count == 1)
        {
            return method.Name switch
            {
                nameof(Math.Abs) => DoubleFunction("ABS", arguments),
                nameof(Math.Round) => DoubleFunction("ROUND", arguments),
                nameof(Math.Floor) => DoubleFunction("FLOOR", arguments),
                nameof(Math.Ceiling) => NegatedFloor(arguments[0]),
                nameof(Math.Truncate) => DoubleFunction("FIX", arguments),
                nameof(Math.Sign) => _sqlExpressionFactory.Function(
                    "SGN",
                    arguments,
                    nullable: true,
                    argumentsPropagateNullability: [true],
                    typeof(int)),
                _ => null,
            };
        }

        return null;
    }

    private SqlExpression NegatedFloor(SqlExpression argument)
    {
        SqlExpression negatedArgument = _sqlExpressionFactory.Negate(argument);
        SqlExpression floor = DoubleFunction("FLOOR", [negatedArgument]);
        return _sqlExpressionFactory.Negate(floor);
    }

    private SqlExpression DoubleFunction(
        string functionName,
        IReadOnlyList<SqlExpression> arguments) =>
        _sqlExpressionFactory.Function(
            functionName,
            arguments,
            nullable: true,
            argumentsPropagateNullability: Enumerable.Repeat(true, arguments.Count),
            typeof(double),
            arguments[0].TypeMapping);
}
