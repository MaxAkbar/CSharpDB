using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

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
    private static readonly MethodInfo ContainsStringMethod =
        typeof(string).GetRuntimeMethod(
            nameof(string.Contains),
            [typeof(string)])!;
    private static readonly MethodInfo StartsWithStringComparisonMethod =
        typeof(string).GetRuntimeMethod(
            nameof(string.StartsWith),
            [typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo EndsWithStringComparisonMethod =
        typeof(string).GetRuntimeMethod(
            nameof(string.EndsWith),
            [typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo ContainsStringComparisonMethod =
        typeof(string).GetRuntimeMethod(
            nameof(string.Contains),
            [typeof(string), typeof(StringComparison)])!;

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

        string? ordinalSearchFunction = method switch
        {
            _ when method == ContainsStringMethod =>
                "ORDINAL_CONTAINS",
            _ when method == StartsWithStringComparisonMethod &&
                IsConstantOrdinalComparison(arguments) =>
                "ORDINAL_STARTS_WITH",
            _ when method == EndsWithStringComparisonMethod &&
                IsConstantOrdinalComparison(arguments) =>
                "ORDINAL_ENDS_WITH",
            _ when method == ContainsStringComparisonMethod &&
                IsConstantOrdinalComparison(arguments) =>
                "ORDINAL_CONTAINS",
            _ => null,
        };
        if (ordinalSearchFunction is not null &&
            TryApplyConverterFreeTextMapping(
                instance,
                arguments[0],
                out SqlExpression mappedInstance,
                out SqlExpression mappedPattern))
        {
            return _sqlExpressionFactory.Function(
                ordinalSearchFunction,
                [mappedInstance, mappedPattern],
                nullable: true,
                argumentsPropagateNullability: [true, true],
                typeof(bool));
        }

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

    private bool TryApplyConverterFreeTextMapping(
        SqlExpression instance,
        SqlExpression pattern,
        out SqlExpression mappedInstance,
        out SqlExpression mappedPattern)
    {
        RelationalTypeMapping? textMapping =
            instance.TypeMapping ??
            pattern.TypeMapping;
        if (!IsConverterFreeTextMapping(textMapping) ||
            instance.TypeMapping is { } instanceMapping &&
            !IsConverterFreeTextMapping(instanceMapping) ||
            pattern.TypeMapping is { } patternMapping &&
            !IsConverterFreeTextMapping(patternMapping) ||
            !ContainsOnlyConverterFreeTextMappings(instance) ||
            !ContainsOnlyConverterFreeTextMappings(pattern))
        {
            mappedInstance = null!;
            mappedPattern = null!;
            return false;
        }

        mappedInstance = _sqlExpressionFactory.ApplyTypeMapping(
            instance,
            textMapping);
        mappedPattern = _sqlExpressionFactory.ApplyTypeMapping(
            pattern,
            textMapping);
        return true;
    }

    private static bool ContainsOnlyConverterFreeTextMappings(
        SqlExpression expression)
    {
        var visitor =
            new ConverterFreeTextMappingValidator();
        visitor.Visit(expression);
        return visitor.IsValid;
    }

    private static bool IsConverterFreeTextMapping(
        RelationalTypeMapping? typeMapping) =>
        typeMapping is
        {
            Converter: null,
            ClrType: var clrType,
        } &&
        clrType == typeof(string) &&
        string.Equals(
            typeMapping.StoreType,
            "TEXT",
            StringComparison.OrdinalIgnoreCase);

    private static bool IsConstantOrdinalComparison(
        IReadOnlyList<SqlExpression> arguments) =>
        arguments.Count == 2 &&
        arguments[1] is SqlConstantExpression
        {
            Value: StringComparison.Ordinal,
        };

    private sealed class ConverterFreeTextMappingValidator
        : ExpressionVisitor
    {
        public bool IsValid { get; private set; } = true;

        public override Expression? Visit(Expression? node)
        {
            if (!IsValid || node is null)
                return node;

            if (node is SqlExpression
                {
                    TypeMapping: { } typeMapping,
                } sqlExpression &&
                (typeMapping.Converter is not null ||
                 (sqlExpression.Type == typeof(string) ||
                  typeMapping.ClrType == typeof(string)) &&
                 !IsConverterFreeTextMapping(typeMapping)))
            {
                IsValid = false;
                return node;
            }

            return base.Visit(node);
        }
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
