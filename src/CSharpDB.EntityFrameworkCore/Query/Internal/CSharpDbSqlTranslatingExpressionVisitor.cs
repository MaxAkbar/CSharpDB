using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using CSharpDB.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

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
        if (translated is SqlBinaryExpression sqlBinary &&
            IsDecimalComparison(sqlBinary.OperatorType) &&
            HasMixedProviderDecimalMappings(sqlBinary) &&
            CountDecimalOperands(methodCallExpression) >= 2)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        "Comparing provider-owned scaled decimal storage with a decimal application-converter mapping would compare incompatible raw values."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        if (translated is SqlBinaryExpression sqlBinaryWithScales &&
            IsDecimalComparison(
                sqlBinaryWithScales.OperatorType) &&
            TryGetDecimalScale(
                sqlBinaryWithScales.Left,
                out int leftScale) &&
            TryGetDecimalScale(
                sqlBinaryWithScales.Right,
                out int rightScale) &&
            leftScale != rightScale)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        $"Comparing decimal operands with different scales ({leftScale} and {rightScale}) would compare incompatible scaled integers."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

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

    protected override Expression VisitBinary(
        BinaryExpression binaryExpression)
    {
        if (IsUnsupportedDecimalArithmetic(
                binaryExpression))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        $"Decimal operator '{binaryExpression.NodeType}' is outside the exact scaled-integer foundation."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        Expression translated =
            base.VisitBinary(binaryExpression);
        if (translated is SqlBinaryExpression sqlBinary &&
            IsDecimalComparison(
                binaryExpression.NodeType) &&
            HasMixedProviderDecimalMappings(
                sqlBinary) &&
            IsDecimalType(
                binaryExpression.Left.Type) &&
            IsDecimalType(
                binaryExpression.Right.Type) &&
            !IsNullConstant(
                binaryExpression.Left) &&
            !IsNullConstant(
                binaryExpression.Right))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        "Comparing provider-owned scaled decimal storage with a decimal application-converter mapping would compare incompatible raw values."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        if (translated is SqlBinaryExpression sqlBinaryWithScales &&
            IsDecimalComparison(
                binaryExpression.NodeType) &&
            TryGetDecimalScale(
                sqlBinaryWithScales.Left,
                out int leftScale) &&
            TryGetDecimalScale(
                sqlBinaryWithScales.Right,
                out int rightScale) &&
            leftScale != rightScale)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        $"Comparing decimal operands with different scales ({leftScale} and {rightScale}) would compare incompatible scaled integers."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        return translated;
    }

    protected override Expression VisitConditional(
        ConditionalExpression conditionalExpression)
    {
        if (IsDecimalType(
                conditionalExpression.Type))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        "Conditional decimal projections are outside the exact scaled-integer foundation."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        return base.VisitConditional(
            conditionalExpression);
    }

    protected override Expression VisitUnary(
        UnaryExpression unaryExpression)
    {
        if (IsUnsupportedDecimalUnary(
                unaryExpression))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForDecimal(
                        $"Decimal unary operator or cast '{unaryExpression.NodeType}' is outside the exact scaled-integer foundation."));
            return QueryCompilationContext
                .NotTranslatedExpression;
        }

        return base.VisitUnary(unaryExpression);
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

    private static bool IsUnsupportedDecimalArithmetic(
        BinaryExpression expression) =>
        (expression.NodeType is
            ExpressionType.Add or
            ExpressionType.AddChecked or
            ExpressionType.Subtract or
            ExpressionType.SubtractChecked or
            ExpressionType.Multiply or
            ExpressionType.MultiplyChecked or
            ExpressionType.Divide or
            ExpressionType.Modulo or
            ExpressionType.Power or
            ExpressionType.Coalesce) &&
        (IsDecimalType(expression.Left.Type) ||
         IsDecimalType(expression.Right.Type) ||
         IsDecimalType(expression.Type));

    private static bool IsUnsupportedDecimalUnary(
        UnaryExpression expression)
    {
        bool operandIsDecimal =
            IsDecimalType(expression.Operand.Type);
        bool resultIsDecimal =
            IsDecimalType(expression.Type);
        return (expression.NodeType is
                ExpressionType.Negate or
                ExpressionType.NegateChecked or
                ExpressionType.UnaryPlus or
                ExpressionType.Increment or
                ExpressionType.Decrement) &&
            (operandIsDecimal || resultIsDecimal) ||
            (expression.NodeType is
                ExpressionType.Convert or
                ExpressionType.ConvertChecked) &&
            operandIsDecimal != resultIsDecimal;
    }

    private static bool IsDecimalType(Type type) =>
        (Nullable.GetUnderlyingType(type) ?? type) ==
        typeof(decimal);

    private static bool IsDecimalComparison(
        ExpressionType expressionType) =>
        expressionType is
            ExpressionType.Equal or
            ExpressionType.NotEqual or
            ExpressionType.LessThan or
            ExpressionType.LessThanOrEqual or
            ExpressionType.GreaterThan or
            ExpressionType.GreaterThanOrEqual;

    private static bool HasMixedProviderDecimalMappings(
        SqlBinaryExpression expression)
    {
        bool leftIsProviderDecimal =
            TryGetDecimalScale(
                expression.Left,
                out _);
        bool rightIsProviderDecimal =
            TryGetDecimalScale(
                expression.Right,
                out _);
        return leftIsProviderDecimal !=
            rightIsProviderDecimal;
    }

    private static int CountDecimalOperands(
        MethodCallExpression expression)
    {
        int count = expression.Object is not null &&
            IsDecimalType(
                UnwrapConvert(expression.Object).Type)
            ? 1
            : 0;
        return count + expression.Arguments.Count(
            argument => IsDecimalType(
                UnwrapConvert(argument).Type));
    }

    private static bool IsNullConstant(
        Expression expression)
    {
        expression = UnwrapConvert(expression);
        return expression is ConstantExpression
        {
            Value: null,
        };
    }

    private static Expression UnwrapConvert(
        Expression expression)
    {
        while (expression is UnaryExpression
               {
                   NodeType:
                       ExpressionType.Convert or
                       ExpressionType.ConvertChecked,
               } conversion)
        {
            expression = conversion.Operand;
        }

        return expression;
    }

    private static bool TryGetDecimalScale(
        SqlExpression expression,
        out int scale)
    {
        while (expression is SqlUnaryExpression
               {
                   OperatorType: ExpressionType.Convert,
               } conversion)
        {
            expression = conversion.Operand;
        }

        if (expression.TypeMapping?.Converter is
            CSharpDbDecimalToInt64Converter converter)
        {
            scale = converter.Scale;
            return true;
        }

        scale = 0;
        return false;
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

    public static string ForDistinctAggregate(
        string aggregateName,
        string reason) =>
        $"CDBEF1004: The CSharpDB EF Core provider cannot translate distinct aggregate '{aggregateName}' for this query shape. " +
        $"{reason} Keep Where before a single-column Select and Distinct, use a qualified numeric type, or call AsEnumerable() explicitly after selective server-side filters. See {DocumentationUrl}.";

    public static string ForGroupedAggregate(string reason) =>
        "CDBEF1005: The CSharpDB EF Core provider cannot translate this GroupBy shape to the qualified server-side aggregate surface. " +
        $"{reason} Use direct scalar or composite mapped keys and project keys plus supported aggregates, or call AsEnumerable() explicitly after selective server-side filters. See {DocumentationUrl}.";

    public static string ForDecimal(string reason) =>
        "CDBEF1006: The CSharpDB EF Core provider cannot translate this decimal expression within the exact scaled-integer foundation. " +
        $"{reason} Use comparisons and ordering over directly mapped decimal properties, or perform arithmetic after AsEnumerable(). See {DocumentationUrl}.";

    public static string ForInnerJoin(string reason) =>
        "CDBEF1007: The CSharpDB EF Core provider cannot translate this inner-join shape within the bounded direct-join surface. " +
        $"{reason} Join two direct entity roots on one nonnullable INTEGER-backed int, long, or int/long-backed enum property, then apply filtering, ordering, and pagination after Join. See {DocumentationUrl}.";

    public static string ForLeftJoin(string reason) =>
        "CDBEF1008: The CSharpDB EF Core provider cannot translate this left-join shape within the bounded direct-join surface. " +
        $"{reason} LeftJoin two direct entity roots on one nonnullable INTEGER-backed int, long, or int/long-backed enum property, use nullable unmatched-side projections, then apply filtering, ordering, and pagination after LeftJoin. See {DocumentationUrl}.";

    public static string? FindUnsupportedOperator(Expression expression)
    {
        var visitor = new UnsupportedOperatorFindingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.OperatorName;
    }

    public static string? FindUnsupportedLeftJoinShape(
        Expression expression)
    {
        var visitor =
            new UnsupportedLeftJoinShapeFindingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.Diagnostic;
    }

    public static string? FindUnsafeDecimalExpression(
        Expression expression)
    {
        var visitor =
            new UnsafeDecimalExpressionFindingVisitor();
        visitor.Visit(expression);
        return visitor.Diagnostic;
    }

    public static string? FindUnsafeDecimalParameterReuse(
        Expression expression,
        IModel model)
    {
        var visitor =
            new UnsafeDecimalParameterReuseFindingVisitor(
                model);
        visitor.Visit(expression);
        return visitor.Diagnostic;
    }

    public static string? FindUnsafeDistinctCardinality(
        Expression expression)
    {
        var visitor =
            new UnsafeDistinctCardinalityFindingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.Diagnostic;
    }

    public static string? FindUnsafeGroupedAggregate(
        Expression expression,
        IModel model)
    {
        var visitor =
            new UnsafeGroupedAggregateFindingExpressionVisitor(
                model);
        visitor.Visit(expression);
        return visitor.Diagnostic;
    }

    public static string?
        FindUnsupportedGroupMaterialization(
            Expression expression)
    {
        Type? elementType =
            TryGetSequenceElementType(expression.Type);
        return elementType is not null &&
            GroupingExpressionSupport.IsGroupingType(
                elementType)
            ? ForGroupedAggregate(
                "Materializing IGrouping results is outside the qualified server-side aggregate surface; project the key and supported bare aggregates.")
            : null;
    }

    public static string? FindUnsafeGroupBySource(
        Expression expression)
    {
        var visitor =
            new UnsafeGroupBySourceFindingExpressionVisitor();
        visitor.Visit(expression);
        return visitor.Diagnostic;
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

    private static Type? TryGetSequenceElementType(
        Type sequenceType)
    {
        if (sequenceType.IsGenericType)
        {
            Type definition =
                sequenceType.GetGenericTypeDefinition();
            if (definition == typeof(IQueryable<>) ||
                definition == typeof(IOrderedQueryable<>) ||
                definition == typeof(IEnumerable<>))
            {
                return sequenceType.GetGenericArguments()[0];
            }
        }

        return sequenceType.GetInterfaces()
            .FirstOrDefault(type =>
                type.IsGenericType &&
                (type.GetGenericTypeDefinition() ==
                    typeof(IQueryable<>) ||
                 type.GetGenericTypeDefinition() ==
                    typeof(IEnumerable<>)))?
            .GetGenericArguments()[0];
    }

    private sealed class UnsupportedOperatorFindingExpressionVisitor
        : ExpressionVisitor
    {
        public string? OperatorName { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (OperatorName is null &&
                node.Method.Name ==
                    "ExecuteUpdate" &&
                node.Method.DeclaringType?.Namespace?
                    .StartsWith(
                        "Microsoft.EntityFrameworkCore",
                        StringComparison.Ordinal) ==
                    true)
            {
                OperatorName =
                    "RelationalQueryableExtensions.ExecuteUpdate";
            }
            else if (OperatorName is null &&
                node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name ==
                        nameof(Queryable.Join) &&
                    node.Arguments.Count == 6)
                {
                    OperatorName =
                        "Queryable.Join(comparer)";
                }
                else if (node.Method.Name ==
                             "LeftJoin" &&
                         node.Arguments.Count == 6)
                {
                    OperatorName =
                        "Queryable.LeftJoin(comparer)";
                }
                else
                {
                    OperatorName =
                        node.Method.Name switch
                        {
                            nameof(Queryable.TakeWhile) =>
                                "Queryable.TakeWhile",
                            nameof(Queryable.SkipWhile) =>
                                "Queryable.SkipWhile",
                            nameof(Queryable.Concat) =>
                                "Queryable.Concat",
                            nameof(Queryable.Union) =>
                                "Queryable.Union",
                            nameof(Queryable.Except) =>
                                "Queryable.Except",
                            nameof(Queryable.Intersect) =>
                                "Queryable.Intersect",
                            nameof(Queryable.GroupJoin) =>
                                "Queryable.GroupJoin",
                            nameof(Queryable.SelectMany) =>
                                "Queryable.SelectMany",
                            nameof(Queryable.DefaultIfEmpty) =>
                                "Queryable.DefaultIfEmpty",
                            "RightJoin" =>
                                "Queryable.RightJoin",
                            _ => null,
                        };
                }
            }

            return OperatorName is null
                ? base.VisitMethodCall(node)
                : node;
        }
    }

    private sealed class
        UnsupportedLeftJoinShapeFindingExpressionVisitor
        : ExpressionVisitor
    {
        public string? Diagnostic { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (Diagnostic is null &&
                node.Method.DeclaringType ==
                    typeof(Queryable) &&
                node.Method.Name ==
                    "LeftJoin" &&
                node.Arguments.Count == 5 &&
                (IsCompositeKeySelector(
                    node.Arguments[2]) ||
                 IsCompositeKeySelector(
                    node.Arguments[3])))
            {
                Diagnostic = ForLeftJoin(
                    "Composite left-join keys are not yet qualified; use one direct nonnullable mapped scalar property.");
            }

            return Diagnostic is null
                ? base.VisitMethodCall(node)
                : node;
        }

        private static bool IsCompositeKeySelector(
            Expression expression)
        {
            while (expression is UnaryExpression
                   {
                       NodeType: ExpressionType.Quote,
                   } quote)
            {
                expression = quote.Operand;
            }

            if (expression is not LambdaExpression selector)
                return false;

            Expression body = selector.Body;
            while (body is UnaryExpression
                   {
                       NodeType:
                           ExpressionType.Convert or
                           ExpressionType.ConvertChecked,
                   } conversion)
            {
                body = conversion.Operand;
            }

            return body is NewExpression or
                MemberInitExpression or
                NewArrayExpression;
        }
    }

    private sealed class
        UnsafeDecimalExpressionFindingVisitor
        : ExpressionVisitor
    {
        public string? Diagnostic { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (Diagnostic is null &&
                IsUnsupportedDecimalMethod(node))
            {
                Diagnostic = ForDecimal(
                    $"Decimal method '{node.Method.Name}' is outside the exact scaled-integer foundation.");
            }

            return Diagnostic is null
                ? base.VisitMethodCall(node)
                : node;
        }

        private static bool IsUnsupportedDecimalMethod(
            MethodCallExpression node)
        {
            if (node.Method.Name ==
                    nameof(List<decimal>.Contains) &&
                node.Object is not null &&
                node.Arguments.Count == 1 &&
                IsDecimalType(
                    node.Arguments[0].Type) &&
                (typeof(IEnumerable<decimal>)
                        .IsAssignableFrom(
                            node.Object.Type) ||
                 typeof(IEnumerable<decimal?>)
                        .IsAssignableFrom(
                            node.Object.Type)))
            {
                return true;
            }

            if (node.Method.DeclaringType is
                    { IsGenericType: true } declaringType &&
                declaringType.GetGenericTypeDefinition() ==
                    typeof(Nullable<>) &&
                declaringType.GetGenericArguments()[0] ==
                    typeof(decimal) &&
                node.Method.Name ==
                    nameof(Nullable<decimal>
                        .GetValueOrDefault))
            {
                return true;
            }

            if (node.Method.IsGenericMethod &&
                node.Method.Name == nameof(Queryable.Contains) &&
                node.Method.DeclaringType is not null &&
                (node.Method.DeclaringType ==
                    typeof(Queryable) ||
                 node.Method.DeclaringType ==
                    typeof(Enumerable)) &&
                IsDecimalType(
                    node.Method
                        .GetGenericArguments()[0]))
            {
                return true;
            }

            if (node.Method.DeclaringType == typeof(decimal))
            {
                return node.Method.Name !=
                    nameof(decimal.Equals);
            }

            bool hasDecimalOperand =
                node.Object is not null &&
                    IsDecimalType(node.Object.Type) ||
                node.Arguments.Any(argument =>
                    IsDecimalType(argument.Type));
            bool returnsDecimal =
                IsDecimalType(node.Method.ReturnType);
            return node.Method.DeclaringType == typeof(Math) &&
                    (hasDecimalOperand ||
                     returnsDecimal) ||
                node.Method.DeclaringType == typeof(Convert) &&
                    (hasDecimalOperand ||
                     returnsDecimal);
        }

        protected override Expression VisitBinary(
            BinaryExpression node)
        {
            if (Diagnostic is null &&
                node.NodeType is
                    ExpressionType.Add or
                    ExpressionType.AddChecked or
                    ExpressionType.Subtract or
                    ExpressionType.SubtractChecked or
                    ExpressionType.Multiply or
                    ExpressionType.MultiplyChecked or
                    ExpressionType.Divide or
                    ExpressionType.Modulo or
                    ExpressionType.Power or
                    ExpressionType.Coalesce &&
                (IsDecimalType(node.Left.Type) ||
                 IsDecimalType(node.Right.Type) ||
                 IsDecimalType(node.Type)))
            {
                Diagnostic = ForDecimal(
                    $"Decimal operator '{node.NodeType}' is outside the exact scaled-integer foundation.");
            }

            return Diagnostic is null
                ? base.VisitBinary(node)
                : node;
        }

        protected override Expression VisitUnary(
            UnaryExpression node)
        {
            bool operandIsDecimal =
                IsDecimalType(node.Operand.Type);
            bool resultIsDecimal =
                IsDecimalType(node.Type);
            if (Diagnostic is null &&
                ((node.NodeType is
                        ExpressionType.Negate or
                        ExpressionType.NegateChecked or
                        ExpressionType.UnaryPlus or
                        ExpressionType.Increment or
                        ExpressionType.Decrement &&
                    (operandIsDecimal ||
                     resultIsDecimal)) ||
                 (node.NodeType is
                        ExpressionType.Convert or
                        ExpressionType.ConvertChecked &&
                    operandIsDecimal != resultIsDecimal)))
            {
                Diagnostic = ForDecimal(
                    $"Decimal unary operator or cast '{node.NodeType}' is outside the exact scaled-integer foundation.");
            }

            return Diagnostic is null
                ? base.VisitUnary(node)
                : node;
        }

        protected override Expression VisitConditional(
            ConditionalExpression node)
        {
            if (Diagnostic is null &&
                IsDecimalType(node.Type))
            {
                Diagnostic = ForDecimal(
                    "Conditional decimal projections are outside the exact scaled-integer foundation.");
            }

            return Diagnostic is null
                ? base.VisitConditional(node)
                : node;
        }

        private static bool IsDecimalType(Type type) =>
            (Nullable.GetUnderlyingType(type) ?? type) ==
            typeof(decimal);
    }

    private sealed class
        UnsafeDecimalParameterReuseFindingVisitor
        : ExpressionVisitor
    {
        private readonly IModel _model;
        private readonly Dictionary<
            string,
            (int Precision, int Scale)> _facetsByParameter =
                new(StringComparer.Ordinal);

        public UnsafeDecimalParameterReuseFindingVisitor(
            IModel model)
        {
            _model = model;
        }

        public string? Diagnostic { get; private set; }

        protected override Expression VisitBinary(
            BinaryExpression node)
        {
            if (Diagnostic is null &&
                IsDecimalComparison(node.NodeType))
            {
                RecordParameterFacets(
                    node.Left,
                    node.Right);
                RecordParameterFacets(
                    node.Right,
                    node.Left);
            }

            return Diagnostic is null
                ? base.VisitBinary(node)
                : node;
        }

        private void RecordParameterFacets(
            Expression propertyExpression,
            Expression parameterExpression)
        {
            if (Diagnostic is not null ||
                !TryGetDecimalPropertyFacets(
                    propertyExpression,
                    out int precision,
                    out int scale) ||
                UnwrapConvert(parameterExpression) is not
                    QueryParameterExpression parameter)
            {
                return;
            }

            var facets =
                (Precision: precision, Scale: scale);
            if (_facetsByParameter.TryGetValue(
                    parameter.Name,
                    out var previousFacets) &&
                previousFacets != facets)
            {
                Diagnostic = ForDecimal(
                    $"Parameter '{parameter.Name}' is reused with incompatible decimal facets decimal({previousFacets.Precision}, {previousFacets.Scale}) and decimal({facets.Precision}, {facets.Scale}). Capture a separate parameter value for each decimal mapping.");
                return;
            }

            _facetsByParameter[parameter.Name] =
                facets;
        }

        private bool TryGetDecimalPropertyFacets(
            Expression expression,
            out int precision,
            out int scale)
        {
            precision = 0;
            scale = 0;
            expression = UnwrapConvert(expression);

            if (expression is MemberExpression
                {
                    Member.Name: nameof(Nullable<int>.Value),
                    Expression: MemberExpression nullableMember,
                } &&
                Nullable.GetUnderlyingType(
                    nullableMember.Type) ==
                typeof(decimal))
            {
                expression = nullableMember;
            }

            if (expression is not MemberExpression member)
                return false;

            Type? entityClrType =
                member.Expression?.Type;
            IProperty? property = _model
                .GetEntityTypes()
                .Where(entityType =>
                    entityType.ClrType ==
                        entityClrType ||
                    entityType.ClrType ==
                        member.Member.DeclaringType)
                .Select(entityType =>
                    entityType.FindProperty(
                        member.Member.Name))
                .FirstOrDefault(candidate =>
                    candidate is not null);
            if (property is null ||
                property.GetValueConverter() is not null ||
                (Nullable.GetUnderlyingType(
                        property.ClrType) ??
                    property.ClrType) != typeof(decimal))
            {
                return false;
            }

            (precision, scale) =
                CSharpDbDecimalStorage.ResolveFacets(
                    property.GetPrecision(),
                    property.GetScale());
            return true;
        }

        private static Expression UnwrapConvert(
            Expression expression)
        {
            while (expression is UnaryExpression
                   {
                       NodeType:
                           ExpressionType.Convert or
                           ExpressionType.ConvertChecked,
                   } conversion)
            {
                expression = conversion.Operand;
            }

            return expression;
        }

        private static bool IsDecimalComparison(
            ExpressionType nodeType) =>
            nodeType is
                ExpressionType.Equal or
                ExpressionType.NotEqual or
                ExpressionType.LessThan or
                ExpressionType.LessThanOrEqual or
                ExpressionType.GreaterThan or
                ExpressionType.GreaterThanOrEqual;
    }

    private sealed class UnsafeGroupBySourceFindingExpressionVisitor
        : ExpressionVisitor
    {
        public string? Diagnostic { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (Diagnostic is null &&
                node.Method.DeclaringType == typeof(Queryable) &&
                node.Method.Name == nameof(Queryable.GroupBy) &&
                node.Arguments.Count > 0 &&
                !IsQualifiedSource(node.Arguments[0]))
            {
                string sourceOperator =
                    node.Arguments[0] is MethodCallExpression
                        sourceCall
                        ? sourceCall.Method.Name
                        : node.Arguments[0].NodeType.ToString();
                Diagnostic = ForGroupedAggregate(
                    $"GroupBy source operator '{sourceOperator}' is outside the qualified direct-table shape; only a mapped query root with optional Where filters and transparent EF query annotations is supported.");
            }

            return Diagnostic is null
                ? base.VisitMethodCall(node)
                : node;
        }

        private static bool IsQualifiedSource(
            Expression source)
        {
            if (source is EntityQueryRootExpression)
                return true;

            if (source is not MethodCallExpression
                {
                    Arguments.Count: > 0,
                } methodCall)
            {
                return false;
            }

            if (methodCall.Method.DeclaringType ==
                    typeof(Queryable) &&
                methodCall.Method.Name is
                    nameof(Queryable.Where) or
                    nameof(Queryable.AsQueryable))
            {
                return IsQualifiedSource(
                    methodCall.Arguments[0]);
            }

            return methodCall.Method.DeclaringType?.Namespace ==
                    "Microsoft.EntityFrameworkCore" &&
                methodCall.Method.Name is
                    "AsNoTracking" or
                    "AsNoTrackingWithIdentityResolution" or
                    "AsTracking" or
                    "TagWith" or
                    "TagWithCallSite" or
                    "IgnoreQueryFilters" &&
                IsQualifiedSource(methodCall.Arguments[0]);
        }
    }

    private sealed class
        UnsafeDistinctCardinalityFindingExpressionVisitor
        : ExpressionVisitor
    {
        public string? Diagnostic { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (Diagnostic is null &&
                IsAggregate(node.Method) &&
                node.Arguments.Count > 0 &&
                IsWhereAfterDistinct(node.Arguments[0]))
            {
                Diagnostic = ForDistinctAggregate(
                    node.Method.Name,
                    "A predicate after Distinct is not supported; apply Where before Select and Distinct.");
            }
            else if (Diagnostic is null &&
                IsAggregate(node.Method) &&
                node.Arguments.Count > 0 &&
                TryFindServerDistinct(
                    node.Arguments[0],
                    out MethodCallExpression? rawDistinct,
                    out IReadOnlyList<string> interveningOperators) &&
                GetRawDistinctShapeDiagnostic(
                    rawDistinct,
                    interveningOperators) is
                    { } rawShapeDiagnostic)
            {
                Diagnostic = ForDistinctAggregate(
                    node.Method.Name,
                    rawShapeDiagnostic);
            }
            else if (Diagnostic is null &&
                IsCountOrLongCount(node.Method) &&
                node.Arguments.Count > 0 &&
                TryFindServerDistinct(
                    node.Arguments[0],
                    out MethodCallExpression? distinctCall,
                    out IReadOnlyList<string> countInterveningOperators) &&
                countInterveningOperators.Count == 0 &&
                IsDistinct(
                    distinctCall.Method,
                    out Type? elementType) &&
                IsServerDistinctSource(distinctCall))
            {
                string aggregateName = node.Method.Name;
                if (node.Arguments.Count != 1)
                {
                    Diagnostic = ForDistinctAggregate(
                        aggregateName,
                        "A predicate after Distinct is not supported; apply Where before Select and Distinct.");
                }
                else
                {
                    Type? nullableType =
                        Nullable.GetUnderlyingType(elementType);
                    if (nullableType is not null)
                    {
                        Diagnostic = ForDistinctAggregate(
                            aggregateName,
                            "Nullable distinct cardinality counts NULL once in LINQ, while SQL COUNT(DISTINCT ...) ignores NULL.");
                    }
                    else if (elementType != typeof(int))
                    {
                        Diagnostic = ForDistinctAggregate(
                            aggregateName,
                            "Distinct Count and LongCount are qualified only for a single nonnullable int column.");
                    }
                }
            }

            return Diagnostic is null
                ? base.VisitMethodCall(node)
                : node;
        }

        private static string? GetRawDistinctShapeDiagnostic(
            MethodCallExpression distinctCall,
            IReadOnlyList<string> interveningOperators)
        {
            if (interveningOperators.Count > 0)
            {
                return
                    $"Operator '{interveningOperators[0]}' between Distinct and the aggregate is outside the qualified direct shape.";
            }

            if (distinctCall.Arguments.Count != 1)
            {
                return
                    "Distinct must have exactly one direct sequence source.";
            }

            Expression source =
                PeelTransparentDistinctWrappers(
                    distinctCall.Arguments[0]);
            if (source is not MethodCallExpression select ||
                select.Method.DeclaringType is null ||
                select.Method.DeclaringType != typeof(Queryable) &&
                select.Method.DeclaringType != typeof(Enumerable) ||
                select.Method.Name != nameof(Queryable.Select) ||
                select.Arguments.Count != 2)
            {
                string sourceOperator =
                    source is MethodCallExpression sourceCall
                        ? sourceCall.Method.Name
                        : source.NodeType.ToString();
                return
                    $"Distinct source operator '{sourceOperator}' is outside the qualified shape; Distinct must immediately follow one direct-column Select.";
            }

            if (!GroupingExpressionSupport.TryGetLambda(
                    select.Arguments[1],
                    out LambdaExpression selector) ||
                selector.Parameters.Count != 1 ||
                selector.Body is not MemberExpression member ||
                !ReferenceEquals(
                    member.Expression,
                    selector.Parameters[0]))
            {
                return
                    "The pre-Distinct selector must be one direct member access without casts, identity projections, arithmetic, or composite construction.";
            }

            Expression selectedSource =
                PeelTransparentDistinctWrappers(
                    select.Arguments[0]);
            if (GroupingExpressionSupport
                .IsDirectGroupingParameter(
                    selectedSource))
            {
                return null;
            }

            while (selectedSource is MethodCallExpression sourceCall &&
                   sourceCall.Arguments.Count > 0)
            {
                if (sourceCall.Method.DeclaringType ==
                        typeof(Queryable) &&
                    sourceCall.Method.Name ==
                        nameof(Queryable.Where))
                {
                    selectedSource =
                        PeelTransparentDistinctWrappers(
                            sourceCall.Arguments[0]);
                    continue;
                }

                return
                    $"Distinct source operator '{sourceCall.Method.Name}' is outside the qualified shape; only optional Where filters may precede the direct-column Select.";
            }

            return selectedSource is EntityQueryRootExpression
                ? null
                : "The distinct aggregate source must be a direct mapped query root with optional Where filters, or a direct grouping parameter.";
        }

        private static Expression
            PeelTransparentDistinctWrappers(
                Expression source)
        {
            while (source is MethodCallExpression methodCall &&
                   methodCall.Arguments.Count > 0 &&
                   (IsTransparentEfAnnotation(
                        methodCall.Method) ||
                    methodCall.Method.DeclaringType ==
                        typeof(Queryable) &&
                    methodCall.Method.Name ==
                        nameof(Queryable.AsQueryable)))
            {
                source = methodCall.Arguments[0];
            }

            return source;
        }

        private static bool IsCountOrLongCount(MethodInfo method) =>
            method.DeclaringType is not null &&
            (method.DeclaringType == typeof(Queryable) ||
             method.DeclaringType == typeof(Enumerable)) &&
            (method.Name == nameof(Queryable.Count) ||
             method.Name == nameof(Queryable.LongCount));

        private static bool IsAggregate(MethodInfo method) =>
            method.DeclaringType is not null &&
            (method.DeclaringType == typeof(Queryable) ||
             method.DeclaringType == typeof(Enumerable)) &&
            method.Name is
                nameof(Queryable.Count) or
                nameof(Queryable.LongCount) or
                nameof(Queryable.Sum) or
                nameof(Queryable.Average) or
                nameof(Queryable.Min) or
                nameof(Queryable.Max);

        private static bool IsWhereAfterDistinct(
            Expression source)
        {
            bool foundWhere = false;
            while (source is MethodCallExpression methodCall &&
                   methodCall.Arguments.Count > 0)
            {
                bool isSequenceOperator =
                    methodCall.Method.DeclaringType ==
                        typeof(Queryable) ||
                    methodCall.Method.DeclaringType ==
                        typeof(Enumerable);
                if (isSequenceOperator &&
                    methodCall.Method.Name ==
                        nameof(Queryable.Distinct))
                {
                    return foundWhere &&
                        IsServerDistinctSource(
                            methodCall);
                }

                if (isSequenceOperator &&
                    methodCall.Method.Name ==
                        nameof(Queryable.Where))
                {
                    foundWhere = true;
                }

                if (isSequenceOperator ||
                    IsTransparentEfAnnotation(
                        methodCall.Method))
                {
                    source = methodCall.Arguments[0];
                    continue;
                }

                break;
            }

            return false;
        }

        private static bool TryFindServerDistinct(
            Expression source,
            out MethodCallExpression distinctCall,
            out IReadOnlyList<string> interveningOperators)
        {
            var operators = new List<string>();
            while (source is MethodCallExpression methodCall &&
                   methodCall.Arguments.Count > 0)
            {
                if (IsDistinct(
                        methodCall.Method,
                        out _) &&
                    IsServerDistinctSource(
                        methodCall))
                {
                    distinctCall = methodCall;
                    interveningOperators = operators;
                    return true;
                }

                bool isSequenceOperator =
                    methodCall.Method.DeclaringType ==
                        typeof(Queryable) ||
                    methodCall.Method.DeclaringType ==
                        typeof(Enumerable);
                if (!isSequenceOperator &&
                    !IsTransparentEfAnnotation(
                        methodCall.Method))
                {
                    break;
                }

                if (!IsTransparentEfAnnotation(
                        methodCall.Method) &&
                    !(methodCall.Method.DeclaringType ==
                        typeof(Queryable) &&
                      methodCall.Method.Name ==
                        nameof(Queryable.AsQueryable)))
                {
                    operators.Add(
                        methodCall.Method.Name);
                }

                source = methodCall.Arguments[0];
            }

            distinctCall = null!;
            interveningOperators = [];
            return false;
        }

        private static bool IsTransparentEfAnnotation(
            MethodInfo method) =>
            method.DeclaringType?.Namespace ==
                "Microsoft.EntityFrameworkCore" &&
            method.Name is
                "AsNoTracking" or
                "AsNoTrackingWithIdentityResolution" or
                "AsTracking" or
                "TagWith" or
                "TagWithCallSite" or
                "IgnoreQueryFilters";

        private static bool IsDistinct(
            MethodInfo method,
            out Type elementType)
        {
            elementType = null!;
            if (method.DeclaringType is null ||
                method.DeclaringType != typeof(Queryable) &&
                method.DeclaringType != typeof(Enumerable) ||
                method.Name != nameof(Queryable.Distinct) ||
                !method.IsGenericMethod)
            {
                return false;
            }

            elementType = method.GetGenericArguments()[0];
            return true;
        }

        private static bool IsServerDistinctSource(
            MethodCallExpression distinctCall)
        {
            if (GroupingExpressionSupport.ContainsGroupingParameter(
                    distinctCall.Arguments[0]))
            {
                return true;
            }

            var visitor =
                new EntityQueryRootFindingExpressionVisitor();
            visitor.Visit(
                distinctCall.Arguments[0]);
            return visitor.Found;
        }

        private sealed class
            EntityQueryRootFindingExpressionVisitor
            : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitExtension(
                Expression node)
            {
                if (node is EntityQueryRootExpression)
                {
                    Found = true;
                    return node;
                }

                return Found
                    ? node
                    : base.VisitExtension(node);
            }
        }
    }

    private sealed class UnsafeGroupedAggregateFindingExpressionVisitor
        : ExpressionVisitor
    {
        private readonly IModel _model;

        public UnsafeGroupedAggregateFindingExpressionVisitor(
            IModel model)
        {
            _model = model;
        }

        public string? Diagnostic { get; private set; }

        protected override Expression VisitMethodCall(
            MethodCallExpression node)
        {
            if (Diagnostic is null &&
                GetPostGroupedOrderingDiagnostic(node) is
                    { } orderingDiagnostic)
            {
                Diagnostic = orderingDiagnostic;
            }
            else if (Diagnostic is null &&
                IsPostGroupedProjectionTransform(node))
            {
                Diagnostic = ForGroupedAggregate(
                    $"Post-GroupBy operator '{node.Method.Name}' would require transforming an already-projected grouped result through an unsupported derived-query shape; keep filtering on the groups, project direct keys and bare aggregates once, and order only by those projected values.");
            }
            else if (Diagnostic is null &&
                IsSelect(node.Method) &&
                node.Arguments.Count == 2 &&
                GroupingExpressionSupport.TryGetLambda(
                    node.Arguments[1],
                    out LambdaExpression selector) &&
                selector.Parameters.Count == 1 &&
                GroupingExpressionSupport.IsGroupingType(
                    selector.Parameters[0].Type) &&
                GetProjectionDiagnostic(
                    selector.Body,
                    selector.Parameters[0]) is
                    { } projectionDiagnostic)
            {
                Diagnostic = projectionDiagnostic;
            }
            else if (Diagnostic is null &&
                node.Arguments.Count > 0 &&
                IsUnsupportedGroupedSequenceOperator(
                    node.Method) &&
                (GroupingExpressionSupport.IsGroupingSequence(
                        node.Arguments[0]) ||
                 IsGroupingResultSequence(
                     node.Arguments[0]) ||
                 IsUnsupportedProjectedGroupedTerminal(
                     node) &&
                 GroupingExpressionSupport.ContainsGroupByQuery(
                     node.Arguments[0])))
            {
                Diagnostic = ForGroupedAggregate(
                    $"Grouped sequence operator '{node.Method.Name}' is outside the qualified aggregate surface; project the key and supported bare aggregates instead.");
            }
            else if (Diagnostic is null &&
                node.Arguments.Count > 0 &&
                IsAggregate(node.Method) &&
                GroupingExpressionSupport.IsGroupingSequence(
                    node.Arguments[0]))
            {
                Diagnostic = ValidateAggregate(node);
            }

            return Diagnostic is null
                ? base.VisitMethodCall(node)
                : node;
        }

        private static bool IsGroupingResultSequence(
            Expression expression) =>
            TryGetSequenceElementType(
                expression.Type) is
                { } elementType &&
            GroupingExpressionSupport.IsGroupingType(
                elementType);

        private static bool IsUnsupportedProjectedGroupedTerminal(
            MethodCallExpression node) =>
            node.Arguments.Count != 1 ||
            node.Method.Name is not (
                nameof(Queryable.First) or
                nameof(Queryable.FirstOrDefault) or
                nameof(Queryable.Single) or
                nameof(Queryable.SingleOrDefault));

        private static string? GetPostGroupedOrderingDiagnostic(
            MethodCallExpression node)
        {
            if (node.Method.DeclaringType != typeof(Queryable) ||
                node.Method.Name is not (
                    nameof(Queryable.OrderBy) or
                    nameof(Queryable.OrderByDescending) or
                    nameof(Queryable.ThenBy) or
                    nameof(Queryable.ThenByDescending)) ||
                node.Arguments.Count != 2 ||
                !GroupingExpressionSupport.ContainsGroupByQuery(
                    node.Arguments[0]) ||
                TryGetSequenceElementType(
                    node.Arguments[0].Type) is
                    not { } sourceElementType ||
                GroupingExpressionSupport.IsGroupingType(
                    sourceElementType) ||
                !GroupingExpressionSupport.TryGetLambda(
                    node.Arguments[1],
                    out LambdaExpression selector) ||
                selector.Parameters.Count != 1)
            {
                return null;
            }

            Expression body = selector.Body;
            bool isBareProjectedValue =
                ReferenceEquals(
                    body,
                    selector.Parameters[0]) ||
                body is MemberExpression member &&
                ReferenceEquals(
                    member.Expression,
                    selector.Parameters[0]);
            return isBareProjectedValue
                ? null
                : ForGroupedAggregate(
                    "Grouped ordering must use one directly projected key or bare aggregate without casts, arithmetic, member transformations, or other computed expressions.");
        }

        private static bool IsPostGroupedProjectionTransform(
            MethodCallExpression node)
        {
            if (node.Arguments.Count == 0 ||
                node.Method.DeclaringType is null ||
                node.Method.DeclaringType != typeof(Queryable) &&
                node.Method.DeclaringType != typeof(Enumerable) ||
                !node.Arguments.Any(
                    GroupingExpressionSupport
                        .ContainsGroupByQuery))
            {
                return false;
            }

            Type? sourceElementType =
                TryGetSequenceElementType(
                    node.Arguments[0].Type);
            bool sourceIsGrouping =
                sourceElementType is not null &&
                GroupingExpressionSupport.IsGroupingType(
                    sourceElementType);
            if (sourceIsGrouping &&
                node.Method.Name is
                    nameof(Queryable.Select) or
                    nameof(Queryable.Where))
            {
                return false;
            }

            if (sourceIsGrouping &&
                node.Method.Name is
                    nameof(Queryable.OrderBy) or
                    nameof(Queryable.OrderByDescending) or
                    nameof(Queryable.ThenBy) or
                    nameof(Queryable.ThenByDescending))
            {
                return true;
            }

            return node.Method.Name is
                nameof(Queryable.Select) or
                nameof(Queryable.Where) or
                nameof(Queryable.Distinct) or
                nameof(Queryable.Skip) or
                nameof(Queryable.Take) or
                nameof(Queryable.Reverse) or
                nameof(Queryable.Concat) or
                nameof(Queryable.Union) or
                nameof(Queryable.Intersect) or
                nameof(Queryable.Except) or
                nameof(Queryable.SelectMany) or
                nameof(Queryable.Join) or
                nameof(Queryable.GroupJoin) or
                nameof(Queryable.GroupBy) or
                nameof(Queryable.Cast) or
                nameof(Queryable.OfType) or
                nameof(Queryable.DefaultIfEmpty) or
                nameof(Queryable.Append) or
                nameof(Queryable.Prepend);
        }

        private string? GetProjectionDiagnostic(
            Expression expression,
            ParameterExpression groupingParameter)
        {
            switch (expression)
            {
                case NewExpression projection:
                    if (projection.Arguments.Count == 0)
                        return UnsupportedProjection();

                    foreach (Expression argument in
                             projection.Arguments)
                    {
                        string? diagnostic =
                            GetProjectionDiagnostic(
                                argument,
                                groupingParameter);
                        if (diagnostic is not null)
                            return diagnostic;
                    }

                    return null;

                case MemberInitExpression memberInit:
                    if (memberInit.NewExpression.Arguments.Count ==
                            0 &&
                        memberInit.Bindings.Count == 0)
                    {
                        return UnsupportedProjection();
                    }

                    foreach (Expression argument in
                             memberInit.NewExpression.Arguments)
                    {
                        string? diagnostic =
                            GetProjectionDiagnostic(
                                argument,
                                groupingParameter);
                        if (diagnostic is not null)
                            return diagnostic;
                    }

                    foreach (MemberBinding binding in
                             memberInit.Bindings)
                    {
                        if (binding is not MemberAssignment
                            assignment)
                        {
                            return UnsupportedProjection();
                        }

                        string? diagnostic =
                            GetProjectionDiagnostic(
                                assignment.Expression,
                                groupingParameter);
                        if (diagnostic is not null)
                            return diagnostic;
                    }

                    return null;

                case MemberExpression member:
                    return IsDirectGroupingKeyMember(
                            member,
                            groupingParameter)
                        ? null
                        : UnsupportedProjection();

                case MethodCallExpression aggregate
                    when aggregate.Arguments.Count > 0 &&
                         IsAggregate(aggregate.Method) &&
                         GroupingExpressionSupport
                             .IsGroupingSequence(
                                 aggregate.Arguments[0]):
                    return ValidateAggregate(aggregate);

                default:
                    return UnsupportedProjection();
            }
        }

        private static string UnsupportedProjection() =>
            ForGroupedAggregate(
                "Grouped projections must contain only the direct key and bare supported aggregates; transformed key or aggregate expressions require an unsupported shape.");

        private static bool IsDirectGroupingKeyMember(
            MemberExpression member,
            ParameterExpression groupingParameter)
        {
            if (ReferenceEquals(
                    member.Expression,
                    groupingParameter) &&
                member.Member.Name == "Key")
            {
                return true;
            }

            if (member.Expression is not MemberExpression
                {
                    Member.Name: "Key",
                    Expression: ParameterExpression keyOwner,
                } ||
                !ReferenceEquals(
                    keyOwner,
                    groupingParameter))
            {
                return false;
            }

            Type keyType =
                groupingParameter.Type.GetGenericArguments()[0];
            keyType =
                Nullable.GetUnderlyingType(keyType) ??
                keyType;
            return IsQualifiedCompositeGroupingType(keyType);
        }

        private static bool IsQualifiedCompositeGroupingType(
            Type type) =>
            type.IsDefined(
                typeof(CompilerGeneratedAttribute),
                inherit: false) &&
            type.IsSealed &&
            type.IsGenericType &&
            type.Name.StartsWith(
                "<>f__AnonymousType",
                StringComparison.Ordinal) ||
            type.IsValueType &&
            type.IsGenericType &&
            type.FullName?.StartsWith(
                "System.ValueTuple`",
                StringComparison.Ordinal) == true;

        protected override Expression VisitConditional(
            ConditionalExpression node)
        {
            if (Diagnostic is null &&
                GroupingExpressionSupport.ContainsGroupingParameter(
                    node))
            {
                Diagnostic = ForGroupedAggregate(
                    "Conditional grouped expressions produce CASE SQL, which is outside the qualified aggregate surface.");
            }

            return Diagnostic is null
                ? base.VisitConditional(node)
                : node;
        }

        protected override Expression VisitUnary(
            UnaryExpression node)
        {
            if (Diagnostic is null &&
                node.NodeType is
                    ExpressionType.Convert or
                    ExpressionType.ConvertChecked &&
                GroupingExpressionSupport.ContainsGroupingParameter(
                    node.Operand))
            {
                Diagnostic = ForGroupedAggregate(
                    "Casts over grouped keys or aggregates are outside the qualified aggregate surface.");
            }

            return Diagnostic is null
                ? base.VisitUnary(node)
                : node;
        }

        private string? ValidateAggregate(
            MethodCallExpression aggregate)
        {
            string aggregateName = aggregate.Method.Name;
            Expression source = aggregate.Arguments[0];

            if (aggregateName is
                nameof(Queryable.Count) or
                nameof(Queryable.LongCount))
            {
                if (aggregate.Arguments.Count != 1)
                {
                    return ForGroupedAggregate(
                        $"Predicate {aggregateName} produces CASE SQL; filter before GroupBy or use a supported HAVING predicate over an unfiltered aggregate.");
                }

                if (GroupingExpressionSupport
                    .IsDirectGroupingParameter(source))
                {
                    return null;
                }

                if (!TryGetDirectDistinctGroupingSelector(
                        source,
                        out Type distinctType,
                        out Type distinctProviderType,
                        out bool distinctHasConverter))
                {
                    return ForGroupedAggregate(
                        $"{aggregateName} must apply directly to the group, or to Distinct over one directly mapped numeric column.");
                }

                if (distinctType != typeof(int))
                {
                    return ForGroupedAggregate(
                        $"Distinct {aggregateName} is qualified only for a single nonnullable int column.");
                }

                return ValidateAggregateStorageType(
                    aggregateName,
                    distinctType,
                    distinctProviderType,
                    distinctHasConverter);
            }

            if (aggregate.Arguments.Count == 2)
            {
                if (!GroupingExpressionSupport
                        .IsDirectGroupingParameter(source) ||
                    !GroupingExpressionSupport.TryGetDirectMemberSelector(
                        aggregate.Arguments[1],
                        _model,
                        out Type selectorType,
                        out Type selectorProviderType,
                        out bool selectorHasConverter))
                {
                    return ForGroupedAggregate(
                        $"{aggregateName} selectors must be one directly mapped numeric column; computed, filtered, conditional, and cast selectors are unsupported.");
                }

                return ValidateAggregateType(
                    aggregateName,
                    selectorType) ??
                    ValidateAggregateStorageType(
                        aggregateName,
                        selectorType,
                        selectorProviderType,
                        selectorHasConverter);
            }

            if (!TryGetDirectDistinctGroupingSelector(
                    source,
                    out Type directDistinctType,
                    out Type directDistinctProviderType,
                    out bool directDistinctHasConverter))
            {
                return ForGroupedAggregate(
                    $"{aggregateName} without a selector is qualified only for Distinct over one directly mapped numeric column.");
            }

            return ValidateDistinctAggregateType(
                    aggregateName,
                    directDistinctType) ??
                ValidateAggregateStorageType(
                    aggregateName,
                    directDistinctType,
                    directDistinctProviderType,
                    directDistinctHasConverter);
        }

        private static string? ValidateAggregateType(
            string aggregateName,
            Type valueType)
        {
            bool supported = aggregateName switch
            {
                nameof(Queryable.Sum) =>
                    valueType == typeof(int) ||
                    valueType == typeof(double) ||
                    valueType == typeof(double?),
                nameof(Queryable.Average) =>
                    valueType == typeof(double) ||
                    valueType == typeof(double?),
                nameof(Queryable.Min) or
                nameof(Queryable.Max) =>
                    valueType == typeof(int) ||
                    valueType == typeof(double) ||
                    valueType == typeof(double?),
                _ => false,
            };
            if (supported)
                return null;

            return ForGroupedAggregate(
                $"{aggregateName} over '{valueType.Name}' is outside the qualified numeric aggregate types.");
        }

        private static string? ValidateDistinctAggregateType(
            string aggregateName,
            Type valueType)
        {
            if (aggregateName == nameof(Queryable.Average))
            {
                return ForGroupedAggregate(
                    "Average over Distinct is outside the qualified surface because EF Core introduces a CAST that CSharpDB cannot execute; use Count, LongCount, Sum, Min, or Max over a directly mapped nonnullable int column.");
            }

            if (valueType == typeof(int) &&
                aggregateName is
                    nameof(Queryable.Sum) or
                    nameof(Queryable.Min) or
                    nameof(Queryable.Max))
            {
                return null;
            }

            return ForGroupedAggregate(
                $"{aggregateName} over Distinct is qualified only for a single nonnullable int column.");
        }

        private static string? ValidateAggregateStorageType(
            string aggregateName,
            Type valueType,
            Type providerType,
            bool hasConverter)
        {
            if (hasConverter)
            {
                return ForGroupedAggregate(
                    $"{aggregateName} column type '{valueType.Name}' uses a value converter, which is outside the qualified direct numeric storage mappings because aggregate algebra may change.");
            }

            Type unwrappedValueType =
                Nullable.GetUnderlyingType(valueType) ??
                valueType;
            Type unwrappedProviderType =
                Nullable.GetUnderlyingType(providerType) ??
                providerType;
            if (unwrappedProviderType == unwrappedValueType)
                return null;

            return ForGroupedAggregate(
                $"{aggregateName} column type '{valueType.Name}' maps to provider type '{providerType.Name}', which is outside the qualified direct numeric storage mappings.");
        }

        private bool TryGetDirectDistinctGroupingSelector(
            Expression source,
            out Type selectorType,
            out Type providerType,
            out bool hasConverter)
        {
            selectorType = null!;
            providerType = null!;
            hasConverter = false;
            if (source is not MethodCallExpression distinct ||
                distinct.Method.Name != nameof(Queryable.Distinct) ||
                distinct.Method.DeclaringType is not null &&
                distinct.Method.DeclaringType != typeof(Queryable) &&
                distinct.Method.DeclaringType != typeof(Enumerable) ||
                distinct.Arguments.Count != 1 ||
                distinct.Arguments[0] is not MethodCallExpression select ||
                select.Method.Name != nameof(Queryable.Select) ||
                select.Method.DeclaringType is not null &&
                select.Method.DeclaringType != typeof(Queryable) &&
                select.Method.DeclaringType != typeof(Enumerable) ||
                select.Arguments.Count != 2 ||
                !GroupingExpressionSupport.IsDirectGroupingParameter(
                    select.Arguments[0]))
            {
                return false;
            }

            return GroupingExpressionSupport.TryGetDirectMemberSelector(
                select.Arguments[1],
                _model,
                out selectorType,
                out providerType,
                out hasConverter);
        }

        private static bool IsAggregate(MethodInfo method) =>
            method.DeclaringType is not null &&
            (method.DeclaringType == typeof(Queryable) ||
             method.DeclaringType == typeof(Enumerable)) &&
            method.Name is
                nameof(Queryable.Count) or
                nameof(Queryable.LongCount) or
                nameof(Queryable.Sum) or
                nameof(Queryable.Average) or
                nameof(Queryable.Min) or
                nameof(Queryable.Max);

        private static bool IsSelect(MethodInfo method) =>
            method.DeclaringType is not null &&
            (method.DeclaringType == typeof(Queryable) ||
             method.DeclaringType == typeof(Enumerable)) &&
            method.Name == nameof(Queryable.Select);

        private static bool
            IsUnsupportedGroupedSequenceOperator(
                MethodInfo method) =>
            method.DeclaringType is not null &&
            (method.DeclaringType == typeof(Queryable) ||
             method.DeclaringType == typeof(Enumerable)) &&
            method.Name is
                nameof(Queryable.Any) or
                nameof(Queryable.All) or
                nameof(Queryable.First) or
                nameof(Queryable.FirstOrDefault) or
                nameof(Queryable.Single) or
                nameof(Queryable.SingleOrDefault) or
                nameof(Queryable.Last) or
                nameof(Queryable.LastOrDefault) or
                nameof(Queryable.ElementAt) or
                nameof(Queryable.ElementAtOrDefault) or
                nameof(Queryable.Contains) or
                nameof(Enumerable.ToList) or
                nameof(Enumerable.ToArray);
    }

    private static class GroupingExpressionSupport
    {
        public static bool ContainsGroupingParameter(
            Expression expression)
        {
            var visitor =
                new GroupingParameterFindingExpressionVisitor();
            visitor.Visit(expression);
            return visitor.Found;
        }

        public static bool IsDirectGroupingParameter(
            Expression expression) =>
            expression is ParameterExpression parameter &&
            IsGroupingType(parameter.Type);

        public static bool IsGroupingSequence(
            Expression expression)
        {
            if (IsDirectGroupingParameter(expression))
                return true;

            return expression is MethodCallExpression
                {
                    Arguments.Count: > 0,
                } methodCall &&
                IsGroupingSequence(methodCall.Arguments[0]);
        }

        public static bool ContainsGroupByQuery(
            Expression expression)
        {
            var visitor =
                new GroupByQueryFindingExpressionVisitor();
            visitor.Visit(expression);
            return visitor.Found;
        }

        public static bool TryGetDirectMemberSelector(
            Expression expression,
            IModel model,
            out Type selectorType,
            out Type providerType,
            out bool hasConverter)
        {
            selectorType = null!;
            providerType = null!;
            hasConverter = false;
            while (expression is UnaryExpression
                   {
                       NodeType: ExpressionType.Quote,
                   } quote)
            {
                expression = quote.Operand;
            }

            if (expression is not LambdaExpression
                {
                    Parameters.Count: 1,
                    Body: MemberExpression member,
                } selector ||
                !ReferenceEquals(
                    member.Expression,
                    selector.Parameters[0]))
            {
                return false;
            }

            IProperty? property = model
                .FindEntityType(selector.Parameters[0].Type)?
                .FindProperty(member.Member.Name);
            if (property is null)
                return false;

            selectorType = selector.ReturnType;
            hasConverter =
                property.GetTypeMapping().Converter is not null;
            providerType =
                property.GetTypeMapping().Converter?.ProviderClrType ??
                property.GetProviderClrType() ??
                selectorType;
            return true;
        }

        public static bool TryGetLambda(
            Expression expression,
            out LambdaExpression lambda)
        {
            while (expression is UnaryExpression
                   {
                       NodeType: ExpressionType.Quote,
                   } quote)
            {
                expression = quote.Operand;
            }

            lambda = expression as LambdaExpression ?? null!;
            return lambda is not null;
        }

        public static bool IsGroupingType(Type type) =>
            type.IsGenericType &&
            type.GetGenericTypeDefinition() == typeof(IGrouping<,>);

        private sealed class
            GroupByQueryFindingExpressionVisitor
            : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitMethodCall(
                MethodCallExpression node)
            {
                if (node.Method.DeclaringType ==
                        typeof(Queryable) &&
                    node.Method.Name ==
                        nameof(Queryable.GroupBy))
                {
                    Found = true;
                    return node;
                }

                return Found
                    ? node
                    : base.VisitMethodCall(node);
            }
        }

        private sealed class
            GroupingParameterFindingExpressionVisitor
            : ExpressionVisitor
        {
            public bool Found { get; private set; }

            protected override Expression VisitParameter(
                ParameterExpression node)
            {
                if (IsGroupingType(node.Type))
                    Found = true;

                return node;
            }

            public override Expression? Visit(Expression? node) =>
                Found ? node : base.Visit(node);
        }
    }
}
