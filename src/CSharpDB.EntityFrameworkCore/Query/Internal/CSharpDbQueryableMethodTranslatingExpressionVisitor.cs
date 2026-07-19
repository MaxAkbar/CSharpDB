using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQueryableMethodTranslatingExpressionVisitorFactory
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    private readonly QueryableMethodTranslatingExpressionVisitorDependencies _dependencies;
    private readonly RelationalQueryableMethodTranslatingExpressionVisitorDependencies _relationalDependencies;
    private readonly IModel _designTimeModel;

    public CSharpDbQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        IDesignTimeModel designTimeModel)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
        _designTimeModel = designTimeModel.Model;
    }

    public QueryableMethodTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext)
        => new CSharpDbQueryableMethodTranslatingExpressionVisitor(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext,
            _designTimeModel);
}

public sealed class CSharpDbQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly IModel _model;

    public CSharpDbQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        IModel designTimeModel)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _model = designTimeModel;
    }

    private CSharpDbQueryableMethodTranslatingExpressionVisitor(
        CSharpDbQueryableMethodTranslatingExpressionVisitor parent)
        : base(parent)
    {
        _model = parent._model;
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

    protected override ShapedQueryExpression? TranslateAverage(
        ShapedQueryExpression source,
        LambdaExpression? selector,
        Type resultType)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.Average));
        }

        if (IsDecimalType(resultType))
        {
            return UnsupportedDecimalAggregate(
                nameof(Queryable.Average));
        }

        if (!IsDistinct(source))
            return base.TranslateAverage(source, selector, resultType);

        return UnsupportedDistinctAggregate(
            nameof(Queryable.Average),
            "Average over Distinct is outside the qualified surface because EF Core introduces a CAST that CSharpDB cannot execute; use Count, LongCount, Sum, Min, or Max over a directly mapped nonnullable int column.");
    }

    protected override ShapedQueryExpression? TranslateCount(
        ShapedQueryExpression source,
        LambdaExpression? predicate)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.Count));
        }

        if (!IsDistinct(source))
            return base.TranslateCount(source, predicate);

        if (predicate is not null)
        {
            return UnsupportedDistinctAggregate(
                nameof(Queryable.Count),
                "A predicate after Distinct is not supported; apply Where before Select and Distinct.");
        }

        return TranslateSimpleDistinctAggregate(
            source,
            nameof(Queryable.Count),
            type => QueryableMethods.CountWithoutPredicate.MakeGenericMethod(type),
            typeof(int),
            allowNullableColumn: false,
            allowedColumnTypes: [typeof(int)]);
    }

    protected override ShapedQueryExpression? TranslateLongCount(
        ShapedQueryExpression source,
        LambdaExpression? predicate)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.LongCount));
        }

        if (!IsDistinct(source))
            return base.TranslateLongCount(source, predicate);

        if (predicate is not null)
        {
            return UnsupportedDistinctAggregate(
                nameof(Queryable.LongCount),
                "A predicate after Distinct is not supported; apply Where before Select and Distinct.");
        }

        return TranslateSimpleDistinctAggregate(
            source,
            nameof(Queryable.LongCount),
            type => QueryableMethods.LongCountWithoutPredicate.MakeGenericMethod(type),
            typeof(long),
            allowNullableColumn: false,
            allowedColumnTypes: [typeof(int)]);
    }

    protected override ShapedQueryExpression? TranslateMax(
        ShapedQueryExpression source,
        LambdaExpression? selector,
        Type resultType)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.Max));
        }

        if (IsDecimalType(resultType))
        {
            return UnsupportedDecimalAggregate(
                nameof(Queryable.Max));
        }

        if (!IsDistinct(source))
            return base.TranslateMax(source, selector, resultType);

        if (selector is not null)
        {
            return UnsupportedDistinctAggregate(
                nameof(Queryable.Max),
                "Only Max over a simple distinct nonnullable int column is supported.");
        }

        return TranslateSimpleDistinctAggregate(
            source,
            nameof(Queryable.Max),
            type => QueryableMethods.MaxWithoutSelector.MakeGenericMethod(type),
            resultType,
            allowNullableColumn: false,
            allowedColumnTypes: [typeof(int)]);
    }

    protected override ShapedQueryExpression? TranslateMin(
        ShapedQueryExpression source,
        LambdaExpression? selector,
        Type resultType)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.Min));
        }

        if (IsDecimalType(resultType))
        {
            return UnsupportedDecimalAggregate(
                nameof(Queryable.Min));
        }

        if (!IsDistinct(source))
            return base.TranslateMin(source, selector, resultType);

        if (selector is not null)
        {
            return UnsupportedDistinctAggregate(
                nameof(Queryable.Min),
                "Only Min over a simple distinct nonnullable int column is supported.");
        }

        return TranslateSimpleDistinctAggregate(
            source,
            nameof(Queryable.Min),
            type => QueryableMethods.MinWithoutSelector.MakeGenericMethod(type),
            resultType,
            allowNullableColumn: false,
            allowedColumnTypes: [typeof(int)]);
    }

    protected override ShapedQueryExpression? TranslateSum(
        ShapedQueryExpression source,
        LambdaExpression? selector,
        Type resultType)
    {
        if (IsGrouped(source))
        {
            return UnsupportedGroupedResultAggregate(
                nameof(Queryable.Sum));
        }

        if (IsDecimalType(resultType))
        {
            return UnsupportedDecimalAggregate(
                nameof(Queryable.Sum));
        }

        if (!IsDistinct(source))
            return base.TranslateSum(source, selector, resultType);

        if (selector is not null)
        {
            return UnsupportedDistinctAggregate(
                nameof(Queryable.Sum),
                "Only Sum over a simple distinct nonnullable int column is supported.");
        }

        return TranslateSimpleDistinctAggregate(
            source,
            nameof(Queryable.Sum),
            QueryableMethods.GetSumWithoutSelector,
            resultType,
            allowNullableColumn: false,
            allowedColumnTypes: [typeof(int)]);
    }

    protected override ShapedQueryExpression? TranslateGroupBy(
        ShapedQueryExpression source,
        LambdaExpression keySelector,
        LambdaExpression? elementSelector,
        LambdaExpression? resultSelector)
    {
        if (!TryValidateGroupingSource(
                source,
                elementSelector,
                resultSelector,
                out string reason) ||
            !TryValidateGroupingKey(
                keySelector.Body,
                keySelector.Parameters[0],
                out reason))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForGroupedAggregate(reason));
            return null;
        }

        return base.TranslateGroupBy(
            source,
            keySelector,
            elementSelector,
            resultSelector);
    }

    protected override ShapedQueryExpression? TranslateJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
    {
        if (TryValidateRequiredNavigationJoinKeys(
                outerKeySelector,
                innerKeySelector,
                outer,
                inner,
                out _,
                out _,
                out _))
        {
            return base.TranslateJoin(
                outer,
                inner,
                outerKeySelector,
                innerKeySelector,
                resultSelector);
        }

        if (!TryValidateInnerJoinSource(
                outer,
                allowPredicate: true,
                "outer",
                out string reason) ||
            !TryValidateInnerJoinSource(
                inner,
                allowPredicate: false,
                "inner",
                out reason))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForInnerJoin(reason));
            return null;
        }

        if (!TryValidateInnerJoinKey(
                outerKeySelector,
                "outer",
                out InnerJoinKeyDescriptor outerKey,
                out reason) ||
            !TryValidateInnerJoinKey(
                innerKeySelector,
                "inner",
                out InnerJoinKeyDescriptor innerKey,
                out reason))
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForInnerJoin(reason));
            return null;
        }

        if (outerKey.ClrType !=
                innerKey.ClrType ||
            outerKey.ProviderClrType !=
            innerKey.ProviderClrType)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForInnerJoin(
                        $"The key mappings use incompatible CLR/provider types '{outerKey.ClrType.Name}/{outerKey.ProviderClrType.Name}' and '{innerKey.ClrType.Name}/{innerKey.ProviderClrType.Name}'. Both sides must use the same direct INTEGER-backed mapping."));
            return null;
        }

        ShapedQueryExpression? translation =
            base.TranslateJoin(
                outer,
                inner,
                outerKeySelector,
                innerKeySelector,
                resultSelector);
        if (translation is null)
        {
            AddTranslationErrorDetails(
                CSharpDbQueryTranslationDiagnostics
                    .ForInnerJoin(
                        "The direct equality predicate or result projection could not be translated by the qualified scalar translation surface."));
        }

        return translation;
    }

    private ShapedQueryExpression? TranslateSimpleDistinctAggregate(
        ShapedQueryExpression source,
        string aggregateName,
        Func<Type, MethodInfo> methodGenerator,
        Type resultType,
        bool allowNullableColumn,
        Type[] allowedColumnTypes)
    {
        if (!TryGetSimpleDistinctColumn(
                source,
                out SelectExpression selectExpression,
                out ColumnExpression column,
                out string reason))
        {
            return UnsupportedDistinctAggregate(aggregateName, reason);
        }

        Type columnType =
            Nullable.GetUnderlyingType(column.Type) ?? column.Type;
        if (column.TypeMapping?.Converter is not null)
        {
            return UnsupportedDistinctAggregate(
                aggregateName,
                $"The distinct column type '{column.Type.Name}' uses a value converter, which is outside the qualified direct numeric storage mappings because aggregate algebra may change.");
        }

        Type? providerType =
            column.TypeMapping?.ClrType;
        providerType = providerType is null
            ? null
            : Nullable.GetUnderlyingType(providerType) ??
              providerType;
        if (providerType != columnType)
        {
            return UnsupportedDistinctAggregate(
                aggregateName,
                $"The distinct column type '{column.Type.Name}' maps to provider type '{providerType?.Name ?? "<unknown>"}', which is outside the qualified direct numeric storage mappings.");
        }

        if (!allowedColumnTypes.Contains(columnType))
        {
            return UnsupportedDistinctAggregate(
                aggregateName,
                $"The distinct column type '{column.Type.Name}' is outside the qualified numeric surface; only a directly mapped nonnullable int column is supported.");
        }

        if (!allowNullableColumn && column.IsNullable)
        {
            return UnsupportedDistinctAggregate(
                aggregateName,
                "Only a nonnullable distinct int column is supported. Nullable distinct Count and LongCount additionally count NULL once in LINQ, while SQL COUNT(DISTINCT ...) ignores NULL.");
        }

        var distinctSource =
            new EnumerableExpression(column).SetDistinct(true);
        MethodCallExpression methodCall = Expression.Call(
            methodGenerator(column.Type),
            Expression.Call(
                QueryableMethods.AsQueryable.MakeGenericMethod(column.Type),
                distinctSource));
        SqlExpression? translation = TranslateExpression(methodCall);
        if (translation is null)
        {
            return UnsupportedDistinctAggregate(
                aggregateName,
                "The aggregate could not be translated for the selected column.");
        }

        selectExpression.IsDistinct = false;
        selectExpression.ClearOrdering();
        selectExpression.ReplaceProjection(
            new Dictionary<ProjectionMember, Expression>
            {
                [new ProjectionMember()] = translation,
            });

        Expression shaper = new ProjectionBindingExpression(
            source.QueryExpression,
            new ProjectionMember(),
            MakeNullable(translation.Type));
        if (resultType != shaper.Type)
            shaper = Expression.Convert(shaper, resultType);

        return source.UpdateShaperExpression(shaper);
    }

    private ShapedQueryExpression? UnsupportedDistinctAggregate(
        string aggregateName,
        string reason)
    {
        AddTranslationErrorDetails(
            CSharpDbQueryTranslationDiagnostics.ForDistinctAggregate(
                aggregateName,
                reason));
        return null;
    }

    private ShapedQueryExpression? UnsupportedGroupedResultAggregate(
        string aggregateName)
    {
        AddTranslationErrorDetails(
            CSharpDbQueryTranslationDiagnostics.ForGroupedAggregate(
                $"Applying {aggregateName} to already-grouped result rows requires an unsupported derived-table shape. Project keys and aggregates from each group directly."));
        return null;
    }

    private ShapedQueryExpression? UnsupportedDecimalAggregate(
        string aggregateName)
    {
        AddTranslationErrorDetails(
            CSharpDbQueryTranslationDiagnostics.ForDecimal(
                $"Decimal aggregate '{aggregateName}' is deferred until scaled accumulation, overflow, and result-scale semantics are qualified."));
        return null;
    }

    private static bool TryGetSimpleDistinctColumn(
        ShapedQueryExpression source,
        out SelectExpression selectExpression,
        out ColumnExpression column,
        out string reason)
    {
        selectExpression =
            source.QueryExpression as SelectExpression ?? null!;
        column = null!;

        if (selectExpression is null ||
            !selectExpression.IsDistinct)
        {
            reason = "The query source is not a distinct relational projection.";
            return false;
        }

        if (selectExpression.Tables.Count != 1 ||
            selectExpression.Tables[0] is not TableExpression table ||
            selectExpression.GroupBy.Count != 0 ||
            selectExpression.Having is not null ||
            selectExpression.Limit is not null ||
            selectExpression.Offset is not null ||
            selectExpression.Orderings.Count != 0)
        {
            reason =
                "Only Where -> Select(single column) -> Distinct -> aggregate is supported; joins, grouping, ordering, row limits, set operations, and derived sources are not.";
            return false;
        }

        Expression shaper = source.ShaperExpression;
        if (shaper is UnaryExpression
            {
                NodeType: ExpressionType.Convert,
            } conversion)
        {
            shaper = conversion.Operand;
        }

        if (shaper is not ProjectionBindingExpression projectionBinding ||
            !ReferenceEquals(
                projectionBinding.QueryExpression,
                selectExpression) ||
            selectExpression.GetProjection(projectionBinding)
                is not ColumnExpression projectedColumn ||
            !string.Equals(
                projectedColumn.TableAlias,
                table.Alias,
                StringComparison.Ordinal))
        {
            reason =
                "The distinct selector must be one directly mapped scalar column.";
            return false;
        }

        column = projectedColumn;
        reason = string.Empty;
        return true;
    }

    private static bool IsDistinct(ShapedQueryExpression source) =>
        source.QueryExpression is SelectExpression { IsDistinct: true };

    private static bool IsGrouped(ShapedQueryExpression source) =>
        source.QueryExpression is SelectExpression
        {
            GroupBy.Count: > 0,
        };

    private static Type MakeNullable(Type type) =>
        type.IsValueType &&
        Nullable.GetUnderlyingType(type) is null
            ? typeof(Nullable<>).MakeGenericType(type)
            : type;

    private static bool IsDecimalType(Type type) =>
        (Nullable.GetUnderlyingType(type) ?? type) ==
        typeof(decimal);

    private bool TryValidateGroupingKey(
        Expression expression,
        ParameterExpression parameter,
        out string reason)
    {
        if (_model.FindEntityType(parameter.Type) is null)
        {
            reason =
                "GroupBy must operate directly on a mapped entity; grouping a prior projection is outside the qualified single-table shape.";
            return false;
        }

        switch (expression)
        {
            case NewExpression { Arguments.Count: > 0 } composite:
                if (!IsQualifiedCompositeGroupingType(
                        composite.Type))
                {
                    reason =
                        "Composite GroupBy keys are qualified only for anonymous types and ValueTuple shapes with structural equality.";
                    return false;
                }

                foreach (Expression argument in composite.Arguments)
                {
                    if (!TryValidateGroupingKey(
                            argument,
                            parameter,
                            out reason))
                    {
                        return false;
                    }
                }

                reason = string.Empty;
                return true;

            case MemberInitExpression:
                reason =
                    "Composite GroupBy keys are qualified only for anonymous types and ValueTuple shapes with structural equality.";
                return false;

            case MemberExpression member
                when ReferenceEquals(
                    StripGroupingKeyConversion(member.Expression),
                    parameter):
                return ValidateGroupingProperty(
                    parameter.Type,
                    member,
                    out reason);

            default:
                reason =
                    "GroupBy keys must be direct mapped scalar columns or composites of direct mapped scalar columns; transformed and expression keys require derived-table support.";
                return false;
        }
    }

    private bool ValidateGroupingProperty(
        Type entityClrType,
        MemberExpression member,
        out string reason)
    {
        IEntityType? entityType = _model.FindEntityType(entityClrType);
        IProperty? property =
            entityType?.FindProperty(member.Member.Name);
        if (property is null)
        {
            reason =
                $"GroupBy member '{member.Member.Name}' is not a directly mapped property of '{entityClrType.Name}'.";
            return false;
        }

        if (property.GetValueConverter() is not null)
        {
            reason =
                $"GroupBy property '{member.Member.Name}' uses a configured value converter, which is outside the qualified key surface because key equality may change.";
            return false;
        }

        Type keyType =
            Nullable.GetUnderlyingType(member.Type) ?? member.Type;
        if (!IsQualifiedGroupingType(keyType))
        {
            reason =
                $"GroupBy key type '{member.Type.Name}' is outside the qualified direct-column surface.";
            return false;
        }

        Type providerClrType =
            property.GetTypeMapping().Converter?.ProviderClrType ??
            property.GetProviderClrType() ??
            keyType;
        providerClrType =
            Nullable.GetUnderlyingType(providerClrType) ??
            providerClrType;
        if (!IsQualifiedGroupingType(providerClrType))
        {
            reason =
                $"GroupBy property '{member.Member.Name}' maps to provider type '{providerClrType.Name}', which is outside the qualified grouping storage types.";
            return false;
        }

        if (providerClrType == typeof(string))
        {
            string? collation =
                property.GetCollation() ??
                _model.GetCollation();
            if (collation is not null &&
                !string.Equals(
                    collation,
                    "BINARY",
                    StringComparison.OrdinalIgnoreCase))
            {
                reason =
                    $"GroupBy over text collation '{collation}' is not qualified because grouped hashing currently uses binary equality.";
                return false;
            }
        }

        reason = string.Empty;
        return true;
    }

    private static bool IsQualifiedGroupingType(Type type) =>
        type.IsEnum ||
        type == typeof(bool) ||
        type == typeof(byte) ||
        type == typeof(sbyte) ||
        type == typeof(short) ||
        type == typeof(ushort) ||
        type == typeof(int) ||
        type == typeof(uint) ||
        type == typeof(long) ||
        type == typeof(ulong) ||
        type == typeof(string);

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

    private static bool TryValidateInnerJoinSource(
        ShapedQueryExpression source,
        bool allowPredicate,
        string sourceName,
        out string reason)
    {
        if (source.QueryExpression is not SelectExpression
            {
                Tables.Count: 1,
            } selectExpression ||
            selectExpression.Tables[0] is not TableExpression ||
            source.ShaperExpression is not
                RelationalStructuralTypeShaperExpression
                {
                    StructuralType: IEntityType,
                } ||
            selectExpression.IsDistinct ||
            selectExpression.GroupBy.Count != 0 ||
            selectExpression.Having is not null ||
            selectExpression.Limit is not null ||
            selectExpression.Offset is not null ||
            selectExpression.Orderings.Count != 0)
        {
            reason =
                $"The {sourceName} source must be one direct mapped entity table without projection, ordering, row limits, distinct, grouping, another join, or a derived source.";
            return false;
        }

        if (!allowPredicate &&
            selectExpression.Predicate is not null)
        {
            reason =
                "The inner source cannot be pre-filtered because EF Core represents that shape as a derived JOIN target, which is outside CSharpDB's qualified SQL grammar. Move the predicate after Join.";
            return false;
        }

        reason = string.Empty;
        return true;
    }

    private bool TryValidateInnerJoinKey(
        LambdaExpression keySelector,
        string selectorName,
        out InnerJoinKeyDescriptor descriptor,
        out string reason)
    {
        descriptor = default;
        if (keySelector.Parameters.Count != 1 ||
            _model.FindEntityType(
                keySelector.Parameters[0].Type) is null)
        {
            reason =
                $"The {selectorName} key selector must operate directly on a mapped entity.";
            return false;
        }

        Expression expression =
            StripInnerJoinKeyBoxing(
                keySelector.Body);
        if (expression is NewExpression or
            MemberInitExpression)
        {
            reason =
                "Composite inner-join keys are not yet qualified; use one direct nonnullable mapped scalar property.";
            return false;
        }

        if (expression is not MemberExpression member ||
            !ReferenceEquals(
                member.Expression,
                keySelector.Parameters[0]))
        {
            reason =
                $"The {selectorName} key must be one direct mapped scalar property; transformed keys require a broader join translation surface.";
            return false;
        }

        IEntityType entityType =
            _model.FindEntityType(
                keySelector.Parameters[0].Type)!;
        IProperty? property =
            entityType.FindProperty(
                member.Member.Name);
        if (property is null)
        {
            reason =
                $"The {selectorName} key member '{member.Member.Name}' is not a directly mapped property of '{entityType.ClrType.Name}'.";
            return false;
        }

        return TryValidateInnerJoinProperty(
            property,
            member.Type,
            selectorName,
            out descriptor,
            out reason);
    }

    private static bool TryValidateInnerJoinProperty(
        IProperty property,
        Type expressionType,
        string selectorName,
        out InnerJoinKeyDescriptor descriptor,
        out string reason)
    {
        descriptor = default;
        if (property.IsNullable)
        {
            reason =
                $"The {selectorName} key property '{property.Name}' is nullable. Nullable-key equality is deferred until LINQ and SQL null semantics are explicitly qualified.";
            return false;
        }

        Type keyType =
            Nullable.GetUnderlyingType(
                expressionType) ??
            expressionType;
        CoreTypeMapping typeMapping =
            property.GetTypeMapping();
        if (typeMapping is not
                RelationalTypeMapping relationalTypeMapping ||
            !string.Equals(
                relationalTypeMapping.StoreType,
                "INTEGER",
                StringComparison.OrdinalIgnoreCase))
        {
            reason =
                $"The {selectorName} key property '{property.Name}' does not use the required direct INTEGER store mapping.";
            return false;
        }

        ValueConverter? mappingConverter =
            typeMapping.Converter;
        if (property.GetValueConverter() is not null ||
            mappingConverter is not null &&
            !IsDefaultEnumNumberConverter(
                keyType,
                mappingConverter))
        {
            reason =
                $"The {selectorName} key property '{property.Name}' uses a configured value converter, which is outside the qualified equality surface.";
            return false;
        }

        Type providerClrType =
            mappingConverter?.ProviderClrType ??
            property.GetProviderClrType() ??
            keyType;
        providerClrType =
            Nullable.GetUnderlyingType(
                providerClrType) ??
            providerClrType;
        if (!IsQualifiedInnerJoinKeyType(keyType) ||
            !IsIntegralProviderType(providerClrType))
        {
            reason =
                $"The {selectorName} key property '{property.Name}' maps '{keyType.Name}' to '{providerClrType.Name}', outside the qualified direct INTEGER-backed int, long, and enum join keys.";
            return false;
        }

        descriptor = new InnerJoinKeyDescriptor(
            keyType,
            providerClrType);
        reason = string.Empty;
        return true;
    }

    private bool TryValidateRequiredNavigationJoinKeys(
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        out InnerJoinKeyDescriptor outerDescriptor,
        out InnerJoinKeyDescriptor innerDescriptor,
        out string reason)
    {
        outerDescriptor = default;
        innerDescriptor = default;
        reason = string.Empty;

        if (outer.ShaperExpression is not
                RelationalStructuralTypeShaperExpression
                {
                    StructuralType: IEntityType outerEntityType,
                } ||
            inner.ShaperExpression is not
                RelationalStructuralTypeShaperExpression
                {
                    StructuralType: IEntityType innerEntityType,
                } ||
            !TryGetNavigationExpansionProperty(
                outerKeySelector,
                outerEntityType,
                out IProperty outerProperty) ||
            !TryGetNavigationExpansionProperty(
                innerKeySelector,
                innerEntityType,
                out IProperty innerProperty) ||
            !IsSinglePropertyForeignKeyPair(
                outerEntityType,
                outerProperty,
                innerEntityType,
                innerProperty))
        {
            return false;
        }

        return TryValidateInnerJoinProperty(
                outerProperty,
                outerProperty.ClrType,
                "outer navigation",
                out outerDescriptor,
                out reason) &&
            TryValidateInnerJoinProperty(
                innerProperty,
                innerProperty.ClrType,
                "inner navigation",
                out innerDescriptor,
                out reason);
    }

    private static bool TryGetNavigationExpansionProperty(
        LambdaExpression keySelector,
        IEntityType sourceEntityType,
        out IProperty property)
    {
        // Navigation expansion emits an unboxed EF.Property<T?> call.
        // Keep this exact shape so user-authored, object-boxed Join selectors
        // still pass through the bounded public-Join validation.
        property = null!;
        if (keySelector.Parameters.Count != 1 ||
            sourceEntityType.ClrType !=
            keySelector.Parameters[0].Type ||
            keySelector.Body is not MethodCallExpression
            {
                Method:
                {
                    IsGenericMethod: true,
                    Name: nameof(EF.Property),
                    DeclaringType: { } declaringType,
                },
                Arguments:
                [
                    Expression entityExpression,
                    ConstantExpression
                    {
                        Value: string propertyName,
                    },
                ],
            } propertyCall ||
            declaringType != typeof(EF) ||
            Nullable.GetUnderlyingType(propertyCall.Type) is not
                Type selectorType ||
            !ReferenceEquals(
                StripInnerJoinKeyBoxing(entityExpression),
                keySelector.Parameters[0]))
        {
            return false;
        }

        IProperty? resolvedProperty =
            sourceEntityType.FindProperty(propertyName);
        if (resolvedProperty is null ||
            (Nullable.GetUnderlyingType(
                resolvedProperty.ClrType) ??
             resolvedProperty.ClrType) != selectorType)
        {
            return false;
        }

        property = resolvedProperty;
        return true;
    }

    private static bool IsSinglePropertyForeignKeyPair(
        IEntityType outerEntityType,
        IProperty outerProperty,
        IEntityType innerEntityType,
        IProperty innerProperty) =>
        IsSinglePropertyForeignKeyPairInDirection(
            outerEntityType,
            outerProperty,
            innerEntityType,
            innerProperty) ||
        IsSinglePropertyForeignKeyPairInDirection(
            innerEntityType,
            innerProperty,
            outerEntityType,
            outerProperty);

    private static bool IsSinglePropertyForeignKeyPairInDirection(
        IEntityType dependentEntityType,
        IProperty dependentProperty,
        IEntityType principalEntityType,
        IProperty principalProperty) =>
        dependentEntityType.GetForeignKeys().Any(
            foreignKey =>
                foreignKey.Properties.Count == 1 &&
                foreignKey.PrincipalKey.Properties.Count == 1 &&
                ReferenceEquals(
                    foreignKey.PrincipalEntityType,
                    principalEntityType) &&
                ReferenceEquals(
                    foreignKey.Properties[0],
                    dependentProperty) &&
                ReferenceEquals(
                    foreignKey.PrincipalKey.Properties[0],
                    principalProperty));

    private static bool IsQualifiedInnerJoinKeyType(
        Type type)
    {
        if (type == typeof(int) ||
            type == typeof(long))
        {
            return true;
        }

        if (!type.IsEnum)
            return false;

        Type underlying =
            Enum.GetUnderlyingType(type);
        return underlying == typeof(int) ||
            underlying == typeof(long);
    }

    private static bool IsDefaultEnumNumberConverter(
        Type keyType,
        ValueConverter converter) =>
        keyType.IsEnum &&
        converter.GetType().IsGenericType &&
        converter.GetType()
            .GetGenericTypeDefinition() ==
        typeof(EnumToNumberConverter<,>);

    private static bool IsIntegralProviderType(
        Type type) =>
        type == typeof(byte) ||
        type == typeof(sbyte) ||
        type == typeof(short) ||
        type == typeof(ushort) ||
        type == typeof(int) ||
        type == typeof(uint) ||
        type == typeof(long) ||
        type == typeof(ulong);

    private readonly record struct InnerJoinKeyDescriptor(
        Type ClrType,
        Type ProviderClrType);

    private static Expression StripInnerJoinKeyBoxing(
        Expression expression) =>
        expression is UnaryExpression
        {
            NodeType:
                ExpressionType.Convert or
                ExpressionType.ConvertChecked,
            Type: var targetType,
        } conversion &&
        targetType == typeof(object)
            ? conversion.Operand
            : expression;

    private static bool TryValidateGroupingSource(
        ShapedQueryExpression source,
        LambdaExpression? elementSelector,
        LambdaExpression? resultSelector,
        out string reason)
    {
        if (elementSelector is not null ||
            resultSelector is not null)
        {
            reason =
                "GroupBy element-selector and result-selector overloads are outside the qualified direct grouping shape.";
            return false;
        }

        if (source.QueryExpression is not SelectExpression
            {
                Tables.Count: 1,
            } selectExpression ||
            selectExpression.Tables[0] is not TableExpression ||
            selectExpression.IsDistinct ||
            selectExpression.GroupBy.Count != 0 ||
            selectExpression.Having is not null ||
            selectExpression.Limit is not null ||
            selectExpression.Offset is not null ||
            selectExpression.Orderings.Count != 0)
        {
            reason =
                "GroupBy is qualified only for a single mapped table with an optional pre-filter; joins, prior projections, ordering, row limits, distinct/set operations, and derived sources are not supported.";
            return false;
        }

        reason = string.Empty;
        return true;
    }

    private static Expression? StripGroupingKeyConversion(
        Expression? expression)
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
}
