using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Engine;

internal sealed class CollectionIndexBinding<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    T>
{
    private readonly Func<T, object?> _fieldAccessor;
    private readonly string _jsonPropertyName;
    private readonly CollectionIndexValueKind _valueKind;

    private enum CollectionIndexValueKind
    {
        Integer,
        Text,
    }

    private CollectionIndexBinding(
        string fieldPath,
        string indexName,
        IIndexStore indexStore,
        Func<T, object?> fieldAccessor,
        string jsonPropertyName,
        CollectionIndexValueKind valueKind)
    {
        FieldPath = fieldPath;
        IndexName = indexName;
        IndexStore = indexStore;
        _fieldAccessor = fieldAccessor;
        _jsonPropertyName = jsonPropertyName;
        _valueKind = valueKind;
    }

    internal string FieldPath { get; }

    internal string IndexName { get; }

    internal IIndexStore IndexStore { get; }

    internal string JsonPropertyName => _jsonPropertyName;

    internal bool UsesIntegerKey => _valueKind == CollectionIndexValueKind.Integer;

    internal bool UsesTextKey => _valueKind == CollectionIndexValueKind.Text;

    internal bool MatchesValue<TField>(T document, TField value, EqualityComparer<TField> comparer)
    {
        object? fieldValue = _fieldAccessor(document);
        if (fieldValue is TField typed)
            return comparer.Equals(typed, value);

        if (fieldValue is null)
            return value is null;

        return false;
    }
    
    internal static string GetFieldPath<TField>(Expression<Func<T, TField>> fieldSelector)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);

        Expression body = StripConvert(fieldSelector.Body);
        if (body is not MemberExpression memberExpression ||
            memberExpression.Expression != fieldSelector.Parameters[0])
        {
            throw new NotSupportedException(
                "Collection indexes currently support only direct field/property selectors like x => x.Age.");
        }

        return memberExpression.Member.Name;
    }

    internal static CollectionIndexBinding<T> Create(
        string fieldPath,
        string indexName,
        IIndexStore indexStore)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentNullException.ThrowIfNull(indexStore);

        MemberInfo member = ResolveMember(fieldPath);
        var accessor = BuildAccessor(member);
        var valueKind = ResolveValueKind(GetMemberType(member), fieldPath);
        string jsonPropertyName = JsonNamingPolicy.CamelCase.ConvertName(fieldPath);
        return new CollectionIndexBinding<T>(
            fieldPath,
            indexName,
            indexStore,
            accessor,
            jsonPropertyName,
            valueKind);
    }

    internal static void ValidateFieldPath(string fieldPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        MemberInfo member = ResolveMember(fieldPath);
        _ = ResolveValueKind(GetMemberType(member), fieldPath);
    }

    internal bool TryBuildKeyFromDocument(T document, out long indexKey)
        => TryBuildKey(_fieldAccessor(document), out indexKey);

    internal bool TryBuildKeyFromValue(object? value, out long indexKey)
        => TryBuildKey(value, out indexKey);

    internal bool TryBuildKeyFromDirectPayload(ReadOnlySpan<byte> payload, out long indexKey)
    {
        indexKey = 0;
        if (!CollectionIndexedFieldReader.TryReadValue(payload, _jsonPropertyName, out var value))
            return false;

        return TryBuildKey(value, out indexKey);
    }

    internal bool TryDirectPayloadTextEquals(ReadOnlySpan<byte> payload, string value)
        => UsesTextKey && CollectionIndexedFieldReader.TryTextEquals(payload, _jsonPropertyName, value);

    private bool TryBuildKey(object? value, out long indexKey)
    {
        indexKey = 0;
        if (!TryConvertToDbValue(value, out var dbValue))
            return false;

        return TryBuildKey(dbValue, out indexKey);
    }

    private bool TryBuildKey(DbValue dbValue, out long indexKey)
    {
        indexKey = 0;

        if (_valueKind == CollectionIndexValueKind.Integer)
        {
            indexKey = dbValue.AsInteger;
            return true;
        }

        indexKey = ComputeIndexKey([dbValue]);
        return true;
    }

    private static MemberInfo ResolveMember(string fieldPath)
    {
        foreach (PropertyInfo property in typeof(T).GetProperties())
        {
            if (property.GetMethod?.IsStatic == true)
                continue;

            if (string.Equals(property.Name, fieldPath, StringComparison.OrdinalIgnoreCase))
                return property;
        }

        foreach (FieldInfo field in typeof(T).GetFields())
        {
            if (field.IsStatic)
                continue;

            if (string.Equals(field.Name, fieldPath, StringComparison.OrdinalIgnoreCase))
                return field;
        }

        throw new InvalidOperationException(
            $"Cannot bind collection index field '{fieldPath}' for document type '{typeof(T).Name}'.");
    }

    private static Func<T, object?> BuildAccessor(MemberInfo member)
    {
        var document = Expression.Parameter(typeof(T), "document");
        Expression memberAccess = member switch
        {
            PropertyInfo property => Expression.Property(document, property),
            FieldInfo field => Expression.Field(document, field),
            _ => throw new InvalidOperationException(
                $"Member '{member.Name}' cannot be used for collection indexing."),
        };

        var boxed = Expression.Convert(memberAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(boxed, document).Compile();
    }

    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo property => property.PropertyType,
            FieldInfo field => field.FieldType,
            _ => throw new InvalidOperationException(
                $"Member '{member.Name}' cannot be used for collection indexing."),
        };

    private static CollectionIndexValueKind ResolveValueKind(Type memberType, string fieldPath)
    {
        Type effectiveType = Nullable.GetUnderlyingType(memberType) ?? memberType;
        if (effectiveType == typeof(string))
            return CollectionIndexValueKind.Text;

        if (effectiveType.IsEnum)
            effectiveType = Enum.GetUnderlyingType(effectiveType);

        if (effectiveType == typeof(byte) ||
            effectiveType == typeof(sbyte) ||
            effectiveType == typeof(short) ||
            effectiveType == typeof(ushort) ||
            effectiveType == typeof(int) ||
            effectiveType == typeof(uint) ||
            effectiveType == typeof(long) ||
            effectiveType == typeof(ulong))
        {
            return CollectionIndexValueKind.Integer;
        }

        throw new NotSupportedException(
            $"Collection index field '{fieldPath}' on '{typeof(T).Name}' must be string or integer typed.");
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool TryConvertToDbValue(object? value, out DbValue dbValue)
    {
        switch (value)
        {
            case null:
                dbValue = default;
                return false;
            case string text:
                dbValue = DbValue.FromText(text);
                return true;
            case byte byteValue:
                dbValue = DbValue.FromInteger(byteValue);
                return true;
            case sbyte sbyteValue:
                dbValue = DbValue.FromInteger(sbyteValue);
                return true;
            case short shortValue:
                dbValue = DbValue.FromInteger(shortValue);
                return true;
            case ushort ushortValue:
                dbValue = DbValue.FromInteger(ushortValue);
                return true;
            case int intValue:
                dbValue = DbValue.FromInteger(intValue);
                return true;
            case uint uintValue:
                dbValue = DbValue.FromInteger(uintValue);
                return true;
            case long longValue:
                dbValue = DbValue.FromInteger(longValue);
                return true;
            case ulong ulongValue when ulongValue <= long.MaxValue:
                dbValue = DbValue.FromInteger((long)ulongValue);
                return true;
            case Enum enumValue:
                return TryConvertToDbValue(
                    Convert.ChangeType(enumValue, Enum.GetUnderlyingType(enumValue.GetType()), CultureInfo.InvariantCulture),
                    out dbValue);
            default:
                dbValue = default;
                return false;
        }
    }

    private static long ComputeIndexKey(ReadOnlySpan<DbValue> keyComponents)
    {
        if (keyComponents.Length == 1 && keyComponents[0].Type == DbType.Integer)
            return keyComponents[0].AsInteger;

        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int i = 0; i < keyComponents.Length; i++)
            hash = HashIndexKeyComponent(hash, keyComponents[i], prime);

        return unchecked((long)hash);
    }


    private static ulong HashIndexKeyComponent(ulong hash, DbValue value, ulong prime)
    {
        hash ^= (byte)value.Type;
        hash *= prime;

        switch (value.Type)
        {
            case DbType.Integer:
                hash ^= unchecked((ulong)value.AsInteger);
                hash *= prime;
                return hash;
            case DbType.Text:
                foreach (byte b in Encoding.UTF8.GetBytes(value.AsText))
                {
                    hash ^= b;
                    hash *= prime;
                }

                return hash;
            default:
                throw new InvalidOperationException(
                    $"Collection indexes do not support values of type '{value.Type}'.");
        }
    }
}
