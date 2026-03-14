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
    private readonly CollectionFieldAccessor _payloadAccessor;
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
        CollectionFieldAccessor payloadAccessor,
        CollectionIndexValueKind valueKind)
    {
        FieldPath = fieldPath;
        IndexName = indexName;
        IndexStore = indexStore;
        _fieldAccessor = fieldAccessor;
        _payloadAccessor = payloadAccessor;
        _valueKind = valueKind;
    }

    internal string FieldPath { get; }

    internal string IndexName { get; }

    internal IIndexStore IndexStore { get; }

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

        var pathSegments = new List<string>();
        Expression current = StripConvert(fieldSelector.Body);
        while (current is MemberExpression memberExpression)
        {
            pathSegments.Add(memberExpression.Member.Name);
            current = StripConvert(memberExpression.Expression!);
        }

        if (current != fieldSelector.Parameters[0] || pathSegments.Count == 0)
        {
            throw new NotSupportedException(
                "Collection indexes currently support only field/property selector paths like x => x.Age or x => x.Address.City.");
        }

        pathSegments.Reverse();
        return string.Join(".", pathSegments);
    }

    internal static CollectionIndexBinding<T> Create(
        string fieldPath,
        string indexName,
        IIndexStore indexStore)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentNullException.ThrowIfNull(indexStore);

        MemberInfo[] memberPath = ResolveMemberPath(fieldPath);
        var accessor = BuildAccessor(memberPath);
        var valueKind = ResolveValueKind(GetMemberType(memberPath[^1]), fieldPath);
        var payloadAccessor = CollectionFieldAccessor.FromFieldPath(fieldPath);
        return new CollectionIndexBinding<T>(
            fieldPath,
            indexName,
            indexStore,
            accessor,
            payloadAccessor,
            valueKind);
    }

    internal static void ValidateFieldPath(string fieldPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        MemberInfo[] memberPath = ResolveMemberPath(fieldPath);
        _ = ResolveValueKind(GetMemberType(memberPath[^1]), fieldPath);
    }

    internal bool TryBuildKeyFromDocument(T document, out long indexKey)
        => TryBuildKey(_fieldAccessor(document), out indexKey);

    internal bool TryBuildKeyFromValue(object? value, out long indexKey)
        => TryBuildKey(value, out indexKey);

    internal static bool TryConvertComparableValue(object? value, out DbValue dbValue)
        => TryConvertToDbValue(value, out dbValue);

    internal bool TryBuildKeyFromDirectPayload(ReadOnlySpan<byte> payload, out long indexKey)
    {
        indexKey = 0;
        if (!_payloadAccessor.TryReadValue(payload, out var value))
            return false;

        return TryBuildKey(value, out indexKey);
    }

    internal bool TryDirectPayloadTextEquals(ReadOnlySpan<byte> payload, string value)
        => UsesTextKey && _payloadAccessor.TryTextEquals(payload, value);

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

    private static MemberInfo[] ResolveMemberPath(string fieldPath)
    {
        string[] segments = fieldPath.Split('.');
        var memberPath = new MemberInfo[segments.Length];
        Type currentType = typeof(T);

        for (int i = 0; i < segments.Length; i++)
        {
            string segment = segments[i].Trim();
            if (segment.Length == 0)
            {
                throw new InvalidOperationException(
                    $"Cannot bind collection index field '{fieldPath}' for document type '{typeof(T).Name}'.");
            }

            MemberInfo member = ResolveMember(currentType, segment, fieldPath);
            memberPath[i] = member;
            currentType = Nullable.GetUnderlyingType(GetMemberType(member)) ?? GetMemberType(member);
        }

        return memberPath;
    }

    private static MemberInfo ResolveMember(Type sourceType, string segment, string fieldPath)
    {
        foreach (PropertyInfo property in sourceType.GetProperties())
        {
            if (property.GetMethod?.IsStatic == true)
                continue;

            if (string.Equals(property.Name, segment, StringComparison.OrdinalIgnoreCase))
                return property;
        }

        foreach (FieldInfo field in sourceType.GetFields())
        {
            if (field.IsStatic)
                continue;

            if (string.Equals(field.Name, segment, StringComparison.OrdinalIgnoreCase))
                return field;
        }

        throw new InvalidOperationException(
            $"Cannot bind collection index field '{fieldPath}' for document type '{typeof(T).Name}'.");
    }

    private static Func<T, object?> BuildAccessor(IReadOnlyList<MemberInfo> memberPath)
    {
        MemberInfo[] capturedPath = memberPath.ToArray();
        return document => ReadMemberPathValue(document, capturedPath);
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

    private static object? ReadMemberPathValue(object? current, IReadOnlyList<MemberInfo> memberPath)
    {
        object? value = current;
        for (int i = 0; i < memberPath.Count; i++)
        {
            if (value is null)
                return null;

            value = ReadMemberValue(value, memberPath[i]);
        }

        return value;
    }

    private static object? ReadMemberValue(object source, MemberInfo member)
        => member switch
        {
            PropertyInfo property => property.GetValue(source),
            FieldInfo field => field.GetValue(source),
            _ => throw new InvalidOperationException(
                $"Member '{member.Name}' cannot be used for collection indexing."),
        };

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
