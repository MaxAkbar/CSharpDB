using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using CSharpDB.Execution;
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
    private readonly string? _collation;

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
        CollectionIndexValueKind valueKind,
        string? collation)
    {
        FieldPath = fieldPath;
        IndexName = indexName;
        IndexStore = indexStore;
        _fieldAccessor = fieldAccessor;
        _payloadAccessor = payloadAccessor;
        _valueKind = valueKind;
        _collation = ValidateCollationForValueKind(valueKind, collation, fieldPath);
    }

    internal string FieldPath { get; }

    internal string IndexName { get; }

    internal IIndexStore IndexStore { get; }

    internal bool UsesIntegerKey => _valueKind == CollectionIndexValueKind.Integer;

    internal bool UsesTextKey => _valueKind == CollectionIndexValueKind.Text;

    internal bool IsMultiValueArray => _payloadAccessor.TargetsArrayElements;

    internal bool SupportsOrderedRange => !IsMultiValueArray && (UsesIntegerKey || UsesTextKey);

    internal string? Collation => _collation;

    internal bool MatchesValue<TField>(T document, TField value, EqualityComparer<TField> comparer)
    {
        object? fieldValue = _fieldAccessor(document);
        if (UsesTextKey && TryConvertToDbValue(value, out var expectedValue))
            return ValueMatches(fieldValue, expectedValue);

        if (IsMultiValueArray)
        {
            if (fieldValue is string || fieldValue is not System.Collections.IEnumerable enumerable)
                return false;

            foreach (object? element in enumerable)
            {
                if (element is TField arrayElement && comparer.Equals(arrayElement, value))
                    return true;

                if (element is null && value is null)
                    return true;
            }

            return false;
        }

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

    internal static string NormalizeFieldPath(string fieldPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        string[] segments = ParseFieldPathSegments(fieldPath, out bool[] arraySegments, out bool targetsArrayElements);
        MemberInfo[] memberPath = ResolveMemberPath(segments, arraySegments, fieldPath);
        _ = ResolveValueKind(GetMemberType(memberPath[^1]), fieldPath, targetsArrayElements);

        string[] canonicalSegments = Array.ConvertAll(memberPath, static member => member.Name);
        for (int i = 0; i < canonicalSegments.Length; i++)
        {
            if (arraySegments[i])
                canonicalSegments[i] += "[]";
        }

        return string.Join(".", canonicalSegments);
    }

    internal static CollectionIndexBinding<T> Create(
        string fieldPath,
        string indexName,
        IIndexStore indexStore,
        string? collation = null)
    {
        fieldPath = NormalizeFieldPath(fieldPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        ArgumentNullException.ThrowIfNull(indexStore);

        string[] segments = ParseFieldPathSegments(fieldPath, out bool[] arraySegments, out bool targetsArrayElements);
        MemberInfo[] memberPath = ResolveMemberPath(segments, arraySegments, fieldPath);
        var accessor = BuildAccessor(memberPath, arraySegments);
        var valueKind = ResolveValueKind(GetMemberType(memberPath[^1]), fieldPath, targetsArrayElements);
        var payloadAccessor = CollectionFieldAccessor.FromFieldPath(fieldPath);
        return new CollectionIndexBinding<T>(
            fieldPath,
            indexName,
            indexStore,
            accessor,
            payloadAccessor,
            valueKind,
            collation);
    }

    internal static Func<T, object?> CreateFieldAccessor(string fieldPath)
    {
        fieldPath = NormalizeFieldPath(fieldPath);
        string[] segments = ParseFieldPathSegments(fieldPath, out bool[] arraySegments, out _);
        MemberInfo[] memberPath = ResolveMemberPath(segments, arraySegments, fieldPath);
        return BuildAccessor(memberPath, arraySegments);
    }

    internal static void ValidateFieldPath(string fieldPath)
    {
        fieldPath = NormalizeFieldPath(fieldPath);
        string[] segments = ParseFieldPathSegments(fieldPath, out bool[] arraySegments, out bool targetsArrayElements);
        MemberInfo[] memberPath = ResolveMemberPath(segments, arraySegments, fieldPath);
        _ = ResolveValueKind(GetMemberType(memberPath[^1]), fieldPath, targetsArrayElements);
    }

    internal static CollectionIndexBinding<T> CreateTransient(string fieldPath, string? collation = null)
        => Create(fieldPath, "__collection_index_transient__", NoopIndexStore.Instance, collation);

    internal bool TryBuildKeyFromDocument(T document, out long indexKey)
        => TryBuildKey(_fieldAccessor(document), out indexKey);

    internal bool TryBuildKeyFromValue(object? value, out long indexKey)
        => TryBuildKey(value, out indexKey);

    internal static bool TryConvertComparableValue(object? value, out DbValue dbValue)
        => TryConvertToDbValue(value, out dbValue);

    internal bool TryBuildKeyFromDirectPayload(ReadOnlySpan<byte> payload, out long indexKey)
    {
        if (IsMultiValueArray)
        {
            indexKey = 0;
            return false;
        }

        indexKey = 0;
        if (_valueKind == CollectionIndexValueKind.Integer)
        {
            if (!_payloadAccessor.TryReadInt64(payload, out long integerValue))
                return false;

            indexKey = integerValue;
            return true;
        }

        if (!_payloadAccessor.TryReadString(payload, out string? textValue) || textValue == null)
            return false;

        return TryBuildKey(DbValue.FromText(textValue), out indexKey);
    }

    internal bool TryCollectKeysFromDocument(T document, HashSet<long> indexKeys)
        => TryCollectKeys(_fieldAccessor(document), indexKeys);

    internal bool TryCollectTextValuesFromDocument(T document, HashSet<string> textValues)
        => TryCollectTextValues(_fieldAccessor(document), textValues);

    internal bool TryCollectKeysFromDirectPayload(ReadOnlySpan<byte> payload, HashSet<long> indexKeys)
    {
        int startCount = indexKeys.Count;

        if (!IsMultiValueArray)
        {
            if (TryBuildKeyFromDirectPayload(payload, out long indexKey))
                indexKeys.Add(indexKey);

            return indexKeys.Count != startCount;
        }

        var values = new List<DbValue>();
        if (!_payloadAccessor.TryReadIndexValues(payload, values))
            return false;

        for (int i = 0; i < values.Count; i++)
        {
            if (TryBuildKey(values[i], out long indexKey))
                indexKeys.Add(indexKey);
        }

        return indexKeys.Count != startCount;
    }

    internal bool TryCollectTextValuesFromDirectPayload(ReadOnlySpan<byte> payload, HashSet<string> textValues)
    {
        int startCount = textValues.Count;
        if (!UsesTextKey)
            return false;

        if (!IsMultiValueArray)
        {
            if (_payloadAccessor.TryReadString(payload, out string? textValue) && textValue != null)
                textValues.Add(NormalizeTextForIndex(textValue));

            return textValues.Count != startCount;
        }

        var values = new List<DbValue>();
        if (!_payloadAccessor.TryReadIndexValues(payload, values))
            return false;

        for (int i = 0; i < values.Count; i++)
        {
            if (values[i].Type == DbType.Text)
                textValues.Add(NormalizeTextForIndex(values[i].AsText));
        }

        return textValues.Count != startCount;
    }

    internal bool TryDirectPayloadValueEquals(ReadOnlySpan<byte> payload, DbValue value)
    {
        if (!IsMultiValueArray)
        {
            if (!TryReadComparableValue(payload, out var actualValue))
                return false;

            return CollationSupport.Compare(actualValue, value, _collation) == 0;
        }

        var values = new List<DbValue>();
        if (!_payloadAccessor.TryReadIndexValues(payload, values))
            return false;

        for (int i = 0; i < values.Count; i++)
        {
            if (CollationSupport.Compare(values[i], value, _collation) == 0)
                return true;
        }

        return false;
    }

    internal bool TryDirectPayloadTextEquals(ReadOnlySpan<byte> payload, string value)
        => UsesTextKey && TryDirectPayloadValueEquals(payload, DbValue.FromText(value));

    internal bool TryDirectPayloadValueInRange(
        ReadOnlySpan<byte> payload,
        DbValue lowerBound,
        bool lowerInclusive,
        DbValue upperBound,
        bool upperInclusive)
    {
        if (IsMultiValueArray)
            return false;

        if (!TryReadComparableValue(payload, out var actualValue))
            return false;

        return IsWithinRange(actualValue, lowerBound, lowerInclusive, upperBound, upperInclusive);
    }

    internal bool MatchesRangeValue(
        T document,
        DbValue lowerBound,
        bool lowerInclusive,
        DbValue upperBound,
        bool upperInclusive)
    {
        if (IsMultiValueArray || !TryConvertToDbValue(_fieldAccessor(document), out var actualValue))
            return false;

        return IsWithinRange(actualValue, lowerBound, lowerInclusive, upperBound, upperInclusive);
    }

    private bool TryBuildKey(object? value, out long indexKey)
    {
        indexKey = 0;
        if (!TryConvertToDbValue(value, out var dbValue))
            return false;

        return TryBuildKey(dbValue, out indexKey);
    }

    private bool TryCollectKeys(object? value, HashSet<long> indexKeys)
    {
        int startCount = indexKeys.Count;

        if (!IsMultiValueArray)
        {
            if (TryBuildKey(value, out long indexKey))
                indexKeys.Add(indexKey);

            return indexKeys.Count != startCount;
        }

        if (value is string || value is not System.Collections.IEnumerable enumerable)
            return false;

        foreach (object? element in enumerable)
        {
            if (TryBuildKey(element, out long indexKey))
                indexKeys.Add(indexKey);
        }

        return indexKeys.Count != startCount;
    }

    private bool TryCollectTextValues(object? value, HashSet<string> textValues)
    {
        int startCount = textValues.Count;
        if (!UsesTextKey)
            return false;

        if (!IsMultiValueArray)
        {
            if (TryConvertToDbValue(value, out var dbValue) && dbValue.Type == DbType.Text)
                textValues.Add(NormalizeTextForIndex(dbValue.AsText));

            return textValues.Count != startCount;
        }

        if (value is string || value is not System.Collections.IEnumerable enumerable)
            return false;

        foreach (object? element in enumerable)
        {
            if (TryConvertToDbValue(element, out var dbValue) && dbValue.Type == DbType.Text)
                textValues.Add(NormalizeTextForIndex(dbValue.AsText));
        }

        return textValues.Count != startCount;
    }

    private bool TryBuildKey(DbValue dbValue, out long indexKey)
    {
        indexKey = 0;

        if (_valueKind == CollectionIndexValueKind.Integer)
        {
            if (dbValue.Type != DbType.Integer)
                return false;

            indexKey = dbValue.AsInteger;
            return true;
        }

        if (dbValue.Type != DbType.Text)
            return false;

        indexKey = OrderedTextIndexKeyCodec.ComputeKey(NormalizeTextForIndex(dbValue.AsText));
        return true;
    }

    private bool TryReadComparableValue(ReadOnlySpan<byte> payload, out DbValue value)
    {
        value = DbValue.Null;

        if (!IsMultiValueArray)
            return _payloadAccessor.TryReadValue(payload, out value);

        var values = new List<DbValue>();
        if (!_payloadAccessor.TryReadIndexValues(payload, values))
            return false;

        for (int i = 0; i < values.Count; i++)
        {
            if (values[i].Type == (_valueKind == CollectionIndexValueKind.Text ? DbType.Text : DbType.Integer))
            {
                value = values[i];
                return true;
            }
        }

        return false;
    }

    private bool ValueMatches(object? actual, DbValue expected)
    {
        if (!IsMultiValueArray)
        {
            if (!TryConvertToDbValue(actual, out var actualValue))
                return false;

            return CollationSupport.Compare(actualValue, expected, _collation) == 0;
        }

        if (actual is string || actual is not System.Collections.IEnumerable enumerable)
            return false;

        foreach (object? element in enumerable)
        {
            if (TryConvertToDbValue(element, out var actualValue) &&
                CollationSupport.Compare(actualValue, expected, _collation) == 0)
            {
                return true;
            }
        }

        return false;
    }

    private string NormalizeTextForIndex(string text)
        => CollationSupport.NormalizeText(text, _collation);

    private static MemberInfo[] ResolveMemberPath(string[] segments, bool[] arraySegments, string fieldPath)
    {
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
            Type memberType = Nullable.GetUnderlyingType(GetMemberType(member)) ?? GetMemberType(member);
            if (arraySegments[i])
            {
                if (!TryGetCollectionElementType(memberType, out Type? elementType) || elementType == null)
                {
                    throw new NotSupportedException(
                        $"Collection index field '{fieldPath}' on '{typeof(T).Name}' must target an array or list field when using '[]'.");
                }

                currentType = Nullable.GetUnderlyingType(elementType) ?? elementType;
                continue;
            }

            currentType = memberType;
        }

        return memberPath;
    }

    private static string[] ParseFieldPathSegments(string fieldPath, out bool[] arraySegments, out bool targetsArrayElements)
    {
        targetsArrayElements = false;
        string trimmed = fieldPath.Trim();
        if (trimmed.Length == 0)
        {
            throw new ArgumentException(
                $"Cannot bind collection index field '{fieldPath}' for document type '{typeof(T).Name}'.",
                nameof(fieldPath));
        }

        if (trimmed[0] == '$')
        {
            if (trimmed.Length == 1)
            {
                throw new ArgumentException(
                    $"Collection index field path '{fieldPath}' does not contain a path after '$'.",
                    nameof(fieldPath));
            }

            if (!trimmed.StartsWith("$.", StringComparison.Ordinal))
            {
                throw new NotSupportedException(
                    "Collection path indexes currently support only object-member paths like '$.address.city'.");
            }

            trimmed = trimmed[2..];
        }

        string[] segments = trimmed.Split('.');
        arraySegments = new bool[segments.Length];
        for (int i = 0; i < segments.Length; i++)
        {
            segments[i] = segments[i].Trim();
            if (segments[i].Length == 0)
            {
                throw new ArgumentException(
                    $"Collection index field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            if (segments[i].EndsWith("[]", StringComparison.Ordinal) ||
                segments[i].EndsWith("[*]", StringComparison.Ordinal))
            {
                targetsArrayElements = true;
                arraySegments[i] = true;
                segments[i] = segments[i].EndsWith("[*]", StringComparison.Ordinal)
                    ? segments[i][..^3]
                    : segments[i][..^2];
            }
            else if (segments[i].IndexOf('[') >= 0 || segments[i].IndexOf(']') >= 0)
            {
                throw new NotSupportedException(
                    "Collection path indexes currently support only wildcard array segments like 'Tags[]' or '$.orders[].sku'.");
            }

            if (segments[i].Length == 0)
            {
                throw new ArgumentException(
                    $"Collection index field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }
        }

        return segments;
    }

    private static MemberInfo ResolveMember(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields)]
        Type sourceType,
        string segment,
        string fieldPath)
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

    private static Func<T, object?> BuildAccessor(IReadOnlyList<MemberInfo> memberPath, bool[] arraySegments)
    {
        MemberInfo[] capturedPath = memberPath.ToArray();
        bool[] capturedArraySegments = arraySegments.ToArray();
        return document => ReadMemberPathValue(document, capturedPath, capturedArraySegments, 0);
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2073",
        Justification = "Collection index binding is a reflection-based feature layered on Collection<T>, which already requires public document members and is not trim-safe without those members preserved.")]
    [return: DynamicallyAccessedMembers(
        DynamicallyAccessedMemberTypes.PublicProperties |
        DynamicallyAccessedMemberTypes.PublicFields |
        DynamicallyAccessedMemberTypes.Interfaces)]
    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo property => property.PropertyType,
            FieldInfo field => field.FieldType,
            _ => throw new InvalidOperationException(
                $"Member '{member.Name}' cannot be used for collection indexing."),
        };

    private static CollectionIndexValueKind ResolveValueKind(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type memberType,
        string fieldPath,
        bool targetsArrayElements)
    {
        Type effectiveType = Nullable.GetUnderlyingType(memberType) ?? memberType;
        if (targetsArrayElements)
        {
            if (TryGetCollectionElementType(memberType, out Type? elementType) && elementType != null)
                effectiveType = Nullable.GetUnderlyingType(elementType) ?? elementType;
        }
        else if (TryGetCollectionElementType(memberType, out _, out _))
        {
            throw new NotSupportedException(
                $"Collection index field '{fieldPath}' on '{typeof(T).Name}' is multi-valued. Use an explicit '[]' segment for array/list element indexing.");
        }

        if (effectiveType == typeof(string))
            return CollectionIndexValueKind.Text;
        if (effectiveType == typeof(Guid))
            return CollectionIndexValueKind.Text;
        if (effectiveType == typeof(DateOnly))
            return CollectionIndexValueKind.Text;
        if (effectiveType == typeof(TimeOnly))
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
            $"Collection index field '{fieldPath}' on '{typeof(T).Name}' must be string, Guid, DateOnly, TimeOnly, or integer typed.");
    }

    private static bool TryGetCollectionElementType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type memberType,
        [NotNullWhen(true)] out Type? elementType)
        => TryGetCollectionElementType(memberType, out elementType, out _);

    private static bool TryGetCollectionElementType(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]
        Type memberType,
        [NotNullWhen(true)] out Type? elementType,
        out bool isConcreteList)
    {
        if (memberType == typeof(string))
        {
            elementType = null;
            isConcreteList = false;
            return false;
        }

        if (memberType.IsArray)
        {
            elementType = memberType.GetElementType();
            isConcreteList = true;
            return elementType != null;
        }

        if (memberType.IsGenericType && memberType.GetGenericTypeDefinition() == typeof(List<>))
        {
            elementType = memberType.GenericTypeArguments[0];
            isConcreteList = true;
            return true;
        }

        if (memberType.IsGenericType)
        {
            Type genericDefinition = memberType.GetGenericTypeDefinition();
            if (genericDefinition == typeof(IEnumerable<>) ||
                genericDefinition == typeof(ICollection<>) ||
                genericDefinition == typeof(IList<>) ||
                genericDefinition == typeof(IReadOnlyCollection<>) ||
                genericDefinition == typeof(IReadOnlyList<>))
            {
                elementType = memberType.GenericTypeArguments[0];
                isConcreteList = false;
                return true;
            }
        }

        foreach (Type implementedInterface in memberType.GetInterfaces())
        {
            if (!implementedInterface.IsGenericType)
                continue;

            Type genericDefinition = implementedInterface.GetGenericTypeDefinition();
            if (genericDefinition == typeof(IEnumerable<>) ||
                genericDefinition == typeof(ICollection<>) ||
                genericDefinition == typeof(IList<>) ||
                genericDefinition == typeof(IReadOnlyCollection<>) ||
                genericDefinition == typeof(IReadOnlyList<>))
            {
                elementType = implementedInterface.GenericTypeArguments[0];
                isConcreteList = false;
                return true;
            }
        }

        elementType = null;
        isConcreteList = false;
        return false;
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

    private static object? ReadMemberPathValue(
        object? current,
        IReadOnlyList<MemberInfo> memberPath,
        IReadOnlyList<bool> arraySegments,
        int pathIndex)
    {
        if (current is null)
            return null;

        object? value = ReadMemberValue(current, memberPath[pathIndex]);
        if (arraySegments[pathIndex])
        {
            if (value is string || value is not System.Collections.IEnumerable enumerable)
                return null;

            var flattened = new List<object?>();
            foreach (object? element in enumerable)
            {
                object? nestedValue = pathIndex == memberPath.Count - 1
                    ? element
                    : ReadMemberPathValue(element, memberPath, arraySegments, pathIndex + 1);
                AddFlattenedValue(flattened, nestedValue);
            }

            return flattened.Count == 0 ? null : flattened;
        }

        if (pathIndex == memberPath.Count - 1)
            return value;

        return ReadMemberPathValue(value, memberPath, arraySegments, pathIndex + 1);
    }

    private static void AddFlattenedValue(List<object?> values, object? value)
    {
        if (value is null)
            return;

        if (value is string || value is not System.Collections.IEnumerable enumerable)
        {
            values.Add(value);
            return;
        }

        foreach (object? element in enumerable)
            AddFlattenedValue(values, element);
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
            case Guid guidValue:
                dbValue = DbValue.FromText(guidValue.ToString("D"));
                return true;
            case DateOnly dateOnlyValue:
                dbValue = DbValue.FromText(dateOnlyValue.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case TimeOnly timeOnlyValue:
                dbValue = DbValue.FromText(timeOnlyValue.ToString("O", CultureInfo.InvariantCulture));
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

    private bool IsWithinRange(
        DbValue actualValue,
        DbValue lowerBound,
        bool lowerInclusive,
        DbValue upperBound,
        bool upperInclusive)
    {
        if (actualValue.Type != lowerBound.Type || actualValue.Type != upperBound.Type)
            return false;

        int lowerComparison = CollationSupport.Compare(actualValue, lowerBound, _collation);
        if (lowerComparison < 0 || (!lowerInclusive && lowerComparison == 0))
            return false;

        int upperComparison = CollationSupport.Compare(actualValue, upperBound, _collation);
        if (upperComparison > 0 || (!upperInclusive && upperComparison == 0))
            return false;

        return true;
    }

    private static string? ValidateCollationForValueKind(CollectionIndexValueKind valueKind, string? collation, string fieldPath)
    {
        string? normalized = CollationSupport.NormalizeMetadataName(collation);
        if (valueKind != CollectionIndexValueKind.Text)
        {
            if (normalized != null)
            {
                throw new NotSupportedException(
                    $"Collection index field '{fieldPath}' only supports collation for text values.");
            }

            return null;
        }

        if (!CollationSupport.IsSupported(normalized))
        {
            throw new NotSupportedException(
                $"Collection indexes currently support only BINARY and NOCASE collations. '{collation}' is not supported.");
        }

        return normalized;
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

    private sealed class NoopIndexStore : IIndexStore
    {
        internal static readonly NoopIndexStore Instance = new();

        public uint RootPageId => 0;

        public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
            => ValueTask.FromResult<byte[]?>(null);

        public ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default)
            => ValueTask.FromResult<long?>(null);

        public ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
            => throw new NotSupportedException("Transient collection index bindings do not support writes.");

        public ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
            => throw new NotSupportedException("Transient collection index bindings do not support writes.");

        public ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
            => throw new NotSupportedException("Transient collection index bindings do not support writes.");

        public IIndexCursor CreateCursor(IndexScanRange range)
            => throw new NotSupportedException("Transient collection index bindings do not support scans.");
    }
}
