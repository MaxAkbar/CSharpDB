using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Engine;

/// <summary>
/// Precomputed field-path accessor over collection payloads.
/// This keeps path normalization and UTF-8 encoding out of hot read paths.
/// </summary>
internal sealed class CollectionFieldAccessor
{
    private readonly string[] _jsonPathSegments;
    private readonly byte[][] _jsonPathSegmentsUtf8;
    private readonly bool[] _jsonPathArraySegments;
    private readonly bool _targetsArrayElements;

    private CollectionFieldAccessor(
        string fieldPath,
        string[] jsonPathSegments,
        byte[][] jsonPathSegmentsUtf8,
        bool[] jsonPathArraySegments,
        bool targetsArrayElements)
    {
        FieldPath = fieldPath;
        _jsonPathSegments = jsonPathSegments;
        _jsonPathSegmentsUtf8 = jsonPathSegmentsUtf8;
        _jsonPathArraySegments = jsonPathArraySegments;
        _targetsArrayElements = targetsArrayElements;
    }

    internal string FieldPath { get; }

    internal string[] JsonPathSegments => _jsonPathSegments;

    internal byte[][] JsonPathSegmentsUtf8 => _jsonPathSegmentsUtf8;

    internal bool[] JsonPathArraySegments => _jsonPathArraySegments;

    internal bool TargetsArrayElements => _targetsArrayElements;

    internal static CollectionFieldAccessor FromFieldPath(string fieldPath)
        => Create(fieldPath, static segment => JsonNamingPolicy.CamelCase.ConvertName(segment));

    internal static CollectionFieldAccessor FromJsonFieldPath(string fieldPath)
        => Create(fieldPath, static segment => segment);

    private static CollectionFieldAccessor Create(
        string fieldPath,
        Func<string, string> jsonSegmentSelector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        ArgumentNullException.ThrowIfNull(jsonSegmentSelector);

        string[] fieldPathSegments = fieldPath.Split('.');
        var jsonPathSegments = new string[fieldPathSegments.Length];
        var jsonPathSegmentsUtf8 = new byte[fieldPathSegments.Length][];
        var jsonPathArraySegments = new bool[fieldPathSegments.Length];
        bool targetsArrayElements = false;

        for (int i = 0; i < fieldPathSegments.Length; i++)
        {
            string segment = fieldPathSegments[i].Trim();
            if (segment.Length == 0)
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            if (segment.EndsWith("[]", StringComparison.Ordinal) || segment.EndsWith("[*]", StringComparison.Ordinal))
            {
                targetsArrayElements = true;
                jsonPathArraySegments[i] = true;
                segment = segment.EndsWith("[*]", StringComparison.Ordinal)
                    ? segment[..^3]
                    : segment[..^2];
            }
            else if (segment.IndexOf('[') >= 0 || segment.IndexOf(']') >= 0)
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an unsupported array/index segment.",
                    nameof(fieldPath));
            }

            if (segment.Length == 0)
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            string jsonSegment = jsonSegmentSelector(segment);
            jsonPathSegments[i] = jsonSegment;
            jsonPathSegmentsUtf8[i] = Encoding.UTF8.GetBytes(jsonSegment);
        }

        return new CollectionFieldAccessor(
            fieldPath,
            jsonPathSegments,
            jsonPathSegmentsUtf8,
            jsonPathArraySegments,
            targetsArrayElements);
    }

    internal bool TryReadValue(ReadOnlySpan<byte> payload, out DbValue value)
        => CollectionIndexedFieldReader.TryReadValue(payload, this, out value);

    internal bool TryReadInt64(ReadOnlySpan<byte> payload, out long value)
        => CollectionIndexedFieldReader.TryReadInt64(payload, this, out value);

    internal bool TryReadString(ReadOnlySpan<byte> payload, out string? value)
        => CollectionIndexedFieldReader.TryReadString(payload, this, out value);

    internal bool TryReadStringUtf8(ReadOnlySpan<byte> payload, out ReadOnlySpan<byte> value)
        => CollectionIndexedFieldReader.TryReadStringUtf8(payload, this, out value);

    internal bool TryReadBoolean(ReadOnlySpan<byte> payload, out bool value)
        => CollectionIndexedFieldReader.TryReadBoolean(payload, this, out value);

    internal bool TryReadDecimal(ReadOnlySpan<byte> payload, out decimal value)
        => CollectionIndexedFieldReader.TryReadDecimal(payload, this, out value);

    internal bool TryTextEquals(ReadOnlySpan<byte> payload, string expectedValue)
        => CollectionIndexedFieldReader.TryTextEquals(payload, this, expectedValue);

    internal bool TryReadIndexValues(ReadOnlySpan<byte> payload, List<DbValue> values)
        => CollectionIndexedFieldReader.TryReadIndexValues(payload, this, values);

    internal bool TryValueEquals(ReadOnlySpan<byte> payload, DbValue expectedValue)
    {
        if (_targetsArrayElements)
            return CollectionIndexedFieldReader.TryArrayContainsValue(payload, this, expectedValue);

        if (expectedValue.Type == DbType.Text)
            return TryTextEquals(payload, expectedValue.AsText);

        if (expectedValue.Type == DbType.Integer &&
            TryReadInt64(payload, out long actualInteger))
        {
            return actualInteger == expectedValue.AsInteger;
        }

        if (!TryReadValue(payload, out var actualValue))
            return false;

        return actualValue.Type == expectedValue.Type &&
               DbValue.Compare(actualValue, expectedValue) == 0;
    }
}
