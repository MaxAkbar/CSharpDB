using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Engine;

/// <summary>
/// Precomputed field-path accessor over collection payload JSON.
/// This keeps path normalization and UTF-8 encoding out of hot read paths.
/// </summary>
internal sealed class CollectionFieldAccessor
{
    private readonly string[] _jsonPathSegments;
    private readonly byte[][] _jsonPathSegmentsUtf8;

    private CollectionFieldAccessor(string fieldPath, string[] jsonPathSegments, byte[][] jsonPathSegmentsUtf8)
    {
        FieldPath = fieldPath;
        _jsonPathSegments = jsonPathSegments;
        _jsonPathSegmentsUtf8 = jsonPathSegmentsUtf8;
    }

    internal string FieldPath { get; }

    internal string[] JsonPathSegments => _jsonPathSegments;

    internal byte[][] JsonPathSegmentsUtf8 => _jsonPathSegmentsUtf8;

    internal static CollectionFieldAccessor FromFieldPath(string fieldPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        string[] fieldPathSegments = fieldPath.Split('.');
        var jsonPathSegments = new string[fieldPathSegments.Length];
        var jsonPathSegmentsUtf8 = new byte[fieldPathSegments.Length][];

        for (int i = 0; i < fieldPathSegments.Length; i++)
        {
            string segment = fieldPathSegments[i].Trim();
            if (segment.Length == 0)
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            string jsonSegment = JsonNamingPolicy.CamelCase.ConvertName(segment);
            jsonPathSegments[i] = jsonSegment;
            jsonPathSegmentsUtf8[i] = Encoding.UTF8.GetBytes(jsonSegment);
        }

        return new CollectionFieldAccessor(fieldPath, jsonPathSegments, jsonPathSegmentsUtf8);
    }

    internal bool TryReadValue(ReadOnlySpan<byte> payload, out DbValue value)
        => CollectionIndexedFieldReader.TryReadValue(payload, this, out value);

    internal bool TryTextEquals(ReadOnlySpan<byte> payload, string expectedValue)
        => CollectionIndexedFieldReader.TryTextEquals(payload, this, expectedValue);

    internal bool TryValueEquals(ReadOnlySpan<byte> payload, DbValue expectedValue)
    {
        if (expectedValue.Type == DbType.Text)
            return TryTextEquals(payload, expectedValue.AsText);

        if (!TryReadValue(payload, out var actualValue))
            return false;

        return actualValue.Type == expectedValue.Type &&
               DbValue.Compare(actualValue, expectedValue) == 0;
    }
}
