using System.Collections.ObjectModel;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Identifies one losslessly represented JSON value kind.</summary>
public enum JsonLogicalValueKind
{
    /// <summary>The JSON <c>null</c> literal.</summary>
    Null,

    /// <summary>A JSON boolean literal.</summary>
    Boolean,

    /// <summary>A decoded JSON string.</summary>
    String,

    /// <summary>An exact JSON number lexeme.</summary>
    Number,

    /// <summary>An ordered JSON object.</summary>
    Object,

    /// <summary>An ordered JSON array.</summary>
    Array,
}

/// <summary>One immutable, ordinal object property.</summary>
public sealed class JsonLogicalProperty
{
    private JsonLogicalProperty(int ordinal, string name, JsonLogicalValue value)
    {
        Ordinal = ordinal;
        Name = name;
        Value = value;
    }

    /// <summary>Gets the zero-based encounter ordinal within the object.</summary>
    public int Ordinal { get; }

    /// <summary>Gets the exact decoded property name.</summary>
    public string Name { get; }

    /// <summary>Gets the property value.</summary>
    public JsonLogicalValue Value { get; }

    internal static JsonLogicalProperty Create(
        int ordinal,
        string name,
        JsonLogicalValue value)
    {
        if (ordinal < 0)
            throw new ArgumentOutOfRangeException(nameof(ordinal));
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(value);
        JsonLogicalText.RequireValidUnicode(name, "JSON property name");
        return new JsonLogicalProperty(ordinal, name, value);
    }
}

/// <summary>
/// An immutable logical JSON value that preserves object encounter order and
/// exact number lexemes.
/// </summary>
public sealed class JsonLogicalValue
{
    private static readonly ReadOnlyCollection<JsonLogicalProperty> s_noProperties =
        Array.AsReadOnly(Array.Empty<JsonLogicalProperty>());
    private static readonly ReadOnlyCollection<JsonLogicalValue> s_noElements =
        Array.AsReadOnly(Array.Empty<JsonLogicalValue>());

    private readonly bool booleanValue;
    private readonly string? textValue;
    private readonly ReadOnlyCollection<JsonLogicalProperty> properties;
    private readonly ReadOnlyCollection<JsonLogicalValue> elements;

    private JsonLogicalValue(
        JsonLogicalValueKind kind,
        bool booleanValue = false,
        string? textValue = null,
        JsonLogicalProperty[]? properties = null,
        JsonLogicalValue[]? elements = null,
        int nodeCount = 1)
    {
        Kind = kind;
        this.booleanValue = booleanValue;
        this.textValue = textValue;
        this.properties = properties is null
            ? s_noProperties
            : Array.AsReadOnly(properties);
        this.elements = elements is null
            ? s_noElements
            : Array.AsReadOnly(elements);
        NodeCount = nodeCount;
    }

    /// <summary>Gets the logical JSON kind.</summary>
    public JsonLogicalValueKind Kind { get; }

    /// <summary>Gets the boolean value, or throws when this is not a boolean.</summary>
    public bool BooleanValue =>
        Kind == JsonLogicalValueKind.Boolean
            ? booleanValue
            : throw WrongKind(JsonLogicalValueKind.Boolean);

    /// <summary>Gets the decoded string, or throws when this is not a string.</summary>
    public string StringValue =>
        Kind == JsonLogicalValueKind.String
            ? textValue!
            : throw WrongKind(JsonLogicalValueKind.String);

    /// <summary>
    /// Gets the exact source number lexeme, or throws when this is not a number.
    /// </summary>
    public string NumberLexeme =>
        Kind == JsonLogicalValueKind.Number
            ? textValue!
            : throw WrongKind(JsonLogicalValueKind.Number);

    /// <summary>Gets ordered properties, or throws when this is not an object.</summary>
    public ReadOnlyCollection<JsonLogicalProperty> Properties =>
        Kind == JsonLogicalValueKind.Object
            ? properties
            : throw WrongKind(JsonLogicalValueKind.Object);

    /// <summary>Gets ordered elements, or throws when this is not an array.</summary>
    public ReadOnlyCollection<JsonLogicalValue> Elements =>
        Kind == JsonLogicalValueKind.Array
            ? elements
            : throw WrongKind(JsonLogicalValueKind.Array);

    internal int NodeCount { get; }

    internal static JsonLogicalValue CreateNull() =>
        new(JsonLogicalValueKind.Null);

    internal static JsonLogicalValue CreateBoolean(bool value) =>
        new(JsonLogicalValueKind.Boolean, booleanValue: value);

    internal static JsonLogicalValue CreateString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        JsonLogicalText.RequireValidUnicode(value, "JSON string");
        return new JsonLogicalValue(JsonLogicalValueKind.String, textValue: value);
    }

    internal static JsonLogicalValue CreateNumber(string exactLexeme)
    {
        ArgumentNullException.ThrowIfNull(exactLexeme);
        if (!JsonNumberLexeme.IsValid(exactLexeme))
            throw new ArgumentException("The value is not a valid JSON number lexeme.", nameof(exactLexeme));
        return new JsonLogicalValue(JsonLogicalValueKind.Number, textValue: exactLexeme);
    }

    internal static JsonLogicalValue CreateObject(
        IReadOnlyList<JsonLogicalProperty> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        var copy = new JsonLogicalProperty[properties.Count];
        var names = new HashSet<string>(StringComparer.Ordinal);
        int nodes = 1;
        for (int index = 0; index < copy.Length; index++)
        {
            JsonLogicalProperty property = properties[index] ??
                throw new ArgumentException("A JSON property cannot be null.", nameof(properties));
            if (property.Ordinal != index)
            {
                throw new ArgumentException(
                    "JSON property ordinals must be contiguous encounter ordinals.",
                    nameof(properties));
            }
            if (!names.Add(property.Name))
            {
                throw new ArgumentException(
                    "A JSON object cannot contain duplicate decoded property names.",
                    nameof(properties));
            }

            copy[index] = property;
            nodes = checked(nodes + property.Value.NodeCount);
        }

        return new JsonLogicalValue(
            JsonLogicalValueKind.Object,
            properties: copy,
            nodeCount: nodes);
    }

    internal static JsonLogicalValue CreateArray(IReadOnlyList<JsonLogicalValue> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);
        var copy = new JsonLogicalValue[elements.Count];
        int nodes = 1;
        for (int index = 0; index < copy.Length; index++)
        {
            JsonLogicalValue element = elements[index] ??
                throw new ArgumentException("A JSON array element cannot be null.", nameof(elements));
            copy[index] = element;
            nodes = checked(nodes + element.NodeCount);
        }

        return new JsonLogicalValue(
            JsonLogicalValueKind.Array,
            elements: copy,
            nodeCount: nodes);
    }

    private static InvalidOperationException WrongKind(JsonLogicalValueKind expected) =>
        new($"The JSON logical value is not {expected}.");
}

/// <summary>One immutable top-level JSON input record and source location.</summary>
public sealed class JsonLogicalRecord
{
    private JsonLogicalRecord(
        long recordOrdinal,
        JsonLogicalValue value,
        long startByteOffset,
        long endByteOffsetExclusive,
        long startLineNumber,
        long startBytePositionInLine)
    {
        RecordOrdinal = recordOrdinal;
        Value = value;
        StartByteOffset = startByteOffset;
        EndByteOffsetExclusive = endByteOffsetExclusive;
        StartLineNumber = startLineNumber;
        StartBytePositionInLine = startBytePositionInLine;
    }

    /// <summary>Gets the one-based logical record ordinal.</summary>
    public long RecordOrdinal { get; }

    /// <summary>Gets the complete logical value.</summary>
    public JsonLogicalValue Value { get; }

    /// <summary>Gets the zero-based source byte offset at which the value starts.</summary>
    public long StartByteOffset { get; }

    /// <summary>Gets the exclusive zero-based source byte offset after the value.</summary>
    public long EndByteOffsetExclusive { get; }

    /// <summary>Gets the encoded source byte length of the value.</summary>
    public long RawByteLength => EndByteOffsetExclusive - StartByteOffset;

    /// <summary>Gets the one-based physical line on which the value starts.</summary>
    public long StartLineNumber { get; }

    /// <summary>Gets the zero-based byte position within the starting line.</summary>
    public long StartBytePositionInLine { get; }

    internal static JsonLogicalRecord Create(
        long recordOrdinal,
        JsonLogicalValue value,
        long startByteOffset,
        long endByteOffsetExclusive,
        long startLineNumber,
        long startBytePositionInLine)
    {
        if (recordOrdinal < 1)
            throw new ArgumentOutOfRangeException(nameof(recordOrdinal));
        ArgumentNullException.ThrowIfNull(value);
        if (startByteOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(startByteOffset));
        if (endByteOffsetExclusive <= startByteOffset)
            throw new ArgumentOutOfRangeException(nameof(endByteOffsetExclusive));
        if (startLineNumber < 1)
            throw new ArgumentOutOfRangeException(nameof(startLineNumber));
        if (startBytePositionInLine < 0)
            throw new ArgumentOutOfRangeException(nameof(startBytePositionInLine));

        return new JsonLogicalRecord(
            recordOrdinal,
            value,
            startByteOffset,
            endByteOffsetExclusive,
            startLineNumber,
            startBytePositionInLine);
    }
}

internal static class JsonLogicalText
{
    internal static void RequireValidUnicode(string value, string description)
    {
        for (int index = 0; index < value.Length; index++)
        {
            char current = value[index];
            if (!char.IsSurrogate(current))
                continue;
            if (!char.IsHighSurrogate(current) ||
                index + 1 >= value.Length ||
                !char.IsLowSurrogate(value[index + 1]))
            {
                throw new ArgumentException($"{description} contains invalid Unicode.");
            }

            index++;
        }
    }
}

internal static class JsonNumberLexeme
{
    internal static bool IsValid(string value)
    {
        if (value.Length == 0)
            return false;
        int index = value[0] == '-' ? 1 : 0;
        if (index == value.Length)
            return false;

        if (value[index] == '0')
        {
            index++;
        }
        else
        {
            if (value[index] is < '1' or > '9')
                return false;
            while (++index < value.Length && value[index] is >= '0' and <= '9')
            {
            }
        }

        if (index < value.Length && value[index] == '.')
        {
            index++;
            int fractionalStart = index;
            while (index < value.Length && value[index] is >= '0' and <= '9')
                index++;
            if (index == fractionalStart)
                return false;
        }

        if (index < value.Length && value[index] is 'e' or 'E')
        {
            index++;
            if (index < value.Length && value[index] is '+' or '-')
                index++;
            int exponentStart = index;
            while (index < value.Length && value[index] is >= '0' and <= '9')
                index++;
            if (index == exponentStart)
                return false;
        }

        return index == value.Length;
    }
}
