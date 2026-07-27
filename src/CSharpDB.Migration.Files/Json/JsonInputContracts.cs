namespace CSharpDB.Migration.Files.Json;

/// <summary>Identifies how top-level JSON input values are framed.</summary>
public enum JsonInputFraming
{
    /// <summary>One JSON array whose elements are logical input records.</summary>
    RootArray,

    /// <summary>Consecutive complete JSON values separated by JSON whitespace.</summary>
    MultipleValues,
}

/// <summary>
/// Freezes the strict JSON input representation and its absolute safety
/// ceilings.
/// </summary>
public static class JsonInputContracts
{
    /// <summary>The only supported character encoding.</summary>
    public const string EncodingName = "utf-8";

    /// <summary>The strict UTF-8 and optional leading BOM policy.</summary>
    public const string EncodingPolicy = "strict-utf8-optional-leading-bom/v1";

    /// <summary>Whether one leading UTF-8 byte-order mark is accepted.</summary>
    public const bool AcceptsLeadingUtf8Bom = true;

    /// <summary>The decoded property-name comparison contract.</summary>
    public const string DecodedPropertyNameComparison = "unicode-ordinal/v1";

    /// <summary>The logical object property-order contract.</summary>
    public const string PropertyOrderPolicy = "encounter-order/v1";

    /// <summary>The JSON-number representation contract.</summary>
    public const string NumberLexemePolicy = "exact-source-json-number/v1";

    /// <summary>The duplicate-property policy.</summary>
    public const string DuplicatePropertyPolicy =
        "reject-duplicate-decoded-property-name/v1";

    /// <summary>The deterministic nested JSON representation version.</summary>
    public const string CanonicalNestedJsonVersion =
        "csharpdb-json-ordered-value/v1";

    /// <summary>Absolute encoded-byte ceiling for one logical value.</summary>
    public const int MaximumValueBytes = 64 * 1024 * 1024;

    /// <summary>Absolute JSON container-depth ceiling.</summary>
    public const int MaximumDepth = 128;

    /// <summary>Absolute property-count ceiling for one object.</summary>
    public const int MaximumPropertiesPerObject = 16_384;

    /// <summary>Absolute element-count ceiling for one array.</summary>
    public const int MaximumArrayElements = 65_536;

    /// <summary>Absolute node-count ceiling for one logical value.</summary>
    public const int MaximumTotalNodes = 65_536;

    /// <summary>Absolute UTF-8 byte ceiling for one decoded property name.</summary>
    public const int MaximumPropertyNameBytes = 1024 * 1024;

    /// <summary>Absolute UTF-8 byte ceiling for one decoded string.</summary>
    public const int MaximumStringBytes = 16 * 1024 * 1024;

    /// <summary>Absolute ASCII byte ceiling for one exact number lexeme.</summary>
    public const int MaximumNumberBytes = 16 * 1024 * 1024;
}

/// <summary>
/// Controls strict, forward-only JSON parsing. The value limit counts source
/// bytes; property-name and string limits count decoded UTF-8 bytes; and the
/// number limit counts exact source-lexeme bytes.
/// </summary>
public sealed record JsonStreamingReaderOptions
{
    /// <summary>Gets the top-level input framing mode.</summary>
    public JsonInputFraming Framing { get; init; } = JsonInputFraming.RootArray;

    /// <summary>Gets the maximum source bytes in one logical value.</summary>
    public int MaxValueBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the maximum JSON container depth.</summary>
    public int MaxDepth { get; init; } = 64;

    /// <summary>Gets the maximum properties in one object.</summary>
    public int MaxPropertiesPerObject { get; init; } = 4_096;

    /// <summary>Gets the maximum elements in one array.</summary>
    public int MaxArrayElements { get; init; } = 65_536;

    /// <summary>Gets the maximum object, array, and scalar nodes in one value.</summary>
    public int MaxTotalNodes { get; init; } = 65_536;

    /// <summary>Gets the maximum UTF-8 bytes in one decoded property name.</summary>
    public int MaxPropertyNameBytes { get; init; } = 64 * 1024;

    /// <summary>Gets the maximum UTF-8 bytes in one decoded string.</summary>
    public int MaxStringBytes { get; init; } = 16 * 1024 * 1024;

    /// <summary>Gets the maximum ASCII bytes in one exact number lexeme.</summary>
    public int MaxNumberBytes { get; init; } = 1024 * 1024;

    /// <summary>Gets whether disposing the reader leaves its source open.</summary>
    public bool LeaveOpen { get; init; }
}

internal sealed class JsonStreamingReaderSettings
{
    private JsonStreamingReaderSettings(JsonStreamingReaderOptions options)
    {
        Framing = options.Framing;
        MaxValueBytes = options.MaxValueBytes;
        MaxDepth = options.MaxDepth;
        MaxPropertiesPerObject = options.MaxPropertiesPerObject;
        MaxArrayElements = options.MaxArrayElements;
        MaxTotalNodes = options.MaxTotalNodes;
        MaxPropertyNameBytes = options.MaxPropertyNameBytes;
        MaxStringBytes = options.MaxStringBytes;
        MaxNumberBytes = options.MaxNumberBytes;
        LeaveOpen = options.LeaveOpen;
    }

    internal JsonInputFraming Framing { get; }

    internal int MaxValueBytes { get; }

    internal int MaxDepth { get; }

    internal int MaxPropertiesPerObject { get; }

    internal int MaxArrayElements { get; }

    internal int MaxTotalNodes { get; }

    internal int MaxPropertyNameBytes { get; }

    internal int MaxStringBytes { get; }

    internal int MaxNumberBytes { get; }

    internal bool LeaveOpen { get; }

    internal static JsonStreamingReaderSettings Create(
        JsonStreamingReaderOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!Enum.IsDefined(options.Framing))
            throw new ArgumentOutOfRangeException(nameof(options), "JSON input framing is invalid.");

        RequireRange(
            options.MaxValueBytes,
            JsonInputContracts.MaximumValueBytes,
            nameof(options.MaxValueBytes));
        RequireRange(
            options.MaxDepth,
            JsonInputContracts.MaximumDepth,
            nameof(options.MaxDepth));
        RequireRange(
            options.MaxPropertiesPerObject,
            JsonInputContracts.MaximumPropertiesPerObject,
            nameof(options.MaxPropertiesPerObject));
        RequireRange(
            options.MaxArrayElements,
            JsonInputContracts.MaximumArrayElements,
            nameof(options.MaxArrayElements));
        RequireRange(
            options.MaxTotalNodes,
            JsonInputContracts.MaximumTotalNodes,
            nameof(options.MaxTotalNodes));
        RequireRange(
            options.MaxPropertyNameBytes,
            JsonInputContracts.MaximumPropertyNameBytes,
            nameof(options.MaxPropertyNameBytes));
        RequireRange(
            options.MaxStringBytes,
            JsonInputContracts.MaximumStringBytes,
            nameof(options.MaxStringBytes));
        RequireRange(
            options.MaxNumberBytes,
            JsonInputContracts.MaximumNumberBytes,
            nameof(options.MaxNumberBytes));

        return new JsonStreamingReaderSettings(options);
    }

    private static void RequireRange(int value, int maximum, string name)
    {
        if (value is < 1 || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                $"The value must be between 1 and {maximum}.");
        }
    }
}
