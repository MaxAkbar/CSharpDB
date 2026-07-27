namespace CSharpDB.Migration.Files.Json;

/// <summary>Stable, value-free rule identifiers for strict JSON input.</summary>
public static class JsonDiagnosticRules
{
    /// <summary>Strict UTF-8 decoding failed.</summary>
    public const string InvalidEncoding = "MIG-JSON-ENCODING-001";

    /// <summary>JSON token syntax or Unicode escape syntax is malformed.</summary>
    public const string MalformedData = "MIG-JSON-FORMAT-001";

    /// <summary>The source does not match its selected top-level framing.</summary>
    public const string InvalidFraming = "MIG-JSON-FRAMING-001";

    /// <summary>An object repeats an ordinally equal decoded property name.</summary>
    public const string DuplicateProperty = "MIG-JSON-DUPLICATE-PROPERTY-001";

    /// <summary>One logical value exceeds its encoded source-byte limit.</summary>
    public const string ValueLimitExceeded = "MIG-JSON-LIMIT-VALUE-001";

    /// <summary>A logical value exceeds its container depth limit.</summary>
    public const string DepthLimitExceeded = "MIG-JSON-LIMIT-DEPTH-001";

    /// <summary>An object exceeds its property-count limit.</summary>
    public const string PropertyCountLimitExceeded = "MIG-JSON-LIMIT-PROPERTIES-001";

    /// <summary>An array exceeds its element-count limit.</summary>
    public const string ArrayElementLimitExceeded = "MIG-JSON-LIMIT-ARRAY-001";

    /// <summary>A logical value exceeds its total node-count limit.</summary>
    public const string NodeCountLimitExceeded = "MIG-JSON-LIMIT-NODES-001";

    /// <summary>A decoded property name exceeds its UTF-8 byte limit.</summary>
    public const string PropertyNameLimitExceeded = "MIG-JSON-LIMIT-PROPERTY-NAME-001";

    /// <summary>A decoded string exceeds its UTF-8 byte limit.</summary>
    public const string StringLimitExceeded = "MIG-JSON-LIMIT-STRING-001";

    /// <summary>An exact number lexeme exceeds its ASCII byte limit.</summary>
    public const string NumberLimitExceeded = "MIG-JSON-LIMIT-NUMBER-001";

    internal static string Message(string ruleId) => ruleId switch
    {
        InvalidEncoding => "JSON input is not valid under the strict UTF-8 encoding policy.",
        MalformedData => "JSON input is malformed.",
        InvalidFraming => "JSON input does not match the selected top-level framing.",
        DuplicateProperty => "A JSON object contains a duplicate decoded property name.",
        ValueLimitExceeded => "A JSON logical value exceeds the configured byte limit.",
        DepthLimitExceeded => "JSON input exceeds the configured depth limit.",
        PropertyCountLimitExceeded => "A JSON object exceeds the configured property-count limit.",
        ArrayElementLimitExceeded => "A JSON array exceeds the configured element-count limit.",
        NodeCountLimitExceeded => "A JSON logical value exceeds the configured node-count limit.",
        PropertyNameLimitExceeded => "A JSON property name exceeds the configured byte limit.",
        StringLimitExceeded => "A JSON string exceeds the configured byte limit.",
        NumberLimitExceeded => "A JSON number exceeds the configured byte limit.",
        _ => throw new ArgumentOutOfRangeException(nameof(ruleId), "Unknown JSON diagnostic rule."),
    };
}

/// <summary>One deterministic JSON read diagnostic without source values.</summary>
public sealed class JsonReadDiagnostic
{
    private JsonReadDiagnostic(
        string ruleId,
        long? recordOrdinal,
        long? byteOffset,
        long? lineNumber,
        long? bytePositionInLine,
        long? limit,
        long? observed)
    {
        RuleId = ruleId;
        Message = JsonDiagnosticRules.Message(ruleId);
        RecordOrdinal = recordOrdinal;
        ByteOffset = byteOffset;
        LineNumber = lineNumber;
        BytePositionInLine = bytePositionInLine;
        Limit = limit;
        Observed = observed;
    }

    /// <summary>Gets the stable diagnostic rule identifier.</summary>
    public string RuleId { get; }

    /// <summary>Gets a deterministic message that contains no source value.</summary>
    public string Message { get; }

    /// <summary>Gets the one-based logical record ordinal when known.</summary>
    public long? RecordOrdinal { get; }

    /// <summary>Gets the zero-based absolute source byte offset when known.</summary>
    public long? ByteOffset { get; }

    /// <summary>Gets the one-based physical source line when known.</summary>
    public long? LineNumber { get; }

    /// <summary>Gets the zero-based byte position within the line when known.</summary>
    public long? BytePositionInLine { get; }

    /// <summary>Gets the configured numeric limit when relevant.</summary>
    public long? Limit { get; }

    /// <summary>Gets the observed numeric count when safe and relevant.</summary>
    public long? Observed { get; }

    internal static JsonReadDiagnostic Create(
        string ruleId,
        long? recordOrdinal = null,
        long? byteOffset = null,
        long? lineNumber = null,
        long? bytePositionInLine = null,
        long? limit = null,
        long? observed = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleId);
        RequireNullableMinimum(recordOrdinal, 1, nameof(recordOrdinal));
        RequireNullableMinimum(byteOffset, 0, nameof(byteOffset));
        RequireNullableMinimum(lineNumber, 1, nameof(lineNumber));
        RequireNullableMinimum(bytePositionInLine, 0, nameof(bytePositionInLine));
        RequireNullableMinimum(limit, 1, nameof(limit));
        RequireNullableMinimum(observed, 0, nameof(observed));
        return new JsonReadDiagnostic(
            ruleId,
            recordOrdinal,
            byteOffset,
            lineNumber,
            bytePositionInLine,
            limit,
            observed);
    }

    private static void RequireNullableMinimum(long? value, long minimum, string name)
    {
        if (value < minimum)
            throw new ArgumentOutOfRangeException(name);
    }
}

/// <summary>Reports a deterministic failure while reading strict JSON input.</summary>
public sealed class JsonReadException : Exception
{
    internal JsonReadException(JsonReadDiagnostic diagnostic)
        : base((diagnostic ?? throw new ArgumentNullException(nameof(diagnostic))).Message)
    {
        Diagnostic = diagnostic;
    }

    /// <summary>Gets the structured, value-free read diagnostic.</summary>
    public JsonReadDiagnostic Diagnostic { get; }
}
