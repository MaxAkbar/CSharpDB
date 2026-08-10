using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

/// <summary>
/// A validated W3C trace identifier. Arbitrary request identifiers and
/// caller-provided strings must not be converted into this type.
/// </summary>
[JsonConverter(typeof(DiagnosticsTraceIdJsonConverter))]
public sealed record DiagnosticsTraceId
{
    public const int HexLength = 32;

    public DiagnosticsTraceId(string value)
    {
        if (!IsValid(value))
        {
            throw new ArgumentException(
                "A non-zero 32-character lowercase hexadecimal W3C trace id is required.",
                nameof(value));
        }

        Value = value;
    }

    public string Value { get; }

    public static DiagnosticsTraceId FromActivityTraceId(ActivityTraceId traceId)
    {
        if (traceId == default)
            throw new ArgumentException("A non-default W3C trace id is required.", nameof(traceId));

        return new DiagnosticsTraceId(traceId.ToHexString());
    }

    public static bool IsValid(string? value)
    {
        if (value is null || value.Length != HexLength)
            return false;

        bool hasNonZeroDigit = false;
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }

            hasNonZeroDigit |= character != '0';
        }

        return hasNonZeroDigit;
    }

    public override string ToString() => Value;
}

public sealed class DiagnosticsTraceIdJsonConverter : JsonConverter<DiagnosticsTraceId>
{
    public override DiagnosticsTraceId Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
        => new(reader.GetString() ?? throw new JsonException("A trace id is required."));

    public override void Write(
        Utf8JsonWriter writer,
        DiagnosticsTraceId value,
        JsonSerializerOptions options)
        => writer.WriteStringValue(value.Value);
}
