using System.Text.Json;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Helpers;

/// <summary>
/// Converts <see cref="JsonElement"/> values that arrive from System.Text.Json deserialization
/// into CLR primitives that the CSharpDB engine understands.
/// </summary>
public static class JsonHelper
{
    public static object? CoerceJsonElement(object? value)
    {
        if (value is JsonElement { ValueKind: JsonValueKind.Object } bitElement &&
            TryDecodeBitStringEnvelope(bitElement, out SqlBitString? bitString))
        {
            return bitString;
        }

        if (value is JsonElement { ValueKind: JsonValueKind.Object } objectElement &&
            TryDecodeBinaryEnvelope(objectElement, out byte[]? binary))
        {
            return binary;
        }

        return value switch
        {
            JsonElement { ValueKind: JsonValueKind.Null } => null,
            JsonElement { ValueKind: JsonValueKind.String } e => e.GetString(),
            JsonElement { ValueKind: JsonValueKind.Number } e when e.TryGetInt64(out long l) => l,
            JsonElement { ValueKind: JsonValueKind.Number } e when e.TryGetDecimal(out decimal d) => d,
            JsonElement { ValueKind: JsonValueKind.Number } e => e.GetDouble(),
            JsonElement { ValueKind: JsonValueKind.True } => 1L,
            JsonElement { ValueKind: JsonValueKind.False } => 0L,
            _ => value,
        };
    }

    private static bool TryDecodeBinaryEnvelope(JsonElement value, out byte[]? binary)
    {
        binary = null;
        if (!value.TryGetProperty("$csharpdb", out JsonElement marker) ||
            marker.ValueKind != JsonValueKind.String ||
            !string.Equals(marker.GetString(), "binary-v1", StringComparison.Ordinal))
        {
            return false;
        }

        if (!value.TryGetProperty("base64", out JsonElement payload) ||
            payload.ValueKind != JsonValueKind.String)
        {
            throw new BadHttpRequestException(
                "A tagged CSharpDB binary value requires a base64 string.",
                StatusCodes.Status400BadRequest);
        }

        try
        {
            binary = Convert.FromBase64String(payload.GetString() ?? string.Empty);
            return true;
        }
        catch (FormatException)
        {
            throw new BadHttpRequestException(
                "A tagged CSharpDB binary value contains invalid base64 data.",
                StatusCodes.Status400BadRequest);
        }
    }

    private static bool TryDecodeBitStringEnvelope(
        JsonElement value,
        out SqlBitString? bitString)
    {
        bitString = null;
        if (!value.TryGetProperty("$csharpdb", out JsonElement marker) ||
            marker.ValueKind != JsonValueKind.String ||
            !string.Equals(marker.GetString(), "bit-string-v1", StringComparison.Ordinal))
        {
            return false;
        }

        if (!value.TryGetProperty("base64", out JsonElement payload) ||
            payload.ValueKind != JsonValueKind.String ||
            !value.TryGetProperty("bitLength", out JsonElement length) ||
            !length.TryGetInt32(out int bitLength))
        {
            throw new BadHttpRequestException(
                "A tagged CSharpDB bit-string value requires base64 and bitLength fields.",
                StatusCodes.Status400BadRequest);
        }

        try
        {
            bitString = new SqlBitString(
                Convert.FromBase64String(payload.GetString() ?? string.Empty),
                bitLength);
            return true;
        }
        catch (Exception error) when (error is FormatException or ArgumentException or OverflowException)
        {
            throw new BadHttpRequestException(
                "A tagged CSharpDB bit-string value is invalid.",
                StatusCodes.Status400BadRequest);
        }
    }

    /// <summary>
    /// Coerces all values in a dictionary from JsonElement to CLR types.
    /// </summary>
    public static Dictionary<string, object?> CoerceDictionary(Dictionary<string, object?> dict)
    {
        var result = new Dictionary<string, object?>(dict.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, val) in dict)
            result[key] = CoerceJsonElement(val);
        return result;
    }

    public static Dictionary<string, object?> EncodeDictionary(
        IReadOnlyDictionary<string, object?> values)
    {
        var result = new Dictionary<string, object?>(values.Count, StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in values)
            result[key] = EncodeTransportValue(value);
        return result;
    }

    /// <summary>
    /// Converts positional row data (object?[]) into named dictionaries using column names.
    /// </summary>
    public static List<Dictionary<string, object?>> RowsToNamedDictionaries(
        string[] columnNames, IReadOnlyList<object?[]> rows)
    {
        var result = new List<Dictionary<string, object?>>(rows.Count);
        foreach (var row in rows)
        {
            var dict = new Dictionary<string, object?>(columnNames.Length);
            for (int i = 0; i < columnNames.Length; i++)
            {
                dict[columnNames[i]] =
                    EncodeTransportValue(i < row.Length ? row[i] : null);
            }
            result.Add(dict);
        }
        return result;
    }


    private static object? EncodeTransportValue(object? value)
    {
        if (value is not SqlBitString bitString)
            return value;

        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["$csharpdb"] = "bit-string-v1",
            ["base64"] = Convert.ToBase64String(bitString.PackedBytes.Span),
            ["bitLength"] = bitString.BitLength,
        };
    }

    public static object? EncodeValue(object? value) => EncodeTransportValue(value);
}
