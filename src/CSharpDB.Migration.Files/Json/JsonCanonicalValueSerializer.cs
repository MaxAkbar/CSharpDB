using System.Buffers;
using System.Text;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Serializes logical JSON values under
/// <see cref="JsonInputContracts.CanonicalNestedJsonVersion"/>.
/// </summary>
public static class JsonCanonicalValueSerializer
{
    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    /// <summary>Returns deterministic canonical UTF-8 bytes for one value.</summary>
    public static byte[] SerializeToUtf8Bytes(JsonLogicalValue value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var writer = new ArrayBufferWriter<byte>();
        Write(writer, value);
        return writer.WrittenSpan.ToArray();
    }

    /// <summary>
    /// Writes one value in encounter property order, retaining exact number
    /// lexemes and using minimal JSON string escaping.
    /// </summary>
    public static void Write(IBufferWriter<byte> destination, JsonLogicalValue value)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(value);
        WriteValue(destination, value);
    }

    private static void WriteValue(IBufferWriter<byte> destination, JsonLogicalValue value)
    {
        switch (value.Kind)
        {
            case JsonLogicalValueKind.Null:
                WriteAscii(destination, "null");
                return;

            case JsonLogicalValueKind.Boolean:
                WriteAscii(destination, value.BooleanValue ? "true" : "false");
                return;

            case JsonLogicalValueKind.String:
                WriteString(destination, value.StringValue);
                return;

            case JsonLogicalValueKind.Number:
                WriteAscii(destination, value.NumberLexeme);
                return;

            case JsonLogicalValueKind.Object:
                WriteByte(destination, (byte)'{');
                for (int index = 0; index < value.Properties.Count; index++)
                {
                    if (index != 0)
                        WriteByte(destination, (byte)',');
                    JsonLogicalProperty property = value.Properties[index];
                    WriteString(destination, property.Name);
                    WriteByte(destination, (byte)':');
                    WriteValue(destination, property.Value);
                }
                WriteByte(destination, (byte)'}');
                return;

            case JsonLogicalValueKind.Array:
                WriteByte(destination, (byte)'[');
                for (int index = 0; index < value.Elements.Count; index++)
                {
                    if (index != 0)
                        WriteByte(destination, (byte)',');
                    WriteValue(destination, value.Elements[index]);
                }
                WriteByte(destination, (byte)']');
                return;

            default:
                throw new InvalidOperationException("The JSON logical value kind is invalid.");
        }
    }

    private static void WriteString(IBufferWriter<byte> destination, string value)
    {
        WriteByte(destination, (byte)'"');
        int segmentStart = 0;
        Span<byte> escaped = stackalloc byte[6];
        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            if (character > 0x1F && character is not '"' and not '\\')
                continue;

            WriteUtf8(destination, value.AsSpan(segmentStart, index - segmentStart));
            switch (character)
            {
                case '"':
                    WriteAscii(destination, "\\\"");
                    break;
                case '\\':
                    WriteAscii(destination, "\\\\");
                    break;
                case '\b':
                    WriteAscii(destination, "\\b");
                    break;
                case '\t':
                    WriteAscii(destination, "\\t");
                    break;
                case '\n':
                    WriteAscii(destination, "\\n");
                    break;
                case '\f':
                    WriteAscii(destination, "\\f");
                    break;
                case '\r':
                    WriteAscii(destination, "\\r");
                    break;
                default:
                    escaped[0] = (byte)'\\';
                    escaped[1] = (byte)'u';
                    escaped[2] = (byte)'0';
                    escaped[3] = (byte)'0';
                    escaped[4] = Hex(character >> 4);
                    escaped[5] = Hex(character);
                    WriteBytes(destination, escaped);
                    break;
            }

            segmentStart = index + 1;
        }

        WriteUtf8(destination, value.AsSpan(segmentStart));
        WriteByte(destination, (byte)'"');
    }

    private static byte Hex(int value) =>
        "0123456789abcdef"u8[value & 0x0F];

    private static void WriteUtf8(
        IBufferWriter<byte> destination,
        ReadOnlySpan<char> value)
    {
        if (value.IsEmpty)
            return;
        int byteCount = s_strictUtf8.GetByteCount(value);
        Span<byte> output = destination.GetSpan(byteCount);
        int written = s_strictUtf8.GetBytes(value, output);
        destination.Advance(written);
    }

    private static void WriteAscii(IBufferWriter<byte> destination, string value)
    {
        Span<byte> output = destination.GetSpan(value.Length);
        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            if (character > 0x7F)
                throw new InvalidOperationException("A canonical JSON token is not ASCII.");
            output[index] = (byte)character;
        }
        destination.Advance(value.Length);
    }

    private static void WriteBytes(
        IBufferWriter<byte> destination,
        ReadOnlySpan<byte> value)
    {
        value.CopyTo(destination.GetSpan(value.Length));
        destination.Advance(value.Length);
    }

    private static void WriteByte(IBufferWriter<byte> destination, byte value)
    {
        destination.GetSpan(1)[0] = value;
        destination.Advance(1);
    }
}
