using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Writes the package-v1 JSON subset without depending on runtime Unicode or
/// HTML encoder tables. Object order comes from explicitly ordered DTO
/// properties; numbers have already been normalized by typed serialization.
/// </summary>
internal static class JsonSnapshotPackageCanonicalJson
{
    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    internal static byte[] Serialize(JsonElement element)
    {
        var writer = new ArrayBufferWriter<byte>();
        WriteElement(writer, element);
        return writer.WrittenSpan.ToArray();
    }

    private static void WriteElement(
        ArrayBufferWriter<byte> writer,
        JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                WriteByte(writer, (byte)'{');
                bool firstProperty = true;
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (!firstProperty)
                        WriteByte(writer, (byte)',');
                    firstProperty = false;
                    WriteString(writer, property.Name);
                    WriteByte(writer, (byte)':');
                    WriteElement(writer, property.Value);
                }
                WriteByte(writer, (byte)'}');
                break;

            case JsonValueKind.Array:
                WriteByte(writer, (byte)'[');
                bool firstItem = true;
                foreach (JsonElement item in element.EnumerateArray())
                {
                    if (!firstItem)
                        WriteByte(writer, (byte)',');
                    firstItem = false;
                    WriteElement(writer, item);
                }
                WriteByte(writer, (byte)']');
                break;

            case JsonValueKind.String:
                string value;
                try
                {
                    value = element.GetString() ?? throw new InvalidDataException(
                        "The JSON package manifest contains an invalid JSON string.");
                }
                catch (InvalidOperationException exception)
                {
                    throw new InvalidDataException(
                        "The JSON package manifest contains invalid UTF-16 text.",
                        exception);
                }
                WriteString(writer, value);
                break;

            case JsonValueKind.Number:
                WriteAscii(writer, element.GetRawText());
                break;

            case JsonValueKind.True:
                WriteAscii(writer, "true");
                break;

            case JsonValueKind.False:
                WriteAscii(writer, "false");
                break;

            case JsonValueKind.Null:
                WriteAscii(writer, "null");
                break;

            default:
                throw new InvalidDataException(
                    "The JSON package manifest contains an unsupported JSON token.");
        }
    }

    private static void WriteString(ArrayBufferWriter<byte> writer, string value)
    {
        WriteByte(writer, (byte)'"');
        int segmentStart = 0;
        Span<char> unicodeEscape =
            stackalloc char[6] { '\\', 'u', '0', '0', '0', '0' };
        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            if (character > 0x1F && character is not '"' and not '\\')
                continue;

            WriteUtf8(writer, value.AsSpan(segmentStart, index - segmentStart));
            switch (character)
            {
                case '"':
                    WriteAscii(writer, "\\\"");
                    break;
                case '\\':
                    WriteAscii(writer, "\\\\");
                    break;
                case '\b':
                    WriteAscii(writer, "\\b");
                    break;
                case '\t':
                    WriteAscii(writer, "\\t");
                    break;
                case '\n':
                    WriteAscii(writer, "\\n");
                    break;
                case '\f':
                    WriteAscii(writer, "\\f");
                    break;
                case '\r':
                    WriteAscii(writer, "\\r");
                    break;
                default:
                    unicodeEscape[4] = Hex(character >> 4);
                    unicodeEscape[5] = Hex(character);
                    WriteAscii(writer, unicodeEscape);
                    break;
            }

            segmentStart = index + 1;
        }

        WriteUtf8(writer, value.AsSpan(segmentStart));
        WriteByte(writer, (byte)'"');
    }

    private static char Hex(int value) =>
        "0123456789abcdef"[value & 0xF];

    private static void WriteUtf8(
        ArrayBufferWriter<byte> writer,
        ReadOnlySpan<char> value)
    {
        if (value.IsEmpty)
            return;
        try
        {
            int byteCount = s_strictUtf8.GetByteCount(value);
            Span<byte> destination = writer.GetSpan(byteCount);
            int written = s_strictUtf8.GetBytes(value, destination);
            writer.Advance(written);
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidDataException(
                "The JSON package manifest contains invalid UTF-16 text.",
                exception);
        }
    }

    private static void WriteAscii(
        ArrayBufferWriter<byte> writer,
        string value) =>
        WriteAscii(writer, value.AsSpan());

    private static void WriteAscii(
        ArrayBufferWriter<byte> writer,
        ReadOnlySpan<char> value)
    {
        Span<byte> destination = writer.GetSpan(value.Length);
        for (int index = 0; index < value.Length; index++)
        {
            char character = value[index];
            if (character > 0x7F)
            {
                throw new InvalidDataException(
                    "The canonical JSON token is not ASCII.");
            }
            destination[index] = (byte)character;
        }
        writer.Advance(value.Length);
    }

    private static void WriteByte(
        ArrayBufferWriter<byte> writer,
        byte value)
    {
        writer.GetSpan(1)[0] = value;
        writer.Advance(1);
    }
}
