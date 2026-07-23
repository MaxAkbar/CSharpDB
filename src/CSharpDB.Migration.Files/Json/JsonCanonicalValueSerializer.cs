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
    public static byte[] SerializeToUtf8Bytes(JsonLogicalValue value) =>
        SerializeToUtf8Bytes(value, CancellationToken.None);

    /// <summary>Returns deterministic canonical UTF-8 bytes for one value.</summary>
    public static byte[] SerializeToUtf8Bytes(
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var writer = new ArrayBufferWriter<byte>();
        Write(writer, value, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        byte[] result = writer.WrittenSpan.ToArray();
        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }

    /// <summary>Returns deterministic canonical JSON text for one value.</summary>
    public static string SerializeToString(JsonLogicalValue value) =>
        SerializeToString(value, CancellationToken.None);

    /// <summary>Returns deterministic canonical JSON text for one value.</summary>
    public static string SerializeToString(
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var writer = new ArrayBufferWriter<byte>();
        Write(writer, value, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        string result = s_strictUtf8.GetString(writer.WrittenSpan);
        cancellationToken.ThrowIfCancellationRequested();
        return result;
    }

    /// <summary>
    /// Writes one value in encounter property order, retaining exact number
    /// lexemes and using minimal JSON string escaping.
    /// </summary>
    public static void Write(
        IBufferWriter<byte> destination,
        JsonLogicalValue value) =>
        Write(destination, value, CancellationToken.None);

    /// <summary>
    /// Writes one value in encounter property order, retaining exact number
    /// lexemes and using minimal JSON string escaping.
    /// </summary>
    public static void Write(
        IBufferWriter<byte> destination,
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        WriteValue(destination, value, cancellationToken);
    }

    private static void WriteValue(
        IBufferWriter<byte> destination,
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        switch (value.Kind)
        {
            case JsonLogicalValueKind.Null:
                WriteAscii(destination, "null", cancellationToken);
                return;

            case JsonLogicalValueKind.Boolean:
                WriteAscii(
                    destination,
                    value.BooleanValue ? "true" : "false",
                    cancellationToken);
                return;

            case JsonLogicalValueKind.String:
                WriteString(
                    destination,
                    value.StringValue,
                    cancellationToken);
                return;

            case JsonLogicalValueKind.Number:
                WriteAscii(
                    destination,
                    value.NumberLexeme,
                    cancellationToken);
                return;

            case JsonLogicalValueKind.Object:
                WriteByte(destination, (byte)'{');
                for (int index = 0; index < value.Properties.Count; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (index != 0)
                        WriteByte(destination, (byte)',');
                    JsonLogicalProperty property = value.Properties[index];
                    WriteString(
                        destination,
                        property.Name,
                        cancellationToken);
                    WriteByte(destination, (byte)':');
                    WriteValue(
                        destination,
                        property.Value,
                        cancellationToken);
                }
                WriteByte(destination, (byte)'}');
                return;

            case JsonLogicalValueKind.Array:
                WriteByte(destination, (byte)'[');
                for (int index = 0; index < value.Elements.Count; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (index != 0)
                        WriteByte(destination, (byte)',');
                    WriteValue(
                        destination,
                        value.Elements[index],
                        cancellationToken);
                }
                WriteByte(destination, (byte)']');
                return;

            default:
                throw new InvalidOperationException("The JSON logical value kind is invalid.");
        }
    }

    private static void WriteString(
        IBufferWriter<byte> destination,
        string value,
        CancellationToken cancellationToken)
    {
        WriteByte(destination, (byte)'"');
        int segmentStart = 0;
        Span<byte> escaped = stackalloc byte[6];
        for (int index = 0; index < value.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            char character = value[index];
            if (character > 0x1F && character is not '"' and not '\\')
                continue;

            WriteUtf8(
                destination,
                value.AsSpan(segmentStart, index - segmentStart),
                cancellationToken);
            switch (character)
            {
                case '"':
                    WriteAscii(
                        destination,
                        "\\\"",
                        cancellationToken);
                    break;
                case '\\':
                    WriteAscii(
                        destination,
                        "\\\\",
                        cancellationToken);
                    break;
                case '\b':
                    WriteAscii(
                        destination,
                        "\\b",
                        cancellationToken);
                    break;
                case '\t':
                    WriteAscii(
                        destination,
                        "\\t",
                        cancellationToken);
                    break;
                case '\n':
                    WriteAscii(
                        destination,
                        "\\n",
                        cancellationToken);
                    break;
                case '\f':
                    WriteAscii(
                        destination,
                        "\\f",
                        cancellationToken);
                    break;
                case '\r':
                    WriteAscii(
                        destination,
                        "\\r",
                        cancellationToken);
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

        WriteUtf8(
            destination,
            value.AsSpan(segmentStart),
            cancellationToken);
        WriteByte(destination, (byte)'"');
    }

    private static byte Hex(int value) =>
        "0123456789abcdef"u8[value & 0x0F];

    private static void WriteUtf8(
        IBufferWriter<byte> destination,
        ReadOnlySpan<char> value,
        CancellationToken cancellationToken)
    {
        const int maximumCharactersPerChunk = 4 * 1024;
        while (!value.IsEmpty)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int characterCount = Math.Min(
                value.Length,
                maximumCharactersPerChunk);
            if (characterCount < value.Length &&
                char.IsHighSurrogate(value[characterCount - 1]) &&
                char.IsLowSurrogate(value[characterCount]))
            {
                characterCount--;
            }

            ReadOnlySpan<char> chunk = value[..characterCount];
            int byteCount = s_strictUtf8.GetByteCount(chunk);
            Span<byte> output = destination.GetSpan(byteCount);
            int written = s_strictUtf8.GetBytes(chunk, output);
            destination.Advance(written);
            value = value[characterCount..];
        }
    }

    private static void WriteAscii(
        IBufferWriter<byte> destination,
        string value,
        CancellationToken cancellationToken)
    {
        Span<byte> output = destination.GetSpan(value.Length);
        for (int index = 0; index < value.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
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
