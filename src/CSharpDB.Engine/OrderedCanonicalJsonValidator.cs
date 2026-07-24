using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CSharpDB.Engine;

/// <summary>
/// Validates the ordered canonical JSON representation used by migration
/// sources without taking a dependency on a migration-format assembly.
/// </summary>
internal static class OrderedCanonicalJsonValidator
{
    internal const int MaximumDocumentBytes = 64 * 1024 * 1024;

    private const int MaximumDepth = 128;
    private const int MaximumPropertiesPerObject = 16_384;
    private const int MaximumArrayElements = 65_536;
    private const int MaximumTotalNodes = 65_536;
    private const int MaximumPropertyNameBytes = 1024 * 1024;
    private const int MaximumStringBytes = 16 * 1024 * 1024;
    private const int MaximumNumberBytes = 16 * 1024 * 1024;

    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    internal static void Validate(
        ReadOnlySpan<byte> canonicalUtf8Json,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (canonicalUtf8Json.IsEmpty)
            throw InvalidJson();

        var canonical = new ArrayBufferWriter<byte>(canonicalUtf8Json.Length);
        var reader = new Utf8JsonReader(
            canonicalUtf8Json,
            isFinalBlock: true,
            new JsonReaderState(
                new JsonReaderOptions
                {
                    AllowTrailingCommas = false,
                    CommentHandling = JsonCommentHandling.Disallow,
                    MaxDepth = MaximumDepth,
                }));

        try
        {
            if (!reader.Read())
                throw InvalidJson();

            var budget = new ValidationBudget();
            WriteValue(
                ref reader,
                canonical,
                ref budget,
                cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.Read())
                throw InvalidJson();
        }
        catch (JsonException exception)
        {
            throw InvalidJson(exception);
        }
        catch (EncoderFallbackException exception)
        {
            throw InvalidJson(exception);
        }
        catch (InvalidOperationException exception)
        {
            throw InvalidJson(exception);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (!canonical.WrittenSpan.SequenceEqual(canonicalUtf8Json))
        {
            throw new InvalidDataException(
                "The document is valid JSON but is not in the required ordered canonical UTF-8 representation.");
        }
    }

    private static void WriteValue(
        ref Utf8JsonReader reader,
        IBufferWriter<byte> destination,
        ref ValidationBudget budget,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        budget.AddNode();
        switch (reader.TokenType)
        {
            case JsonTokenType.Null:
                WriteAscii(destination, "null");
                return;

            case JsonTokenType.True:
                WriteAscii(destination, "true");
                return;

            case JsonTokenType.False:
                WriteAscii(destination, "false");
                return;

            case JsonTokenType.String:
                string stringValue =
                    reader.GetString() ?? throw InvalidJson();
                RequireUtf8ByteLimit(
                    stringValue,
                    MaximumStringBytes,
                    "A canonical JSON string exceeds the absolute decoded UTF-8 limit.");
                WriteString(
                    destination,
                    stringValue,
                    cancellationToken);
                return;

            case JsonTokenType.Number:
                if (reader.ValueSpan.Length > MaximumNumberBytes)
                {
                    throw new InvalidDataException(
                        "A canonical JSON number exceeds the absolute lexeme limit.");
                }
                WriteBytes(destination, reader.ValueSpan);
                return;

            case JsonTokenType.StartObject:
                WriteObject(
                    ref reader,
                    destination,
                    ref budget,
                    cancellationToken);
                return;

            case JsonTokenType.StartArray:
                WriteArray(
                    ref reader,
                    destination,
                    ref budget,
                    cancellationToken);
                return;

            default:
                throw InvalidJson();
        }
    }

    private static void WriteObject(
        ref Utf8JsonReader reader,
        IBufferWriter<byte> destination,
        ref ValidationBudget budget,
        CancellationToken cancellationToken)
    {
        WriteByte(destination, (byte)'{');
        var names = new HashSet<string>(StringComparer.Ordinal);
        bool hasProperty = false;
        int propertyCount = 0;

        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                WriteByte(destination, (byte)'}');
                return;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw InvalidJson();

            propertyCount = checked(propertyCount + 1);
            if (propertyCount > MaximumPropertiesPerObject)
            {
                throw new InvalidDataException(
                    "A canonical JSON object exceeds the absolute property-count limit.");
            }
            string name = reader.GetString() ?? throw InvalidJson();
            RequireUtf8ByteLimit(
                name,
                MaximumPropertyNameBytes,
                "A canonical JSON property name exceeds the absolute decoded UTF-8 limit.");
            if (!names.Add(name))
            {
                throw new InvalidDataException(
                    "A canonical JSON object cannot contain duplicate decoded property names.");
            }

            if (hasProperty)
                WriteByte(destination, (byte)',');
            hasProperty = true;
            WriteString(destination, name, cancellationToken);
            WriteByte(destination, (byte)':');

            if (!reader.Read())
                throw InvalidJson();
            WriteValue(
                ref reader,
                destination,
                ref budget,
                cancellationToken);
        }

        throw InvalidJson();
    }

    private static void WriteArray(
        ref Utf8JsonReader reader,
        IBufferWriter<byte> destination,
        ref ValidationBudget budget,
        CancellationToken cancellationToken)
    {
        WriteByte(destination, (byte)'[');
        bool hasElement = false;
        int elementCount = 0;

        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (reader.TokenType == JsonTokenType.EndArray)
            {
                WriteByte(destination, (byte)']');
                return;
            }

            elementCount = checked(elementCount + 1);
            if (elementCount > MaximumArrayElements)
            {
                throw new InvalidDataException(
                    "A canonical JSON array exceeds the absolute element-count limit.");
            }
            if (hasElement)
                WriteByte(destination, (byte)',');
            hasElement = true;
            WriteValue(
                ref reader,
                destination,
                ref budget,
                cancellationToken);
        }

        throw InvalidJson();
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

        WriteUtf8(destination, value.AsSpan(segmentStart), cancellationToken);
        WriteByte(destination, (byte)'"');
    }

    private static byte Hex(int value) =>
        "0123456789abcdef"u8[value & 0x0F];

    private static void RequireUtf8ByteLimit(
        string value,
        int maximumBytes,
        string message)
    {
        if (s_strictUtf8.GetByteCount(value) > maximumBytes)
            throw new InvalidDataException(message);
    }

    private static void WriteUtf8(
        IBufferWriter<byte> destination,
        ReadOnlySpan<char> value,
        CancellationToken cancellationToken)
    {
        const int maximumCharactersPerChunk = 4 * 1024;
        while (!value.IsEmpty)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int characterCount = Math.Min(value.Length, maximumCharactersPerChunk);
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

    private static void WriteAscii(IBufferWriter<byte> destination, string value)
    {
        Span<byte> output = destination.GetSpan(value.Length);
        for (int index = 0; index < value.Length; index++)
            output[index] = checked((byte)value[index]);
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

    private static InvalidDataException InvalidJson(Exception? innerException = null) =>
        new("The document must contain exactly one valid JSON value encoded as strict UTF-8.", innerException);

    private struct ValidationBudget
    {
        private int nodes;

        internal void AddNode()
        {
            nodes = checked(nodes + 1);
            if (nodes > MaximumTotalNodes)
            {
                throw new InvalidDataException(
                    "A canonical JSON document exceeds the absolute node-count limit.");
            }
        }
    }
}
