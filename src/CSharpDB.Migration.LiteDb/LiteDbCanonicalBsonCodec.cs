using System.Buffers;
using System.Globalization;
using System.Text;
using CSharpDB.Migration;
using LiteDB;

namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// Deterministic, type-preserving tagged JSON for LiteDB BSON documents and
/// identifiers.
/// </summary>
public static class LiteDbCanonicalBsonCodec
{
    public const string EncodingContract =
        MigrationLiteDbDocumentCollectionContract.DocumentEncoding;

    public const string TypedKeyContract =
        MigrationLiteDbDocumentCollectionContract.KeyContract;

    public const string TypedKeyPrefix =
        MigrationLiteDbDocumentCollectionContract.TypedKeyPrefix;

    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string EncodeDocument(BsonDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);
        return Encode(document, requireScalar: false, LiteDbInspectionLimits.Default);
    }

    public static string EncodeTypedKey(BsonValue id)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (id.IsNull)
        {
            throw new LiteDbMigrationException(
                "LiteDB document _id values cannot be null.");
        }

        string tagged = Encode(id, requireScalar: true, LiteDbInspectionLimits.Default);
        return CreateTypedKey(tagged, LiteDbInspectionLimits.Default);
    }

    internal static string EncodeDocument(
        BsonDocument document,
        LiteDbInspectionLimits limits)
    {
        ArgumentNullException.ThrowIfNull(document);
        return Encode(document, requireScalar: false, limits);
    }

    internal static string EncodeTypedKey(
        BsonValue id,
        LiteDbInspectionLimits limits)
    {
        ArgumentNullException.ThrowIfNull(id);
        if (id.IsNull)
        {
            throw new LiteDbMigrationException(
                "LiteDB document _id values cannot be null.");
        }

        string tagged = Encode(id, requireScalar: true, limits);
        return CreateTypedKey(tagged, limits);
    }

    private static string Encode(
        BsonValue value,
        bool requireScalar,
        LiteDbInspectionLimits limits)
    {
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        if (requireScalar && (value.IsDocument || value.IsArray))
        {
            throw new LiteDbMigrationException(
                "LiteDB document _id values must be scalar BSON values.");
        }

        var output = new BoundedByteBufferWriter(limits.MaxCanonicalOutputBytes);
        var writer = new CanonicalJsonWriter(output);
        var state = new EncodingState(limits);
        WriteTaggedValue(
            writer,
            value,
            state,
            depth: 0,
            pathBytes: 0,
            jsonContainerDepth: 0);
        writer.Complete();

        return StrictUtf8.GetString(output.WrittenSpan);
    }

    private static void WriteTaggedValue(
        CanonicalJsonWriter writer,
        BsonValue value,
        EncodingState state,
        int depth,
        int pathBytes,
        int jsonContainerDepth)
    {
        if (depth > state.Limits.MaxDepth)
            throw LimitExceeded("nesting depth");

        state.AddNodes(2);
        int wrapperDepth = state.EnterContainer(jsonContainerDepth);
        writer.WriteStartObject();
        writer.WriteString(
            MigrationLiteDbDocumentCollectionContract.TaggedBsonTypeProperty,
            GetTypeLabel(value));

        switch (value.Type)
        {
            case BsonType.MinValue:
            case BsonType.Null:
            case BsonType.MaxValue:
                break;
            case BsonType.Int32:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsInt32.ToString(CultureInfo.InvariantCulture));
                break;
            case BsonType.Int64:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsInt64.ToString(CultureInfo.InvariantCulture));
                break;
            case BsonType.Double:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    unchecked((ulong)BitConverter.DoubleToInt64Bits(value.AsDouble))
                        .ToString("X16", CultureInfo.InvariantCulture));
                break;
            case BsonType.Decimal:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    DecimalBits(value.AsDecimal));
                break;
            case BsonType.String:
                state.AddNodes(1);
                EnsureUtf8Bound(value.AsString, state.Limits.MaxStringBytes, "string");
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsString);
                break;
            case BsonType.Document:
                WriteDocument(
                    writer,
                    value.AsDocument,
                    state,
                    depth,
                    pathBytes,
                    wrapperDepth);
                break;
            case BsonType.Array:
                WriteArray(
                    writer,
                    value.AsArray,
                    state,
                    depth,
                    pathBytes,
                    wrapperDepth);
                break;
            case BsonType.Binary:
                state.AddNodes(1);
                if (value.AsBinary.Length > state.Limits.MaxBinaryBytes)
                    throw LimitExceeded("binary value");
                writer.WriteBase64String(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsBinary);
                break;
            case BsonType.ObjectId:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsObjectId.ToString());
                break;
            case BsonType.Guid:
                state.AddNodes(1);
                writer.WriteString(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsGuid.ToString("D", CultureInfo.InvariantCulture));
                break;
            case BsonType.Boolean:
                state.AddNodes(1);
                writer.WriteBoolean(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty,
                    value.AsBoolean);
                break;
            case BsonType.DateTime:
                state.AddNodes(3);
                _ = state.EnterContainer(wrapperDepth);
                writer.WriteStartObject(
                    MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty);
                writer.WriteString(
                    "ticks",
                    value.AsDateTime.Ticks.ToString(CultureInfo.InvariantCulture));
                writer.WriteString(
                    "kind",
                    ((int)value.AsDateTime.Kind).ToString(CultureInfo.InvariantCulture));
                writer.WriteEndObject();
                break;
            default:
                throw new LiteDbMigrationException(
                    $"LiteDB BSON type '{value.Type}' is not supported by {EncodingContract}.");
        }

        writer.WriteEndObject();
    }

    private static void WriteDocument(
        CanonicalJsonWriter writer,
        BsonDocument document,
        EncodingState state,
        int depth,
        int pathBytes,
        int jsonContainerDepth)
    {
        state.AddFields(document.Count);
        KeyValuePair<string, BsonValue>[] fields = document
            .OrderBy(static field => field.Key, StringComparer.Ordinal)
            .ToArray();

        state.AddNodes(1);
        int arrayDepth = state.EnterContainer(jsonContainerDepth);
        writer.WriteStartArray(
            MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty);
        foreach (KeyValuePair<string, BsonValue> field in fields)
        {
            int nameBytes = EnsureUtf8Bound(
                field.Key,
                state.Limits.MaxPropertyNameBytes,
                "property name");
            int nextPathBytes = checked(pathBytes + 3 + Base64UrlEncodedLength(nameBytes));
            if (nextPathBytes > state.Limits.MaxPathBytes)
                throw LimitExceeded("field path");

            state.AddNodes(2);
            int entryDepth = state.EnterContainer(arrayDepth);
            writer.WriteStartObject();
            writer.WriteString(
                MigrationLiteDbDocumentCollectionContract.DocumentEntryNameProperty,
                field.Key);
            writer.WritePropertyName(
                MigrationLiteDbDocumentCollectionContract.DocumentEntryValueProperty);
            WriteTaggedValue(
                writer,
                field.Value,
                state,
                depth + 1,
                nextPathBytes,
                entryDepth);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
    }

    private static void WriteArray(
        CanonicalJsonWriter writer,
        BsonArray array,
        EncodingState state,
        int depth,
        int pathBytes,
        int jsonContainerDepth)
    {
        state.AddFields(array.Count);
        int nextPathBytes = checked(pathBytes + 2);
        if (nextPathBytes > state.Limits.MaxPathBytes)
            throw LimitExceeded("field path");

        state.AddNodes(1);
        int arrayDepth = state.EnterContainer(jsonContainerDepth);
        writer.WriteStartArray(
            MigrationLiteDbDocumentCollectionContract.TaggedBsonValueProperty);
        foreach (BsonValue item in array)
        {
            WriteTaggedValue(
                writer,
                item,
                state,
                depth + 1,
                nextPathBytes,
                arrayDepth);
        }
        writer.WriteEndArray();
    }

    internal static string GetTypeLabel(BsonValue value) =>
        value.Type switch
        {
            BsonType.MinValue => "min",
            BsonType.Null => "null",
            BsonType.Int32 => "int32",
            BsonType.Int64 => "int64",
            BsonType.Double => "double",
            BsonType.Decimal => "decimal",
            BsonType.String => "string",
            BsonType.Document => "document",
            BsonType.Array => "array",
            BsonType.Binary => "binary",
            BsonType.ObjectId => "objectId",
            BsonType.Guid => "guid",
            BsonType.Boolean => "boolean",
            BsonType.DateTime => "dateTime",
            BsonType.MaxValue => "max",
            _ => throw new LiteDbMigrationException(
                $"LiteDB BSON type '{value.Type}' is not supported by {EncodingContract}."),
        };

    private static string DecimalBits(decimal value) =>
        string.Join(
            "-",
            decimal.GetBits(value)
                .Select(static part =>
                    unchecked((uint)part).ToString("X8", CultureInfo.InvariantCulture)));

    private static int EnsureUtf8Bound(string value, int maximum, string subject)
    {
        int byteCount;
        try
        {
            byteCount = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new LiteDbMigrationException(
                $"LiteDB {subject} is not valid Unicode.",
                exception);
        }

        if (byteCount > maximum)
            throw LimitExceeded(subject);
        return byteCount;
    }

    private static int Base64UrlEncodedLength(int byteCount) =>
        checked(((byteCount + 2) / 3) * 4);

    private static string Base64UrlEncode(ReadOnlySpan<byte> bytes) =>
        Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private static string CreateTypedKey(
        string tagged,
        LiteDbInspectionLimits limits)
    {
        byte[] bytes = StrictUtf8.GetBytes(tagged);
        int encodedLength = checked(
            TypedKeyPrefix.Length + Base64UrlEncodedLength(bytes.Length));
        if (encodedLength > limits.MaxTypedKeyBytes)
            throw LimitExceeded("typed _id key");
        return TypedKeyPrefix + Base64UrlEncode(bytes);
    }

    private static LiteDbMigrationException LimitExceeded(string subject) =>
        new($"The LiteDB {subject} exceeds the fixed inspection limit.");

    private sealed class EncodingState(LiteDbInspectionLimits limits)
    {
        private int fields;
        private int nodes;

        public LiteDbInspectionLimits Limits { get; } = limits;

        public void AddFields(int count)
        {
            try
            {
                fields = checked(fields + count);
            }
            catch (OverflowException exception)
            {
                throw new LiteDbMigrationException(
                    "The LiteDB document field count exceeds the fixed inspection limit.",
                    exception);
            }

            if (fields > Limits.MaxFieldsPerDocument)
                throw LimitExceeded("document field count");
        }

        public void AddNodes(int count)
        {
            try
            {
                nodes = checked(nodes + count);
            }
            catch (OverflowException exception)
            {
                throw new LiteDbMigrationException(
                    "The LiteDB tagged JSON node count exceeds the fixed target limit.",
                    exception);
            }

            if (nodes > Limits.MaxJsonNodes)
                throw LimitExceeded("tagged JSON node count");
        }

        public int EnterContainer(int parentDepth)
        {
            int depth = checked(parentDepth + 1);
            if (depth > Limits.MaxJsonContainerDepth)
                throw LimitExceeded("tagged JSON container depth");
            return depth;
        }
    }

    private sealed class CanonicalJsonWriter(BoundedByteBufferWriter output)
    {
        private readonly List<ContainerState> containers = [];
        private bool rootWritten;

        public void WriteStartObject()
        {
            BeforeValue();
            WriteByte((byte)'{');
            containers.Add(new ContainerState(isObject: true));
        }

        public void WriteStartObject(string propertyName)
        {
            WritePropertyName(propertyName);
            WriteStartObject();
        }

        public void WriteEndObject()
        {
            ContainerState current = Current(isObject: true);
            if (current.PropertyPending)
                throw new InvalidOperationException("A JSON property is missing its value.");
            containers.RemoveAt(containers.Count - 1);
            WriteByte((byte)'}');
        }

        public void WriteStartArray(string propertyName)
        {
            WritePropertyName(propertyName);
            BeforeValue();
            WriteByte((byte)'[');
            containers.Add(new ContainerState(isObject: false));
        }

        public void WriteEndArray()
        {
            _ = Current(isObject: false);
            containers.RemoveAt(containers.Count - 1);
            WriteByte((byte)']');
        }

        public void WritePropertyName(string name)
        {
            ContainerState current = Current(isObject: true);
            if (current.PropertyPending)
                throw new InvalidOperationException("A JSON property is missing its value.");
            if (current.ValueCount > 0)
                WriteByte((byte)',');
            current.ValueCount++;
            WriteCanonicalString(name);
            WriteByte((byte)':');
            current.PropertyPending = true;
        }

        public void WriteString(string propertyName, string value)
        {
            WritePropertyName(propertyName);
            BeforeValue();
            WriteCanonicalString(value);
        }

        public void WriteBoolean(string propertyName, bool value)
        {
            WritePropertyName(propertyName);
            BeforeValue();
            WriteAscii(value ? "true" : "false");
        }

        public void WriteBase64String(string propertyName, byte[] value) =>
            WriteString(propertyName, Convert.ToBase64String(value));

        public void Complete()
        {
            if (!rootWritten || containers.Count != 0)
                throw new InvalidOperationException("The canonical JSON value is incomplete.");
        }

        private void BeforeValue()
        {
            if (containers.Count == 0)
            {
                if (rootWritten)
                    throw new InvalidOperationException("Only one root JSON value is allowed.");
                rootWritten = true;
                return;
            }

            ContainerState current = containers[^1];
            if (current.IsObject)
            {
                if (!current.PropertyPending)
                    throw new InvalidOperationException("A JSON object value requires a property name.");
                current.PropertyPending = false;
                return;
            }

            if (current.ValueCount > 0)
                WriteByte((byte)',');
            current.ValueCount++;
        }

        private ContainerState Current(bool isObject)
        {
            if (containers.Count == 0 || containers[^1].IsObject != isObject)
                throw new InvalidOperationException("The canonical JSON container is unbalanced.");
            return containers[^1];
        }

        private void WriteCanonicalString(string value)
        {
            WriteByte((byte)'"');
            int segmentStart = 0;
            Span<byte> escaped = stackalloc byte[6];
            for (int index = 0; index < value.Length; index++)
            {
                char character = value[index];
                if (character > 0x1F && character is not '"' and not '\\')
                    continue;

                WriteUtf8(value.AsSpan(segmentStart, index - segmentStart));
                switch (character)
                {
                    case '"':
                        WriteAscii("\\\"");
                        break;
                    case '\\':
                        WriteAscii("\\\\");
                        break;
                    case '\b':
                        WriteAscii("\\b");
                        break;
                    case '\t':
                        WriteAscii("\\t");
                        break;
                    case '\n':
                        WriteAscii("\\n");
                        break;
                    case '\f':
                        WriteAscii("\\f");
                        break;
                    case '\r':
                        WriteAscii("\\r");
                        break;
                    default:
                        escaped[0] = (byte)'\\';
                        escaped[1] = (byte)'u';
                        escaped[2] = (byte)'0';
                        escaped[3] = (byte)'0';
                        escaped[4] = Hex(character >> 4);
                        escaped[5] = Hex(character);
                        WriteBytes(escaped);
                        break;
                }
                segmentStart = index + 1;
            }
            WriteUtf8(value.AsSpan(segmentStart));
            WriteByte((byte)'"');
        }

        private void WriteUtf8(ReadOnlySpan<char> value)
        {
            while (!value.IsEmpty)
            {
                int characterCount = Math.Min(value.Length, 4 * 1024);
                if (characterCount < value.Length &&
                    char.IsHighSurrogate(value[characterCount - 1]) &&
                    char.IsLowSurrogate(value[characterCount]))
                {
                    characterCount--;
                }

                ReadOnlySpan<char> chunk = value[..characterCount];
                int byteCount = StrictUtf8.GetByteCount(chunk);
                Span<byte> destination = output.GetSpan(byteCount);
                int written = StrictUtf8.GetBytes(chunk, destination);
                output.Advance(written);
                value = value[characterCount..];
            }
        }

        private void WriteAscii(string value)
        {
            Span<byte> destination = output.GetSpan(value.Length);
            for (int index = 0; index < value.Length; index++)
                destination[index] = checked((byte)value[index]);
            output.Advance(value.Length);
        }

        private void WriteBytes(ReadOnlySpan<byte> value)
        {
            value.CopyTo(output.GetSpan(value.Length));
            output.Advance(value.Length);
        }

        private void WriteByte(byte value)
        {
            output.GetSpan(1)[0] = value;
            output.Advance(1);
        }

        private static byte Hex(int value) =>
            "0123456789abcdef"u8[value & 0x0F];

        private sealed class ContainerState(bool isObject)
        {
            public bool IsObject { get; } = isObject;

            public int ValueCount { get; set; }

            public bool PropertyPending { get; set; }
        }
    }

    private sealed class BoundedByteBufferWriter(int capacity) : IBufferWriter<byte>
    {
        private byte[] buffer = new byte[Math.Min(capacity, 256)];
        private int written;

        public ReadOnlySpan<byte> WrittenSpan => buffer.AsSpan(0, written);

        public void Advance(int count)
        {
            if (count < 0 || written > capacity - count)
                throw LimitExceeded("canonical BSON output");
            written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return buffer.AsMemory(written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return buffer.AsSpan(written);
        }

        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint));
            if (sizeHint == 0)
                sizeHint = 1;
            if (written > capacity - sizeHint)
                throw LimitExceeded("canonical BSON output");

            int required = written + sizeHint;
            if (required <= buffer.Length)
                return;

            int doubled = buffer.Length > capacity / 2 ? capacity : buffer.Length * 2;
            Array.Resize(ref buffer, Math.Max(required, doubled));
        }
    }
}
