using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

internal static class CollectionBinaryDocumentCodec
{
    private const byte NullTag = 0;
    private const byte StringTag = 1;
    private const byte IntegerTag = 2;
    private const byte FalseTag = 3;
    private const byte TrueTag = 4;
    private const byte DoubleTag = 5;
    private const byte DecimalTag = 6;
    private const byte ObjectTag = 7;
    private const byte ArrayTag = 8;

    private delegate object? MemberGetter(object instance);
    private delegate void MemberSetter(object instance, object? value);
    private delegate object ObjectFactory(object?[] args);

    private enum MemberValueKind : byte
    {
        String,
        Boolean,
        Byte,
        SByte,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Single,
        Double,
        Decimal,
        Guid,
        DateOnly,
        TimeOnly,
        Array,
        Enum,
        Object,
    }

    private enum CollectionContainerKind : byte
    {
        None,
        Array,
        List,
        Interface,
    }

    [RequiresUnreferencedCode("Binary collection encoding relies on reflection over document types.")]
    [RequiresDynamicCode("Binary collection encoding relies on runtime reflection over document types.")]
    public static byte[] Encode<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        T>(T document)
    {
        ArgumentNullException.ThrowIfNull(document);

        var writer = new ArrayBufferWriter<byte>();
        WriteObject(writer, document!, TypeMetadataCache<T>.Metadata);
        return writer.WrittenSpan.ToArray();
    }

    [RequiresUnreferencedCode("Binary collection decoding relies on reflection over document types.")]
    [RequiresDynamicCode("Binary collection decoding relies on runtime reflection over document types.")]
    public static T Decode<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        T>(ReadOnlySpan<byte> payload)
    {
        int position = 0;
        object instance = ReadObject(payload, ref position, TypeMetadataCache<T>.Metadata);
        if (position != payload.Length)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid binary collection payload length.");

        return (T)instance;
    }

    public static bool IsValidDocument(ReadOnlySpan<byte> payload)
    {
        try
        {
            int position = 0;
            if (!TrySkipObject(payload, ref position))
                return false;

            return position == payload.Length;
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            return false;
        }
    }

    public static bool TryReadValue(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out DbValue value)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadValueFromObject(payload, 0, pathSegmentsUtf8, out value);
    }

    public static bool TryReadInt64(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out long value)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadInt64FromObject(payload, 0, pathSegmentsUtf8, out value);
    }

    public static bool TryReadString(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out string? value)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadStringFromObject(payload, 0, pathSegmentsUtf8, out value);
    }

    public static bool TryReadBoolean(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out bool value)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadBooleanFromObject(payload, 0, pathSegmentsUtf8, out value);
    }

    public static bool TryReadDecimal(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out decimal value)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadDecimalFromObject(payload, 0, pathSegmentsUtf8, out value);
    }

    public static bool TryReadArrayValues(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, List<DbValue> values)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadArrayValues(payload, pathSegmentsUtf8, CreateDefaultArrayFlags(pathSegmentsUtf8.Length), values);
    }

    public static bool TryReadArrayValues(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, bool[] pathArraySegments, List<DbValue> values)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        ArgumentNullException.ThrowIfNull(pathArraySegments);
        ArgumentNullException.ThrowIfNull(values);
        return TryReadArrayValuesFromObject(payload, 0, pathSegmentsUtf8, pathArraySegments, values);
    }

    public static bool TryArrayContainsValue(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, DbValue expectedValue)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryArrayContainsValue(payload, pathSegmentsUtf8, CreateDefaultArrayFlags(pathSegmentsUtf8.Length), expectedValue);
    }

    public static bool TryArrayContainsValue(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, bool[] pathArraySegments, DbValue expectedValue)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        ArgumentNullException.ThrowIfNull(pathArraySegments);
        return TryArrayContainsValueFromObject(payload, 0, pathSegmentsUtf8, pathArraySegments, expectedValue);
    }

    public static bool TryReadDocument(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, out ReadOnlySpan<byte> documentPayload)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        return TryReadDocumentFromObject(payload, 0, pathSegmentsUtf8, out documentPayload);
    }

    public static bool TryTextEquals(ReadOnlySpan<byte> payload, byte[][] pathSegmentsUtf8, string expectedValue)
    {
        ArgumentNullException.ThrowIfNull(pathSegmentsUtf8);
        ArgumentNullException.ThrowIfNull(expectedValue);
        return TryTextEqualsFromObject(payload, 0, pathSegmentsUtf8, expectedValue);
    }

    public static byte[] EncodeJsonUtf8(ReadOnlySpan<byte> payload)
    {
        var output = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(output);
        int position = 0;
        WriteJsonObject(payload, ref position, writer);
        writer.Flush();
        return output.WrittenSpan.ToArray();
    }

    public static string DecodeJson(ReadOnlySpan<byte> payload)
        => Encoding.UTF8.GetString(EncodeJsonUtf8(payload));

    private static bool TryReadValueFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out DbValue value)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength))
        {
            value = default;
            return false;
        }

        return TryConvertValue(tag, payload.Slice(valueStart, valueLength), out value);
    }

    private static bool TryTextEqualsFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        string expectedValue)
    {
        return TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength) &&
               TryTextEquals(tag, payload.Slice(valueStart, valueLength), expectedValue);
    }

    private static bool TryReadInt64FromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out long value)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength) ||
            tag != IntegerTag ||
            valueLength != sizeof(long))
        {
            value = 0;
            return false;
        }

        value = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(valueStart, valueLength));
        return true;
    }

    private static bool TryReadStringFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out string? value)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength) ||
            tag != StringTag)
        {
            value = null;
            return false;
        }

        value = Encoding.UTF8.GetString(payload.Slice(valueStart, valueLength));
        return true;
    }

    private static bool TryReadBooleanFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out bool value)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out _, out _))
        {
            value = default;
            return false;
        }

        if (tag == FalseTag)
        {
            value = false;
            return true;
        }

        if (tag == TrueTag)
        {
            value = true;
            return true;
        }

        value = default;
        return false;
    }

    private static bool TryReadDecimalFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out decimal value)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength) ||
            tag != DecimalTag ||
            valueLength != sizeof(int) * 4)
        {
            value = default;
            return false;
        }

        ReadOnlySpan<byte> valuePayload = payload.Slice(valueStart, valueLength);
        int lo = BinaryPrimitives.ReadInt32LittleEndian(valuePayload);
        int mid = BinaryPrimitives.ReadInt32LittleEndian(valuePayload[4..]);
        int hi = BinaryPrimitives.ReadInt32LittleEndian(valuePayload[8..]);
        int flags = BinaryPrimitives.ReadInt32LittleEndian(valuePayload[12..]);
        value = new decimal([lo, mid, hi, flags]);
        return true;
    }

    private static bool TryReadArrayValuesFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        bool[] pathArraySegments,
        List<DbValue> values)
    {
        if (!TryFindCurrentValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength))
        {
            return false;
        }

        if (pathArraySegments[pathIndex])
        {
            if (tag != ArrayTag)
                return false;

            if (pathIndex == pathSegmentsUtf8.Length - 1)
                return TryCollectScalarArrayValues(payload.Slice(valueStart, valueLength), values);

            return TryReadArrayValuesFromArray(payload.Slice(valueStart, valueLength), pathIndex + 1, pathSegmentsUtf8, pathArraySegments, values);
        }

        if (pathIndex == pathSegmentsUtf8.Length - 1)
        {
            if (!TryConvertValue(tag, payload.Slice(valueStart, valueLength), out DbValue value) ||
                value.Type is not (DbType.Integer or DbType.Text))
            {
                return false;
            }

            values.Add(value);
            return true;
        }

        if (tag != ObjectTag)
            return false;

        return TryReadArrayValuesFromObject(payload.Slice(valueStart, valueLength), pathIndex + 1, pathSegmentsUtf8, pathArraySegments, values);
    }

    private static bool TryArrayContainsValueFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        bool[] pathArraySegments,
        DbValue expectedValue)
    {
        if (!TryFindCurrentValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength))
        {
            return false;
        }

        if (pathArraySegments[pathIndex])
        {
            if (tag != ArrayTag)
                return false;

            if (pathIndex == pathSegmentsUtf8.Length - 1)
                return TryScalarArrayContainsValue(payload.Slice(valueStart, valueLength), expectedValue);

            return TryArrayContainsValueFromArray(payload.Slice(valueStart, valueLength), pathIndex + 1, pathSegmentsUtf8, pathArraySegments, expectedValue);
        }

        if (pathIndex == pathSegmentsUtf8.Length - 1)
        {
            return TryConvertValue(tag, payload.Slice(valueStart, valueLength), out DbValue value) &&
                   value.Type == expectedValue.Type &&
                   DbValue.Compare(value, expectedValue) == 0;
        }

        if (tag != ObjectTag)
            return false;

        return TryArrayContainsValueFromObject(payload.Slice(valueStart, valueLength), pathIndex + 1, pathSegmentsUtf8, pathArraySegments, expectedValue);
    }

    private static bool TryReadArrayValuesFromArray(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        bool[] pathArraySegments,
        List<DbValue> values)
    {
        int position = 0;
        ulong elementCount = ReadVarint(payload, ref position);
        bool foundAny = false;

        for (ulong i = 0; i < elementCount; i++)
        {
            if (!TryReadValuePayload(payload, ref position, out byte tag, out int valueStart, out int valueLength))
                return false;

            if (tag != ObjectTag)
                continue;

            if (TryReadArrayValuesFromObject(payload.Slice(valueStart, valueLength), pathIndex, pathSegmentsUtf8, pathArraySegments, values))
                foundAny = true;
        }

        return foundAny;
    }

    private static bool TryArrayContainsValueFromArray(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        bool[] pathArraySegments,
        DbValue expectedValue)
    {
        int position = 0;
        ulong elementCount = ReadVarint(payload, ref position);

        for (ulong i = 0; i < elementCount; i++)
        {
            if (!TryReadValuePayload(payload, ref position, out byte tag, out int valueStart, out int valueLength))
                return false;

            if (tag != ObjectTag)
                continue;

            if (TryArrayContainsValueFromObject(payload.Slice(valueStart, valueLength), pathIndex, pathSegmentsUtf8, pathArraySegments, expectedValue))
                return true;
        }

        return false;
    }

    private static bool[] CreateDefaultArrayFlags(int length)
    {
        var flags = new bool[length];
        if (length > 0)
            flags[^1] = true;
        return flags;
    }

    private static bool TryCollectScalarArrayValues(ReadOnlySpan<byte> payload, List<DbValue> values)
    {
        int position = 0;
        ulong elementCount = ReadVarint(payload, ref position);
        bool foundAny = false;

        for (ulong i = 0; i < elementCount; i++)
        {
            if (!TryReadValuePayload(payload, ref position, out byte tag, out int valueStart, out int valueLength))
                return false;

            if (!TryConvertValue(tag, payload.Slice(valueStart, valueLength), out DbValue value))
                continue;

            if (value.Type is not (DbType.Integer or DbType.Text))
                continue;

            values.Add(value);
            foundAny = true;
        }

        return foundAny;
    }

    private static bool TryScalarArrayContainsValue(ReadOnlySpan<byte> payload, DbValue expectedValue)
    {
        if (expectedValue.Type is not (DbType.Integer or DbType.Text))
            return false;

        int position = 0;
        ulong elementCount = ReadVarint(payload, ref position);

        for (ulong i = 0; i < elementCount; i++)
        {
            if (!TryReadValuePayload(payload, ref position, out byte tag, out int valueStart, out int valueLength))
                return false;

            if (!TryConvertValue(tag, payload.Slice(valueStart, valueLength), out DbValue value))
                continue;

            if (value.Type != expectedValue.Type)
                continue;

            if (DbValue.Compare(value, expectedValue) == 0)
                return true;
        }

        return false;
    }

    private static bool TryReadDocumentFromObject(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out ReadOnlySpan<byte> documentPayload)
    {
        if (!TryFindValue(payload, pathIndex, pathSegmentsUtf8, out byte tag, out int valueStart, out int valueLength) ||
            tag != ObjectTag)
        {
            documentPayload = default;
            return false;
        }

        documentPayload = payload.Slice(valueStart, valueLength);
        return true;
    }

    private static bool TryFindValue(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out byte tag,
        out int valueStart,
        out int valueLength)
    {
        if (!TryFindCurrentValue(payload, pathIndex, pathSegmentsUtf8, out tag, out valueStart, out valueLength))
            return false;

        if (pathIndex == pathSegmentsUtf8.Length - 1)
            return true;

        if (tag != ObjectTag)
        {
            tag = default;
            valueStart = 0;
            valueLength = 0;
            return false;
        }

        if (TryFindValue(payload.Slice(valueStart, valueLength), pathIndex + 1, pathSegmentsUtf8, out tag, out int nestedValueStart, out int nestedValueLength))
        {
            valueStart += nestedValueStart;
            valueLength = nestedValueLength;
            return true;
        }

        tag = default;
        valueStart = 0;
        valueLength = 0;
        return false;
    }

    private static bool TryFindCurrentValue(
        ReadOnlySpan<byte> payload,
        int pathIndex,
        byte[][] pathSegmentsUtf8,
        out byte tag,
        out int valueStart,
        out int valueLength)
    {
        int position = 0;
        ulong fieldCount = ReadVarint(payload, ref position);
        for (ulong i = 0; i < fieldCount; i++)
        {
            ReadOnlySpan<byte> fieldName = ReadLengthPrefixedBytes(payload, ref position);
            if (!fieldName.SequenceEqual(pathSegmentsUtf8[pathIndex]))
            {
                if (!TrySkipValue(payload, ref position))
                    break;

                continue;
            }

            return TryReadValuePayload(payload, ref position, out tag, out valueStart, out valueLength);
        }

        tag = default;
        valueStart = 0;
        valueLength = 0;
        return false;
    }

    private static bool TryReadValuePayload(
        ReadOnlySpan<byte> payload,
        ref int position,
        out byte tag,
        out int valueStart,
        out int valueLength)
    {
        tag = ReadByte(payload, ref position);
        switch (tag)
        {
            case NullTag:
            case FalseTag:
            case TrueTag:
                valueStart = position;
                valueLength = 0;
                return true;
            case StringTag:
                int stringLength = checked((int)ReadVarint(payload, ref position));
                if (payload.Length - position < stringLength)
                {
                    valueStart = 0;
                    valueLength = 0;
                    return false;
                }

                valueStart = position;
                valueLength = stringLength;
                position += stringLength;
                return true;
            case IntegerTag:
            case DoubleTag:
                if (payload.Length - position < sizeof(long))
                {
                    valueStart = 0;
                    valueLength = 0;
                    return false;
                }

                valueStart = position;
                valueLength = sizeof(long);
                position += sizeof(long);
                return true;
            case DecimalTag:
                if (payload.Length - position < sizeof(int) * 4)
                {
                    valueStart = 0;
                    valueLength = 0;
                    return false;
                }

                valueStart = position;
                valueLength = sizeof(int) * 4;
                position += sizeof(int) * 4;
                return true;
            case ObjectTag:
                int nestedStart = position;
                if (!TrySkipObject(payload, ref position))
                {
                    valueStart = 0;
                    valueLength = 0;
                    return false;
                }

                valueStart = nestedStart;
                valueLength = position - nestedStart;
                return true;
            case ArrayTag:
                int arrayStart = position;
                if (!TrySkipArray(payload, ref position))
                {
                    valueStart = 0;
                    valueLength = 0;
                    return false;
                }

                valueStart = arrayStart;
                valueLength = position - arrayStart;
                return true;
            default:
                valueStart = 0;
                valueLength = 0;
                return false;
        }
    }

    private static bool TryConvertValue(byte tag, ReadOnlySpan<byte> valuePayload, out DbValue value)
    {
        switch (tag)
        {
            case StringTag:
                value = DbValue.FromText(Encoding.UTF8.GetString(valuePayload));
                return true;
            case IntegerTag when valuePayload.Length == sizeof(long):
                value = DbValue.FromInteger(BinaryPrimitives.ReadInt64LittleEndian(valuePayload));
                return true;
            case DoubleTag when valuePayload.Length == sizeof(long):
                value = DbValue.FromReal(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(valuePayload)));
                return true;
            default:
                value = default;
                return false;
        }
    }

    private static bool TryTextEquals(byte tag, ReadOnlySpan<byte> valuePayload, string expectedValue)
    {
        if (tag != StringTag)
            return false;

        int expectedByteCount = Encoding.UTF8.GetByteCount(expectedValue);
        byte[]? rented = null;
        Span<byte> expectedUtf8 = expectedByteCount <= 256
            ? stackalloc byte[256]
            : (rented = ArrayPool<byte>.Shared.Rent(expectedByteCount));

        try
        {
            int written = Encoding.UTF8.GetBytes(expectedValue.AsSpan(), expectedUtf8);
            return valuePayload.SequenceEqual(expectedUtf8[..written]);
        }
        finally
        {
            if (rented != null)
            {
                expectedUtf8[..expectedByteCount].Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static bool TryReadNestedObject(
        ReadOnlySpan<byte> payload,
        ref int position,
        out int nestedStart,
        out int nestedLength)
    {
        nestedStart = 0;
        nestedLength = 0;
        byte tag = ReadByte(payload, ref position);
        if (tag != ObjectTag)
            return false;

        nestedStart = position;
        if (!TrySkipObject(payload, ref position))
            return false;

        nestedLength = position - nestedStart;
        return true;
    }

    private static void WriteJsonObject(ReadOnlySpan<byte> payload, ref int position, Utf8JsonWriter writer)
    {
        ulong fieldCount = ReadVarint(payload, ref position);
        writer.WriteStartObject();
        for (ulong i = 0; i < fieldCount; i++)
        {
            ReadOnlySpan<byte> fieldName = ReadLengthPrefixedBytes(payload, ref position);
            writer.WritePropertyName(fieldName);
            WriteJsonValue(payload, ref position, writer);
        }

        writer.WriteEndObject();
    }

    private static void WriteJsonValue(ReadOnlySpan<byte> payload, ref int position, Utf8JsonWriter writer)
    {
        byte tag = ReadByte(payload, ref position);
        switch (tag)
        {
            case NullTag:
                writer.WriteNullValue();
                return;
            case StringTag:
                writer.WriteStringValue(ReadLengthPrefixedBytes(payload, ref position));
                return;
            case IntegerTag:
                writer.WriteNumberValue(ReadInt64(payload, ref position));
                return;
            case FalseTag:
                writer.WriteBooleanValue(false);
                return;
            case TrueTag:
                writer.WriteBooleanValue(true);
                return;
            case DoubleTag:
                writer.WriteNumberValue(ReadDouble(payload, ref position));
                return;
            case DecimalTag:
                writer.WriteNumberValue(ReadDecimal(payload, ref position));
                return;
            case ObjectTag:
                WriteJsonObject(payload, ref position, writer);
                return;
            case ArrayTag:
                ulong elementCount = ReadVarint(payload, ref position);
                writer.WriteStartArray();
                for (ulong i = 0; i < elementCount; i++)
                    WriteJsonValue(payload, ref position, writer);
                writer.WriteEndArray();
                return;
            default:
                throw new CSharpDbException(ErrorCode.CorruptDatabase, "Unknown binary collection value tag.");
        }
    }

    private static void WriteObject(IBufferWriter<byte> writer, object instance, TypeMetadata metadata)
    {
        WriteVarint(writer, (ulong)metadata.Members.Length);
        for (int i = 0; i < metadata.Members.Length; i++)
        {
            var member = metadata.Members[i];
            WriteLengthPrefixedBytes(writer, member.EncodedNameUtf8);
            WriteValue(writer, member, member.GetValue(instance));
        }
    }

    private static void WriteValue(
        IBufferWriter<byte> writer,
        MemberMetadata member,
        object? value)
    {
        if (member.ValueKind == MemberValueKind.Array)
        {
            WriteArrayValue(writer, member, value);
            return;
        }

        WriteTypedValue(
            writer,
            member.Member.Name,
            member.ValueKind,
            member.EffectiveType,
            value,
            member.ValueKind == MemberValueKind.Object ? member.GetNestedMetadata() : null);
    }

    private static void WriteTypedValue(
        IBufferWriter<byte> writer,
        string memberName,
        MemberValueKind valueKind,
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type effectiveType,
        object? value,
        TypeMetadata? nestedMetadata)
    {
        if (value is null)
        {
            WriteByte(writer, NullTag);
            return;
        }

        switch (valueKind)
        {
            case MemberValueKind.Enum:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                return;

            case MemberValueKind.String:
                WriteByte(writer, StringTag);
                WriteLengthPrefixedString(writer, (string)value);
                return;
            case MemberValueKind.Guid:
                WriteByte(writer, StringTag);
                WriteLengthPrefixedString(writer, ((Guid)value).ToString("D"));
                return;
            case MemberValueKind.DateOnly:
                WriteByte(writer, StringTag);
                WriteLengthPrefixedString(writer, ((DateOnly)value).ToString("O", CultureInfo.InvariantCulture));
                return;
            case MemberValueKind.TimeOnly:
                WriteByte(writer, StringTag);
                WriteLengthPrefixedString(writer, ((TimeOnly)value).ToString("O", CultureInfo.InvariantCulture));
                return;

            case MemberValueKind.Boolean:
                WriteByte(writer, (bool)value ? TrueTag : FalseTag);
                return;

            case MemberValueKind.Byte:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (byte)value);
                return;
            case MemberValueKind.SByte:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (sbyte)value);
                return;
            case MemberValueKind.Int16:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (short)value);
                return;
            case MemberValueKind.UInt16:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (ushort)value);
                return;
            case MemberValueKind.Int32:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (int)value);
                return;
            case MemberValueKind.UInt32:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (uint)value);
                return;
            case MemberValueKind.Int64:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, (long)value);
                return;
            case MemberValueKind.UInt64:
                WriteByte(writer, IntegerTag);
                WriteInt64(writer, unchecked((long)(ulong)value));
                return;

            case MemberValueKind.Single:
                WriteByte(writer, DoubleTag);
                WriteDouble(writer, (float)value);
                return;
            case MemberValueKind.Double:
                WriteByte(writer, DoubleTag);
                WriteDouble(writer, (double)value);
                return;

            case MemberValueKind.Decimal:
                WriteByte(writer, DecimalTag);
                WriteDecimal(writer, (decimal)value);
                return;

            case MemberValueKind.Object:
                WriteByte(writer, ObjectTag);
                WriteObject(writer, value, nestedMetadata ?? TypeMetadataCache.GetMetadata(effectiveType));
                return;

            default:
                throw new NotSupportedException(
                    $"Member '{memberName}' is not supported for binary collection storage.");
        }
    }

    private static void WriteArrayValue(
        IBufferWriter<byte> writer,
        MemberMetadata member,
        object? value)
    {
        if (value is null)
        {
            WriteByte(writer, NullTag);
            return;
        }

        if (value is not IEnumerable enumerable || value is string)
        {
            throw new NotSupportedException(
                $"Member '{member.Member.Name}' is not supported for binary collection storage.");
        }

        WriteByte(writer, ArrayTag);

        if (value is ICollection collection)
        {
            WriteVarint(writer, (ulong)collection.Count);
            foreach (object? element in enumerable)
            {
                WriteTypedValue(
                    writer,
                    member.Member.Name,
                    member.ArrayElementValueKind,
                    member.ArrayElementEffectiveType,
                    element,
                    member.ArrayElementValueKind == MemberValueKind.Object
                        ? member.GetArrayElementNestedMetadata()
                        : null);
            }

            return;
        }

        var buffered = new List<object?>();
        foreach (object? element in enumerable)
            buffered.Add(element);

        WriteVarint(writer, (ulong)buffered.Count);
        for (int i = 0; i < buffered.Count; i++)
        {
            WriteTypedValue(
                writer,
                member.Member.Name,
                member.ArrayElementValueKind,
                member.ArrayElementEffectiveType,
                buffered[i],
                member.ArrayElementValueKind == MemberValueKind.Object
                    ? member.GetArrayElementNestedMetadata()
                    : null);
        }
    }

    private static object ReadObject(ReadOnlySpan<byte> payload, ref int position, TypeMetadata metadata)
    {
        ulong fieldCount = ReadVarint(payload, ref position);
        object?[] ctorArgs = metadata.CreateConstructorBuffer();
        object?[]? pendingValues = metadata.CreatePendingValueBuffer();
        int expectedMemberIndex = 0;

        for (ulong i = 0; i < fieldCount; i++)
        {
            ReadOnlySpan<byte> fieldName = ReadLengthPrefixedBytes(payload, ref position);
            int memberIndex = metadata.ResolveMemberIndex(fieldName, ref expectedMemberIndex);
            if (memberIndex < 0)
            {
                SkipValue(payload, ref position);
                continue;
            }

            var member = metadata.Members[memberIndex];
            object? value = ReadValue(payload, ref position, member);
            if (member.ConstructorParameterIndex >= 0)
                ctorArgs[member.ConstructorParameterIndex] = value;
            else if (member.PendingValueIndex >= 0)
                pendingValues![member.PendingValueIndex] = value;
        }

        object instance = metadata.CreateInstance(ctorArgs);
        metadata.ApplyPendingValues(instance, pendingValues);

        return instance;
    }

    private static object? ReadValue(
        ReadOnlySpan<byte> payload,
        ref int position,
        MemberMetadata member)
    {
        byte tag = ReadByte(payload, ref position);
        return ReadTypedValue(
            payload,
            ref position,
            tag,
            member.Member.Name,
            member.MemberType,
            member.EffectiveType,
            member.ValueKind,
            member.ValueKind == MemberValueKind.Object ? member.GetNestedMetadata() : null,
            member);
    }

    private static object? ReadTypedValue(
        ReadOnlySpan<byte> payload,
        ref int position,
        byte tag,
        string memberName,
        Type targetType,
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type effectiveType,
        MemberValueKind valueKind,
        TypeMetadata? nestedMetadata,
        MemberMetadata? member)
    {
        if (tag == NullTag)
            return targetType.IsValueType && Nullable.GetUnderlyingType(targetType) == null
                ? CreateDefaultValueInstance(targetType)
                : null;

        switch (valueKind)
        {
            case MemberValueKind.Enum:
                EnsureTag(tag, IntegerTag);
                long enumValue = ReadInt64(payload, ref position);
                return Enum.ToObject(
                    effectiveType,
                    Convert.ChangeType(
                        enumValue,
                        Enum.GetUnderlyingType(effectiveType),
                        System.Globalization.CultureInfo.InvariantCulture)!);

            case MemberValueKind.String:
                EnsureTag(tag, StringTag);
                return Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position));
            case MemberValueKind.Guid:
                EnsureTag(tag, StringTag);
                return Guid.ParseExact(
                    Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position)),
                    "D");
            case MemberValueKind.DateOnly:
                EnsureTag(tag, StringTag);
                return DateOnly.ParseExact(
                    Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position)),
                    "O",
                    CultureInfo.InvariantCulture);
            case MemberValueKind.TimeOnly:
                EnsureTag(tag, StringTag);
                return TimeOnly.ParseExact(
                    Encoding.UTF8.GetString(ReadLengthPrefixedBytes(payload, ref position)),
                    "O",
                    CultureInfo.InvariantCulture);

            case MemberValueKind.Boolean:
                if (tag == FalseTag)
                    return false;
                if (tag == TrueTag)
                    return true;

                throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid boolean value in binary collection payload.");

            case MemberValueKind.Byte:
                EnsureTag(tag, IntegerTag);
                return checked((byte)ReadInt64(payload, ref position));
            case MemberValueKind.SByte:
                EnsureTag(tag, IntegerTag);
                return checked((sbyte)ReadInt64(payload, ref position));
            case MemberValueKind.Int16:
                EnsureTag(tag, IntegerTag);
                return checked((short)ReadInt64(payload, ref position));
            case MemberValueKind.UInt16:
                EnsureTag(tag, IntegerTag);
                return checked((ushort)ReadInt64(payload, ref position));
            case MemberValueKind.Int32:
                EnsureTag(tag, IntegerTag);
                return checked((int)ReadInt64(payload, ref position));
            case MemberValueKind.UInt32:
                EnsureTag(tag, IntegerTag);
                return checked((uint)ReadInt64(payload, ref position));
            case MemberValueKind.Int64:
                EnsureTag(tag, IntegerTag);
                return ReadInt64(payload, ref position);
            case MemberValueKind.UInt64:
                EnsureTag(tag, IntegerTag);
                return unchecked((ulong)ReadInt64(payload, ref position));
            case MemberValueKind.Single:
                EnsureTag(tag, DoubleTag);
                return (float)ReadDouble(payload, ref position);
            case MemberValueKind.Double:
                EnsureTag(tag, DoubleTag);
                return ReadDouble(payload, ref position);
            case MemberValueKind.Decimal:
                EnsureTag(tag, DecimalTag);
                return ReadDecimal(payload, ref position);
            case MemberValueKind.Array:
                if (member == null)
                {
                    throw new NotSupportedException(
                        $"Member '{memberName}' is not supported for binary collection storage.");
                }

                EnsureTag(tag, ArrayTag);
                return ReadArray(payload, ref position, member);
            case MemberValueKind.Object:
                EnsureTag(tag, ObjectTag);
                return ReadObject(payload, ref position, nestedMetadata ?? TypeMetadataCache.GetMetadata(effectiveType));
            default:
                throw new NotSupportedException(
                    $"Member '{memberName}' is not supported for binary collection storage.");
        }
    }

    private static object ReadArray(ReadOnlySpan<byte> payload, ref int position, MemberMetadata member)
    {
        int elementCount = checked((int)ReadVarint(payload, ref position));
        object?[] values = new object?[elementCount];

        for (int i = 0; i < elementCount; i++)
        {
            byte elementTag = ReadByte(payload, ref position);
            values[i] = ReadTypedValue(
                payload,
                ref position,
                elementTag,
                member.Member.Name,
                member.ArrayElementType,
                member.ArrayElementEffectiveType,
                member.ArrayElementValueKind,
                member.ArrayElementValueKind == MemberValueKind.Object
                    ? member.GetArrayElementNestedMetadata()
                    : null,
                member.ArrayElementValueKind == MemberValueKind.Array ? member : null);
        }

        return member.MaterializeArray(values);
    }

    private static void SkipValue(ReadOnlySpan<byte> payload, ref int position)
    {
        if (!TrySkipValue(payload, ref position))
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid binary collection payload.");
    }

    private static bool TrySkipValue(ReadOnlySpan<byte> payload, ref int position)
    {
        byte tag = ReadByte(payload, ref position);
        switch (tag)
        {
            case NullTag:
            case FalseTag:
            case TrueTag:
                return true;
            case StringTag:
                _ = ReadLengthPrefixedBytes(payload, ref position);
                return true;
            case IntegerTag:
                position += sizeof(long);
                return position <= payload.Length;
            case DoubleTag:
                position += sizeof(double);
                return position <= payload.Length;
            case DecimalTag:
                position += sizeof(int) * 4;
                return position <= payload.Length;
            case ObjectTag:
                return TrySkipObject(payload, ref position);
            case ArrayTag:
                return TrySkipArray(payload, ref position);
            default:
                return false;
        }
    }

    private static bool TrySkipObject(ReadOnlySpan<byte> payload, ref int position)
    {
        ulong fieldCount = ReadVarint(payload, ref position);
        for (ulong i = 0; i < fieldCount; i++)
        {
            _ = ReadLengthPrefixedBytes(payload, ref position);
            if (!TrySkipValue(payload, ref position))
                return false;
        }

        return true;
    }

    private static bool TrySkipArray(ReadOnlySpan<byte> payload, ref int position)
    {
        ulong elementCount = ReadVarint(payload, ref position);
        for (ulong i = 0; i < elementCount; i++)
        {
            if (!TrySkipValue(payload, ref position))
                return false;
        }

        return true;
    }

    private static byte ReadByte(ReadOnlySpan<byte> payload, ref int position)
    {
        if ((uint)position >= (uint)payload.Length)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Unexpected end of binary collection payload.");

        return payload[position++];
    }

    private static ulong ReadVarint(ReadOnlySpan<byte> payload, ref int position)
    {
        ulong value = Varint.Read(payload[position..], out int bytesRead);
        position += bytesRead;
        return value;
    }

    private static ReadOnlySpan<byte> ReadLengthPrefixedBytes(ReadOnlySpan<byte> payload, ref int position)
    {
        int length = checked((int)ReadVarint(payload, ref position));
        if (length < 0 || payload.Length - position < length)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid binary collection payload length.");

        ReadOnlySpan<byte> value = payload.Slice(position, length);
        position += length;
        return value;
    }

    private static long ReadInt64(ReadOnlySpan<byte> payload, ref int position)
    {
        if (payload.Length - position < sizeof(long))
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Unexpected end of binary collection payload.");

        long value = BinaryPrimitives.ReadInt64LittleEndian(payload[position..]);
        position += sizeof(long);
        return value;
    }

    private static double ReadDouble(ReadOnlySpan<byte> payload, ref int position)
    {
        long bits = ReadInt64(payload, ref position);
        return BitConverter.Int64BitsToDouble(bits);
    }

    private static decimal ReadDecimal(ReadOnlySpan<byte> payload, ref int position)
    {
        if (payload.Length - position < sizeof(int) * 4)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Unexpected end of binary collection payload.");

        int lo = BinaryPrimitives.ReadInt32LittleEndian(payload[position..]);
        int mid = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 4)..]);
        int hi = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 8)..]);
        int flags = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..]);
        position += sizeof(int) * 4;
        return new decimal([lo, mid, hi, flags]);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> span = writer.GetSpan(1);
        span[0] = value;
        writer.Advance(1);
    }

    private static void WriteVarint(IBufferWriter<byte> writer, ulong value)
    {
        Span<byte> scratch = stackalloc byte[10];
        int bytesWritten = Varint.Write(scratch, value);
        WriteBytes(writer, scratch[..bytesWritten]);
    }

    private static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        Span<byte> span = writer.GetSpan(value.Length);
        value.CopyTo(span);
        writer.Advance(value.Length);
    }

    private static void WriteLengthPrefixedBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        WriteVarint(writer, (ulong)value.Length);
        WriteBytes(writer, value);
    }

    private static void WriteLengthPrefixedString(IBufferWriter<byte> writer, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        WriteVarint(writer, (ulong)byteCount);
        Span<byte> span = writer.GetSpan(byteCount);
        int written = Encoding.UTF8.GetBytes(value.AsSpan(), span[..byteCount]);
        writer.Advance(written);
    }

    private static void WriteInt64(IBufferWriter<byte> writer, long value)
    {
        Span<byte> span = writer.GetSpan(sizeof(long));
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        writer.Advance(sizeof(long));
    }

    private static void WriteDouble(IBufferWriter<byte> writer, double value)
        => WriteInt64(writer, BitConverter.DoubleToInt64Bits(value));

    private static void WriteDecimal(IBufferWriter<byte> writer, decimal value)
    {
        int[] bits = decimal.GetBits(value);
        Span<byte> span = writer.GetSpan(sizeof(int) * 4);
        BinaryPrimitives.WriteInt32LittleEndian(span, bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(span[4..], bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(span[8..], bits[2]);
        BinaryPrimitives.WriteInt32LittleEndian(span[12..], bits[3]);
        writer.Advance(sizeof(int) * 4);
    }

    private static void EnsureTag(byte actual, byte expected)
    {
        if (actual != expected)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Unexpected binary collection value tag '{actual}' (expected '{expected}').");
        }
    }

    private static MemberInfo[] GetSerializableMembers(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields)]
        Type type)
    {
        var members = new List<MemberInfo>();
        foreach (PropertyInfo property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (property.GetMethod == null || property.GetMethod.IsStatic)
                continue;
            if (property.GetIndexParameters().Length != 0)
                continue;

            members.Add(property);
        }

        foreach (FieldInfo field in type.GetFields(BindingFlags.Instance | BindingFlags.Public))
        {
            if (field.IsStatic)
                continue;

            members.Add(field);
        }

        members.Sort(static (left, right) => left.MetadataToken.CompareTo(right.MetadataToken));
        return members.ToArray();
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2073",
        Justification = "Binary collection serialization is reflection-based and already surfaced through RequiresUnreferencedCode/RequiresDynamicCode APIs.")]
    [return: DynamicallyAccessedMembers(
        DynamicallyAccessedMemberTypes.PublicProperties |
        DynamicallyAccessedMemberTypes.PublicFields |
        DynamicallyAccessedMemberTypes.PublicConstructors |
        DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
        DynamicallyAccessedMemberTypes.Interfaces)]
    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo property => property.PropertyType,
            FieldInfo field => field.FieldType,
            _ => throw new InvalidOperationException($"Member '{member.Name}' is not supported for binary collection storage.")
        };

    private static string GetEncodedMemberName(MemberInfo member)
        => JsonNamingPolicy.CamelCase.ConvertName(member.Name);

    private static TypeMetadata BuildMetadata(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type type)
    {
        if (type == typeof(string) || type.IsPrimitive || type == typeof(decimal))
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' cannot be used as a binary collection document root.");
        }

        MemberInfo[] members = GetSerializableMembers(type);
        var serializableMembers = new MemberMetadata[members.Length];
        for (int i = 0; i < members.Length; i++)
            serializableMembers[i] = new MemberMetadata(members[i], GetEncodedMemberName(members[i]), GetMemberType(members[i]));

        ConstructorBinding constructor = CreateConstructorBinding(type, serializableMembers);
        var postConstructionMembers = new List<MemberMetadata>();
        for (int i = 0; i < serializableMembers.Length; i++)
        {
            var member = serializableMembers[i];
            if (member.ConstructorParameterIndex >= 0 || !member.CanApplyAfterConstruction)
                continue;

            member.BindPendingValueIndex(postConstructionMembers.Count);
            postConstructionMembers.Add(member);
        }

        return new TypeMetadata(type, serializableMembers, constructor, postConstructionMembers.ToArray());
    }

    private static ConstructorBinding CreateConstructorBinding(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
        Type type,
        MemberMetadata[] members)
    {
        ConstructorInfo? parameterless = type.GetConstructor(Type.EmptyTypes);
        if (parameterless != null)
            return ConstructorBinding.ForParameterless(parameterless);

        foreach (ConstructorInfo ctor in type.GetConstructors(BindingFlags.Instance | BindingFlags.Public)
                     .OrderByDescending(static c => c.GetParameters().Length))
        {
            ParameterInfo[] parameters = ctor.GetParameters();
            int[] memberIndices = new int[parameters.Length];
            Array.Fill(memberIndices, -1);
            bool matches = true;

            for (int i = 0; i < parameters.Length; i++)
            {
                string? parameterName = parameters[i].Name;
                if (parameterName == null)
                {
                    matches = false;
                    break;
                }

                int memberIndex = FindMemberIndex(members, parameterName);
                if (memberIndex < 0 || !IsParameterCompatible(parameters[i].ParameterType, members[memberIndex].MemberType))
                {
                    matches = false;
                    break;
                }

                memberIndices[i] = memberIndex;
            }

            if (matches)
            {
                for (int i = 0; i < memberIndices.Length; i++)
                    members[memberIndices[i]].BindConstructorParameter(i);

                return ConstructorBinding.ForParameterized(ctor, parameters);
            }
        }

        throw new NotSupportedException(
            $"Type '{type.Name}' requires a public parameterless constructor or a constructor whose parameters match public fields/properties for binary collection storage.");
    }

    private static int FindMemberIndex(MemberMetadata[] members, string memberName)
    {
        for (int i = 0; i < members.Length; i++)
        {
            if (string.Equals(members[i].Member.Name, memberName, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static bool IsParameterCompatible(Type parameterType, Type memberType)
    {
        Type effectiveParameterType = Nullable.GetUnderlyingType(parameterType) ?? parameterType;
        Type effectiveMemberType = Nullable.GetUnderlyingType(memberType) ?? memberType;
        if (effectiveParameterType == effectiveMemberType)
            return true;

        if (effectiveParameterType.IsEnum && effectiveMemberType.IsEnum)
            return Enum.GetUnderlyingType(effectiveParameterType) == Enum.GetUnderlyingType(effectiveMemberType);

        return false;
    }

    private static MemberValueKind ResolveValueKind(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type effectiveType)
    {
        if (effectiveType == typeof(string))
            return MemberValueKind.String;
        if (effectiveType == typeof(Guid))
            return MemberValueKind.Guid;
        if (effectiveType == typeof(DateOnly))
            return MemberValueKind.DateOnly;
        if (effectiveType == typeof(TimeOnly))
            return MemberValueKind.TimeOnly;
        if (TryGetCollectionElementType(effectiveType, out _, out _))
            return MemberValueKind.Array;
        if (effectiveType.IsEnum)
            return MemberValueKind.Enum;
        if (effectiveType == typeof(bool))
            return MemberValueKind.Boolean;
        if (effectiveType == typeof(byte))
            return MemberValueKind.Byte;
        if (effectiveType == typeof(sbyte))
            return MemberValueKind.SByte;
        if (effectiveType == typeof(short))
            return MemberValueKind.Int16;
        if (effectiveType == typeof(ushort))
            return MemberValueKind.UInt16;
        if (effectiveType == typeof(int))
            return MemberValueKind.Int32;
        if (effectiveType == typeof(uint))
            return MemberValueKind.UInt32;
        if (effectiveType == typeof(long))
            return MemberValueKind.Int64;
        if (effectiveType == typeof(ulong))
            return MemberValueKind.UInt64;
        if (effectiveType == typeof(float))
            return MemberValueKind.Single;
        if (effectiveType == typeof(double))
            return MemberValueKind.Double;
        if (effectiveType == typeof(decimal))
            return MemberValueKind.Decimal;
        if (effectiveType.IsValueType)
        {
            throw new NotSupportedException(
                $"Type '{effectiveType}' is not supported for binary collection storage.");
        }

        return MemberValueKind.Object;
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2072",
        Justification = "CollectionBinaryDocumentCodec is a reflection-based collection payload path used behind Collection<T>, which is already marked as not trim-safe. The element type is only inspected for supported collection members discovered from preserved public members.")]
    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2062",
        Justification = "CollectionBinaryDocumentCodec is a reflection-based collection payload path used behind Collection<T>, which is already marked as not trim-safe. The element type is only inspected for supported collection members discovered from preserved public members.")]
    private static bool TryGetCollectionElementType(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type type,
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        [NotNullWhen(true)] out Type? elementType,
        out CollectionContainerKind containerKind)
    {
        if (type == typeof(string))
        {
            elementType = null;
            containerKind = CollectionContainerKind.None;
            return false;
        }

        if (type.IsArray)
        {
            elementType = type.GetElementType();
            containerKind = CollectionContainerKind.Array;
            return elementType != null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
        {
            elementType = type.GenericTypeArguments[0];
            containerKind = CollectionContainerKind.List;
            return true;
        }

        if (TryGetEnumerableElementType(type, out elementType))
        {
            containerKind = CollectionContainerKind.Interface;
            return true;
        }

        containerKind = CollectionContainerKind.None;
        elementType = null;
        return false;
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2062",
        Justification = "CollectionBinaryDocumentCodec is a reflection-based collection payload path used behind Collection<T>, which is already marked as not trim-safe. The element type is only inspected for supported collection members discovered from preserved public members.")]
    private static bool TryGetEnumerableElementType(
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        Type type,
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        [NotNullWhen(true)] out Type? elementType)
    {
        if (type.IsGenericType)
        {
            Type genericDefinition = type.GetGenericTypeDefinition();
            if (genericDefinition == typeof(IEnumerable<>) ||
                genericDefinition == typeof(ICollection<>) ||
                genericDefinition == typeof(IList<>) ||
                genericDefinition == typeof(IReadOnlyCollection<>) ||
                genericDefinition == typeof(IReadOnlyList<>))
            {
                elementType = type.GenericTypeArguments[0];
                return true;
            }
        }

        foreach (Type implementedInterface in type.GetInterfaces())
        {
            if (!implementedInterface.IsGenericType)
                continue;

            Type genericDefinition = implementedInterface.GetGenericTypeDefinition();
            if (genericDefinition == typeof(IEnumerable<>) ||
                genericDefinition == typeof(ICollection<>) ||
                genericDefinition == typeof(IList<>) ||
                genericDefinition == typeof(IReadOnlyCollection<>) ||
                genericDefinition == typeof(IReadOnlyList<>))
            {
                elementType = implementedInterface.GenericTypeArguments[0];
                return true;
            }
        }

        elementType = null;
        return false;
    }

    private static class TypeMetadataCache<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        T>
    {
        internal static readonly TypeMetadata Metadata = BuildMetadata(typeof(T));
    }

    private static class TypeMetadataCache
    {
        private static readonly Dictionary<Type, TypeMetadata> s_cache = new();
        private static readonly object s_gate = new();

        internal static TypeMetadata GetMetadata(
            [DynamicallyAccessedMembers(
                DynamicallyAccessedMemberTypes.PublicProperties |
                DynamicallyAccessedMemberTypes.PublicFields |
                DynamicallyAccessedMemberTypes.PublicConstructors |
                DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                DynamicallyAccessedMemberTypes.Interfaces)]
            Type type)
        {
            lock (s_gate)
            {
                if (!s_cache.TryGetValue(type, out var metadata))
                {
                    metadata = BuildMetadata(type);
                    s_cache[type] = metadata;
                }

                return metadata;
            }
        }
    }

    private sealed class TypeMetadata
    {
        internal TypeMetadata(
            [DynamicallyAccessedMembers(
                DynamicallyAccessedMemberTypes.PublicProperties |
                DynamicallyAccessedMemberTypes.PublicFields |
                DynamicallyAccessedMemberTypes.PublicConstructors |
                DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                DynamicallyAccessedMemberTypes.Interfaces)]
            Type type,
            MemberMetadata[] members,
            ConstructorBinding constructor,
            MemberMetadata[] postConstructionMembers)
        {
            Type = type;
            Members = members;
            Constructor = constructor;
            PostConstructionMembers = postConstructionMembers;
        }

        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        internal Type Type { get; }

        internal MemberMetadata[] Members { get; }

        internal ConstructorBinding Constructor { get; }

        internal MemberMetadata[] PostConstructionMembers { get; }

        internal object?[] CreateConstructorBuffer() => Constructor.CreateBuffer();

        internal object?[]? CreatePendingValueBuffer()
            => PostConstructionMembers.Length == 0 ? null : new object?[PostConstructionMembers.Length];

        internal int ResolveMemberIndex(ReadOnlySpan<byte> encodedNameUtf8, ref int expectedMemberIndex)
        {
            if ((uint)expectedMemberIndex < (uint)Members.Length &&
                Members[expectedMemberIndex].NameEquals(encodedNameUtf8))
            {
                return expectedMemberIndex++;
            }

            for (int i = 0; i < Members.Length; i++)
            {
                if (Members[i].NameEquals(encodedNameUtf8))
                    return i;
            }

            return -1;
        }

        internal object CreateInstance(object?[] ctorArgs)
            => Constructor.CreateInstance(Type, ctorArgs);

        internal void ApplyPendingValues(object instance, object?[]? pendingValues)
        {
            if (pendingValues == null)
                return;

            for (int i = 0; i < PostConstructionMembers.Length; i++)
                PostConstructionMembers[i].Apply(instance, pendingValues[i]);
        }
    }

    private sealed class ConstructorBinding
    {
        private readonly ConstructorInfo _constructor;
        private readonly ParameterInfo[] _parameters;
        private readonly ObjectFactory _factory;

        private ConstructorBinding(ConstructorInfo constructor, ParameterInfo[] parameters)
        {
            _constructor = constructor;
            _parameters = parameters;
            _factory = CreateFactory(constructor, parameters);
        }

        internal static ConstructorBinding ForParameterless(ConstructorInfo constructor)
            => new(constructor, Array.Empty<ParameterInfo>());

        internal static ConstructorBinding ForParameterized(ConstructorInfo constructor, ParameterInfo[] parameters)
            => new(constructor, parameters);

        internal object?[] CreateBuffer()
        {
            if (_parameters.Length == 0)
                return Array.Empty<object?>();

            var values = new object?[_parameters.Length];
            for (int i = 0; i < values.Length; i++)
            {
                if (_parameters[i].HasDefaultValue)
                    values[i] = _parameters[i].DefaultValue;
                else if (_parameters[i].ParameterType.IsValueType && Nullable.GetUnderlyingType(_parameters[i].ParameterType) == null)
                    values[i] = CreateDefaultValueInstance(_parameters[i].ParameterType);
            }

            return values;
        }

        internal object CreateInstance(Type _, object?[] values)
            => _factory(values);

        private static ObjectFactory CreateFactory(ConstructorInfo constructor, ParameterInfo[] parameters)
        {
            var argsParameter = Expression.Parameter(typeof(object?[]), "args");
            Expression[] arguments = new Expression[parameters.Length];
            for (int i = 0; i < parameters.Length; i++)
            {
                arguments[i] = Expression.Convert(
                    Expression.ArrayIndex(argsParameter, Expression.Constant(i)),
                    parameters[i].ParameterType);
            }

            Expression body = Expression.Convert(Expression.New(constructor, arguments), typeof(object));
            return Expression.Lambda<ObjectFactory>(body, argsParameter).Compile();
        }
    }

    private sealed class MemberMetadata
    {
        private readonly PropertyInfo? _property;
        private readonly FieldInfo? _field;
        private readonly MemberGetter _getter;
        private readonly MemberSetter? _setter;
        private TypeMetadata? _nestedMetadata;
        private TypeMetadata? _arrayElementNestedMetadata;
        private readonly CollectionContainerKind _collectionContainerKind;

        internal MemberMetadata(
            MemberInfo member,
            string encodedName,
            [DynamicallyAccessedMembers(
                DynamicallyAccessedMemberTypes.PublicProperties |
                DynamicallyAccessedMemberTypes.PublicFields |
                DynamicallyAccessedMemberTypes.PublicConstructors |
                DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
                DynamicallyAccessedMemberTypes.Interfaces)]
            Type memberType)
        {
            Member = member;
            EncodedName = encodedName;
            EncodedNameUtf8 = Encoding.UTF8.GetBytes(encodedName);
            MemberType = memberType;
            EffectiveType = Nullable.GetUnderlyingType(memberType) ?? memberType;
            ValueKind = ResolveValueKind(EffectiveType);
            if (ValueKind == MemberValueKind.Array)
            {
                if (!TryGetCollectionElementType(memberType, out Type? arrayElementType, out _collectionContainerKind) ||
                    arrayElementType == null)
                {
                    throw new NotSupportedException(
                        $"Member '{member.Name}' is not supported for binary collection storage.");
                }

                ArrayElementType = arrayElementType;
                ArrayElementEffectiveType = Nullable.GetUnderlyingType(arrayElementType) ?? arrayElementType;
                ArrayElementValueKind = ResolveValueKind(ArrayElementEffectiveType);
                if (ArrayElementValueKind == MemberValueKind.Array)
                {
                    throw new NotSupportedException(
                        $"Member '{member.Name}' is not supported for binary collection storage.");
                }
            }
            else
            {
                ArrayElementType = typeof(object);
                ArrayElementEffectiveType = typeof(object);
                ArrayElementValueKind = MemberValueKind.Object;
                _collectionContainerKind = CollectionContainerKind.None;
            }

            _property = member as PropertyInfo;
            _field = member as FieldInfo;
            _getter = CreateGetter(member);
            _setter = CreateSetter(member, memberType);
            ConstructorParameterIndex = -1;
            PendingValueIndex = -1;
        }

        internal MemberInfo Member { get; }

        internal string EncodedName { get; }

        internal byte[] EncodedNameUtf8 { get; }

        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        internal Type MemberType { get; }

        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        internal Type EffectiveType { get; }

        internal MemberValueKind ValueKind { get; }

        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        internal Type ArrayElementType { get; }

        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicProperties |
            DynamicallyAccessedMemberTypes.PublicFields |
            DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor |
            DynamicallyAccessedMemberTypes.Interfaces)]
        internal Type ArrayElementEffectiveType { get; }

        internal MemberValueKind ArrayElementValueKind { get; }

        internal int ConstructorParameterIndex { get; private set; }

        internal int PendingValueIndex { get; private set; }

        internal bool CanApplyAfterConstruction => _setter != null;

        internal object? GetValue(object instance)
            => _getter(instance);

        internal void BindConstructorParameter(int parameterIndex)
            => ConstructorParameterIndex = parameterIndex;

        internal void BindPendingValueIndex(int pendingValueIndex)
            => PendingValueIndex = pendingValueIndex;

        internal void Apply(object instance, object? value)
        {
            if (ConstructorParameterIndex >= 0)
                return;

            _setter?.Invoke(instance, value);
        }

        internal bool NameEquals(ReadOnlySpan<byte> encodedNameUtf8)
        {
            ReadOnlySpan<byte> name = EncodedNameUtf8;
            return name.Length == encodedNameUtf8.Length &&
                   (name.IsEmpty || name[0] == encodedNameUtf8[0]) &&
                   name.SequenceEqual(encodedNameUtf8);
        }

        internal TypeMetadata GetNestedMetadata()
        {
            if (ValueKind != MemberValueKind.Object)
                throw new InvalidOperationException($"Member '{Member.Name}' is not an object.");

            return _nestedMetadata ??= TypeMetadataCache.GetMetadata(EffectiveType);
        }

        internal TypeMetadata GetArrayElementNestedMetadata()
        {
            if (ArrayElementValueKind != MemberValueKind.Object)
                throw new InvalidOperationException($"Member '{Member.Name}' does not contain object array elements.");

            return _arrayElementNestedMetadata ??= TypeMetadataCache.GetMetadata(ArrayElementEffectiveType);
        }

        [UnconditionalSuppressMessage(
            "Aot",
            "IL3050",
            Justification = "Binary collection array materialization is only used from the reflection-based decode path, which already requires dynamic code.")]
        internal object MaterializeArray(object?[] values)
        {
            switch (_collectionContainerKind)
            {
                case CollectionContainerKind.Array:
                    Array array = Array.CreateInstance(ArrayElementType, values.Length);
                    for (int i = 0; i < values.Length; i++)
                        array.SetValue(values[i], i);
                    return array;

                case CollectionContainerKind.List:
                case CollectionContainerKind.Interface:
                    Type listType = typeof(List<>).MakeGenericType(ArrayElementType);
                    var list = (IList)Activator.CreateInstance(listType)!;
                    for (int i = 0; i < values.Length; i++)
                        list.Add(values[i]);
                    return list;

                default:
                    throw new NotSupportedException(
                        $"Member '{Member.Name}' is not supported for binary collection storage.");
            }
        }

        private static MemberGetter CreateGetter(MemberInfo member)
        {
            var instanceParameter = Expression.Parameter(typeof(object), "instance");
            Expression typedInstance = Expression.Convert(instanceParameter, member.DeclaringType!);
            Expression memberAccess = member switch
            {
                PropertyInfo property => Expression.Property(typedInstance, property),
                FieldInfo field => Expression.Field(typedInstance, field),
                _ => throw new InvalidOperationException($"Member '{member.Name}' is not supported for binary collection storage."),
            };

            Expression body = Expression.Convert(memberAccess, typeof(object));
            return Expression.Lambda<MemberGetter>(body, instanceParameter).Compile();
        }

        private static MemberSetter? CreateSetter(MemberInfo member, Type memberType)
        {
            if (member is PropertyInfo property)
            {
                if (property.SetMethod == null)
                    return null;

                var instanceParameter = Expression.Parameter(typeof(object), "instance");
                var valueParameter = Expression.Parameter(typeof(object), "value");
                Expression assign = Expression.Assign(
                    Expression.Property(Expression.Convert(instanceParameter, property.DeclaringType!), property),
                    Expression.Convert(valueParameter, memberType));
                return Expression.Lambda<MemberSetter>(assign, instanceParameter, valueParameter).Compile();
            }

            if (member is FieldInfo field && !field.IsInitOnly)
            {
                var instanceParameter = Expression.Parameter(typeof(object), "instance");
                var valueParameter = Expression.Parameter(typeof(object), "value");
                Expression assign = Expression.Assign(
                    Expression.Field(Expression.Convert(instanceParameter, field.DeclaringType!), field),
                    Expression.Convert(valueParameter, memberType));
                return Expression.Lambda<MemberSetter>(assign, instanceParameter, valueParameter).Compile();
            }

            return null;
        }
    }

    [UnconditionalSuppressMessage(
        "TrimAnalysis",
        "IL2067",
        Justification = "Binary collection decoding intentionally creates uninitialized value-type defaults inside the reflection-based decode path, which is already surfaced through RequiresUnreferencedCode/RequiresDynamicCode APIs.")]
    private static object CreateDefaultValueInstance(Type type)
        => RuntimeHelpers.GetUninitializedObject(type);
}
