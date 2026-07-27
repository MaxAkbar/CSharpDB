using System.Numerics;

namespace CSharpDB.Migration.Canonicalization;

/// <summary>
/// One typed logical field in a canonical row. Factory methods preserve the
/// distinction between a typed NULL, a value, and a registered exclusion.
/// </summary>
public readonly struct CanonicalValue
{
    private const ulong NanosecondsPerTick = 100;

    private readonly object? _reference;
    private readonly ulong _bits0;
    private readonly ulong _bits1;

    private CanonicalValue(
        CanonicalType type,
        CanonicalFieldState state,
        ulong bits0 = 0,
        ulong bits1 = 0,
        object? reference = null)
    {
        Type = type;
        State = state;
        _bits0 = bits0;
        _bits1 = bits1;
        _reference = reference;
    }

    public CanonicalType Type { get; }

    public CanonicalFieldState State { get; }

    public static CanonicalValue Null(CanonicalType type)
    {
        ValidateType(type);
        return new CanonicalValue(type, CanonicalFieldState.Null);
    }

    public static CanonicalValue RegeneratedRowVersion() =>
        new(
            CanonicalType.Blob,
            CanonicalFieldState.Excluded,
            bits0: (byte)CanonicalExclusionReason.RegeneratedRowVersion);

    public static CanonicalValue Boolean(bool value) =>
        new(CanonicalType.Boolean, CanonicalFieldState.Value, bits0: value ? 1UL : 0UL);

    public static CanonicalValue Int64(long value) =>
        new(CanonicalType.Int64, CanonicalFieldState.Value, bits0: unchecked((ulong)value));

    public static CanonicalValue UInt64(ulong value) =>
        new(CanonicalType.UInt64, CanonicalFieldState.Value, bits0: value);

    public static CanonicalValue Decimal(BigInteger coefficient, uint scale) =>
        Decimal(new CanonicalDecimal(coefficient, scale));

    public static CanonicalValue Decimal(decimal value) =>
        Decimal(new CanonicalDecimal(value));

    public static CanonicalValue Decimal(CanonicalDecimal value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new CanonicalValue(CanonicalType.Decimal, CanonicalFieldState.Value, reference: value);
    }

    public static CanonicalValue Binary32(float value)
    {
        if (!float.IsFinite(value))
            throw new ArgumentOutOfRangeException(nameof(value), "Canonical REAL values must be finite.");

        uint bits = value == 0F ? 0U : BitConverter.SingleToUInt32Bits(value);
        return new CanonicalValue(CanonicalType.Binary32, CanonicalFieldState.Value, bits0: bits);
    }

    public static CanonicalValue Binary64(double value)
    {
        if (!double.IsFinite(value))
            throw new ArgumentOutOfRangeException(nameof(value), "Canonical REAL values must be finite.");

        ulong bits = value == 0D ? 0UL : BitConverter.DoubleToUInt64Bits(value);
        return new CanonicalValue(CanonicalType.Binary64, CanonicalFieldState.Value, bits0: bits);
    }

    public static CanonicalValue Text(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new CanonicalValue(CanonicalType.Text, CanonicalFieldState.Value, reference: value);
    }

    public static CanonicalValue Blob(ReadOnlyMemory<byte> value) =>
        new(CanonicalType.Blob, CanonicalFieldState.Value, reference: value);

    public static CanonicalValue Guid(Guid value) =>
        new(CanonicalType.Guid, CanonicalFieldState.Value, reference: value);

    public static CanonicalValue Date(int daysSinceUnixEpoch) =>
        new(CanonicalType.Date, CanonicalFieldState.Value, bits0: unchecked((uint)daysSinceUnixEpoch));

    public static CanonicalValue Date(DateOnly value) =>
        Date(checked(value.DayNumber - new DateOnly(1970, 1, 1).DayNumber));

    public static CanonicalValue Time(ulong nanosecondsSinceMidnight)
    {
        ValidateNanosecondsSinceMidnight(nanosecondsSinceMidnight);
        return new CanonicalValue(CanonicalType.Time, CanonicalFieldState.Value, bits0: nanosecondsSinceMidnight);
    }

    public static CanonicalValue Time(TimeOnly value) =>
        Time(checked((ulong)value.Ticks * NanosecondsPerTick));

    public static CanonicalValue WallDateTime(int daysSinceUnixEpoch, ulong nanosecondsSinceMidnight)
    {
        ValidateNanosecondsSinceMidnight(nanosecondsSinceMidnight);
        return new CanonicalValue(
            CanonicalType.WallDateTime,
            CanonicalFieldState.Value,
            bits0: unchecked((uint)daysSinceUnixEpoch),
            bits1: nanosecondsSinceMidnight);
    }

    public static CanonicalValue WallDateTime(DateTime value)
    {
        if (value.Kind != DateTimeKind.Unspecified)
        {
            throw new ArgumentException(
                "A canonical wall date-time must have DateTimeKind.Unspecified; it has no implicit time zone.",
                nameof(value));
        }

        int days = checked(DateOnly.FromDateTime(value).DayNumber - new DateOnly(1970, 1, 1).DayNumber);
        ulong nanoseconds = checked((ulong)value.TimeOfDay.Ticks * NanosecondsPerTick);
        return WallDateTime(days, nanoseconds);
    }

    public static CanonicalValue UtcInstant(long unixSeconds, uint nanoseconds)
    {
        ValidateFractionalNanoseconds(nanoseconds);
        return new CanonicalValue(
            CanonicalType.UtcInstant,
            CanonicalFieldState.Value,
            bits0: unchecked((ulong)unixSeconds),
            bits1: nanoseconds);
    }

    public static CanonicalValue UtcInstant(DateTimeOffset value)
    {
        const long unixEpochTicks = 621_355_968_000_000_000;
        long deltaTicks = value.UtcDateTime.Ticks - unixEpochTicks;
        long seconds = Math.DivRem(deltaTicks, TimeSpan.TicksPerSecond, out long remainderTicks);
        if (remainderTicks < 0)
        {
            seconds--;
            remainderTicks += TimeSpan.TicksPerSecond;
        }

        return UtcInstant(seconds, checked((uint)(remainderTicks * (long)NanosecondsPerTick)));
    }

    public static CanonicalValue OffsetDateTime(
        int daysSinceUnixEpoch,
        ulong nanosecondsSinceMidnight,
        short offsetMinutes)
    {
        ValidateNanosecondsSinceMidnight(nanosecondsSinceMidnight);
        return new CanonicalValue(
            CanonicalType.OffsetDateTime,
            CanonicalFieldState.Value,
            bits0: unchecked((uint)daysSinceUnixEpoch) | (unchecked((ulong)(ushort)offsetMinutes) << 32),
            bits1: nanosecondsSinceMidnight);
    }

    public static CanonicalValue OffsetDateTime(DateTimeOffset value)
    {
        int days = checked(DateOnly.FromDateTime(value.DateTime).DayNumber - new DateOnly(1970, 1, 1).DayNumber);
        ulong nanoseconds = checked((ulong)value.TimeOfDay.Ticks * NanosecondsPerTick);
        short offsetMinutes = checked((short)value.Offset.TotalMinutes);
        return OffsetDateTime(days, nanoseconds, offsetMinutes);
    }

    internal ulong Bits0 => _bits0;

    internal ulong Bits1 => _bits1;

    internal T Reference<T>() where T : notnull =>
        _reference is T value
            ? value
            : throw new InvalidDataException($"Canonical {Type} value is missing its logical payload.");

    internal void Validate()
    {
        ValidateType(Type);
        if (State is not (CanonicalFieldState.Value or CanonicalFieldState.Null or CanonicalFieldState.Excluded))
            throw new InvalidDataException($"Unknown canonical field state 0x{(byte)State:x2}.");

        if (State == CanonicalFieldState.Excluded &&
            (Type != CanonicalType.Blob ||
             Bits0 != (byte)CanonicalExclusionReason.RegeneratedRowVersion))
        {
            throw new InvalidDataException("The canonical exclusion is not registered by csharpdb-canon-v1.");
        }
    }

    private static void ValidateType(CanonicalType type)
    {
        if (type is < CanonicalType.Boolean or > CanonicalType.OffsetDateTime)
            throw new ArgumentOutOfRangeException(nameof(type), $"Unknown canonical type tag 0x{(byte)type:x2}.");
    }

    private static void ValidateNanosecondsSinceMidnight(ulong value)
    {
        const ulong nanosecondsPerDay = 86_400_000_000_000;
        if (value >= nanosecondsPerDay)
            throw new ArgumentOutOfRangeException(nameof(value), "Canonical time must be earlier than 24:00:00.");
    }

    private static void ValidateFractionalNanoseconds(uint value)
    {
        if (value >= 1_000_000_000)
            throw new ArgumentOutOfRangeException(nameof(value), "Fractional nanoseconds must be below 1,000,000,000.");
    }
}
