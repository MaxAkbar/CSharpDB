using System.Globalization;
using System.Numerics;

namespace CSharpDB.Primitives;

/// <summary>
/// A dynamically-typed database value (discriminated union).
/// </summary>
public readonly struct DbValue : IEquatable<DbValue>
{
    private const long MaximumDecimalCoefficient = 999_999_999_999_999_999L;

    public DbType Type { get; }

    private readonly long _intValue;
    private readonly double _realValue;
    private readonly object? _refValue; // string, byte[], or BitStringValue

    private sealed class BitStringValue
    {
        public BitStringValue(byte[] packedBytes, int bitLength)
        {
            PackedBytes = packedBytes;
            BitLength = bitLength;
        }

        public byte[] PackedBytes { get; }
        public int BitLength { get; }
    }

    private DbValue(DbType type, long intVal = 0, double realVal = 0, object? refVal = null)
    {
        Type = type;
        _intValue = intVal;
        _realValue = realVal;
        _refValue = refVal;
    }

    public static readonly DbValue Null = new(DbType.Null);

    public static DbValue FromInteger(long value) => new(DbType.Integer, intVal: value);
    public static DbValue FromReal(double value) => new(DbType.Real, realVal: value);
    public static DbValue FromDecimal(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int scale = (bits[3] >> 16) & 0xFF;
        var coefficient =
            ((BigInteger)(uint)bits[2] << 64) |
            ((BigInteger)(uint)bits[1] << 32) |
            (uint)bits[0];
        if ((bits[3] & int.MinValue) != 0)
            coefficient = -coefficient;

        while (scale > 0 && coefficient % 10 == 0)
        {
            coefficient /= 10;
            scale--;
        }

        if (coefficient < -MaximumDecimalCoefficient ||
            coefficient > MaximumDecimalCoefficient ||
            scale > SqlTypeDescriptor.MaximumDecimalPrecision)
        {
            throw new OverflowException(
                $"Decimal value exceeds the supported precision of {SqlTypeDescriptor.MaximumDecimalPrecision} digits.");
        }

        return FromDecimalParts((long)coefficient, scale);
    }

    /// <summary>
    /// Creates an exact decimal value from coefficient × 10^(-scale). Trailing
    /// coefficient zeroes are removed so equal decimals have one representation.
    /// </summary>
    public static DbValue FromDecimalParts(long coefficient, int scale)
    {
        if (scale < 0)
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "Decimal scale cannot be negative.");

        while (scale > 0 && coefficient % 10 == 0)
        {
            coefficient /= 10;
            scale--;
        }

        if (coefficient == 0)
            scale = 0;

        if (coefficient < -MaximumDecimalCoefficient ||
            coefficient > MaximumDecimalCoefficient)
        {
            throw new OverflowException(
                $"Decimal coefficient exceeds the supported precision of {SqlTypeDescriptor.MaximumDecimalPrecision} digits.");
        }
        if (scale > SqlTypeDescriptor.MaximumDecimalPrecision)
        {
            throw new ArgumentOutOfRangeException(
                nameof(scale),
                scale,
                $"Decimal scale cannot exceed {SqlTypeDescriptor.MaximumDecimalPrecision}.");
        }

        return new DbValue(DbType.Decimal, intVal: coefficient, realVal: scale);
    }

    public static DbValue FromText(string value) => new(DbType.Text, refVal: value ?? throw new ArgumentNullException(nameof(value)));
    public static DbValue FromBlob(byte[] value) => new(DbType.Blob, refVal: value ?? throw new ArgumentNullException(nameof(value)));

    /// <summary>
    /// Creates a packed SQL bit string while retaining its exact bit length.
    /// The public physical type remains <see cref="DbType.Blob"/> so existing
    /// binary materialization APIs continue to expose the packed bytes.
    /// </summary>
    public static DbValue FromBitString(byte[] packedBytes, int bitLength)
    {
        ArgumentNullException.ThrowIfNull(packedBytes);
        if (bitLength <= 0)
            throw new ArgumentOutOfRangeException(nameof(bitLength), "Bit strings must contain at least one bit.");

        int expectedByteLength = checked((bitLength + 7) / 8);
        if (packedBytes.Length != expectedByteLength)
        {
            throw new ArgumentException(
                $"A {bitLength}-bit value requires exactly {expectedByteLength} packed bytes.",
                nameof(packedBytes));
        }

        int unusedBits = (expectedByteLength * 8) - bitLength;
        if (unusedBits > 0 && (packedBytes[^1] & ((1 << unusedBits) - 1)) != 0)
            throw new ArgumentException("Unused trailing bits in a packed bit string must be zero.", nameof(packedBytes));

        return new DbValue(DbType.Blob, refVal: new BitStringValue(packedBytes, bitLength));
    }

    public bool IsNull => Type == DbType.Null;

    public long AsInteger => Type == DbType.Integer ? _intValue
        : throw new InvalidOperationException($"Cannot read {Type} as Integer.");

    public double AsReal => Type == DbType.Real ? _realValue
        : Type == DbType.Integer ? _intValue // implicit promotion
        : Type == DbType.Decimal ? (double)AsDecimal
        : throw new InvalidOperationException($"Cannot read {Type} as Real.");

    public decimal AsDecimal
    {
        get
        {
            EnsureDecimal();
            ulong magnitude = _intValue < 0
                ? (ulong)(-_intValue)
                : (ulong)_intValue;
            return new decimal(
                (int)(uint)magnitude,
                (int)(uint)(magnitude >> 32),
                0,
                _intValue < 0,
                (byte)DecimalScale);
        }
    }

    public long DecimalCoefficient
    {
        get
        {
            EnsureDecimal();
            return _intValue;
        }
    }

    public int DecimalScale
    {
        get
        {
            EnsureDecimal();
            return (int)_realValue;
        }
    }

    /// <summary>Compatibility alias for the normalized decimal scale.</summary>
    public int Scale => DecimalScale;

    public string AsText => Type == DbType.Text ? (string)_refValue!
        : throw new InvalidOperationException($"Cannot read {Type} as Text.");

    public byte[] AsBlob => Type == DbType.Blob
        ? _refValue switch
        {
            byte[] blob => blob,
            BitStringValue bits => bits.PackedBytes,
            _ => throw new InvalidOperationException("Malformed Blob value."),
        }
        : throw new InvalidOperationException($"Cannot read {Type} as Blob.");

    /// <summary>True when this BLOB payload represents a logical SQL bit string.</summary>
    public bool IsBitString => Type == DbType.Blob && _refValue is BitStringValue;

    /// <summary>The exact logical length of a SQL bit string.</summary>
    public int BitLength => _refValue is BitStringValue bits
        ? bits.BitLength
        : throw new InvalidOperationException("This Blob value is not a SQL bit string.");

    /// <summary>Returns a SQL bit string as its exact sequence of 0 and 1 characters.</summary>
    public string AsBitString
    {
        get
        {
            if (_refValue is not BitStringValue bits)
                throw new InvalidOperationException("This Blob value is not a SQL bit string.");

            return string.Create(bits.BitLength, bits, static (destination, value) =>
            {
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (value.PackedBytes[i / 8] & (1 << (7 - (i % 8)))) != 0
                        ? '1'
                        : '0';
                }
            });
        }
    }

    /// <summary>
    /// Compare two DbValues for ordering. NULLs sort first.
    /// </summary>
    public static int Compare(DbValue a, DbValue b)
    {
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        // All numeric representations share one exact comparison domain.
        if (IsNumeric(a.Type) && IsNumeric(b.Type))
            return CompareNumeric(a, b);

        if (a.Type != b.Type)
            return a.Type.CompareTo(b.Type); // deterministic but arbitrary cross-type order

        return a.Type switch
        {
            DbType.Text => string.Compare(a.AsText, b.AsText, StringComparison.Ordinal),
            DbType.Blob => CompareBlobValues(a, b),
            _ => 0,
        };
    }

    private static int CompareBlobs(byte[] a, byte[] b)
    {
        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int cmp = a[i].CompareTo(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.Length.CompareTo(b.Length);
    }

    private static int CompareBlobValues(DbValue a, DbValue b)
    {
        if (a.IsBitString != b.IsBitString)
            return a.IsBitString ? 1 : -1;

        int comparison = CompareBlobs(a.AsBlob, b.AsBlob);
        if (comparison != 0 || !a.IsBitString)
            return comparison;

        return a.BitLength.CompareTo(b.BitLength);
    }

    public bool Equals(DbValue other)
    {
        if (IsNumeric(Type) && IsNumeric(other.Type))
            return CompareNumeric(this, other) == 0;
        if (Type != other.Type) return false;
        return Type switch
        {
            DbType.Null => true,
            DbType.Text => AsText == other.AsText,
            DbType.Blob =>
                IsBitString == other.IsBitString &&
                (!IsBitString || BitLength == other.BitLength) &&
                AsBlob.AsSpan().SequenceEqual(other.AsBlob),
            _ => false,
        };
    }

    public override bool Equals(object? obj) => obj is DbValue other && Equals(other);

    public override int GetHashCode() => Type switch
    {
        DbType.Null => 0,
        DbType.Integer or DbType.Real or DbType.Decimal => GetNumericHashCode(this),
        DbType.Text => HashCode.Combine(Type, AsText),
        DbType.Blob => GetBlobHashCode(this),
        _ => 0,
    };

    public static bool operator ==(DbValue left, DbValue right) => left.Equals(right);
    public static bool operator !=(DbValue left, DbValue right) => !left.Equals(right);

    public override string ToString() => Type switch
    {
        DbType.Null => "NULL",
        DbType.Integer => _intValue.ToString(CultureInfo.InvariantCulture),
        DbType.Real => _realValue.ToString(CultureInfo.InvariantCulture),
        DbType.Decimal => AsDecimal.ToString(CultureInfo.InvariantCulture),
        DbType.Text => AsText,
        DbType.Blob => $"BLOB({AsBlob.Length} bytes)",
        _ => "?",
    };

    /// <summary>
    /// Returns true if this value is "truthy" for WHERE clause evaluation.
    /// NULL is falsy. Zero is falsy. Empty string is truthy (SQL semantics).
    /// </summary>
    public bool IsTruthy => Type switch
    {
        DbType.Null => false,
        DbType.Integer => _intValue != 0,
        DbType.Real => _realValue != 0.0,
        DbType.Decimal => _intValue != 0,
        DbType.Text => true,
        DbType.Blob => true,
        _ => false,
    };

    private void EnsureDecimal()
    {
        if (Type != DbType.Decimal)
            throw new InvalidOperationException($"Cannot read {Type} as Decimal.");
    }

    private static bool IsNumeric(DbType type) =>
        type is DbType.Integer or DbType.Real or DbType.Decimal;

    private static int CompareNumeric(DbValue a, DbValue b)
    {
        if (a.Type == DbType.Integer && b.Type == DbType.Integer)
            return a._intValue.CompareTo(b._intValue);
        if (a.Type == DbType.Real && b.Type == DbType.Real)
            return a._realValue.CompareTo(b._realValue);
        if (a.Type == DbType.Decimal && b.Type == DbType.Decimal &&
            a.DecimalScale == b.DecimalScale)
        {
            return a._intValue.CompareTo(b._intValue);
        }

        if (a.Type == DbType.Real && !double.IsFinite(a._realValue))
            return CompareNonFiniteReal(a._realValue, b);
        if (b.Type == DbType.Real && !double.IsFinite(b._realValue))
            return -CompareNonFiniteReal(b._realValue, a);

        GetNumericRational(a, out BigInteger aNumerator, out BigInteger aDenominator);
        GetNumericRational(b, out BigInteger bNumerator, out BigInteger bDenominator);
        return (aNumerator * bDenominator).CompareTo(bNumerator * aDenominator);
    }

    private static int CompareNonFiniteReal(double value, DbValue other)
    {
        if (double.IsNaN(value))
            return other.Type == DbType.Real && double.IsNaN(other._realValue) ? 0 : -1;
        if (double.IsNegativeInfinity(value))
        {
            return other.Type == DbType.Real && double.IsNegativeInfinity(other._realValue)
                ? 0
                : -1;
        }

        return other.Type == DbType.Real && double.IsPositiveInfinity(other._realValue)
            ? 0
            : 1;
    }

    private static int GetNumericHashCode(DbValue value)
    {
        if (value.Type == DbType.Real && !double.IsFinite(value._realValue))
        {
            if (double.IsNaN(value._realValue))
                return HashCode.Combine(0x4E554D, "NaN");
            return HashCode.Combine(
                0x4E554D,
                double.IsPositiveInfinity(value._realValue) ? "Infinity" : "-Infinity");
        }

        GetNumericRational(value, out BigInteger numerator, out BigInteger denominator);
        return HashCode.Combine(0x4E554D, numerator, denominator);
    }

    private static int GetBlobHashCode(DbValue value)
    {
        var hash = new HashCode();
        hash.Add(DbType.Blob);
        hash.Add(value.IsBitString);
        if (value.IsBitString)
            hash.Add(value.BitLength);
        foreach (byte item in value.AsBlob)
            hash.Add(item);
        return hash.ToHashCode();
    }

    private static void GetNumericRational(
        DbValue value,
        out BigInteger numerator,
        out BigInteger denominator)
    {
        switch (value.Type)
        {
            case DbType.Integer:
                numerator = value._intValue;
                denominator = BigInteger.One;
                return;
            case DbType.Decimal:
                numerator = value._intValue;
                denominator = BigInteger.Pow(10, value.DecimalScale);
                ReduceRational(ref numerator, ref denominator);
                return;
            case DbType.Real:
                GetDoubleRational(value._realValue, out numerator, out denominator);
                return;
            default:
                throw new InvalidOperationException($"{value.Type} is not numeric.");
        }
    }

    private static void GetDoubleRational(
        double value,
        out BigInteger numerator,
        out BigInteger denominator)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);
        bool negative = bits < 0;
        int exponentBits = (int)((bits >> 52) & 0x7FF);
        long fraction = bits & 0x000F_FFFF_FFFF_FFFFL;

        long significand;
        int binaryExponent;
        if (exponentBits == 0)
        {
            significand = fraction;
            binaryExponent = -1074;
        }
        else
        {
            significand = fraction | (1L << 52);
            binaryExponent = exponentBits - 1023 - 52;
        }

        numerator = negative ? -significand : significand;
        if (binaryExponent >= 0)
        {
            numerator <<= binaryExponent;
            denominator = BigInteger.One;
        }
        else
        {
            denominator = BigInteger.One << -binaryExponent;
            ReduceRational(ref numerator, ref denominator);
        }
    }

    private static void ReduceRational(
        ref BigInteger numerator,
        ref BigInteger denominator)
    {
        if (numerator.IsZero)
        {
            denominator = BigInteger.One;
            return;
        }

        BigInteger divisor = BigInteger.GreatestCommonDivisor(
            BigInteger.Abs(numerator),
            denominator);
        numerator /= divisor;
        denominator /= divisor;
    }
}
