using System.Globalization;

namespace CSharpDB.Core;

/// <summary>
/// A dynamically-typed database value (discriminated union).
/// </summary>
public readonly struct DbValue : IEquatable<DbValue>
{
    public DbType Type { get; }

    private readonly long _intValue;
    private readonly double _realValue;
    private readonly object? _refValue; // string or byte[]

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
    public static DbValue FromText(string value) => new(DbType.Text, refVal: value ?? throw new ArgumentNullException(nameof(value)));
    public static DbValue FromBlob(byte[] value) => new(DbType.Blob, refVal: value ?? throw new ArgumentNullException(nameof(value)));

    public bool IsNull => Type == DbType.Null;

    public long AsInteger => Type == DbType.Integer ? _intValue
        : throw new InvalidOperationException($"Cannot read {Type} as Integer.");

    public double AsReal => Type == DbType.Real ? _realValue
        : Type == DbType.Integer ? _intValue // implicit promotion
        : throw new InvalidOperationException($"Cannot read {Type} as Real.");

    public string AsText => Type == DbType.Text ? (string)_refValue!
        : throw new InvalidOperationException($"Cannot read {Type} as Text.");

    public byte[] AsBlob => Type == DbType.Blob ? (byte[])_refValue!
        : throw new InvalidOperationException($"Cannot read {Type} as Blob.");

    /// <summary>
    /// Compare two DbValues for ordering. NULLs sort first.
    /// </summary>
    public static int Compare(DbValue a, DbValue b)
    {
        if (a.IsNull && b.IsNull) return 0;
        if (a.IsNull) return -1;
        if (b.IsNull) return 1;

        // Numeric comparison (Integer and Real are comparable)
        if (a.Type is DbType.Integer or DbType.Real && b.Type is DbType.Integer or DbType.Real)
        {
            if (a.Type == DbType.Integer && b.Type == DbType.Integer)
                return a._intValue.CompareTo(b._intValue);
            if (a.Type == DbType.Real && b.Type == DbType.Real)
                return a._realValue.CompareTo(b._realValue);
            return a.AsReal.CompareTo(b.AsReal);
        }

        if (a.Type != b.Type)
            return a.Type.CompareTo(b.Type); // deterministic but arbitrary cross-type order

        return a.Type switch
        {
            DbType.Text => string.Compare(a.AsText, b.AsText, StringComparison.Ordinal),
            DbType.Blob => CompareBlobs(a.AsBlob, b.AsBlob),
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

    public bool Equals(DbValue other)
    {
        if (Type != other.Type) return false;
        return Type switch
        {
            DbType.Null => true,
            DbType.Integer => _intValue == other._intValue,
            DbType.Real => _realValue == other._realValue,
            DbType.Text => AsText == other.AsText,
            DbType.Blob => AsBlob.AsSpan().SequenceEqual(other.AsBlob),
            _ => false,
        };
    }

    public override bool Equals(object? obj) => obj is DbValue other && Equals(other);

    public override int GetHashCode() => Type switch
    {
        DbType.Null => 0,
        DbType.Integer => HashCode.Combine(Type, _intValue),
        DbType.Real => HashCode.Combine(Type, _realValue),
        DbType.Text => HashCode.Combine(Type, AsText),
        DbType.Blob => HashCode.Combine(Type, AsBlob.Length),
        _ => 0,
    };

    public static bool operator ==(DbValue left, DbValue right) => left.Equals(right);
    public static bool operator !=(DbValue left, DbValue right) => !left.Equals(right);

    public override string ToString() => Type switch
    {
        DbType.Null => "NULL",
        DbType.Integer => _intValue.ToString(CultureInfo.InvariantCulture),
        DbType.Real => _realValue.ToString(CultureInfo.InvariantCulture),
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
        DbType.Text => true,
        DbType.Blob => true,
        _ => false,
    };
}
