namespace CSharpDB.Client.Models;

/// <summary>
/// A packed SQL BIT or VARBIT value whose logical length may not be a whole
/// number of bytes. Ordinary BLOB values continue to materialize as
/// <see cref="byte[]"/>.
/// </summary>
public sealed class SqlBitString : IEquatable<SqlBitString>
{
    private readonly byte[] _packedBytes;

    public SqlBitString(byte[] packedBytes, int bitLength)
    {
        ArgumentNullException.ThrowIfNull(packedBytes);

        // Reuse the primitive contract so client-created values follow the
        // same byte-count and zero-padding rules as engine values.
        _ = CSharpDB.Primitives.DbValue.FromBitString(packedBytes, bitLength);
        _packedBytes = packedBytes.ToArray();
        BitLength = bitLength;
    }

    public ReadOnlyMemory<byte> PackedBytes => _packedBytes;

    public int BitLength { get; }

    public string ToBitString() => string.Create(
        BitLength,
        this,
        static (destination, value) =>
        {
            ReadOnlySpan<byte> bytes = value._packedBytes;
            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] =
                    (bytes[i / 8] & (1 << (7 - (i % 8)))) != 0
                        ? '1'
                        : '0';
            }
        });

    public bool Equals(SqlBitString? other) =>
        other is not null &&
        BitLength == other.BitLength &&
        _packedBytes.AsSpan().SequenceEqual(other._packedBytes);

    public override bool Equals(object? obj) => Equals(obj as SqlBitString);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(BitLength);
        foreach (byte value in _packedBytes)
            hash.Add(value);
        return hash.ToHashCode();
    }

    public override string ToString() => ToBitString();
}
