namespace CSharpDB.Migration.Validation;

/// <summary>
/// A fixed-width validation record containing two raw SHA-256 hashes.
/// Records are ordered by the unsigned lexicographic order of all 64 bytes.
/// </summary>
public sealed class ValidationHashRecord :
    IComparable<ValidationHashRecord>,
    IEquatable<ValidationHashRecord>
{
    public const int HashLength = 32;
    public const int SerializedLength = HashLength * 2;

    private readonly byte[] _bytes;

    public ValidationHashRecord(ReadOnlySpan<byte> firstHash, ReadOnlySpan<byte> secondHash)
    {
        ValidateHashLength(firstHash, nameof(firstHash));
        ValidateHashLength(secondHash, nameof(secondHash));

        _bytes = GC.AllocateUninitializedArray<byte>(SerializedLength);
        firstHash.CopyTo(_bytes);
        secondHash.CopyTo(_bytes.AsSpan(HashLength));
    }

    private ValidationHashRecord(ReadOnlySpan<byte> serializedRecord)
    {
        if (serializedRecord.Length != SerializedLength)
        {
            throw new ArgumentException(
                $"A validation hash record must contain exactly {SerializedLength} bytes.",
                nameof(serializedRecord));
        }

        _bytes = serializedRecord.ToArray();
    }

    public ReadOnlyMemory<byte> FirstHash => _bytes.AsMemory(0, HashLength);

    public ReadOnlyMemory<byte> SecondHash => _bytes.AsMemory(HashLength, HashLength);

    public static ValidationHashRecord FromBytes(ReadOnlySpan<byte> serializedRecord)
        => new(serializedRecord);

    public byte[] ToArray() => (byte[])_bytes.Clone();

    public void CopyTo(Span<byte> destination)
    {
        if (destination.Length < SerializedLength)
        {
            throw new ArgumentException(
                $"The destination must have room for {SerializedLength} bytes.",
                nameof(destination));
        }

        _bytes.CopyTo(destination);
    }

    public int CompareTo(ValidationHashRecord? other)
        => other is null ? 1 : _bytes.AsSpan().SequenceCompareTo(other._bytes);

    public bool Equals(ValidationHashRecord? other)
        => other is not null && _bytes.AsSpan().SequenceEqual(other._bytes);

    public override bool Equals(object? obj) => Equals(obj as ValidationHashRecord);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        for (int offset = 0; offset < SerializedLength; offset += sizeof(long))
            hash.Add(BitConverter.ToInt64(_bytes, offset));
        return hash.ToHashCode();
    }

    internal ReadOnlySpan<byte> AsSpan() => _bytes;

    internal static int CompareSerialized(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        if (left.Length != SerializedLength || right.Length != SerializedLength)
            throw new ArgumentException("Validation hash records must be exactly 64 bytes.");

        return left.SequenceCompareTo(right);
    }

    private static void ValidateHashLength(ReadOnlySpan<byte> hash, string parameterName)
    {
        if (hash.Length != HashLength)
        {
            throw new ArgumentException(
                $"A SHA-256 hash must contain exactly {HashLength} bytes.",
                parameterName);
        }
    }
}
