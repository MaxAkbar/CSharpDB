namespace CSharpDB.Storage.Paging;

/// <summary>
/// Represents a page-sized read result. It can wrap an owned <see cref="byte[]"/>
/// or a read-only view that must be materialized before mutation/caching.
/// </summary>
internal readonly struct PageReadBuffer
{
    private readonly byte[]? _ownedBuffer;

    private PageReadBuffer(ReadOnlyMemory<byte> memory, byte[]? ownedBuffer)
    {
        Memory = memory;
        _ownedBuffer = ownedBuffer;
    }

    public ReadOnlyMemory<byte> Memory { get; }

    public static PageReadBuffer FromOwnedBuffer(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return new PageReadBuffer(buffer, buffer);
    }

    public static PageReadBuffer FromReadOnlyMemory(ReadOnlyMemory<byte> memory)
        => new(memory, ownedBuffer: null);

    public bool TryGetOwnedBuffer(out byte[]? buffer)
    {
        buffer = _ownedBuffer;
        return buffer is not null;
    }

    public byte[] MaterializeOwnedBuffer()
    {
        if (_ownedBuffer is not null)
            return _ownedBuffer;

        byte[] copy = GC.AllocateUninitializedArray<byte>(Memory.Length);
        Memory.Span.CopyTo(copy);
        return copy;
    }
}
