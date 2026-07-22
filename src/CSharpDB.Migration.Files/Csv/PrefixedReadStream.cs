namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Replays bytes read during BOM detection, then delegates to the original
/// stream. Disposing this wrapper never disposes the caller-owned stream.
/// </summary>
internal sealed class PrefixedReadStream : Stream
{
    private readonly Stream inner;
    private readonly byte[] prefix;
    private int prefixOffset;
    private bool disposed;

    public PrefixedReadStream(Stream inner, byte[] prefix)
    {
        this.inner = inner;
        this.prefix = prefix;
    }

    public override bool CanRead => !disposed && inner.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        int copied = CopyPrefix(buffer);
        return copied > 0 ? copied : inner.Read(buffer);
    }

    public override int ReadByte()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (prefixOffset < prefix.Length)
            return prefix[prefixOffset++];
        return inner.ReadByte();
    }

    public override Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        int copied = CopyPrefix(buffer.Span);
        return copied > 0
            ? ValueTask.FromResult(copied)
            : inner.ReadAsync(buffer, cancellationToken);
    }

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        disposed = true;
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        disposed = true;
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private int CopyPrefix(Span<byte> destination)
    {
        int count = Math.Min(destination.Length, prefix.Length - prefixOffset);
        if (count <= 0)
            return 0;

        prefix.AsSpan(prefixOffset, count).CopyTo(destination);
        prefixOffset += count;
        return count;
    }
}
