namespace CSharpDB.Storage.Device;

public sealed class MemoryStorageDevice : IStorageDevice
{
    private readonly object _gate = new();
    private byte[] _buffer;
    private long _length;

    public MemoryStorageDevice()
        : this(ReadOnlyMemory<byte>.Empty)
    {
    }

    public MemoryStorageDevice(ReadOnlyMemory<byte> initialBytes)
    {
        _buffer = initialBytes.IsEmpty ? Array.Empty<byte>() : initialBytes.ToArray();
        _length = _buffer.Length;
    }

    public long Length
    {
        get
        {
            lock (_gate)
                return _length;
        }
    }

    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset));

        lock (_gate)
        {
            if (buffer.IsEmpty)
                return ValueTask.FromResult(0);

            if (offset >= _length)
            {
                buffer.Span.Clear();
                return ValueTask.FromResult(0);
            }

            int available = (int)Math.Min(buffer.Length, _length - offset);
            new ReadOnlySpan<byte>(_buffer, (int)offset, available).CopyTo(buffer.Span);
            if (available < buffer.Length)
                buffer.Span[available..].Clear();

            return ValueTask.FromResult(available);
        }
    }

    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset));

        lock (_gate)
        {
            long requiredLength = offset + buffer.Length;
            EnsureCapacity(requiredLength);
            buffer.Span.CopyTo(_buffer.AsSpan((int)offset, buffer.Length));
            if (requiredLength > _length)
                _length = requiredLength;
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }

    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));

        lock (_gate)
        {
            EnsureCapacity(length);
            if (length > _length)
                _buffer.AsSpan((int)_length, (int)(length - _length)).Clear();

            _length = length;
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
    }

    private void EnsureCapacity(long requiredLength)
    {
        if (requiredLength <= _buffer.Length)
            return;

        if (requiredLength > Array.MaxLength)
        {
            throw new InvalidOperationException(
                $"Memory storage device exceeded the supported buffer size (requiredLength={requiredLength}, currentLength={_length}, currentBufferLength={_buffer.Length}, requestedCapacity={requiredLength}, maxSupportedLength={Array.MaxLength}).");
        }

        long newLength = _buffer.Length == 0 ? requiredLength : _buffer.Length;
        while (newLength < requiredLength)
        {
            if (newLength > Array.MaxLength / 2)
            {
                newLength = requiredLength;
                break;
            }

            newLength *= 2;
        }

        if (newLength > Array.MaxLength)
        {
            throw new InvalidOperationException(
                $"Memory storage device exceeded the supported buffer size (requiredLength={requiredLength}, currentLength={_length}, currentBufferLength={_buffer.Length}, requestedCapacity={newLength}, maxSupportedLength={Array.MaxLength}).");
        }

        Array.Resize(ref _buffer, (int)newLength);
    }
}
