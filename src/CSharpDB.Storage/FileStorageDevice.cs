using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Storage;

public sealed class FileStorageDevice : IStorageDevice
{
    private readonly SafeFileHandle _handle;

    public FileStorageDevice(string filePath, bool createNew = false)
    {
        _handle = File.OpenHandle(
            filePath,
            createNew ? FileMode.CreateNew : FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            FileOptions.Asynchronous | FileOptions.RandomAccess);
    }

    public long Length => RandomAccess.GetLength(_handle);

    public async ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await RandomAccess.ReadAsync(_handle, buffer[totalRead..], offset + totalRead, ct);
            if (read == 0) break;
            totalRead += read;
        }
        // Zero-fill any unread portion (reading past end of file)
        if (totalRead < buffer.Length)
            buffer[totalRead..].Span.Clear();
        return totalRead;
    }

    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        return RandomAccess.WriteAsync(_handle, buffer, offset, ct);
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        RandomAccess.FlushToDisk(_handle);
        return ValueTask.CompletedTask;
    }

    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        RandomAccess.SetLength(_handle, length);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _handle.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        _handle.Dispose();
    }
}
