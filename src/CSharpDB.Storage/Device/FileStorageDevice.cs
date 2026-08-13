using CSharpDB.Storage.Diagnostics;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Storage.Device;

public sealed class FileStorageDevice :
    IStorageDevice,
    IStorageDeviceIoRuntimeDiagnosticsProvider
{
    private readonly SafeFileHandle _handle;
    private readonly string _filePath;
    private readonly Lazy<SafeFileHandle?> _sequentialReadHandle;
    private StorageDeviceIoRuntimeCounters? _runtimeDiagnostics;

    public FileStorageDevice(
        string filePath,
        bool createNew = false,
        FileShare fileShare = FileShare.ReadWrite)
        : this(
            filePath,
            createNew,
            fileShare,
            sequentialReadHandleFactory: null)
    {
    }

    internal FileStorageDevice(
        string filePath,
        bool createNew,
        FileShare fileShare,
        Func<SafeFileHandle?>? sequentialReadHandleFactory)
    {
        _filePath = filePath;
        _handle = File.OpenHandle(
            filePath,
            createNew ? FileMode.CreateNew : FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            EffectiveFileShare(fileShare),
            FileOptions.Asynchronous | FileOptions.RandomAccess);

        _sequentialReadHandle = new Lazy<SafeFileHandle?>(
            sequentialReadHandleFactory ?? CreateSequentialReadHandle,
            isThreadSafe: true);
    }

    public long Length => RandomAccess.GetLength(_handle);

    internal SafeFileHandle Handle => _handle;
    internal SafeFileHandle SequentialReadHandle => _sequentialReadHandle.Value ?? _handle;

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

        Volatile.Read(ref _runtimeDiagnostics)?.RecordRead(totalRead, sequential: false);
        return totalRead;
    }

    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        StorageDeviceIoRuntimeCounters? diagnostics = Volatile.Read(ref _runtimeDiagnostics);
        if (diagnostics is null)
            return RandomAccess.WriteAsync(_handle, buffer, offset, ct);

        ValueTask write = RandomAccess.WriteAsync(_handle, buffer, offset, ct);
        if (write.IsCompletedSuccessfully)
        {
            write.GetAwaiter().GetResult();
            diagnostics.RecordWrite(buffer.Length);
            return ValueTask.CompletedTask;
        }

        return CompleteObservedWriteAsync(write, buffer.Length, diagnostics);
    }

    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        RandomAccess.FlushToDisk(_handle);
        Volatile.Read(ref _runtimeDiagnostics)?.RecordFlush();
        return ValueTask.CompletedTask;
    }

    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        RandomAccess.SetLength(_handle, length);
        Volatile.Read(ref _runtimeDiagnostics)?.RecordResize();
        return ValueTask.CompletedTask;
    }

    internal async ValueTask<int> ReadSequentialAsync(
        long offset,
        Memory<byte> buffer,
        CancellationToken ct = default)
    {
        SafeFileHandle handle = SequentialReadHandle;
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await RandomAccess.ReadAsync(
                handle,
                buffer[totalRead..],
                offset + totalRead,
                ct);
            if (read == 0)
                break;

            totalRead += read;
        }

        if (totalRead < buffer.Length)
            buffer[totalRead..].Span.Clear();

        Volatile.Read(ref _runtimeDiagnostics)?.RecordRead(totalRead, sequential: true);
        return totalRead;
    }

    internal void RecordMemoryMappedPageExposure(long bytesExposed)
        => Volatile.Read(ref _runtimeDiagnostics)?
            .RecordMemoryMappedPageExposure(bytesExposed);

    public ValueTask DisposeAsync()
    {
        if (_sequentialReadHandle.IsValueCreated)
            _sequentialReadHandle.Value?.Dispose();
        _handle.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_sequentialReadHandle.IsValueCreated)
            _sequentialReadHandle.Value?.Dispose();
        _handle.Dispose();
    }

    StorageDeviceIoRuntimeCounters
        IStorageDeviceIoRuntimeDiagnosticsProvider.EnableRuntimeDiagnostics()
    {
        StorageDeviceIoRuntimeCounters? diagnostics =
            Volatile.Read(ref _runtimeDiagnostics);
        if (diagnostics is not null)
            return diagnostics;

        var created = new StorageDeviceIoRuntimeCounters();
        return Interlocked.CompareExchange(
                ref _runtimeDiagnostics,
                created,
                comparand: null)
            ?? created;
    }

    private static async ValueTask CompleteObservedWriteAsync(
        ValueTask write,
        int bytesWritten,
        StorageDeviceIoRuntimeCounters diagnostics)
    {
        await write.ConfigureAwait(false);
        diagnostics.RecordWrite(bytesWritten);
    }

    private SafeFileHandle? CreateSequentialReadHandle()
    {
        try
        {
            return File.OpenHandle(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
        catch
        {
            return null;
        }
    }

    private static FileShare EffectiveFileShare(FileShare fileShare)
    {
        if (OperatingSystem.IsWindows() ||
            (fileShare & FileShare.Write) != 0)
        {
            return fileShare;
        }

        // .NET's Unix FileShare implementation maps every non-None share to a
        // shared flock. Use an exclusive flock when writes are denied so a
        // second database writer cannot bypass the requested writer barrier.
        return FileShare.None;
    }
}
