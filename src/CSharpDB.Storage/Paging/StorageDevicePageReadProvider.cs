using CSharpDB.Storage.Device;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Default page read provider backed by the existing <see cref="IStorageDevice"/>
/// copy-based read path.
/// </summary>
internal sealed class StorageDevicePageReadProvider : IPageReadProvider
{
    private readonly IStorageDevice _device;
    private readonly FileStorageDevice? _fileDevice;
    private readonly bool _useSequentialAccessHint;

    public StorageDevicePageReadProvider(IStorageDevice device, bool useSequentialAccessHint = false)
    {
        _device = device ?? throw new ArgumentNullException(nameof(device));
        _fileDevice = device as FileStorageDevice;
        _useSequentialAccessHint = useSequentialAccessHint;
    }

    public async ValueTask<PageReadBuffer> ReadPageAsync(uint pageId, CancellationToken ct = default)
    {
        byte[] buffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        long offset = (long)pageId * PageConstants.PageSize;
        if (_useSequentialAccessHint && _fileDevice is not null)
            await ReadFromHandleAsync(_fileDevice.SequentialReadHandle, offset, buffer, ct);
        else
            await _device.ReadAsync(offset, buffer, ct);
        return PageReadBuffer.FromOwnedBuffer(buffer);
    }

    private static async ValueTask<int> ReadFromHandleAsync(
        SafeFileHandle handle,
        long offset,
        Memory<byte> buffer,
        CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int read = await RandomAccess.ReadAsync(handle, buffer[totalRead..], offset + totalRead, ct);
            if (read == 0)
                break;

            totalRead += read;
        }

        if (totalRead < buffer.Length)
            buffer[totalRead..].Span.Clear();

        return totalRead;
    }
}
