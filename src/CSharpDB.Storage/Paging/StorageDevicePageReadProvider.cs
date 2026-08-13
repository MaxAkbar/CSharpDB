using CSharpDB.Storage.Device;

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
        return PageReadBuffer.FromOwnedBuffer(await ReadOwnedPageAsync(pageId, ct));
    }

    public async ValueTask<byte[]> ReadOwnedPageAsync(uint pageId, CancellationToken ct = default)
    {
        byte[] buffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        long offset = (long)pageId * PageConstants.PageSize;
        if (_useSequentialAccessHint && _fileDevice is not null)
            await _fileDevice.ReadSequentialAsync(offset, buffer, ct);
        else
            await _device.ReadAsync(offset, buffer, ct);
        return buffer;
    }
}
