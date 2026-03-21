using System.Buffers;
using System.IO.MemoryMappedFiles;
using CSharpDB.Storage.Device;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Serves clean file-backed pages from a memory-mapped view when possible.
/// Pages outside the initial mapped file length fall back to the normal
/// copy-based storage read path.
/// </summary>
internal sealed class MemoryMappedPageReadProvider : IPageReadProvider, IDisposable
{
    private readonly FileStorageDevice _device;
    private readonly StorageDevicePageReadProvider _fallback;
    private MemoryMappedFile? _mappedFile;
    private MemoryMappedViewMemoryManager? _mappedMemory;
    private long _mappedLength;

    public MemoryMappedPageReadProvider(FileStorageDevice device)
    {
        ArgumentNullException.ThrowIfNull(device);

        _device = device;
        _fallback = new StorageDevicePageReadProvider(device);
        RefreshMapping();
    }

    public ValueTask<PageReadBuffer> ReadPageAsync(uint pageId, CancellationToken ct = default)
    {
        long offset = (long)pageId * PageConstants.PageSize;
        if (_mappedMemory is not null && offset >= 0 && offset + PageConstants.PageSize <= _mappedLength)
        {
            return ValueTask.FromResult(
                PageReadBuffer.FromReadOnlyMemory(
                    _mappedMemory.Memory.Slice((int)offset, PageConstants.PageSize)));
        }

        return _fallback.ReadPageAsync(pageId, ct);
    }

    public async ValueTask<byte[]> ReadOwnedPageAsync(uint pageId, CancellationToken ct = default)
    {
        long offset = (long)pageId * PageConstants.PageSize;
        if (_mappedMemory is not null && offset >= 0 && offset + PageConstants.PageSize <= _mappedLength)
        {
            byte[] buffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            _mappedMemory.Memory.Span.Slice((int)offset, PageConstants.PageSize).CopyTo(buffer);
            return buffer;
        }

        return await _fallback.ReadOwnedPageAsync(pageId, ct);
    }

    internal void RefreshMapping()
    {
        long mappedLength = _device.Length;
        bool shouldMap = mappedLength > 0 && mappedLength <= int.MaxValue;
        if (mappedLength == _mappedLength && (_mappedMemory is not null) == shouldMap)
            return;

        ResetMapping();
        _mappedLength = mappedLength;

        if (!shouldMap)
            return;

        _mappedFile = MemoryMappedFile.CreateFromFile(
            _device.Handle,
            mapName: null,
            capacity: mappedLength,
            access: MemoryMappedFileAccess.Read,
            inheritability: HandleInheritability.None,
            leaveOpen: true);

        var accessor = _mappedFile.CreateViewAccessor(0, mappedLength, MemoryMappedFileAccess.Read);
        _mappedMemory = new MemoryMappedViewMemoryManager(accessor, checked((int)mappedLength));
    }

    public void Dispose()
    {
        ResetMapping();
    }

    private void ResetMapping()
    {
        _mappedMemory?.Release();
        _mappedMemory = null;
        _mappedFile?.Dispose();
        _mappedFile = null;
    }

    private unsafe sealed class MemoryMappedViewMemoryManager : MemoryManager<byte>
    {
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly int _length;
        private byte* _pointer;
        private bool _disposed;

        public MemoryMappedViewMemoryManager(MemoryMappedViewAccessor accessor, int length)
        {
            _accessor = accessor ?? throw new ArgumentNullException(nameof(accessor));
            _length = length;

            byte* pointer = null;
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            _pointer = pointer;
        }

        public override Span<byte> GetSpan()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return new Span<byte>(_pointer, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if ((uint)elementIndex > (uint)_length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));

            return new MemoryHandle(_pointer + elementIndex);
        }

        public override void Unpin()
        {
        }

        public void Release()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _disposed = true;
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            if (disposing)
                _accessor.Dispose();
        }
    }
}
