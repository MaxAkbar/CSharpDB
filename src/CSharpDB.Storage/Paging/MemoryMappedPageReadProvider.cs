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
    private readonly bool _copyOnRead;
    private readonly object _mappingGate = new();
    private MemoryMappedFile? _mappedFile;
    private MemoryMappedViewMemoryManager? _mappedMemory;
    private long _mappedLength;

    public MemoryMappedPageReadProvider(FileStorageDevice device, bool copyOnRead = false)
    {
        ArgumentNullException.ThrowIfNull(device);

        _device = device;
        _fallback = new StorageDevicePageReadProvider(device);
        _copyOnRead = copyOnRead;
        RefreshMapping();
    }

    public ValueTask<PageReadBuffer> ReadPageAsync(uint pageId, CancellationToken ct = default)
    {
        if (_copyOnRead)
        {
            // Shared-pager callers can retain a page while checkpointing remaps
            // this provider. A read-only copy survives that remap and makes later
            // mutable cache access copy again instead of changing a reader's data.
            return TryCopyMappedPage(pageId, out var copy)
                ? ValueTask.FromResult(PageReadBuffer.FromReadOnlyMemory(copy))
                : _fallback.ReadPageAsync(pageId, ct);
        }

        long offset = (long)pageId * PageConstants.PageSize;
        if (_mappedMemory is not null && offset >= 0 && offset + PageConstants.PageSize <= _mappedLength)
        {
            PageReadBuffer buffer = PageReadBuffer.FromReadOnlyMemory(
                _mappedMemory.Memory.Slice((int)offset, PageConstants.PageSize));
            _device.RecordMemoryMappedPageExposure(PageConstants.PageSize);
            return ValueTask.FromResult(buffer);
        }

        return _fallback.ReadPageAsync(pageId, ct);
    }

    public ValueTask<byte[]> ReadOwnedPageAsync(uint pageId, CancellationToken ct = default)
        => TryCopyMappedPage(pageId, out var copy)
            ? ValueTask.FromResult(copy)
            : _fallback.ReadOwnedPageAsync(pageId, ct);

    private bool TryCopyMappedPage(uint pageId, out byte[] copy)
    {
        lock (_mappingGate)
        {
            long offset = (long)pageId * PageConstants.PageSize;
            if (_mappedMemory is not null && offset >= 0 && offset + PageConstants.PageSize <= _mappedLength)
            {
                copy = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
                _mappedMemory.Memory.Span.Slice((int)offset, PageConstants.PageSize).CopyTo(copy);
                _device.RecordMemoryMappedPageExposure(PageConstants.PageSize);
                return true;
            }

            copy = null!;
            return false;
        }
    }

    internal void RefreshMapping()
    {
        lock (_mappingGate)
            RefreshMappingCore();
    }

    private void RefreshMappingCore()
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
        lock (_mappingGate)
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
