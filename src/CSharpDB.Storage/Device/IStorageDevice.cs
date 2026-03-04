namespace CSharpDB.Storage.Device;

public interface IStorageDevice : IAsyncDisposable, IDisposable
{
    long Length { get; }
    ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default);
    ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
    ValueTask SetLengthAsync(long length, CancellationToken ct = default);
}
