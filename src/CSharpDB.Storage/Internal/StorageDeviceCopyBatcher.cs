namespace CSharpDB.Storage.Internal;

internal static class StorageDeviceCopyBatcher
{
    private const int DefaultChunkBytes = 256 * 1024;

    public static async ValueTask CopyDeviceRangeToStreamAsync(
        IStorageDevice source,
        long sourceOffset,
        long byteCount,
        Stream destination,
        CancellationToken ct = default,
        int chunkBytes = DefaultChunkBytes)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentOutOfRangeException.ThrowIfNegative(sourceOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(byteCount);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(chunkBytes, 0);

        if (byteCount == 0)
            return;

        byte[] buffer = new byte[chunkBytes];
        long remaining = byteCount;
        long offset = sourceOffset;
        while (remaining > 0)
        {
            int chunkLength = (int)Math.Min(buffer.Length, remaining);
            int bytesRead = await source.ReadAsync(offset, buffer.AsMemory(0, chunkLength), ct);
            if (bytesRead != chunkLength)
            {
                throw new InvalidOperationException(
                    $"Short device read while copying persisted state (expected {chunkLength} bytes, read {bytesRead}).");
            }

            await destination.WriteAsync(buffer.AsMemory(0, chunkLength), ct);
            offset += chunkLength;
            remaining -= chunkLength;
        }
    }
}
