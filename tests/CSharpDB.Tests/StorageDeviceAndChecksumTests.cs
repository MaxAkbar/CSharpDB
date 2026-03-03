using System.Buffers.Binary;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Integrity;

namespace CSharpDB.Tests;

public sealed class StorageDeviceAndChecksumTests
{
    [Fact]
    public async Task FileStorageDevice_WriteReadAndLength_AreConsistent()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);

            Assert.Equal(0, device.Length);

            var payload = new byte[] { 1, 2, 3, 4, 5 };
            await device.WriteAsync(0, payload, ct);
            await device.FlushAsync(ct);

            Assert.Equal(payload.Length, device.Length);

            var readBuffer = new byte[payload.Length];
            int bytesRead = await device.ReadAsync(0, readBuffer, ct);

            Assert.Equal(payload.Length, bytesRead);
            Assert.Equal(payload, readBuffer);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task FileStorageDevice_ReadAsync_PastEnd_ZeroFillsUnreadBytes()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, new byte[] { 7, 8, 9 }, ct);
            await device.FlushAsync(ct);

            var readBuffer = new byte[6];
            int bytesRead = await device.ReadAsync(0, readBuffer, ct);

            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 7, 8, 9, 0, 0, 0 }, readBuffer);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task FileStorageDevice_SetLengthAsync_TruncatesAndExtendsFile()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, new byte[] { 10, 11, 12, 13, 14 }, ct);
            await device.FlushAsync(ct);

            await device.SetLengthAsync(3, ct);
            Assert.Equal(3, device.Length);

            var truncatedRead = new byte[5];
            int truncatedBytesRead = await device.ReadAsync(0, truncatedRead, ct);
            Assert.Equal(3, truncatedBytesRead);
            Assert.Equal(new byte[] { 10, 11, 12, 0, 0 }, truncatedRead);

            await device.SetLengthAsync(8, ct);
            Assert.Equal(8, device.Length);

            var extendedRead = new byte[5];
            int extendedBytesRead = await device.ReadAsync(3, extendedRead, ct);
            Assert.Equal(5, extendedBytesRead);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0 }, extendedRead);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void AdditiveChecksumProvider_EmptyBuffer_IsZero()
    {
        var provider = new AdditiveChecksumProvider();
        Assert.Equal(0u, provider.Compute(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void AdditiveChecksumProvider_AlignedBuffer_MatchesExpectedSum()
    {
        var provider = new AdditiveChecksumProvider();
        var data = new byte[] { 1, 0, 0, 0, 2, 0, 0, 0 };

        uint checksum = provider.Compute(data);

        Assert.Equal(ComputeExpectedChecksum(data), checksum);
    }

    [Fact]
    public void AdditiveChecksumProvider_UnalignedBuffer_MatchesExpectedSum()
    {
        var provider = new AdditiveChecksumProvider();
        var data = new byte[] { 5, 4, 3, 2, 9, 8, 7, 6, 10, 11, 12 };

        uint checksum = provider.Compute(data);

        Assert.Equal(ComputeExpectedChecksum(data), checksum);
    }

    private static uint ComputeExpectedChecksum(ReadOnlySpan<byte> data)
    {
        uint sum = 0;
        int offset = 0;

        while (offset + sizeof(uint) <= data.Length)
        {
            sum += BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(offset, sizeof(uint)));
            offset += sizeof(uint);
        }

        for (; offset < data.Length; offset++)
            sum += data[offset];

        return sum;
    }

    private static string NewTempPath() =>
        Path.Combine(Path.GetTempPath(), $"csharpdb_storage_device_test_{Guid.NewGuid():N}.bin");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
