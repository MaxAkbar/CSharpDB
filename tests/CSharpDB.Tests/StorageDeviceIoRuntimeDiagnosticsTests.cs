using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class StorageDeviceIoRuntimeDiagnosticsTests
{
    [Fact]
    public async Task FileDevice_OperationsBeforeEnable_AreNotRecorded()
    {
        string path = NewTempPath();

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(
                0,
                new byte[] { 1, 2, 3 },
                TestContext.Current.CancellationToken);
            await device.FlushAsync(TestContext.Current.CancellationToken);
            await device.SetLengthAsync(8, TestContext.Current.CancellationToken);
            _ = await device.ReadAsync(
                0,
                new byte[8],
                TestContext.Current.CancellationToken);

            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);

            Assert.Equal(
                default(StorageDeviceIoRuntimeRawSnapshot),
                counters.Capture());
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task FileDevice_SuccessfulOperations_RecordCallsAndActualByteCounts()
    {
        string path = NewTempPath();
        File.WriteAllBytes(path, new byte[] { 1, 2, 3 });

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);

            var readBuffer = new byte[5];
            int bytesRead = await device.ReadAsync(
                0,
                readBuffer,
                TestContext.Current.CancellationToken);
            await device.WriteAsync(
                8,
                new byte[] { 4, 5, 6, 7 },
                TestContext.Current.CancellationToken);
            await device.FlushAsync(TestContext.Current.CancellationToken);
            await device.SetLengthAsync(20, TestContext.Current.CancellationToken);

            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3, 0, 0 }, readBuffer);
            Assert.Equal(
                new StorageDeviceIoRuntimeRawSnapshot(
                    ReadCount: 1,
                    BytesRead: 3,
                    WriteCount: 1,
                    BytesWritten: 4,
                    FlushCount: 1,
                    ResizeCount: 1,
                    SequentialReadCount: 0,
                    SequentialBytesRead: 0,
                    MemoryMappedPageExposureCount: 0,
                    MemoryMappedBytesExposed: 0),
                counters.Capture());
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task FileDevice_CanceledReadAndWrite_DoNotRecordOperations()
    {
        string path = NewTempPath();
        File.WriteAllBytes(path, new byte[PageConstants.PageSize]);

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
            var canceled = new CancellationToken(canceled: true);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await device.ReadAsync(0, new byte[32], canceled));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await device.WriteAsync(0, new byte[32], canceled));

            Assert.Equal(
                default(StorageDeviceIoRuntimeRawSnapshot),
                counters.Capture());
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task FileDevice_FailedOperations_DoNotRecordOperations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        var device = new FileStorageDevice(path, createNew: true);
        StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
        await device.DisposeAsync();

        try
        {
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await device.ReadAsync(0, new byte[8], ct));
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await device.WriteAsync(0, new byte[8], ct));
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await device.FlushAsync(ct));
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await device.SetLengthAsync(8, ct));

            Assert.Equal(
                default(StorageDeviceIoRuntimeRawSnapshot),
                counters.Capture());
        }
        finally
        {
            device.Dispose();
            DeleteIfExists(path);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SequentialProvider_RecordsTotalAndSequentialSubsetsExactlyOnce(
        bool forceFallbackToPrimaryHandle)
    {
        string path = NewTempPath();
        File.WriteAllBytes(path, new byte[PageConstants.PageSize]);

        try
        {
            bool sequentialFactoryInvoked = false;
            await using FileStorageDevice device = forceFallbackToPrimaryHandle
                ? new FileStorageDevice(
                    path,
                    createNew: false,
                    fileShare: FileShare.ReadWrite,
                    sequentialReadHandleFactory: () =>
                    {
                        sequentialFactoryInvoked = true;
                        return null;
                    })
                : new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
            var provider = new StorageDevicePageReadProvider(
                device,
                useSequentialAccessHint: true);

            if (forceFallbackToPrimaryHandle)
            {
                Assert.Same(device.Handle, device.SequentialReadHandle);
                Assert.True(sequentialFactoryInvoked);
            }

            byte[] page = await provider.ReadOwnedPageAsync(
                pageId: 0,
                TestContext.Current.CancellationToken);

            Assert.Equal(PageConstants.PageSize, page.Length);
            StorageDeviceIoRuntimeRawSnapshot snapshot = counters.Capture();
            Assert.Equal(1, snapshot.ReadCount);
            Assert.Equal(PageConstants.PageSize, snapshot.BytesRead);
            Assert.Equal(1, snapshot.SequentialReadCount);
            Assert.Equal(PageConstants.PageSize, snapshot.SequentialBytesRead);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task MemoryMappedProvider_DirectViewAndOwnedCopy_RecordMappedAccessOnly()
    {
        string path = NewTempPath();
        byte[] contents = new byte[PageConstants.PageSize];
        contents[123] = 45;
        File.WriteAllBytes(path, contents);

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
            using var provider = new MemoryMappedPageReadProvider(device);

            PageReadBuffer view = await provider.ReadPageAsync(
                pageId: 0,
                TestContext.Current.CancellationToken);
            byte[] copy = await provider.ReadOwnedPageAsync(
                pageId: 0,
                TestContext.Current.CancellationToken);

            Assert.Equal((byte)45, view.Memory.Span[123]);
            Assert.Equal((byte)45, copy[123]);
            Assert.False(view.TryGetOwnedBuffer(out _));

            StorageDeviceIoRuntimeRawSnapshot snapshot = counters.Capture();
            Assert.Equal(0, snapshot.ReadCount);
            Assert.Equal(0, snapshot.BytesRead);
            Assert.Equal(2, snapshot.MemoryMappedPageExposureCount);
            Assert.Equal(2L * PageConstants.PageSize, snapshot.MemoryMappedBytesExposed);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task MemoryMappedProvider_UnmappedPage_FallsBackToOrdinaryPhysicalRead()
    {
        string path = NewTempPath();
        File.WriteAllBytes(path, new byte[PageConstants.PageSize]);

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
            using var provider = new MemoryMappedPageReadProvider(device);

            PageReadBuffer page = await provider.ReadPageAsync(
                pageId: 1,
                TestContext.Current.CancellationToken);

            Assert.Equal(PageConstants.PageSize, page.Memory.Length);
            StorageDeviceIoRuntimeRawSnapshot snapshot = counters.Capture();
            Assert.Equal(1, snapshot.ReadCount);
            Assert.Equal(0, snapshot.BytesRead);
            Assert.Equal(0, snapshot.MemoryMappedPageExposureCount);
            Assert.Equal(0, snapshot.MemoryMappedBytesExposed);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public async Task WalTraffic_DoesNotAffectPrimaryDeviceCounters()
    {
        string path = NewTempPath();
        string walPath = path + ".wal";
        File.WriteAllBytes(path, new byte[PageConstants.PageSize]);

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters counters = EnableDiagnostics(device);
            await using var wal = new WriteAheadLog(path, new WalIndex());

            await wal.OpenAsync(
                currentDbPageCount: 1,
                TestContext.Current.CancellationToken);
            wal.BeginTransaction();
            await wal.AppendFrameAsync(
                pageId: 0,
                new byte[PageConstants.PageSize],
                TestContext.Current.CancellationToken);
            await wal.CommitAsync(
                newDbPageCount: 1,
                TestContext.Current.CancellationToken);

            Assert.Equal(
                default(StorageDeviceIoRuntimeRawSnapshot),
                counters.Capture());
        }
        finally
        {
            DeleteIfExists(path);
            DeleteIfExists(walPath);
        }
    }

    [Fact]
    public async Task RuntimeDiagnostics_EnableIsIdempotent_AndSealFreezesTerminalSample()
    {
        string path = NewTempPath();
        File.WriteAllBytes(path, new byte[] { 1, 2, 3, 4 });

        try
        {
            await using var device = new FileStorageDevice(path);
            StorageDeviceIoRuntimeCounters first = EnableDiagnostics(device);
            StorageDeviceIoRuntimeCounters second = EnableDiagnostics(device);
            Assert.Same(first, second);

            int bytesRead = await device.ReadAsync(
                0,
                new byte[8],
                TestContext.Current.CancellationToken);
            Assert.Equal(4, bytesRead);
            Assert.True(first.TrySeal(out StorageDeviceIoRuntimeRawSnapshot terminal));

            await device.WriteAsync(
                0,
                new byte[] { 9, 8 },
                TestContext.Current.CancellationToken);
            await device.FlushAsync(TestContext.Current.CancellationToken);
            await device.SetLengthAsync(16, TestContext.Current.CancellationToken);

            Assert.Equal(terminal, first.Capture());
            Assert.False(first.TrySeal(out StorageDeviceIoRuntimeRawSnapshot duplicate));
            Assert.Equal(default(StorageDeviceIoRuntimeRawSnapshot), duplicate);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    private static StorageDeviceIoRuntimeCounters EnableDiagnostics(
        FileStorageDevice device)
        => ((IStorageDeviceIoRuntimeDiagnosticsProvider)device)
            .EnableRuntimeDiagnostics();

    private static string NewTempPath()
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_physical_io_{Guid.NewGuid():N}.db");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
