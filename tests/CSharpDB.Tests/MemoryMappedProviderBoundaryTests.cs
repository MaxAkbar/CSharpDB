using System.ComponentModel;
using System.Runtime.InteropServices;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Tests;

public sealed class MemoryMappedProviderBoundaryTests
{
    [Fact]
    public async Task CopiedReadOnlyPage_SurvivesSourceMutationRemapAndProviderDisposal()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        byte[] original = PatternPage(19);
        byte[] changed = PatternPage(71);
        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, original, ct);
            using var provider = new MemoryMappedPageReadProvider(device, copyOnRead: true);
            PageReadBuffer retained = await provider.ReadPageAsync(0, ct);
            Assert.False(retained.TryGetOwnedBuffer(out _));
            byte[] mutable = retained.MaterializeOwnedBuffer();
            mutable[0] ^= 0xff;
            Assert.Equal(original, retained.Memory.ToArray());

            await device.WriteAsync(0, changed, ct);
            await device.WriteAsync(PageConstants.PageSize, changed, ct);
            provider.RefreshMapping();
            Assert.Equal(original, retained.Memory.ToArray());
            Assert.Equal(changed, (await provider.ReadPageAsync(0, ct)).Memory.ToArray());
            byte[] owned = await provider.ReadOwnedPageAsync(0, ct);
            owned[0] ^= 0xff;
            Assert.Equal(changed, (await provider.ReadPageAsync(0, ct)).Memory.ToArray());

            provider.Dispose();
            Assert.Equal(original, retained.Memory.ToArray());
            Assert.Equal((byte)(original[0] ^ 0xff), mutable[0]);
            Assert.Equal((byte)(changed[0] ^ 0xff), owned[0]);
        }
        finally { File.Delete(path); }
    }

    [Fact]
    public async Task GrowthBeyondMappedLength_UsesOwnedFallbackUntilRefresh()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        byte[] first = PatternPage(17);
        byte[] appended = PatternPage(83);

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, first, ct);
            using var provider = new MemoryMappedPageReadProvider(device);
            PageReadBuffer originalView = await provider.ReadPageAsync(0, ct);
            AssertMapped(first, originalView);
            byte[] originalCopy = await provider.ReadOwnedPageAsync(0, ct);

            // Append through the device rather than SetLength: Windows does not
            // support truncating a file while its mapped views are still open.
            await device.WriteAsync(PageConstants.PageSize, appended, ct);
            Assert.Equal(2L * PageConstants.PageSize, device.Length);

            PageReadBuffer fallback = await provider.ReadPageAsync(1, ct);
            AssertOwned(appended, fallback);
            AssertMapped(first, await provider.ReadPageAsync(0, ct));

            provider.RefreshMapping();

            AssertMapped(first, await provider.ReadPageAsync(0, ct));
            AssertMapped(appended, await provider.ReadPageAsync(1, ct));
            Assert.Throws<ObjectDisposedException>(() => ReadFirstByte(originalView));
            Assert.Equal(first, originalCopy);
            AssertOwned(appended, fallback);

            // An unchanged refresh must retain its existing view lifetime.
            PageReadBuffer refreshedView = await provider.ReadPageAsync(1, ct);
            provider.RefreshMapping();
            AssertMapped(appended, refreshedView);

            provider.Dispose();
            provider.Dispose();
            Assert.Throws<ObjectDisposedException>(() => ReadFirstByte(refreshedView));
            AssertOwned(appended, fallback);
            Assert.Equal(first, originalCopy);

            // Mapping disposal must leave the FileStorageDevice's handle open.
            byte[] afterMappingDisposal = new byte[PageConstants.PageSize];
            Assert.Equal(PageConstants.PageSize,
                await device.ReadAsync(PageConstants.PageSize, afterMappingDisposal, ct));
            Assert.Equal(appended, afterMappingDisposal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task PartialLastPage_FallsBackToIndependentZeroPaddedOwnedBuffers()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        byte[] first = PatternPage(29);
        byte[] partial = PatternPage(107)[..137];
        byte[] expectedPartialPage = new byte[PageConstants.PageSize];
        partial.CopyTo(expectedPartialPage, 0);

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, first, ct);
            await device.WriteAsync(PageConstants.PageSize, partial, ct);
            using var provider = new MemoryMappedPageReadProvider(device);

            AssertMapped(first, await provider.ReadPageAsync(0, ct));
            PageReadBuffer partialPage = await provider.ReadPageAsync(1, ct);
            AssertOwned(expectedPartialPage, partialPage);
            byte[] independentCopy = await provider.ReadOwnedPageAsync(1, ct);
            Assert.Equal(expectedPartialPage, independentCopy);
            Assert.True(partialPage.TryGetOwnedBuffer(out byte[]? owned));
            Assert.NotSame(owned, independentCopy);

            owned![0] ^= 0xff;
            Assert.Equal(expectedPartialPage, independentCopy);
            AssertOwned(expectedPartialPage, await provider.ReadPageAsync(1, ct));
            AssertOwned(new byte[PageConstants.PageSize], await provider.ReadPageAsync(2, ct));

            provider.Dispose();
            Assert.Equal(expectedPartialPage, independentCopy);
            Assert.Equal((byte)(expectedPartialPage[0] ^ 0xff), partialPage.Memory.Span[0]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ShrinkAfterReleasingMapping_RemapsRetainedPageAndZeroFillsRemovedPage()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        byte[] retained = PatternPage(41);
        byte[] removed = PatternPage(151);

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            await device.WriteAsync(0, retained, ct);
            await device.WriteAsync(PageConstants.PageSize, removed, ct);
            PageReadBuffer previousView;
            byte[] ownedCopy;
            using (var original = new MemoryMappedPageReadProvider(device))
            {
                previousView = await original.ReadPageAsync(1, ct);
                AssertMapped(removed, previousView);
                ownedCopy = await original.ReadOwnedPageAsync(1, ct);
            }

            Assert.Throws<ObjectDisposedException>(() => ReadFirstByte(previousView));
            // Releasing every mapped view before truncation is the supported
            // cross-platform lifecycle. Do not access a live mapping after shrink.
            await device.SetLengthAsync(PageConstants.PageSize, ct);
            using (var replacement = new MemoryMappedPageReadProvider(device))
            {
                Assert.Equal(PageConstants.PageSize, device.Length);
                AssertMapped(retained, await replacement.ReadPageAsync(0, ct));
                AssertOwned(new byte[PageConstants.PageSize], await replacement.ReadPageAsync(1, ct));
            }
            Assert.Equal(removed, ownedCopy);

            // Both providers used leaveOpen mappings; the owner still supports I/O.
            byte[] changed = PatternPage(67);
            await device.WriteAsync(0, changed, ct);
            byte[] actual = new byte[PageConstants.PageSize];
            Assert.Equal(PageConstants.PageSize, await device.ReadAsync(0, actual, ct));
            Assert.Equal(changed, actual);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task GrowthPastIntMaxValue_DisablesMappingAndPreservesLongOffsetFallback()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = NewTempPath();
        byte[] first = PatternPage(53);
        byte[] highPage = PatternPage(193);
        const long highOffset = (long)int.MaxValue + 1;
        const uint highPageId = (uint)(highOffset / PageConstants.PageSize);

        try
        {
            await using var device = new FileStorageDevice(path, createNew: true);
            MakeTemporaryFileSparseOnWindows(path);
            await device.WriteAsync(0, first, ct);
            using var provider = new MemoryMappedPageReadProvider(device);
            PageReadBuffer originalView = await provider.ReadPageAsync(0, ct);
            AssertMapped(first, originalView);

            // Only two pages are written. The intervening range is a hole; no
            // 2 GiB buffer, write loop, or mapped view is created by this test.
            await device.WriteAsync(highOffset, highPage, ct);
            Assert.Equal(highOffset + PageConstants.PageSize, device.Length);
            AssertMapped(first, await provider.ReadPageAsync(0, ct));
            PageReadBuffer beforeRefresh = await provider.ReadPageAsync(highPageId, ct);
            AssertOwned(highPage, beforeRefresh);

            provider.RefreshMapping();

            Assert.Throws<ObjectDisposedException>(() => ReadFirstByte(originalView));
            AssertOwned(first, await provider.ReadPageAsync(0, ct));
            AssertOwned(highPage, await provider.ReadPageAsync(highPageId, ct));
            AssertOwned(new byte[PageConstants.PageSize], await provider.ReadPageAsync(highPageId - 1, ct));
            byte[] independentCopy = await provider.ReadOwnedPageAsync(highPageId, ct);
            Assert.Equal(highPage, independentCopy);
            Assert.True(beforeRefresh.TryGetOwnedBuffer(out byte[]? owned));
            Assert.NotSame(owned, independentCopy);
            owned![0] ^= 0xff;
            Assert.Equal(highPage, independentCopy);
            AssertOwned(highPage, await provider.ReadPageAsync(highPageId, ct));

            provider.Dispose();
            Assert.Equal(highPage, independentCopy);
            byte[] actual = new byte[PageConstants.PageSize];
            Assert.Equal(PageConstants.PageSize, await device.ReadAsync(highOffset, actual, ct));
            Assert.Equal(highPage, actual);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static void AssertMapped(byte[] expected, PageReadBuffer buffer)
    {
        Assert.False(buffer.TryGetOwnedBuffer(out _));
        Assert.Equal(expected, buffer.Memory.ToArray());
    }

    private static void AssertOwned(byte[] expected, PageReadBuffer buffer)
    {
        Assert.True(buffer.TryGetOwnedBuffer(out byte[]? owned));
        Assert.Equal(expected, owned);
        Assert.Equal(expected, buffer.Memory.ToArray());
        Assert.Same(owned, buffer.MaterializeOwnedBuffer());
    }

    private static byte ReadFirstByte(PageReadBuffer buffer) => buffer.Memory.Span[0];

    private static byte[] PatternPage(int seed)
    {
        byte[] page = new byte[PageConstants.PageSize];
        for (int i = 0; i < page.Length; i++)
            page[i] = (byte)((i * 131 + seed) & 0xff);
        return page;
    }

    private static string NewTempPath() =>
        Path.Combine(Path.GetTempPath(), $"csharpdb_mmap_boundary_{Guid.NewGuid():N}.db");

    private static void MakeTemporaryFileSparseOnWindows(string path)
    {
        if (!OperatingSystem.IsWindows())
            return;

        // This marks only this throwaway file sparse; it changes no OS settings.
        // Use a synchronous handle for this synchronous metadata operation.
        using SafeFileHandle handle = File.OpenHandle(path, FileMode.Open,
            FileAccess.ReadWrite, FileShare.ReadWrite, FileOptions.None);
        if (DeviceIoControl(handle, 0x000900C4 /* FSCTL_SET_SPARSE */,
            IntPtr.Zero, 0, IntPtr.Zero, 0, out _, IntPtr.Zero))
            return;

        int error = Marshal.GetLastWin32Error();
        if (error is 1 or 50) // ERROR_INVALID_FUNCTION / ERROR_NOT_SUPPORTED
            Assert.Skip("The temporary volume does not support sparse files; refusing to allocate a 2 GiB test file.");
        throw new Win32Exception(error, "Could not mark the temporary boundary-test file sparse.");
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool DeviceIoControl(SafeFileHandle handle, uint controlCode,
        IntPtr input, uint inputLength, IntPtr output, uint outputLength,
        out uint bytesReturned, IntPtr overlapped);
}
