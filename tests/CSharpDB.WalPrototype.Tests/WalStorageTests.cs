using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CSharpDB.WalPrototype.Tests;

public sealed class WalStorageTests
{
    internal static byte[] Page(byte value) => Enumerable.Repeat(value, ControlCodec.PageSize).ToArray();
    internal static ExperimentalWalStore Create(string directory, long budget = ExperimentalWalStore.DefaultBudget,
        Action<string>? point = null) => ExperimentalWalStore.Create(directory, [Page(10), Page(20), Page(30)], budget, point);
    internal static void Seed(ExperimentalWalStore store)
    {
        store.Commit([new(0, Page(11)), new(1, Page(21))], 3, TestContext.Current.CancellationToken);
        store.Commit([new(0, Page(12))], 3, TestContext.Current.CancellationToken);
    }
    internal static void AssertPages(ExperimentalWalStore store, params byte[] values)
    {
        Assert.Equal((uint)values.Length, store.PageCount);
        for (uint i = 0; i < values.Length; i++) Assert.Equal(Page(values[i]), store.Read(i));
    }
    internal static string WalPath(string directory) => Path.Combine(directory, "database.wal");
    internal static string BasePath(string directory) => Path.Combine(directory, "database.pages");

    [Fact]
    public void FrameBytes_MatchProductionLayoutAndIndependentChecksumVector()
    {
        var frame = WalFrameCodec.Encode(new(2, Page(1)), 3, new(5, 7));
        Assert.Equal("020000000300000005000000070000001100000000040404", Convert.ToHexString(frame.AsSpan(0, 24)));
        Assert.Equal(new FrameHeader(2, 3), WalFrameCodec.Decode(frame, new(5, 7)));
        Assert.Equal(Page(1), frame[24..]);
    }

    [Fact]
    public void Reopen_ReplaysWholeBatchesDuplicatePagesAndGrowth()
    {
        string directory = Scratch.Create("storage-batches");
        using (var store = Create(directory))
        {
            Seed(store);
            store.Commit([new(1, Page(22)), new(1, Page(23)), new(3, Page(40))], 4, TestContext.Current.CancellationToken);
            AssertPages(store, 12, 23, 30, 40);
            Assert.Equal(ControlCodec.DataStart + 6L * ControlCodec.FrameSize, store.PublishedTail);
        }
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, 12, 23, 30, 40);
        reopened.Reset();
        Assert.Equal(ControlCodec.DataStart, reopened.PhysicalLength);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void RetiredCorruptIndexedOrSupersededFrame_DoesNotHideLaterCommit(int frame)
    {
        string directory = Scratch.Create("storage-retired-corruption");
        long physicalTail;
        using (var store = Create(directory))
        {
            store.Commit([new(0, Page(11))], 3, TestContext.Current.CancellationToken);
            store.Commit([new(0, Page(12))], 3, TestContext.Current.CancellationToken);
            var capture = store.CaptureCheckpoint();
            store.CopyCheckpoint(capture);
            store.Commit([new(1, Page(21))], 3, TestContext.Current.CancellationToken);
            physicalTail = store.PublishedTail;
            store.RetireCheckpoint(capture);
            Assert.Equal(physicalTail, store.PublishedTail);
            Assert.Equal(physicalTail, store.PhysicalLength);
            Assert.Equal(2L * ControlCodec.FrameSize, store.RetiredBytes);
        }
        byte[] wal = File.ReadAllBytes(WalPath(directory));
        wal[ControlCodec.DataStart + frame * ControlCodec.FrameSize + 24] ^= 1;
        File.WriteAllBytes(WalPath(directory), wal);
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, 12, 21, 30);
        Assert.Equal(physicalTail, reopened.PublishedTail);
        reopened.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken);
        Assert.Equal(physicalTail + ControlCodec.FrameSize, reopened.PublishedTail);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(23)]
    [InlineData(24)]
    [InlineData(25)]
    [InlineData(2072)]
    [InlineData(4119)]
    [InlineData(4120)]
    [InlineData(4121)]
    [InlineData(8239)]
    [InlineData(8240)]
    public void InterruptedTwoFrameBatch_IsAtomicOrRejectedWithoutMutation(int bytesWritten)
    {
        string directory = Scratch.Create("storage-tail");
        GenerationSalt salt;
        long oldTail;
        using (var store = Create(directory)) { Seed(store); salt = store.Salt; oldTail = store.PublishedTail; }
        byte[] tail = [.. WalFrameCodec.Encode(new(0, Page(50)), 0, salt), .. WalFrameCodec.Encode(new(1, Page(60)), 3, salt)];
        using (var file = new FileStream(WalPath(directory), FileMode.Append)) { file.Write(tail.AsSpan(0, bytesWritten)); file.Flush(true); }
        if (bytesWritten != 0 && bytesWritten != 4120 && bytesWritten != 8240)
        {
            AssertRejectedUnchanged(directory);
            return;
        }
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, bytesWritten == 8240 ? (byte)50 : (byte)12, bytesWritten == 8240 ? (byte)60 : (byte)21, 30);
        Assert.Equal(oldTail + (bytesWritten == 8240 ? 8240 : 0), reopened.PublishedTail);
        reopened.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken);
        AssertPages(reopened, bytesWritten == 8240 ? (byte)50 : (byte)12, bytesWritten == 8240 ? (byte)60 : (byte)21, 31);
    }

    [Theory]
    [InlineData(0)] // page ID / header checksum
    [InlineData(4)] // commit marker / header checksum
    [InlineData(8)] // salt
    [InlineData(16)] // header checksum
    [InlineData(20)] // data checksum
    [InlineData(24)] // page payload
    public void ActiveCorruptionBeforeLaterCommit_FailsWithoutChangingEitherFile(int byteOffset)
    {
        string directory = Scratch.Create("storage-active-corruption");
        using (var store = Create(directory)) Seed(store);
        byte[] wal = File.ReadAllBytes(WalPath(directory));
        wal[ControlCodec.DataStart + byteOffset] ^= 1;
        File.WriteAllBytes(WalPath(directory), wal);
        AssertRejectedUnchanged(directory);
    }

    [Fact]
    public void CleanUncommittedSuffix_IsRemovedBeforeNextCommit()
    {
        string directory = Scratch.Create("storage-clean-tail");
        GenerationSalt salt;
        long tail;
        using (var store = Create(directory)) { Seed(store); salt = store.Salt; tail = store.PublishedTail; }
        using (var file = new FileStream(WalPath(directory), FileMode.Append))
        {
            file.Write(WalFrameCodec.Encode(new(0, Page(50)), 0, salt));
            file.Write(WalFrameCodec.Encode(new(1, Page(60)), 0, salt));
            file.Flush(true);
        }
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, 12, 21, 30);
        Assert.Equal(tail, reopened.PhysicalLength);
        reopened.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken);
        AssertPages(reopened, 12, 21, 31);
    }

    [Fact]
    public void ReaderPins_BlockCopyAndReset_AndKeepOldWalAndBaseViews()
    {
        string directory = Scratch.Create("storage-reader");
        using var store = Create(directory);
        store.Commit([new(0, Page(11))], 3, TestContext.Current.CancellationToken);
        using var reader = store.BeginRead();
        store.Commit([new(0, Page(12)), new(1, Page(21))], 3, TestContext.Current.CancellationToken);
        Assert.Equal(Page(11), reader.Read(0));
        Assert.Equal(Page(20), reader.Read(1));
        var capture = store.CaptureCheckpoint();
        Assert.Throws<PrototypeBusyException>(() => store.CopyCheckpoint(capture));
        Assert.Throws<PrototypeBusyException>(store.Reset);
        Assert.Throws<PrototypeBusyException>(() => store.RetireCheckpoint(capture));
        reader.Dispose();
        Assert.Throws<InvalidOperationException>(() => store.RetireCheckpoint(capture));
        store.CopyCheckpoint(capture);
        using var newer = store.BeginRead();
        Assert.Equal(Page(12), newer.Read(0));
        Assert.Throws<PrototypeBusyException>(() => store.RetireCheckpoint(capture));
        newer.Dispose();
        store.RetireCheckpoint(capture);
        store.Reset();
        AssertPages(store, 12, 21, 30);
    }

    [Fact]
    public async Task ConcurrentWriterAndPinnedReader_KeepSnapshotStable()
    {
        string directory = Scratch.Create("storage-reader-concurrent");
        using var store = Create(directory);
        using var reader = store.BeginRead();
        await Task.WhenAll(Task.Run(() =>
        {
            for (byte i = 40; i < 80; i++) store.Commit([new(0, Page(i)), new(1, Page(i))], 3, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken), Task.Run(() =>
        {
            for (int i = 0; i < 120; i++) { Assert.Equal(Page(10), reader.Read(0)); Assert.Equal(Page(20), reader.Read(1)); }
        }, TestContext.Current.CancellationToken));
        AssertPages(store, 79, 79, 30);
    }

    [Fact]
    public void BudgetCountsRetiredPhysicalBytes_AndRefusesPinnedResetBeforeAppend()
    {
        string directory = Scratch.Create("storage-budget");
        long budget = ControlCodec.DataStart + 3L * ControlCodec.FrameSize;
        using var store = Create(directory, budget);
        Seed(store);
        var capture = store.CaptureCheckpoint();
        store.CopyCheckpoint(capture); store.RetireCheckpoint(capture);
        Assert.Equal(budget, store.PhysicalLength);
        using var reader = store.BeginRead();
        ulong epoch = store.Epoch;
        Assert.Throws<PrototypeBusyException>(() => store.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken));
        Assert.Equal(budget, store.PhysicalLength);
        Assert.Equal(0, store.ReservedBytes);
        Assert.Equal(epoch, store.Epoch);
        Assert.Equal(Page(30), reader.Read(2));
        reader.Dispose();
        store.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken);
        Assert.Equal(epoch + 1, store.Epoch);
        Assert.Equal(ControlCodec.DataStart + ControlCodec.FrameSize, store.PhysicalLength);
        AssertPages(store, 12, 21, 31);
    }

    [Fact]
    public void RepeatedBudgetCrossings_BoundExtentAndRecoverAfterEveryGeneration()
    {
        string directory = Scratch.Create("storage-budget-cycles");
        long budget = ControlCodec.DataStart + 4L * ControlCodec.FrameSize;
        using (Create(directory, budget)) { }
        ulong epoch = 1;
        long maximum = 0;
        for (byte i = 40; i < 64; i++)
        {
            using var store = ExperimentalWalStore.Open(directory, budget);
            store.Commit([new(0, Page(i)), new(1, Page(i))], 3, TestContext.Current.CancellationToken);
            maximum = Math.Max(maximum, store.PhysicalLength);
            Assert.InRange(store.PhysicalLength, ControlCodec.DataStart, budget);
            Assert.True(store.Epoch >= epoch); epoch = store.Epoch;
            AssertPages(store, i, i, 30);
        }
        using var final = ExperimentalWalStore.Open(directory, budget);
        AssertPages(final, 63, 63, 30);
        Assert.Equal((ulong)12, epoch);
        Assert.Equal(budget, maximum);
        Scratch.WriteJson(Path.Combine(directory, "budget-results.json"), new { commits = 24, resets = 11, maximumWalBytes = maximum, budget });
    }

    [Fact]
    public void OversizedBatchAndInvalidGrowth_LeaveStoreUsable()
    {
        string directory = Scratch.Create("storage-rejection");
        using var store = Create(directory, ControlCodec.DataStart + ControlCodec.FrameSize);
        Assert.Throws<InvalidOperationException>(() => store.Commit([new(0, Page(11)), new(1, Page(21))], 3, TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentException>(() => store.Commit([new(4, Page(44))], 5, TestContext.Current.CancellationToken));
        Assert.Throws<ArgumentException>(() => store.Commit([new(0, Page(11))], 2, TestContext.Current.CancellationToken));
        Assert.Equal(ControlCodec.DataStart, store.PhysicalLength);
        Assert.Equal(0, store.ReservedBytes);
        store.Commit([new(0, Page(11))], 3, TestContext.Current.CancellationToken);
        AssertPages(store, 11, 20, 30);
    }

    [Theory]
    [InlineData("frame-0-header-written", true)]
    [InlineData("frame-0-written", false)]
    [InlineData("commit-marker-half-written", true)]
    [InlineData("commit-marker-flushed", false)]
    public void FailedAppend_ReleasesReservationAndRequiresReopen(string point, bool rejected)
    {
        string directory = Scratch.Create("storage-io-failure");
        bool armed = false;
        using (var store = Create(directory, point: name => { if (armed && name == point) throw new IOException("Injected append failure."); }))
        {
            Seed(store); armed = true;
            Assert.Throws<IOException>(() => store.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken));
            Assert.Equal(0, store.ReservedBytes);
            Assert.Throws<InvalidOperationException>(() => store.Commit([new(2, Page(32))], 3, TestContext.Current.CancellationToken));
        }
        if (rejected) AssertRejectedUnchanged(directory);
        else
        {
            using var reopened = ExperimentalWalStore.Open(directory);
            AssertPages(reopened, 12, 21, point == "commit-marker-flushed" ? (byte)31 : (byte)30);
        }
    }

    internal static void AssertRejectedUnchanged(string directory)
    {
        byte[] wal = File.ReadAllBytes(WalPath(directory)), database = File.ReadAllBytes(BasePath(directory));
        Assert.Throws<InvalidDataException>(() => ExperimentalWalStore.Open(directory));
        Assert.Equal(wal, File.ReadAllBytes(WalPath(directory)));
        Assert.Equal(database, File.ReadAllBytes(BasePath(directory)));
    }

    [Fact]
    public void CancellationAfterFirstFrame_RollsBackBeforeReusingAppendPosition()
    {
        string directory = Scratch.Create("storage-cancel");
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        bool armed = false;
        using (var store = Create(directory, point: name => { if (armed && name == "frame-0-written") cancellation.Cancel(); }))
        {
            Seed(store);
            long tail = store.PublishedTail;
            armed = true;
            Assert.Throws<OperationCanceledException>(() => store.Commit([new(0, Page(50)), new(1, Page(60))], 3, cancellation.Token));
            armed = false;
            Assert.Equal(tail, store.PhysicalLength);
            Assert.Equal(tail, store.PublishedTail);
            Assert.Equal(0, store.ReservedBytes);
            AssertPages(store, 12, 21, 30);
            store.Commit([new(2, Page(31))], 3, TestContext.Current.CancellationToken);
        }
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, 12, 21, 31);
    }

    [Fact]
    public void CommitMarker_UsesChecksumOfEncodedPageAfterCallerBufferChanges()
    {
        string directory = Scratch.Create("storage-buffer-ownership");
        byte[] callerPage = Page(11);
        using (var store = Create(directory, point: name =>
            { if (name == "frame-0-written") callerPage.AsSpan().Fill(99); }))
        {
            store.Commit([new(0, callerPage)], 3, TestContext.Current.CancellationToken);
            AssertPages(store, 11, 20, 30);
        }
        using var reopened = ExperimentalWalStore.Open(directory);
        AssertPages(reopened, 11, 20, 30);
    }
}
