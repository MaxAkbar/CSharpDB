using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CSharpDB.WalPrototype.Tests;

public sealed class CommittedTailTests
{
    internal static ExperimentalWalStore Create(string directory, Action<string>? point = null, long budget = ExperimentalWalStore.DefaultBudget) =>
        ExperimentalWalStore.Create(directory, [WalStorageTests.Page(10), WalStorageTests.Page(20), WalStorageTests.Page(30)],
            budget, point, committedTail: true);

    [Fact]
    public void OptInField_IsChecksummedAndCannotBeMixedWithFloorOnlySlots()
    {
        var record = ControlCodecTests.Record() with { CommittedTail = ControlCodec.DataStart + 2L * ControlCodec.FrameSize };
        byte[] bytes = ControlCodec.Encode(record);
        Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8)));
        Assert.Equal((ulong)record.CommittedTail!.Value, BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(96)));
        Assert.Equal(record, ControlCodec.Decode(bytes, record.CommittedTail.Value));
        for (int i = 96; i < 104; i++)
        {
            bytes[i] ^= 1;
            Assert.Null(ControlCodec.Decode(bytes, record.CommittedTail.Value));
            bytes[i] ^= 1;
        }
        Assert.Throws<InvalidDataException>(() => ControlCodec.Select(
            ControlCodecTests.Header(record, ControlCodecTests.Record(2)), record.CommittedTail.Value));
        Assert.Throws<InvalidDataException>(() => ControlCodec.Select(ControlCodecTests.Header(record,
            record with { Sequence = 2, CommittedTail = ControlCodec.DataStart }), record.CommittedTail.Value));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), 0);
        SHA256.HashData(bytes.AsSpan(0, ControlCodec.DigestOffset), bytes.AsSpan(ControlCodec.DigestOffset));
        Assert.Throws<InvalidDataException>(() => ControlCodec.Decode(bytes, record.CommittedTail.Value));
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(8191UL)]
    [InlineData(8193UL)]
    [InlineData(12312UL)]
    [InlineData(9223372036854775808UL)]
    [InlineData(18446744073709551615UL)]
    public void ImpossibleChecksummedTail_IsFatalEvenWithUsablePeer(ulong tail)
    {
        var record = ControlCodecTests.Record() with { CommittedTail = ControlCodec.DataStart };
        byte[] header = ControlCodecTests.Header(record, record with { Sequence = 2 });
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(96), tail);
        SHA256.HashData(header.AsSpan(0, ControlCodec.DigestOffset), header.AsSpan(ControlCodec.DigestOffset, 32));
        Assert.Throws<InvalidDataException>(() => ControlCodec.Select(header, ControlCodec.DataStart));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void AcknowledgedCommit_SurvivesOneLostControlAndLaterTornAppend(int lostSlot)
    {
        string directory = Scratch.Create("tail-slot-loss");
        GenerationSalt salt;
        using (var store = Create(directory)) { WalStorageTests.Seed(store); salt = store.Salt; }
        byte[] wal = File.ReadAllBytes(WalStorageTests.WalPath(directory));
        long acknowledged = wal.Length;
        byte[] torn = WalFrameCodec.Encode(new(2, WalStorageTests.Page(31)), 0, salt)[..2072];
        wal = [.. wal, .. torn];
        wal.AsSpan(lostSlot * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
        File.WriteAllBytes(WalStorageTests.WalPath(directory), wal);
        using var recovered = ExperimentalWalStore.Open(directory);
        WalStorageTests.AssertPages(recovered, 12, 21, 30);
        Assert.Equal(acknowledged, recovered.PhysicalLength);
    }

    [Theory]
    [InlineData("payload")]
    [InlineData("truncated")]
    [InlineData("marker-reverted")]
    [InlineData("boundary-inside-batch")]
    public void DamageInsideDeclaredCommit_IsRejectedWithoutMutation(string damage)
    {
        string directory = Scratch.Create("tail-committed-damage");
        using (var store = Create(directory)) store.Commit([new(0, WalStorageTests.Page(11)), new(1, WalStorageTests.Page(21))], 3, TestContext.Current.CancellationToken);
        byte[] wal = File.ReadAllBytes(WalStorageTests.WalPath(directory));
        var record = ControlCodec.Select(wal.AsSpan(0, ControlCodec.DataStart), wal.Length).Current;
        if (damage == "payload") wal[ControlCodec.DataStart + 24] ^= 1;
        else if (damage == "truncated") Array.Resize(ref wal, wal.Length - 1);
        else if (damage == "marker-reverted")
        {
            byte[] uncommitted = WalFrameCodec.Encode(new(1, WalStorageTests.Page(21)), 0, record.Salt);
            uncommitted.AsSpan(0, 24).CopyTo(wal.AsSpan(wal.Length - ControlCodec.FrameSize));
        }
        else
        {
            var inside = record with { CommittedTail = ControlCodec.DataStart + ControlCodec.FrameSize };
            ControlCodecTests.Header(inside, inside with { Sequence = inside.Sequence + 1 }).CopyTo(wal, 0);
        }
        File.WriteAllBytes(WalStorageTests.WalPath(directory), wal);
        WalStorageTests.AssertRejectedUnchanged(directory);
    }

    [Fact]
    public void CancellationCannotTruncateAfterCommitBoundaryPublicationStarts()
    {
        string directory = Scratch.Create("tail-cancel-publication");
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        bool armed = false;
        using (var store = Create(directory, name =>
        {
            if (armed && name == "control-active-0-written")
            { cancellation.Cancel(); throw new OperationCanceledException(cancellation.Token); }
        }))
        {
            WalStorageTests.Seed(store); armed = true;
            Assert.Throws<OperationCanceledException>(() => store.Commit([new(2, WalStorageTests.Page(31))], 3, cancellation.Token));
            Assert.Equal(0, store.ReservedBytes);
            Assert.Throws<InvalidOperationException>(() => store.Read(0));
        }
        // The call did not return success, but its complete published batch may be recovered.
        using var recovered = ExperimentalWalStore.Open(directory);
        WalStorageTests.AssertPages(recovered, 12, 21, 31);
    }

    [Fact]
    public void RetirementGrowthResetAndReaderBackpressure_KeepTailInCurrentGeneration()
    {
        string directory = Scratch.Create("tail-generations");
        long budget = ControlCodec.DataStart + 4L * ControlCodec.FrameSize;
        using (var store = Create(directory, budget: budget))
        {
            WalStorageTests.Seed(store);
            var capture = store.CaptureCheckpoint(); store.CopyCheckpoint(capture);
            store.Commit([new(3, WalStorageTests.Page(40))], 4, TestContext.Current.CancellationToken);
            store.RetireCheckpoint(capture);
            using var reader = store.BeginRead();
            Assert.Throws<PrototypeBusyException>(() => store.Commit([new(2, WalStorageTests.Page(31))], 4, TestContext.Current.CancellationToken));
            Assert.Equal(WalStorageTests.Page(40), reader.Read(3));
            reader.Dispose();
            store.Commit([new(2, WalStorageTests.Page(31))], 4, TestContext.Current.CancellationToken);
            Assert.Equal((ulong)2, store.Epoch);
        }
        using var recovered = ExperimentalWalStore.Open(directory, budget);
        WalStorageTests.AssertPages(recovered, 12, 21, 31, 40);
        recovered.Reset();
        Assert.Equal(ControlCodec.DataStart, recovered.PublishedTail);
    }

    [Fact]
    public void CommitPublicationFlushesDataThenEachControlBeforeReturning()
    {
        string directory = Scratch.Create("tail-io-cost");
        List<string> points = [];
        using var store = Create(directory, points.Add);
        points.Clear();
        store.Commit([new(0, WalStorageTests.Page(11))], 3, TestContext.Current.CancellationToken);
        Assert.Equal(new[] { "commit-marker-flushed", "control-active-0-flushed", "control-active-1-flushed" },
            points.Where(p => p.EndsWith("-flushed", StringComparison.Ordinal)));
        Assert.Equal(new[] { "control-active-0-written", "control-active-1-written" },
            points.Where(p => p.StartsWith("control-", StringComparison.Ordinal) && p.EndsWith("-written", StringComparison.Ordinal)));
        Scratch.WriteJson(Path.Combine(directory, "commit-io-cost.json"), new
        { durableWalFlushes = 3, extraFlushes = 2, extraControlBytesWritten = 8192, points });
    }

    [Fact]
    public void InterruptedBoundaryPublication_NeverDropsAnAcknowledgedBatch()
    {
        string root = Scratch.Create("tail-control-cuts");
        string seed = Path.Combine(root, "seed");
        using (var store = Create(seed)) WalStorageTests.Seed(store);
        byte[] oldWal = File.ReadAllBytes(WalStorageTests.WalPath(seed));
        byte[] database = File.ReadAllBytes(WalStorageTests.BasePath(seed));
        var control = ControlCodec.Select(oldWal.AsSpan(0, ControlCodec.DataStart), oldWal.Length).Current;
        byte[] staged = [.. oldWal, .. WalFrameCodec.Encode(new(0, WalStorageTests.Page(50)), 0, control.Salt),
            .. WalFrameCodec.Encode(new(1, WalStorageTests.Page(60)), 3, control.Salt)];
        string traceRoot = Path.Combine(root, "trace"); Directory.CreateDirectory(traceRoot);
        using var trace = new TraceFile(traceRoot, staged, database);
        var protocol = new ControlProtocol(trace);
        protocol.PrepareForWrites(); protocol.PublishCommittedTail(staged.Length);
        trace.Dispose();
        int oldRecovered = 0, newRecovered = 0, refused = 0;
        List<object> cases = [];
        foreach (var image in trace.Images)
            foreach (int lost in new[] { -1, 0, 1 })
            {
                string directory = Path.Combine(root, $"case-{cases.Count:D4}"); Directory.CreateDirectory(directory);
                byte[] bytes = (byte[])image.Wal.Clone();
                if (lost >= 0) bytes.AsSpan(lost * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
                File.WriteAllBytes(WalStorageTests.WalPath(directory), bytes);
                File.WriteAllBytes(WalStorageTests.BasePath(directory), database);
                byte[]? selected = null; ulong sequence = 0;
                for (int slot = 0; slot < 2; slot++)
                {
                    var candidate = bytes.AsSpan(slot * ControlCodec.SlotSize, ControlCodec.SlotSize).ToArray();
                    if (!SHA256.HashData(candidate.AsSpan(0, 4064)).AsSpan().SequenceEqual(candidate.AsSpan(4064))) continue;
                    ulong seq = BinaryPrimitives.ReadUInt64LittleEndian(candidate.AsSpan(48));
                    if (seq >= sequence) { sequence = seq; selected = candidate; }
                }
                string expected;
                if (selected is null) { WalStorageTests.AssertRejectedUnchanged(directory); refused++; expected = "refused"; }
                else
                {
                    long tail = checked((long)BinaryPrimitives.ReadUInt64LittleEndian(selected.AsSpan(96)));
                    Assert.True(tail == oldWal.Length || tail == staged.Length);
                    bool latest = tail == staged.Length;
                    using var recovered = ExperimentalWalStore.Open(directory);
                    WalStorageTests.AssertPages(recovered, latest ? (byte)50 : (byte)12, latest ? (byte)60 : (byte)21, 30);
                    Assert.Equal(tail, recovered.PhysicalLength);
                    if (latest) newRecovered++; else oldRecovered++;
                    expected = latest ? "new" : "old";
                }
                cases.Add(new { image.Point, lost, expected, originalWalSha256 = Convert.ToHexString(SHA256.HashData(bytes)) });
            }
        Scratch.WriteJson(Path.Combine(root, "tail-control-results.json"), new
        { count = cases.Count, oldRecovered, newRecovered, refused, cases });
    }
}
