using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CSharpDB.WalPrototype.Tests;

public sealed class ControlProtocolTests
{
    [Theory]
    [InlineData("retire")]
    [InlineData("retire-corrupt-superseded")]
    [InlineData("retire-corrupt-indexed")]
    [InlineData("reset")]
    [InlineData("recover-empty-missing")]
    [InlineData("recover-empty-stale")]
    [InlineData("recover-active-empty")]
    [InlineData("recover-active-missing")]
    [InlineData("recover-active-retired-stale")]
    public void InterruptedPublication_ReopensExactStateOrFailsClosedAsSpecified(string scenario)
    {
        string root = Scratch.Create(scenario);
        var (wal, database, corruptOffset) = InitialState(scenario);
        using var trace = new TraceFile(root, wal, database);
        var protocol = new ControlProtocol(trace);
        Assert.False(protocol.WritersAdmitted);
        protocol.PrepareForWrites();
        if (scenario.StartsWith("retire", StringComparison.Ordinal))
            protocol.Retire(ControlCodec.DataStart + 2L * ControlCodec.FrameSize, 3,
                () => trace.CopyAndFlushBase(PageFixture.RetiredPages));
        else if (scenario == "reset")
            protocol.Reset(3, true, () => trace.CopyAndFlushBase(PageFixture.FinalPages), _ => new(7, 8));
        Assert.True(protocol.WritersAdmitted);
        trace.Dispose();

        int recovered = 0, rejected = 0;
        List<object> results = [];
        foreach (var image in trace.Images)
            foreach (int lostSlot in new[] { -1, 0, 1 })
            {
                byte[] bytes = (byte[])image.Wal.Clone();
                if (lostSlot >= 0) bytes.AsSpan(lostSlot * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
                string name = $"case-{results.Count:D4}";
                string walPath = Path.Combine(root, name + ".wal"), basePath = Path.Combine(root, name + ".base");
                File.WriteAllBytes(walPath, bytes);
                File.WriteAllBytes(basePath, image.Database);
                bool expectFailure = ExpectedFailure(bytes, corruptOffset);
                bool failed = false;
                try
                {
                    using (var reopened = new FileControlStore(walPath))
                    {
                        var recovery = new ControlProtocol(reopened);
                        Assert.False(recovery.WritersAdmitted);
                    }
                    Assert.Equal(PageFixture.Database(PageFixture.FinalPages),
                        PageFixture.Recover(File.ReadAllBytes(walPath), File.ReadAllBytes(basePath)));
                    recovered++;
                }
                catch (InvalidDataException)
                {
                    failed = true;
                    rejected++;
                }
                Assert.True(expectFailure == failed, $"{scenario}: {image.Point}, lost={lostSlot}, expected failure={expectFailure}");
                Assert.Equal(bytes, File.ReadAllBytes(walPath));
                Assert.Equal(image.Database, File.ReadAllBytes(basePath));
                results.Add(new { name, image.Point, lostSlot, failed,
                    walSha256 = Convert.ToHexString(SHA256.HashData(bytes)),
                    baseSha256 = Convert.ToHexString(SHA256.HashData(image.Database)) });
            }

        // Successful admission must survive loss of either slot AFTER a fresh acknowledged append.
        byte[] finalWal = File.ReadAllBytes(Path.Combine(root, "working.wal"));
        byte[] finalBase = File.ReadAllBytes(Path.Combine(root, "working.base"));
        var selected = ControlCodec.Select(finalWal.AsSpan(0, ControlCodec.DataStart), finalWal.Length);
        Assert.True(selected.Mirrored);
        Assert.Equal(ControlMode.Active, selected.Current.Mode);
        for (int lost = 0; lost < 2; lost++)
        {
            byte[] appended = [.. finalWal, .. PageFixture.Frame(2, 31, selected.Current.Salt)];
            appended.AsSpan(lost * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
            string path = Path.Combine(root, $"acknowledged-append-lost-{lost}.wal");
            File.WriteAllBytes(path, appended);
            Assert.Equal(PageFixture.Database([12, 21, 31]), PageFixture.Recover(File.ReadAllBytes(path), finalBase));
        }
        Scratch.WriteJson(Path.Combine(root, "results.json"), new
        { scenario, recovered, rejected, crashCases = results.Count, acknowledgedAppendCases = 2, trace.Operations, cases = results });
    }

    [Fact]
    public void ResetFlushOrder_ClosesAdmissionUntilBothActiveSlotsComplete()
    {
        string root = Scratch.Create("flush-order");
        using var trace = new TraceFile(root, PageFixture.Wal(), PageFixture.Database(PageFixture.InitialPages));
        var protocol = new ControlProtocol(trace);
        protocol.PrepareForWrites();
        trace.OnOperation = _ => Assert.False(protocol.WritersAdmitted);
        protocol.Reset(3, true, () => trace.CopyAndFlushBase(PageFixture.FinalPages), _ => new(7, 8));
        Assert.Equal(new[] { "flush-wal", "write-base", "flush-base", "write-slot-0", "flush-wal", "write-slot-1",
            "flush-wal", "truncate", "flush-wal", "write-slot-0", "flush-wal", "write-slot-1", "flush-wal" }, trace.Operations);
        Assert.True(protocol.WritersAdmitted);
        Assert.Equal((ulong)6, protocol.Current.Sequence);
        Assert.Equal((ulong)2, protocol.Current.Epoch);
        Assert.Equal(ControlCodec.DataStart, trace.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AnyResetIoFailure_StopsFurtherIoAndRequiresReopen(bool after)
    {
        for (int operation = 1; operation <= 12; operation++)
        {
            string root = Scratch.Create($"io-failure-{operation}-{after}");
            // Operation 1 is the recovery durability barrier, before the reset under test.
            using var trace = new TraceFile(root, PageFixture.Wal(), PageFixture.Database(PageFixture.InitialPages), operation + 1, after);
            var protocol = new ControlProtocol(trace);
            protocol.PrepareForWrites();
            Assert.Throws<IOException>(() => protocol.Reset(3, true,
                () => trace.CopyAndFlushBase(PageFixture.FinalPages), _ => new(7, 8)));
            Assert.False(protocol.WritersAdmitted);
            Assert.Equal(operation + 1, trace.Operations.Count);
            Assert.Throws<InvalidOperationException>(protocol.PrepareForWrites);
            Assert.Equal(operation + 1, trace.Operations.Count);
            trace.Dispose();
            byte[] wal = File.ReadAllBytes(Path.Combine(root, "working.wal"));
            byte[] database = File.ReadAllBytes(Path.Combine(root, "working.base"));
            Assert.Equal(PageFixture.Database(PageFixture.FinalPages), PageFixture.Recover(wal, database));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CounterExhaustion_IsReadableButCannotWrite(bool epoch)
    {
        var record = ControlCodecTests.Record(epoch ? 1 : ulong.MaxValue) with
        { Epoch = epoch ? ulong.MaxValue : 1 };
        string root = Scratch.Create("counter-exhaustion");
        byte[] wal = ControlCodecTests.Header(record, record);
        using var trace = new TraceFile(root, wal, PageFixture.Database(PageFixture.FinalPages));
        var protocol = new ControlProtocol(trace);
        Assert.Equal(record, protocol.Current);
        Assert.Throws<InvalidOperationException>(protocol.PrepareForWrites);
        Assert.Empty(trace.Operations);
        Assert.False(protocol.WritersAdmitted);
    }

    [Theory]
    [InlineData("sequences")]
    [InlineData("epoch")]
    [InlineData("salt")]
    [InlineData("busy")]
    [InlineData("floor")]
    public void InvalidMaintenanceRequest_FailsBeforeBaseCopyOrWalIo(string kind)
    {
        var record = ControlCodecTests.Record(kind == "sequences" ? ulong.MaxValue - 4 : 1) with
        { Epoch = kind == "epoch" ? ulong.MaxValue - 1 : 1 };
        byte[] wal = ControlCodecTests.Header(record, record);
        using var trace = new TraceFile(Scratch.Create(kind), wal, PageFixture.Database(PageFixture.FinalPages));
        var protocol = new ControlProtocol(trace);
        protocol.PrepareForWrites();
        int copies = 0;
        Action action = kind == "floor"
            ? () => protocol.Retire(ControlCodec.DataStart + 1, 3, () => copies++)
            : () => protocol.Reset(3, kind != "busy", () => copies++,
                old => kind == "salt" ? old : new(7, 8));
        if (kind is "floor" or "salt") Assert.Throws<InvalidDataException>(action);
        else Assert.Throws<InvalidOperationException>(action);
        Assert.Equal(0, copies);
        Assert.Equal(new[] { "flush-wal" }, trace.Operations);
        Assert.False(protocol.WritersAdmitted);
    }

    [Fact]
    public void RecoveredMirroredControls_AreFlushedBeforeAdmissionOrDestructiveIo()
    {
        foreach (bool emptyMode in new[] { false, true })
        {
            var record = ControlCodecTests.Record() with { Mode = emptyMode ? ControlMode.Empty : ControlMode.Active };
            byte[] wal = [.. ControlCodecTests.Header(record, record with { Sequence = 2 }), .. PageFixture.Wal()[8192..]];
            using var trace = new TraceFile(Scratch.Create("recovery-flush"), wal, PageFixture.Database(PageFixture.FinalPages));
            var protocol = new ControlProtocol(trace);
            trace.OnOperation = _ => Assert.False(protocol.WritersAdmitted);
            protocol.PrepareForWrites();
            Assert.Equal("flush-wal", trace.Operations[0]);
            Assert.True(protocol.WritersAdmitted);
            if (emptyMode) Assert.Equal("truncate", trace.Operations[1]);
            else Assert.Single(trace.Operations);

            foreach (bool after in new[] { false, true })
            {
                using var failing = new TraceFile(Scratch.Create("recovery-flush-failure"), wal,
                    PageFixture.Database(PageFixture.FinalPages), throwAt: 1, throwAfter: after);
                var recovery = new ControlProtocol(failing);
                Assert.Throws<IOException>(recovery.PrepareForWrites);
                Assert.False(recovery.WritersAdmitted);
                Assert.Equal(new[] { "flush-wal" }, failing.Operations);
            }
        }
    }

    [Fact]
    public void NegativeOrderingControls_ExposeHiddenCommitAndUnusableBoundary()
    {
        var old = ControlCodecTests.Record(2);
        var empty = old with { Sequence = 3, Epoch = 2, Salt = new(7, 8), Mode = ControlMode.Empty };
        var active = empty with { Sequence = 5, Mode = ControlMode.Active };
        // EMPTY before the base flush silently omits acknowledged WAL state.
        byte[] prematureEmpty = [.. ControlCodecTests.Header(empty, empty with { Sequence = 4 }), .. PageFixture.Wal()[8192..]];
        Assert.False(PageFixture.Database(PageFixture.FinalPages).AsSpan().SequenceEqual(
            PageFixture.Recover(prematureEmpty, PageFixture.Database(PageFixture.InitialPages))));
        // Admit a commit with only one ACTIVE, then lose it: surviving EMPTY hides that commit.
        byte[] prematureAppend = [.. ControlCodecTests.Header(active, empty), .. PageFixture.Frame(2, 31, active.Salt)];
        prematureAppend.AsSpan(0, ControlCodec.SlotSize).Clear();
        Assert.False(PageFixture.Database([12, 21, 31]).AsSpan().SequenceEqual(
            PageFixture.Recover(prematureAppend, PageFixture.Database(PageFixture.FinalPages))));
        // Truncate before mirrored EMPTY, then lose it: the old floor points past EOF.
        byte[] prematureTruncate = ControlCodecTests.Header(empty,
            old with { Floor = ControlCodec.DataStart + ControlCodec.FrameSize });
        prematureTruncate.AsSpan(0, ControlCodec.SlotSize).Clear();
        Assert.Throws<InvalidDataException>(() => PageFixture.Recover(prematureTruncate, PageFixture.Database(PageFixture.FinalPages)));
        // Declare retirement complete after one floor write; lose it and expose obsolete corruption.
        byte[] prematureRetirement = PageFixture.Wal();
        ControlCodec.Encode(old with { Sequence = 3, Floor = ControlCodec.DataStart + 2L * ControlCodec.FrameSize })
            .CopyTo(prematureRetirement, 0);
        prematureRetirement[ControlCodec.DataStart + 24] ^= 1;
        prematureRetirement.AsSpan(0, ControlCodec.SlotSize).Clear();
        Assert.Throws<InvalidDataException>(() => PageFixture.Recover(prematureRetirement, PageFixture.Database(PageFixture.RetiredPages)));
    }

    private static (byte[] Wal, byte[] Database, int? CorruptOffset) InitialState(string scenario)
    {
        byte[] wal = PageFixture.Wal(), database = PageFixture.Database(PageFixture.InitialPages);
        int? corruptOffset = null;
        var empty = ControlCodecTests.Record(3) with { Mode = ControlMode.Empty, Epoch = 2, Salt = new(7, 8) };
        if (scenario.Contains("corrupt", StringComparison.Ordinal))
        {
            corruptOffset = ControlCodec.DataStart + (scenario.EndsWith("indexed", StringComparison.Ordinal) ? ControlCodec.FrameSize : 0);
            wal[corruptOffset.Value + 24] ^= 1;
        }
        else if (scenario.StartsWith("recover-empty", StringComparison.Ordinal))
        {
            ControlCodec.Encode(empty).CopyTo(wal, 0);
            if (scenario.EndsWith("missing", StringComparison.Ordinal)) wal.AsSpan(4096, 4096).Clear();
            database = PageFixture.Database(PageFixture.FinalPages);
        }
        else if (scenario == "recover-active-empty")
        {
            wal = ControlCodecTests.Header(empty with { Sequence = 5, Mode = ControlMode.Active }, empty with { Sequence = 4 });
            database = PageFixture.Database(PageFixture.FinalPages);
        }
        else if (scenario == "recover-active-missing") wal.AsSpan(4096, 4096).Clear();
        else if (scenario == "recover-active-retired-stale")
        {
            ControlCodec.Encode(ControlCodecTests.Record(3) with { Floor = ControlCodec.DataStart + 2L * ControlCodec.FrameSize }).CopyTo(wal, 0);
            database = PageFixture.Database(PageFixture.RetiredPages);
        }
        return (wal, database, corruptOffset);
    }

    // Independent oracle for this bounded fixture: hash the two slots, select the largest
    // encoded sequence, and reject only absent controls or an explicitly injected active error.
    private static bool ExpectedFailure(byte[] wal, int? corruptOffset)
    {
        byte[]? selected = null;
        ulong sequence = 0;
        for (int slot = 0; slot < 2; slot++)
        {
            byte[] bytes = wal.AsSpan(slot * ControlCodec.SlotSize, ControlCodec.SlotSize).ToArray();
            if (!SHA256.HashData(bytes.AsSpan(0, 4064)).AsSpan().SequenceEqual(bytes.AsSpan(4064))) continue;
            ulong candidate = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(48));
            if (candidate >= sequence) { selected = bytes; sequence = candidate; }
        }
        if (selected is null) return true;
        bool active = BinaryPrimitives.ReadUInt32LittleEndian(selected.AsSpan(72)) == 1;
        ulong floor = BinaryPrimitives.ReadUInt64LittleEndian(selected.AsSpan(80));
        return active && corruptOffset is int offset && floor <= (ulong)offset;
    }
}
