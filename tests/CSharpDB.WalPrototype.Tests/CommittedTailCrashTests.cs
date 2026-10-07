namespace CSharpDB.WalPrototype.Tests;

public sealed class CommittedTailCrashTests
{
    [Theory]
    [InlineData("commit", "frame-0-header-written", "old")]
    [InlineData("commit", "frame-0-half-written", "old")]
    [InlineData("commit", "frame-0-written", "old")]
    [InlineData("commit", "frame-1-header-written", "old")]
    [InlineData("commit", "frame-1-half-written", "old")]
    [InlineData("commit", "frame-1-written", "old")]
    [InlineData("commit", "commit-marker-half-written", "old")]
    [InlineData("commit", "commit-marker-written", "old")]
    [InlineData("commit", "commit-marker-flushed", "old")]
    [InlineData("commit", "control-active-0-written", "new")]
    [InlineData("commit", "control-active-0-flushed", "new")]
    [InlineData("commit", "control-active-1-written", "new")]
    [InlineData("commit", "control-active-1-flushed", "new")]
    [InlineData("commit", "after-commit-return", "new")]
    [InlineData("copy", "base-page-0-written", "old")]
    [InlineData("copy", "base-page-1-written", "old")]
    [InlineData("copy", "base-flushed", "old")]
    [InlineData("retire", "control-active-0-written", "suffix")]
    [InlineData("retire", "control-active-0-flushed", "suffix")]
    [InlineData("retire", "control-active-1-written", "suffix")]
    [InlineData("retire", "control-active-1-flushed", "suffix")]
    [InlineData("retire", "floor-published", "suffix")]
    [InlineData("reset", "base-page-0-written", "old")]
    [InlineData("reset", "base-flushed", "old")]
    [InlineData("reset", "control-empty-0-written", "old")]
    [InlineData("reset", "control-empty-0-flushed", "old")]
    [InlineData("reset", "control-empty-1-written", "old")]
    [InlineData("reset", "control-empty-1-flushed", "old")]
    [InlineData("reset", "truncate-written", "old")]
    [InlineData("reset", "truncate-flushed", "old")]
    [InlineData("reset", "control-active-0-written", "old")]
    [InlineData("reset", "control-active-0-flushed", "old")]
    [InlineData("reset", "control-active-1-written", "old")]
    [InlineData("reset", "control-active-1-flushed", "old")]
    [InlineData("reset", "reset-complete", "old")]
    public async Task CommittedTail_RecoversAllSelectedCrashPoints(string operation, string point, string expected)
    {
        string directory = Scratch.Create("tail-process-" + point);
        using (var store = CommittedTailTests.Create(directory)) WalStorageTests.Seed(store);
        await WalProcessCrashTests.KillAt(directory, operation, point, "crash");
        WalProcessCrashTests.PreserveBeforeRecovery(directory, "before");
        using (var recovered = ExperimentalWalStore.Open(directory))
        {
            WalStorageTests.AssertPages(recovered, expected == "new" ? (byte)50 : (byte)12,
                expected == "new" ? (byte)60 : (byte)21, expected == "suffix" ? (byte)31 : (byte)30);
            recovered.Commit([new(2, WalStorageTests.Page(32))], 3, TestContext.Current.CancellationToken);
        }
        using (var reopened = ExperimentalWalStore.Open(directory))
            WalStorageTests.AssertPages(reopened, expected == "new" ? (byte)50 : (byte)12,
                expected == "new" ? (byte)60 : (byte)21, 32);
        Scratch.WriteJson(Path.Combine(directory, "tail-process-result.json"), new { operation, point, expected, kills = 1, status = "passed" });
    }

    [Theory]
    [InlineData("control-empty-1-flushed")]
    [InlineData("truncate-written")]
    [InlineData("truncate-flushed")]
    [InlineData("control-active-0-flushed")]
    [InlineData("control-active-1-flushed")]
    [InlineData("after-recovery-return")]
    public async Task InterruptedResetRecovery_PreservesCommittedTailAcrossGeneration(string recoveryPoint)
    {
        string directory = Scratch.Create("tail-process-recovery");
        using (var store = CommittedTailTests.Create(directory)) WalStorageTests.Seed(store);
        await WalProcessCrashTests.KillAt(directory, "reset", "control-empty-0-flushed", "first");
        WalProcessCrashTests.PreserveBeforeRecovery(directory, "after-first");
        await WalProcessCrashTests.KillAt(directory, "recover", recoveryPoint, "second");
        WalProcessCrashTests.PreserveBeforeRecovery(directory, "after-second");
        using (var recovered = ExperimentalWalStore.Open(directory))
        {
            WalStorageTests.AssertPages(recovered, 12, 21, 30);
            Assert.Equal(ControlCodec.DataStart, recovered.PhysicalLength);
            recovered.Commit([new(2, WalStorageTests.Page(31))], 3, TestContext.Current.CancellationToken);
        }
        for (int lost = 0; lost < 2; lost++)
        {
            string copy = Path.Combine(directory, "lost-" + lost); Directory.CreateDirectory(copy);
            byte[] wal = File.ReadAllBytes(WalStorageTests.WalPath(directory));
            wal.AsSpan(lost * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
            File.WriteAllBytes(WalStorageTests.WalPath(copy), wal);
            File.Copy(WalStorageTests.BasePath(directory), WalStorageTests.BasePath(copy));
            using var reopened = ExperimentalWalStore.Open(copy);
            WalStorageTests.AssertPages(reopened, 12, 21, 31);
        }
        Scratch.WriteJson(Path.Combine(directory, "tail-process-result.json"), new
        { operation = "repeated-recovery", point = recoveryPoint, expected = "old-then-new-append", kills = 2, slotLossCases = 2, status = "passed" });
    }
}
