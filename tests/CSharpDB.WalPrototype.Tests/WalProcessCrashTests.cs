using System.Diagnostics;
using System.Text.Json;

namespace CSharpDB.WalPrototype.Tests;

public sealed class WalProcessCrashTests
{
    [Theory]
    [InlineData("commit", "frame-0-header-written", "reject")]
    [InlineData("commit", "frame-0-half-written", "reject")]
    [InlineData("commit", "frame-0-written", "old")]
    [InlineData("commit", "frame-1-header-written", "reject")]
    [InlineData("commit", "frame-1-half-written", "reject")]
    [InlineData("commit", "frame-1-written", "old")]
    [InlineData("commit", "commit-marker-half-written", "reject")]
    [InlineData("commit", "commit-marker-written", "new")]
    [InlineData("commit", "commit-marker-flushed", "new")]
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
    public async Task KilledWorker_ReopensWholeCommittedStateOrRejectsAmbiguousTail(string operation, string point, string expected)
    {
        string directory = Scratch.Create($"process-{operation}-{point}");
        using (var store = WalStorageTests.Create(directory)) WalStorageTests.Seed(store);
        await KillAt(directory, operation, point, "crash");
        PreserveBeforeRecovery(directory, "before");
        if (expected == "reject") WalStorageTests.AssertRejectedUnchanged(directory);
        else
        {
            using var recovered = ExperimentalWalStore.Open(directory);
            WalStorageTests.AssertPages(recovered, expected == "new" ? (byte)50 : (byte)12,
                expected == "new" ? (byte)60 : (byte)21, expected == "suffix" ? (byte)31 : (byte)30);
            // A new commit after restart must not join any discarded uncommitted batch.
            recovered.Commit([new(2, WalStorageTests.Page(32))], 3, TestContext.Current.CancellationToken);
        }
        if (expected != "reject")
        {
            using var reopened = ExperimentalWalStore.Open(directory);
            WalStorageTests.AssertPages(reopened, expected == "new" ? (byte)50 : (byte)12,
                expected == "new" ? (byte)60 : (byte)21, 32);
        }
        Scratch.WriteJson(Path.Combine(directory, "process-result.json"), new { operation, point, expected, status = "passed" });
    }

    [Theory]
    [InlineData("control-empty-1-flushed")]
    [InlineData("truncate-written")]
    [InlineData("truncate-flushed")]
    [InlineData("control-active-0-flushed")]
    [InlineData("control-active-1-flushed")]
    [InlineData("after-recovery-return")]
    public async Task RecoveryOfInterruptedReset_CanItselfBeInterrupted(string recoveryPoint)
    {
        string directory = Scratch.Create("process-repeated-recovery");
        using (var store = WalStorageTests.Create(directory)) WalStorageTests.Seed(store);
        await KillAt(directory, "reset", "control-empty-0-flushed", "first");
        PreserveBeforeRecovery(directory, "after-first");
        await KillAt(directory, "recover", recoveryPoint, "second");
        PreserveBeforeRecovery(directory, "after-second");
        using (var recovered = ExperimentalWalStore.Open(directory))
        {
            WalStorageTests.AssertPages(recovered, 12, 21, 30);
            Assert.Equal(ControlCodec.DataStart, recovered.PhysicalLength);
            recovered.Commit([new(2, WalStorageTests.Page(31))], 3, TestContext.Current.CancellationToken);
        }
        // Both ACTIVE copies must now protect a post-recovery acknowledged commit.
        for (int lost = 0; lost < 2; lost++)
        {
            string copy = Path.Combine(directory, "lost-slot-" + lost);
            Directory.CreateDirectory(copy);
            File.Copy(WalStorageTests.BasePath(directory), WalStorageTests.BasePath(copy));
            byte[] wal = File.ReadAllBytes(WalStorageTests.WalPath(directory));
            wal.AsSpan(lost * ControlCodec.SlotSize, ControlCodec.SlotSize).Clear();
            File.WriteAllBytes(WalStorageTests.WalPath(copy), wal);
            using var reopened = ExperimentalWalStore.Open(copy);
            WalStorageTests.AssertPages(reopened, 12, 21, 31);
        }
        Scratch.WriteJson(Path.Combine(directory, "process-result.json"), new
        { operation = "repeated-recovery", point = recoveryPoint, expected = "old-then-new-append", status = "passed", kills = 2, slotLossCases = 2 });
    }

    internal static async Task KillAt(string directory, string operation, string point, string name)
    {
        string worker = Path.Combine(AppContext.BaseDirectory, "crash-worker", "CSharpDB.WalPrototype.CrashHarness.dll");
        Assert.True(File.Exists(worker), "Build the test project to produce its isolated crash worker.");
        string marker = Path.Combine(directory, name + "-marker.json");
        var info = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true
        };
        foreach (string argument in new[] { worker, operation, point, directory, marker }) info.ArgumentList.Add(argument);
        using var process = Process.Start(info) ?? throw new InvalidOperationException("Worker did not start.");
        Task<string> stdout = process.StandardOutput.ReadToEndAsync(), stderr = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try { await process.WaitForExitAsync(timeout.Token); }
        catch (OperationCanceledException)
        {
            process.Kill(); // Only the exact worker created above, never unrelated processes.
            await process.WaitForExitAsync();
            throw new TimeoutException("Worker did not reach " + point);
        }
        string output = await stdout, error = await stderr;
        File.WriteAllText(Path.Combine(directory, name + "-stdout.log"), output);
        File.WriteAllText(Path.Combine(directory, name + "-stderr.log"), error);
        Assert.True(File.Exists(marker), $"Worker exited {process.ExitCode} before {point}: {error}");
        Assert.NotEqual(0, process.ExitCode);
        using var document = JsonDocument.Parse(File.ReadAllText(marker));
        Assert.Equal(point, document.RootElement.GetProperty("point").GetString());
        Assert.Equal(process.Id, document.RootElement.GetProperty("pid").GetInt32());
        Scratch.WriteJson(Path.Combine(directory, name + "-exit.json"), new { pid = process.Id, process.ExitCode, operation, point });
    }

    internal static void PreserveBeforeRecovery(string directory, string prefix)
    {
        File.Copy(WalStorageTests.WalPath(directory), Path.Combine(directory, prefix + ".wal"));
        File.Copy(WalStorageTests.BasePath(directory), Path.Combine(directory, prefix + ".pages"));
    }
}
