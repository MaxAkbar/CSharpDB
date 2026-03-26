using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

[Collection("CrashHarnessProcess")]
public sealed class ProcessCrashDurabilityTests
{
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    [Fact]
    public async Task CommitReturned_WriteSurvivesProcessCrash()
    {
        var ct = TestContext.Current.CancellationToken;
        string workDir = NewTempDirectory();
        string dbPath = Path.Combine(workDir, "commit-returned.db");
        string markerPath = Path.Combine(workDir, "commit-returned.marker");

        try
        {
            await CreateTestTableAsync(dbPath, ct);

            CrashHarnessResult crash = await RunCrashHarnessAsync("commit-after-return", dbPath, markerPath, ct);
            AssertCrashAfterMarker(crash, markerPath);

            await using var db = await Database.OpenAsync(dbPath, ct);
            Assert.Equal(1L, await GetScalarAsync(db, "SELECT COUNT(*) FROM t", ct));
            Assert.Equal(101L, await GetScalarAsync(db, "SELECT val FROM t WHERE id = 1", ct));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    [Fact]
    public async Task CheckpointCrash_DoesNotLoseCommittedWrite()
        => await AssertCheckpointCrashDoesNotLoseCommittedWriteAsync(
            scenario: "checkpoint-start",
            markerFileName: "checkpoint-start.marker",
            expectedValue: 202L,
            TestContext.Current.CancellationToken);

    [Fact]
    public async Task CheckpointCrash_AfterDeviceFlush_DoesNotLoseCommittedWrite()
        => await AssertCheckpointCrashDoesNotLoseCommittedWriteAsync(
            scenario: "checkpoint-after-device-flush",
            markerFileName: "checkpoint-after-device-flush.marker",
            expectedValue: 303L,
            TestContext.Current.CancellationToken);

    [Fact]
    public async Task CheckpointCrash_AfterWalFinalize_DoesNotLoseCommittedWrite()
        => await AssertCheckpointCrashDoesNotLoseCommittedWriteAsync(
            scenario: "checkpoint-after-wal-finalize",
            markerFileName: "checkpoint-after-wal-finalize.marker",
            expectedValue: 404L,
            TestContext.Current.CancellationToken);

    private static async Task AssertCheckpointCrashDoesNotLoseCommittedWriteAsync(
        string scenario,
        string markerFileName,
        long expectedValue,
        CancellationToken ct)
    {
        string workDir = NewTempDirectory();
        string dbPath = Path.Combine(workDir, $"{scenario}.db");
        string markerPath = Path.Combine(workDir, markerFileName);

        try
        {
            await CreateTestTableAsync(dbPath, ct);

            CrashHarnessResult crash = await RunCrashHarnessAsync(scenario, dbPath, markerPath, ct);
            AssertCrashAfterMarker(crash, markerPath);

            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                Assert.Equal(1L, await GetScalarAsync(db, "SELECT COUNT(*) FROM t", ct));
                Assert.Equal(expectedValue, await GetScalarAsync(db, "SELECT val FROM t WHERE id = 1", ct));
                await db.CheckpointAsync(ct);
            }

            await using var reopened = await Database.OpenAsync(dbPath, ct);
            Assert.Equal(1L, await GetScalarAsync(reopened, "SELECT COUNT(*) FROM t", ct));
            Assert.Equal(expectedValue, await GetScalarAsync(reopened, "SELECT val FROM t WHERE id = 1", ct));
        }
        finally
        {
            DeleteDirectoryIfExists(workDir);
        }
    }

    private static void AssertCrashAfterMarker(CrashHarnessResult crash, string markerPath)
    {
        if (crash.ExitCode == 0)
            throw new Xunit.Sdk.XunitException($"Crash harness exited cleanly.\nSTDOUT:\n{crash.StdOut}\nSTDERR:\n{crash.StdErr}");

        if (!File.Exists(markerPath))
            throw new Xunit.Sdk.XunitException($"Crash harness exited before writing marker '{markerPath}'.\nSTDOUT:\n{crash.StdOut}\nSTDERR:\n{crash.StdErr}");
    }

    private static async Task CreateTestTableAsync(string dbPath, CancellationToken ct)
    {
        await using var db = await Database.OpenAsync(dbPath, ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ct);
    }

    private static async Task<long> GetScalarAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        var rows = await result.ToListAsync(ct);
        return rows[0][0].AsInteger;
    }

    private static async Task<CrashHarnessResult> RunCrashHarnessAsync(
        string scenario,
        string dbPath,
        string markerPath,
        CancellationToken ct)
    {
        string benchmarksProjectPath = FindBenchmarksProjectPath();
        var startInfo = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(benchmarksProjectPath)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        startInfo.ArgumentList.Add("run");
        startInfo.ArgumentList.Add("--project");
        startInfo.ArgumentList.Add(benchmarksProjectPath);
        startInfo.ArgumentList.Add("--no-launch-profile");
        startInfo.ArgumentList.Add("-c");
        startInfo.ArgumentList.Add(BuildConfiguration);
        startInfo.ArgumentList.Add("--");
        startInfo.ArgumentList.Add("--crash-harness");
        startInfo.ArgumentList.Add(scenario);
        startInfo.ArgumentList.Add(dbPath);
        startInfo.ArgumentList.Add(markerPath);

        using var process = new Process { StartInfo = startInfo };
        if (!process.Start())
            throw new InvalidOperationException("Failed to start crash harness process.");

        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        Task<string> stderrTask = process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);

        return new CrashHarnessResult(
            process.ExitCode,
            StripAnsi(await stdoutTask),
            StripAnsi(await stderrTask));
    }

    private static string FindBenchmarksProjectPath()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate = Path.Combine(
                current.FullName,
                "tests",
                "CSharpDB.Benchmarks",
                "CSharpDB.Benchmarks.csproj");
            if (File.Exists(candidate))
                return candidate;

            current = current.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate tests/CSharpDB.Benchmarks/CSharpDB.Benchmarks.csproj.");
    }

    private static string StripAnsi(string value)
        => Regex.Replace(value, @"\x1B\[[0-9;]*m", string.Empty);

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_crash_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (!Directory.Exists(path))
            return;

        Directory.Delete(path, recursive: true);
    }

    private sealed record CrashHarnessResult(int ExitCode, string StdOut, string StdErr);
}
