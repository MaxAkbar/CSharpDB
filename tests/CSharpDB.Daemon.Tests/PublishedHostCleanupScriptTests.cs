using System.Diagnostics;

namespace CSharpDB.Daemon.Tests;

public sealed class PublishedHostCleanupScriptTests
{
    [Fact]
    public async Task WindowsCleanup_RetriesUntilTransientFileLockIsReleased()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = CreateTemporaryRoot();
        string lockedFile = Path.Combine(temporaryRoot, "CSharpDB.Daemon.exe");
        await File.WriteAllTextAsync(lockedFile, "fixture", Ct);

        try
        {
            using FileStream lockStream = File.Open(
                lockedFile,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None);
            using Process process = StartCleanupHarness(temporaryRoot, timeoutMilliseconds: 5000);

            Assert.Equal("READY", await ReadLineAsync(process));
            await Task.Delay(250, Ct);
            Assert.False(process.HasExited);

            lockStream.Dispose();
            await process.WaitForExitAsync(Ct);
            string stdout = await process.StandardOutput.ReadToEndAsync(Ct);
            string stderr = await process.StandardError.ReadToEndAsync(Ct);

            Assert.True(process.ExitCode == 0, stdout + Environment.NewLine + stderr);
            Assert.Contains("DELETED", stdout, StringComparison.Ordinal);
            Assert.False(Directory.Exists(temporaryRoot));
        }
        finally
        {
            TryDeleteTemporaryRoot(temporaryRoot);
        }
    }

    [Fact]
    public async Task WindowsCleanup_FailsClosedWhenFileLockOutlivesDeadline()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string temporaryRoot = CreateTemporaryRoot();
        string lockedFile = Path.Combine(temporaryRoot, "CSharpDB.Daemon.exe");
        await File.WriteAllTextAsync(lockedFile, "fixture", Ct);

        try
        {
            using FileStream lockStream = File.Open(
                lockedFile,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.None);
            using Process process = StartCleanupHarness(temporaryRoot, timeoutMilliseconds: 300);

            Assert.Equal("READY", await ReadLineAsync(process));
            await process.WaitForExitAsync(Ct);
            string stdout = await process.StandardOutput.ReadToEndAsync(Ct);
            string stderr = await process.StandardError.ReadToEndAsync(Ct);

            Assert.NotEqual(0, process.ExitCode);
            Assert.DoesNotContain("DELETED", stdout, StringComparison.Ordinal);
            Assert.True(Directory.Exists(temporaryRoot), stderr);
        }
        finally
        {
            TryDeleteTemporaryRoot(temporaryRoot);
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Process StartCleanupHarness(string targetPath, int timeoutMilliseconds)
    {
        string helperPath = Path.Combine(
            FindRepoRoot(),
            "scripts",
            "CSharpDbHostQualificationCleanup.ps1");
        ProcessStartInfo startInfo = new()
        {
            FileName = "pwsh",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (string argument in new[]
        {
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "& { param($helperPath, $targetPath, $timeoutMilliseconds) " +
                ". $helperPath; Write-Output 'READY'; " +
                "Remove-CSharpDbDirectoryWithRetry -LiteralPath $targetPath " +
                "-TimeoutMilliseconds $timeoutMilliseconds -RetryDelayMilliseconds 50; " +
                "Write-Output 'DELETED' }",
            helperPath,
            targetPath,
            timeoutMilliseconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
        })
        {
            startInfo.ArgumentList.Add(argument);
        }

        return Process.Start(startInfo) ??
            throw new InvalidOperationException("Could not start the cleanup test harness.");
    }

    private static async Task<string?> ReadLineAsync(Process process)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        return await process.StandardOutput.ReadLineAsync(timeout.Token);
    }

    private static string CreateTemporaryRoot()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            "csharpdb-host-cleanup-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteTemporaryRoot(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private static string FindRepoRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);

        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root from test base directory.");
    }
}
