using System.Diagnostics;
using CSharpDB.Migration;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class MySqlWorkerClientTests
{
    private const string ConnectionEnvironment =
        "CSHARPDB_TEST_MYSQL_WORKER_CONNECTION";
    private const string ModeEnvironment =
        "CSHARPDB_TEST_MYSQL_WORKER_MODE";
    private const string PidFileEnvironment =
        "CSHARPDB_TEST_MYSQL_WORKER_PID_FILE";
    private const string WorkerSecret = "worker-stderr-secret";
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    [Fact]
    public async Task Inspect_MissingWorkerFailsBeforeConnectionMaterialIsUsed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: false);
        using var connection = new EnvironmentScope(
            ConnectionEnvironment,
            "Password=base-process-must-not-read-this");
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Contains(
                "MIG-MYSQL-CLI-ADAPTER-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "base-process-must-not-read-this",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.False(File.Exists(catalogPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_ValidFramedWorkerCatalogIsPublished()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(ModeEnvironment, "success");
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, exitCode);
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
            Assert.Contains(
                "Status: OK",
                output.ToString(),
                StringComparison.Ordinal);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(
                    await File.ReadAllTextAsync(catalogPath, ct));
            Assert.Equal(MigrationSourceKind.MySql, catalog.Source.Kind);
            Assert.Equal(
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                catalog.TargetCSharpDbVersion);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_WorkerResolvesOnlyTheNamedInheritedEnvironmentValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(
            ModeEnvironment,
            "connection-check");
        using var connection = new EnvironmentScope(
            ConnectionEnvironment,
            "Server=worker.example;Password=inherited-worker-secret");
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, exitCode);
            Assert.DoesNotContain(
                "inherited-worker-secret",
                output.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "inherited-worker-secret",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.True(File.Exists(catalogPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("bad-header")]
    [InlineData("invalid-utf8")]
    [InlineData("wrong-source")]
    [InlineData("wrong-target")]
    [InlineData("internal-error")]
    public async Task Inspect_IncompatibleWorkerOutputFailsClosed(string modeValue)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(ModeEnvironment, modeValue);
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Contains(
                "MIG-MYSQL-CLI-ADAPTER-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                WorkerSecret,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.False(File.Exists(catalogPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("connection-error", "MIG-MYSQL-CLI-CONNECTION-001")]
    [InlineData("inspection-error", "MIG-MYSQL-CLI-INSPECT-001")]
    public async Task Inspect_WorkerFailureIgnoresSecretBearingStderr(
        string modeValue,
        string expectedCode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(ModeEnvironment, modeValue);
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Contains(
                expectedCode,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                WorkerSecret,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.False(File.Exists(catalogPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("stdout-overflow")]
    [InlineData("stderr-overflow")]
    public async Task Inspect_BoundedWorkerOutputViolationFailsClosed(
        string modeValue)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(ModeEnvironment, modeValue);
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await RunInspectAsync(
                catalogPath,
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Contains(
                "MIG-MYSQL-CLI-ADAPTER-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.False(File.Exists(catalogPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_CancellationKillsWorkerProcessTree()
    {
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(ModeEnvironment, "hang-tree");
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string pidPath = Path.Combine(directory, "worker-pids.txt");
        using var pidFile = new EnvironmentScope(PidFileEnvironment, pidPath);
        using var cancellation = new CancellationTokenSource();
        var output = new StringWriter();
        var error = new StringWriter();
        int[] processIds = [];

        try
        {
            Task<int> run = RunInspectAsync(
                    catalogPath,
                    output,
                    error,
                    cancellation.Token)
                .AsTask();
            processIds = await WaitForProcessIdsAsync(
                pidPath,
                expectedCount: 2,
                TestContext.Current.CancellationToken);

            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await run);
            await WaitForProcessesToExitAsync(
                processIds,
                TestContext.Current.CancellationToken);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        }
        finally
        {
            cancellation.Cancel();
            foreach (int processId in processIds)
                TryKill(processId);
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_OutputViolationKillsWorkerProcessTree()
    {
        using var installation = new WorkerInstallation(installHarness: true);
        using var mode = new EnvironmentScope(
            ModeEnvironment,
            "stdout-overflow-tree");
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string pidPath = Path.Combine(directory, "worker-pids.txt");
        using var pidFile = new EnvironmentScope(PidFileEnvironment, pidPath);
        var output = new StringWriter();
        var error = new StringWriter();
        int[] processIds = [];

        try
        {
            Task<int> run = RunInspectAsync(
                    catalogPath,
                    output,
                    error,
                    TestContext.Current.CancellationToken)
                .AsTask();
            processIds = await WaitForProcessIdsAsync(
                pidPath,
                expectedCount: 2,
                TestContext.Current.CancellationToken);

            int exitCode = await run;

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Contains(
                "MIG-MYSQL-CLI-ADAPTER-001",
                error.ToString(),
                StringComparison.Ordinal);
            await WaitForProcessesToExitAsync(
                processIds,
                TestContext.Current.CancellationToken);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        }
        finally
        {
            foreach (int processId in processIds)
                TryKill(processId);
            TryDeleteDirectory(directory);
        }
    }

    private static ValueTask<int> RunInspectAsync(
        string catalogPath,
        TextWriter output,
        TextWriter error,
        CancellationToken cancellationToken) =>
        MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "mysql",
                "--connection-env", ConnectionEnvironment,
                "--out", catalogPath,
            ],
            output,
            error,
            cancellationToken);

    private static async Task<int[]> WaitForProcessIdsAsync(
        string path,
        int expectedCount,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();
            if (File.Exists(path))
            {
                try
                {
                    int[] processIds = (await File.ReadAllLinesAsync(
                            path,
                            timeout.Token))
                        .Where(line => !string.IsNullOrWhiteSpace(line))
                        .Select(line => int.Parse(
                            line,
                            System.Globalization.CultureInfo.InvariantCulture))
                        .Distinct()
                        .ToArray();
                    if (processIds.Length >= expectedCount)
                        return processIds;
                }
                catch (IOException)
                {
                }
            }
            await Task.Delay(25, timeout.Token);
        }
    }

    private static async Task WaitForProcessesToExitAsync(
        IEnumerable<int> processIds,
        CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        foreach (int processId in processIds)
        {
            while (IsRunning(processId))
            {
                timeout.Token.ThrowIfCancellationRequested();
                await Task.Delay(25, timeout.Token);
            }
        }
    }

    private static bool IsRunning(int processId)
    {
        try
        {
            using Process process = Process.GetProcessById(processId);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static void TryKill(int processId)
    {
        try
        {
            using Process process = Process.GetProcessById(processId);
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (Exception exception) when (
            exception is ArgumentException or InvalidOperationException or
                System.ComponentModel.Win32Exception or NotSupportedException)
        {
        }
    }

    private static string FindHarnessDirectory()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate = Path.Combine(
                current.FullName,
                "tests",
                "CSharpDB.Migration.MySql.WorkerHarness",
                "bin",
                BuildConfiguration,
                "net10.0");
            string appHost = Path.Combine(
                candidate,
                OperatingSystem.IsWindows()
                    ? "csharpdb-migration-mysql-worker.exe"
                    : "csharpdb-migration-mysql-worker");
            if (File.Exists(appHost))
                return candidate;
            current = current.Parent;
        }

        throw new FileNotFoundException(
            $"Could not locate the {BuildConfiguration} MySQL worker harness.");
    }

    private static string CreateTempDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_mysql_worker_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.EnumerateFiles(source))
        {
            string target = Path.Combine(destination, Path.GetFileName(file));
            File.Copy(file, target);
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(
                    target,
                    File.GetUnixFileMode(file));
            }
        }
        foreach (string child in Directory.EnumerateDirectories(source))
        {
            CopyDirectory(
                child,
                Path.Combine(destination, Path.GetFileName(child)));
        }
    }

    private static void TryDeleteDirectory(string directory)
    {
        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private sealed class EnvironmentScope : IDisposable
    {
        private readonly string name;
        private readonly string? original;

        internal EnvironmentScope(string name, string? value)
        {
            this.name = name;
            original = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
        }

        public void Dispose() =>
            Environment.SetEnvironmentVariable(name, original);
    }

    private sealed class WorkerInstallation : IDisposable
    {
        private readonly string adapterDirectory;
        private readonly string? backupDirectory;

        internal WorkerInstallation(bool installHarness)
        {
            adapterDirectory = Path.Combine(
                AppContext.BaseDirectory,
                "adapters",
                "mysql");
            if (Directory.Exists(adapterDirectory))
            {
                backupDirectory =
                    adapterDirectory + ".backup-" + Guid.NewGuid().ToString("N");
                Directory.Move(adapterDirectory, backupDirectory);
            }

            try
            {
                if (installHarness)
                {
                    CopyDirectory(
                        FindHarnessDirectory(),
                        adapterDirectory);
                }
            }
            catch
            {
                if (Directory.Exists(adapterDirectory))
                    DeleteDirectoryWithRetry(adapterDirectory);
                RestoreBackup();
                throw;
            }
        }

        public void Dispose()
        {
            if (Directory.Exists(adapterDirectory))
                DeleteDirectoryWithRetry(adapterDirectory);
            RestoreBackup();
        }

        private static void DeleteDirectoryWithRetry(string directory)
        {
            for (int attempt = 0; attempt < 40; attempt++)
            {
                try
                {
                    Directory.Delete(directory, recursive: true);
                    return;
                }
                catch (Exception exception) when (
                    attempt < 39 &&
                    exception is IOException or UnauthorizedAccessException)
                {
                    Thread.Sleep(50);
                }
            }
        }

        private void RestoreBackup()
        {
            if (backupDirectory is not null &&
                Directory.Exists(backupDirectory))
            {
                Directory.Move(backupDirectory, adapterDirectory);
            }
        }
    }
}
