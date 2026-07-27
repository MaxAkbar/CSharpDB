using System.Text;
using System.Text.Json;
using CSharpDB.Engine;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class RetainedSnapshotCommandRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task CaptureJsonResult_PublishesVerifiedSnapshotAndIdentity()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.csdb");
        string snapshotPath = workspace.PathFor("source.export-snapshot.db");
        await CreateSourceAsync(sourcePath);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "snapshot", sourcePath,
                "--out", snapshotPath,
                "--offline",
                "--workspace", workspace.CreateDirectory("capture-workspace"),
                "--max-database-bytes", "10485760",
                "--max-wal-bytes", "10485760",
                "--max-snapshot-bytes", "10485760",
                "--json",
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        Assert.True(File.Exists(snapshotPath));
        Assert.False(File.Exists(snapshotPath + ".wal"));

        using JsonDocument result = JsonDocument.Parse(output.ToString());
        JsonElement root = result.RootElement;
        Assert.Equal(
            "csharpdb-migration-snapshot-result/v1",
            root.GetProperty("format").GetString());
        Assert.Equal("complete", root.GetProperty("status").GetString());
        Assert.Equal(sourcePath, root.GetProperty("sourcePath").GetString());
        Assert.Equal(snapshotPath, root.GetProperty("snapshotPath").GetString());
        Assert.Equal(
            "offline-confirmed",
            root.GetProperty("sourceState").GetString());
        Assert.Equal(
            "published",
            root.GetProperty("publicationState").GetString());

        long byteLength = root.GetProperty("byteLength").GetInt64();
        string sha256 = Assert.IsType<string>(
            root.GetProperty("sha256").GetString());
        string snapshotIdentity = Assert.IsType<string>(
            root.GetProperty("snapshotIdentity").GetString());
        Assert.Equal(new FileInfo(snapshotPath).Length, byteLength);
        Assert.StartsWith("sha256:", sha256, StringComparison.Ordinal);
        Assert.Equal(
            $"csharpdb-retained-snapshot/v1:{byteLength}:{sha256}",
            snapshotIdentity);

        var identity = new RetainedDatabaseSnapshotIdentity(
            byteLength,
            sha256,
            snapshotIdentity);
        await using RetainedDatabaseSnapshotSession session =
            await RetainedDatabaseSnapshot.OpenAsync(
                snapshotPath,
                identity,
                databaseOptions: null,
                new RetainedDatabaseSnapshotOptions
                {
                    WorkspacePath =
                        workspace.CreateDirectory("open-workspace"),
                },
                Cancellation);
        await using var rows = await session.ExecuteReadAsync(
            "SELECT id, value FROM export_rows",
            Cancellation);
        var values = await rows.ToListAsync(Cancellation);
        Assert.Single(values);
        Assert.Equal(7, values[0][0].AsInteger);
        Assert.Equal("ready", values[0][1].AsText);
    }

    [Fact]
    public async Task CapturedIdentity_IsAcceptedDirectlyByCsvExport()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.csdb");
        string snapshotPath = workspace.PathFor("source.export-snapshot.db");
        await CreateSourceAsync(sourcePath);
        var captureOutput = new StringWriter();
        var captureError = new StringWriter();

        int captureCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "snapshot", sourcePath,
                "--out", snapshotPath,
                "--offline",
                "--workspace", workspace.CreateDirectory("capture-workspace"),
                "--json",
            ],
            captureOutput,
            captureError,
            Cancellation);

        using JsonDocument capture = JsonDocument.Parse(
            captureOutput.ToString());
        string snapshotIdentity = Assert.IsType<string>(
            capture.RootElement
                .GetProperty("snapshotIdentity")
                .GetString());
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        var exportOutput = new StringWriter();
        var exportError = new StringWriter();

        int exportCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "export", snapshotPath,
                "--format", "csv",
                "--table", "export_rows",
                "--out", dataPath,
                "--manifest", manifestPath,
                "--expected-snapshot-identity", snapshotIdentity,
            ],
            exportOutput,
            exportError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, captureCode);
        Assert.True(string.IsNullOrWhiteSpace(captureError.ToString()));
        Assert.Equal(InspectorCommandRunner.ExitOk, exportCode);
        Assert.True(string.IsNullOrWhiteSpace(exportError.ToString()));
        Assert.Equal(
            "id,value\r\n7,ready\r\n",
            Encoding.UTF8.GetString(
                await File.ReadAllBytesAsync(dataPath, Cancellation)));
        Assert.True(File.Exists(manifestPath));
    }

    [Fact]
    public async Task MissingOfflineConfirmation_FailsWithoutPublishing()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.csdb");
        string snapshotPath = workspace.PathFor("snapshot.db");
        await CreateSourceAsync(sourcePath);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "snapshot", sourcePath,
                "--out", snapshotPath,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "Close every writer",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(snapshotPath));
    }

    [Fact]
    public async Task ExistingDestination_IsNotOverwritten()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.csdb");
        string snapshotPath = workspace.PathFor("snapshot.db");
        await CreateSourceAsync(sourcePath);
        byte[] existing = [0x43, 0x53, 0x44, 0x42];
        await File.WriteAllBytesAsync(
            snapshotPath,
            existing,
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "snapshot", sourcePath,
                "--out", snapshotPath,
                "--offline",
                "--workspace", workspace.CreateDirectory("capture-workspace"),
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "already exists",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            existing,
            await File.ReadAllBytesAsync(snapshotPath, Cancellation));
    }

    [Theory]
    [InlineData("--max-database-bytes")]
    [InlineData("--max-wal-bytes")]
    [InlineData("--max-snapshot-bytes")]
    public async Task InvalidLimits_FailBeforePublishing(string option)
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.csdb");
        string snapshotPath = workspace.PathFor("snapshot.db");
        await CreateSourceAsync(sourcePath);

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "snapshot", sourcePath,
                "--out", snapshotPath,
                "--offline",
                option, "0",
            ],
            new StringWriter(),
            new StringWriter(),
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.False(File.Exists(snapshotPath));
    }

    [Fact]
    public void ExportPublicationCapability_ExplainsUnqualifiedUnixBoundary()
    {
        bool supported =
            MigrationCommandRunner.TryGetExportPublicationCapability(
                isWindows: false,
                out string diagnostic);

        Assert.False(supported);
        Assert.Contains(
            "requires Windows",
            diagnostic,
            StringComparison.Ordinal);
        Assert.Contains(
            "handle-bound parent-directory validation",
            diagnostic,
            StringComparison.Ordinal);
        Assert.Contains(
            "Unix substrate has not been qualified",
            diagnostic,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ExportPublicationCapability_AcceptsQualifiedWindowsBoundary()
    {
        bool supported =
            MigrationCommandRunner.TryGetExportPublicationCapability(
                isWindows: true,
                out string diagnostic);

        Assert.True(supported);
        Assert.Equal(string.Empty, diagnostic);
    }

    private static async Task CreateSourceAsync(string sourcePath)
    {
        await using Database database =
            await Database.OpenAsync(sourcePath, Cancellation);
        await database.ExecuteAsync(
            "CREATE TABLE export_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            Cancellation);
        await database.ExecuteAsync(
            "INSERT INTO export_rows VALUES (7, 'ready')",
            Cancellation);
        await database.CheckpointAsync(Cancellation);
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-retained-snapshot-cli-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) =>
            Path.Combine(Root, leaf);

        public string CreateDirectory(string leaf)
        {
            string path = PathFor(leaf);
            Directory.CreateDirectory(path);
            return path;
        }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
