using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class JsonExportCommandRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(
        "json",
        JsonExportFraming.RootArray,
        "[{\"id\":-2,\"note\":null,\"amount\":-2.25},{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}]\n")]
    [InlineData(
        "ndjson",
        JsonExportFraming.Ndjson,
        "{\"id\":-2,\"note\":null,\"amount\":-2.25}\n{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}\n")]
    public async Task FreshExport_WritesExactPairAndTextResult(
        string format,
        JsonExportFraming framing,
        string expected)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "fresh");
        string dataPath =
            workspace.PathFor($"rows.{format}");
        string manifestPath =
            workspace.PathFor($"rows.{format}.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                format,
                "export_rows",
                dataPath,
                manifestPath,
                [
                    "--profile", "lossless-v1",
                    "--max-data-bytes", "1048576",
                    "--max-decoded-blob-bytes", "1024",
                    "--checkpoint-row-interval", "1",
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()));
        byte[] data = await File.ReadAllBytesAsync(
            dataPath,
            Cancellation);
        Assert.Equal(expected, Encoding.UTF8.GetString(data));

        byte[] manifestBytes =
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation);
        JsonExportManifest manifest =
            JsonExportManifestSerializer.Deserialize(
                manifestBytes);
        Assert.Equal(
            manifestBytes,
            JsonExportManifestSerializer.Serialize(
                manifest));
        Assert.Equal(
            JsonExportProfile.LosslessV1,
            manifest.Profile);
        Assert.Equal(framing, manifest.Json.Framing);
        Assert.Equal("export_rows", manifest.Table.Name);
        Assert.Equal(2, manifest.Content.RowCount);
        Assert.Equal(
            data.LongLength,
            manifest.Content.DataByteLength);
        Assert.Equal(
            Sha256(data),
            manifest.Content.DataDigest.Value);
        Assert.Equal(
            receipt.ByteLength,
            manifest.Source.SnapshotByteLength);
        Assert.Equal(
            receipt.Sha256["sha256:".Length..],
            manifest.Source.SnapshotDigest.Value);
        string preparedPath =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*.prepared",
                    SearchOption.TopDirectoryOnly));
        string checkpointPath =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*.checkpoint",
                    SearchOption.TopDirectoryOnly));
        Assert.Equal(
            data,
            await File.ReadAllBytesAsync(
                preparedPath,
                Cancellation));
        Assert.NotEmpty(
            await File.ReadAllBytesAsync(
                checkpointPath,
                Cancellation));
        Assert.Empty(
            Directory.GetFiles(
                workspace.Root,
                ".csharpdb-json-export-*.checkpoint.next",
                SearchOption.TopDirectoryOnly));

        string status = output.ToString();
        Assert.Contains(
            "Status: OK",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            $"format={format}",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            "table=export_rows",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            $"data={dataPath}",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            $"manifest={manifestPath}",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            "dataState=published",
            status,
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=published",
            status,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task NdjsonFormatAndJsonReportFlag_AreUnambiguous()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "report");
        string dataPath =
            workspace.PathFor("rows.ndjson");
        string manifestPath =
            workspace.PathFor("rows.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        string[] arguments =
            ExportArguments(
                receipt,
                "ndjson",
                "export_rows",
                dataPath,
                manifestPath,
                [
                    "--checkpoint-row-interval", "1",
                    "--json",
                ]);
        int code = await MigrationCommandRunner.RunAsync(
            arguments,
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()));
        using JsonDocument result =
            JsonDocument.Parse(output.ToString());
        JsonElement root = result.RootElement;
        Assert.Equal(
            "csharpdb-migration-export-result/v1",
            root.GetProperty("format").GetString());
        Assert.Equal(
            "complete",
            root.GetProperty("status").GetString());
        Assert.Equal(
            "ndjson",
            root.GetProperty("exportFormat").GetString());
        Assert.Equal(
            receipt.SnapshotIdentity,
            root.GetProperty("snapshotIdentity").GetString());
        Assert.Equal(
            "export_rows",
            root.GetProperty("table").GetString());
        Assert.Equal(
            "losslessV1",
            root.GetProperty("profile").GetString());
        Assert.Equal(
            dataPath,
            root.GetProperty("dataPath").GetString());
        Assert.False(
            root.GetProperty("reusedData").GetBoolean());
        Assert.False(
            root.GetProperty("reusedManifest").GetBoolean());

        var retryOutput = new StringWriter();
        var retryError = new StringWriter();
        Assert.Equal(
            InspectorCommandRunner.ExitOk,
            await MigrationCommandRunner.RunAsync(
                arguments,
                retryOutput,
                retryError,
                Cancellation));
        Assert.True(
            string.IsNullOrWhiteSpace(
                retryError.ToString()));
        using JsonDocument retry =
            JsonDocument.Parse(
                retryOutput.ToString());
        Assert.True(
            retry.RootElement
                .GetProperty("reusedData")
                .GetBoolean());
        Assert.True(
            retry.RootElement
                .GetProperty("reusedManifest")
                .GetBoolean());
    }

    [Theory]
    [InlineData("json", "[]\n", 3)]
    [InlineData("ndjson", "", 0)]
    public async Task EmptyExport_WritesFramingSpecificBytes(
        string format,
        string expected,
        long expectedLength)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "empty");
        string dataPath =
            workspace.PathFor($"empty.{format}");
        string manifestPath =
            workspace.PathFor("empty.manifest.json");

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                format,
                "empty_rows",
                dataPath,
                manifestPath),
            new StringWriter(),
            new StringWriter(),
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        byte[] data = await File.ReadAllBytesAsync(
            dataPath,
            Cancellation);
        Assert.Equal(expected, Encoding.UTF8.GetString(data));
        Assert.Equal(expectedLength, data.LongLength);
        JsonExportManifest manifest =
            JsonExportManifestSerializer.Deserialize(
                await File.ReadAllBytesAsync(
                    manifestPath,
                    Cancellation));
        Assert.Equal(0, manifest.Content.RowCount);
        Assert.Equal(
            expectedLength,
            manifest.Content.DataByteLength);
    }

    [Theory]
    [InlineData("json")]
    [InlineData("ndjson")]
    public async Task ExactAndDataOnlyRetries_ReportReuse(
        string format)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "retry");
        string dataPath =
            workspace.PathFor($"retry.{format}");
        string manifestPath =
            workspace.PathFor("retry.manifest.json");
        string[] arguments = ExportArguments(
            receipt,
            format,
            "export_rows",
            dataPath,
            manifestPath,
            [
                "--checkpoint-row-interval",
                "1",
            ]);

        Assert.Equal(
            InspectorCommandRunner.ExitOk,
            await MigrationCommandRunner.RunAsync(
                arguments,
                new StringWriter(),
                new StringWriter(),
                Cancellation));
        byte[] dataBefore = await File.ReadAllBytesAsync(
            dataPath,
            Cancellation);
        byte[] manifestBefore =
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation);
        string preparedPath =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*.prepared",
                    SearchOption.TopDirectoryOnly));
        string checkpointPath =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*.checkpoint",
                    SearchOption.TopDirectoryOnly));
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                preparedPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                checkpointPath,
                Cancellation);

        var exactOutput = new StringWriter();
        Assert.Equal(
            InspectorCommandRunner.ExitOk,
            await MigrationCommandRunner.RunAsync(
                arguments,
                exactOutput,
                new StringWriter(),
                Cancellation));
        Assert.Contains(
            "dataState=reused",
            exactOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=reused",
            exactOutput.ToString(),
            StringComparison.Ordinal);

        File.Delete(manifestPath);
        var recoveryOutput = new StringWriter();
        Assert.Equal(
            InspectorCommandRunner.ExitOk,
            await MigrationCommandRunner.RunAsync(
                arguments,
                recoveryOutput,
                new StringWriter(),
                Cancellation));
        Assert.Contains(
            "dataState=reused",
            recoveryOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=published",
            recoveryOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                dataPath,
                Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                preparedPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                checkpointPath,
                Cancellation));
        Assert.Empty(
            Directory.GetFiles(
                workspace.Root,
                ".csharpdb-json-export-*.checkpoint.next",
                SearchOption.TopDirectoryOnly));
    }

    [Theory]
    [InlineData("json")]
    [InlineData("ndjson")]
    public async Task ResumableCommand_BootstrapsSameBindingRestartOnlyExactPair(
        string format)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"legacy-{format}");
        string dataPath =
            workspace.PathFor(
                $"legacy.{format}");
        string manifestPath =
            workspace.PathFor(
                $"legacy.{format}.manifest.json");
        var request =
            new CSharpDbRetainedJsonExportRequest
            {
                SnapshotPath =
                    receipt.SnapshotPath,
                SnapshotIdentity =
                    receipt.Identity,
                TableName =
                    "export_rows",
                DestinationPath =
                    dataPath,
                Framing =
                    format == "json"
                        ? JsonExportFraming
                            .RootArray
                        : JsonExportFraming
                            .Ndjson,
                MaxDataBytes =
                    1L << 40,
            };
        _ =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);
        byte[] legacyData =
            await File.ReadAllBytesAsync(
                dataPath,
                Cancellation);
        byte[] legacyManifest =
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation);
        Assert.Empty(
            Directory.GetFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));

        var output = new StringWriter();
        var error = new StringWriter();
        Assert.Equal(
            InspectorCommandRunner.ExitOk,
            await MigrationCommandRunner
                .RunAsync(
                    ExportArguments(
                        receipt,
                        format,
                        "export_rows",
                        dataPath,
                        manifestPath,
                        [
                            "--checkpoint-row-interval",
                            "1",
                        ]),
                    output,
                    error,
                    Cancellation));

        Assert.True(
            string.IsNullOrWhiteSpace(
                error.ToString()));
        Assert.Contains(
            "dataState=reused",
            output.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=reused",
            output.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            legacyData,
            await File.ReadAllBytesAsync(
                dataPath,
                Cancellation));
        Assert.Equal(
            legacyManifest,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        _ =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*.prepared",
                    SearchOption.TopDirectoryOnly));
        _ =
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                ".csharpdb-json-export-*.checkpoint",
                SearchOption.TopDirectoryOnly));
    }

    [Theory]
    [InlineData("json")]
    [InlineData("ndjson")]
    public async Task ResumableCommand_RejectsRestartOnlyPairWithDifferentSourceBinding(
        string format)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"legacy-binding-{format}");
        string dataPath =
            workspace.PathFor(
                $"legacy-binding.{format}");
        string manifestPath =
            workspace.PathFor(
                $"legacy-binding.{format}.manifest.json");
        var request =
            new CSharpDbRetainedJsonExportRequest
            {
                SnapshotPath =
                    receipt.SnapshotPath,
                SnapshotIdentity =
                    receipt.Identity,
                TableName =
                    "export_rows",
                DestinationPath =
                    dataPath,
                Framing =
                    format == "json"
                        ? JsonExportFraming
                            .RootArray
                        : JsonExportFraming
                            .Ndjson,
                MaxDataBytes =
                    1L << 40,
            };
        _ =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);

        byte[] legacyData =
            await File.ReadAllBytesAsync(
                dataPath,
                Cancellation);
        JsonExportManifest currentManifest =
            JsonExportManifestSerializer.Deserialize(
                await File.ReadAllBytesAsync(
                    manifestPath,
                    Cancellation));
        JsonExportManifest differentlyBoundManifest =
            currentManifest with
            {
                Source = currentManifest.Source with
                {
                    Version =
                        currentManifest.Source.Version +
                        "-legacy-binding-mismatch",
                },
            };
        byte[] legacyManifest =
            JsonExportManifestSerializer.Serialize(
                differentlyBoundManifest);
        await File.WriteAllBytesAsync(
            manifestPath,
            legacyManifest,
            Cancellation);
        Assert.Empty(
            Directory.GetFiles(
                workspace.Root,
                ".csharpdb-json-export-*",
                SearchOption.TopDirectoryOnly));

        var output = new StringWriter();
        var error = new StringWriter();
        int code =
            await MigrationCommandRunner
                .RunAsync(
                    ExportArguments(
                        receipt,
                        format,
                        "export_rows",
                        dataPath,
                        manifestPath,
                        [
                            "--checkpoint-row-interval",
                            "1",
                        ]),
                    output,
                    error,
                    Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitError,
            code);
        Assert.True(
            string.IsNullOrWhiteSpace(
                output.ToString()));
        Assert.DoesNotContain(
            "reused",
            output.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "manifest destination already contains a different file",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            legacyData,
            await File.ReadAllBytesAsync(
                dataPath,
                Cancellation));
        Assert.Equal(
            legacyManifest,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
    }

    [Theory]
    [InlineData("json")]
    [InlineData("ndjson")]
    public async Task JsonFormats_RejectCsvOnlyProfile(
        string format)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "isolation");
        string dataPath =
            workspace.PathFor($"invalid.{format}");
        string manifestPath =
            workspace.PathFor("invalid.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                format,
                "export_rows",
                dataPath,
                manifestPath,
                [
                    "--profile",
                    "spreadsheet-safe-lossy-v1",
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.False(
            string.IsNullOrWhiteSpace(error.ToString()));
        Assert.False(File.Exists(dataPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-json-export-*",
            SearchOption.TopDirectoryOnly));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("not-a-number")]
    [InlineData("9223372036854775808")]
    public async Task JsonFormats_RejectInvalidCheckpointRowIntervals(
        string value)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                "checkpoint-validation");

        foreach (string format in
                 new[] { "json", "ndjson" })
        {
            string dataPath =
                workspace.PathFor(
                    $"invalid-checkpoint.{format}");
            string manifestPath =
                workspace.PathFor(
                    $"invalid-checkpoint.{format}.manifest.json");
            var output = new StringWriter();
            var error = new StringWriter();

            int code =
                await MigrationCommandRunner.RunAsync(
                    ExportArguments(
                        receipt,
                        format,
                        "export_rows",
                        dataPath,
                        manifestPath,
                        [
                            "--checkpoint-row-interval",
                            value,
                        ]),
                    output,
                    error,
                    Cancellation);

            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                code);
            Assert.True(
                string.IsNullOrWhiteSpace(
                    output.ToString()));
            Assert.Contains(
                "checkpoint row interval",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.False(
                File.Exists(dataPath));
            Assert.False(
                File.Exists(manifestPath));
            Assert.Empty(
                Directory.EnumerateFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*",
                    SearchOption.TopDirectoryOnly));
        }
    }

    [Theory]
    [InlineData("json")]
    [InlineData("ndjson")]
    public async Task JsonFormats_RejectSnapshotInReservedPrivateNamespace(
        string format)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        const string snapshotLeaf =
            ".csharpdb-json-export-retained.prepared";
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"reserved-{format}",
                snapshotLeaf);
        byte[] snapshotBefore =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        string dataPath =
            workspace.PathFor(
                $"reserved.{format}");
        string manifestPath =
            workspace.PathFor(
                $"reserved.{format}.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code =
            await MigrationCommandRunner.RunAsync(
                ExportArguments(
                    receipt,
                    format,
                    "export_rows",
                    dataPath,
                    manifestPath),
                output,
                error,
                Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            code);
        Assert.True(
            string.IsNullOrWhiteSpace(
                output.ToString()));
        Assert.Contains(
            "reserved private namespace",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.False(
            File.Exists(dataPath));
        Assert.False(
            File.Exists(manifestPath));
        Assert.Equal(
            snapshotBefore,
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation));
        Assert.Equal(
            receipt.SnapshotPath,
            Assert.Single(
                Directory.GetFiles(
                    workspace.Root,
                    ".csharpdb-json-export-*",
                    SearchOption.TopDirectoryOnly)));
    }

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateSnapshotAsync(
        TemporaryDirectory workspace,
        string name,
        string? snapshotLeaf = null)
    {
        string sourcePath =
            workspace.PathFor($"{name}-source.db");
        await using (Database database =
                     await Database.OpenAsync(
                         sourcePath,
                         Cancellation))
        {
            await database.ExecuteAsync(
                """
                CREATE TABLE export_rows (
                    id INTEGER PRIMARY KEY,
                    note TEXT,
                    amount REAL NOT NULL
                )
                """,
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO export_rows VALUES (3, 'a,\"b\"', 3.5)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO export_rows VALUES (-2, NULL, -2.25)",
                Cancellation);
            await database.ExecuteAsync(
                """
                CREATE TABLE empty_rows (
                    id INTEGER PRIMARY KEY
                )
                """,
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            workspace.PathFor(
                snapshotLeaf ??
                $"{name}-snapshot.db"),
            databaseOptions: null,
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = workspace.CreateDirectory(
                    $"{name}-capture-workspace"),
            },
            Cancellation);
    }

    private static string[] ExportArguments(
        RetainedDatabaseSnapshotReceipt receipt,
        string format,
        string tableName,
        string dataPath,
        string manifestPath,
        IReadOnlyList<string>? suffix = null)
    {
        var arguments = new List<string>
        {
            "migrate",
            "export",
            receipt.SnapshotPath,
            "--format",
            format,
            "--table",
            tableName,
            "--out",
            dataPath,
            "--manifest",
            manifestPath,
            "--expected-snapshot-identity",
            receipt.SnapshotIdentity,
        };
        if (suffix is not null)
            arguments.AddRange(suffix);
        return [.. arguments];
    }

    private static string Sha256(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes))
            .ToLowerInvariant();

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-export-cli-tests",
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
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
        }
    }
}
