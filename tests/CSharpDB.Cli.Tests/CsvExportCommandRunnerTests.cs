using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class CsvExportCommandRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FreshExport_WritesExactPairAndTextResult()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "fresh");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                "export_rows",
                dataPath,
                manifestPath,
                [
                    "--max-data-bytes", "1048576",
                    "--max-decoded-blob-bytes", "1024",
                    "--checkpoint-row-interval", "1",
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        const string expected =
            "id,note,amount\r\n" +
            "-2,\\N,-2.25\r\n" +
            "3,\"a,\"\"b\"\"\",3.5\r\n";
        byte[] data = await File.ReadAllBytesAsync(dataPath, Cancellation);
        Assert.Equal(expected, Encoding.UTF8.GetString(data));

        byte[] manifestBytes =
            await File.ReadAllBytesAsync(manifestPath, Cancellation);
        CsvExportManifest manifest =
            CsvExportManifestSerializer.Deserialize(manifestBytes);
        Assert.Equal(manifestBytes, CsvExportManifestSerializer.Serialize(manifest));
        Assert.Equal(CsvExportProfile.LosslessV1, manifest.Profile);
        Assert.Equal("export_rows", manifest.Table.Name);
        Assert.Equal(2, manifest.Content.RowCount);
        Assert.Equal(data.LongLength, manifest.Content.DataByteLength);
        Assert.Equal(
            Sha256(data),
            manifest.Content.DataDigest.Value);
        Assert.Equal(receipt.ByteLength, manifest.Source.SnapshotByteLength);
        Assert.Equal(
            receipt.Sha256["sha256:".Length..],
            manifest.Source.SnapshotDigest.Value);

        string status = output.ToString();
        Assert.Contains("Status: OK", status, StringComparison.Ordinal);
        Assert.Contains("format=csv", status, StringComparison.Ordinal);
        Assert.Contains("table=export_rows", status, StringComparison.Ordinal);
        Assert.Contains($"csv={dataPath}", status, StringComparison.Ordinal);
        Assert.Contains($"manifest={manifestPath}", status, StringComparison.Ordinal);
        Assert.Contains(
            $"manifestDigest={CsvExportManifestSerializer.ComputeManifestDigest(manifest)}",
            status,
            StringComparison.Ordinal);
        Assert.Contains("rows=2", status, StringComparison.Ordinal);
        Assert.Contains($"bytes={data.LongLength}", status, StringComparison.Ordinal);
        Assert.Contains("dataState=published", status, StringComparison.Ordinal);
        Assert.Contains("manifestState=published", status, StringComparison.Ordinal);
    }

    [Fact]
    public async Task JsonResult_ReportsBoundPublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "json");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                "export_rows",
                dataPath,
                manifestPath,
                ["--json"]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        CsvExportManifest manifest = CsvExportManifestSerializer.Deserialize(
            await File.ReadAllBytesAsync(manifestPath, Cancellation));
        using JsonDocument result = JsonDocument.Parse(output.ToString());
        JsonElement root = result.RootElement;
        Assert.Equal(
            "csharpdb-migration-export-result/v1",
            root.GetProperty("format").GetString());
        Assert.Equal("complete", root.GetProperty("status").GetString());
        Assert.Equal("csv", root.GetProperty("exportFormat").GetString());
        Assert.Equal(
            receipt.SnapshotIdentity,
            root.GetProperty("snapshotIdentity").GetString());
        Assert.Equal("export_rows", root.GetProperty("table").GetString());
        Assert.Equal("losslessV1", root.GetProperty("profile").GetString());
        Assert.Equal(dataPath, root.GetProperty("dataPath").GetString());
        Assert.Equal(manifestPath, root.GetProperty("manifestPath").GetString());
        Assert.Equal(
            CsvExportManifestSerializer.ComputeManifestDigest(manifest),
            root.GetProperty("manifestDigest").GetString());
        Assert.Equal(2, root.GetProperty("rowCount").GetInt64());
        Assert.Equal(
            new FileInfo(dataPath).Length,
            root.GetProperty("dataByteLength").GetInt64());
        JsonElement digest = root.GetProperty("dataDigest");
        Assert.Equal("sha256", digest.GetProperty("algorithm").GetString());
        Assert.Equal(
            manifest.Content.DataDigest.Value,
            digest.GetProperty("value").GetString());
        Assert.False(root.GetProperty("reusedData").GetBoolean());
        Assert.False(root.GetProperty("reusedManifest").GetBoolean());
    }

    [Fact]
    public async Task ExactRetry_ReusesBothFinalArtifacts()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "retry");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        string[] arguments = ExportArguments(
            receipt,
            "export_rows",
            dataPath,
            manifestPath);

        int firstCode = await MigrationCommandRunner.RunAsync(
            arguments,
            new StringWriter(),
            new StringWriter(),
            Cancellation);
        byte[] dataBefore =
            await File.ReadAllBytesAsync(dataPath, Cancellation);
        byte[] manifestBefore =
            await File.ReadAllBytesAsync(manifestPath, Cancellation);
        var retryOutput = new StringWriter();
        var retryError = new StringWriter();

        int retryCode = await MigrationCommandRunner.RunAsync(
            arguments,
            retryOutput,
            retryError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, firstCode);
        Assert.Equal(InspectorCommandRunner.ExitOk, retryCode);
        Assert.True(string.IsNullOrWhiteSpace(retryError.ToString()));
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(dataPath, Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(manifestPath, Cancellation));
        Assert.Contains(
            "dataState=reused",
            retryOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=reused",
            retryOutput.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task CsvOnlyRetry_ReusesDataAndRepublishesManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "recover");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        string[] arguments = ExportArguments(
            receipt,
            "export_rows",
            dataPath,
            manifestPath);

        int firstCode = await MigrationCommandRunner.RunAsync(
            arguments,
            new StringWriter(),
            new StringWriter(),
            Cancellation);
        byte[] dataBefore =
            await File.ReadAllBytesAsync(dataPath, Cancellation);
        byte[] manifestBefore =
            await File.ReadAllBytesAsync(manifestPath, Cancellation);
        File.Delete(manifestPath);
        var retryOutput = new StringWriter();
        var retryError = new StringWriter();

        int retryCode = await MigrationCommandRunner.RunAsync(
            arguments,
            retryOutput,
            retryError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, firstCode);
        Assert.Equal(InspectorCommandRunner.ExitOk, retryCode);
        Assert.True(string.IsNullOrWhiteSpace(retryError.ToString()));
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(dataPath, Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(manifestPath, Cancellation));
        Assert.Contains(
            "dataState=reused",
            retryOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "manifestState=published",
            retryOutput.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task MalformedArguments_ReturnUsageWithoutCreatingExportAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "arguments");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        string[] valid = ExportArguments(
            receipt,
            "export_rows",
            dataPath,
            manifestPath);
        string[] dosAliasSnapshot = [.. valid];
        dosAliasSnapshot[2] = workspace.PathFor("SNAPSH~1.DB");
        string[][] invalidCases =
        [
            RemoveOption(valid, "--expected-snapshot-identity"),
            ReplaceOptionValue(
                valid,
                "--expected-snapshot-identity",
                receipt.SnapshotIdentity.ToUpperInvariant()),
            ReplaceOptionValue(valid, "--format", "yaml"),
            [.. valid, "--profile", "lossy"],
            [.. valid, "--max-data-bytes", "0"],
            [
                .. valid,
                "--max-decoded-blob-bytes",
                (CsvExportContracts.MaximumSupportedDecodedBlobBytes + 1)
                    .ToString(),
            ],
            [.. valid, "--checkpoint-row-interval", "0"],
            dosAliasSnapshot,
            [.. valid, "--table", "export_rows"],
            [.. valid, "--bogus", "value"],
        ];

        foreach (string[] invalid in invalidCases)
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                invalid,
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.False(string.IsNullOrWhiteSpace(error.ToString()));
            AssertNoExportAuthority(workspace.Root, dataPath, manifestPath);
        }
    }

    [Fact]
    public async Task WrongCanonicalIdentity_FailsBeforeCreatingExportAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "identity");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        string wrongIdentity = DifferentCanonicalIdentity(
            receipt.SnapshotIdentity);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                "export_rows",
                dataPath,
                manifestPath,
                expectedIdentity: wrongIdentity),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.False(string.IsNullOrWhiteSpace(error.ToString()));
        AssertNoExportAuthority(workspace.Root, dataPath, manifestPath);
    }

    [Fact]
    public async Task PathAliasesReturnUsageWithoutChangingSnapshot()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "aliases");
        byte[] snapshotBefore =
            await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation);
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        string[][] invalidCases =
        [
            ExportArguments(
                receipt,
                "export_rows",
                receipt.SnapshotPath,
                manifestPath),
            ExportArguments(
                receipt,
                "export_rows",
                dataPath,
                dataPath),
        ];

        foreach (string[] invalid in invalidCases)
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                invalid,
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "must use different files",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                snapshotBefore,
                await File.ReadAllBytesAsync(
                    receipt.SnapshotPath,
                    Cancellation));
            Assert.False(File.Exists(manifestPath));
            Assert.Empty(Directory.EnumerateFiles(
                workspace.Root,
                ".csharpdb-csv-export-*",
                SearchOption.TopDirectoryOnly));
        }
    }

    [Fact]
    public async Task PreCancellationPropagatesWithoutCreatingExportAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "cancel");
        string dataPath = workspace.PathFor("rows.csv");
        string manifestPath = workspace.PathFor("rows.manifest.json");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await MigrationCommandRunner.RunAsync(
                ExportArguments(
                    receipt,
                    "export_rows",
                    dataPath,
                    manifestPath),
                new StringWriter(),
                new StringWriter(),
                cancellation.Token));

        AssertNoExportAuthority(workspace.Root, dataPath, manifestPath);
    }

    [Fact]
    public async Task SpreadsheetSafeProfile_RecordsFormulaTransform()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "lossy");
        string dataPath = workspace.PathFor("formula.csv");
        string manifestPath = workspace.PathFor("formula.manifest.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ExportArguments(
                receipt,
                "formula_rows",
                dataPath,
                manifestPath,
                ["--profile", "spreadsheet-safe-lossy-v1"]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        Assert.Equal(
            "id,note\r\n" +
            "1,'=1+1\r\n" +
            "2,ordinary\r\n",
            await File.ReadAllTextAsync(dataPath, Cancellation));
        CsvExportManifest manifest = CsvExportManifestSerializer.Deserialize(
            await File.ReadAllBytesAsync(manifestPath, Cancellation));
        Assert.Equal(CsvExportProfile.SpreadsheetSafeLossyV1, manifest.Profile);
        CsvExportLossyTransformManifest loss =
            Assert.IsType<CsvExportLossyTransformManifest>(
                manifest.LossyTransform);
        Assert.Equal(CsvExportContracts.SpreadsheetFormulaRuleId, loss.RuleId);
        Assert.Equal(0, loss.TransformedHeaderCount);
        Assert.Equal(1, loss.TransformedRowCount);
        Assert.Equal(1, loss.TransformedCellCount);
        Assert.NotEqual(
            manifest.Content.SourceLogicalDigest,
            manifest.Content.ExportedLogicalDigest);
    }

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateSnapshotAsync(
            TemporaryDirectory workspace,
            string name)
    {
        string sourcePath = workspace.PathFor($"{name}-source.db");
        await using (Database database = await Database.OpenAsync(
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
                CREATE TABLE formula_rows (
                    id INTEGER PRIMARY KEY,
                    note TEXT NOT NULL
                )
                """,
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO formula_rows VALUES (2, 'ordinary')",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO formula_rows VALUES (1, '=1+1')",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            workspace.PathFor($"{name}-snapshot.db"),
            databaseOptions: null,
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath =
                    workspace.CreateDirectory($"{name}-capture-workspace"),
            },
            Cancellation);
    }

    private static string[] ExportArguments(
        RetainedDatabaseSnapshotReceipt receipt,
        string tableName,
        string dataPath,
        string manifestPath,
        IReadOnlyList<string>? suffix = null,
        string? expectedIdentity = null)
    {
        var arguments = new List<string>
        {
            "migrate", "export", receipt.SnapshotPath,
            "--format", "csv",
            "--table", tableName,
            "--out", dataPath,
            "--manifest", manifestPath,
            "--expected-snapshot-identity",
            expectedIdentity ?? receipt.SnapshotIdentity,
        };
        if (suffix is not null)
            arguments.AddRange(suffix);
        return [.. arguments];
    }

    private static string[] RemoveOption(
        IReadOnlyList<string> arguments,
        string option)
    {
        int index = FindOption(arguments, option);
        return
        [
            .. arguments.Take(index),
            .. arguments.Skip(index + 2),
        ];
    }

    private static string[] ReplaceOptionValue(
        IReadOnlyList<string> arguments,
        string option,
        string value)
    {
        string[] replaced = [.. arguments];
        replaced[FindOption(arguments, option) + 1] = value;
        return replaced;
    }

    private static int FindOption(
        IReadOnlyList<string> arguments,
        string option)
    {
        for (int index = 0; index < arguments.Count; index++)
        {
            if (string.Equals(
                    arguments[index],
                    option,
                    StringComparison.Ordinal))
            {
                return index;
            }
        }

        throw new InvalidOperationException($"Option '{option}' was not found.");
    }

    private static string DifferentCanonicalIdentity(string identity)
    {
        char replacement = identity[^1] == '0' ? '1' : '0';
        return identity[..^1] + replacement;
    }

    private static string Sha256(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private static void AssertNoExportAuthority(
        string root,
        string dataPath,
        string manifestPath)
    {
        Assert.False(File.Exists(dataPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(Directory.EnumerateFiles(
            root,
            ".csharpdb-csv-export-*",
            SearchOption.TopDirectoryOnly));
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-csv-export-cli-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) => Path.Combine(Root, leaf);

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
