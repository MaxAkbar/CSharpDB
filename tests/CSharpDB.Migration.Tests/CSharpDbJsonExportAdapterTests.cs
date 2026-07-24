using System.Security.Cryptography;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbJsonExportAdapterTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(
        JsonExportFraming.RootArray,
        "[{\"id\":-2,\"note\":null,\"amount\":-2.25},{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}]\n")]
    [InlineData(
        JsonExportFraming.Ndjson,
        "{\"id\":-2,\"note\":null,\"amount\":-2.25}\n{\"id\":3,\"note\":\"a,\\\"b\\\"\",\"amount\":3.5}\n")]
    public async Task FreshPublication_BindsSnapshotSchemaAndPhysicalRows(
        JsonExportFraming framing,
        string expected)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "fresh");
        string extension = framing == JsonExportFraming.RootArray
            ? "json"
            : "ndjson";
        string destinationPath =
            workspace.PathFor($"rows.{extension}");
        string manifestPath =
            workspace.PathFor($"rows.{extension}.manifest.json");

        JsonExportPublicationResult result =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        "export_rows",
                        framing),
                    manifestPath,
                    Cancellation);

        byte[] data =
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation);
        Assert.Equal(expected, Encoding.UTF8.GetString(data));
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Equal(
            result.CanonicalManifestBytes,
            JsonExportManifestSerializer.Serialize(
                result.Manifest));
        Assert.Equal(
            result.ManifestDigest,
            JsonExportManifestSerializer
                .ComputeManifestDigest(result.Manifest));
        Assert.Equal(
            framing,
            result.Manifest.Json.Framing);
        Assert.Equal(
            "export_rows",
            result.Manifest.Table.Name);
        Assert.Equal(
            ["id", "note", "amount"],
            result.Manifest.Table.Columns.Select(
                static column => column.SourceName));
        Assert.Equal(
            [
                JsonExportDatabaseType.Integer,
                JsonExportDatabaseType.Text,
                JsonExportDatabaseType.Real,
            ],
            result.Manifest.Table.Columns.Select(
                static column => column.DatabaseType));
        Assert.Equal(2, result.Manifest.Content.RowCount);
        Assert.Equal(
            data.LongLength,
            result.Manifest.Content.DataByteLength);
        Assert.Equal(
            Sha256(data),
            result.Manifest.Content.DataDigest.Value);
        Assert.Equal(
            receipt.ByteLength,
            result.Manifest.Source.SnapshotByteLength);
        Assert.Equal(
            receipt.Sha256["sha256:".Length..],
            result.Manifest.Source.SnapshotDigest.Value);
        Assert.Equal(
            result.Manifest.Content.SourceLogicalDigest,
            result.Manifest.Content.ExportedLogicalDigest);
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.False(
            File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task ExactAndDataOnlyReruns_RequalifyAndRecover(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "retry");
        string destinationPath =
            workspace.PathFor("retry.data");
        string manifestPath =
            workspace.PathFor("retry.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                framing);
        var adapter = new CSharpDbJsonExportAdapter();

        JsonExportPublicationResult first =
            await adapter.WriteAndPublishTableAsync(
                request,
                manifestPath,
                Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            destinationPath,
            Cancellation);
        byte[] manifestBefore = await File.ReadAllBytesAsync(
            manifestPath,
            Cancellation);

        JsonExportPublicationResult exact =
            await adapter.WriteAndPublishTableAsync(
                request,
                manifestPath,
                Cancellation);

        Assert.True(exact.ReusedData);
        Assert.True(exact.ReusedManifest);
        Assert.Equal(first.ManifestDigest, exact.ManifestDigest);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));

        File.Delete(manifestPath);
        JsonExportPublicationResult recovered =
            await new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
    }

    [Fact]
    public async Task ExactPairRerun_RequalifiesTamperedSnapshotBeforeReuse()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "requalify");
        string destinationPath =
            workspace.PathFor("requalify.json");
        string manifestPath =
            workspace.PathFor("requalify.manifest.json");
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray);
        var adapter = new CSharpDbJsonExportAdapter();

        await adapter.WriteAndPublishTableAsync(
            request,
            manifestPath,
            Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            destinationPath,
            Cancellation);
        byte[] manifestBefore =
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation);
        byte[] tamperedSnapshot =
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation);
        tamperedSnapshot[tamperedSnapshot.Length / 2] ^= 0x80;
        await File.WriteAllBytesAsync(
            receipt.SnapshotPath,
            tamperedSnapshot,
            Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            () => adapter.WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                destinationPath,
                Cancellation));
        Assert.Equal(
            manifestBefore,
            await File.ReadAllBytesAsync(
                manifestPath,
                Cancellation));
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-json-export-*",
            SearchOption.TopDirectoryOnly));
    }

    [Fact]
    public async Task WrongSnapshotIdentity_FailsBeforePublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "identity");
        string destinationPath =
            workspace.PathFor("identity.json");
        string manifestPath =
            workspace.PathFor("identity.manifest.json");
        string wrongSha256 =
            DifferentCanonicalIdentity(receipt.Sha256);
        RetainedDatabaseSnapshotIdentity wrongIdentity = new(
            receipt.ByteLength,
            wrongSha256,
            DifferentCanonicalIdentity(
                receipt.SnapshotIdentity));
        CSharpDbRetainedJsonExportRequest request =
            Request(
                receipt,
                destinationPath,
                "export_rows",
                JsonExportFraming.RootArray) with
            {
                SnapshotIdentity = wrongIdentity,
            };

        await Assert.ThrowsAnyAsync<IOException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    request,
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
    }

    [Fact]
    public async Task MissingTableAndPreCancellation_CreateNoPublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(workspace, "failures");
        string destinationPath =
            workspace.PathFor("failure.json");
        string manifestPath =
            workspace.PathFor("failure.manifest.json");
        CSharpDbRetainedJsonExportRequest missing =
            Request(
                receipt,
                destinationPath,
                "missing_table",
                JsonExportFraming.RootArray);

        await Assert.ThrowsAnyAsync<Exception>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    missing,
                    manifestPath,
                    Cancellation)
                .AsTask());
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        CSharpDbRetainedJsonExportRequest canceled =
            missing with { TableName = "export_rows" };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    canceled,
                    manifestPath,
                    cancellation.Token)
                .AsTask());
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
    }

    [Theory]
    [InlineData("export_view")]
    [InlineData("sys.tables")]
    [InlineData("missing_rows")]
    public async Task InvalidPhysicalSource_CreatesNoPublicationAuthority(
        string tableName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"invalid-{tableName.Replace('.', '-')}");
        string destinationPath =
            workspace.PathFor("invalid.json");
        string manifestPath =
            workspace.PathFor("invalid.manifest.json");

        Exception? error = await Record.ExceptionAsync(
            () => new CSharpDbJsonExportAdapter()
                .WriteAndPublishTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        tableName,
                        JsonExportFraming.RootArray),
                    manifestPath,
                    Cancellation)
                .AsTask());

        Assert.NotNull(error);
        if (tableName == "missing_rows")
        {
            CSharpDbException missing =
                Assert.IsType<CSharpDbException>(error);
            Assert.Equal(ErrorCode.TableNotFound, missing.Code);
        }
        else
        {
            Assert.IsType<InvalidOperationException>(error);
        }
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-json-export-*",
            SearchOption.TopDirectoryOnly));
        Assert.False(
            File.Exists(receipt.SnapshotPath + ".wal"));
    }

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateSnapshotAsync(
            TemporaryDirectory workspace,
            string name)
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
                "CREATE VIEW export_view AS SELECT id, note, amount FROM export_rows",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            workspace.PathFor($"{name}-snapshot.db"),
            databaseOptions: null,
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = workspace.CreateDirectory(
                    $"{name}-capture-workspace"),
            },
            Cancellation);
    }

    private static CSharpDbRetainedJsonExportRequest Request(
        RetainedDatabaseSnapshotReceipt receipt,
        string destinationPath,
        string tableName,
        JsonExportFraming framing) => new()
        {
            SnapshotPath = receipt.SnapshotPath,
            SnapshotIdentity = receipt.Identity,
            TableName = tableName,
            DestinationPath = destinationPath,
            Framing = framing,
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes = 1_024,
        };

    private static string DifferentCanonicalIdentity(
        string identity)
    {
        char replacement = identity[^1] == '0' ? '1' : '0';
        return identity[..^1] + replacement;
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
                "csharpdb-json-export-adapter-tests",
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
