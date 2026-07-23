using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class CSharpDbCsvExportAdapterTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FreshExport_BindsVerifiedIdentitySchemaAndTypedPhysicalRows()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateTypedSnapshotAsync(workspace, "fresh");
        string destinationPath = workspace.PathFor("typed.csv");
        CSharpDbRetainedCsvExportRequest request = Request(
            receipt,
            destinationPath,
            "typed_rows",
            checkpointRowInterval: 2);

        CsvStreamingExportResult result =
            await new CSharpDbCsvExportAdapter()
                .WriteResumableTableAsync(request, Cancellation);
        PreparedArtifacts artifacts = FindPreparedArtifacts(workspace.Root);
        byte[] data = await File.ReadAllBytesAsync(
            artifacts.DataPath,
            Cancellation);
        const string expected =
            "id,note,score,payload\r\n" +
            "-9,\\N,-9.5,AAEC\r\n" +
            "-2,,-2.25,AA==\r\n" +
            "4,\"a,\"\"b\"\"\",4.25,\\N\r\n" +
            "11,\"\\N\",11.5,/w==\r\n";

        Assert.Equal(expected, Encoding.UTF8.GetString(data));
        Assert.Equal(4, result.Manifest.Content.RowCount);
        Assert.Equal(data.LongLength, result.Manifest.Content.DataByteLength);
        Assert.Equal(CsvExportContracts.SourceKind, result.Manifest.Source.Kind);
        Assert.False(string.IsNullOrWhiteSpace(result.Manifest.Source.Version));
        Assert.Equal(
            receipt.ByteLength,
            result.Manifest.Source.SnapshotByteLength);
        Assert.Equal(
            CsvExportHashManifest.Sha256Algorithm,
            result.Manifest.Source.SnapshotDigest.Algorithm);
        Assert.Equal(
            receipt.Sha256["sha256:".Length..],
            result.Manifest.Source.SnapshotDigest.Value);
        Assert.Equal("typed_rows", result.Manifest.Table.Name);
        Assert.Equal(
            ["id", "note", "score", "payload"],
            result.Manifest.Table.Columns.Select(
                static column => column.SourceName));
        Assert.Equal(
            [
                CsvExportDatabaseType.Integer,
                CsvExportDatabaseType.Text,
                CsvExportDatabaseType.Real,
                CsvExportDatabaseType.Blob,
            ],
            result.Manifest.Table.Columns.Select(
                static column => column.DatabaseType));
        Assert.Equal(
            [false, true, false, true],
            result.Manifest.Table.Columns.Select(
                static column => column.Nullable));
        Assert.Equal(
            [0, 1, 2, 3],
            result.Manifest.Table.Columns.Select(
                static column => column.Ordinal));

        CsvExportCheckpoint checkpoint = ReadCheckpoint(
            artifacts.CheckpointPath);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            checkpoint.Phase);
        Assert.Equal(3, checkpoint.Generation);
        Assert.Equal(
            receipt.SnapshotIdentity,
            checkpoint.Binding.SourceSnapshotIdentity);
        Assert.Equal(
            result.Manifest.Source,
            checkpoint.Binding.Source);
        Assert.Equal(
            result.ManifestDigest,
            checkpoint.Completion!.ManifestDigest);
        Assert.False(File.Exists(artifacts.PendingCheckpointPath));
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Fact]
    public async Task DataCompleteReopen_ReturnsIdenticalResultWithoutChangingAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateTypedSnapshotAsync(workspace, "reopen");
        string destinationPath = workspace.PathFor("reopen.csv");
        CSharpDbRetainedCsvExportRequest firstRequest = Request(
            receipt,
            destinationPath,
            "typed_rows",
            checkpointRowInterval: 2);
        var adapter = new CSharpDbCsvExportAdapter();

        CsvStreamingExportResult first =
            await adapter.WriteResumableTableAsync(
                firstRequest,
                Cancellation);
        PreparedArtifacts artifacts = FindPreparedArtifacts(workspace.Root);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            artifacts.DataPath,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            artifacts.CheckpointPath,
            Cancellation);
        CsvExportCheckpoint completed = CsvExportCheckpointSerializer
            .Deserialize(checkpointBefore);

        CsvStreamingExportResult reopened =
            await new CSharpDbCsvExportAdapter()
                .WriteResumableTableAsync(
                    firstRequest,
                    Cancellation);

        Assert.Equal(
            first.CanonicalManifestBytes,
            reopened.CanonicalManifestBytes);
        Assert.Equal(first.ManifestDigest, reopened.ManifestDigest);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        Assert.Equal(
            completed.Generation,
            ReadCheckpoint(artifacts.CheckpointPath).Generation);
        Assert.False(File.Exists(artifacts.PendingCheckpointPath));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task InterruptedGenericWriter_FreshAdapterResumesVerifiedSnapshotExactly()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateTypedSnapshotAsync(workspace, "resume");
        string destinationPath = workspace.PathFor("resume.csv");
        await using (RetainedDatabaseSnapshotSession session =
                     await RetainedDatabaseSnapshot.OpenAsync(
                         receipt.SnapshotPath,
                         receipt.Identity,
                         databaseOptions: null,
                         SnapshotOptions(
                             workspace.CreateDirectory(
                                 "resume-seed-open-workspace")),
                         Cancellation))
        {
            TableSchema schema = session.GetTableSchema("typed_rows")
                ?? throw new InvalidOperationException(
                    "The typed-row fixture schema is missing.");
            var resumable = new CsvResumableExportRequest
            {
                DestinationPath = destinationPath,
                Profile = CsvExportProfile.LosslessV1,
                Source = Source(receipt),
                SourceSnapshotIdentity = receipt.SnapshotIdentity,
                Table = schema,
                OpenRows = (boundary, token) =>
                {
                    Assert.Null(boundary);
                    return ReadRowsThenThrowAsync(
                        session,
                        schema.TableName,
                        countBeforeFailure: 3,
                        token);
                },
                CheckpointRowInterval = 2,
            };

            await Assert.ThrowsAsync<InjectedReadFailure>(
                () => new CsvStreamingExporter()
                    .WriteResumableAsync(resumable, Cancellation)
                    .AsTask());
        }

        PreparedArtifacts artifacts = FindPreparedArtifacts(workspace.Root);
        CsvExportCheckpoint interrupted =
            ReadCheckpoint(artifacts.CheckpointPath);
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            interrupted.Phase);
        Assert.Equal(1, interrupted.Generation);
        Assert.Equal(2, interrupted.Progress.CompletedRowCount);
        Assert.Equal(-2, interrupted.Progress.LastCompletedRowId);
        Assert.True(
            new FileInfo(artifacts.DataPath).Length >
            interrupted.Progress.DataPrefixByteLength);

        CsvStreamingExportResult resumed =
            await new CSharpDbCsvExportAdapter()
                .WriteResumableTableAsync(
                    Request(
                        receipt,
                        destinationPath,
                        "typed_rows",
                        checkpointRowInterval: 2),
                    Cancellation);
        const string expected =
            "id,note,score,payload\r\n" +
            "-9,\\N,-9.5,AAEC\r\n" +
            "-2,,-2.25,AA==\r\n" +
            "4,\"a,\"\"b\"\"\",4.25,\\N\r\n" +
            "11,\"\\N\",11.5,/w==\r\n";

        Assert.Equal(
            expected,
            Encoding.UTF8.GetString(
                await File.ReadAllBytesAsync(
                    artifacts.DataPath,
                    Cancellation)));
        Assert.Equal(4, resumed.Manifest.Content.RowCount);
        CsvExportCheckpoint completed =
            ReadCheckpoint(artifacts.CheckpointPath);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            completed.Phase);
        Assert.Equal(3, completed.Generation);
        Assert.Equal(11, completed.Progress.LastCompletedRowId);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task TamperedRetainedArtifact_FailsBeforePreparedAuthorityChanges()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateTypedSnapshotAsync(workspace, "tamper");
        string destinationPath = workspace.PathFor("tamper.csv");
        CSharpDbRetainedCsvExportRequest request = Request(
            receipt,
            destinationPath,
            "typed_rows",
            checkpointRowInterval: 2);
        await new CSharpDbCsvExportAdapter()
            .WriteResumableTableAsync(request, Cancellation);
        PreparedArtifacts artifacts = FindPreparedArtifacts(workspace.Root);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            artifacts.DataPath,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            artifacts.CheckpointPath,
            Cancellation);
        byte[] tamperedSnapshot = await File.ReadAllBytesAsync(
            receipt.SnapshotPath,
            Cancellation);
        tamperedSnapshot[tamperedSnapshot.Length / 2] ^= 0x80;
        await File.WriteAllBytesAsync(
            receipt.SnapshotPath,
            tamperedSnapshot,
            Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            () => new CSharpDbCsvExportAdapter()
                .WriteResumableTableAsync(
                    request,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                artifacts.DataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                artifacts.CheckpointPath,
                Cancellation));
        Assert.Equal(
            tamperedSnapshot,
            await File.ReadAllBytesAsync(
                receipt.SnapshotPath,
                Cancellation));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData("typed_view")]
    [InlineData("sys.tables")]
    [InlineData("missing_rows")]
    public async Task InvalidPhysicalSource_CreatesNoPreparedFiles(
        string tableName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        RetainedDatabaseSnapshotReceipt receipt =
            await CreateSnapshotAsync(
                workspace,
                $"invalid-{tableName.Replace('.', '-')}",
                async database =>
                {
                    await database.ExecuteAsync(
                        "CREATE TABLE base_rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                        Cancellation);
                    await database.ExecuteAsync(
                        "INSERT INTO base_rows VALUES (1, 'one')",
                        Cancellation);
                    await database.ExecuteAsync(
                        "CREATE VIEW typed_view AS SELECT id, value FROM base_rows",
                        Cancellation);
                });
        string destinationPath = workspace.PathFor(
            $"invalid-{tableName.Replace('.', '-')}.csv");
        CSharpDbRetainedCsvExportRequest request = new()
        {
            SnapshotPath = receipt.SnapshotPath,
            SnapshotIdentity = receipt.Identity,
            TableName = tableName,
            DestinationPath = destinationPath,
        };

        Exception? error = await Record.ExceptionAsync(
            () => new CSharpDbCsvExportAdapter()
                .WriteResumableTableAsync(request, Cancellation)
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
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-csv-export-*",
            SearchOption.TopDirectoryOnly));
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateTypedSnapshotAsync(
            TemporaryDirectory workspace,
            string name) =>
        await CreateSnapshotAsync(
            workspace,
            name,
            async database =>
            {
                await database.ExecuteAsync(
                    """
                    CREATE TABLE typed_rows (
                        id INTEGER PRIMARY KEY,
                        note TEXT,
                        score REAL NOT NULL,
                        payload BLOB
                    )
                    """,
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO typed_rows VALUES (11, '\\N', 11.5, X'FF')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO typed_rows VALUES (-2, '', -2.25, X'00')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO typed_rows VALUES (7, 'deleted', 7.5, X'07')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO typed_rows VALUES (-9, NULL, -9.5, X'000102')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO typed_rows VALUES (4, 'a,\"b\"', 4.25, NULL)",
                    Cancellation);
                await database.ExecuteAsync(
                    "DELETE FROM typed_rows WHERE id = 7",
                    Cancellation);
            });

    private static async Task<RetainedDatabaseSnapshotReceipt>
        CreateSnapshotAsync(
            TemporaryDirectory workspace,
            string name,
            Func<Database, Task> seed)
    {
        string sourcePath = workspace.PathFor($"{name}-source.db");
        await using (Database database = await Database.OpenAsync(
                         sourcePath,
                         Cancellation))
        {
            await seed(database);
            await database.CheckpointAsync(Cancellation);
        }

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            workspace.PathFor($"{name}-snapshot.db"),
            databaseOptions: null,
            SnapshotOptions(
                workspace.CreateDirectory($"{name}-capture-workspace")),
            Cancellation);
    }

    private static CSharpDbRetainedCsvExportRequest Request(
        RetainedDatabaseSnapshotReceipt receipt,
        string destinationPath,
        string tableName,
        long checkpointRowInterval) => new()
        {
            SnapshotPath = receipt.SnapshotPath,
            SnapshotIdentity = receipt.Identity,
            TableName = tableName,
            DestinationPath = destinationPath,
            CheckpointRowInterval = checkpointRowInterval,
        };

    private static CsvExportSourceManifest Source(
        RetainedDatabaseSnapshotReceipt receipt) => new()
        {
            Kind = CsvExportContracts.SourceKind,
            Version = ReaderVersion(),
            SnapshotByteLength = receipt.ByteLength,
            SnapshotDigest = new CsvExportHashManifest
            {
                Algorithm = CsvExportHashManifest.Sha256Algorithm,
                Value = receipt.Sha256["sha256:".Length..],
            },
        };

    private static string ReaderVersion()
    {
        Assembly assembly = typeof(RetainedDatabaseSnapshotSession).Assembly;
        string? informational = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion;
        return informational?.Split('+', 2)[0]
            ?? assembly.GetName().Version?.ToString()
            ?? throw new InvalidOperationException(
                "The retained snapshot reader version is unavailable.");
    }

    private static async IAsyncEnumerable<CsvExportRow>
        ReadRowsThenThrowAsync(
            RetainedDatabaseSnapshotSession session,
            string tableName,
            int countBeforeFailure,
            [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using RetainedDatabaseSnapshotTableReader reader =
            session.OpenTableReader(tableName);
        int yielded = 0;
        while (await reader.MoveNextAsync(cancellationToken))
        {
            yield return new CsvExportRow(
                reader.CurrentRowId,
                reader.Current);
            yielded++;
            if (yielded == countBeforeFailure)
                throw new InjectedReadFailure();
        }
    }

    private static RetainedDatabaseSnapshotOptions SnapshotOptions(
        string workspacePath) => new()
        {
            WorkspacePath = workspacePath,
        };

    private static PreparedArtifacts FindPreparedArtifacts(string root)
    {
        string dataPath = Assert.Single(Directory.EnumerateFiles(
            root,
            ".csharpdb-csv-export-*.prepared",
            SearchOption.TopDirectoryOnly));
        string stem = dataPath[..^".prepared".Length];
        return new PreparedArtifacts(
            dataPath,
            stem + ".checkpoint",
            stem + ".checkpoint.next");
    }

    private static CsvExportCheckpoint ReadCheckpoint(string path) =>
        CsvExportCheckpointSerializer.Deserialize(File.ReadAllBytes(path));

    private sealed record PreparedArtifacts(
        string DataPath,
        string CheckpointPath,
        string PendingCheckpointPath);

    private sealed class InjectedReadFailure : Exception;

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-csv-export-adapter-tests",
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
