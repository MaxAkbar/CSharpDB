using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite.Tests;

public sealed class SqliteBackupSnapshotTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateOnlineBackupLeavesSourceUnchangedAndSurvivesSourceDeletion()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("source.sqlite");
        string snapshotPath = temporary.PathFor("snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE people (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO people(id, name) VALUES
                (1, 'Ada'),
                (2, 'Grace');
            """,
            Ct);

        byte[] sourceBefore = await SqliteTestDatabase.ReadBytesAsync(sourcePath, Ct);
        IReadOnlyDictionary<string, byte[]> sidecarsBefore =
            SqliteTestDatabase.CaptureSidecars(sourcePath);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);

            Assert.Equal(
                sourceBefore,
                await SqliteTestDatabase.ReadBytesAsync(sourcePath, Ct));
            AssertSidecarsEqual(
                sidecarsBefore,
                SqliteTestDatabase.CaptureSidecars(sourcePath));

            Assert.Equal(new FileInfo(snapshotPath).Length, snapshot.ContentLength);
            Assert.Equal(
                SqliteTestDatabase.Digest(
                    await SqliteTestDatabase.ReadBytesAsync(snapshotPath, Ct)),
                snapshot.ContentDigest);
            Assert.Contains(
                snapshot.ContentDigest,
                snapshot.SnapshotIdentity,
                StringComparison.Ordinal);
            Assert.Equal(MigrationSourceKind.Sqlite, snapshot.Source.Kind);
            Assert.Equal(MigrationConsistencyKind.Backup, snapshot.Source.Consistency.Kind);

            File.Delete(sourcePath);
            Assert.False(File.Exists(sourcePath));

            MigrationCatalog catalog = await InspectAsync(snapshot);
            Assert.Equal(snapshot.Source, catalog.Source);
            Assert.Contains(
                catalog.Objects,
                candidate =>
                    candidate.Kind == MigrationObjectKind.Table &&
                    candidate.SourceName == "people");
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
        }
    }

    [Fact]
    public async Task CreateOnlineBackupIncludesCommittedWalFramesWithoutChangingSourceFiles()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("source.sqlite");
        string snapshotPath = temporary.PathFor("snapshot.sqlite");
        string connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = sourcePath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
        }.ToString();

        await using var writer = new SqliteConnection(connectionString);
        await writer.OpenAsync(Ct);
        await using (SqliteCommand setup = writer.CreateCommand())
        {
            setup.CommandText = "PRAGMA journal_mode = WAL;";
            Assert.Equal(
                "wal",
                Convert.ToString(
                    await setup.ExecuteScalarAsync(Ct),
                    System.Globalization.CultureInfo.InvariantCulture));

            setup.CommandText = "PRAGMA wal_autocheckpoint = 0;";
            await setup.ExecuteNonQueryAsync(Ct);
            setup.CommandText =
                """
                CREATE TABLE people (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                """;
            await setup.ExecuteNonQueryAsync(Ct);
            setup.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
            await using SqliteDataReader checkpoint =
                await setup.ExecuteReaderAsync(Ct);
            Assert.True(await checkpoint.ReadAsync(Ct));
            Assert.Equal(0, checkpoint.GetInt32(0));
        }

        byte[] mainDatabaseBeforeWalCommit =
            await ReadSharedBytesAsync(sourcePath, Ct);

        await using var pinnedReader = new SqliteConnection(connectionString);
        await pinnedReader.OpenAsync(Ct);
        await using (SqliteCommand begin = pinnedReader.CreateCommand())
        {
            begin.CommandText = "BEGIN;";
            await begin.ExecuteNonQueryAsync(Ct);
            begin.CommandText = "SELECT COUNT(*) FROM people;";
            Assert.Equal(
                0L,
                Convert.ToInt64(
                    await begin.ExecuteScalarAsync(Ct),
                    System.Globalization.CultureInfo.InvariantCulture));
        }

        await using (SqliteCommand insert = writer.CreateCommand())
        {
            insert.CommandText =
                """
                INSERT INTO people(id, name) VALUES
                    (1, 'Ada'),
                    (2, 'Grace'),
                    (3, 'Linus');
                """;
            Assert.Equal(3, await insert.ExecuteNonQueryAsync(Ct));
        }

        Assert.Equal(
            mainDatabaseBeforeWalCommit,
            await ReadSharedBytesAsync(sourcePath, Ct));
        string walPath = sourcePath + "-wal";
        Assert.True(File.Exists(walPath));
        Assert.True(new FileInfo(walPath).Length > 32);

        byte[] sourceBefore = await ReadSharedBytesAsync(sourcePath, Ct);
        byte[] walBefore = await ReadSharedBytesAsync(walPath, Ct);

        SqliteBackupSnapshot? snapshot = null;
        try
        {
            snapshot = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);

            Assert.Equal(
                sourceBefore,
                await ReadSharedBytesAsync(sourcePath, Ct));
            Assert.Equal(walBefore, await ReadSharedBytesAsync(walPath, Ct));

            var snapshotConnectionString = new SqliteConnectionStringBuilder
            {
                DataSource = snapshotPath,
                Mode = SqliteOpenMode.ReadOnly,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
            }.ToString();
            await using var retained = new SqliteConnection(snapshotConnectionString);
            await retained.OpenAsync(Ct);
            await using SqliteCommand rows = retained.CreateCommand();
            rows.CommandText =
                """
                SELECT group_concat(value, '|')
                FROM (
                    SELECT id || ':' || name AS value
                    FROM people
                    ORDER BY id
                );
                """;
            Assert.Equal(
                "1:Ada|2:Grace|3:Linus",
                Convert.ToString(
                    await rows.ExecuteScalarAsync(Ct),
                    System.Globalization.CultureInfo.InvariantCulture));

            await using SqliteCommand sourceRows = writer.CreateCommand();
            sourceRows.CommandText = "SELECT COUNT(*) FROM people;";
            Assert.Equal(
                3L,
                Convert.ToInt64(
                    await sourceRows.ExecuteScalarAsync(Ct),
                    System.Globalization.CultureInfo.InvariantCulture));
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(snapshot);
            await using SqliteCommand rollback = pinnedReader.CreateCommand();
            rollback.CommandText = "ROLLBACK;";
            await rollback.ExecuteNonQueryAsync(Ct);
        }
    }

    [Fact]
    public async Task OpenRequiresExactContentDigestAndRejectsTamperedSnapshot()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("source.sqlite");
        string snapshotPath = temporary.PathFor("snapshot.sqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE values_table (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO values_table(id, value) VALUES (1, 'one');
            """,
            Ct);

        SqliteBackupSnapshot? created = null;
        SqliteBackupSnapshot? reopened = null;
        try
        {
            created = await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);
            string expectedDigest = created.ContentDigest;
            long expectedLength = created.ContentLength;
            string expectedIdentity = created.SnapshotIdentity;

            reopened = await SqliteBackupSnapshot.OpenAsync(
                snapshotPath,
                expectedDigest,
                Ct);
            Assert.Equal(expectedDigest, reopened.ContentDigest);
            Assert.Equal(expectedLength, reopened.ContentLength);
            Assert.Equal(expectedIdentity, reopened.SnapshotIdentity);
            await SqliteTestDatabase.DisposeIfSupportedAsync(reopened);
            reopened = null;

            await Assert.ThrowsAsync<SqliteMigrationException>(
                async () =>
                {
                    _ = await SqliteBackupSnapshot.OpenAsync(
                        snapshotPath,
                        "sha256:" + new string('0', 64),
                        Ct);
                });

            byte[] tampered = await SqliteTestDatabase.ReadBytesAsync(snapshotPath, Ct);
            int tamperOffset = Math.Min(100, tampered.Length - 1);
            tampered[tamperOffset] ^= 0x01;
            await File.WriteAllBytesAsync(snapshotPath, tampered, Ct);

            await Assert.ThrowsAsync<SqliteMigrationException>(
                async () =>
                {
                    _ = await SqliteBackupSnapshot.OpenAsync(
                        snapshotPath,
                        expectedDigest,
                        Ct);
                });
        }
        finally
        {
            await SqliteTestDatabase.DisposeIfSupportedAsync(reopened);
            await SqliteTestDatabase.DisposeIfSupportedAsync(created);
        }
    }

    [Fact]
    public async Task CreateRejectsOversizedBackupWithoutPublishingArtifacts()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("private-source.sqlite");
        string snapshotPath = temporary.PathFor("retained.csdbsqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE payload (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
            INSERT INTO payload(value) VALUES (zeroblob(32768));
            """,
            Ct);
        byte[] sourceBefore =
            await SqliteTestDatabase.ReadBytesAsync(sourcePath, Ct);

        SqliteMigrationException failure =
            await Assert.ThrowsAsync<SqliteMigrationException>(
                async () =>
                {
                    _ = await SqliteBackupSnapshot.CreateAsync(
                        sourcePath,
                        snapshotPath,
                        maxSnapshotBytes: 1,
                        Ct);
                });

        Assert.Contains(
            "byte limit",
            failure.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            sourcePath,
            failure.Message,
            StringComparison.Ordinal);
        Assert.Equal(
            sourceBefore,
            await SqliteTestDatabase.ReadBytesAsync(sourcePath, Ct));
        Assert.False(File.Exists(snapshotPath));
        AssertNoSnapshotArtifacts(temporary.Root, sourcePath);
    }

    [Fact]
    public async Task CreateCanBeCancelledBetweenBackupStepsWithoutPublishingArtifacts()
    {
        using var temporary = new SqliteTestDirectory();
        string sourcePath = temporary.PathFor("source.sqlite");
        string snapshotPath = temporary.PathFor("retained.csdbsqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE payload (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
            INSERT INTO payload(value) VALUES (zeroblob(67108864));
            """,
            Ct);
        using var cancellation = new CancellationTokenSource();

        Task<SqliteBackupSnapshot> backup = SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                SqliteBackupSnapshot.DefaultMaxSnapshotBytes,
                cancellation.Token)
            .AsTask();
        while (!backup.IsCompleted &&
               !Directory.EnumerateFiles(temporary.Root)
                   .Where(path => !string.Equals(
                       path,
                       sourcePath,
                       StringComparison.Ordinal))
                   .Any(path => new FileInfo(path).Length > 0))
        {
            await Task.Yield();
        }

        Assert.False(backup.IsCompleted);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await backup);

        Assert.False(File.Exists(snapshotPath));
        AssertNoSnapshotArtifacts(temporary.Root, sourcePath);
    }

    private static async ValueTask<MigrationCatalog> InspectAsync(
        SqliteBackupSnapshot snapshot)
    {
        var inspector = new SqliteMigrationSourceInspector(snapshot);
        return await inspector.InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            },
            Ct);
    }

    private static void AssertSidecarsEqual(
        IReadOnlyDictionary<string, byte[]> expected,
        IReadOnlyDictionary<string, byte[]> actual)
    {
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), actual.Keys.Order(StringComparer.Ordinal));
        foreach ((string path, byte[] bytes) in expected)
            Assert.Equal(bytes, actual[path]);
    }

    private static void AssertNoSnapshotArtifacts(
        string directory,
        string sourcePath)
    {
        string[] unexpected = Directory.EnumerateFiles(directory)
            .Where(path => !string.Equals(
                path,
                sourcePath,
                StringComparison.Ordinal))
            .ToArray();
        Assert.Empty(unexpected);
    }

    private static async ValueTask<byte[]> ReadSharedBytesAsync(
        string path,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            });
        byte[] bytes = new byte[checked((int)stream.Length)];
        await stream.ReadExactlyAsync(bytes, cancellationToken);
        return bytes;
    }
}
