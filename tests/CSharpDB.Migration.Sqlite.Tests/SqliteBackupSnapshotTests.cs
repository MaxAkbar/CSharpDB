using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;

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
}
