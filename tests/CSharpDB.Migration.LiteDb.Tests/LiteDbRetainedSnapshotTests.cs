using System.Security.Cryptography;
using CSharpDB.Migration;
using CSharpDB.Migration.LiteDb;
using LiteDB;

namespace CSharpDB.Migration.LiteDb.Tests;

public sealed class LiteDbRetainedSnapshotTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task CaptureIsContentPinnedAndSurvivesSourceRemoval()
    {
        using var files = new TemporaryDirectory();
        string sourcePath = files.PathFor("source.db");
        string snapshotPath = files.PathFor("retained.csdblitedb");
        CreateDatabase(sourcePath);
        byte[] sourceBefore =
            await File.ReadAllBytesAsync(sourcePath, Ct);

        LiteDbRetainedSnapshot snapshot =
            await LiteDbRetainedSnapshot.CreateAsync(
                sourcePath,
                snapshotPath,
                Ct);

        Assert.Equal(
            sourceBefore,
            await File.ReadAllBytesAsync(sourcePath, Ct));
        Assert.Equal(
            sourceBefore,
            await File.ReadAllBytesAsync(snapshotPath, Ct));
        Assert.Equal(sourceBefore.Length, snapshot.ContentLength);
        Assert.Equal(
            "sha256:" +
            Convert.ToHexString(
                    SHA256.HashData(sourceBefore))
                .ToLowerInvariant(),
            snapshot.ContentDigest);
        Assert.Equal(
            MigrationConsistencyKind.Snapshot,
            snapshot.Source.Consistency.Kind);
        Assert.Equal(
            snapshot.ContentDigest,
            snapshot.Source.Fingerprint);
        Assert.Contains(
            snapshot.ContentDigest,
            snapshot.SnapshotIdentity,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sourcePath,
            snapshot.Source.Identity,
            StringComparison.OrdinalIgnoreCase);

        File.Delete(sourcePath);
        MigrationCatalog catalog =
            await new LiteDbMigrationSourceInspector(snapshot)
                .InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion =
                            CSharpDbCapabilityCatalogLoader
                                .CurrentTargetVersion,
                        IncludeProfile = true,
                    },
                    Ct);
        Assert.Equal(snapshot.Source, catalog.Source);
        Assert.Equal(
            "3",
            catalog.Objects.Single(item =>
                    item.Kind ==
                    MigrationObjectKind.Collection)
                .Facets.Single(facet =>
                    facet.Name ==
                    "liteDbDocumentCount")
                .Value);
        Assert.DoesNotContain(
            sourcePath,
            MigrationArtifactSerializer.SerializeCatalog(
                catalog),
            StringComparison.OrdinalIgnoreCase);

        LiteDbRetainedSnapshot reopened =
            await LiteDbRetainedSnapshot.OpenAsync(
                snapshotPath,
                snapshot.ContentDigest,
                Ct);
        Assert.Equal(snapshot.Source, reopened.Source);
        await Assert.ThrowsAsync<LiteDbMigrationException>(
            async () =>
                await LiteDbRetainedSnapshot.OpenAsync(
                    snapshotPath,
                    DifferentDigest(
                        snapshot.ContentDigest),
                    Ct));
    }

    [Fact]
    public async Task CaptureRejectsWriterOversizeEncryptionAndCollisions()
    {
        using var files = new TemporaryDirectory();
        string sourcePath = files.PathFor("source.db");
        string snapshotPath = files.PathFor("retained.csdblitedb");
        CreateDatabase(sourcePath);
        long sourceBytes = new FileInfo(sourcePath).Length;

        await Assert.ThrowsAsync<LiteDbMigrationException>(
            async () =>
                await LiteDbRetainedSnapshot.CreateAsync(
                    sourcePath,
                    snapshotPath,
                    sourceBytes - 1,
                    Ct));
        Assert.False(File.Exists(snapshotPath));

        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
                await LiteDbRetainedSnapshot.CreateAsync(
                    sourcePath,
                    sourcePath,
                    Ct));

        await File.WriteAllTextAsync(
            snapshotPath,
            "existing",
            Ct);
        await Assert.ThrowsAsync<IOException>(
            async () =>
                await LiteDbRetainedSnapshot.CreateAsync(
                    sourcePath,
                    snapshotPath,
                    Ct));
        Assert.Equal(
            "existing",
            await File.ReadAllTextAsync(snapshotPath, Ct));
        File.Delete(snapshotPath);

        using (var writer = OpenDatabase(sourcePath))
        {
            writer.GetCollection<BsonDocument>(
                    "documents")
                .Insert(
                    new BsonDocument
                    {
                        ["_id"] = 4,
                    });
            await Assert.ThrowsAsync<
                LiteDbMigrationException>(
                async () =>
                    await LiteDbRetainedSnapshot
                        .CreateAsync(
                            sourcePath,
                            snapshotPath,
                            Ct));
        }
        Assert.False(File.Exists(snapshotPath));

        string encryptedPath =
            files.PathFor("encrypted.db");
        using (var encrypted = new LiteDatabase(
                   new ConnectionString
                   {
                       Filename = encryptedPath,
                       Password = "not-a-cli-secret",
                       Connection =
                           ConnectionType.Direct,
                   }))
        {
            encrypted.GetCollection<BsonDocument>(
                    "documents")
                .Insert(
                    new BsonDocument
                    {
                        ["_id"] = 1,
                    });
            encrypted.Checkpoint();
        }

        await Assert.ThrowsAsync<LiteDbMigrationException>(
            async () =>
                await LiteDbRetainedSnapshot.CreateAsync(
                    encryptedPath,
                    snapshotPath,
                    Ct));
        Assert.False(File.Exists(snapshotPath));
    }

    private static void CreateDatabase(string path)
    {
        using LiteDatabase database =
            OpenDatabase(path);
        ILiteCollection<BsonDocument> documents =
            database.GetCollection<BsonDocument>(
                "documents");
        documents.Insert(
            new BsonDocument
            {
                ["_id"] = 2,
                ["value"] = "second",
            });
        documents.Insert(
            new BsonDocument
            {
                ["_id"] = 1,
                ["value"] = "first",
            });
        documents.Insert(
            new BsonDocument
            {
                ["_id"] = "1",
                ["value"] = "text-key",
            });
        database.Checkpoint();
    }

    private static LiteDatabase OpenDatabase(
        string path) =>
        new(
            new ConnectionString
            {
                Filename = path,
                Connection = ConnectionType.Direct,
            });

    private static string DifferentDigest(
        string digest)
    {
        char replacement =
            digest[7] == '0' ? '1' : '0';
        return digest[..7] +
            replacement +
            digest[8..];
    }

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-litedb-snapshot-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

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
