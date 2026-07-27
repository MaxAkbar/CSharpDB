using CSharpDB.Migration;
using CSharpDB.Migration.LiteDb;
using LiteDB;

namespace CSharpDB.Migration.LiteDb.Tests;

public sealed class LiteDbSnapshotPackageSessionTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task
        OpenUsesVerifiedPrivateCopyAndCleansWorkspaceAfterStreaming()
    {
        using var temporary = new LiteDbPackageTestDirectory();
        string sourcePath =
            temporary.PathFor("source.db");
        string packagePath =
            temporary.PathFor("retained.csdblitedb");
        CreateDatabase(sourcePath, firstId: 1);

        LiteDbRetainedSnapshot snapshot =
            await LiteDbRetainedSnapshot.CreateAsync(
                sourcePath,
                packagePath,
                Ct);
        MigrationCatalog catalog =
            await InspectAsync(snapshot, includeProfile: true);
        byte[] retainedBytes =
            await File.ReadAllBytesAsync(packagePath, Ct);

        string? privatePath = null;
        string? privateDirectory = null;
        await using (LiteDbSnapshotPackageSession session =
                     await LiteDbSnapshotPackageSession.OpenAsync(
                         packagePath,
                         catalog,
                         Options(
                             temporary.Root,
                             snapshot.ContentDigest),
                         Ct))
        {
            privatePath = session.PrivateSnapshotPath;
            privateDirectory =
                Path.GetDirectoryName(privatePath);

            Assert.Equal(
                LiteDbSnapshotPackageSession.Format,
                "csharpdb-litedb-snapshot-v1");
            Assert.Equal(
                snapshot.ContentDigest,
                session.ContentDigest);
            Assert.Equal(
                snapshot.SnapshotIdentity,
                session.SnapshotIdentity);
            Assert.Equal(
                MigrationArtifactSerializer
                    .ComputeCatalogDigest(catalog),
                MigrationArtifactSerializer
                    .ComputeCatalogDigest(
                        session.Catalog));
            Assert.Equal(
                snapshot.Source,
                session.DataSource.Source);
            Assert.Equal(
                snapshot.SnapshotIdentity,
                session.DataSource.SnapshotIdentity);

            Assert.NotEqual(
                Path.GetFullPath(packagePath),
                Path.GetFullPath(privatePath));
            Assert.True(File.Exists(privatePath));
            Assert.NotNull(privateDirectory);
            Assert.StartsWith(
                "csharpdb-litedb-",
                Path.GetFileName(privateDirectory),
                StringComparison.Ordinal);
            Assert.Equal(
                retainedBytes,
                await File.ReadAllBytesAsync(
                    privatePath,
                    Ct));

            if (OperatingSystem.IsWindows())
            {
                Assert.Throws<IOException>(() =>
                {
                    using var writer = new FileStream(
                        privatePath,
                        FileMode.Open,
                        FileAccess.Write,
                        FileShare.ReadWrite);
                });
            }

            File.Delete(sourcePath);
            Assert.Equal(
                3,
                await CountPeopleRowsAsync(
                    session,
                    catalog));
        }

        Assert.NotNull(privatePath);
        Assert.NotNull(privateDirectory);
        Assert.False(File.Exists(privatePath));
        Assert.False(Directory.Exists(privateDirectory));
        Assert.Equal(
            retainedBytes,
            await File.ReadAllBytesAsync(packagePath, Ct));
        AssertNoPrivateWorkspaces(temporary.Root);
    }

    [Fact]
    public async Task
        TamperAndCatalogSubstitutionFailBeforeSessionPublicationAndCleanWorkspace()
    {
        using var temporary = new LiteDbPackageTestDirectory();
        string firstDirectory =
            temporary.CreateDirectory("first");
        string secondDirectory =
            temporary.CreateDirectory("second");
        (LiteDbRetainedSnapshot First, MigrationCatalog FirstCatalog) =
            await CreateArtifactsAsync(
                firstDirectory,
                firstId: 1);
        (LiteDbRetainedSnapshot Second, MigrationCatalog SecondCatalog) =
            await CreateArtifactsAsync(
                secondDirectory,
                firstId: 100);

        string firstPackage = First.FilePath;
        await using (var tamper = new FileStream(
            firstPackage,
            FileMode.Append,
            FileAccess.Write,
            FileShare.None))
        {
            await tamper.WriteAsync(
                new byte[] { 0x5a },
                Ct);
        }

        LiteDbMigrationException tamperFailure =
            await Assert.ThrowsAsync<
                LiteDbMigrationException>(
                async () =>
                {
                    await using
                        LiteDbSnapshotPackageSession _ =
                            await LiteDbSnapshotPackageSession
                                .OpenAsync(
                                    firstPackage,
                                    FirstCatalog,
                                    Options(
                                        temporary.Root,
                                        First.ContentDigest),
                                    Ct);
                });
        Assert.Contains(
            "digest",
            tamperFailure.Message,
            StringComparison.OrdinalIgnoreCase);
        AssertNoPrivateWorkspaces(temporary.Root);

        LiteDbMigrationException catalogFailure =
            await Assert.ThrowsAsync<
                LiteDbMigrationException>(
                async () =>
                {
                    await using
                        LiteDbSnapshotPackageSession _ =
                            await LiteDbSnapshotPackageSession
                                .OpenAsync(
                                    Second.FilePath,
                                    FirstCatalog,
                                    Options(
                                        temporary.Root,
                                        Second.ContentDigest),
                                    Ct);
                });
        Assert.Contains(
            "catalog",
            catalogFailure.Message,
            StringComparison.OrdinalIgnoreCase);
        AssertNoPrivateWorkspaces(temporary.Root);

        Assert.NotEqual(
            FirstCatalog.Source,
            SecondCatalog.Source);
    }

    [Fact]
    public async Task
        LimitsCancellationAndInvalidPinsLeaveNoPrivateWorkspace()
    {
        using var temporary = new LiteDbPackageTestDirectory();
        (LiteDbRetainedSnapshot snapshot, MigrationCatalog catalog) =
            await CreateArtifactsAsync(
                temporary.Root,
                firstId: 1);

        LiteDbMigrationException limitFailure =
            await Assert.ThrowsAsync<
                LiteDbMigrationException>(
                async () =>
                {
                    await using
                        LiteDbSnapshotPackageSession _ =
                            await LiteDbSnapshotPackageSession
                                .OpenAsync(
                                    snapshot.FilePath,
                                    catalog,
                                    Options(
                                        temporary.Root,
                                        snapshot.ContentDigest)
                                    with
                                    {
                                        MaxSourceBytes = 1,
                                    },
                                    Ct);
                });
        Assert.Contains(
            "byte limit",
            limitFailure.Message,
            StringComparison.OrdinalIgnoreCase);
        AssertNoPrivateWorkspaces(temporary.Root);

        await Assert.ThrowsAsync<ArgumentException>(
            async () =>
            {
                await using
                    LiteDbSnapshotPackageSession _ =
                        await LiteDbSnapshotPackageSession
                            .OpenAsync(
                                snapshot.FilePath,
                                catalog,
                                Options(
                                    temporary.Root,
                                    "SHA256:" +
                                    new string('0', 64)),
                                Ct);
            });
        AssertNoPrivateWorkspaces(temporary.Root);

        using var canceled =
            new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            async () =>
            {
                await using
                    LiteDbSnapshotPackageSession _ =
                        await LiteDbSnapshotPackageSession
                            .OpenAsync(
                                snapshot.FilePath,
                                catalog,
                                Options(
                                    temporary.Root,
                                    snapshot.ContentDigest),
                                canceled.Token);
            });
        AssertNoPrivateWorkspaces(temporary.Root);
    }

    [Fact]
    public async Task
        WorkspaceRefusesToDeleteAnUnownedChild()
    {
        using var temporary = new LiteDbPackageTestDirectory();
        var workspace =
            new LiteDbSnapshotWorkspace(temporary.Root);
        string directory = workspace.DirectoryPath;
        string owned =
            workspace.GetImmediateChildPath(
                "snapshot.csdblitedb");
        await File.WriteAllBytesAsync(
            owned,
            new byte[] { 0x01 },
            Ct);
        string unowned =
            Path.Combine(directory, "unowned.txt");
        await File.WriteAllTextAsync(
            unowned,
            "must-not-delete",
            Ct);

        IOException failure =
            await Assert.ThrowsAsync<IOException>(
                async () =>
                    await workspace.DisposeAsync());

        Assert.Contains(
            "unowned child",
            failure.Message,
            StringComparison.Ordinal);
        Assert.True(File.Exists(unowned));
        Assert.True(Directory.Exists(directory));
    }

    private static async ValueTask<int>
        CountPeopleRowsAsync(
        LiteDbSnapshotPackageSession session,
        MigrationCatalog catalog)
    {
        MigrationCatalogObject collection =
            catalog.Objects.Single(item =>
                item.Kind ==
                    MigrationObjectKind.Collection &&
                item.SourceName == "People");
        MigrationCatalogObject key =
            catalog.Objects.Single(item =>
                item.ParentObjectId ==
                    collection.ObjectId &&
                item.SourceName ==
                    MigrationLiteDbDocumentCollectionContract
                        .KeyColumnName);
        MigrationCatalogObject document =
            catalog.Objects.Single(item =>
                item.ParentObjectId ==
                    collection.ObjectId &&
                item.SourceName ==
                    MigrationLiteDbDocumentCollectionContract
                        .DocumentColumnName);

        int rows = 0;
        await foreach (MigrationDataBatch batch in
                       session.DataSource.ReadAsync(
                           new MigrationReadRequest
                           {
                               SourceObjectId =
                                   collection.ObjectId,
                               ColumnObjectIds =
                               [
                                   key.ObjectId,
                                   document.ObjectId,
                               ],
                               BatchSize = 2,
                               SnapshotToken =
                                   session.SnapshotIdentity,
                           },
                           Ct))
        {
            rows += batch.Rows.Count;
        }

        return rows;
    }

    private static async ValueTask<(
        LiteDbRetainedSnapshot Snapshot,
        MigrationCatalog Catalog)> CreateArtifactsAsync(
        string directory,
        int firstId)
    {
        string sourcePath =
            Path.Combine(directory, "source.db");
        string packagePath =
            Path.Combine(
                directory,
                "retained.csdblitedb");
        CreateDatabase(sourcePath, firstId);
        LiteDbRetainedSnapshot snapshot =
            await LiteDbRetainedSnapshot.CreateAsync(
                sourcePath,
                packagePath,
                Ct);
        MigrationCatalog catalog =
            await InspectAsync(
                snapshot,
                includeProfile: true);
        return (snapshot, catalog);
    }

    private static async ValueTask<MigrationCatalog>
        InspectAsync(
        LiteDbRetainedSnapshot snapshot,
        bool includeProfile) =>
        await new LiteDbMigrationSourceInspector(snapshot)
            .InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader
                            .CurrentTargetVersion,
                    IncludeProfile = includeProfile,
                    ProfileSampleSize = 1,
                },
                Ct);

    private static LiteDbSnapshotPackageOpenOptions
        Options(
        string workspacePath,
        string contentDigest) =>
        new()
        {
            WorkspacePath = workspacePath,
            ExpectedContentDigest = contentDigest,
        };

    private static void CreateDatabase(
        string path,
        int firstId)
    {
        using var database =
            new LiteDatabase(
                new ConnectionString
                {
                    Filename = path,
                    Connection =
                        ConnectionType.Direct,
                });
        ILiteCollection<BsonDocument> people =
            database.GetCollection(
                "People",
                BsonAutoId.Int32);
        people.Insert(
            new BsonDocument
            {
                ["_id"] = firstId,
                ["name"] = "Ada",
            });
        people.Insert(
            new BsonDocument
            {
                ["_id"] = firstId + 1,
                ["name"] = "Grace",
            });
        people.Insert(
            new BsonDocument
            {
                ["_id"] = firstId + 2,
                ["name"] = "Linus",
            });
        database.Checkpoint();
    }

    private static void AssertNoPrivateWorkspaces(
        string directory) =>
        Assert.Empty(
            Directory.EnumerateDirectories(
                directory,
                "csharpdb-litedb-*",
                SearchOption.TopDirectoryOnly));

    private sealed class LiteDbPackageTestDirectory :
        IDisposable
    {
        internal LiteDbPackageTestDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-litedb-package-tests",
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

        internal string CreateDirectory(string name)
        {
            string path = PathFor(name);
            Directory.CreateDirectory(path);
            return path;
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
