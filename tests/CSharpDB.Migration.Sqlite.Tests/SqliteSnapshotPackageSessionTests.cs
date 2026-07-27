using CSharpDB.Migration;
using CSharpDB.Migration.Sqlite;

namespace CSharpDB.Migration.Sqlite.Tests;

public sealed class SqliteSnapshotPackageSessionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OpensOnlyPrivateCopyAndCleansItAfterUse()
    {
        using var temporary = new SqliteTestDirectory();
        (string packagePath, MigrationCatalog catalog, string digest) =
            await CreatePackageAsync(temporary, "private-copy");
        string workspace = temporary.PathFor("workspace");
        Directory.CreateDirectory(workspace);

        SqliteSnapshotPackageSession session =
            await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                catalog,
                new SqliteSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace,
                    ExpectedContentDigest = digest,
                },
                Ct);
        string privatePath = session.PrivateSnapshotPath;
        try
        {
            Assert.NotEqual(
                Path.GetFullPath(packagePath),
                Path.GetFullPath(privatePath));
            Assert.StartsWith(
                Path.GetFullPath(workspace) + Path.DirectorySeparatorChar,
                Path.GetFullPath(privatePath),
                StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(privatePath));
            Assert.Single(Directory.EnumerateDirectories(workspace));
            if (OperatingSystem.IsWindows())
            {
                Exception? writeFailure = Record.Exception(() =>
                {
                    using var writer = new FileStream(
                        privatePath,
                        FileMode.Open,
                        FileAccess.Write,
                        FileShare.Read);
                });
                Assert.True(
                    writeFailure is IOException or UnauthorizedAccessException);

                string privateDirectory =
                    Assert.IsType<string>(
                        Path.GetDirectoryName(privatePath));
                Exception? moveFailure = Record.Exception(() =>
                    Directory.Move(
                        privateDirectory,
                        privateDirectory + "-moved"));
                Assert.True(
                    moveFailure is IOException or UnauthorizedAccessException);
            }

            File.Delete(packagePath);
            Assert.False(File.Exists(packagePath));

            MigrationCatalogObject table = Assert.Single(
                catalog.Objects,
                item =>
                    item.Kind == MigrationObjectKind.Table &&
                    item.SourceName == "items");
            MigrationCatalogObject id = Assert.Single(
                catalog.Objects,
                item =>
                    item.Kind == MigrationObjectKind.Column &&
                    item.ParentObjectId == table.ObjectId &&
                    item.SourceName == "id");
            var request = new MigrationReadRequest
            {
                SourceObjectId = table.ObjectId,
                ColumnObjectIds = [id.ObjectId],
                SnapshotToken = session.DataSource.SnapshotIdentity,
                BatchSize = 100,
            };
            var values = new List<string>();
            await foreach (MigrationDataBatch batch in
                session.DataSource.ReadAsync(request, Ct))
            {
                values.AddRange(
                    batch.Rows.Select(
                        row => Assert.IsType<string>(
                            row.Values[0].CanonicalText)));
            }

            Assert.Equal(["1", "2", "3"], values);
        }
        finally
        {
            await session.DisposeAsync();
        }

        Assert.False(File.Exists(privatePath));
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));
        await session.DisposeAsync();
    }

    [Fact]
    public async Task IntegrityCatalogAndSizeFailuresLeaveNoPrivateCopy()
    {
        using var temporary = new SqliteTestDirectory();
        (string packagePath, MigrationCatalog catalog, string digest) =
            await CreatePackageAsync(temporary, "failures");
        byte[] original = await File.ReadAllBytesAsync(packagePath, Ct);
        string workspace = temporary.PathFor("workspace");
        Directory.CreateDirectory(workspace);

        string wrongDigest =
            "sha256:" + new string(digest[^1] == '0' ? '1' : '0', 64);
        await Assert.ThrowsAsync<SqliteMigrationException>(
            async () => await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                catalog,
                Options(workspace, wrongDigest),
                Ct));
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));

        byte[] tampered = original.ToArray();
        tampered[^1] ^= 0x01;
        await File.WriteAllBytesAsync(packagePath, tampered, Ct);
        await Assert.ThrowsAsync<SqliteMigrationException>(
            async () => await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                catalog,
                Options(workspace, digest),
                Ct));
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));
        await File.WriteAllBytesAsync(packagePath, original, Ct);

        MigrationCatalog changedCatalog = catalog with
        {
            Objects = catalog.Objects.Select(item =>
                item.Kind != MigrationObjectKind.Namespace
                    ? item
                    : item with
                    {
                        Facets = item.Facets.Select(facet =>
                            facet.Name == "sqliteProfileIncluded"
                                ? facet with
                                {
                                    Value = facet.Value == "true"
                                        ? "false"
                                        : "true",
                                }
                                : facet).ToArray(),
                    }).ToArray(),
        };
        await Assert.ThrowsAsync<SqliteMigrationException>(
            async () => await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                changedCatalog,
                Options(workspace, digest),
                Ct));
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));

        await Assert.ThrowsAsync<SqliteMigrationException>(
            async () => await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                catalog,
                Options(
                    workspace,
                    digest,
                    maxSourceBytes: original.LongLength - 1),
                Ct));
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));
        Assert.Equal(original, await File.ReadAllBytesAsync(packagePath, Ct));
    }

    [Fact]
    public async Task RejectsUnsupportedCatalogRecipeBeforeCreatingWorkspaceCopy()
    {
        using var temporary = new SqliteTestDirectory();
        (string packagePath, MigrationCatalog catalog, string digest) =
            await CreatePackageAsync(temporary, "unsupported");
        string workspace = temporary.PathFor("workspace");
        Directory.CreateDirectory(workspace);
        MigrationCatalog futureCatalog = catalog with
        {
            Objects = catalog.Objects.Select(item =>
                item.Kind != MigrationObjectKind.Namespace
                    ? item
                    : item with
                    {
                        Facets = item.Facets.Select(facet =>
                            facet.Name == "sqliteCatalogContract"
                                ? facet with
                                {
                                    Value = "csharpdb-sqlite-catalog-v2",
                                }
                                : facet).ToArray(),
                    }).ToArray(),
        };

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await SqliteSnapshotPackageSession.OpenAsync(
                packagePath,
                futureCatalog,
                Options(workspace, digest),
                Ct));

        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace));
    }

    [Fact]
    public void UnixWorkspacePolicyRequiresDirectoryTrustedOwnerAndStickyWrites()
    {
        const uint directory = 0x4000;
        const uint regularFile = 0x8000;
        const uint ownerOnly = 0x01C0;
        const uint allWritable = 0x01FF;
        const uint sticky = 0x0200;
        const uint currentUser = 1000;

        Assert.True(
            SqliteSnapshotWorkspace.IsTrustedUnixDirectoryMetadata(
                directory | ownerOnly,
                currentUser,
                currentUser));
        Assert.True(
            SqliteSnapshotWorkspace.IsTrustedUnixDirectoryMetadata(
                directory | allWritable | sticky,
                ownerUserId: 0,
                currentUser));
        Assert.False(
            SqliteSnapshotWorkspace.IsTrustedUnixDirectoryMetadata(
                directory | allWritable,
                currentUser,
                currentUser));
        Assert.False(
            SqliteSnapshotWorkspace.IsTrustedUnixDirectoryMetadata(
                directory | ownerOnly,
                ownerUserId: 2000,
                currentUser));
        Assert.False(
            SqliteSnapshotWorkspace.IsTrustedUnixDirectoryMetadata(
                regularFile | ownerOnly,
                currentUser,
                currentUser));
    }

    [Fact]
    public async Task UnixWorkspaceRejectsWritableAndLinkedAncestorButAllowsStickyRoot()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var temporary = new SqliteTestDirectory();
        string writableParent = temporary.PathFor("writable-parent");
        string writableWorkspace = Path.Combine(
            writableParent,
            "workspace");
        Directory.CreateDirectory(writableWorkspace);
        File.SetUnixFileMode(
            writableParent,
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.UserExecute |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupWrite |
            UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherWrite |
            UnixFileMode.OtherExecute);

        Assert.Throws<IOException>(
            () => new SqliteSnapshotWorkspace(
                writableWorkspace));
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                writableWorkspace));

        File.SetUnixFileMode(
            writableParent,
            File.GetUnixFileMode(writableParent) |
                UnixFileMode.StickyBit);
        await using (var allowed =
                     new SqliteSnapshotWorkspace(
                         writableWorkspace))
        {
            Assert.True(Directory.Exists(
                allowed.DirectoryPath));
        }
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                writableWorkspace));

        string realParent = temporary.PathFor("real-parent");
        string realWorkspace = Path.Combine(
            realParent,
            "workspace");
        string linkedParent = temporary.PathFor(
            "linked-parent");
        Directory.CreateDirectory(realWorkspace);
        Directory.CreateSymbolicLink(
            linkedParent,
            realParent);

        Assert.Throws<IOException>(
            () => new SqliteSnapshotWorkspace(
                Path.Combine(
                    linkedParent,
                    "workspace")));
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                realWorkspace));
    }

    private static SqliteSnapshotPackageOpenOptions Options(
        string workspace,
        string digest,
        long maxSourceBytes =
            SqliteSnapshotPackageOpenOptions.DefaultMaxSourceBytes) =>
        new()
        {
            WorkspacePath = workspace,
            ExpectedContentDigest = digest,
            MaxSourceBytes = maxSourceBytes,
        };

    private static async ValueTask<(
        string PackagePath,
        MigrationCatalog Catalog,
        string Digest)> CreatePackageAsync(
        SqliteTestDirectory temporary,
        string prefix)
    {
        string sourcePath = temporary.PathFor(prefix + "-source.sqlite");
        string packagePath = temporary.PathFor(prefix + ".csdbsqlite");
        await SqliteTestDatabase.CreateAsync(
            sourcePath,
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL
            );
            INSERT INTO items(id, label) VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three');
            """,
            Ct);
        SqliteBackupSnapshot snapshot =
            await SqliteBackupSnapshot.CreateAsync(
                sourcePath,
                packagePath,
                Ct);
        MigrationCatalog catalog =
            await new SqliteMigrationSourceInspector(snapshot)
                .InspectAsync(
                    new MigrationInspectionRequest
                    {
                        TargetCSharpDbVersion =
                            CSharpDbCapabilityCatalogLoader
                                .CurrentTargetVersion,
                        IncludeProfile = true,
                        ProfileSampleSize = 2,
                    },
                    Ct);
        return (packagePath, catalog, snapshot.ContentDigest);
    }
}
