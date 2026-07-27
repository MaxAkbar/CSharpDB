using System.Security.Cryptography;
using System.Runtime.InteropServices;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class RetainedDatabaseSnapshotTests : IAsyncLifetime
{
    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        $"csharpdb_retained_snapshot_{Guid.NewGuid():N}");

    private CancellationToken Cancellation => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_root);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);

        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CaptureAsync_MissingSource_DoesNotCreateSourceWalOrDestination()
    {
        string sourcePath = PathInRoot("missing.db");
        string destinationPath = PathInRoot("snapshot.db");
        string workspacePath = CreateDirectory("workspace");

        await Assert.ThrowsAsync<FileNotFoundException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(workspacePath),
                Cancellation));

        Assert.False(File.Exists(sourcePath));
        Assert.False(File.Exists(sourcePath + ".wal"));
        Assert.False(File.Exists(destinationPath));
        Assert.Empty(Directory.EnumerateFiles(workspacePath, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task CaptureAsync_NoWalDatabase_ReopensReadOnlyWithBoundIdentity()
    {
        string sourcePath = PathInRoot("source.db");
        string destinationPath = PathInRoot("snapshot.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);

        Assert.False(File.Exists(sourcePath + ".wal"));

        RetainedDatabaseSnapshotReceipt receipt = await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            destinationPath,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("capture-workspace")),
            Cancellation);

        byte[] snapshotBytes = await File.ReadAllBytesAsync(destinationPath, Cancellation);
        string expectedSha256 = "sha256:" +
            Convert.ToHexString(SHA256.HashData(snapshotBytes)).ToLowerInvariant();

        Assert.Equal(destinationPath, receipt.SnapshotPath);
        Assert.Equal(snapshotBytes.LongLength, receipt.ByteLength);
        Assert.Equal(expectedSha256, receipt.Sha256);
        Assert.Equal(receipt.ByteLength, receipt.Identity.ByteLength);
        Assert.Equal(receipt.Sha256, receipt.Identity.Sha256);
        Assert.Equal(receipt.SnapshotIdentity, receipt.Identity.SnapshotIdentity);
        Assert.False(File.Exists(destinationPath + ".wal"));

        await using (RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            destinationPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("open-workspace")),
            Cancellation))
        {
            Assert.Equal(destinationPath, session.SnapshotPath);
            Assert.Equal(receipt.Identity, session.Identity);
            Assert.Contains("items", session.GetTableNames());
            await AssertItemAsync(session, expectedId: 1, expectedValue: "checkpointed");
        }

        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_OfflineDatabaseAndCommittedWal_RecoversWalOnlyRow()
    {
        OfflineWalFixture fixture = await CreateOfflineWalFixtureAsync("wal-fixture");

        await using (Database baseOnly = await Database.OpenAsync(
            fixture.BaseOnlyPath,
            Cancellation))
        {
            await using QueryResult rows = await baseOnly.ExecuteAsync(
                "SELECT COUNT(*) FROM items",
                Cancellation);
            var values = await rows.ToListAsync(Cancellation);
            Assert.Equal(0L, values[0][0].AsInteger);
        }

        RetainedDatabaseSnapshotReceipt receipt = await RetainedDatabaseSnapshot.CaptureAsync(
            fixture.SourcePath,
            PathInRoot("wal-snapshot.db"),
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("wal-capture-workspace")),
            Cancellation);

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("wal-open-workspace")),
            Cancellation);

        await AssertItemAsync(session, expectedId: 7, expectedValue: "committed-in-wal");
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_DoesNotChangeSourceDatabaseOrWalBytesOrWriteTimes()
    {
        OfflineWalFixture fixture = await CreateOfflineWalFixtureAsync("immutable-source");
        string walPath = fixture.SourcePath + ".wal";
        byte[] databaseBefore = await File.ReadAllBytesAsync(fixture.SourcePath, Cancellation);
        byte[] walBefore = await File.ReadAllBytesAsync(walPath, Cancellation);
        DateTime databaseWriteTimeBefore = File.GetLastWriteTimeUtc(fixture.SourcePath);
        DateTime walWriteTimeBefore = File.GetLastWriteTimeUtc(walPath);

        await RetainedDatabaseSnapshot.CaptureAsync(
            fixture.SourcePath,
            PathInRoot("immutable-snapshot.db"),
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("immutable-workspace")),
            Cancellation);

        Assert.Equal(databaseBefore, await File.ReadAllBytesAsync(fixture.SourcePath, Cancellation));
        Assert.Equal(walBefore, await File.ReadAllBytesAsync(walPath, Cancellation));
        Assert.Equal(databaseWriteTimeBefore, File.GetLastWriteTimeUtc(fixture.SourcePath));
        Assert.Equal(walWriteTimeBefore, File.GetLastWriteTimeUtc(walPath));
    }

    [Fact]
    public async Task CaptureAsync_ActiveWriterOnWindows_FailsWithoutPublishingDestination()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string sourcePath = PathInRoot("active-writer.db");
        string destinationPath = PathInRoot("active-writer-snapshot.db");
        DatabaseOptions databaseOptions = CreateDatabaseOptions(FileShare.Read);

        await using Database writer = await Database.OpenAsync(
            sourcePath,
            databaseOptions,
            Cancellation);
        await writer.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            Cancellation);
        await writer.ExecuteAsync(
            "INSERT INTO items VALUES (1, 'writer-is-active')",
            Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("active-writer-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));

        await using QueryResult result = await writer.ExecuteAsync(
            "SELECT value FROM items WHERE id = 1",
            Cancellation);
        var rows = await result.ToListAsync(Cancellation);
        Assert.Equal("writer-is-active", rows[0][0].AsText);
    }

    [Fact]
    public async Task CaptureAsync_SourceSymbolicLinkIsRejectedWithoutPublishing()
    {
        string sourceTargetPath = PathInRoot("source-link-target.db");
        string sourceLinkPath = PathInRoot("source-link.db");
        string destinationPath = PathInRoot("source-link-snapshot.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourceTargetPath);
        if (!TryCreateFileSymbolicLink(sourceLinkPath, sourceTargetPath))
            return;

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourceLinkPath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("source-link-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_SourceHardLinkIsRejectedWithoutPublishing()
    {
        string sourceTargetPath = PathInRoot("source-hardlink-target.db");
        string sourceLinkPath = PathInRoot("source-hardlink.db");
        string destinationPath = PathInRoot("source-hardlink-snapshot.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourceTargetPath);
        if (!TryCreateHardLink(sourceLinkPath, sourceTargetPath))
            return;

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourceLinkPath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("source-hardlink-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_WalSymbolicLinkIsRejectedWithoutPublishing()
    {
        OfflineWalFixture fixture = await CreateOfflineWalFixtureAsync("wal-link");
        string walPath = fixture.SourcePath + ".wal";
        string walTargetPath = PathInRoot("wal-link-target.wal");
        string destinationPath = PathInRoot("wal-link-snapshot.db");
        File.Move(walPath, walTargetPath);
        if (!TryCreateFileSymbolicLink(walPath, walTargetPath))
        {
            File.Move(walTargetPath, walPath);
            return;
        }

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                fixture.SourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("wal-link-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_DestinationParentSymbolicLinkIsRejectedWithoutPublishing()
    {
        string sourcePath = PathInRoot("destination-parent-source.db");
        string realParentPath = CreateDirectory("destination-real-parent");
        string linkedParentPath = PathInRoot("destination-linked-parent");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        if (!TryCreateDirectorySymbolicLink(linkedParentPath, realParentPath))
            return;
        string destinationPath = Path.Combine(linkedParentPath, "snapshot.db");

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("destination-parent-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(Path.Combine(realParentPath, "snapshot.db")));
    }

    [Fact]
    public async Task CaptureAsync_ExistingDestinationIsPreserved()
    {
        string sourcePath = PathInRoot("existing-destination-source.db");
        string destinationPath = PathInRoot("existing-destination.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        byte[] sentinel = [0x43, 0x53, 0x44, 0x42, 0x7F];
        await File.WriteAllBytesAsync(destinationPath, sentinel, Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("existing-destination-workspace")),
                Cancellation));

        Assert.Equal(sentinel, await File.ReadAllBytesAsync(destinationPath, Cancellation));
        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    [Fact]
    public async Task CaptureAsync_DestinationCannotOccupySourceWalNamespace()
    {
        string sourcePath = PathInRoot("pair-alias-source.db");
        string destinationPath = sourcePath + ".wal";
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);

        await Assert.ThrowsAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory("pair-alias-workspace")),
                Cancellation));

        Assert.False(File.Exists(destinationPath));
        await using Database source = await Database.OpenAsync(sourcePath, Cancellation);
        await using QueryResult result = await source.ExecuteAsync(
            "SELECT value FROM items WHERE id = 1",
            Cancellation);
        Assert.Equal(
            "checkpointed",
            Assert.Single(await result.ToListAsync(Cancellation))[0].AsText);
    }

    [Fact]
    public async Task CaptureAsync_InaccessibleExistingWalIsNotTreatedAsAbsent()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string sourcePath = PathInRoot("locked-wal-source.db");
        string walPath = sourcePath + ".wal";
        string destinationPath = PathInRoot("locked-wal-snapshot.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        byte[] sentinel = [0x43, 0x57, 0x41, 0x4c, 0x7f];
        await File.WriteAllBytesAsync(walPath, sentinel, Cancellation);

        await using (var lockedWal = new FileStream(
                         walPath,
                         FileMode.Open,
                         FileAccess.ReadWrite,
                         FileShare.None))
        {
            await Assert.ThrowsAnyAsync<IOException>(
                async () => await RetainedDatabaseSnapshot.CaptureAsync(
                    sourcePath,
                    destinationPath,
                    databaseOptions: null,
                    SnapshotOptions(CreateDirectory("locked-wal-workspace")),
                    Cancellation));
        }

        Assert.Equal(sentinel, await File.ReadAllBytesAsync(walPath, Cancellation));
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task CaptureAsync_EnforcesDatabaseWalAndSnapshotByteLimits()
    {
        OfflineWalFixture fixture = await CreateOfflineWalFixtureAsync("bounded-source");
        string baselinePath = PathInRoot("bounded-baseline.db");
        RetainedDatabaseSnapshotReceipt baseline = await RetainedDatabaseSnapshot.CaptureAsync(
            fixture.SourcePath,
            baselinePath,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("bounded-baseline-workspace")),
            Cancellation);

        long databaseLength = new FileInfo(fixture.SourcePath).Length;
        long walLength = new FileInfo(fixture.SourcePath + ".wal").Length;
        Assert.True(databaseLength > 1);
        Assert.True(walLength > PageConstants.WalHeaderSize);
        Assert.True(baseline.ByteLength > 1);

        await AssertLimitFailureAsync(
            fixture.SourcePath,
            PathInRoot("database-limit.db"),
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = CreateDirectory("database-limit-workspace"),
                MaxDatabaseBytes = databaseLength - 1,
                CopyBufferBytes = 4 * 1024,
                MaxCachedPages = 4,
                MaxCachedWalReadPages = 2,
            });

        await AssertLimitFailureAsync(
            fixture.SourcePath,
            PathInRoot("wal-limit.db"),
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = CreateDirectory("wal-limit-workspace"),
                MaxWalBytes = walLength - 1,
                CopyBufferBytes = 4 * 1024,
                MaxCachedPages = 4,
                MaxCachedWalReadPages = 2,
            });

        await AssertLimitFailureAsync(
            fixture.SourcePath,
            PathInRoot("snapshot-limit.db"),
            new RetainedDatabaseSnapshotOptions
            {
                WorkspacePath = CreateDirectory("snapshot-limit-workspace"),
                MaxSnapshotBytes = baseline.ByteLength - 1,
                CopyBufferBytes = 4 * 1024,
                MaxCachedPages = 4,
                MaxCachedWalReadPages = 2,
            });
    }

    [Theory]
    [InlineData(null, 4)]
    [InlineData(0, 4)]
    [InlineData(-1, 4)]
    [InlineData(2, 2)]
    [InlineData(8, 4)]
    public void CreateBoundedDatabaseOptions_AlwaysUsesFinitePageCache(
        int? sourceMaxCachedPages,
        int expectedMaxCachedPages)
    {
        var source = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    MaxCachedPages = sourceMaxCachedPages,
                },
            },
        };
        var retained = new RetainedDatabaseSnapshotOptions
        {
            MaxCachedPages = 4,
        };

        DatabaseOptions bounded = RetainedDatabaseSnapshot.CreateBoundedDatabaseOptions(
            source,
            retained);

        Assert.Equal(
            expectedMaxCachedPages,
            bounded.StorageEngineOptions.PagerOptions.MaxCachedPages);
        Assert.Null(bounded.StorageEngineOptions.PagerOptions.PageCacheFactory);
    }

    [Fact]
    public async Task DefaultWorkspace_CreatesPrivateRandomChildDirectlyUnderTempParent()
    {
        RetainedDatabaseSnapshotWorkspace workspace =
            RetainedDatabaseSnapshotWorkspace.Create(configuredParent: null);
        string root = workspace.RootPath;
        try
        {
            Assert.Equal(
                RetainedDatabaseSnapshotWorkspace.CanonicalizeDefaultParent(Path.GetTempPath()),
                Path.GetDirectoryName(root));
            Assert.StartsWith(".csharpdb-retained-", Path.GetFileName(root), StringComparison.Ordinal);
            Assert.NotEqual(
                "csharpdb-retained-snapshots",
                Path.GetFileName(Path.GetDirectoryName(root)));
            if (!OperatingSystem.IsWindows())
            {
                Assert.Equal(
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute,
                    File.GetUnixFileMode(root));
            }
        }
        finally
        {
            await workspace.DisposeAsync();
        }

        Assert.False(Directory.Exists(root));
    }

    [Fact]
    public void CanonicalizeDefaultParent_ResolvesUnixAncestorLinks()
    {
        if (OperatingSystem.IsWindows())
            return;

        string actual = CreateDirectory("physical-default-parent");
        string link = PathInRoot("linked-default-parent");
        if (!TryCreateDirectorySymbolicLink(link, actual))
            return;

        Assert.Equal(
            RetainedDatabaseSnapshotWorkspace.CanonicalizeDefaultParent(actual),
            RetainedDatabaseSnapshotWorkspace.CanonicalizeDefaultParent(link));
    }

    [Fact]
    public void CanonicalizeDefaultParent_RejectsSharedWritableUnixDirectoryWithoutStickyBit()
    {
        if (OperatingSystem.IsWindows())
            return;

        string parent = CreateDirectory("unsafe-default-parent");
        UnixFileMode originalMode = File.GetUnixFileMode(parent);
        try
        {
            File.SetUnixFileMode(
                parent,
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
                () => RetainedDatabaseSnapshotWorkspace.CanonicalizeDefaultParent(parent));
        }
        finally
        {
            File.SetUnixFileMode(parent, originalMode);
        }
    }

    [Fact]
    public void ValidateUnixDefaultParentSecurity_RejectsUntrustedOwnerEvenWithStickyBit()
    {
        UnixFileMode sharedSticky =
            UnixFileMode.UserRead |
            UnixFileMode.UserWrite |
            UnixFileMode.UserExecute |
            UnixFileMode.GroupRead |
            UnixFileMode.GroupWrite |
            UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead |
            UnixFileMode.OtherWrite |
            UnixFileMode.OtherExecute |
            UnixFileMode.StickyBit;

        Assert.Throws<IOException>(
            () => RetainedDatabaseSnapshotWorkspace.ValidateUnixDefaultParentSecurity(
                sharedSticky,
                ownerUserId: 1001,
                effectiveUserId: 1000));

        RetainedDatabaseSnapshotWorkspace.ValidateUnixDefaultParentSecurity(
            sharedSticky,
            ownerUserId: 0,
            effectiveUserId: 1000);
        RetainedDatabaseSnapshotWorkspace.ValidateUnixDefaultParentSecurity(
            sharedSticky,
            ownerUserId: 1000,
            effectiveUserId: 1000);
    }

    [Fact]
    public async Task CaptureAsync_PreCanceled_CleansTemporaryFilesAndDoesNotPublish()
    {
        string sourcePath = PathInRoot("canceled-source.db");
        string destinationPath = PathInRoot("canceled-snapshot.db");
        string workspacePath = CreateDirectory("canceled-workspace");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        string[] filesBefore = EnumerateRelativeFiles();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                SnapshotOptions(workspacePath),
                cancellation.Token));

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));
        Assert.Equal(filesBefore, EnumerateRelativeFiles());
        Assert.Empty(Directory.EnumerateFiles(workspacePath, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task OpenAsync_WrongLengthOrHash_RejectsArtifactBeforeCreatingCompanionWal()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync("wrong-identity");
        byte[] artifactBefore = await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation);
        const int firstDigestCharacter = 7;
        char replacement = receipt.Sha256[firstDigestCharacter] == '0' ? '1' : '0';
        string wrongHash = receipt.Sha256[..firstDigestCharacter] +
            replacement +
            receipt.Sha256[(firstDigestCharacter + 1)..];
        long wrongLength = receipt.ByteLength + 1;
        RetainedDatabaseSnapshotIdentity[] invalidIdentities =
        [
            new(
                wrongLength,
                receipt.Sha256,
                CreateSnapshotIdentity(wrongLength, receipt.Sha256)),
            new(
                receipt.ByteLength,
                wrongHash,
                CreateSnapshotIdentity(receipt.ByteLength, wrongHash)),
        ];

        for (int index = 0; index < invalidIdentities.Length; index++)
        {
            await Assert.ThrowsAnyAsync<IOException>(
                async () => await RetainedDatabaseSnapshot.OpenAsync(
                    receipt.SnapshotPath,
                    invalidIdentities[index],
                    databaseOptions: null,
                    SnapshotOptions(CreateDirectory($"wrong-identity-workspace-{index}")),
                    Cancellation));

            Assert.Equal(artifactBefore, await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation));
            Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
        }
    }

    [Fact]
    public async Task OpenAsync_MissingArtifact_DoesNotCreateArtifactWalOrWorkspaceFiles()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync("missing-artifact");
        string workspacePath = CreateDirectory("missing-artifact-open-workspace");
        File.Delete(receipt.SnapshotPath);

        await Assert.ThrowsAsync<FileNotFoundException>(
            async () => await RetainedDatabaseSnapshot.OpenAsync(
                receipt.SnapshotPath,
                receipt.Identity,
                databaseOptions: null,
                SnapshotOptions(workspacePath),
                Cancellation));

        Assert.False(File.Exists(receipt.SnapshotPath));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
        Assert.Empty(Directory.EnumerateFiles(workspacePath, "*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task OpenAsync_SymbolicAndHardLinkedArtifactsAreRejectedWithoutCompanionWal()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync("artifact-links");
        string symbolicLinkPath = PathInRoot("artifact-symbolic-link.db");
        string hardLinkPath = PathInRoot("artifact-hard-link.db");

        if (TryCreateFileSymbolicLink(symbolicLinkPath, receipt.SnapshotPath))
        {
            await Assert.ThrowsAnyAsync<IOException>(
                async () => await RetainedDatabaseSnapshot.OpenAsync(
                    symbolicLinkPath,
                    receipt.Identity,
                    databaseOptions: null,
                    SnapshotOptions(CreateDirectory("artifact-symbolic-link-workspace")),
                    Cancellation));
            Assert.False(File.Exists(symbolicLinkPath + ".wal"));
        }

        if (TryCreateHardLink(hardLinkPath, receipt.SnapshotPath))
        {
            await Assert.ThrowsAnyAsync<IOException>(
                async () => await RetainedDatabaseSnapshot.OpenAsync(
                    hardLinkPath,
                    receipt.Identity,
                    databaseOptions: null,
                    SnapshotOptions(CreateDirectory("artifact-hard-link-workspace")),
                    Cancellation));
            Assert.False(File.Exists(hardLinkPath + ".wal"));
        }

        Assert.True(File.Exists(receipt.SnapshotPath));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Theory]
    [InlineData("bit-flip")]
    [InlineData("truncate")]
    [InlineData("append")]
    public async Task OpenAsync_TamperedArtifact_IsRejectedAndPreservedWithoutCompanionWal(
        string tamperMode)
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync(
            $"tamper-{tamperMode}");
        byte[] tampered = await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation);

        switch (tamperMode)
        {
            case "bit-flip":
                tampered[tampered.Length / 2] ^= 0x80;
                break;
            case "truncate":
                Array.Resize(ref tampered, tampered.Length - 1);
                break;
            case "append":
                Array.Resize(ref tampered, tampered.Length + 1);
                tampered[^1] = 0xA5;
                break;
            default:
                throw new InvalidOperationException($"Unexpected tamper mode '{tamperMode}'.");
        }

        await File.WriteAllBytesAsync(receipt.SnapshotPath, tampered, Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.OpenAsync(
                receipt.SnapshotPath,
                receipt.Identity,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory($"tamper-workspace-{tamperMode}")),
                Cancellation));

        Assert.Equal(tampered, await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Fact]
    public async Task OpenAsync_SessionRejectsDmlAndLeavesSnapshotUnchanged()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync("read-only");
        byte[] snapshotBefore = await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation);

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("read-only-workspace")),
            Cancellation);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            async () =>
            {
                await using QueryResult ignored = await session.ExecuteReadAsync(
                    "INSERT INTO items VALUES (2, 'must-not-write')",
                    Cancellation);
            });

        Assert.Contains("read-only", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(snapshotBefore, await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Fact]
    public async Task Snapshot_RemainsUsableAfterSourceMutationAndDeletion_AndAcrossRepeatedReopens()
    {
        string sourcePath = PathInRoot("independent-source.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        string snapshotPath = PathInRoot("independent-snapshot.db");
        RetainedDatabaseSnapshotReceipt receipt = await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            snapshotPath,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("independent-capture-workspace")),
            Cancellation);

        await File.WriteAllBytesAsync(sourcePath, [0xDE, 0xAD, 0xBE, 0xEF], Cancellation);
        File.Delete(sourcePath);
        if (File.Exists(sourcePath + ".wal"))
            File.Delete(sourcePath + ".wal");

        Assert.False(File.Exists(sourcePath));
        for (int reopen = 0; reopen < 2; reopen++)
        {
            await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
                snapshotPath,
                receipt.Identity,
                databaseOptions: null,
                SnapshotOptions(CreateDirectory($"independent-open-workspace-{reopen}")),
                Cancellation);

            await AssertItemAsync(session, expectedId: 1, expectedValue: "checkpointed");
            Assert.False(File.Exists(snapshotPath + ".wal"));
        }

        Assert.True(File.Exists(snapshotPath));
        Assert.False(File.Exists(snapshotPath + ".wal"));
    }

    [Fact]
    public async Task OpenTableReader_StreamsPhysicalRowsAscending_AndResumesAfterExclusiveBoundary()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureConfiguredSnapshotAsync(
            "ordered-reader",
            async database =>
            {
                await database.ExecuteAsync(
                    "CREATE TABLE ordered_items (id INTEGER PRIMARY KEY, note TEXT, score REAL NOT NULL, payload BLOB)",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO ordered_items VALUES (7, NULL, 7.5, X'0708')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO ordered_items VALUES (-2, '', -2.25, X'00')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO ordered_items VALUES (4, 'four', 4.25, NULL)",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO ordered_items VALUES (5, 'deleted', 5.25, X'05')",
                    Cancellation);
                await database.ExecuteAsync(
                    "DELETE FROM ordered_items WHERE id = 5",
                    Cancellation);
            });
        byte[] retainedBefore = await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation);

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("ordered-reader-open-workspace")),
            Cancellation);

        await using (RetainedDatabaseSnapshotTableReader reader =
                     session.OpenTableReader("ordered_items"))
        {
            Assert.Equal("ordered_items", reader.TableName);
            Assert.Equal(
                ["id", "note", "score", "payload"],
                reader.Columns.Select(static column => column.Name));
            Assert.Equal(
                [DbType.Integer, DbType.Text, DbType.Real, DbType.Blob],
                reader.Columns.Select(static column => column.Type));
            var mutableColumns = Assert.IsAssignableFrom<ICollection<ColumnDefinition>>(
                reader.Columns);
            Assert.True(mutableColumns.IsReadOnly);
            Assert.Throws<NotSupportedException>(
                () => mutableColumns.Add(
                    new ColumnDefinition
                    {
                        Name = "injected",
                        Type = DbType.Text,
                    }));
            Assert.Throws<InvalidOperationException>(() => reader.CurrentRowId);
            Assert.Throws<InvalidOperationException>(() => reader.Current);

            Assert.True(await reader.MoveNextAsync(Cancellation));
            Assert.Equal(-2, reader.CurrentRowId);
            Assert.Equal(-2, reader.Current.Span[0].AsInteger);
            Assert.Equal(string.Empty, reader.Current.Span[1].AsText);
            Assert.Equal(-2.25, reader.Current.Span[2].AsReal);
            Assert.Equal([0x00], reader.Current.Span[3].AsBlob);

            Assert.True(await reader.MoveNextAsync(Cancellation));
            Assert.Equal(4, reader.CurrentRowId);
            Assert.Equal("four", reader.Current.Span[1].AsText);
            Assert.True(reader.Current.Span[3].IsNull);

            Assert.True(await reader.MoveNextAsync(Cancellation));
            Assert.Equal(7, reader.CurrentRowId);
            Assert.True(reader.Current.Span[1].IsNull);
            Assert.Equal([0x07, 0x08], reader.Current.Span[3].AsBlob);

            Assert.False(await reader.MoveNextAsync(Cancellation));
            Assert.False(await reader.MoveNextAsync(Cancellation));
            Assert.Throws<InvalidOperationException>(() => reader.Current);
        }

        Assert.Equal(
            [4L, 7L],
            (await ReadPhysicalRowsAsync(session, "ordered_items", afterRowIdExclusive: -2))
            .Select(static row => row.RowId));
        Assert.Equal(
            [7L],
            (await ReadPhysicalRowsAsync(session, "ordered_items", afterRowIdExclusive: 5))
            .Select(static row => row.RowId));
        Assert.Empty(
            await ReadPhysicalRowsAsync(
                session,
                "ordered_items",
                afterRowIdExclusive: long.MaxValue));

        Assert.Equal(retainedBefore, await File.ReadAllBytesAsync(receipt.SnapshotPath, Cancellation));
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    [Fact]
    public async Task OpenTableReader_UsesOneActiveReadGate_AndCancellationReleasesIt()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureSimpleSnapshotAsync(
            "reader-lifecycle");
        var blockingRead = new OneShotBlockingReadInterceptor();
        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            CreateDatabaseOptions(FileShare.Read, interceptors: [blockingRead]),
            SnapshotOptions(CreateDirectory("reader-lifecycle-open-workspace")),
            Cancellation);

        RetainedDatabaseSnapshotTableReader reader = session.OpenTableReader("items");
        Assert.Throws<InvalidOperationException>(
            () => session.OpenTableReader("items"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await using QueryResult ignored = await session.ExecuteReadAsync(
                    "SELECT COUNT(*) FROM items",
                    Cancellation);
            });
        await reader.DisposeAsync();
        await reader.DisposeAsync();

        await using (QueryResult query = await session.ExecuteReadAsync(
                         "SELECT COUNT(*) FROM items",
                         Cancellation))
        {
            Assert.Throws<InvalidOperationException>(
                () => session.OpenTableReader("items"));
        }

        reader = session.OpenTableReader("items");
        using (var cancellation = new CancellationTokenSource())
        {
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await reader.MoveNextAsync(cancellation.Token));
        }
        Assert.Throws<ObjectDisposedException>(() => reader.Current);
        await reader.DisposeAsync();

        await using (QueryResult afterCancellation = await session.ExecuteReadAsync(
                         "SELECT COUNT(*) FROM items",
                         Cancellation))
        {
            DbValue[] row = Assert.Single(await afterCancellation.ToListAsync(Cancellation));
            Assert.Equal(1, row[0].AsInteger);
        }

        reader = session.OpenTableReader("items", afterRowIdExclusive: 0);
        blockingRead.Arm();
        using (var cancellation = new CancellationTokenSource())
        {
            Task<bool> move = reader.MoveNextAsync(cancellation.Token).AsTask();
            await blockingRead.WaitForReadStartAsync(Cancellation);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await move);
        }
        Assert.Throws<ObjectDisposedException>(() => reader.Current);
        await reader.DisposeAsync();

        await using (QueryResult afterInFlightCancellation = await session.ExecuteReadAsync(
                         "SELECT COUNT(*) FROM items",
                         Cancellation))
        {
            DbValue[] row = Assert.Single(
                await afterInFlightCancellation.ToListAsync(Cancellation));
            Assert.Equal(1, row[0].AsInteger);
        }

        reader = session.OpenTableReader("items");
        Assert.True(await reader.MoveNextAsync(Cancellation));
        await session.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => reader.Current);
        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await reader.MoveNextAsync(Cancellation));
        await reader.DisposeAsync();

        var ownerBlockingRead = new OneShotBlockingReadInterceptor();
        await using RetainedDatabaseSnapshotSession ownerSession =
            await RetainedDatabaseSnapshot.OpenAsync(
                receipt.SnapshotPath,
                receipt.Identity,
                CreateDatabaseOptions(FileShare.Read, interceptors: [ownerBlockingRead]),
                SnapshotOptions(CreateDirectory("reader-owner-disposal-open-workspace")),
                Cancellation);
        RetainedDatabaseSnapshotTableReader ownerReader =
            ownerSession.OpenTableReader("items", afterRowIdExclusive: 0);
        ownerBlockingRead.Arm();
        Task<bool> blockedMove = ownerReader.MoveNextAsync(Cancellation).AsTask();
        await ownerBlockingRead.WaitForReadStartAsync(Cancellation);

        Task ownerDisposal = ownerSession.DisposeAsync().AsTask();
        Assert.False(ownerDisposal.IsCompleted);
        ownerBlockingRead.Release();
        Assert.True(await blockedMove);
        await ownerDisposal;
        Assert.Throws<ObjectDisposedException>(() => ownerReader.Current);
        await ownerReader.DisposeAsync();
    }

    [Fact]
    public async Task OpenTableReader_RejectsNonLocalPhysicalSources_BeforeExternalArchiveRead()
    {
        string sourcePath = PathInRoot("reader-rejections-source.db");
        string archivePath = PathInRoot("reader-rejections-archive.csdbtable");
        await using (Database database = await Database.OpenAsync(
                         sourcePath,
                         CreateDatabaseOptions(FileShare.ReadWrite),
                         Cancellation))
        {
            await database.ExecuteAsync(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO items VALUES (1, 'local')",
                Cancellation);
            await database.ExecuteAsync(
                "CREATE VIEW item_view AS SELECT id, value FROM items",
                Cancellation);
            await database.ExecuteAsync(
                "CREATE TABLE sys_saved_queries (id INTEGER PRIMARY KEY, value TEXT)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO sys_saved_queries VALUES (1, 'reserved')",
                Cancellation);

            TableSchema schema = database.GetTableSchema("items")!;
            await using QueryResult rows = await database.ExecuteAsync(
                "SELECT id, value FROM items ORDER BY id",
                Cancellation);
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    await rows.ToListAsync(Cancellation),
                    Cancellation),
                Cancellation);
            string escapedArchivePath = archivePath.Replace("'", "''", StringComparison.Ordinal);
            await database.ExecuteAsync(
                $"CREATE EXTERNAL TABLE archived_items FROM '{escapedArchivePath}'",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }
        if (File.Exists(sourcePath + ".wal"))
            File.Delete(sourcePath + ".wal");

        RetainedDatabaseSnapshotReceipt receipt = await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            PathInRoot("reader-rejections-snapshot.db"),
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("reader-rejections-capture-workspace")),
            Cancellation);
        File.Delete(archivePath);

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("reader-rejections-open-workspace")),
            Cancellation);

        Assert.Throws<InvalidOperationException>(
            () => session.OpenTableReader("item_view"));
        Assert.Throws<InvalidOperationException>(
            () => session.OpenTableReader("sys.tables"));
        Assert.Throws<InvalidOperationException>(
            () => session.OpenTableReader("SYS_SAVED_QUERIES"));
        Assert.Throws<InvalidOperationException>(
            () => session.OpenTableReader("__external_tables"));
        CSharpDbException external = Assert.Throws<CSharpDbException>(
            () => session.OpenTableReader("archived_items"));
        Assert.Equal(ErrorCode.TableNotFound, external.Code);
        Assert.Contains("External tables", external.Message, StringComparison.Ordinal);
        CSharpDbException missing = Assert.Throws<CSharpDbException>(
            () => session.OpenTableReader("missing_items"));
        Assert.Equal(ErrorCode.TableNotFound, missing.Code);
        Assert.False(File.Exists(archivePath));

        await using RetainedDatabaseSnapshotTableReader local =
            session.OpenTableReader("items");
        Assert.True(await local.MoveNextAsync(Cancellation));
        Assert.Equal(1, local.CurrentRowId);
        Assert.Equal("local", local.Current.Span[1].AsText);
    }

    [Fact]
    public async Task OpenTableReader_RejectsOverWideEncodedRows_AndReleasesReadGate()
    {
        string sourcePath = PathInRoot("over-wide-reader-source.db");
        var serializerProvider = new OverWideSerializerProvider();
        DatabaseOptions databaseOptions = CreateDatabaseOptions(
            FileShare.ReadWrite,
            serializerProvider: serializerProvider);
        await using (Database database = await Database.OpenAsync(
                         sourcePath,
                         databaseOptions,
                         Cancellation))
        {
            await database.ExecuteAsync(
                "CREATE TABLE malformed_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO malformed_items VALUES (1, 'value')",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }
        if (File.Exists(sourcePath + ".wal"))
            File.Delete(sourcePath + ".wal");

        RetainedDatabaseSnapshotReceipt receipt = await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            PathInRoot("over-wide-reader-snapshot.db"),
            databaseOptions,
            SnapshotOptions(CreateDirectory("over-wide-reader-capture-workspace")),
            Cancellation);
        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions,
            SnapshotOptions(CreateDirectory("over-wide-reader-open-workspace")),
            Cancellation);

        RetainedDatabaseSnapshotTableReader reader =
            session.OpenTableReader("malformed_items");
        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await reader.MoveNextAsync(Cancellation));
        Assert.Contains("more values", error.Message, StringComparison.Ordinal);
        Assert.Throws<ObjectDisposedException>(() => reader.Current);

        await using QueryResult afterFailure = await session.ExecuteReadAsync(
            "SELECT COUNT(*) FROM malformed_items",
            Cancellation);
        DbValue[] count = Assert.Single(await afterFailure.ToListAsync(Cancellation));
        Assert.Equal(1, count[0].AsInteger);
    }

    [Fact]
    public async Task OpenTableReader_RejectsOversizedOverflowRowBeforeMaterialization()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureConfiguredSnapshotAsync(
            "oversized-reader-row",
            async database =>
            {
                await database.ExecuteAsync(
                    "CREATE TABLE oversized_items (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
                    Cancellation);
                InsertBatch batch = database.PrepareInsertBatch("oversized_items", initialCapacity: 1);
                batch.AddRow(
                    DbValue.FromInteger(1),
                    DbValue.FromBlob(new byte[2 * 1024 * 1024]));
                Assert.Equal(1, await batch.ExecuteAsync(Cancellation));
            });

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(
                CreateDirectory("oversized-reader-row-open-workspace"),
                maxEncodedRowBytes: 1024),
            Cancellation);
        RetainedDatabaseSnapshotTableReader reader =
            session.OpenTableReader("oversized_items");

        InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await reader.MoveNextAsync(Cancellation));
        Assert.Contains("1024-byte limit", error.Message, StringComparison.Ordinal);
        Assert.Throws<ObjectDisposedException>(() => reader.Current);

        await using QueryResult afterFailure = await session.ExecuteReadAsync(
            "SELECT COUNT(*) FROM oversized_items",
            Cancellation);
        DbValue[] count = Assert.Single(await afterFailure.ToListAsync(Cancellation));
        Assert.Equal(1, count[0].AsInteger);
    }

    [Fact]
    public async Task OpenTableReader_ImplicitRowIdsRemainStableAcrossFreshReopens()
    {
        RetainedDatabaseSnapshotReceipt receipt = await CaptureConfiguredSnapshotAsync(
            "implicit-rowids",
            async database =>
            {
                await database.ExecuteAsync(
                    "CREATE TABLE implicit_items (value TEXT NOT NULL)",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO implicit_items VALUES ('first')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO implicit_items VALUES ('second')",
                    Cancellation);
                await database.ExecuteAsync(
                    "INSERT INTO implicit_items VALUES ('third')",
                    Cancellation);
            });

        (long RowId, DbValue[] Values)[]? baseline = null;
        for (int reopen = 0; reopen < 2; reopen++)
        {
            await using RetainedDatabaseSnapshotSession session =
                await RetainedDatabaseSnapshot.OpenAsync(
                    receipt.SnapshotPath,
                    receipt.Identity,
                    databaseOptions: null,
                    SnapshotOptions(CreateDirectory($"implicit-rowids-open-workspace-{reopen}")),
                    Cancellation);
            (long RowId, DbValue[] Values)[] rows =
                (await ReadPhysicalRowsAsync(session, "implicit_items", null)).ToArray();

            Assert.Equal([1L, 2L, 3L], rows.Select(static row => row.RowId));
            Assert.Equal(
                ["first", "second", "third"],
                rows.Select(static row => row.Values[0].AsText));
            if (baseline is null)
            {
                baseline = rows;
            }
            else
            {
                Assert.Equal(
                    baseline.Select(static row => row.RowId),
                    rows.Select(static row => row.RowId));
                Assert.Equal(
                    baseline.Select(static row => row.Values[0].AsText),
                    rows.Select(static row => row.Values[0].AsText));
            }
        }
    }

    [Fact]
    public async Task OpenTableReader_FiftyThousandRows_ReusesOneForwardOnlyRowBuffer()
    {
        const int rowCount = 50_000;
        RetainedDatabaseSnapshotReceipt receipt = await CaptureConfiguredSnapshotAsync(
            "bounded-reader",
            async database =>
            {
                await database.ExecuteAsync(
                    "CREATE TABLE streamed_items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                    Cancellation);
                InsertBatch batch = database.PrepareInsertBatch(
                    "streamed_items",
                    initialCapacity: 512);
                await database.BeginTransactionAsync(Cancellation);
                for (int id = 1; id <= rowCount; id++)
                {
                    batch.AddRow(
                        DbValue.FromInteger(id),
                        DbValue.FromText($"value-{id}"));
                    if (id % 512 == 0)
                        Assert.True(await batch.ExecuteAsync(Cancellation) > 0);
                }
                if (rowCount % 512 != 0)
                    Assert.True(await batch.ExecuteAsync(Cancellation) > 0);
                await database.CommitAsync(Cancellation);
            });

        await using RetainedDatabaseSnapshotSession session = await RetainedDatabaseSnapshot.OpenAsync(
            receipt.SnapshotPath,
            receipt.Identity,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory("bounded-reader-open-workspace")),
            Cancellation);
        await using RetainedDatabaseSnapshotTableReader reader =
            session.OpenTableReader("streamed_items");

        DbValue[]? rowBuffer = null;
        int seen = 0;
        while (await reader.MoveNextAsync(Cancellation))
        {
            seen++;
            Assert.Equal(seen, reader.CurrentRowId);
            Assert.Equal(seen, reader.Current.Span[0].AsInteger);
            if (seen is 1 or rowCount)
                Assert.Equal($"value-{seen}", reader.Current.Span[1].AsText);

            Assert.True(
                MemoryMarshal.TryGetArray(
                    reader.Current,
                    out ArraySegment<DbValue> currentBuffer));
            rowBuffer ??= currentBuffer.Array;
            Assert.Same(rowBuffer, currentBuffer.Array);
        }

        Assert.Equal(rowCount, seen);
        Assert.False(File.Exists(receipt.SnapshotPath + ".wal"));
    }

    private async Task<RetainedDatabaseSnapshotReceipt> CaptureSimpleSnapshotAsync(string name)
    {
        string sourcePath = PathInRoot($"{name}-source.db");
        string destinationPath = PathInRoot($"{name}-snapshot.db");
        await CreateCheckpointedDatabaseWithoutWalAsync(sourcePath);
        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            destinationPath,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory($"{name}-capture-workspace")),
            Cancellation);
    }

    private async Task<RetainedDatabaseSnapshotReceipt> CaptureConfiguredSnapshotAsync(
        string name,
        Func<Database, Task> configure)
    {
        string sourcePath = PathInRoot($"{name}-source.db");
        string snapshotPath = PathInRoot($"{name}-snapshot.db");
        await using (Database database = await Database.OpenAsync(
                         sourcePath,
                         CreateDatabaseOptions(FileShare.ReadWrite),
                         Cancellation))
        {
            await configure(database);
            await database.CheckpointAsync(Cancellation);
        }
        if (File.Exists(sourcePath + ".wal"))
            File.Delete(sourcePath + ".wal");

        return await RetainedDatabaseSnapshot.CaptureAsync(
            sourcePath,
            snapshotPath,
            databaseOptions: null,
            SnapshotOptions(CreateDirectory($"{name}-capture-workspace")),
            Cancellation);
    }

    private static async Task<IReadOnlyList<(long RowId, DbValue[] Values)>> ReadPhysicalRowsAsync(
        RetainedDatabaseSnapshotSession session,
        string tableName,
        long? afterRowIdExclusive)
    {
        var rows = new List<(long RowId, DbValue[] Values)>();
        await using RetainedDatabaseSnapshotTableReader reader =
            session.OpenTableReader(tableName, afterRowIdExclusive);
        while (await reader.MoveNextAsync(TestContext.Current.CancellationToken))
        {
            rows.Add((reader.CurrentRowId, reader.Current.ToArray()));
        }
        return rows;
    }

    private async Task AssertLimitFailureAsync(
        string sourcePath,
        string destinationPath,
        RetainedDatabaseSnapshotOptions options)
    {
        await Assert.ThrowsAnyAsync<IOException>(
            async () => await RetainedDatabaseSnapshot.CaptureAsync(
                sourcePath,
                destinationPath,
                databaseOptions: null,
                options,
                Cancellation));
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(destinationPath + ".wal"));
    }

    private async Task CreateCheckpointedDatabaseWithoutWalAsync(string path)
    {
        await using (Database database = await Database.OpenAsync(
            path,
            CreateDatabaseOptions(FileShare.ReadWrite),
            Cancellation))
        {
            await database.ExecuteAsync(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                Cancellation);
            await database.ExecuteAsync(
                "INSERT INTO items VALUES (1, 'checkpointed')",
                Cancellation);
            await database.CheckpointAsync(Cancellation);
        }

        if (File.Exists(path + ".wal"))
            File.Delete(path + ".wal");
    }

    private async Task<OfflineWalFixture> CreateOfflineWalFixtureAsync(string name)
    {
        string livePath = PathInRoot($"{name}-live.db");
        string sourcePath = PathInRoot($"{name}-offline.db");
        string baseOnlyPath = PathInRoot($"{name}-base-only.db");

        await using (Database writer = await Database.OpenAsync(
            livePath,
            CreateDatabaseOptions(FileShare.ReadWrite),
            Cancellation))
        {
            await writer.ExecuteAsync(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                Cancellation);
            await writer.CheckpointAsync(Cancellation);

            byte[] baseDatabaseBytes = await ReadAllBytesSharedAsync(livePath);
            await File.WriteAllBytesAsync(baseOnlyPath, baseDatabaseBytes, Cancellation);

            await writer.ExecuteAsync(
                "INSERT INTO items VALUES (7, 'committed-in-wal')",
                Cancellation);

            string liveWalPath = livePath + ".wal";
            Assert.True(File.Exists(liveWalPath));
            Assert.True(new FileInfo(liveWalPath).Length > PageConstants.WalHeaderSize);
            Assert.Equal(baseDatabaseBytes, await ReadAllBytesSharedAsync(livePath));

            await File.WriteAllBytesAsync(sourcePath, baseDatabaseBytes, Cancellation);
            await File.WriteAllBytesAsync(
                sourcePath + ".wal",
                await ReadAllBytesSharedAsync(liveWalPath),
                Cancellation);
        }

        Assert.Equal(
            await File.ReadAllBytesAsync(baseOnlyPath, Cancellation),
            await File.ReadAllBytesAsync(sourcePath, Cancellation));

        return new OfflineWalFixture(sourcePath, baseOnlyPath);
    }

    private async Task<byte[]> ReadAllBytesSharedAsync(string path)
    {
        await using var source = new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
            });
        if (source.Length > int.MaxValue)
            throw new IOException("Test fixture is unexpectedly large.");

        byte[] bytes = new byte[(int)source.Length];
        await source.ReadExactlyAsync(bytes, Cancellation);
        return bytes;
    }

    private async Task AssertItemAsync(
        RetainedDatabaseSnapshotSession session,
        long expectedId,
        string expectedValue)
    {
        await using QueryResult result = await session.ExecuteReadAsync(
            "SELECT id, value FROM items ORDER BY id",
            Cancellation);
        var rows = await result.ToListAsync(Cancellation);
        Assert.Single(rows);
        Assert.Equal(expectedId, rows[0][0].AsInteger);
        Assert.Equal(expectedValue, rows[0][1].AsText);
    }

    private RetainedDatabaseSnapshotOptions SnapshotOptions(
        string workspacePath,
        int maxEncodedRowBytes = RetainedDatabaseSnapshotOptions.DefaultMaxEncodedRowBytes)
        => new()
        {
            WorkspacePath = workspacePath,
            CopyBufferBytes = 4 * 1024,
            MaxCachedPages = 4,
            MaxCachedWalReadPages = 2,
            MaxEncodedRowBytes = maxEncodedRowBytes,
        };

    private static DatabaseOptions CreateDatabaseOptions(
        FileShare primaryFileShare,
        IReadOnlyList<IPageOperationInterceptor>? interceptors = null,
        ISerializerProvider? serializerProvider = null)
        => new()
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PrimaryFileShare = primaryFileShare,
                SerializerProvider = serializerProvider ?? new DefaultSerializerProvider(),
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
                    Interceptors = interceptors ?? Array.Empty<IPageOperationInterceptor>(),
                },
            },
        };

    private static string CreateSnapshotIdentity(long byteLength, string sha256) =>
        $"csharpdb-retained-snapshot/v1:{byteLength}:{sha256}";

    private string PathInRoot(string name) => Path.Combine(_root, name);

    private string CreateDirectory(string name)
    {
        string path = PathInRoot(name);
        Directory.CreateDirectory(path);
        return path;
    }

    private string[] EnumerateRelativeFiles() =>
        Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(_root, path))
            .Order(StringComparer.Ordinal)
            .ToArray();

    private sealed class OneShotBlockingReadInterceptor : IPageOperationInterceptor
    {
        private readonly TaskCompletionSource<bool> _readStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _allowRead =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public void Arm() => Volatile.Write(ref _armed, 1);

        public void Release() => _allowRead.TrySetResult(true);

        public Task WaitForReadStartAsync(CancellationToken ct) =>
            _readStarted.Task.WaitAsync(ct);

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
        {
            if (Interlocked.CompareExchange(ref _armed, 0, 1) == 0)
                return ValueTask.CompletedTask;

            _readStarted.TrySetResult(true);
            return new ValueTask(_allowRead.Task.WaitAsync(ct));
        }

        public ValueTask OnAfterReadAsync(
            uint pageId,
            PageReadSource source,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) =>
            ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(
            uint pageId,
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitStartAsync(
            int dirtyPageCount,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitEndAsync(
            int dirtyPageCount,
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCheckpointStartAsync(
            int committedFrameCount,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCheckpointEndAsync(
            int committedFrameCount,
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) =>
            ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;
    }

    private sealed class OverWideSerializerProvider : ISerializerProvider
    {
        public IRecordSerializer RecordSerializer { get; } = new OverWideRecordSerializer();

        public ISchemaSerializer SchemaSerializer { get; } = new DefaultSchemaSerializer();
    }

    private sealed class OverWideRecordSerializer : IRecordSerializer
    {
        private readonly IRecordSerializer _inner = new DefaultRecordSerializer();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            var widened = new DbValue[values.Length + 1];
            values.CopyTo(widened);
            widened[^1] = DbValue.Null;
            return _inner.Encode(widened);
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(buffer);

        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination) =>
            _inner.DecodeInto(buffer, destination);

        public void DecodeSelectedInto(
            ReadOnlySpan<byte> buffer,
            Span<DbValue> destination,
            ReadOnlySpan<int> selectedColumnIndices) =>
            _inner.DecodeSelectedInto(buffer, destination, selectedColumnIndices);

        public void DecodeSelectedCompactInto(
            ReadOnlySpan<byte> buffer,
            Span<DbValue> destination,
            ReadOnlySpan<int> selectedColumnIndices) =>
            _inner.DecodeSelectedCompactInto(buffer, destination, selectedColumnIndices);

        public DbValue[] DecodeUpTo(
            ReadOnlySpan<byte> buffer,
            int maxColumnIndexInclusive) =>
            _inner.DecodeUpTo(buffer, maxColumnIndexInclusive);

        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex) =>
            _inner.DecodeColumn(buffer, columnIndex);

        public bool TryColumnTextEquals(
            ReadOnlySpan<byte> buffer,
            int columnIndex,
            ReadOnlySpan<byte> expectedUtf8,
            out bool equals) =>
            _inner.TryColumnTextEquals(buffer, columnIndex, expectedUtf8, out equals);

        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex) =>
            _inner.IsColumnNull(buffer, columnIndex);

        public bool TryDecodeNumericColumn(
            ReadOnlySpan<byte> buffer,
            int columnIndex,
            out long intValue,
            out double realValue,
            out bool isReal) =>
            _inner.TryDecodeNumericColumn(
                buffer,
                columnIndex,
                out intValue,
                out realValue,
                out isReal);
    }

    private sealed record OfflineWalFixture(string SourcePath, string BaseOnlyPath);

    private static bool TryCreateFileSymbolicLink(string linkPath, string targetPath)
    {
        try
        {
            File.CreateSymbolicLink(linkPath, targetPath);
            return true;
        }
        catch (Exception error) when (
            error is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return false;
        }
    }

    private static bool TryCreateDirectorySymbolicLink(string linkPath, string targetPath)
    {
        try
        {
            Directory.CreateSymbolicLink(linkPath, targetPath);
            return true;
        }
        catch (Exception error) when (
            error is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return false;
        }
    }

    private static bool TryCreateHardLink(string linkPath, string targetPath)
    {
        if (OperatingSystem.IsWindows())
            return CreateHardLinkWindows(linkPath, targetPath, IntPtr.Zero);
        if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            return CreateHardLinkUnix(targetPath, linkPath) == 0;
        return false;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, EntryPoint = "CreateHardLinkW")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkWindows(
        string fileName,
        string existingFileName,
        IntPtr securityAttributes);

    [DllImport("libc", SetLastError = true, EntryPoint = "link")]
    private static extern int CreateHardLinkUnix(string existingPath, string newPath);
}
