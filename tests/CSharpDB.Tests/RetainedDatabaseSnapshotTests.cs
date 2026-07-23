using System.Security.Cryptography;
using System.Runtime.InteropServices;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
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

    private RetainedDatabaseSnapshotOptions SnapshotOptions(string workspacePath) => new()
    {
        WorkspacePath = workspacePath,
        CopyBufferBytes = 4 * 1024,
        MaxCachedPages = 4,
        MaxCachedWalReadPages = 2,
    };

    private static DatabaseOptions CreateDatabaseOptions(FileShare primaryFileShare) => new()
    {
        StorageEngineOptions = new StorageEngineOptions
        {
            PrimaryFileShare = primaryFileShare,
            PagerOptions = new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
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
