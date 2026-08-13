using System.Text;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Tests;

public sealed class DatabaseRestoreSafetyTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public void PublicRestore_MethodGroupSignatureRemainsCompatible()
    {
        Func<string, string, Func<CancellationToken, ValueTask>,
            CancellationToken, ValueTask<DatabaseRestoreResult>> restore =
                DatabaseBackupCoordinator.RestoreAsync;

        GC.KeepAlive(restore);
    }

    [Fact]
    public async Task PublicRestore_CancellationAtFinalGate_DoesNotMutateDestination()
    {
        string sourcePath = NewPath("source");
        string destinationPath = NewPath("destination");
        using var callerCancellation = new CancellationTokenSource();
        int releaseCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");
            await CreateSnapshotAsync(destinationPath, "original");

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => DatabaseBackupCoordinator.RestoreAsync(
                    sourcePath,
                    destinationPath,
                    token =>
                    {
                        Assert.False(token.IsCancellationRequested);
                        releaseCount++;
                        callerCancellation.Cancel();
                        return ValueTask.CompletedTask;
                    },
                    callerCancellation.Token).AsTask());

            Assert.Equal(1, releaseCount);
            Assert.Equal("original", await ReadMarkerAsync(destinationPath, Ct));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    public async Task ClientRestore_PrePonrFailure_ReopensOnlyDetachedOriginal(
        bool reopenUnchangedDestinationOnPrePonr,
        int expectedReopenCount)
    {
        string missingSourcePath = NewPath("missing-source");
        string destinationPath = NewPath("destination");
        int reopenCount = 0;

        try
        {
            await CreateSnapshotAsync(destinationPath, "original");

            await Assert.ThrowsAsync<FileNotFoundException>(
                () => DatabaseBackupCoordinator.RestoreFromClientAsync(
                    missingSourcePath,
                    destinationPath,
                    async token =>
                    {
                        Assert.Equal(CancellationToken.None, token);
                        reopenCount++;
                        Assert.Equal(
                            "original",
                            await ReadMarkerAsync(
                                destinationPath,
                                CancellationToken.None));
                    },
                    reopenUnchangedDestinationOnPrePonr,
                    observation: null,
                    Ct).AsTask());

            Assert.Equal(expectedReopenCount, reopenCount);
            Assert.Equal("original", await ReadMarkerAsync(destinationPath, Ct));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Fact]
    public async Task ClientRestore_AfterPonr_IgnoresCancellationAndWaitsForAdoption()
    {
        string sourcePath = NewPath("source");
        string destinationPath = NewPath("destination");
        using var callerCancellation = new CancellationTokenSource();
        var callbackEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowAdoption = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int reopenCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");
            await CreateSnapshotAsync(destinationPath, "original");

            Task<DatabaseRestoreResult> restore =
                DatabaseBackupCoordinator.RestoreFromClientAsync(
                    sourcePath,
                    destinationPath,
                    async token =>
                    {
                        Assert.Equal(CancellationToken.None, token);
                        reopenCount++;
                        callerCancellation.Cancel();
                        callbackEntered.TrySetResult();
                        await allowAdoption.Task;
                        Assert.Equal(
                            "replacement",
                            await ReadMarkerAsync(
                                destinationPath,
                                CancellationToken.None));
                    },
                    reopenUnchangedDestinationOnPrePonr: true,
                    observation: null,
                    callerCancellation.Token).AsTask();

            await callbackEntered.Task.WaitAsync(Ct);
            bool completedBeforeAdoption = restore.IsCompleted;
            allowAdoption.TrySetResult();

            DatabaseRestoreResult result = await restore.WaitAsync(Ct);

            Assert.False(completedBeforeAdoption);
            Assert.True(callerCancellation.IsCancellationRequested);
            Assert.Equal(1, reopenCount);
            Assert.False(result.ValidateOnly);
            Assert.Equal(
                Path.GetFullPath(destinationPath),
                result.DestinationPath);
            Assert.Equal("replacement", await ReadMarkerAsync(destinationPath, Ct));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            allowAdoption.TrySetResult();
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Fact]
    public async Task ClientRestore_ReplacementReopenFailure_RollsBackDatabaseAndWal()
    {
        string sourcePath = NewPath("source");
        string destinationPath = NewPath("destination");
        byte[] originalDatabase = Encoding.UTF8.GetBytes("original-database");
        byte[] originalWal = Encoding.UTF8.GetBytes("original-write-ahead-log");
        var replacementFailure = new IOException("replacement-open-failure");
        int reopenCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");
            await File.WriteAllBytesAsync(destinationPath, originalDatabase, Ct);
            await File.WriteAllBytesAsync(
                destinationPath + ".wal",
                originalWal,
                Ct);

            IOException thrown = await Assert.ThrowsAsync<IOException>(
                () => DatabaseBackupCoordinator.RestoreFromClientAsync(
                    sourcePath,
                    destinationPath,
                    async token =>
                    {
                        Assert.Equal(CancellationToken.None, token);
                        reopenCount++;
                        if (reopenCount == 1)
                            throw replacementFailure;

                        Assert.Equal(
                            originalDatabase,
                            await File.ReadAllBytesAsync(
                                destinationPath,
                                CancellationToken.None));
                        Assert.Equal(
                            originalWal,
                            await File.ReadAllBytesAsync(
                                destinationPath + ".wal",
                                CancellationToken.None));
                    },
                    reopenUnchangedDestinationOnPrePonr: true,
                    observation: null,
                    Ct).AsTask());

            Assert.Same(replacementFailure, thrown);
            Assert.Equal(2, reopenCount);
            Assert.Equal(
                originalDatabase,
                await File.ReadAllBytesAsync(destinationPath, Ct));
            Assert.Equal(
                originalWal,
                await File.ReadAllBytesAsync(destinationPath + ".wal", Ct));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Fact]
    public async Task ClientRestore_OriginalReopenFailure_RetainsPairedBackups()
    {
        string sourcePath = NewPath("source-private");
        string destinationPath = NewPath("destination-private");
        byte[] originalDatabase = Encoding.UTF8.GetBytes("original-database");
        byte[] originalWal = Encoding.UTF8.GetBytes("original-write-ahead-log");
        var replacementFailure = new IOException("replacement-private-failure");
        var originalReopenFailure = new IOException("original-private-failure");
        int reopenCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");
            await File.WriteAllBytesAsync(destinationPath, originalDatabase, Ct);
            await File.WriteAllBytesAsync(
                destinationPath + ".wal",
                originalWal,
                Ct);

            InvalidOperationException thrown =
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => DatabaseBackupCoordinator.RestoreFromClientAsync(
                        sourcePath,
                        destinationPath,
                        token =>
                        {
                            Assert.Equal(CancellationToken.None, token);
                            reopenCount++;
                            return ValueTask.FromException(
                                reopenCount == 1
                                    ? replacementFailure
                                    : originalReopenFailure);
                        },
                        reopenUnchangedDestinationOnPrePonr: true,
                        observation: null,
                        Ct).AsTask());

            Assert.Equal(
                "Restore failed and the original destination could not be restored to a usable state.",
                thrown.Message);
            AggregateException aggregate = Assert.IsType<AggregateException>(
                thrown.InnerException);
            Assert.Collection(
                aggregate.InnerExceptions,
                failure => Assert.Same(replacementFailure, failure),
                failure => Assert.Same(originalReopenFailure, failure));
            Assert.Equal(2, reopenCount);

            Assert.Equal(
                originalDatabase,
                await File.ReadAllBytesAsync(destinationPath, Ct));
            Assert.Equal(
                originalWal,
                await File.ReadAllBytesAsync(destinationPath + ".wal", Ct));

            string backupPath = Assert.Single(GetBackupDatabaseFiles(
                destinationPath));
            Assert.Equal(
                originalDatabase,
                await File.ReadAllBytesAsync(backupPath, Ct));
            Assert.Equal(
                originalWal,
                await File.ReadAllBytesAsync(backupPath + ".wal", Ct));

            Assert.DoesNotContain(sourcePath, thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(destinationPath, thrown.Message, StringComparison.Ordinal);
            SafeErrorProjection safeError = SafeErrorProjector.Project(thrown);
            string safeProjection = string.Join(
                '|',
                safeError.Code,
                safeError.ErrorType,
                safeError.PublicDetail);
            Assert.DoesNotContain(sourcePath, safeProjection, StringComparison.Ordinal);
            Assert.DoesNotContain(destinationPath, safeProjection, StringComparison.Ordinal);
            Assert.DoesNotContain(
                "private-failure",
                safeProjection,
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Fact]
    public async Task ClientRestore_NoOriginalAndReopenFails_RemovesReplacement()
    {
        string sourcePath = NewPath("source");
        string destinationPath = NewPath("destination");
        var replacementFailure = new IOException("replacement-open-failure");
        int reopenCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");

            IOException thrown = await Assert.ThrowsAsync<IOException>(
                () => DatabaseBackupCoordinator.RestoreFromClientAsync(
                    sourcePath,
                    destinationPath,
                    token =>
                    {
                        Assert.Equal(CancellationToken.None, token);
                        reopenCount++;
                        return ValueTask.FromException(replacementFailure);
                    },
                    reopenUnchangedDestinationOnPrePonr: true,
                    observation: null,
                    Ct).AsTask());

            Assert.Same(replacementFailure, thrown);
            Assert.Equal(1, reopenCount);
            Assert.False(File.Exists(destinationPath));
            Assert.False(File.Exists(destinationPath + ".wal"));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    [Fact]
    public async Task PublicRestore_NoOriginal_ReopensAndValidatesBeforeSuccess()
    {
        string sourcePath = NewPath("source");
        string destinationPath = NewPath("destination");
        int releaseCount = 0;

        try
        {
            await CreateSnapshotAsync(sourcePath, "replacement");

            DatabaseRestoreResult result =
                await DatabaseBackupCoordinator.RestoreAsync(
                    sourcePath,
                    destinationPath,
                    _ =>
                    {
                        releaseCount++;
                        return ValueTask.CompletedTask;
                    },
                    Ct);

            Assert.Equal(1, releaseCount);
            Assert.False(result.ValidateOnly);
            Assert.Equal(
                Path.GetFullPath(destinationPath),
                result.DestinationPath);
            Assert.Equal("replacement", await ReadMarkerAsync(destinationPath, Ct));
            AssertNoRestoreArtifacts(destinationPath);
        }
        finally
        {
            DeleteRestoreFiles(sourcePath);
            DeleteRestoreFiles(destinationPath);
        }
    }

    private static async ValueTask CreateSnapshotAsync(
        string path,
        string marker)
    {
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await using (var create = await database.ExecuteAsync(
                         "CREATE TABLE restore_marker (id INTEGER PRIMARY KEY, value TEXT)",
                         Ct))
        {
        }

        await using (var insert = await database.ExecuteAsync(
                         $"INSERT INTO restore_marker VALUES (1, '{marker}')",
                         Ct))
        {
        }

        await database.SaveToFileAsync(path, Ct);
    }

    private static async ValueTask<string> ReadMarkerAsync(
        string path,
        CancellationToken ct)
    {
        await using Database database = await Database.OpenAsync(path, ct);
        await using var result = await database.ExecuteAsync(
            "SELECT value FROM restore_marker WHERE id = 1",
            ct);
        var rows = await result.ToListAsync(ct);
        return Assert.Single(rows)[0].AsText;
    }

    private static string NewPath(string purpose)
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_restore_safety_{purpose}_{Guid.NewGuid():N}.db");

    private static IReadOnlyList<string> GetBackupDatabaseFiles(
        string destinationPath)
    {
        string directory = Path.GetDirectoryName(destinationPath) ??
            Environment.CurrentDirectory;
        string pattern = Path.GetFileName(destinationPath) +
            ".restorebak.*.tmp";
        return Directory.Exists(directory)
            ? Directory.GetFiles(directory, pattern)
            : [];
    }

    private static IReadOnlyList<string> GetRestoreArtifacts(
        string destinationPath)
    {
        string directory = Path.GetDirectoryName(destinationPath) ??
            Environment.CurrentDirectory;
        string pattern = Path.GetFileName(destinationPath) + ".restore*";
        return Directory.Exists(directory)
            ? Directory.GetFiles(directory, pattern)
            : [];
    }

    private static void AssertNoRestoreArtifacts(string destinationPath)
        => Assert.Empty(GetRestoreArtifacts(destinationPath));

    private static void DeleteRestoreFiles(string path)
    {
        TryDelete(path);
        TryDelete(path + ".wal");
        foreach (string artifact in GetRestoreArtifacts(path))
            TryDelete(artifact);
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Test cleanup only.
        }
    }
}
