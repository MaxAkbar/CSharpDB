using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class PagerSaveToFileProgressTests
{
    [Fact]
    public async Task SaveToFileAsync_WithCommittedWal_ReportsOrderedPhases()
    {
        var ct = TestContext.Current.CancellationToken;
        string snapshotPath = CreateSnapshotPath();

        try
        {
            await using Pager pager = await CreatePagerAsync(ct);
            (uint pageId, byte expectedValue) = await CommitDirtyPageAsync(pager, 0x5A, ct);
            var observer = new RecordingObserver();

            await pager.SaveToFileAsync(snapshotPath, ct, observer);

            Assert.Equal(
                [
                    PagerSaveToFilePhase.Checkpointing,
                    PagerSaveToFilePhase.Copying,
                    PagerSaveToFilePhase.Staging,
                ],
                observer.Phases);
            await AssertSnapshotPageAsync(snapshotPath, pageId, expectedValue, ct);
        }
        finally
        {
            DeleteSnapshotArtifacts(snapshotPath);
        }
    }

    [Fact]
    public async Task SaveToFileAsync_WithoutCheckpointWork_SkipsCheckpointing()
    {
        var ct = TestContext.Current.CancellationToken;
        string snapshotPath = CreateSnapshotPath();

        try
        {
            await using Pager pager = await CreatePagerAsync(ct);
            var observer = new RecordingObserver();

            await pager.SaveToFileAsync(snapshotPath, ct, observer);

            Assert.Equal(
                [PagerSaveToFilePhase.Copying, PagerSaveToFilePhase.Staging],
                observer.Phases);
            Assert.Equal(PageConstants.PageSize, new FileInfo(snapshotPath).Length);
        }
        finally
        {
            DeleteSnapshotArtifacts(snapshotPath);
        }
    }

    [Fact]
    public async Task SaveToFileAsync_WhenObserverThrows_StillPublishesSnapshot()
    {
        var ct = TestContext.Current.CancellationToken;
        string snapshotPath = CreateSnapshotPath();
        string controlPath = CreateSnapshotPath();

        try
        {
            await using Pager pager = await CreatePagerAsync(ct);
            (uint pageId, byte expectedValue) = await CommitDirtyPageAsync(pager, 0xA5, ct);
            var observer = new RecordingObserver(throwOnEveryPhase: true);

            await pager.SaveToFileAsync(snapshotPath, ct, observer);

            Assert.Equal(
                [
                    PagerSaveToFilePhase.Checkpointing,
                    PagerSaveToFilePhase.Copying,
                    PagerSaveToFilePhase.Staging,
                ],
                observer.Phases);
            await AssertSnapshotPageAsync(snapshotPath, pageId, expectedValue, ct);

            await pager.SaveToFileAsync(controlPath, ct, observer: null);
            Assert.Equal(
                await File.ReadAllBytesAsync(controlPath, ct),
                await File.ReadAllBytesAsync(snapshotPath, ct));
            Assert.Empty(GetTemporarySnapshotFiles(snapshotPath));
        }
        finally
        {
            DeleteSnapshotArtifacts(snapshotPath);
            DeleteSnapshotArtifacts(controlPath);
        }
    }

    [Fact]
    public async Task BackgroundCheckpointWait_ReportsCheckpointingBeforeCompletion()
    {
        var ct = TestContext.Current.CancellationToken;
        using var coordinator = new CheckpointCoordinator();
        var checkpointStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCheckpoint = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observer = new RecordingObserver();

        coordinator.RequestDeferredCheckpoint();
        Assert.True(coordinator.TryStartBackgroundCheckpoint(async _ =>
        {
            checkpointStarted.SetResult();
            await releaseCheckpoint.Task;
        }));
        await checkpointStarted.Task.WaitAsync(ct);

        Task<bool> waitTask = coordinator
            .WaitForBackgroundCheckpointWithProgressAsync(observer, ct)
            .AsTask();
        try
        {
            Assert.Equal([PagerSaveToFilePhase.Checkpointing], observer.Phases);
            Assert.False(waitTask.IsCompleted);
        }
        finally
        {
            releaseCheckpoint.TrySetResult();
        }

        Assert.True(await waitTask);
    }

    private static async ValueTask<Pager> CreatePagerAsync(CancellationToken ct)
    {
        var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        var wal = new MemoryWriteAheadLog(walIndex);
        Pager? pager = null;

        try
        {
            pager = await Pager.CreateAsync(
                device,
                wal,
                walIndex,
                new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(10_000),
                },
                ct);
            await pager.InitializeNewDatabaseAsync(ct);
            return pager;
        }
        catch
        {
            if (pager is not null)
                await pager.DisposeAsync();
            else
            {
                await wal.DisposeAsync();
                await device.DisposeAsync();
            }

            throw;
        }
    }

    private static async ValueTask<(uint PageId, byte Value)> CommitDirtyPageAsync(
        Pager pager,
        byte value,
        CancellationToken ct)
    {
        await pager.BeginTransactionAsync(ct);
        uint pageId = await pager.AllocatePageAsync(ct);
        byte[] page = await pager.GetPageAsync(pageId, ct);
        page[128] = value;
        await pager.MarkDirtyAsync(pageId, ct);
        await pager.CommitAsync(ct);
        return (pageId, value);
    }

    private static async ValueTask AssertSnapshotPageAsync(
        string snapshotPath,
        uint pageId,
        byte expectedValue,
        CancellationToken ct)
    {
        byte[] image = await File.ReadAllBytesAsync(snapshotPath, ct);
        int valueOffset = checked((int)(pageId * PageConstants.PageSize) + 128);
        Assert.InRange(valueOffset, 0, image.Length - 1);
        Assert.Equal(expectedValue, image[valueOffset]);
    }

    private static string CreateSnapshotPath()
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_pager_save_progress_{Guid.NewGuid():N}.db");

    private static IEnumerable<string> GetTemporarySnapshotFiles(string path)
    {
        string directory = Path.GetDirectoryName(path)!;
        string pattern = Path.GetFileName(path) + ".tmp.*";
        return Directory.EnumerateFiles(directory, pattern, SearchOption.TopDirectoryOnly);
    }

    private static void DeleteSnapshotArtifacts(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
            foreach (string tempPath in GetTemporarySnapshotFiles(path))
                File.Delete(tempPath);
        }
        catch
        {
            // Best-effort test cleanup.
        }
    }

    private sealed class RecordingObserver(bool throwOnEveryPhase = false)
        : IPagerSaveToFileProgressObserver
    {
        public List<PagerSaveToFilePhase> Phases { get; } = [];

        public void OnPhase(PagerSaveToFilePhase phase)
        {
            Phases.Add(phase);
            if (throwOnEveryPhase)
                throw new InvalidOperationException("observer failure");
        }
    }
}
