using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;
using System.Reflection;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class StorageRuntimeDiagnosticsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void FaultedCheckpointPhase_HasHighestPrecedenceAndProjectsFaulted()
    {
        Assert.True(
            StorageRuntimeDiagnostics.GetCheckpointPhaseRank(
                StorageCheckpointPhaseRaw.Faulted) >
            StorageRuntimeDiagnostics.GetCheckpointPhaseRank(
                StorageCheckpointPhaseRaw.Finalizing));
        Assert.Equal(
            CheckpointPhase.Faulted,
            StorageRuntimeDiagnostics.MapCheckpointPhase(
                StorageCheckpointPhaseRaw.Faulted));
    }

    [Fact]
    public void RawLifetimeCounterShape_DistinguishesNotApplicableFromUnknown()
    {
        var storage = new StorageRuntimeRawSnapshot(
            PageSize: 4096,
            PageCount: 1,
            LogicalBytes: 4096,
            AllocatedBytes: null,
            DirtyPageCount: 0,
            ActiveReaderCount: 0,
            ActiveWriterCount: 0);
        var memoryWal = new WalRuntimeRawSnapshot(
            LogicalBytes: 32,
            AllocatedBytes: null,
            FrameCount: 0,
            CommittedFrameBytes: 0,
            RetainedBytes: 0,
            PendingCommitCount: 0,
            CheckpointPhase: StorageCheckpointPhaseRaw.Idle,
            LogicalCommitCount: 0,
            CommitFlushBatchCount: null,
            CommittedFrameBytesWritten: null);

        Assert.True(StorageRuntimeDiagnostics.IsValid(new(storage, memoryWal)));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(storage, memoryWal with { LogicalCommitCount = null })));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage with { TerminalConflictCount = -1 },
                    memoryWal)));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage,
                    memoryWal with { LogicalPageWriteCount = -1 })));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(storage, memoryWal with { CommitFlushBatchCount = 0 })));

        WalRuntimeRawSnapshot fileWal = memoryWal with
        {
            AllocatedBytes = 32,
            CommitFlushBatchCount = 0,
            CommittedFrameBytesWritten = 0,
            FlushedCommitCount = 0,
            DurableFlushCount = 0,
            GroupCommitBatchCount = 0,
            GroupCommitCount = 0,
        };
        Assert.True(StorageRuntimeDiagnostics.IsValid(new(storage, fileWal)));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage,
                    fileWal with { CommittedFrameBytesWritten = null })));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage,
                    fileWal with { DurableFlushCount = null })));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage,
                    fileWal with
                    {
                        LogicalCommitCount = 1,
                        CommitFlushBatchCount = 1,
                        FlushedCommitCount = 2,
                    })));
        Assert.False(
            StorageRuntimeDiagnostics.IsValid(
                new(
                    storage,
                    fileWal with
                    {
                        CommitFlushBatchCount = 2,
                        FlushedCommitCount = 4,
                        GroupCommitBatchCount = 2,
                        GroupCommitCount = 3,
                    })));
    }

    [Fact]
    public void RawWalCapture_RejectsFlushedCommitCountAboveLogicalCommitCount()
    {
        var valid = new WalRuntimeRawCaptureState(
            LogicalBytes: PageConstants.WalHeaderSize +
                PageConstants.WalFrameSize,
            AllocatedBytes: PageConstants.WalHeaderSize +
                PageConstants.WalFrameSize,
            RetainedWalStartOffset: -1,
            PendingCommitCount: 0,
            FrameCount: 1,
            LogicalCommitCount: 1,
            LogicalPageWriteCount: 1,
            CommitFlushBatchCount: 1,
            CommittedFrameBytesWritten: PageConstants.WalFrameSize)
        {
            FlushedCommitCount = 1,
            DurableFlushCount = 1,
            GroupCommitBatchCount = 0,
            GroupCommitCount = 0,
        };

        Assert.True(valid.TryCreateSnapshot(out _));
        Assert.False(
            (valid with { FlushedCommitCount = 2 })
                .TryCreateSnapshot(out _));
    }

    [Fact]
    public async Task OpenSuccessPublication_SeesRegisteredStorageProvider()
    {
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-runtime-open-publication",
            Logging = new CSharpDbLoggingOptions { Enabled = true },
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var observer = new OpenCaptureObserver(state);
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.DatabaseOpened.Name);

        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = state,
            },
            Ct);

        StorageRuntimeDiagnosticsCapture capture = Assert.IsType<
            StorageRuntimeDiagnosticsCapture>(observer.Capture);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Wal.Availability);
    }

    [Fact]
    public async Task DirectSummary_AggregatesTwoLiveBuiltInHandlesAndUnregistersThem()
    {
        await using var client = new EngineTransportClient(
            $":memory:storage-runtime-{Guid.NewGuid():N}",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            CreateOptions("storage-runtime-multiple"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> beforeOpen =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            beforeOpen.Aggregate.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            beforeOpen.Aggregate.Wal.Availability);
        CSharpDbRuntimeDiagnosticsState beforeOpenState = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(client.CurrentRuntimeDiagnosticsState);
        Assert.False(
            beforeOpenState.TryGetComponent<StorageRuntimeDiagnostics>(out _));

        TransactionSessionInfo? first = null;
        TransactionSessionInfo? second = null;
        try
        {
            first = await client.BeginTransactionAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> mixedWriters =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(
                1,
                mixedWriters.Aggregate.Storage.Value!.ActiveWriters);

            second = await client.BeginTransactionAsync(Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            StorageRuntimeDiagnosticsSnapshot storage = Assert.IsType<
                StorageRuntimeDiagnosticsSnapshot>(runtime.Aggregate.Storage.Value);
            WalRuntimeDiagnosticsSnapshot wal = Assert.IsType<
                WalRuntimeDiagnosticsSnapshot>(runtime.Aggregate.Wal.Value);

            Assert.Equal(DiagnosticsAvailability.Available, runtime.Aggregate.Storage.Availability);
            Assert.Equal(DiagnosticsAvailability.Available, runtime.Aggregate.Wal.Availability);
            Assert.Equal(runtime.Metadata, storage.Metadata);
            Assert.Equal(runtime.Metadata, wal.Metadata);
            Assert.True(storage.PageCount > 0);
            Assert.True(storage.LogicalDatabaseBytes > 0);
            Assert.Null(storage.AllocatedDatabaseBytes);
            Assert.Equal(2, storage.ActiveWriters);
            Assert.NotNull(storage.CommitCount);
            Assert.NotNull(storage.PageWrites);
            Assert.Equal(
                storage.PageWrites * PageConstants.PageSize,
                storage.BytesWritten);
            Assert.Equal(0, storage.ConflictCount);
            Assert.True(wal.LogicalBytes > 0);
            Assert.Null(wal.AllocatedBytes);
            Assert.Null(wal.FlushCount);
            Assert.Null(wal.BytesWritten);
            Assert.Equal(0, wal.PendingCommitCount);
            Assert.Equal(CheckpointPhase.Idle, wal.CheckpointPhase);

            await client.RollbackTransactionAsync(first.TransactionId, Ct);
            first = null;
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> oneRemaining =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(
                1,
                oneRemaining.Aggregate.Storage.Value!.ActiveWriters);
        }
        finally
        {
            if (first is not null)
                await client.RollbackTransactionAsync(first.TransactionId, Ct);
            if (second is not null)
                await client.RollbackTransactionAsync(second.TransactionId, Ct);
        }

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> afterClose =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            afterClose.Aggregate.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            afterClose.Aggregate.Wal.Availability);
        Assert.NotNull(afterClose.Aggregate.Wal.Value);
    }

    [Fact]
    public async Task AggregateDirtyPages_IsUnknownWhenAnyHandleHasExplicitWriter()
    {
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-dirty-unknown");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new ThrowingStorageEngineFactory());
        await using Database writerDatabase =
            await Database.OpenInMemoryAsync(options, Ct);
        await using Database idleDatabase =
            await Database.OpenInMemoryAsync(options, Ct);
        await using var writer = await writerDatabase.BeginWriteTransactionAsync(Ct);

        StorageRuntimeDiagnosticsCapture capture = Capture(state);

        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Storage.Availability);
        Assert.Equal(1, capture.Storage.Value!.ActiveWriters);
        Assert.Null(capture.Storage.Value.DirtyPages);
    }

    [Fact]
    public async Task TerminalExplicitConflictCount_CountsPageKeyAndRangeThrowsOnly()
    {
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-terminal-conflicts");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new ThrowingStorageEngineFactory());
        await using Database database = await Database.OpenInMemoryAsync(
            options,
            Ct);

        await ExecuteStatementAsync(
            database,
            "CREATE TABLE conflict_pages (id INTEGER PRIMARY KEY, value INTEGER)");
        await ExecuteStatementAsync(
            database,
            "INSERT INTO conflict_pages VALUES (1, 10)");
        await ExecuteStatementAsync(
            database,
            "INSERT INTO conflict_pages VALUES (2, 20)");

        Assert.Equal(0L, GetConflictCount(state));

        // A resolved insert-only page overlap is not terminal.
        await using (WriteTransaction first =
                     await database.BeginWriteTransactionAsync(Ct))
        await using (WriteTransaction second =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await first.ExecuteAsync(
                "INSERT INTO conflict_pages VALUES (3, 30)",
                Ct);
            await second.ExecuteAsync(
                "INSERT INTO conflict_pages VALUES (4, 40)",
                Ct);
            await first.CommitAsync(Ct);
            await second.CommitAsync(Ct);
        }
        Assert.Equal(0L, GetConflictCount(state));

        // Non-insert page overlap cannot be rebased and terminates once.
        await using (WriteTransaction first =
                     await database.BeginWriteTransactionAsync(Ct))
        await using (WriteTransaction second =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await first.ExecuteAsync(
                "UPDATE conflict_pages SET value = 11 WHERE id = 1",
                Ct);
            await second.ExecuteAsync(
                "UPDATE conflict_pages SET value = 22 WHERE id = 2",
                Ct);
            await first.CommitAsync(Ct);
            long pageWritesAfterWinner = GetPageWriteCount(state);
            CSharpDbConflictException pageConflict =
                await Assert.ThrowsAsync<CSharpDbConflictException>(
                () => second.CommitAsync(Ct).AsTask());
            Assert.Contains("committing page", pageConflict.Message);
            Assert.Equal(pageWritesAfterWinner, GetPageWriteCount(state));
        }
        Assert.Equal(1L, GetConflictCount(state));

        // A read-only point lookup has no dirty page path, so its terminal
        // conflict is the logical-key boundary.
        await ExecuteStatementAsync(
            database,
            "CREATE TABLE conflict_keys (id INTEGER PRIMARY KEY, value INTEGER)");
        await ExecuteStatementAsync(
            database,
            "INSERT INTO conflict_keys VALUES (1, 10)");
        await using (WriteTransaction keyReader =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await using (QueryResult result = await keyReader.ExecuteAsync(
                             "SELECT value FROM conflict_keys WHERE id = 1",
                             Ct))
            {
                _ = await result.ToListAsync(Ct);
            }
            await ExecuteStatementAsync(
                database,
                "UPDATE conflict_keys SET value = 20 WHERE id = 1");
            CSharpDbConflictException keyConflict =
                await Assert.ThrowsAsync<CSharpDbConflictException>(
                () => keyReader.CommitAsync(Ct).AsTask());
            Assert.Contains("logical key", keyConflict.Message);
        }
        Assert.Equal(2L, GetConflictCount(state));

        // A read-only predicate scan likewise reaches the logical-range
        // terminal boundary without any page-write conflict.
        await ExecuteStatementAsync(
            database,
            "CREATE TABLE conflict_ranges (id INTEGER PRIMARY KEY, value INTEGER)");
        await ExecuteStatementAsync(
            database,
            "INSERT INTO conflict_ranges VALUES (1, 5)");
        await using (WriteTransaction rangeReader =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await using (QueryResult result = await rangeReader.ExecuteAsync(
                             "SELECT COUNT(*) FROM conflict_ranges WHERE value >= 10",
                             Ct))
            {
                _ = await result.ToListAsync(Ct);
            }
            await ExecuteStatementAsync(
                database,
                "INSERT INTO conflict_ranges VALUES (2, 20)");
            CSharpDbConflictException rangeConflict =
                await Assert.ThrowsAsync<CSharpDbConflictException>(
                () => rangeReader.CommitAsync(Ct).AsTask());
            Assert.Contains("logical range", rangeConflict.Message);
        }
        Assert.Equal(3L, GetConflictCount(state));

        await using (WriteTransaction canceled =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await canceled.ExecuteAsync(
                "UPDATE conflict_pages SET value = 31 WHERE id = 3",
                Ct);
            long pageWritesBeforeCancellation = GetPageWriteCount(state);
            using var canceledCommit = new CancellationTokenSource();
            canceledCommit.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => canceled.CommitAsync(canceledCommit.Token).AsTask());
            Assert.Equal(3L, GetConflictCount(state));
            Assert.Equal(
                pageWritesBeforeCancellation,
                GetPageWriteCount(state));
        }

        SetPrivateInt64Field(
            GetPager(database),
            "_terminalExplicitConflictCount",
            long.MaxValue);
        await using (WriteTransaction first =
                     await database.BeginWriteTransactionAsync(Ct))
        await using (WriteTransaction second =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await first.ExecuteAsync(
                "UPDATE conflict_pages SET value = 12 WHERE id = 1",
                Ct);
            await second.ExecuteAsync(
                "UPDATE conflict_pages SET value = 23 WHERE id = 2",
                Ct);
            await first.CommitAsync(Ct);
            await Assert.ThrowsAsync<CSharpDbConflictException>(
                () => second.CommitAsync(Ct).AsTask());
        }
        Assert.Equal(long.MaxValue, GetConflictCount(state));
    }

    [Fact]
    public async Task TerminalExplicitConflictCount_ExcludesInterceptorExceptions()
    {
        var interceptor = new ArmableConflictInterceptor();
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-interceptor-conflict");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
                state,
                new ThrowingStorageEngineFactory())
            .ConfigureStorageEngine(builder => builder.UsePagerOptions(
                new PagerOptions { Interceptors = [interceptor] }));
        await using Database database = await Database.OpenInMemoryAsync(
            options,
            Ct);
        await ExecuteStatementAsync(
            database,
            "CREATE TABLE interceptor_conflicts (id INTEGER PRIMARY KEY)");

        interceptor.Arm();
        await using WriteTransaction transaction =
            await database.BeginWriteTransactionAsync(Ct);
        await transaction.ExecuteAsync(
            "INSERT INTO interceptor_conflicts VALUES (1)",
            Ct);
        long pageWritesBeforeFailure = GetPageWriteCount(state);
        await Assert.ThrowsAsync<CSharpDbConflictException>(
            () => transaction.CommitAsync(Ct).AsTask());

        Assert.Equal(0L, GetConflictCount(state));
        Assert.Equal(pageWritesBeforeFailure, GetPageWriteCount(state));
    }

    [Fact]
    public async Task AggregateTwoBuiltInHandles_UsesMaximaSumsAndHighestPhase()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_runtime_aggregate_{Guid.NewGuid():N}.db");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-aggregate");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        Database? memory = null;
        Database? file = null;
        try
        {
            memory = await Database.OpenInMemoryAsync(options, Ct);
            await using (var result = await memory.ExecuteAsync(
                             "CREATE TABLE aggregate_probe (id INTEGER PRIMARY KEY)",
                             Ct))
            {
            }

            StorageRuntimeDiagnosticsCapture memoryOnly = Capture(state);
            file = await Database.CreateNewAsync(path, options, Ct);
            StorageRuntimeDiagnosticsCapture combined = Capture(state);

            await memory.DisposeAsync();
            memory = null;
            StorageRuntimeDiagnosticsCapture fileOnly = Capture(state);

            StorageRuntimeDiagnosticsSnapshot memoryStorage =
                memoryOnly.Storage.Value!;
            StorageRuntimeDiagnosticsSnapshot fileStorage =
                fileOnly.Storage.Value!;
            StorageRuntimeDiagnosticsSnapshot combinedStorage =
                combined.Storage.Value!;
            WalRuntimeDiagnosticsSnapshot memoryWal = memoryOnly.Wal.Value!;
            WalRuntimeDiagnosticsSnapshot fileWal = fileOnly.Wal.Value!;
            WalRuntimeDiagnosticsSnapshot combinedWal = combined.Wal.Value!;
            Assert.Equal(
                Math.Max(
                    memoryStorage.LogicalDatabaseBytes!.Value,
                    fileStorage.LogicalDatabaseBytes!.Value),
                combinedStorage.LogicalDatabaseBytes);
            Assert.Equal(
                Math.Max(
                    memoryStorage.PageCount!.Value,
                    fileStorage.PageCount!.Value),
                combinedStorage.PageCount);
            Assert.Equal(
                memoryStorage.DirtyPages!.Value + fileStorage.DirtyPages!.Value,
                combinedStorage.DirtyPages);
            Assert.Equal(
                memoryStorage.ActiveReaders!.Value +
                fileStorage.ActiveReaders!.Value,
                combinedStorage.ActiveReaders);
            Assert.Equal(
                memoryStorage.ActiveWriters!.Value +
                fileStorage.ActiveWriters!.Value,
                combinedStorage.ActiveWriters);
            Assert.Equal(
                fileStorage.AllocatedDatabaseBytes,
                combinedStorage.AllocatedDatabaseBytes);
            Assert.Equal(
                Math.Max(
                    memoryWal.LogicalBytes!.Value,
                    fileWal.LogicalBytes!.Value),
                combinedWal.LogicalBytes);
            Assert.Equal(
                Math.Max(
                    memoryWal.CommittedFrameBytes!.Value,
                    fileWal.CommittedFrameBytes!.Value),
                combinedWal.CommittedFrameBytes);
            Assert.Equal(
                Math.Max(
                    memoryWal.FrameCount!.Value,
                    fileWal.FrameCount!.Value),
                combinedWal.FrameCount);
            Assert.Equal(
                Math.Max(
                    memoryWal.RetainedBytes!.Value,
                    fileWal.RetainedBytes!.Value),
                combinedWal.RetainedBytes);
            Assert.Equal(
                memoryWal.PendingCommitCount!.Value +
                fileWal.PendingCommitCount!.Value,
                combinedWal.PendingCommitCount);
            Assert.Equal(
                fileWal.AllocatedBytes,
                combinedWal.AllocatedBytes);
            Assert.Equal(
                HigherPhase(
                    memoryWal.CheckpointPhase,
                    fileWal.CheckpointPhase),
                combinedWal.CheckpointPhase);
            Assert.NotNull(memoryStorage.CommitCount);
            Assert.NotNull(combinedStorage.CommitCount);
            Assert.True(
                combinedStorage.CommitCount >= memoryStorage.CommitCount);
            Assert.True(fileStorage.CommitCount >= combinedStorage.CommitCount);
            Assert.Equal(
                fileStorage.PageWrites,
                combinedStorage.PageWrites);
            Assert.Equal(
                fileStorage.ConflictCount,
                combinedStorage.ConflictCount);
            Assert.Equal(
                combinedStorage.PageWrites * PageConstants.PageSize,
                combinedStorage.BytesWritten);
            Assert.Null(memoryWal.FlushCount);
            Assert.Null(memoryWal.BytesWritten);
            Assert.Null(combinedWal.FlushCount);
            Assert.Null(combinedWal.BytesWritten);
            Assert.NotNull(fileWal.FlushCount);
            Assert.NotNull(fileWal.BytesWritten);
        }
        finally
        {
            if (memory is not null)
                await memory.DisposeAsync();
            if (file is not null)
                await file.DisposeAsync();
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task FileWal_GroupPublicationProjectsExactCountersAndMixedMemorySuppressesFileOnlyFields()
    {
        string path = NewDatabasePath("group-publication");
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 10, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-group-publication");
        using var state = new CSharpDbRuntimeDiagnosticsState(
            observability,
            clock);
        DatabaseOptions options = CreateOptions(
                state,
                new DefaultStorageEngineFactory())
            .ConfigureStorageEngine(
                builder => builder.UseDurableGroupCommit(
                    TimeSpan.FromMilliseconds(250)));
        Database? file = null;
        Database? memory = null;
        try
        {
            file = await Database.CreateNewAsync(path, options, Ct);
            StorageRuntimeDiagnosticsCapture baseline = Capture(state);
            WalRuntimeDiagnosticsSnapshot baselineWal = baseline.Wal.Value!;
            WriteAheadLog wal = GetFileWal(file);
            uint pageCount = GetPager(file).PageCount;
            var releaseBatchDelay = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            wal.DurableCommitBatchWindowDelayOverrideForTests =
                releaseBatchDelay.Task;

            clock.Advance(TimeSpan.FromMinutes(1));
            try
            {
                await CommitDeterministicGroupAsync(
                    wal,
                    pageCount,
                    firstPageValue: 0x71);
            }
            finally
            {
                wal.DurableCommitBatchWindowDelayOverrideForTests = null;
                releaseBatchDelay.TrySetResult();
            }

            StorageRuntimeDiagnosticsCapture grouped = Capture(state);
            WalRuntimeDiagnosticsSnapshot groupedWal = grouped.Wal.Value!;
            Assert.Equal(
                baselineWal.FlushCount + 1,
                groupedWal.FlushCount);
            Assert.Equal(
                baselineWal.FlushedCommitCount +
                    WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold,
                groupedWal.FlushedCommitCount);
            Assert.Equal(
                baselineWal.DurableFlushCount + 1,
                groupedWal.DurableFlushCount);
            Assert.Equal(
                baselineWal.GroupCommitBatchCount + 1,
                groupedWal.GroupCommitBatchCount);
            Assert.Equal(
                baselineWal.GroupCommitCount +
                    WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold,
                groupedWal.GroupCommitCount);
            Assert.Equal(clock.GetUtcNow(), groupedWal.LastSuccessfulFlushAtUtc);
            Assert.Equal(
                clock.GetUtcNow(),
                groupedWal.LastSuccessfulDurableFlushAtUtc);
            Assert.Equal(
                clock.GetUtcNow(),
                groupedWal.LastSuccessfulGroupCommitAtUtc);

            memory = await Database.OpenInMemoryAsync(options, Ct);
            WalRuntimeDiagnosticsSnapshot mixed = Capture(state).Wal.Value!;
            Assert.Null(mixed.FlushCount);
            Assert.Null(mixed.BytesWritten);
            Assert.Null(mixed.FlushedCommitCount);
            Assert.Null(mixed.DurableFlushCount);
            Assert.Null(mixed.LastSuccessfulDurableFlushAtUtc);
            Assert.Null(mixed.GroupCommitBatchCount);
            Assert.Null(mixed.GroupCommitCount);
            Assert.Null(mixed.LastSuccessfulGroupCommitAtUtc);

            await memory.DisposeAsync();
            memory = null;
            WalRuntimeDiagnosticsSnapshot restored = Capture(state).Wal.Value!;
            Assert.Equal(
                groupedWal.FlushedCommitCount,
                restored.FlushedCommitCount);
            Assert.Equal(
                groupedWal.DurableFlushCount,
                restored.DurableFlushCount);
            Assert.Equal(
                groupedWal.LastSuccessfulDurableFlushAtUtc,
                restored.LastSuccessfulDurableFlushAtUtc);
            Assert.Equal(
                groupedWal.GroupCommitBatchCount,
                restored.GroupCommitBatchCount);
            Assert.Equal(
                groupedWal.GroupCommitCount,
                restored.GroupCommitCount);
            Assert.Equal(
                groupedWal.LastSuccessfulGroupCommitAtUtc,
                restored.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            if (memory is not null)
                await memory.DisposeAsync();
            if (file is not null)
                await file.DisposeAsync();
            DeleteDatabaseFiles(path);
        }
    }

    [Fact]
    public async Task FileWal_ClockFailurePreservesCountsAndOmitsSupplementalTimestamps()
    {
        string path = NewDatabasePath("durability-clock-failure");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-durability-clock-failure");
        using var state = new CSharpDbRuntimeDiagnosticsState(
            observability,
            new ThrowingTimeProvider());
        DatabaseOptions options = CreateOptions(
                state,
                new DefaultStorageEngineFactory())
            .ConfigureStorageEngine(
                builder => builder.UseDurableGroupCommit(
                    TimeSpan.FromMilliseconds(250)));
        Database? database = null;
        try
        {
            database = await Database.CreateNewAsync(path, options, Ct);
            WriteAheadLog wal = GetFileWal(database);
            var releaseBatchDelay = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            wal.DurableCommitBatchWindowDelayOverrideForTests =
                releaseBatchDelay.Task;
            try
            {
                await CommitDeterministicGroupAsync(
                    wal,
                    GetPager(database).PageCount,
                    firstPageValue: 0x73);
            }
            finally
            {
                wal.DurableCommitBatchWindowDelayOverrideForTests = null;
                releaseBatchDelay.TrySetResult();
            }

            WalRuntimeDiagnosticsSnapshot snapshot = Capture(state).Wal.Value!;
            Assert.True(snapshot.FlushedCommitCount > 0);
            Assert.True(snapshot.DurableFlushCount > 0);
            Assert.Null(snapshot.LastSuccessfulDurableFlushAtUtc);
            Assert.True(snapshot.GroupCommitBatchCount > 0);
            Assert.True(snapshot.GroupCommitCount >= 2);
            Assert.Null(snapshot.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            if (database is not null)
                await database.DisposeAsync();
            DeleteDatabaseFiles(path);
        }
    }

    [Fact]
    public async Task BufferedFileWal_ProjectsPublicationButNoDurableSuccess()
    {
        string path = NewDatabasePath("buffered-durable-pairing");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-buffered-durable-pairing");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
                state,
                new DefaultStorageEngineFactory())
            .ConfigureStorageEngine(
                builder => builder.UseDurabilityMode(DurabilityMode.Buffered));
        try
        {
            await using Database database = await Database.CreateNewAsync(
                path,
                options,
                Ct);
            await ExecuteStatementAsync(
                database,
                "CREATE TABLE buffered_pairing (id INTEGER PRIMARY KEY)");

            WalRuntimeDiagnosticsSnapshot wal = Capture(state).Wal.Value!;
            Assert.True(wal.FlushCount > 0);
            Assert.True(wal.FlushedCommitCount > 0);
            Assert.NotNull(wal.LastSuccessfulFlushAtUtc);
            Assert.Equal(0L, wal.DurableFlushCount);
            Assert.Null(wal.LastSuccessfulDurableFlushAtUtc);
            Assert.Equal(0L, wal.GroupCommitBatchCount);
            Assert.Equal(0L, wal.GroupCommitCount);
            Assert.Null(wal.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Fact]
    public async Task FileWal_PostDrainDurableFlushesRetireExactlyOnceAndAggregateWithReopen()
    {
        string firstPath = NewDatabasePath("durable-post-drain-first");
        string reopenedPath = NewDatabasePath("durable-post-drain-reopened");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-durable-post-drain");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        var durableIncremented = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDurableCallback = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Database? first = null;
        Database? reopened = null;
        WriteAheadLog? firstWal = null;
        Task? commitTask = null;
        try
        {
            first = await Database.CreateNewAsync(firstPath, options, Ct);
            firstWal = GetFileWal(first);
            Pager firstPager = GetPager(first);
            StorageRuntimeDiagnostics.Registration registration =
                GetStorageRuntimeRegistration(first);
            await firstPager.BeginTransactionAsync(Ct);
            uint pageId = await firstPager.AllocatePageAsync(Ct);
            byte[] page = await firstPager.GetPageAsync(pageId, Ct);
            page[0] = 0x74;
            await firstPager.MarkDirtyAsync(pageId, Ct);
            firstWal.RuntimeDiagnosticsBeforeDurableFlushCallbackForTests = () =>
            {
                durableIncremented.TrySetResult();
                releaseDurableCallback.Task.GetAwaiter().GetResult();
            };

            commitTask = firstPager.CommitAsync(Ct).AsTask();
            await durableIncremented.Task.WaitAsync(
                TimeSpan.FromSeconds(10),
                Ct);

            WalRuntimeRawSnapshot capturedAtDrain =
                GetLiveWalRuntimeSnapshot(firstWal);
            registration.DrainProvider();
            releaseDurableCallback.TrySetResult();
            await commitTask.WaitAsync(
                TimeSpan.FromSeconds(10),
                Ct);
            firstWal.RuntimeDiagnosticsBeforeDurableFlushCallbackForTests = null;

            WalRuntimeRawSnapshot afterPublication =
                GetLiveWalRuntimeSnapshot(firstWal);
            Assert.Equal(
                capturedAtDrain.DurableFlushCount,
                afterPublication.DurableFlushCount);
            Assert.Equal(
                capturedAtDrain.FlushedCommitCount + 1,
                afterPublication.FlushedCommitCount);

            await first.DisposeAsync();
            first = null;
            long firstFinalDurableFlushCount = GetPrivateInt64Field(
                firstWal,
                "_runtimeDurableFlushCount");
            Assert.Equal(
                capturedAtDrain.DurableFlushCount + 1,
                firstFinalDurableFlushCount);

            reopened = await Database.CreateNewAsync(reopenedPath, options, Ct);
            WalRuntimeRawSnapshot reopenedRaw =
                GetLiveWalRuntimeSnapshot(GetFileWal(reopened));
            WalRuntimeDiagnosticsSnapshot aggregate = Capture(state).Wal.Value!;

            Assert.Equal(
                StorageRuntimeDiagnostics.SaturatingAdd(
                    capturedAtDrain.FlushedCommitCount!.Value,
                    reopenedRaw.FlushedCommitCount!.Value),
                aggregate.FlushedCommitCount);
            Assert.Equal(
                StorageRuntimeDiagnostics.SaturatingAdd(
                    firstFinalDurableFlushCount,
                    reopenedRaw.DurableFlushCount!.Value),
                aggregate.DurableFlushCount);
            Assert.Equal(
                StorageRuntimeDiagnostics.SaturatingAdd(
                    capturedAtDrain.GroupCommitBatchCount!.Value,
                    reopenedRaw.GroupCommitBatchCount!.Value),
                aggregate.GroupCommitBatchCount);
            Assert.Equal(
                StorageRuntimeDiagnostics.SaturatingAdd(
                    capturedAtDrain.GroupCommitCount!.Value,
                    reopenedRaw.GroupCommitCount!.Value),
                aggregate.GroupCommitCount);
            Assert.NotNull(aggregate.LastSuccessfulDurableFlushAtUtc);
            Assert.Null(aggregate.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            releaseDurableCallback.TrySetResult();
            if (firstWal is not null)
                firstWal.RuntimeDiagnosticsBeforeDurableFlushCallbackForTests = null;
            if (commitTask is not null)
            {
                try
                {
                    await commitTask;
                }
                catch
                {
                }
            }

            if (first is not null)
                await first.DisposeAsync();
            if (reopened is not null)
                await reopened.DisposeAsync();
            DeleteDatabaseFiles(firstPath);
            DeleteDatabaseFiles(reopenedPath);
        }
    }

    [Fact]
    public async Task LifetimeCounters_SurviveTwoHandleDetachAndLaterReopen()
    {
        string firstPath = NewDatabasePath("lifetime-first");
        string secondPath = NewDatabasePath("lifetime-second");
        string reopenedPath = NewDatabasePath("lifetime-reopened");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-lifetime-detach");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        Database? first = null;
        Database? second = null;
        Database? reopened = null;
        try
        {
            first = await Database.CreateNewAsync(firstPath, options, Ct);
            second = await Database.CreateNewAsync(secondPath, options, Ct);
            await ExecuteStatementAsync(
                first,
                "CREATE TABLE lifetime_first (id INTEGER PRIMARY KEY)");
            await ExecuteStatementAsync(
                second,
                "CREATE TABLE lifetime_second (id INTEGER PRIMARY KEY)");

            StorageRuntimeDiagnosticsCapture bothLive = Capture(state);
            AssertKnownFileLifetimeCounters(bothLive);

            IDisposable firstRegistration = GetStorageRuntimeRegistration(first);
            await first.DisposeAsync();
            first = null;
            StorageRuntimeDiagnosticsCapture afterFirstDetach = Capture(state);
            AssertLifetimeCountersAtLeast(afterFirstDetach, bothLive);
            firstRegistration.Dispose();
            firstRegistration.Dispose();
            AssertLifetimeCountersEqual(Capture(state), afterFirstDetach);

            await ExecuteStatementAsync(
                second,
                "INSERT INTO lifetime_second VALUES (1)");
            StorageRuntimeDiagnosticsCapture afterMoreWork = Capture(state);
            AssertLifetimeCountersAtLeast(afterMoreWork, afterFirstDetach);
            Assert.True(
                afterMoreWork.Storage.Value!.CommitCount >
                afterFirstDetach.Storage.Value!.CommitCount);
            Assert.True(
                afterMoreWork.Storage.Value.PageWrites >
                afterFirstDetach.Storage.Value.PageWrites);
            Assert.Equal(
                afterFirstDetach.Storage.Value.ConflictCount,
                afterMoreWork.Storage.Value.ConflictCount);

            await second.DisposeAsync();
            second = null;
            StorageRuntimeDiagnosticsCapture withoutLiveProvider = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                withoutLiveProvider.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                withoutLiveProvider.Wal.Availability);
            Assert.NotNull(withoutLiveProvider.Wal.Value);

            reopened = await Database.CreateNewAsync(reopenedPath, options, Ct);
            StorageRuntimeDiagnosticsCapture afterReopen = Capture(state);
            AssertKnownFileLifetimeCounters(afterReopen);
            AssertLifetimeCountersAtLeast(afterReopen, afterMoreWork);
        }
        finally
        {
            if (first is not null)
                await first.DisposeAsync();
            if (second is not null)
                await second.DisposeAsync();
            if (reopened is not null)
                await reopened.DisposeAsync();
            DeleteDatabaseFiles(firstPath);
            DeleteDatabaseFiles(secondPath);
            DeleteDatabaseFiles(reopenedPath);
        }
    }

    [Fact]
    public async Task FailedFinalCapture_PermanentlyPoisonsLifetimeCounters()
    {
        string failedPath = NewDatabasePath("lifetime-final-failure");
        string replacementPath = NewDatabasePath("lifetime-after-failure");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-final-failure");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        Database? failed = null;
        Database? replacement = null;
        try
        {
            failed = await Database.CreateNewAsync(failedPath, options, Ct);
            await ExecuteStatementAsync(
                failed,
                "CREATE TABLE final_failure (id INTEGER PRIMARY KEY)");
            AssertKnownFileLifetimeCounters(Capture(state));

            ForceFinalRuntimeCaptureFailure(failed);
            await failed.DisposeAsync();
            failed = null;
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                Capture(state).Storage.Availability);

            replacement = await Database.CreateNewAsync(
                replacementPath,
                options,
                Ct);
            await ExecuteStatementAsync(
                replacement,
                "CREATE TABLE after_failure (id INTEGER PRIMARY KEY)");
            StorageRuntimeDiagnosticsCapture capture = Capture(state);

            Assert.Equal(
                DiagnosticsAvailability.Available,
                capture.Storage.Availability);
            Assert.Null(capture.Storage.Value!.CommitCount);
            Assert.Null(capture.Storage.Value.PageWrites);
            Assert.Null(capture.Storage.Value.BytesWritten);
            Assert.Null(capture.Storage.Value.ConflictCount);
            Assert.Null(capture.Wal.Value!.FlushCount);
            Assert.Null(capture.Wal.Value.BytesWritten);
            Assert.Null(capture.Wal.Value.FlushedCommitCount);
            Assert.Null(capture.Wal.Value.DurableFlushCount);
            Assert.Null(capture.Wal.Value.LastSuccessfulDurableFlushAtUtc);
            Assert.Null(capture.Wal.Value.GroupCommitBatchCount);
            Assert.Null(capture.Wal.Value.GroupCommitCount);
            Assert.Null(capture.Wal.Value.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            if (failed is not null)
                await failed.DisposeAsync();
            if (replacement is not null)
                await replacement.DisposeAsync();
            DeleteDatabaseFiles(failedPath);
            DeleteDatabaseFiles(replacementPath);
        }
    }

    [Fact]
    public async Task InvalidFinalCapture_PermanentlyPoisonsLifetimeCounters()
    {
        string invalidPath = NewDatabasePath("lifetime-final-invalid");
        string replacementPath = NewDatabasePath("lifetime-after-invalid");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-final-invalid");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        Database? invalid = null;
        Database? replacement = null;
        try
        {
            invalid = await Database.CreateNewAsync(invalidPath, options, Ct);
            await ExecuteStatementAsync(
                invalid,
                "CREATE TABLE final_invalid (id INTEGER PRIMARY KEY)");
            AssertKnownFileLifetimeCounters(Capture(state));

            ForceFinalRuntimeCaptureInvalid(invalid);
            await invalid.DisposeAsync();
            invalid = null;

            replacement = await Database.CreateNewAsync(
                replacementPath,
                options,
                Ct);
            StorageRuntimeDiagnosticsCapture capture = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                capture.Storage.Availability);
            Assert.Null(capture.Storage.Value!.CommitCount);
            Assert.Null(capture.Storage.Value.PageWrites);
            Assert.Null(capture.Storage.Value.BytesWritten);
            Assert.Null(capture.Storage.Value.ConflictCount);
            Assert.Null(capture.Wal.Value!.FlushCount);
            Assert.Null(capture.Wal.Value.BytesWritten);
            Assert.Null(capture.Wal.Value.FlushedCommitCount);
            Assert.Null(capture.Wal.Value.DurableFlushCount);
            Assert.Null(capture.Wal.Value.LastSuccessfulDurableFlushAtUtc);
            Assert.Null(capture.Wal.Value.GroupCommitBatchCount);
            Assert.Null(capture.Wal.Value.GroupCommitCount);
            Assert.Null(capture.Wal.Value.LastSuccessfulGroupCommitAtUtc);
        }
        finally
        {
            if (invalid is not null)
                await invalid.DisposeAsync();
            if (replacement is not null)
                await replacement.DisposeAsync();
            DeleteDatabaseFiles(invalidPath);
            DeleteDatabaseFiles(replacementPath);
        }
    }

    [Fact]
    public async Task MemoryLifetimeCounters_ExposeCommitsButNotFileFlushFields()
    {
        string filePath = NewDatabasePath("lifetime-memory-to-file");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-memory-lifetimes");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new ThrowingStorageEngineFactory());

        long firstCommitCount;
        await using (Database first = await Database.OpenInMemoryAsync(options, Ct))
        {
            await ExecuteStatementAsync(
                first,
                "CREATE TABLE memory_lifetime_first (id INTEGER PRIMARY KEY)");
            StorageRuntimeDiagnosticsCapture capture = Capture(state);
            firstCommitCount = Assert.IsType<long>(
                capture.Storage.Value!.CommitCount);
            Assert.True(firstCommitCount > 0);
            Assert.True(capture.Storage.Value.PageWrites > 0);
            Assert.Equal(
                capture.Storage.Value.PageWrites * PageConstants.PageSize,
                capture.Storage.Value.BytesWritten);
            Assert.Equal(0, capture.Storage.Value.ConflictCount);
            Assert.Null(capture.Wal.Value!.FlushCount);
            Assert.Null(capture.Wal.Value.BytesWritten);
        }

        long secondCommitCount;
        await using (Database second = await Database.OpenInMemoryAsync(options, Ct))
        {
            await ExecuteStatementAsync(
                second,
                "CREATE TABLE memory_lifetime_second (id INTEGER PRIMARY KEY)");
            StorageRuntimeDiagnosticsCapture reopened = Capture(state);
            secondCommitCount = Assert.IsType<long>(
                reopened.Storage.Value!.CommitCount);
            Assert.True(secondCommitCount > firstCommitCount);
            Assert.True(reopened.Storage.Value.PageWrites > 0);
            Assert.Equal(
                reopened.Storage.Value.PageWrites * PageConstants.PageSize,
                reopened.Storage.Value.BytesWritten);
            Assert.Equal(0, reopened.Storage.Value.ConflictCount);
            Assert.Null(reopened.Wal.Value!.FlushCount);
            Assert.Null(reopened.Wal.Value.BytesWritten);
        }

        try
        {
            await using (Database created = await Database.CreateNewAsync(filePath, Ct))
            {
            }

            await using Database file = await Database.OpenAsync(
                filePath,
                CreateOptions(state, new DefaultStorageEngineFactory()),
                Ct);
            StorageRuntimeDiagnosticsCapture fileCapture = Capture(state);
            Assert.True(
                fileCapture.Storage.Value!.CommitCount >= secondCommitCount);
            Assert.NotNull(fileCapture.Storage.Value.PageWrites);
            Assert.Equal(
                fileCapture.Storage.Value.PageWrites * PageConstants.PageSize,
                fileCapture.Storage.Value.BytesWritten);
            Assert.Equal(0, fileCapture.Storage.Value.ConflictCount);
            Assert.NotNull(fileCapture.Wal.Value!.FlushCount);
            Assert.NotNull(fileCapture.Wal.Value.BytesWritten);
        }
        finally
        {
            DeleteDatabaseFiles(filePath);
        }
    }

    [Fact]
    public async Task LifetimeCounters_SaturateAndFollowCaptureMetadataEpoch()
    {
        string path = NewDatabasePath("lifetime-saturation");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-lifetime-saturation");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new DefaultStorageEngineFactory());
        try
        {
            await using Database database = await Database.CreateNewAsync(
                path,
                options,
                Ct);
            Assert.Equal(
                long.MaxValue,
                StorageRuntimeDiagnostics.SaturatingAdd(
                    long.MaxValue - 1,
                    2));
            Assert.Equal(
                long.MaxValue,
                StorageRuntimeDiagnostics.SaturatingMultiply(
                    long.MaxValue - 1,
                    PageConstants.PageSize));

            StorageRuntimeDiagnostics diagnostics = Assert.IsType<
                StorageRuntimeDiagnostics>(GetStorageRuntimeComponent(state));
            SetRetiredLifetimeCounters(diagnostics, long.MaxValue);
            StorageRuntimeDiagnosticsCapture beforeReset = Capture(state);
            Assert.Equal(
                long.MaxValue,
                beforeReset.Storage.Value!.CommitCount);
            Assert.Equal(long.MaxValue, beforeReset.Storage.Value.PageWrites);
            Assert.Equal(long.MaxValue, beforeReset.Storage.Value.BytesWritten);
            Assert.Equal(long.MaxValue, beforeReset.Storage.Value.ConflictCount);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value!.FlushCount);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value.BytesWritten);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value.FlushedCommitCount);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value.DurableFlushCount);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value.GroupCommitBatchCount);
            Assert.Equal(long.MaxValue, beforeReset.Wal.Value.GroupCommitCount);

            long priorEpoch = state.CounterEpoch;
            database.ResetWalFlushDiagnostics();
            StorageRuntimeDiagnosticsCapture afterReset = Capture(state);
            Assert.True(state.CounterEpoch > priorEpoch);
            Assert.Equal(
                state.CounterEpoch,
                afterReset.Storage.Value!.Metadata.CounterEpoch);
            Assert.Equal(
                afterReset.Storage.Value.Metadata,
                afterReset.Wal.Value!.Metadata);
            Assert.Equal(long.MaxValue, afterReset.Storage.Value.CommitCount);
            Assert.Equal(long.MaxValue, afterReset.Storage.Value.PageWrites);
            Assert.Equal(long.MaxValue, afterReset.Storage.Value.BytesWritten);
            Assert.Equal(long.MaxValue, afterReset.Storage.Value.ConflictCount);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.FlushCount);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.BytesWritten);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.FlushedCommitCount);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.DurableFlushCount);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.GroupCommitBatchCount);
            Assert.Equal(long.MaxValue, afterReset.Wal.Value.GroupCommitCount);
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Fact]
    public async Task DirectSummary_CustomStorageFactoryIsUnsupported()
    {
        DatabaseOptions options = CreateOptions(
            "storage-runtime-custom",
            new CustomInMemoryStorageEngineFactory());
        await using var client = new EngineTransportClient(
            Path.Combine(
                Path.GetTempPath(),
                $"csharpdb_storage_runtime_custom_{Guid.NewGuid():N}.db"),
            options);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            runtime.Aggregate.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            runtime.Aggregate.Wal.Availability);
        Assert.Null(runtime.Aggregate.Storage.Value);
        Assert.Null(runtime.Aggregate.Wal.Value);
    }

    [Fact]
    public async Task CustomCreateNewFactoryReturningBuiltInPagerIsUnsupported()
    {
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-custom-create");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new CustomInMemoryStorageEngineFactory());
        await using Database database = await Database.CreateNewAsync(
            Path.Combine(
                Path.GetTempPath(),
                $"csharpdb_storage_runtime_custom_create_{Guid.NewGuid():N}.db"),
            options,
            Ct);

        StorageRuntimeDiagnosticsCapture capture = Capture(state);
        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            capture.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            capture.Wal.Availability);
    }

    [Fact]
    public async Task MixedBuiltInAndCustomRegistrationsFailWholeFamilyUnsupported()
    {
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-mixed-provenance");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        await using Database builtIn = await Database.OpenInMemoryAsync(
            CreateOptions(state, new ThrowingStorageEngineFactory()),
            Ct);
        Database? custom = null;
        try
        {
            custom = await Database.OpenAsync(
                Path.Combine(
                    Path.GetTempPath(),
                    $"csharpdb_storage_runtime_mixed_{Guid.NewGuid():N}.db"),
                CreateOptions(state, new CustomInMemoryStorageEngineFactory()),
                Ct);

            StorageRuntimeDiagnosticsCapture mixed = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Unsupported,
                mixed.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Unsupported,
                mixed.Wal.Availability);

            await custom.DisposeAsync();
            custom = null;
            StorageRuntimeDiagnosticsCapture builtInOnly = Capture(state);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                builtInOnly.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                builtInOnly.Wal.Availability);
        }
        finally
        {
            if (custom is not null)
                await custom.DisposeAsync();
        }
    }

    [Fact]
    public async Task DirectSummary_MultipleRuntimeFamiliesKeepOnlyExactStorageAvailable()
    {
        await using var client = new EngineTransportClient(
            $":memory:storage-runtime-families-{Guid.NewGuid():N}",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            CreateOptions("storage-runtime-families"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo? transaction = null;
        try
        {
            transaction = await client.BeginTransactionAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, runtime.Metadata.Scope);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                runtime.Aggregate.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                runtime.Aggregate.Wal.Availability);
            RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>[] families =
                Assert.IsAssignableFrom<IEnumerable<
                    RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>>>(
                        runtime.RuntimeFamilies)
                    .ToArray();
            Assert.Equal(2, families.Length);
            Assert.All(
                families,
                family =>
                {
                    Assert.Equal(
                        DiagnosticsAvailability.Available,
                        family.Value.Storage.Availability);
                    Assert.Equal(
                        DiagnosticsAvailability.Available,
                        family.Value.Wal.Availability);
                    Assert.Equal(
                        family.Value.Metadata,
                        family.Value.Storage.Value!.Metadata);
                    Assert.Equal(
                        family.Value.Metadata,
                        family.Value.Wal.Value!.Metadata);
                });
        }
        finally
        {
            if (transaction is not null)
                await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task BuiltInMemoryLoadAndHybridModesIgnoreCustomFactoryProvenance()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_runtime_modes_{Guid.NewGuid():N}.db");
        CSharpDbObservabilityOptions observability = CreateObservability(
            "storage-runtime-modes");
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        DatabaseOptions options = CreateOptions(
            state,
            new ThrowingStorageEngineFactory());
        try
        {
            await using (Database memory = await Database.OpenInMemoryAsync(options, Ct))
            {
                AssertMemoryBackedAvailable(Capture(state));
            }

            await using (Database created = await Database.CreateNewAsync(path, Ct))
            {
            }

            await using (Database file = await Database.OpenAsync(
                             path,
                             CreateOptions(
                                 state,
                                 new DefaultStorageEngineFactory()),
                             Ct))
            {
                StorageRuntimeDiagnosticsCapture capture = Capture(state);
                Assert.Equal(
                    DiagnosticsAvailability.Available,
                    capture.Storage.Availability);
                Assert.Equal(
                    DiagnosticsAvailability.Available,
                    capture.Wal.Availability);
                Assert.NotNull(capture.Storage.Value!.AllocatedDatabaseBytes);
                Assert.NotNull(capture.Wal.Value!.AllocatedBytes);
                Assert.NotNull(capture.Storage.Value.CommitCount);
                Assert.NotNull(capture.Wal.Value.FlushCount);
                Assert.NotNull(capture.Wal.Value.BytesWritten);
            }

            await using (Database loaded = await Database.LoadIntoMemoryAsync(path, options, Ct))
            {
                AssertMemoryBackedAvailable(Capture(state));
            }

            await using (Database snapshot = await Database.OpenHybridAsync(
                             path,
                             options,
                             new HybridDatabaseOptions
                             {
                                 PersistenceMode = HybridPersistenceMode.Snapshot,
                                 PersistenceTriggers = HybridPersistenceTriggers.None,
                             },
                             Ct))
            {
                AssertMemoryBackedAvailable(Capture(state));
            }

            await using (Database lazy = await Database.OpenHybridAsync(
                             path,
                             options,
                             new HybridDatabaseOptions
                             {
                                 PersistenceMode = HybridPersistenceMode.IncrementalDurable,
                             },
                             Ct))
            {
                StorageRuntimeDiagnosticsCapture capture = Capture(state);
                Assert.Equal(
                    DiagnosticsAvailability.Available,
                    capture.Storage.Availability);
                Assert.Equal(
                    DiagnosticsAvailability.Available,
                    capture.Wal.Availability);
                Assert.NotNull(capture.Storage.Value!.AllocatedDatabaseBytes);
                Assert.NotNull(capture.Wal.Value!.AllocatedBytes);
            }
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    [Fact]
    public async Task FailingAndExplicitDisabledOpensDoNotCreateStorageComponent()
    {
        CSharpDbObservabilityOptions enabled = CreateObservability(
            "storage-runtime-failing");
        using (var enabledState = new CSharpDbRuntimeDiagnosticsState(enabled))
        {
            DatabaseOptions failingOptions = CreateOptions(
                enabledState,
                new ThrowingStorageEngineFactory());
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => Database.OpenAsync(
                    Path.Combine(
                        Path.GetTempPath(),
                        $"csharpdb_storage_runtime_failure_{Guid.NewGuid():N}.db"),
                    failingOptions,
                    Ct).AsTask());
            Assert.False(
                enabledState.TryGetComponent<StorageRuntimeDiagnostics>(out _));
        }

        var disabled = new CSharpDbObservabilityOptions
        {
            Enabled = false,
            DatabaseAlias = "storage-runtime-disabled",
        };
        using var disabledState = new CSharpDbRuntimeDiagnosticsState(disabled);
        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = disabled,
                RuntimeDiagnosticsState = disabledState,
            },
            Ct);
        Assert.False(
            disabledState.TryGetComponent<StorageRuntimeDiagnostics>(out _));
        Assert.Null(GetRuntimeComponents(disabledState));
    }

    [Fact]
    public async Task PrivateSnapshotHandle_DoesNotRegisterStorageProvider()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_runtime_private_{Guid.NewGuid():N}.db");
        try
        {
            await using (Database created = await Database.CreateNewAsync(path, Ct))
            {
            }

            CSharpDbObservabilityOptions observability = CreateObservability(
                "storage-runtime-private");
            using var state = new CSharpDbRuntimeDiagnosticsState(observability);
            var options = new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = state,
            };
            await using Database privateSnapshot =
                await Database.OpenPrivateSnapshotCopyAsync(path, options, Ct);

            DiagnosticsSnapshotMetadata metadata = state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Engine);
            StorageRuntimeDiagnosticsCapture capture =
                StorageRuntimeDiagnostics.Capture(state, metadata);

            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                capture.Storage.Availability);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                capture.Wal.Availability);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    private static DatabaseOptions CreateOptions(
        string alias,
        IStorageEngineFactory? storageEngineFactory = null)
        => new()
        {
            ObservabilityOptions = CreateObservability(alias),
            StorageEngineFactory = storageEngineFactory ??
                new DefaultStorageEngineFactory(),
        };

    private static DatabaseOptions CreateOptions(
        CSharpDbRuntimeDiagnosticsState state,
        IStorageEngineFactory storageEngineFactory)
        => new()
        {
            ObservabilityOptions = state.CreateOptionsSnapshot(),
            RuntimeDiagnosticsState = state,
            StorageEngineFactory = storageEngineFactory,
        };

    private static CSharpDbObservabilityOptions CreateObservability(string alias)
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                Queries = false,
                SlowQueries = false,
            },
        };

    private static StorageRuntimeDiagnosticsCapture Capture(
        CSharpDbRuntimeDiagnosticsState state)
        => StorageRuntimeDiagnostics.Capture(
            state,
            state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Engine));

    private static void AssertMemoryBackedAvailable(
        StorageRuntimeDiagnosticsCapture capture)
    {
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Wal.Availability);
        Assert.Null(capture.Storage.Value!.AllocatedDatabaseBytes);
        Assert.Null(capture.Wal.Value!.AllocatedBytes);
        Assert.NotNull(capture.Storage.Value.CommitCount);
        Assert.NotNull(capture.Storage.Value.PageWrites);
        Assert.Equal(
            capture.Storage.Value.PageWrites * PageConstants.PageSize,
            capture.Storage.Value.BytesWritten);
        Assert.NotNull(capture.Storage.Value.ConflictCount);
        Assert.Null(capture.Wal.Value.FlushCount);
        Assert.Null(capture.Wal.Value.BytesWritten);
        Assert.Null(capture.Wal.Value.FlushedCommitCount);
        Assert.Null(capture.Wal.Value.DurableFlushCount);
        Assert.Null(capture.Wal.Value.LastSuccessfulDurableFlushAtUtc);
        Assert.Null(capture.Wal.Value.GroupCommitBatchCount);
        Assert.Null(capture.Wal.Value.GroupCommitCount);
        Assert.Null(capture.Wal.Value.LastSuccessfulGroupCommitAtUtc);
    }

    private static string NewDatabasePath(string name)
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_runtime_{name}_{Guid.NewGuid():N}.db");

    private static async Task ExecuteStatementAsync(Database database, string sql)
    {
        await using var result = await database.ExecuteAsync(sql, Ct);
    }

    private static async Task CommitDeterministicGroupAsync(
        WriteAheadLog wal,
        uint pageCount,
        byte firstPageValue)
    {
        var commits = new List<WalCommitResult>(
            WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold);
        for (int i = 0;
             i < WriteAheadLog.DurableCommitBatchBypassPendingCommitThreshold;
             i++)
        {
            commits.Add(await wal.AppendFramesAndCommitAsync(
                new[] { new WalFrameWrite(
                    0,
                    CreateFilledPage((byte)(firstPageValue + i))) },
                pageCount,
                Ct));
        }

        var completions = new List<Task>(commits.Count);
        foreach (WalCommitResult commit in commits)
            completions.Add(commit.WaitAsync(Ct).AsTask());
        await Task.WhenAll(completions).WaitAsync(
            TimeSpan.FromSeconds(10),
            Ct);
    }

    private static byte[] CreateFilledPage(byte value)
    {
        var page = new byte[PageConstants.PageSize];
        page.AsSpan().Fill(value);
        return page;
    }

    private static void AssertKnownFileLifetimeCounters(
        StorageRuntimeDiagnosticsCapture capture)
    {
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            capture.Wal.Availability);
        Assert.True(capture.Storage.Value!.CommitCount > 0);
        Assert.True(capture.Storage.Value.PageWrites > 0);
        Assert.Equal(
            capture.Storage.Value.PageWrites * PageConstants.PageSize,
            capture.Storage.Value.BytesWritten);
        Assert.True(capture.Storage.Value.ConflictCount >= 0);
        Assert.True(capture.Wal.Value!.FlushCount > 0);
        Assert.True(capture.Wal.Value.BytesWritten > 0);
        Assert.True(capture.Wal.Value.FlushedCommitCount > 0);
        Assert.True(capture.Wal.Value.DurableFlushCount > 0);
        Assert.NotNull(capture.Wal.Value.LastSuccessfulDurableFlushAtUtc);
        Assert.NotNull(capture.Wal.Value.GroupCommitBatchCount);
        Assert.NotNull(capture.Wal.Value.GroupCommitCount);
        Assert.Equal(
            capture.Wal.Value.GroupCommitBatchCount > 0,
            capture.Wal.Value.LastSuccessfulGroupCommitAtUtc.HasValue);
    }

    private static void AssertLifetimeCountersAtLeast(
        StorageRuntimeDiagnosticsCapture current,
        StorageRuntimeDiagnosticsCapture baseline)
    {
        Assert.Equal(
            DiagnosticsAvailability.Available,
            current.Storage.Availability);
        Assert.Equal(
            DiagnosticsAvailability.Available,
            current.Wal.Availability);
        Assert.True(
            current.Storage.Value!.CommitCount >=
            baseline.Storage.Value!.CommitCount);
        Assert.True(
            current.Storage.Value.PageWrites >=
            baseline.Storage.Value.PageWrites);
        Assert.True(
            current.Storage.Value.BytesWritten >=
            baseline.Storage.Value.BytesWritten);
        Assert.True(
            current.Storage.Value.ConflictCount >=
            baseline.Storage.Value.ConflictCount);
        Assert.True(
            current.Wal.Value!.FlushCount >= baseline.Wal.Value!.FlushCount);
        Assert.True(
            current.Wal.Value.BytesWritten >= baseline.Wal.Value.BytesWritten);
        Assert.True(
            current.Wal.Value.FlushedCommitCount >=
            baseline.Wal.Value.FlushedCommitCount);
        Assert.True(
            current.Wal.Value.DurableFlushCount >=
            baseline.Wal.Value.DurableFlushCount);
        Assert.True(
            current.Wal.Value.GroupCommitBatchCount >=
            baseline.Wal.Value.GroupCommitBatchCount);
        Assert.True(
            current.Wal.Value.GroupCommitCount >=
            baseline.Wal.Value.GroupCommitCount);
    }

    private static void AssertLifetimeCountersEqual(
        StorageRuntimeDiagnosticsCapture current,
        StorageRuntimeDiagnosticsCapture baseline)
    {
        Assert.Equal(
            baseline.Storage.Value!.CommitCount,
            current.Storage.Value!.CommitCount);
        Assert.Equal(
            baseline.Storage.Value.PageWrites,
            current.Storage.Value.PageWrites);
        Assert.Equal(
            baseline.Storage.Value.BytesWritten,
            current.Storage.Value.BytesWritten);
        Assert.Equal(
            baseline.Storage.Value.ConflictCount,
            current.Storage.Value.ConflictCount);
        Assert.Equal(
            baseline.Wal.Value!.FlushCount,
            current.Wal.Value!.FlushCount);
        Assert.Equal(
            baseline.Wal.Value.BytesWritten,
            current.Wal.Value.BytesWritten);
        Assert.Equal(
            baseline.Wal.Value.FlushedCommitCount,
            current.Wal.Value.FlushedCommitCount);
        Assert.Equal(
            baseline.Wal.Value.DurableFlushCount,
            current.Wal.Value.DurableFlushCount);
        Assert.Equal(
            baseline.Wal.Value.GroupCommitBatchCount,
            current.Wal.Value.GroupCommitBatchCount);
        Assert.Equal(
            baseline.Wal.Value.GroupCommitCount,
            current.Wal.Value.GroupCommitCount);
    }

    private static void ForceFinalRuntimeCaptureFailure(Database database)
    {
        Pager pager = GetPager(database);
        IDisposable registration = GetStorageRuntimeRegistration(database);
        FieldInfo disposeRequested = typeof(Pager).GetField(
            "_disposeRequested",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

        disposeRequested.SetValue(pager, 1);
        try
        {
            registration.Dispose();
        }
        finally
        {
            disposeRequested.SetValue(pager, 0);
        }
    }

    private static void ForceFinalRuntimeCaptureInvalid(Database database)
    {
        Pager pager = GetPager(database);
        IDisposable registration = GetStorageRuntimeRegistration(database);
        FieldInfo checkpointsField = typeof(Pager).GetField(
            "_checkpoints",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        object checkpoints = Assert.IsAssignableFrom<object>(
            checkpointsField.GetValue(pager));
        FieldInfo activeReaderCount = checkpoints.GetType().GetField(
            "_activeReaderCount",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        object? originalActiveReaderCount = activeReaderCount.GetValue(checkpoints);

        activeReaderCount.SetValue(checkpoints, -1);
        try
        {
            registration.Dispose();
        }
        finally
        {
            activeReaderCount.SetValue(checkpoints, originalActiveReaderCount);
        }
    }

    private static Pager GetPager(Database database)
        => Assert.IsType<Pager>(
            typeof(Database)
                .GetField("_pager", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(database));

    private static WriteAheadLog GetFileWal(Database database)
        => Assert.IsType<WriteAheadLog>(
            typeof(Pager)
                .GetField("_wal", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(GetPager(database)));

    private static WalRuntimeRawSnapshot GetLiveWalRuntimeSnapshot(
        WriteAheadLog wal)
    {
        var provider = Assert.IsAssignableFrom<ILiveWalRuntimeSnapshotProvider>(
            wal);
        Assert.True(
            provider.TryGetLiveRuntimeDiagnosticsSnapshot(
                out WalRuntimeRawSnapshot snapshot));
        return snapshot;
    }

    private static long GetConflictCount(CSharpDbRuntimeDiagnosticsState state)
        => Assert.IsType<long>(Capture(state).Storage.Value!.ConflictCount);

    private static long GetPageWriteCount(CSharpDbRuntimeDiagnosticsState state)
        => Assert.IsType<long>(Capture(state).Storage.Value!.PageWrites);

    private static void SetPrivateInt64Field(
        object instance,
        string fieldName,
        long value)
        => instance.GetType()
            .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(instance, value);

    private static long GetPrivateInt64Field(
        object instance,
        string fieldName)
        => Assert.IsType<long>(
            instance.GetType()
                .GetField(
                    fieldName,
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(instance));

    private static StorageRuntimeDiagnostics.Registration
        GetStorageRuntimeRegistration(Database database)
        => Assert.IsType<StorageRuntimeDiagnostics.Registration>(
            typeof(Database)
                .GetField(
                    "_storageRuntimeDiagnosticsRegistration",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(database));

    private static StorageRuntimeDiagnostics GetStorageRuntimeComponent(
        CSharpDbRuntimeDiagnosticsState state)
    {
        Assert.True(
            state.TryGetComponent<StorageRuntimeDiagnostics>(
                out StorageRuntimeDiagnostics? diagnostics));
        return Assert.IsType<StorageRuntimeDiagnostics>(diagnostics);
    }

    private static void SetRetiredLifetimeCounters(
        StorageRuntimeDiagnostics diagnostics,
        long value)
    {
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredLogicalCommitCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredLogicalPageWriteCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredTerminalConflictCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredCommitFlushBatchCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredCommittedFrameBytesWritten",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredFlushedCommitCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredDurableFlushCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredGroupCommitBatchCount",
            value);
        SetRetiredLifetimeCounter(
            diagnostics,
            "_retiredGroupCommitCount",
            value);
    }

    private static void SetRetiredLifetimeCounter(
        StorageRuntimeDiagnostics diagnostics,
        string fieldName,
        long value)
        => typeof(StorageRuntimeDiagnostics)
            .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(diagnostics, value);

    private static void DeleteDatabaseFiles(string path)
    {
        File.Delete(path);
        File.Delete(path + ".wal");
    }

    private static CheckpointPhase HigherPhase(
        CheckpointPhase left,
        CheckpointPhase right)
        => (int)left >= (int)right ? left : right;

    private static object? GetRuntimeComponents(
        CSharpDbRuntimeDiagnosticsState state)
        => typeof(CSharpDbRuntimeDiagnosticsState)
            .GetField("_components", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(state);

    private sealed class CustomInMemoryStorageEngineFactory : IStorageEngineFactory
    {
        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
            => InMemoryStorageEngineFactory.OpenAsync(options, ct: ct);

        public ValueTask<StorageEngineContext> CreateNewAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
            => InMemoryStorageEngineFactory.OpenAsync(options, ct: ct);
    }

    private sealed class ThrowingStorageEngineFactory : IStorageEngineFactory
    {
        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
            => throw new InvalidOperationException(
                "The custom storage factory must not be called.");
    }

    private sealed class ArmableConflictInterceptor : IPageOperationInterceptor
    {
        private int _armed;

        internal void Arm() => Volatile.Write(ref _armed, 1);

        public ValueTask OnBeforeReadAsync(
            uint pageId,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(
            uint pageId,
            PageReadSource source,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnBeforeWriteAsync(
            uint pageId,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(
            uint pageId,
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitStartAsync(
            int dirtyPageCount,
            CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _armed, 0) != 0)
            {
                throw new CSharpDbConflictException(
                    "Synthetic interceptor-owned conflict.");
            }

            return ValueTask.CompletedTask;
        }

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

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;
    }

    private sealed class OpenCaptureObserver(
        CSharpDbRuntimeDiagnosticsState runtimeState)
        : IObserver<KeyValuePair<string, object?>>
    {
        internal StorageRuntimeDiagnosticsCapture? Capture { get; private set; }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Key != CSharpDbLogEvents.DatabaseOpened.Name ||
                value.Value is not CSharpDbLifecycleCompletedEvent)
            {
                return;
            }

            Capture = StorageRuntimeDiagnostics.Capture(
                runtimeState,
                runtimeState.CreateMetadata(
                    DiagnosticsScope.Instance,
                    DiagnosticsAvailability.Available,
                    DiagnosticsSource.Engine));
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private readonly object _gate = new();
        private DateTimeOffset _utcNow;
        private long _timestamp;

        internal ManualTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow.ToUniversalTime();
        }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
        {
            lock (_gate)
                return _utcNow;
        }

        public override long GetTimestamp()
        {
            lock (_gate)
                return _timestamp;
        }

        internal void Advance(TimeSpan elapsed)
        {
            lock (_gate)
            {
                _utcNow = _utcNow.Add(elapsed);
                _timestamp += elapsed.Ticks;
            }
        }
    }

    private sealed class ThrowingTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() =>
            throw new InvalidOperationException("Synthetic clock failure.");

        public override long GetTimestamp() =>
            throw new InvalidOperationException("Synthetic clock failure.");
    }
}
