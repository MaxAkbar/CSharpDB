using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class StorageIoRuntimeProducerTests
{
    [Fact]
    public async Task LogicalReads_RecordOnlySuccessfulRecordingAndMaterializationPaths()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(ct: ct);

        Assert.True(runtime.Pager.TryGetCachedPageReadBuffer(0, out _));
        AssertLogicalReads(runtime.Pager, cacheHits: 0, cacheMisses: 0);

        Assert.True(runtime.Pager.TryGetCachedPageReadBufferAndRecordRead(0, out _));
        Assert.False(runtime.Pager.TryGetCachedPageReadBufferAndRecordRead(uint.MaxValue, out _));
        AssertLogicalReads(runtime.Pager, cacheHits: 1, cacheMisses: 0);

        _ = await runtime.Pager.ReadPageUncachedAsync(0, ct);
        AssertLogicalReads(runtime.Pager, cacheHits: 1, cacheMisses: 1);
    }

    [Fact]
    public async Task SnapshotRecordingProbe_SplitsSuccessfulAndFailedCacheOnlyReads()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(ct: ct);
        WalSnapshot snapshot = runtime.Pager.AcquireReaderSnapshot();

        try
        {
            Assert.True(
                runtime.Pager.TryGetSnapshotCachedPageReadBufferAndRecordRead(
                    0,
                    snapshot,
                    out _));
            Assert.False(
                runtime.Pager.TryGetSnapshotCachedPageReadBufferAndRecordRead(
                    uint.MaxValue,
                    snapshot,
                    out _));
            AssertLogicalReads(runtime.Pager, cacheHits: 1, cacheMisses: 0);
        }
        finally
        {
            runtime.Pager.ReleaseReaderSnapshot(snapshot);
        }
    }

    [Fact]
    public async Task TransactionModifiedPageRecordingProbe_IsExcluded()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(ct: ct);

        await using var transaction =
            await runtime.Pager.BeginWriteTransactionAsync(ct);
        using (transaction.Bind())
        {
            _ = await runtime.Pager.GetPageAsync(0, ct);
            StorageIoRuntimeRawSnapshot before = Capture(runtime.Pager);

            Assert.True(
                runtime.Pager.TryGetCachedPageReadBufferAndRecordRead(0, out _));
            Assert.Equal(before.LogicalReads, Capture(runtime.Pager).LogicalReads);
        }
    }

    [Fact]
    public async Task SnapshotPager_SharesCountersAndPublishesThenRemovesCacheLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var options = new PagerOptions
        {
            MaxCachedPages = 4,
            MaxCachedWalReadPages = 2,
        };
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(options, ct);
        AssertCache(
            runtime.Pager,
            sharedResident: 1,
            sharedCapacity: 4,
            walResident: 0,
            walCapacity: 2);

        WalSnapshot snapshot = runtime.Pager.AcquireReaderSnapshot();
        Pager reader = runtime.Pager.CreateSnapshotReader(snapshot);
        try
        {
            AssertCache(
                runtime.Pager,
                sharedResident: 1,
                sharedCapacity: 8,
                walResident: 0,
                walCapacity: 4);

            _ = await reader.GetPageAsync(0, ct);
            AssertCache(
                runtime.Pager,
                sharedResident: 2,
                sharedCapacity: 8,
                walResident: 0,
                walCapacity: 4);
            AssertLogicalReads(runtime.Pager, cacheHits: 0, cacheMisses: 1);
        }
        finally
        {
            await reader.DisposeAsync();
            runtime.Pager.ReleaseReaderSnapshot(snapshot);
        }

        AssertCache(
            runtime.Pager,
            sharedResident: 1,
            sharedCapacity: 4,
            walResident: 0,
            walCapacity: 2);
    }

    [Fact]
    public async Task CustomCache_IsUnsupportedWhileMemoryPhysicalIoIsNotApplicable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var options = new PagerOptions
        {
            PageCacheFactory = static () => new TestPageCache(),
        };
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(options, ct);

        StorageIoRuntimeRawSnapshot raw = Capture(runtime.Pager);
        Assert.Equal(StorageRuntimeDetailAvailabilityRaw.Unsupported, raw.CacheAvailability);
        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.NotApplicable,
            raw.PhysicalIoAvailability);
    }

    [Fact]
    public void CacheRegistry_InvalidInitialLeaseDoesNotSubtractAnotherLease()
    {
        var registry = new StorageCacheRuntimeDiagnostics();
        var validCache = new LruPageCache(4);
        validCache.Set(7, new byte[PageConstants.PageSize]);
        StorageCacheRuntimeDiagnostics.Lease valid = Assert.IsType<
            StorageCacheRuntimeDiagnostics.Lease>(
                registry.TryRegister(validCache, walResidentPages: 0, walCapacityPages: 2));
        StorageCacheRuntimeDiagnostics.Lease invalid = Assert.IsType<
            StorageCacheRuntimeDiagnostics.Lease>(
                registry.TryRegister(
                    new InvalidRuntimePageCache(),
                    walResidentPages: 0,
                    walCapacityPages: 0));

        try
        {
            Assert.Equal(
                StorageRuntimeDetailAvailabilityRaw.Unavailable,
                registry.Capture(out _));

            invalid.Dispose();
            Assert.Equal(
                StorageRuntimeDetailAvailabilityRaw.Available,
                registry.Capture(out StorageCacheRuntimeRawSnapshot raw));
            Assert.Equal(1, raw.SharedResidentPages);
            Assert.Equal(4, raw.SharedCapacityPages);
            Assert.Equal(0, raw.WalResidentPages);
            Assert.Equal(2, raw.WalCapacityPages);
        }
        finally
        {
            invalid.Dispose();
            valid.Dispose();
        }

        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.Unavailable,
            registry.Capture(out _));
    }

    [Fact]
    public async Task ThrowingAfterReadInterceptor_DoesNotRecordCacheHit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var options = new PagerOptions
        {
            Interceptors = new[] { new ThrowingAfterReadInterceptor() },
        };
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(options, ct);

        Assert.Throws<InvalidOperationException>(
            () => runtime.Pager.TryGetCachedPageAndRecordRead(0));
        AssertLogicalReads(runtime.Pager, cacheHits: 0, cacheMisses: 0);
    }

    [Fact]
    public async Task MutableWalRead_FinalSharedCacheFailureDoesNotRecordRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var cache = new ThrowingSetPageCache();
        var options = new PagerOptions
        {
            PageCacheFactory = () => cache,
            MaxCachedWalReadPages = 2,
        };
        await using ObservedMemoryPager runtime = await ObservedMemoryPager.OpenAsync(options, ct);
        var page = new byte[PageConstants.PageSize];
        page[0] = 42;
        WalCommitResult commit = await runtime.Wal.AppendFramesAndCommitAsync(
            new[] { new WalFrameWrite(1, page) },
            newDbPageCount: 2,
            ct);
        await commit.WaitAsync(ct);

        cache.ThrowOnSet = true;
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => _ = await runtime.Pager.GetPageAsync(1, ct));
            AssertLogicalReads(runtime.Pager, cacheHits: 0, cacheMisses: 0);
        }
        finally
        {
            cache.ThrowOnSet = false;
        }
    }

    [Fact]
    public async Task DisabledHotCacheHit_AllocatesNoDiagnosticsStateOrBytes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var device = new MemoryStorageDevice();
        var walIndex = new WalIndex();
        await using var wal = new MemoryWriteAheadLog(walIndex);
        await using Pager pager = await Pager.CreateAsync(
            device,
            wal,
            walIndex,
            new PagerOptions(),
            ct);
        await pager.InitializeNewDatabaseAsync(ct);

        for (int i = 0; i < 64; i++)
            _ = pager.TryGetCachedPageAndRecordRead(0);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1_000; i++)
            _ = pager.TryGetCachedPageAndRecordRead(0);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
        Assert.False(pager.TryGetStorageIoRuntimeDiagnosticsSnapshot(out _));
    }

    [Fact]
    public async Task EngineProjection_DerivesLogicalReadsAndContainsDetailAvailability()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-projection",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = state,
            },
            ct);

        StorageRuntimeDiagnosticsCapture capture = StorageRuntimeDiagnostics.Capture(
            state,
            state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Engine));
        StorageRuntimeDiagnosticsSnapshot storage = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(capture.Storage.Value);
        long hits = Assert.IsType<long>(storage.CacheHits);
        long misses = Assert.IsType<long>(storage.CacheMisses);
        long reads = Assert.IsType<long>(storage.PageReads);
        Assert.Equal(StorageRuntimeCounterMath.SaturatingAdd(hits, misses), reads);
        Assert.Equal(
            StorageRuntimeCounterMath.SaturatingMultiply(
                reads,
                PageConstants.PageSize),
            Assert.IsType<long>(storage.BytesRead));
        Assert.Equal(DiagnosticsAvailability.Available, storage.Cache.Availability);
        Assert.Equal(storage.Metadata, storage.Cache.Value!.Metadata);
        Assert.Equal(
            DiagnosticsAvailability.NotApplicable,
            storage.PhysicalIo.Availability);
    }

    [Fact]
    public async Task FileShutdownPhysicalIo_RetiresFinalDeltaAndReopenAddsItExactlyOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_io_shutdown_{Guid.NewGuid():N}.db");
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-shutdown",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var options = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
            StorageEngineFactory = new DefaultStorageEngineFactory(),
        };
        Database? first = null;
        Database? reopened = null;

        try
        {
            first = await Database.CreateNewAsync(path, options, ct);
            _ = await first.ExecuteAsync(
                "CREATE TABLE shutdown_io (id INTEGER PRIMARY KEY)",
                ct);
            LogicalPageReadRuntimeRawSnapshot logicalBeforeShutdown =
                Capture(GetPager(first)).LogicalReads;
            StorageDeviceIoRuntimeRawSnapshot beforeShutdown = ToRaw(
                Assert.IsType<StorageDeviceIoDiagnosticsSnapshot>(
                    CaptureEngine(state).Storage.Value!.PhysicalIo.Value));

            await first.DisposeAsync();
            first = null;
            StorageDeviceIoRuntimeRawSnapshot retiredAfterShutdown =
                GetRetiredPhysicalIo(state);
            Assert.Equal(
                StorageRuntimeDetailAvailabilityRaw.Available,
                GetRetiredPhysicalAvailability(state));
            Assert.True(retiredAfterShutdown.WriteCount > beforeShutdown.WriteCount);
            Assert.True(retiredAfterShutdown.FlushCount > beforeShutdown.FlushCount);
            Assert.Equal(
                logicalBeforeShutdown.CacheHits,
                GetRetiredLogicalReadCount(state, "_retiredCacheHitCount"));
            Assert.Equal(
                logicalBeforeShutdown.CacheMisses,
                GetRetiredLogicalReadCount(state, "_retiredCacheMissCount"));
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                CaptureEngine(state).Storage.Availability);

            reopened = await Database.OpenAsync(path, options, ct);
            StorageIoRuntimeRawSnapshot reopenedRaw = Capture(GetPager(reopened));
            Assert.Equal(
                StorageRuntimeDetailAvailabilityRaw.Available,
                reopenedRaw.PhysicalIoAvailability);
            StorageDeviceIoRuntimeRawSnapshot expectedAggregate = AddPhysical(
                retiredAfterShutdown,
                reopenedRaw.PhysicalIo);
            StorageDeviceIoRuntimeRawSnapshot projectedAggregate = ToRaw(
                Assert.IsType<StorageDeviceIoDiagnosticsSnapshot>(
                    CaptureEngine(state).Storage.Value!.PhysicalIo.Value));
            Assert.Equal(expectedAggregate, projectedAggregate);
            StorageRuntimeDiagnosticsSnapshot reopenedProjection = Assert.IsType<
                StorageRuntimeDiagnosticsSnapshot>(
                    CaptureEngine(state).Storage.Value);
            Assert.Equal(
                StorageRuntimeCounterMath.SaturatingAdd(
                    logicalBeforeShutdown.CacheHits,
                    reopenedRaw.LogicalReads.CacheHits),
                reopenedProjection.CacheHits);
            Assert.Equal(
                StorageRuntimeCounterMath.SaturatingAdd(
                    logicalBeforeShutdown.CacheMisses,
                    reopenedRaw.LogicalReads.CacheMisses),
                reopenedProjection.CacheMisses);

            StorageRuntimeDiagnostics.Registration reopenedRegistration =
                GetStorageRegistration(reopened);
            reopenedRegistration.DrainProvider();
            reopenedRegistration.DrainProvider();
            await reopened.DisposeAsync();
            StorageDeviceIoRuntimeRawSnapshot retiredOnce =
                GetRetiredPhysicalIo(state);
            long? retiredHitsOnce = GetRetiredLogicalReadCount(
                state,
                "_retiredCacheHitCount");
            long? retiredMissesOnce = GetRetiredLogicalReadCount(
                state,
                "_retiredCacheMissCount");
            reopenedRegistration.DrainProvider();
            Assert.Equal(retiredOnce, GetRetiredPhysicalIo(state));
            Assert.Equal(
                retiredHitsOnce,
                GetRetiredLogicalReadCount(state, "_retiredCacheHitCount"));
            Assert.Equal(
                retiredMissesOnce,
                GetRetiredLogicalReadCount(state, "_retiredCacheMissCount"));
            reopened = null;
        }
        finally
        {
            if (reopened is not null)
                await reopened.DisposeAsync();
            if (first is not null)
                await first.DisposeAsync();
            DeleteDatabaseFiles(path);
        }
    }

    [Fact]
    public async Task FileAndMemoryLiveAggregation_UsesFilePhysicalAndLiveCacheGauges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_storage_io_mixed_{Guid.NewGuid():N}.db");
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-mixed",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var options = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
            StorageEngineFactory = new DefaultStorageEngineFactory(),
        };
        Database? file = null;
        Database? memory = null;

        try
        {
            file = await Database.CreateNewAsync(path, options, ct);
            memory = await Database.OpenInMemoryAsync(options, ct);
            StorageIoRuntimeRawSnapshot fileRaw = Capture(GetPager(file));
            StorageIoRuntimeRawSnapshot memoryRaw = Capture(GetPager(memory));
            StorageRuntimeDiagnosticsSnapshot aggregate = Assert.IsType<
                StorageRuntimeDiagnosticsSnapshot>(
                    CaptureEngine(state).Storage.Value);

            Assert.Equal(DiagnosticsAvailability.Available, aggregate.Cache.Availability);
            Assert.Equal(
                StorageRuntimeCounterMath.SaturatingAdd(
                    fileRaw.Cache.SharedResidentPages,
                    memoryRaw.Cache.SharedResidentPages),
                aggregate.Cache.Value!.SharedResidentPages);
            Assert.Null(aggregate.Cache.Value.SharedCapacityPages);
            Assert.Equal(
                StorageRuntimeCounterMath.SaturatingAdd(
                    fileRaw.Cache.WalResidentPages,
                    memoryRaw.Cache.WalResidentPages),
                aggregate.Cache.Value.WalResidentPages);
            Assert.Equal(
                StorageRuntimeCounterMath.SaturatingAdd(
                    fileRaw.Cache.WalCapacityPages,
                    memoryRaw.Cache.WalCapacityPages),
                aggregate.Cache.Value.WalCapacityPages);
            Assert.Equal(DiagnosticsAvailability.Available, aggregate.PhysicalIo.Availability);
            Assert.Equal(
                fileRaw.PhysicalIo,
                ToRaw(aggregate.PhysicalIo.Value!));

            await memory.DisposeAsync();
            memory = null;
            StorageIoRuntimeRawSnapshot fileOnlyRaw = Capture(GetPager(file));
            StorageRuntimeDiagnosticsSnapshot fileOnly = Assert.IsType<
                StorageRuntimeDiagnosticsSnapshot>(
                    CaptureEngine(state).Storage.Value);
            Assert.Equal(
                fileOnlyRaw.Cache,
                ToRaw(fileOnly.Cache.Value!));

            await file.DisposeAsync();
            file = null;
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                CaptureEngine(state).Storage.Availability);
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
    public async Task LogicalReadLifetimeCounters_SaturateWithLiveContributions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-logical-saturation",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = state,
            },
            ct);
        StorageRuntimeDiagnostics diagnostics = GetStorageDiagnostics(state);
        SetPrivateField(
            diagnostics,
            "_retiredCacheHitCount",
            long.MaxValue - 1);
        SetPrivateField(
            diagnostics,
            "_retiredCacheMissCount",
            long.MaxValue - 1);
        Pager pager = GetPager(database);
        _ = await pager.GetPageAsync(0, ct);
        Assert.True(pager.TryGetCachedPageReadBufferAndRecordRead(0, out _));
        _ = await pager.ReadPageUncachedAsync(0, ct);

        StorageRuntimeDiagnosticsSnapshot storage = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(
                CaptureEngine(state).Storage.Value);
        Assert.Equal(long.MaxValue, storage.CacheHits);
        Assert.Equal(long.MaxValue, storage.CacheMisses);
        Assert.Equal(long.MaxValue, storage.PageReads);
        Assert.Equal(long.MaxValue, storage.BytesRead);
    }

    [Fact]
    public async Task InvalidLogicalFinalCapture_PoisonsOnlyLogicalReadScalars()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-invalid-logical-final",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        var options = new DatabaseOptions
        {
            ObservabilityOptions = observability,
            RuntimeDiagnosticsState = state,
        };

        await using (Database invalid = await Database.OpenInMemoryAsync(options, ct))
        {
            LogicalPageReadRuntimeCounters logicalReads = GetLogicalReadCounters(
                GetPager(invalid));
            SetPrivateField(logicalReads, "_cacheHits", -1L);
            GetStorageRegistration(invalid).DrainProvider();
        }

        await using Database replacement = await Database.OpenInMemoryAsync(options, ct);
        StorageRuntimeDiagnosticsSnapshot storage = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(
                CaptureEngine(state).Storage.Value);
        Assert.Null(storage.CacheHits);
        Assert.Null(storage.CacheMisses);
        Assert.Null(storage.PageReads);
        Assert.Null(storage.BytesRead);
        Assert.Equal(DiagnosticsAvailability.Available, storage.Cache.Availability);
        Assert.Equal(
            DiagnosticsAvailability.NotApplicable,
            storage.PhysicalIo.Availability);
    }

    [Fact]
    public void PhysicalSealBeforePromotion_IsRetainedAndIdenticalDuplicateIsIdempotent()
    {
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-pre-promotion-seal",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        using StorageRuntimeDiagnostics.Registration registration = Assert.IsType<
            StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));
        var sample = new StorageDeviceIoRuntimeRawSnapshot(
            ReadCount: 1,
            BytesRead: PageConstants.PageSize,
            WriteCount: 2,
            BytesWritten: PageConstants.PageSize * 2L,
            FlushCount: 1,
            ResizeCount: 1,
            SequentialReadCount: 0,
            SequentialBytesRead: 0,
            MemoryMappedPageExposureCount: 0,
            MemoryMappedBytesExposed: 0);

        registration.Observer.OnStorageDeviceIoSealed(in sample);
        registration.Observer.OnStorageDeviceIoSealed(in sample);

        Assert.Equal(sample, registration.PendingFinalPhysicalIo);
        Assert.Equal(sample, registration.FinalPhysicalIoSample);
        Assert.False(registration.PendingPhysicalIoInvalid);
        Assert.False(registration.FinalPhysicalIoReconciled);
        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.NotApplicable,
            GetRetiredPhysicalAvailability(state));
    }

    [Fact]
    public void PhysicalSealAfterDrain_AppliesDeltaOnceAndPoisonsOnlyConflictingDuplicate()
    {
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-io-post-drain-seal",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        using StorageRuntimeDiagnostics.Registration registration = Assert.IsType<
            StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: false));
        StorageRuntimeDiagnostics diagnostics = GetStorageDiagnostics(state);
        var watermark = new StorageDeviceIoRuntimeRawSnapshot(
            ReadCount: 2,
            BytesRead: PageConstants.PageSize * 2L,
            WriteCount: 3,
            BytesWritten: PageConstants.PageSize * 3L,
            FlushCount: 1,
            ResizeCount: 1,
            SequentialReadCount: 1,
            SequentialBytesRead: PageConstants.PageSize,
            MemoryMappedPageExposureCount: 1,
            MemoryMappedBytesExposed: PageConstants.PageSize);
        var terminal = new StorageDeviceIoRuntimeRawSnapshot(
            ReadCount: 3,
            BytesRead: PageConstants.PageSize * 3L,
            WriteCount: 5,
            BytesWritten: PageConstants.PageSize * 5L,
            FlushCount: 2,
            ResizeCount: 2,
            SequentialReadCount: 1,
            SequentialBytesRead: PageConstants.PageSize,
            MemoryMappedPageExposureCount: 2,
            MemoryMappedBytesExposed: PageConstants.PageSize * 2L);
        SetPrivateField(
            diagnostics,
            "_retiredPhysicalIoAvailability",
            StorageRuntimeDetailAvailabilityRaw.Available);
        SetPrivateField(diagnostics, "_retiredPhysicalIo", watermark);
        registration.ProviderDrained = true;
        registration.RetiredPhysicalIoWatermark = watermark;

        registration.Observer.OnStorageDeviceIoSealed(in terminal);
        Assert.Equal(terminal, GetRetiredPhysicalIo(state));
        Assert.True(registration.FinalPhysicalIoReconciled);

        registration.Observer.OnStorageDeviceIoSealed(in terminal);
        Assert.Equal(terminal, GetRetiredPhysicalIo(state));
        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.Available,
            GetRetiredPhysicalAvailability(state));

        StorageDeviceIoRuntimeRawSnapshot conflicting = terminal with
        {
            ReadCount = terminal.ReadCount + 1,
        };
        registration.Observer.OnStorageDeviceIoSealed(in conflicting);
        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.Unavailable,
            GetRetiredPhysicalAvailability(state));
    }

    private static StorageIoRuntimeRawSnapshot Capture(Pager pager)
    {
        Assert.True(
            pager.TryGetStorageIoRuntimeDiagnosticsSnapshot(
                out StorageIoRuntimeRawSnapshot raw));
        return raw;
    }

    private static StorageRuntimeDiagnostics GetStorageDiagnostics(
        CSharpDbRuntimeDiagnosticsState state)
    {
        Assert.True(
            state.TryGetComponent<StorageRuntimeDiagnostics>(
                out StorageRuntimeDiagnostics? diagnostics));
        return Assert.IsType<StorageRuntimeDiagnostics>(diagnostics);
    }

    private static StorageRuntimeDiagnosticsCapture CaptureEngine(
        CSharpDbRuntimeDiagnosticsState state)
        => StorageRuntimeDiagnostics.Capture(
            state,
            state.CreateMetadata(
                DiagnosticsScope.Instance,
                DiagnosticsAvailability.Available,
                DiagnosticsSource.Engine));

    private static Pager GetPager(Database database)
        => Assert.IsType<Pager>(
            typeof(Database)
                .GetField(
                    "_pager",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic)!
                .GetValue(database));

    private static StorageRuntimeDiagnostics.Registration GetStorageRegistration(
        Database database)
        => Assert.IsType<StorageRuntimeDiagnostics.Registration>(
            typeof(Database)
                .GetField(
                    "_storageRuntimeDiagnosticsRegistration",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic)!
                .GetValue(database));

    private static LogicalPageReadRuntimeCounters GetLogicalReadCounters(
        Pager pager)
    {
        StorageIoRuntimeDiagnostics diagnostics = Assert.IsType<
            StorageIoRuntimeDiagnostics>(
                typeof(Pager)
                    .GetField(
                        "_storageIoRuntimeDiagnostics",
                        System.Reflection.BindingFlags.Instance |
                        System.Reflection.BindingFlags.NonPublic)!
                    .GetValue(pager));
        return diagnostics.LogicalReads;
    }

    private static StorageCacheRuntimeRawSnapshot ToRaw(
        StorageCacheDiagnosticsSnapshot snapshot)
        => new(
            snapshot.SharedResidentPages,
            snapshot.SharedCapacityPages,
            snapshot.WalResidentPages,
            snapshot.WalCapacityPages);

    private static StorageDeviceIoRuntimeRawSnapshot ToRaw(
        StorageDeviceIoDiagnosticsSnapshot snapshot)
        => new(
            snapshot.ReadCount,
            snapshot.BytesRead,
            snapshot.WriteCount,
            snapshot.BytesWritten,
            snapshot.FlushCount,
            snapshot.ResizeCount,
            snapshot.SequentialReadCount,
            snapshot.SequentialBytesRead,
            snapshot.MemoryMappedPageExposureCount,
            snapshot.MemoryMappedBytesExposed);

    private static StorageDeviceIoRuntimeRawSnapshot AddPhysical(
        in StorageDeviceIoRuntimeRawSnapshot left,
        in StorageDeviceIoRuntimeRawSnapshot right)
        => new(
            StorageRuntimeCounterMath.SaturatingAdd(left.ReadCount, right.ReadCount),
            StorageRuntimeCounterMath.SaturatingAdd(left.BytesRead, right.BytesRead),
            StorageRuntimeCounterMath.SaturatingAdd(left.WriteCount, right.WriteCount),
            StorageRuntimeCounterMath.SaturatingAdd(left.BytesWritten, right.BytesWritten),
            StorageRuntimeCounterMath.SaturatingAdd(left.FlushCount, right.FlushCount),
            StorageRuntimeCounterMath.SaturatingAdd(left.ResizeCount, right.ResizeCount),
            StorageRuntimeCounterMath.SaturatingAdd(
                left.SequentialReadCount,
                right.SequentialReadCount),
            StorageRuntimeCounterMath.SaturatingAdd(
                left.SequentialBytesRead,
                right.SequentialBytesRead),
            StorageRuntimeCounterMath.SaturatingAdd(
                left.MemoryMappedPageExposureCount,
                right.MemoryMappedPageExposureCount),
            StorageRuntimeCounterMath.SaturatingAdd(
                left.MemoryMappedBytesExposed,
                right.MemoryMappedBytesExposed));

    private static void DeleteDatabaseFiles(string path)
    {
        File.Delete(path);
        File.Delete(path + ".wal");
    }

    private static StorageRuntimeDetailAvailabilityRaw
        GetRetiredPhysicalAvailability(CSharpDbRuntimeDiagnosticsState state)
        => Assert.IsType<StorageRuntimeDetailAvailabilityRaw>(
            typeof(StorageRuntimeDiagnostics)
                .GetField(
                    "_retiredPhysicalIoAvailability",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic)!
                .GetValue(GetStorageDiagnostics(state)));

    private static StorageDeviceIoRuntimeRawSnapshot GetRetiredPhysicalIo(
        CSharpDbRuntimeDiagnosticsState state)
        => Assert.IsType<StorageDeviceIoRuntimeRawSnapshot>(
            typeof(StorageRuntimeDiagnostics)
                .GetField(
                    "_retiredPhysicalIo",
                    System.Reflection.BindingFlags.Instance |
                    System.Reflection.BindingFlags.NonPublic)!
                .GetValue(GetStorageDiagnostics(state)));

    private static long? GetRetiredLogicalReadCount(
        CSharpDbRuntimeDiagnosticsState state,
        string fieldName)
        => (long?)typeof(StorageRuntimeDiagnostics)
            .GetField(
                fieldName,
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!
            .GetValue(GetStorageDiagnostics(state));

    private static void SetPrivateField(
        object instance,
        string fieldName,
        object value)
        => instance.GetType()
            .GetField(
                fieldName,
                System.Reflection.BindingFlags.Instance |
                System.Reflection.BindingFlags.NonPublic)!
            .SetValue(instance, value);

    private static void AssertLogicalReads(
        Pager pager,
        long cacheHits,
        long cacheMisses)
    {
        LogicalPageReadRuntimeRawSnapshot raw = Capture(pager).LogicalReads;
        Assert.Equal(cacheHits, raw.CacheHits);
        Assert.Equal(cacheMisses, raw.CacheMisses);
    }

    private static void AssertCache(
        Pager pager,
        long sharedResident,
        long? sharedCapacity,
        long walResident,
        long walCapacity)
    {
        StorageIoRuntimeRawSnapshot capture = Capture(pager);
        Assert.Equal(
            StorageRuntimeDetailAvailabilityRaw.Available,
            capture.CacheAvailability);
        Assert.Equal(sharedResident, capture.Cache.SharedResidentPages);
        Assert.Equal(sharedCapacity, capture.Cache.SharedCapacityPages);
        Assert.Equal(walResident, capture.Cache.WalResidentPages);
        Assert.Equal(walCapacity, capture.Cache.WalCapacityPages);
    }

    private sealed class ObservedMemoryPager : IAsyncDisposable
    {
        private ObservedMemoryPager(Pager pager, MemoryWriteAheadLog wal)
        {
            Pager = pager;
            Wal = wal;
        }

        internal Pager Pager { get; }

        internal MemoryWriteAheadLog Wal { get; }

        internal static async ValueTask<ObservedMemoryPager> OpenAsync(
            PagerOptions? options = null,
            CancellationToken ct = default)
        {
            var device = new MemoryStorageDevice();
            var walIndex = new WalIndex();
            var observer = NoOpObserver.Instance;
            var wal = new MemoryWriteAheadLog(
                walIndex,
                checksumProvider: null,
                initialBytes: default,
                runtimeDiagnosticsObserver: observer);
            Pager? pager = null;
            try
            {
                pager = await Pager.CreateAsync(
                    device,
                    wal,
                    walIndex,
                    options ?? new PagerOptions(),
                    observer,
                    ct);
                await pager.InitializeNewDatabaseAsync(ct);
                return new ObservedMemoryPager(pager, wal);
            }
            catch
            {
                if (pager is not null)
                    await pager.DisposeAsync();
                else
                    await device.DisposeAsync();
                await wal.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await Pager.DisposeAsync();
            }
            finally
            {
                await Wal.DisposeAsync();
            }
        }
    }

    private class TestPageCache : IPageCache
    {
        protected readonly Dictionary<uint, byte[]> Pages = [];

        public bool TryGet(uint pageId, out byte[] page)
            => Pages.TryGetValue(pageId, out page!);

        public virtual void Set(uint pageId, byte[] page)
            => Pages[pageId] = page;

        public bool Contains(uint pageId) => Pages.ContainsKey(pageId);

        public bool Remove(uint pageId) => Pages.Remove(pageId);

        public void Clear() => Pages.Clear();
    }

    private sealed class ThrowingSetPageCache : TestPageCache
    {
        internal bool ThrowOnSet { get; set; }

        public override void Set(uint pageId, byte[] page)
        {
            if (ThrowOnSet)
                throw new InvalidOperationException("Synthetic cache-set failure.");

            base.Set(pageId, page);
        }
    }

    private sealed class InvalidRuntimePageCache :
        TestPageCache,
        IPageCacheRuntimeDiagnosticsProvider
    {
        long IPageCacheRuntimeDiagnosticsProvider.RuntimeResidentPageCount => -1;

        long? IPageCacheRuntimeDiagnosticsProvider.RuntimeCapacityPageCount => 4;
    }

    private sealed class ThrowingAfterReadInterceptor : IPageOperationInterceptor
    {
        public ValueTask OnBeforeReadAsync(
            uint pageId,
            CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(
            uint pageId,
            PageReadSource source,
            CancellationToken ct = default)
            => throw new InvalidOperationException("Synthetic after-read failure.");

        public ValueTask OnBeforeWriteAsync(
            uint pageId,
            CancellationToken ct = default) => ValueTask.CompletedTask;

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

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(
            bool succeeded,
            CancellationToken ct = default) => ValueTask.CompletedTask;
    }

    private sealed class NoOpObserver : IStorageRuntimeDiagnosticsObserver
    {
        internal static NoOpObserver Instance { get; } = new();

        public object? CaptureCheckpointCorrelation(StorageCheckpointOriginRaw origin)
            => null;

        public object? CaptureCheckpointCompletionCorrelation() => null;

        public void OnRecoveryStarted() { }

        public void OnRecoveryChanged(in StorageRecoveryRuntimeRawSnapshot snapshot) { }

        public void OnRecoveryCompleted(in StorageRecoveryRuntimeRawSnapshot snapshot) { }

        public void OnCheckpointStarted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }

        public void OnCheckpointChanged(in StorageCheckpointRuntimeRawSnapshot snapshot) { }

        public void OnCheckpointCompleted(
            in StorageCheckpointRuntimeRawSnapshot snapshot,
            object? correlation) { }
    }
}
