using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Composition root for the lazy-resident hybrid mode. The base file and WAL stay
/// on disk, but owned pages read into the pager cache stay resident across
/// checkpoints according to the configured cache policy.
/// </summary>
internal static class HybridStorageEngineFactory
{
    public static async ValueTask<StorageEngineContext> OpenAsync(
        string filePath,
        StorageEngineOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(options);

        string fullPath = Path.GetFullPath(filePath);
        bool isNew = !File.Exists(fullPath);
        var hybridPagerOptions = CreateHybridPagerOptions(options.PagerOptions);
        FileStorageDevice? device = null;
        Pager? pager = null;

        try
        {
            device = new FileStorageDevice(fullPath);
            var walIndex = new WalIndex();
            var wal = new WriteAheadLog(
                fullPath,
                walIndex,
                options.ChecksumProvider,
                options.DurabilityMode,
                options.DurableGroupCommit.BatchWindow,
                options.WalPreallocationChunkBytes);
            pager = await Pager.CreateAsync(device, wal, walIndex, hybridPagerOptions, ct);

            if (isNew)
            {
                await pager.InitializeNewDatabaseAsync(ct);
                await wal.OpenAsync(pager.PageCount, ct);
            }
            else
            {
                await pager.RecoverAsync(ct);
            }

            var schemaSerializer = options.SerializerProvider.SchemaSerializer;
            return new StorageEngineContext
            {
                Pager = pager,
                Catalog = await SchemaCatalog.CreateAsync(
                    pager,
                    schemaSerializer,
                    options.IndexProvider,
                    options.CatalogStore,
                    options.AdvisoryStatisticsPersistenceMode,
                    ct),
                RecordSerializer = options.SerializerProvider.RecordSerializer,
                SchemaSerializer = schemaSerializer,
                IndexProvider = options.IndexProvider,
                CatalogStore = options.CatalogStore,
                ChecksumProvider = options.ChecksumProvider,
                AdvisoryStatisticsPersistenceMode = options.AdvisoryStatisticsPersistenceMode,
            };
        }
        catch
        {
            if (pager != null)
                await pager.DisposeAsync();
            if (device != null)
                await device.DisposeAsync();

            throw;
        }
    }

    private static PagerOptions CreateHybridPagerOptions(PagerOptions pagerOptions)
    {
        return new PagerOptions
        {
            WriterLockTimeout = pagerOptions.WriterLockTimeout,
            CheckpointPolicy = pagerOptions.CheckpointPolicy,
            AutoCheckpointExecutionMode = pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = pagerOptions.AutoCheckpointMaxPagesPerStep,
            MaxCachedPages = pagerOptions.MaxCachedPages,
            MaxCachedWalReadPages = pagerOptions.MaxCachedWalReadPages,
            PageCacheFactory = pagerOptions.PageCacheFactory,
            Interceptors = pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = pagerOptions.UseMemoryMappedReads,
            EnableSequentialLeafReadAhead = pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = true,
        };
    }
}
