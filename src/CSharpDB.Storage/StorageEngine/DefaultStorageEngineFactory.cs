namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Default composition root for storage engine components.
/// </summary>
public sealed class DefaultStorageEngineFactory : IStorageEngineFactory
{
    public async ValueTask<StorageEngineContext> OpenAsync(
        string filePath,
        StorageEngineOptions options,
        CancellationToken ct = default)
    {
        bool isNew = !File.Exists(filePath);
        FileStorageDevice? device = null;
        Pager? pager = null;

        try
        {
            device = new FileStorageDevice(filePath);
            var walIndex = new WalIndex();
            var wal = new WriteAheadLog(
                filePath,
                walIndex,
                options.ChecksumProvider,
                options.DurabilityMode,
                options.DurableCommitBatchWindow,
                options.WalPreallocationChunkBytes);
            pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

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
}
