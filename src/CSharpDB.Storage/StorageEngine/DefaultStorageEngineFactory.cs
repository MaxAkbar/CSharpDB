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
        var device = new FileStorageDevice(filePath);
        var walIndex = new WalIndex();
        var wal = new WriteAheadLog(
            filePath,
            walIndex,
            options.ChecksumProvider,
            options.DurabilityMode,
            options.DurableCommitBatchWindow,
            options.WalPreallocationChunkBytes);
        var pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

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
        var catalog = await SchemaCatalog.CreateAsync(
            pager,
            schemaSerializer,
            options.IndexProvider,
            options.CatalogStore,
            ct);

        return new StorageEngineContext
        {
            Pager = pager,
            Catalog = catalog,
            RecordSerializer = options.SerializerProvider.RecordSerializer,
            SchemaSerializer = schemaSerializer,
            IndexProvider = options.IndexProvider,
            ChecksumProvider = options.ChecksumProvider,
        };
    }
}
