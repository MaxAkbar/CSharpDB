using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Internal composition root for in-memory database instances.
/// </summary>
internal static class InMemoryStorageEngineFactory
{
    public static async ValueTask<StorageEngineContext> OpenAsync(
        StorageEngineOptions options,
        ReadOnlyMemory<byte> databaseBytes = default,
        ReadOnlyMemory<byte> walBytes = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        MemoryStorageDevice? device = null;
        Pager? pager = null;

        try
        {
            device = new MemoryStorageDevice(databaseBytes);
            var walIndex = new WalIndex();
            var wal = new MemoryWriteAheadLog(walIndex, options.ChecksumProvider, walBytes);
            pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

            if (databaseBytes.Length >= PageConstants.PageSize)
            {
                await pager.RecoverAsync(ct);
            }
            else
            {
                await pager.InitializeNewDatabaseAsync(ct);
                await wal.OpenAsync(pager.PageCount, ct);
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
