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

        var device = new MemoryStorageDevice(databaseBytes);
        var walIndex = new WalIndex();
        var wal = new MemoryWriteAheadLog(walIndex, options.ChecksumProvider, walBytes);
        var pager = await Pager.CreateAsync(device, wal, walIndex, options.PagerOptions, ct);

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
