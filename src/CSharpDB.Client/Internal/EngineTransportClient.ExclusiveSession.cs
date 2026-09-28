using CSharpDB.Engine;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public bool SupportsExclusiveSessions => Path.IsPathFullyQualified(_databasePath)
        && _directDatabaseOptions.StorageEngineFactory.GetType() == typeof(DefaultStorageEngineFactory)
        && (_hybridDatabaseOptions is null || _hybridDatabaseOptions.PersistenceMode == HybridPersistenceMode.IncrementalDurable);

    public async ValueTask<CSharpDbExclusiveSession> OpenExclusiveSessionAsync(CancellationToken ct = default)
    {
        if (!SupportsExclusiveSessions) throw new NotSupportedException("Exclusive sessions require a standard direct file or durable hybrid database.");
        var ownership = await AcquireExclusiveDatabaseAccessAsync(ct,
            "Close active snapshot readers before starting an exclusive operation.",
            "Finish active transactions before starting an exclusive operation.");
        EngineTransportClient? session = null;
        try
        {
            if (_activeFinalizations != 0) throw new InvalidOperationException("Wait for transaction completion before starting an exclusive operation.");
            var source = _directDatabaseOptions;
            var storage = source.StorageEngineOptions;
            var options = new DatabaseOptions
            {
                StorageEngineOptions = new StorageEngineOptions
                {
                    PrimaryFileShare = FileShare.Read,
                    DurabilityMode = storage.DurabilityMode,
                    DurableGroupCommit = storage.DurableGroupCommit,
                    AdvisoryStatisticsPersistenceMode = storage.AdvisoryStatisticsPersistenceMode,
                    WalPreallocationChunkBytes = storage.WalPreallocationChunkBytes,
                    PagerOptions = storage.PagerOptions,
                    SerializerProvider = storage.SerializerProvider,
                    IndexProvider = storage.IndexProvider,
                    CatalogStore = storage.CatalogStore,
                    ChecksumProvider = storage.ChecksumProvider
                },
                Functions = source.Functions,
                ImplicitInsertExecutionMode = source.ImplicitInsertExecutionMode,
                AdaptiveQueryReoptimization = source.AdaptiveQueryReoptimization,
                WindowExecution = source.WindowExecution
                // Private maintenance SQL must not enter host SQL diagnostics.
            };
            session = new EngineTransportClient(_databasePath, options, _hybridDatabaseOptions);
            _ = await session.GetDatabaseAsync(ct); // Establish the OS writer barrier before returning.
            var captured = session;
            return new(captured, async () =>
            {
                try { await captured.DisposeAsync(); }
                finally { await ownership.DisposeAsync(); }
            });
        }
        catch
        {
            try { if (session is not null) await session.DisposeAsync(); }
            finally { await ownership.DisposeAsync(); }
            throw;
        }
    }
}
