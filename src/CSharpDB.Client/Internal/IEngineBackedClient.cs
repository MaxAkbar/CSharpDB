using CSharpDB.Engine;

namespace CSharpDB.Client.Internal;

internal interface IEngineBackedClient
{
    ValueTask<Database?> TryGetDatabaseAsync(CancellationToken ct = default);
    ValueTask ReleaseCachedDatabaseAsync(CancellationToken ct = default);
}
