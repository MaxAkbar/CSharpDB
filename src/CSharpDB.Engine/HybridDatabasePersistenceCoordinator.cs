namespace CSharpDB.Engine;

internal sealed class HybridDatabasePersistenceCoordinator : IDisposable
{
    private readonly string _backingFilePath;
    private readonly HybridPersistenceTriggers _persistenceTriggers;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public HybridDatabasePersistenceCoordinator(
        string backingFilePath,
        HybridPersistenceTriggers persistenceTriggers)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(backingFilePath);

        _backingFilePath = Path.GetFullPath(backingFilePath);
        _persistenceTriggers = persistenceTriggers;
    }

    public async ValueTask PersistAsync(
        Database database,
        HybridPersistenceTriggers trigger,
        bool writeScopeHeld = false,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        if ((_persistenceTriggers & trigger) == 0)
            return;

        await _gate.WaitAsync(ct);
        try
        {
            await database.SaveToFileAsync(_backingFilePath, writeScopeHeld, ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _gate.Dispose();
    }
}
