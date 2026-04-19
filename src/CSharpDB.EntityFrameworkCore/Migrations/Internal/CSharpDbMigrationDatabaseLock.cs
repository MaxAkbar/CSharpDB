using CSharpDB.Data;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CSharpDB.EntityFrameworkCore.Migrations.Internal;

internal sealed class CSharpDbMigrationDatabaseLock : IMigrationsDatabaseLock, IDisposable, IAsyncDisposable
{
    private readonly CSharpDbHistoryRepository _historyRepository;
    private readonly CSharpDbConnection _connection;

    public CSharpDbMigrationDatabaseLock(CSharpDbHistoryRepository historyRepository, CSharpDbConnection connection)
    {
        _historyRepository = historyRepository;
        _connection = connection;
    }

    public IHistoryRepository HistoryRepository => _historyRepository;

    public IMigrationsDatabaseLock ReacquireIfNeeded(bool connectionReopened, bool? transactionRestarted)
        => this;

    public async Task<IMigrationsDatabaseLock> ReacquireIfNeededAsync(
        bool connectionReopened,
        bool? transactionRestarted,
        CancellationToken cancellationToken = default)
        => this;

    public void Dispose()
        => _historyRepository.ReleaseDatabaseLock(_connection);

    public ValueTask DisposeAsync()
    {
        _historyRepository.ReleaseDatabaseLock(_connection);
        return ValueTask.CompletedTask;
    }
}
