using System.Data;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CSharpDB.EntityFrameworkCore.Migrations.Internal;

public sealed class CSharpDbHistoryRepository : HistoryRepository
{
    private const string LockTableName = "__EFMigrationsLock";
    private const int LockRetryCount = 100;
    private static readonly TimeSpan LockRetryDelay = TimeSpan.FromMilliseconds(100);

    public CSharpDbHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
        EnsureFileBackedDatabase();
    }

    protected override string ExistsSql => "SELECT 1";

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

    public override bool Exists()
        => ExistsAsync().GetAwaiter().GetResult();

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => await WithOpenConnectionAsync(
            static (connection, repository, ct) => Task.FromResult(connection.GetTableNames().Contains(repository.TableName, StringComparer.OrdinalIgnoreCase)),
            cancellationToken);

    public override IReadOnlyList<HistoryRow> GetAppliedMigrations()
        => GetAppliedMigrationsAsync().GetAwaiter().GetResult();

    public override async Task<IReadOnlyList<HistoryRow>> GetAppliedMigrationsAsync(CancellationToken cancellationToken = default)
    {
        return await WithOpenConnectionAsync<IReadOnlyList<HistoryRow>>(
            async (connection, repository, ct) =>
            {
                if (!connection.GetTableNames().Contains(repository.TableName, StringComparer.OrdinalIgnoreCase))
                    return Array.Empty<HistoryRow>();

                await using var command = connection.CreateCommand();
                command.CommandText = $"SELECT {repository.MigrationIdColumnName}, {repository.ProductVersionColumnName} FROM {repository.TableName} ORDER BY {repository.MigrationIdColumnName}";

                var rows = new List<HistoryRow>();
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    rows.Add(new HistoryRow(reader.GetString(0), reader.GetString(1)));

                return rows;
            },
            cancellationToken);
    }

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => AcquireDatabaseLockAsync().GetAwaiter().GetResult();

    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
    {
        EnsureFileBackedDatabase();

        var connection = GetOrCreateConnection();
        if (connection.State != ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        if (!connection.GetTableNames().Contains(LockTableName, StringComparer.OrdinalIgnoreCase))
        {
            await using var createCommand = connection.CreateCommand();
            createCommand.CommandText = $"CREATE TABLE {LockTableName} (id INTEGER PRIMARY KEY)";
            await createCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        for (int attempt = 0; attempt < LockRetryCount; attempt++)
        {
            try
            {
                await using var insertCommand = connection.CreateCommand();
                insertCommand.CommandText = $"INSERT INTO {LockTableName} (id) VALUES (1)";
                await insertCommand.ExecuteNonQueryAsync(cancellationToken);
                return new CSharpDbMigrationDatabaseLock(this, connection);
            }
            catch (CSharpDbDataException) when (attempt < LockRetryCount - 1)
            {
                await Task.Delay(LockRetryDelay, cancellationToken);
            }
        }

        throw new TimeoutException(
            $"Failed to acquire the CSharpDB migrations lock in table '{LockTableName}'. Delete the stale row manually if a previous migration crashed.");
    }

    public override string GetBeginIfExistsScript(string migrationId)
        => throw new NotSupportedException("Idempotent migration scripts are not supported by the CSharpDB EF Core provider.");

    public override string GetBeginIfNotExistsScript(string migrationId)
        => throw new NotSupportedException("Idempotent migration scripts are not supported by the CSharpDB EF Core provider.");

    public override string GetCreateIfNotExistsScript()
        => $"""
CREATE TABLE IF NOT EXISTS {TableName} (
    {MigrationIdColumnName} TEXT NOT NULL PRIMARY KEY,
    {ProductVersionColumnName} TEXT NOT NULL
);
""";

    public override string GetEndIfScript()
        => throw new NotSupportedException("Idempotent migration scripts are not supported by the CSharpDB EF Core provider.");

    protected override bool InterpretExistsResult(object? value)
        => value switch
        {
            null => false,
            int intValue => intValue != 0,
            long longValue => longValue != 0,
            bool boolValue => boolValue,
            _ => true,
        };

    internal void ReleaseDatabaseLock(CSharpDbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
            return;

        using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {LockTableName} WHERE id = 1";
        command.ExecuteNonQuery();
    }

    private void EnsureFileBackedDatabase()
    {
        var builder = new CSharpDbConnectionStringBuilder(GetOrCreateConnection().ConnectionString);
        if (CSharpDbProviderValidation.IsPrivateMemory(builder.DataSource))
        {
            throw new NotSupportedException(
                "EF Core migrations are only supported for file-backed CSharpDB databases in v1. Use a file-backed Data Source for Database.Migrate and dotnet ef database update.");
        }
    }

    private CSharpDbConnection GetOrCreateConnection()
    {
        if (Dependencies.Connection.DbConnection is CSharpDbConnection connection)
            return connection;

        return new CSharpDbConnection(Dependencies.Connection.ConnectionString ?? string.Empty);
    }

    private async Task<TResult> WithOpenConnectionAsync<TResult>(
        Func<CSharpDbConnection, CSharpDbHistoryRepository, CancellationToken, Task<TResult>> action,
        CancellationToken cancellationToken)
    {
        var connection = GetOrCreateConnection();
        bool closeWhenDone = connection.State != ConnectionState.Open;
        if (closeWhenDone)
            await connection.OpenAsync(cancellationToken);

        try
        {
            return await action(connection, this, cancellationToken);
        }
        finally
        {
            if (closeWhenDone)
                await connection.CloseAsync();
        }
    }
}
