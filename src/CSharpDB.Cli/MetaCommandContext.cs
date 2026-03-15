using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Execution;
using System.Runtime.ExceptionServices;

namespace CSharpDB.Cli;

internal sealed class MetaCommandContext : IDisposable
{
    private readonly Func<string, CancellationToken, ValueTask<bool>> _executeSql;
    private readonly IEngineBackedClient? _engineBackedClient;
    private Database? _localDatabase;
    private Database.ReaderSession? _snapshot;
    private string? _transactionId;

    public MetaCommandContext(
        ICSharpDbClient client,
        Database? localDatabase,
        string databasePath,
        Func<string, CancellationToken, ValueTask<bool>> executeSql)
    {
        Client = client;
        _engineBackedClient = client as IEngineBackedClient;
        _localDatabase = localDatabase;
        DatabasePath = databasePath;
        _executeSql = executeSql;
    }

    public ICSharpDbClient Client { get; }
    public Database? LocalDatabase => _localDatabase;
    public string DatabasePath { get; }
    public bool ShowTiming { get; set; } = true;
    public bool InExplicitTransaction => _transactionId is not null;
    public bool SnapshotEnabled => _snapshot is not null;
    public bool SupportsLocalDirectFeatures => _localDatabase is not null;

    public bool PreferSyncPointLookups
    {
        get => _localDatabase?.PreferSyncPointLookups ?? false;
        set
        {
            if (_localDatabase is null)
                throw new InvalidOperationException("Sync point mode requires direct local access.");

            _localDatabase.PreferSyncPointLookups = value;
        }
    }

    public async ValueTask<bool> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => await _executeSql(sql, ct);

    public async ValueTask<SqlExecutionResult> ExecuteDbSqlAsync(string sql, CancellationToken ct = default)
        => _transactionId is null
            ? await Client.ExecuteSqlAsync(sql, ct)
            : await Client.ExecuteInTransactionAsync(_transactionId, sql, ct);

    public async ValueTask<QueryResult> ExecuteReadSnapshotAsync(string sql, CancellationToken ct = default)
    {
        if (_snapshot is null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is not enabled.");
        return await _snapshot.ExecuteReadAsync(sql, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_transactionId is not null)
            throw new InvalidOperationException("An explicit transaction is already active.");

        DisableSnapshot();

        bool releasedLocalDatabase = false;
        if (_engineBackedClient is not null && _localDatabase is not null)
        {
            await _engineBackedClient.ReleaseCachedDatabaseAsync(ct);
            _localDatabase = null;
            releasedLocalDatabase = true;
        }

        try
        {
            var tx = await Client.BeginTransactionAsync(ct);
            _transactionId = tx.TransactionId;
        }
        catch
        {
            if (releasedLocalDatabase)
                await RefreshLocalDatabaseAsync(ct);

            throw;
        }
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_transactionId is null)
            throw new InvalidOperationException("No explicit transaction is active.");

        ExceptionDispatchInfo? captured = null;
        try
        {
            await Client.CommitTransactionAsync(_transactionId, ct);
        }
        catch (Exception ex)
        {
            captured = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            _transactionId = null;
        }

        await RefreshLocalDatabaseAsync(ct);
        captured?.Throw();
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_transactionId is null)
            throw new InvalidOperationException("No explicit transaction is active.");

        ExceptionDispatchInfo? captured = null;
        try
        {
            await Client.RollbackTransactionAsync(_transactionId, ct);
        }
        catch (Exception ex)
        {
            captured = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            _transactionId = null;
        }

        await RefreshLocalDatabaseAsync(ct);
        captured?.Throw();
    }

    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        await Client.CheckpointAsync(ct);
    }

    public async ValueTask<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
    {
        DisableSnapshot();
        var result = await Client.ReindexAsync(request, ct);
        await RefreshLocalDatabaseAsync(ct);
        return result;
    }

    public async ValueTask<VacuumResult> VacuumAsync(CancellationToken ct = default)
    {
        DisableSnapshot();
        var result = await Client.VacuumAsync(ct);
        await RefreshLocalDatabaseAsync(ct);
        return result;
    }

    public async ValueTask<BackupResult> BackupAsync(
        string destinationPath,
        bool withManifest,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        DisableSnapshot();
        return await Client.BackupAsync(
            new BackupRequest
            {
                DestinationPath = destinationPath,
                WithManifest = withManifest,
            },
            ct);
    }

    public async ValueTask<RestoreResult> RestoreAsync(
        string sourcePath,
        bool validateOnly,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);

        DisableSnapshot();
        ExceptionDispatchInfo? captured = null;
        RestoreResult? result = null;

        try
        {
            result = await Client.RestoreAsync(
                new RestoreRequest
                {
                    SourcePath = sourcePath,
                    ValidateOnly = validateOnly,
                },
                ct);
        }
        catch (Exception ex)
        {
            captured = ExceptionDispatchInfo.Capture(ex);
        }
        finally
        {
            if (!validateOnly && _engineBackedClient is not null)
            {
                string currentDatabasePath = Path.GetFullPath(Client.DataSource);
                if (File.Exists(currentDatabasePath))
                    await RefreshLocalDatabaseAsync(ct);
                else
                    _localDatabase = null;
            }
        }

        captured?.Throw();
        return result!;
    }

    public async ValueTask RefreshLocalDatabaseAsync(CancellationToken ct = default)
    {
        if (_engineBackedClient is null)
            return;

        _localDatabase = await _engineBackedClient.TryGetDatabaseAsync(ct);
    }

    public void EnableSnapshot()
    {
        if (_localDatabase is null)
            throw new InvalidOperationException("Snapshot mode requires direct local access.");

        if (_snapshot is not null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is already enabled.");

        _snapshot = _localDatabase.CreateReaderSession();
    }

    public void DisableSnapshot()
    {
        _snapshot?.Dispose();
        _snapshot = null;
    }

    public void Dispose()
    {
        DisableSnapshot();
    }
}
