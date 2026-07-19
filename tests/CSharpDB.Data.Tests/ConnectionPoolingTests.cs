using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ConnectionPoolingTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _dbPathNoPool;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ConnectionPoolingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_pool_test_{Guid.NewGuid():N}.db");
        _dbPathNoPool = Path.Combine(Path.GetTempPath(), $"csharpdb_pool_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_dbPath);
        DeleteIfExists(_dbPath + ".wal");
        DeleteIfExists(_dbPathNoPool);
        DeleteIfExists(_dbPathNoPool + ".wal");
    }

    [Fact]
    public async Task OpenClose_WithPoolingEnabled_StoresAndRentsIdleDatabase()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var first = new CSharpDbConnection(cs))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));

        await using (var second = new CSharpDbConnection(cs))
        {
            await second.OpenAsync(Ct);
            Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
            await second.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
    }

    [Fact]
    public async Task OpenClose_WithPoolingEnabled_ReusesWarmDatabase()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        var factory = new CountingStorageEngineFactory();
        var options = new DatabaseOptions
        {
            StorageEngineFactory = factory,
        };

        await using (var first = new CSharpDbConnection(cs, options))
        {
            await first.OpenAsync(Ct);
            await ExecuteNonQueryAsync(first, "CREATE TABLE warm_pool (id INTEGER PRIMARY KEY);");
            await first.CloseAsync();
        }

        await using (var second = new CSharpDbConnection(cs, options))
        {
            await second.OpenAsync(Ct);
            await ExecuteNonQueryAsync(second, "INSERT INTO warm_pool VALUES (1);");
            await second.CloseAsync();
        }

        Assert.Equal(1, factory.OpenCount);
        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs, options, hybridDatabaseOptions: null));
    }

    [Fact]
    public async Task CloseAsync_WithPoolingEnabled_KeepsWalUntilPoolIsCleared()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var connection = new CSharpDbConnection(cs))
        {
            await connection.OpenAsync(Ct);
            await ExecuteNonQueryAsync(connection, "CREATE TABLE wal_lifetime (id INTEGER PRIMARY KEY);");
            await ExecuteNonQueryAsync(connection, "INSERT INTO wal_lifetime VALUES (1);");
            await connection.CloseAsync();
        }

        Assert.True(File.Exists(_dbPath + ".wal"));

        await CSharpDbConnection.ClearPoolAsync(cs);

        Assert.False(File.Exists(_dbPath + ".wal"));
    }

    [Fact]
    public async Task CloseAsync_WithActiveTransaction_RollsBackBeforePooling()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var setup = new CSharpDbConnection(cs))
        {
            await setup.OpenAsync(Ct);
            await ExecuteNonQueryAsync(setup, "CREATE TABLE rollback_pool (id INTEGER PRIMARY KEY);");
            await setup.CloseAsync();
        }

        await using (var writer = new CSharpDbConnection(cs))
        {
            await writer.OpenAsync(Ct);
            await writer.BeginTransactionAsync(Ct);
            await ExecuteNonQueryAsync(writer, "INSERT INTO rollback_pool VALUES (1);");
            await writer.CloseAsync();
        }

        await using var reader = new CSharpDbConnection(cs);
        await reader.OpenAsync(Ct);
        Assert.Equal(0L, await ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM rollback_pool;"));
    }

    [Fact]
    public async Task ConcurrentPooledSessions_SharePersistentStateAndIsolateTemporaryTables()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var first = new CSharpDbConnection(cs);
        await using var second = new CSharpDbConnection(cs);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        await ExecuteNonQueryAsync(first, "CREATE TABLE shared_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(first, "INSERT INTO shared_pool VALUES (1);");
        Assert.Equal(1L, await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM shared_pool;"));

        await ExecuteNonQueryAsync(first, "CREATE TEMP TABLE private_scratch (id INTEGER PRIMARY KEY);");
        Assert.Equal(0L, await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM sys.temp_tables;"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            async () => await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM private_scratch;"));
    }

    [Fact]
    public async Task PersistentSelect_StreamsFromSnapshotAndDoesNotBlockWriter()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var first = new CSharpDbConnection(cs);
        await using var second = new CSharpDbConnection(cs);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        await ExecuteNonQueryAsync(first, "CREATE TABLE streaming_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(first, "INSERT INTO streaming_pool VALUES (1);");
        await ExecuteNonQueryAsync(first, "INSERT INTO streaming_pool VALUES (2);");

        PooledDatabaseSession session = Assert.IsType<PooledDatabaseSession>(first.GetSession());
        await using var command = first.CreateCommand();
        command.CommandText = "SELECT id FROM streaming_pool ORDER BY id;";
        await using var reader = await command.ExecuteReaderAsync(Ct);

        Assert.Equal(1, session.ActiveSnapshotReaderCountForTest);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(1L, reader.GetInt64(0));

        await ExecuteNonQueryAsync(second, "INSERT INTO streaming_pool VALUES (3);");

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(2L, reader.GetInt64(0));
        Assert.False(await reader.ReadAsync(Ct));
        Assert.Equal(1, session.ActiveSnapshotReaderCountForTest);

        await reader.DisposeAsync();
        Assert.Equal(0, session.ActiveSnapshotReaderCountForTest);
        Assert.Equal(3L, await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM streaming_pool;"));
    }

    [Fact]
    public async Task ActiveTransaction_OtherSessionReadsCommittedSnapshotAndCannotWrite()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var writer = new CSharpDbConnection(cs);
        await using var reader = new CSharpDbConnection(cs);
        await writer.OpenAsync(Ct);
        await reader.OpenAsync(Ct);

        await ExecuteNonQueryAsync(
            writer,
            "CREATE TABLE transaction_pool (id INTEGER PRIMARY KEY, name TEXT);");
        await ExecuteNonQueryAsync(
            writer,
            "INSERT INTO transaction_pool VALUES (1, 'committed');");

        await using var transaction = await writer.BeginTransactionAsync(Ct);
        await ExecuteNonQueryAsync(
            writer,
            "INSERT INTO transaction_pool VALUES (2, 'pending');");

        Assert.Equal(2L, await ExecuteScalarAsync(writer, "SELECT COUNT(*) FROM transaction_pool;"));
        Assert.Equal(1L, await ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM transaction_pool;"));

        await using (var readCommand = reader.CreateCommand())
        {
            readCommand.CommandText = "SELECT name FROM transaction_pool ORDER BY id;";
            await using var committedReader = await readCommand.ExecuteReaderAsync(Ct);
            Assert.True(await committedReader.ReadAsync(Ct));
            Assert.Equal("committed", committedReader.GetString(0));
            Assert.False(await committedReader.ReadAsync(Ct));
        }

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ExecuteNonQueryAsync(
                reader,
                "INSERT INTO transaction_pool VALUES (3, 'blocked');"));

        await transaction.CommitAsync(Ct);
        Assert.Equal(2L, await ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM transaction_pool;"));
    }

    [Fact]
    public async Task ActiveTransaction_OtherSessionWithTemporaryStateCannotReadLiveWriterState()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var writer = new CSharpDbConnection(cs);
        await using var temporarySession = new CSharpDbConnection(cs);
        await writer.OpenAsync(Ct);
        await temporarySession.OpenAsync(Ct);

        await ExecuteNonQueryAsync(writer, "CREATE TABLE temp_guard_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(writer, "INSERT INTO temp_guard_pool VALUES (1);");
        await ExecuteNonQueryAsync(
            temporarySession,
            "CREATE TEMP TABLE private_before_transaction (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(
            temporarySession,
            "INSERT INTO private_before_transaction VALUES (1);");

        await using var transaction = await writer.BeginTransactionAsync(Ct);
        await ExecuteNonQueryAsync(writer, "INSERT INTO temp_guard_pool VALUES (2);");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ExecuteScalarAsync(temporarySession, "SELECT COUNT(*) FROM temp_guard_pool;"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ExecuteScalarAsync(
                temporarySession,
                "SELECT COUNT(*) FROM private_before_transaction;"));

        await transaction.RollbackAsync(Ct);
        Assert.Equal(
            1L,
            await ExecuteScalarAsync(
                temporarySession,
                "SELECT COUNT(*) FROM private_before_transaction;"));
    }

    [Fact]
    public async Task ActiveTransaction_PersistentSchemaMutationBlocksNonOwnerReadsUntilRollback()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var writer = new CSharpDbConnection(cs);
        await using var reader = new CSharpDbConnection(cs);
        await writer.OpenAsync(Ct);
        await reader.OpenAsync(Ct);

        await ExecuteNonQueryAsync(
            writer,
            "CREATE TABLE schema_snapshot_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(writer, "INSERT INTO schema_snapshot_pool VALUES (1);");

        await using var transaction = await writer.BeginTransactionAsync(Ct);
        await ExecuteNonQueryAsync(
            writer,
            "CREATE TABLE pending_schema_pool (id INTEGER PRIMARY KEY);");

        InvalidOperationException busy = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM schema_snapshot_pool;"));
        Assert.Contains("schema", busy.Message, StringComparison.OrdinalIgnoreCase);

        await transaction.RollbackAsync(Ct);

        Assert.Equal(
            1L,
            await ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM schema_snapshot_pool;"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScalarAsync(reader, "SELECT COUNT(*) FROM pending_schema_pool;"));
    }

    [Fact]
    public async Task ActiveSnapshotReader_BlocksNestedPersistentSchemaMutation()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=2";

        await using var writer = new CSharpDbConnection(cs);
        await using var readerConnection = new CSharpDbConnection(cs);
        await writer.OpenAsync(Ct);
        await readerConnection.OpenAsync(Ct);

        await ExecuteNonQueryAsync(
            writer,
            "CREATE TABLE schema_reader_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(writer, "INSERT INTO schema_reader_pool VALUES (1);");
        await ExecuteNonQueryAsync(writer, "INSERT INTO schema_reader_pool VALUES (2);");

        await using var readCommand = readerConnection.CreateCommand();
        readCommand.CommandText = "SELECT id FROM schema_reader_pool ORDER BY id;";
        await using var reader = await readCommand.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));

        await using var transaction = await writer.BeginTransactionAsync(Ct);
        var conditional = new ConditionalStatement
        {
            ExistsQuery = (QueryStatement)Parser.Parse(
                "SELECT id FROM schema_reader_pool WHERE id = 99;"),
            Negated = true,
            Body =
            [
                Parser.Parse(
                    "CREATE TABLE nested_pending_schema_pool (id INTEGER PRIMARY KEY);"),
            ],
        };

        ICSharpDbSession writerSession = writer.GetSession();
        InvalidOperationException busy = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await using var result = await writerSession.ExecuteAsync(conditional, Ct);
            });
        Assert.Contains("schema", busy.Message, StringComparison.OrdinalIgnoreCase);

        await reader.DisposeAsync();

        await using (var result = await writerSession.ExecuteAsync(conditional, Ct))
        {
            Assert.Equal(0, result.RowsAffected);
        }

        await transaction.RollbackAsync(Ct);
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScalarAsync(
                readerConnection,
                "SELECT COUNT(*) FROM nested_pending_schema_pool;"));
    }

    [Fact]
    public async Task FailedOwnerReset_PoisonsAlreadyOpenSessions()
    {
        Database database = await Database.OpenAsync(_dbPathNoPool, Ct);
        await using (var setup = await database.ExecuteAsync(
                         "CREATE TABLE poisoned_pool (id INTEGER PRIMARY KEY);",
                         Ct))
        {
        }

        var key = new PoolKey(
            DataSource: _dbPathNoPool,
            MaxPoolSize: 2,
            EffectiveOpenMode: CSharpDbEmbeddedOpenMode.Direct,
            EffectiveStoragePreset: null,
            EffectiveAdaptiveQueryReoptimization: false,
            ExplicitDirectDatabaseOptions: null,
            ExplicitHybridDatabaseOptions: null);
        var pool = new CSharpDbConnectionPool(
            key,
            maxPoolSize: 2,
            _ => ValueTask.FromResult(database));

        PooledDatabaseSession owner = await pool.OpenSessionAsync(Ct);
        PooledDatabaseSession observer = await pool.OpenSessionAsync(Ct);
        await owner.BeginTransactionAsync(Ct);

        // End the engine transaction behind the pool's owner bookkeeping so
        // releasing the owner exercises the failed-reset path deterministically.
        await database.RollbackAsync(Ct);
        InvalidOperationException reset = await Assert.ThrowsAsync<InvalidOperationException>(
            () => owner.DisposeAsync().AsTask());
        Assert.Contains("reset", reset.Message, StringComparison.OrdinalIgnoreCase);

        InvalidOperationException poisoned = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await using var result = await observer.ExecuteAsync(
                    "SELECT COUNT(*) FROM poisoned_pool;",
                    Ct);
            });
        Assert.Contains("reset safely", poisoned.Message, StringComparison.OrdinalIgnoreCase);

        await observer.DisposeAsync();
    }

    [Fact]
    public async Task FailedTransactionCompletion_PoisonsAlreadyOpenSessions()
    {
        Database database = await Database.OpenAsync(_dbPathNoPool, Ct);
        await using (var setup = await database.ExecuteAsync(
                         "CREATE TABLE completion_poison_pool (id INTEGER PRIMARY KEY);",
                         Ct))
        {
        }

        var key = new PoolKey(
            DataSource: _dbPathNoPool,
            MaxPoolSize: 2,
            EffectiveOpenMode: CSharpDbEmbeddedOpenMode.Direct,
            EffectiveStoragePreset: null,
            EffectiveAdaptiveQueryReoptimization: false,
            ExplicitDirectDatabaseOptions: null,
            ExplicitHybridDatabaseOptions: null);
        var pool = new CSharpDbConnectionPool(
            key,
            maxPoolSize: 2,
            _ => ValueTask.FromResult(database));

        PooledDatabaseSession owner = await pool.OpenSessionAsync(Ct);
        PooledDatabaseSession observer = await pool.OpenSessionAsync(Ct);
        await owner.BeginTransactionAsync(Ct);

        // Desynchronize the engine from the pool so CommitAsync deterministically
        // exercises the completion-failure path.
        await database.RollbackAsync(Ct);
        await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(
            () => owner.CommitAsync(Ct).AsTask());

        InvalidOperationException poisoned = await Assert.ThrowsAsync<InvalidOperationException>(
            async () =>
            {
                await using var result = await observer.ExecuteAsync(
                    "SELECT COUNT(*) FROM completion_poison_pool;",
                    Ct);
            });
        Assert.Contains("reset safely", poisoned.Message, StringComparison.OrdinalIgnoreCase);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => owner.DisposeAsync().AsTask());
        await observer.DisposeAsync();
    }

    [Fact]
    public async Task Close_WithOutstandingStreamedReader_ReleasesSnapshot()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using var connection = new CSharpDbConnection(cs);
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(connection, "CREATE TABLE reader_close_pool (id INTEGER PRIMARY KEY);");
        await ExecuteNonQueryAsync(connection, "INSERT INTO reader_close_pool VALUES (1);");

        PooledDatabaseSession session = Assert.IsType<PooledDatabaseSession>(connection.GetSession());
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT id FROM reader_close_pool;";
        await using var reader = await command.ExecuteReaderAsync(Ct);
        Assert.Equal(1, session.ActiveSnapshotReaderCountForTest);

        await connection.CloseAsync();

        Assert.Equal(0, session.ActiveSnapshotReaderCountForTest);
        await reader.DisposeAsync();
        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
    }

    [Fact]
    public async Task MaxPoolSize_LimitsSimultaneousLogicalSessions()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using var first = new CSharpDbConnection(cs);
        await using var second = new CSharpDbConnection(cs);
        await first.OpenAsync(Ct);

        Task secondOpen = second.OpenAsync(Ct);
        await Task.Delay(50, Ct);
        Assert.False(secondOpen.IsCompleted);

        await first.CloseAsync();
        await secondOpen.WaitAsync(TimeSpan.FromSeconds(2), Ct);

        Assert.Equal(System.Data.ConnectionState.Open, second.State);
    }

    [Fact]
    public async Task ClearPoolAsync_WhileConnectionIsOpen_DisposesDatabaseOnReturn()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using var connection = new CSharpDbConnection(cs);
        await using var replacement = new CSharpDbConnection(cs);
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(connection, "CREATE TABLE clear_checked_out (id INTEGER PRIMARY KEY);");

        await CSharpDbConnection.ClearPoolAsync(cs);
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());

        Task replacementOpen = replacement.OpenAsync(Ct);
        await Task.Delay(50, Ct);
        Assert.False(replacementOpen.IsCompleted);

        // ClearPool retires a healthy pool for future opens but must not poison
        // logical sessions that were already checked out.
        await ExecuteNonQueryAsync(
            connection,
            "INSERT INTO clear_checked_out VALUES (1);");

        await connection.CloseAsync();
        await replacementOpen.WaitAsync(TimeSpan.FromSeconds(2), Ct);

        Assert.Equal(System.Data.ConnectionState.Open, replacement.State);
        Assert.Equal(
            1L,
            await ExecuteScalarAsync(replacement, "SELECT COUNT(*) FROM clear_checked_out;"));
        await replacement.CloseAsync();
        await CSharpDbConnection.ClearPoolAsync(cs);
        Assert.False(File.Exists(_dbPath + ".wal"));
    }

    [Fact]
    public async Task OpenClose_WithPoolingEnabled_ClearsTemporaryTablesBeforePooling()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var first = new CSharpDbConnection(cs))
        {
            await first.OpenAsync(Ct);
            await ExecuteNonQueryAsync(first, "CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY);");
            await ExecuteNonQueryAsync(first, "INSERT INTO scratch VALUES (1);");
            Assert.Equal(1L, await ExecuteScalarAsync(first, "SELECT COUNT(*) FROM scratch;"));
            await first.CloseAsync();
        }

        await using (var second = new CSharpDbConnection(cs))
        {
            await second.OpenAsync(Ct);
            Assert.Equal(0L, await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM sys.temp_tables;"));
            await Assert.ThrowsAsync<CSharpDbDataException>(
                async () => await ExecuteScalarAsync(second, "SELECT COUNT(*) FROM scratch;"));
            await second.CloseAsync();
        }
    }

    [Fact]
    public async Task ClearPoolAsync_RemovesIdleEntries()
    {
        string cs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";

        await using (var conn = new CSharpDbConnection(cs))
        {
            await conn.OpenAsync(Ct);
            await conn.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(cs));

        await CSharpDbConnection.ClearPoolAsync(cs);

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
    }

    [Fact]
    public async Task OpenClose_WithPoolingDisabled_DoesNotPopulatePool()
    {
        string cs = $"Data Source={_dbPathNoPool};Pooling=false";

        await using var conn = new CSharpDbConnection(cs);
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(cs));
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
    }

    [Fact]
    public async Task NonPooledOpen_RetiresIdlePoolAndBlocksPooledOpenUntilClosed()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPath};Pooling=false";

        await using (var pooled = new CSharpDbConnection(pooledCs))
        {
            await pooled.OpenAsync(Ct);
            await ExecuteNonQueryAsync(
                pooled,
                "CREATE TABLE mixed_mode_pool (id INTEGER PRIMARY KEY);");
            await ExecuteNonQueryAsync(pooled, "INSERT INTO mixed_mode_pool VALUES (1);");
            await pooled.CloseAsync();
        }

        Assert.Equal(1, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));

        await using var direct = new CSharpDbConnection(nonPooledCs);
        await direct.OpenAsync(Ct);

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));
        Assert.Equal(0, CSharpDbConnection.GetPoolCountForTest());
        Assert.Equal(1L, await ExecuteScalarAsync(direct, "SELECT COUNT(*) FROM mixed_mode_pool;"));
        await ExecuteNonQueryAsync(direct, "INSERT INTO mixed_mode_pool VALUES (2);");

        await using (var blockedPool = new CSharpDbConnection(pooledCs))
        {
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                () => blockedPool.OpenAsync(Ct));
            Assert.Contains("non-pooled connections", error.Message, StringComparison.OrdinalIgnoreCase);
        }

        await direct.CloseAsync();

        await using var replacementPool = new CSharpDbConnection(pooledCs);
        await replacementPool.OpenAsync(Ct);
        Assert.Equal(
            2L,
            await ExecuteScalarAsync(replacementPool, "SELECT COUNT(*) FROM mixed_mode_pool;"));
    }

    [Fact]
    public async Task NonPooledOpen_RejectsActivePooledSessionThenSucceedsAfterClose()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPath};Pooling=false";

        await using var pooled = new CSharpDbConnection(pooledCs);
        await pooled.OpenAsync(Ct);
        await ExecuteNonQueryAsync(pooled, "CREATE TABLE active_mixed_mode (id INTEGER PRIMARY KEY);");

        await using var direct = new CSharpDbConnection(nonPooledCs);
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => direct.OpenAsync(Ct));
        Assert.Contains("pooled connections", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(System.Data.ConnectionState.Closed, direct.State);

        await pooled.CloseAsync();
        await direct.OpenAsync(Ct);
        Assert.Equal(System.Data.ConnectionState.Open, direct.State);
    }

    [Fact]
    public async Task FailedNonPooledOpen_ReleasesLeaseForPooledRetry()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPath};Pooling=false";
        var options = new DatabaseOptions
        {
            StorageEngineFactory = new ThrowingStorageEngineFactory(),
        };

        await using (var failedDirect = new CSharpDbConnection(nonPooledCs, options))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => failedDirect.OpenAsync(Ct));
        }

        await using var pooled = new CSharpDbConnection(pooledCs);
        await pooled.OpenAsync(Ct);
        Assert.Equal(System.Data.ConnectionState.Open, pooled.State);
    }

    [Fact]
    public async Task PooledOpen_IsBlockedWhileNonPooledPhysicalOpenIsInProgress()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPath};Pooling=false";
        var factory = new BlockingOpenStorageEngineFactory();
        var options = new DatabaseOptions
        {
            StorageEngineFactory = factory,
        };

        await using var direct = new CSharpDbConnection(nonPooledCs, options);
        Task directOpen = direct.OpenAsync(Ct);
        await factory.OpenStarted.WaitAsync(TimeSpan.FromSeconds(2), Ct);

        try
        {
            await using var pooled = new CSharpDbConnection(pooledCs);
            InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
                () => pooled.OpenAsync(Ct));
            Assert.Contains("non-pooled connections", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            factory.AllowOpen();
        }

        await directOpen.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        await direct.CloseAsync();

        await using var replacementPool = new CSharpDbConnection(pooledCs);
        await replacementPool.OpenAsync(Ct);
        Assert.Equal(System.Data.ConnectionState.Open, replacementPool.State);
    }

    [Fact]
    public async Task OpenAsync_CanceledPooledOpen_DoesNotReturnLaterDatabaseToStalePool()
    {
        string pooledCs = $"Data Source={_dbPath};Pooling=true;Max Pool Size=1";
        string nonPooledCs = $"Data Source={_dbPathNoPool};Pooling=false";

        await using var conn = new CSharpDbConnection(pooledCs);
        using (var cts = new CancellationTokenSource())
        {
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.OpenAsync(cts.Token));
        }

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));

        conn.ConnectionString = nonPooledCs;
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();

        Assert.Equal(0, CSharpDbConnection.GetIdlePoolSizeForTest(pooledCs));
    }

    [Fact]
    public void ConnectionStringBuilder_ParsesPoolingOptions()
    {
        var csb = new CSharpDbConnectionStringBuilder("Data Source=my.db;Pooling=true;Max Pool Size=7");

        Assert.Equal("my.db", csb.DataSource);
        Assert.True(csb.Pooling);
        Assert.Equal(7, csb.MaxPoolSize);
    }

    [Fact]
    public void ConnectionStringBuilder_UsesPoolingDefaults_WhenNotConfigured()
    {
        var csb = new CSharpDbConnectionStringBuilder("Data Source=my.db");

        Assert.False(csb.Pooling);
        Assert.Equal(CSharpDbConnectionStringBuilder.DefaultMaxPoolSize, csb.MaxPoolSize);
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temp benchmark/test files.
        }
    }

    private static async Task ExecuteNonQueryAsync(CSharpDbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<object?> ExecuteScalarAsync(CSharpDbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private sealed class CountingStorageEngineFactory : IStorageEngineFactory
    {
        private readonly DefaultStorageEngineFactory _inner = new();

        internal int OpenCount { get; private set; }

        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            OpenCount++;
            return _inner.OpenAsync(filePath, options, ct);
        }
    }

    private sealed class ThrowingStorageEngineFactory : IStorageEngineFactory
    {
        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            return ValueTask.FromException<StorageEngineContext>(
                new InvalidOperationException("Storage engine open failed."));
        }
    }

    private sealed class BlockingOpenStorageEngineFactory : IStorageEngineFactory
    {
        private readonly DefaultStorageEngineFactory _inner = new();
        private readonly TaskCompletionSource _openStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _allowOpen =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task OpenStarted => _openStarted.Task;

        internal void AllowOpen() => _allowOpen.TrySetResult();

        public async ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            _openStarted.TrySetResult();
            await _allowOpen.Task.WaitAsync(ct);
            return await _inner.OpenAsync(filePath, options, ct);
        }
    }
}
