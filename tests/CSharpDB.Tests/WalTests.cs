using CSharpDB.Core;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public class WalTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public WalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task Commit_PersistsThroughWal()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'hello')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'world')");

        await using var result = await _db.ExecuteAsync("SELECT * FROM t ORDER BY id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }

    [Fact]
    public async Task Rollback_DiscardsChanges()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'keep')");

        await _db.BeginTransactionAsync();
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'discard')");
        await _db.RollbackAsync();

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("keep", rows[0][1].AsText);
    }

    [Fact]
    public async Task CrashRecovery_CommittedDataSurvives()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'survived')");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("survived", rows[0][1].AsText);
    }

    [Fact]
    public async Task CrashRecovery_UncommittedDataLost()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'committed')");

        // Start a transaction but don't commit — dispose will rollback
        await _db.BeginTransactionAsync();
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'uncommitted')");
        await _db.DisposeAsync();

        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t");
        var rows = await result.ToListAsync();
        Assert.Single(rows);
        Assert.Equal("committed", rows[0][1].AsText);
    }

    [Fact]
    public async Task ConcurrentReader_SeesSnapshotWhileWriterModifies()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'original')");

        // Take a reader snapshot
        using var reader = _db.CreateReaderSession();

        // Writer modifies data
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'new')");

        // Reader should still see original data only (snapshot isolation)
        await using var readerResult = await reader.ExecuteReadAsync("SELECT * FROM t");
        var readerRows = await readerResult.ToListAsync();
        Assert.Single(readerRows);
        Assert.Equal("original", readerRows[0][1].AsText);

        // Main database sees both rows
        await using var mainResult = await _db.ExecuteAsync("SELECT * FROM t");
        var mainRows = await mainResult.ToListAsync();
        Assert.Equal(2, mainRows.Count);
    }

    [Fact]
    public async Task MultipleReaders_DontBlockEachOther()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'data')");

        using var reader1 = _db.CreateReaderSession();
        using var reader2 = _db.CreateReaderSession();

        await using var r1 = await reader1.ExecuteReadAsync("SELECT * FROM t");
        await using var r2 = await reader2.ExecuteReadAsync("SELECT * FROM t");

        var rows1 = await r1.ToListAsync();
        var rows2 = await r2.ToListAsync();

        Assert.Single(rows1);
        Assert.Single(rows2);
    }

    [Fact]
    public async Task Checkpoint_CopiesDataToDbFile()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        for (int i = 0; i < 10; i++)
            await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, 'row{i}')");

        // Manual checkpoint
        await _db.CheckpointAsync();

        // Data should still be accessible
        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(10L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task ManyInserts_AutoCheckpointDoesNotCorrupt()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        for (int i = 0; i < 100; i++)
            await _db.ExecuteAsync($"INSERT INTO t VALUES ({i}, 'row{i}')");

        await using var result = await _db.ExecuteAsync("SELECT COUNT(*) FROM t");
        var rows = await result.ToListAsync();
        Assert.Equal(100L, rows[0][0].AsInteger);
    }

    [Fact]
    public async Task Persistence_CloseAndReopen()
    {
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
        await _db.ExecuteAsync("INSERT INTO t VALUES (1, 'hello')");
        await _db.ExecuteAsync("INSERT INTO t VALUES (2, 'world')");

        // Close and reopen
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath);

        await using var result = await _db.ExecuteAsync("SELECT * FROM t ORDER BY id");
        var rows = await result.ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("hello", rows[0][1].AsText);
        Assert.Equal("world", rows[1][1].AsText);
    }
}
