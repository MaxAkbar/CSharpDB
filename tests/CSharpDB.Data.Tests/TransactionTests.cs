using System.Data;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class TransactionTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private CSharpDbConnection _conn = null!;

    public TransactionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_tx_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await _conn.OpenAsync(Ct);

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER, val TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task CommitAsync_PersistsChanges()
    {
        await using var tx = await _conn.BeginTransactionAsync(Ct);
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'committed');";
        await cmd.ExecuteNonQueryAsync(Ct);
        await tx.CommitAsync(Ct);

        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("committed", reader.GetString(1));
    }

    [Fact]
    public async Task RollbackAsync_RevertsChanges()
    {
        // Insert a baseline row outside the transaction
        using var setupCmd = _conn.CreateCommand();
        setupCmd.CommandText = "INSERT INTO t VALUES (0, 'baseline');";
        await setupCmd.ExecuteNonQueryAsync(Ct);

        await using var tx = await _conn.BeginTransactionAsync(Ct);
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "INSERT INTO t VALUES (1, 'rolled back');";
        await cmd.ExecuteNonQueryAsync(Ct);
        await tx.RollbackAsync(Ct);

        cmd.CommandText = "SELECT * FROM t;";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("baseline", reader.GetString(1));
        Assert.False(await reader.ReadAsync(Ct)); // rolled-back row gone
    }

    [Fact]
    public async Task DisposeAsync_WithoutCommit_RollsBack()
    {
        // Insert a baseline
        using var setupCmd = _conn.CreateCommand();
        setupCmd.CommandText = "INSERT INTO t VALUES (0, 'baseline');";
        await setupCmd.ExecuteNonQueryAsync(Ct);

        // Start transaction, insert, but don't commit — just dispose
        await using (var tx = await _conn.BeginTransactionAsync(Ct))
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = "INSERT INTO t VALUES (1, 'not committed');";
            await cmd.ExecuteNonQueryAsync(Ct);
            // tx disposed here without commit
        }

        using var verifyCmd = _conn.CreateCommand();
        verifyCmd.CommandText = "SELECT * FROM t;";
        await using var reader = await verifyCmd.ExecuteReaderAsync(Ct);
        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal("baseline", reader.GetString(1));
        Assert.False(await reader.ReadAsync(Ct)); // uncommitted row gone
    }

    [Fact]
    public async Task BeginTransaction_WhenAlreadyActive_Throws()
    {
        await using var tx = await _conn.BeginTransactionAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await _conn.BeginTransactionAsync(Ct));
        await tx.RollbackAsync(Ct);
    }

    [Fact]
    public async Task CommitAsync_WhenAlreadyCompleted_Throws()
    {
        await using var tx = await _conn.BeginTransactionAsync(Ct);
        await tx.CommitAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync(Ct));
    }
}

