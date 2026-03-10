using CSharpDB.Data;
using CSharpDB.Engine;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class InMemoryConnectionTests : IAsyncLifetime
{
    private readonly List<string> _paths = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();

        foreach (string path in _paths)
        {
            TryDelete(path);
            TryDelete(path + ".wal");
        }
    }

    [Fact]
    public async Task PrivateMemory_IsIsolatedPerConnection()
    {
        await using var first = new CSharpDbConnection("Data Source=:memory:");
        await using var second = new CSharpDbConnection("Data Source=:memory:");
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        using (var cmd = first.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO t VALUES (1, 'first');";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        using (var cmd = second.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO t VALUES (1, 'second');";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "SELECT name FROM t;";
            Assert.Equal("second", await cmd.ExecuteScalarAsync(Ct));
        }

        using var verify = first.CreateCommand();
        verify.CommandText = "SELECT name FROM t;";
        Assert.Equal("first", await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task NamedSharedMemory_SharesStateAcrossConnections()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";

        await using var first = new CSharpDbConnection(connectionString);
        await using var second = new CSharpDbConnection(connectionString);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        using (var cmd = first.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO t VALUES (1, 'shared');";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        using var verify = second.CreateCommand();
        verify.CommandText = "SELECT name FROM t WHERE id = 1;";
        Assert.Equal("shared", await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task NamedSharedMemory_AllowsSnapshotReadsButSingleWriterTransactions()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";

        await using var writer = new CSharpDbConnection(connectionString);
        await using var reader = new CSharpDbConnection(connectionString);
        await writer.OpenAsync(Ct);
        await reader.OpenAsync(Ct);

        using (var setup = writer.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await setup.ExecuteNonQueryAsync(Ct);
            setup.CommandText = "INSERT INTO t VALUES (1, 'baseline');";
            await setup.ExecuteNonQueryAsync(Ct);
        }

        await using var tx = await writer.BeginTransactionAsync(Ct);

        using (var write = writer.CreateCommand())
        {
            write.CommandText = "INSERT INTO t VALUES (2, 'pending');";
            await write.ExecuteNonQueryAsync(Ct);
        }

        using (var read = reader.CreateCommand())
        {
            read.CommandText = "SELECT COUNT(*) FROM t;";
            Assert.Equal(1L, await read.ExecuteScalarAsync(Ct));
        }

        using (var blockedWrite = reader.CreateCommand())
        {
            blockedWrite.CommandText = "INSERT INTO t VALUES (3, 'blocked');";
            await Assert.ThrowsAsync<InvalidOperationException>(() => blockedWrite.ExecuteNonQueryAsync(Ct));
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.BeginTransactionAsync(Ct));

        await tx.CommitAsync(Ct);

        using var verify = reader.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM t;";
        Assert.Equal(2L, await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task NamedSharedMemory_LoadFrom_SeedsFirstOpen_AndRejectsConflictingLaterSeeds()
    {
        string seedOnePath = await CreateSeedDatabaseAsync("seed-one");
        string seedTwoPath = await CreateSeedDatabaseAsync("seed-two");
        string dataSource = $":memory:{Guid.NewGuid():N}";
        string seedOneCs = $"Data Source={dataSource};Load From={seedOnePath}";
        string sameSeedCs = $"Data Source={dataSource};Load From={seedOnePath}";
        string conflictingSeedCs = $"Data Source={dataSource};Load From={seedTwoPath}";

        await using (var first = new CSharpDbConnection(seedOneCs))
        {
            await first.OpenAsync(Ct);
            using var cmd = first.CreateCommand();
            cmd.CommandText = "SELECT name FROM seed_data;";
            Assert.Equal("seed-one", await cmd.ExecuteScalarAsync(Ct));
        }

        await using (var second = new CSharpDbConnection(sameSeedCs))
        {
            await second.OpenAsync(Ct);
            using var cmd = second.CreateCommand();
            cmd.CommandText = "SELECT name FROM seed_data;";
            Assert.Equal("seed-one", await cmd.ExecuteScalarAsync(Ct));
        }

        await using var conflicting = new CSharpDbConnection(conflictingSeedCs);
        await Assert.ThrowsAsync<InvalidOperationException>(() => conflicting.OpenAsync(Ct));
    }

    [Fact]
    public async Task SaveToFileAsync_WritesNamedSharedSnapshot()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        string savePath = NewTempDbPath();

        await using (var shared = new CSharpDbConnection(connectionString))
        {
            await shared.OpenAsync(Ct);
            using var cmd = shared.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO t VALUES (1, 'persisted');";
            await cmd.ExecuteNonQueryAsync(Ct);

            await shared.SaveToFileAsync(savePath, Ct);
        }

        await using var disk = new CSharpDbConnection($"Data Source={savePath}");
        await disk.OpenAsync(Ct);
        using var verify = disk.CreateCommand();
        verify.CommandText = "SELECT name FROM t WHERE id = 1;";
        Assert.Equal("persisted", await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task ClearPoolAsync_ClearsNamedSharedMemoryHosts()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";

        await using (var conn = new CSharpDbConnection(connectionString))
        {
            await conn.OpenAsync(Ct);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await cmd.ExecuteNonQueryAsync(Ct);
            cmd.CommandText = "INSERT INTO t VALUES (1, 'before-clear');";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        Assert.Equal(1, CSharpDbConnection.GetSharedMemoryHostCountForTest());
        await CSharpDbConnection.ClearPoolAsync(connectionString);
        Assert.Equal(0, CSharpDbConnection.GetSharedMemoryHostCountForTest());

        await using var reopened = new CSharpDbConnection(connectionString);
        await reopened.OpenAsync(Ct);
        using var verify = reopened.CreateCommand();
        verify.CommandText = "SELECT name FROM t;";
        await Assert.ThrowsAsync<CSharpDbDataException>(() => verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task ClearPoolAsync_PreservesCommittedSnapshotForActiveReadersDuringTransaction()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";

        await using var writer = new CSharpDbConnection(connectionString);
        await using var reader = new CSharpDbConnection(connectionString);
        await writer.OpenAsync(Ct);
        await reader.OpenAsync(Ct);

        using (var setup = writer.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);";
            await setup.ExecuteNonQueryAsync(Ct);
            setup.CommandText = "INSERT INTO t VALUES (1, 'baseline');";
            await setup.ExecuteNonQueryAsync(Ct);
        }

        await using var tx = await writer.BeginTransactionAsync(Ct);

        using (var pending = writer.CreateCommand())
        {
            pending.CommandText = "INSERT INTO t VALUES (2, 'pending');";
            await pending.ExecuteNonQueryAsync(Ct);
        }

        await CSharpDbConnection.ClearPoolAsync(connectionString);
        Assert.Equal(0, CSharpDbConnection.GetSharedMemoryHostCountForTest());

        using (var readCommitted = reader.CreateCommand())
        {
            readCommitted.CommandText = "SELECT COUNT(*) FROM t;";
            Assert.Equal(1L, await readCommitted.ExecuteScalarAsync(Ct));
        }

        await tx.CommitAsync(Ct);

        using var verify = reader.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM t;";
        Assert.Equal(2L, await verify.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task ClearAllPoolsAsync_ClearsAllNamedSharedMemoryHosts()
    {
        string firstCs = $"Data Source=:memory:{Guid.NewGuid():N}";
        string secondCs = $"Data Source=:memory:{Guid.NewGuid():N}";

        await using (var first = new CSharpDbConnection(firstCs))
        await using (var second = new CSharpDbConnection(secondCs))
        {
            await first.OpenAsync(Ct);
            await second.OpenAsync(Ct);
        }

        Assert.Equal(2, CSharpDbConnection.GetSharedMemoryHostCountForTest());
        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(0, CSharpDbConnection.GetSharedMemoryHostCountForTest());
    }

    private async Task<string> CreateSeedDatabaseAsync(string value)
    {
        string path = NewTempDbPath();

        await using var db = await Database.OpenAsync(path, Ct);
        await db.ExecuteAsync("CREATE TABLE seed_data (name TEXT)", Ct);
        await db.ExecuteAsync($"INSERT INTO seed_data VALUES ('{value}')", Ct);

        return path;
    }

    private string NewTempDbPath()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_data_memory_test_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort temp file cleanup.
        }
    }
}
