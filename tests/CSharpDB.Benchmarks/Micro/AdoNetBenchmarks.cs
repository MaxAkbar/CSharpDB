using BenchmarkDotNet.Attributes;
using CSharpDB.Data;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures ADO.NET provider overhead: connection lifecycle,
/// parameterized queries, and data reader throughput.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class AdoNetBenchmarks
{
    private string _dbPath = null!;
    private string _openCloseNoPoolPath = null!;
    private string _openClosePoolPath = null!;
    private string _openCloseNoPoolConnectionString = null!;
    private string _openClosePoolConnectionString = null!;
    private CSharpDbConnection _conn = null!;
    private CSharpDbCommand _preparedInsertCmd = null!;
    private CSharpDbParameter _preparedInsertId = null!;
    private CSharpDbParameter _preparedInsertName = null!;
    private CSharpDbParameter _preparedInsertVal = null!;
    private CSharpDbCommand _preparedSelectCmd = null!;
    private CSharpDbParameter _preparedSelectMinVal = null!;
    private int _nextId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_adonet_bench_{Guid.NewGuid():N}.db");
        _conn = new CSharpDbConnection($"Data Source={_dbPath}");
        _conn.OpenAsync().GetAwaiter().GetResult();

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
        cmd.ExecuteNonQuery();

        // Seed 1000 rows
        for (int i = 0; i < 1000; i++)
        {
            cmd.CommandText = $"INSERT INTO t VALUES ({i}, 'row_{i}', {i * 10})";
            cmd.ExecuteNonQuery();
        }

        _preparedSelectCmd = (CSharpDbCommand)_conn.CreateCommand();
        _preparedSelectCmd.CommandText = "SELECT * FROM t WHERE value > @minVal";
        _preparedSelectMinVal = _preparedSelectCmd.Parameters.AddWithValue("@minVal", 5000);
        _preparedSelectCmd.Prepare();

        _preparedInsertCmd = (CSharpDbCommand)_conn.CreateCommand();
        _preparedInsertCmd.CommandText = "INSERT INTO t VALUES (@id, @name, @val)";
        _preparedInsertId = _preparedInsertCmd.Parameters.AddWithValue("@id", 0);
        _preparedInsertName = _preparedInsertCmd.Parameters.AddWithValue("@name", "");
        _preparedInsertVal = _preparedInsertCmd.Parameters.AddWithValue("@val", 0);
        _preparedInsertCmd.Prepare();

        _openCloseNoPoolPath = Path.Combine(Path.GetTempPath(), $"csharpdb_adonet_oc_nopool_{Guid.NewGuid():N}.db");
        _openClosePoolPath = Path.Combine(Path.GetTempPath(), $"csharpdb_adonet_oc_pool_{Guid.NewGuid():N}.db");
        _openCloseNoPoolConnectionString = $"Data Source={_openCloseNoPoolPath};Pooling=false";
        _openClosePoolConnectionString = $"Data Source={_openClosePoolPath};Pooling=true;Max Pool Size=16";

        using (var noPoolConn = new CSharpDbConnection(_openCloseNoPoolConnectionString))
        {
            noPoolConn.Open();
            noPoolConn.Close();
        }

        using (var pooledConn = new CSharpDbConnection(_openClosePoolConnectionString))
        {
            pooledConn.Open();
            pooledConn.Close();
        }

        CSharpDbConnection.ClearPool(_openClosePoolConnectionString);
        _nextId = 1_000_000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _preparedInsertCmd.Dispose();
        _preparedSelectCmd.Dispose();
        _conn.Dispose();
        CSharpDbConnection.ClearAllPools();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_dbPath + ".wal"); } catch { }
        try { File.Delete(_openCloseNoPoolPath); } catch { }
        try { File.Delete(_openCloseNoPoolPath + ".wal"); } catch { }
        try { File.Delete(_openClosePoolPath); } catch { }
        try { File.Delete(_openClosePoolPath + ".wal"); } catch { }
    }

    [Benchmark(Baseline = true, Description = "ADO.NET Connection Open+Close (Pooling Off)")]
    public async Task ConnectionOpenClose_PoolingOff()
    {
        await using var conn = new CSharpDbConnection(_openCloseNoPoolConnectionString);
        await conn.OpenAsync();
        await conn.CloseAsync();
    }

    [Benchmark(Description = "ADO.NET Connection Open+Close (Pooling On)")]
    public async Task ConnectionOpenClose_PoolingOn()
    {
        await using var conn = new CSharpDbConnection(_openClosePoolConnectionString);
        await conn.OpenAsync();
        await conn.CloseAsync();
    }

    [Benchmark(Description = "ADO.NET ExecuteNonQuery (INSERT)")]
    public async Task ExecuteNonQuery_Insert()
    {
        int id = Interlocked.Increment(ref _nextId);
        var cmd = new CSharpDbCommand { Connection = _conn };
        cmd.CommandText = "INSERT INTO t VALUES (@id, @name, @val)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@name", $"row_{id}");
        cmd.Parameters.AddWithValue("@val", id * 10);
        await cmd.ExecuteNonQueryAsync();
        cmd.Dispose();
    }

    [Benchmark(Description = "ADO.NET Prepared INSERT (reused command)")]
    public async Task PreparedExecuteNonQuery_Insert_ReusedCommand()
    {
        int id = Interlocked.Increment(ref _nextId);
        _preparedInsertId.Value = id;
        _preparedInsertName.Value = $"row_{id}";
        _preparedInsertVal.Value = id * 10;
        await _preparedInsertCmd.ExecuteNonQueryAsync();
    }

    [Benchmark(Description = "ADO.NET ExecuteNonQuery Batch100 (new command)")]
    public async Task ExecuteNonQuery_Insert_Batch100()
    {
        await using var tx = await _conn.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            var cmd = new CSharpDbCommand { Connection = _conn };
            cmd.CommandText = "INSERT INTO t VALUES (@id, @name, @val)";
            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@name", $"row_{id}");
            cmd.Parameters.AddWithValue("@val", id * 10);
            await cmd.ExecuteNonQueryAsync();
            cmd.Dispose();
        }

        await tx.CommitAsync();
    }

    [Benchmark(Description = "ADO.NET Prepared INSERT Batch100 (reused command)")]
    public async Task PreparedExecuteNonQuery_Insert_ReusedCommand_Batch100()
    {
        await using var tx = await _conn.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            _preparedInsertId.Value = id;
            _preparedInsertName.Value = $"row_{id}";
            _preparedInsertVal.Value = id * 10;
            await _preparedInsertCmd.ExecuteNonQueryAsync();
        }

        await tx.CommitAsync();
    }

    [Benchmark(Description = "ADO.NET ExecuteScalar (COUNT)")]
    public async Task ExecuteScalar_Count()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM t";
        await cmd.ExecuteScalarAsync();
    }

    [Benchmark(Description = "ADO.NET ExecuteReader (100 rows)")]
    public async Task ExecuteReader_100Rows()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM t LIMIT 100";
        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetInt64(2);
            count++;
        }
    }

    [Benchmark(Description = "ADO.NET Parameterized SELECT")]
    public async Task ParameterizedSelect()
    {
        var cmd = new CSharpDbCommand { Connection = _conn };
        cmd.CommandText = "SELECT * FROM t WHERE value > @minVal";
        cmd.Parameters.AddWithValue("@minVal", 5000);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _ = reader.GetInt64(0);
        }
        cmd.Dispose();
    }

    [Benchmark(Description = "ADO.NET Prepared SELECT (reused command)")]
    public async Task PreparedParameterizedSelect_ReusedCommand()
    {
        _preparedSelectMinVal.Value = 5000;
        await using var reader = await _preparedSelectCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            _ = reader.GetInt64(0);
        }
    }
}
