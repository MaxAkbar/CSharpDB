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
    private CSharpDbConnection _conn = null!;
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

        _nextId = 1_000_000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _preparedSelectCmd.Dispose();
        _conn.Dispose();
        try { File.Delete(_dbPath); } catch { }
        try { File.Delete(_dbPath + ".wal"); } catch { }
    }

    [Benchmark(Description = "ADO.NET Connection Open+Close")]
    public async Task ConnectionOpenClose()
    {
        // Uses a unique file per invocation because CSharpDB holds an exclusive file lock
        var tempPath = Path.Combine(Path.GetTempPath(), $"csharpdb_oc_{Guid.NewGuid():N}.db");
        try
        {
            await using var conn = new CSharpDbConnection($"Data Source={tempPath}");
            await conn.OpenAsync();
            await conn.CloseAsync();
        }
        finally
        {
            try { File.Delete(tempPath); } catch { }
            try { File.Delete(tempPath + ".wal"); } catch { }
        }
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
