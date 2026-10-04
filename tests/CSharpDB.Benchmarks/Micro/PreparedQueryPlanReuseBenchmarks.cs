using BenchmarkDotNet.Attributes;
using CSharpDB.Data;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>Exercises prepared-plan reuse while cycling parameter values across cache-size boundaries.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PreparedQueryPlanReuseBenchmarks
{
    [Params(1, 64, 65, 256, 2048)]
    public int ValueSetCount { get; set; }

    private CSharpDbConnection _connection = null!;
    private CSharpDbCommand _lookup = null!;
    private CSharpDbCommand _range = null!;
    private CSharpDbCommand _primaryKey = null!;
    private CSharpDbCommand _primaryKeyOnly = null!;
    private CSharpDbCommand _primaryKeyRange = null!;
    private CSharpDbParameter _lookupValue = null!;
    private CSharpDbParameter _lower = null!;
    private CSharpDbParameter _upper = null!;
    private CSharpDbParameter _primaryKeyValue = null!;
    private CSharpDbParameter _primaryKeyOnlyValue = null!;
    private CSharpDbParameter _primaryKeyRangeLower = null!;
    private CSharpDbParameter _primaryKeyRangeUpper = null!;
    private int _lookupIndex;
    private int _rangeIndex;
    private int _primaryKeyIndex;
    private int _primaryKeyOnlyIndex;
    private int _primaryKeyRangeIndex;

    [GlobalSetup]
    public async Task Setup()
    {
        _connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await _connection.OpenAsync();
        using var setup = _connection.CreateCommand();
        setup.CommandText = "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER)";
        await setup.ExecuteNonQueryAsync();
        await using (var transaction = await _connection.BeginTransactionAsync())
        {
            setup.Transaction = transaction;
            setup.CommandText = "INSERT INTO bench VALUES (@id, @value)";
            var id = setup.Parameters.AddWithValue("id", 0L);
            var value = setup.Parameters.AddWithValue("value", 0L);
            setup.Prepare();
            for (int i = 0; i < 10000; i++)
            {
                id.Value = value.Value = (long)i;
                await setup.ExecuteNonQueryAsync();
            }
            await transaction.CommitAsync();
        }
        setup.Transaction = null;
        setup.Parameters.Clear();
        setup.CommandText = "CREATE INDEX ix_bench_value ON bench(value)";
        await setup.ExecuteNonQueryAsync();

        _lookup = _connection.CreateCommand();
        _lookup.CommandText = "SELECT id, value FROM bench WHERE value = @p";
        _lookupValue = _lookup.Parameters.AddWithValue("p", 0L);
        _lookup.Prepare();
        _range = _connection.CreateCommand();
        _range.CommandText = "SELECT id, value FROM bench WHERE value >= @lower AND value < @upper ORDER BY value LIMIT 4";
        _lower = _range.Parameters.AddWithValue("lower", 0L);
        _upper = _range.Parameters.AddWithValue("upper", 0L);
        _range.Prepare();
        _primaryKey = _connection.CreateCommand();
        _primaryKey.CommandText = "SELECT id, value FROM bench WHERE id = @p";
        _primaryKeyValue = _primaryKey.Parameters.AddWithValue("p", 0L);
        _primaryKey.Prepare();
        _primaryKeyOnly = _connection.CreateCommand();
        _primaryKeyOnly.CommandText = "SELECT id, id FROM bench WHERE id = @p";
        _primaryKeyOnlyValue = _primaryKeyOnly.Parameters.AddWithValue("p", 0L);
        _primaryKeyOnly.Prepare();
        _primaryKeyRange = _connection.CreateCommand();
        _primaryKeyRange.CommandText = "SELECT id, value FROM bench WHERE id >= @lower AND id < @upper ORDER BY id LIMIT 4";
        _primaryKeyRangeLower = _primaryKeyRange.Parameters.AddWithValue("lower", 0L);
        _primaryKeyRangeUpper = _primaryKeyRange.Parameters.AddWithValue("upper", 0L);
        _primaryKeyRange.Prepare();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _lookup?.Dispose();
        _range?.Dispose();
        _primaryKey?.Dispose();
        _primaryKeyOnly?.Dispose();
        _primaryKeyRange?.Dispose();
        if (_connection is not null) await _connection.DisposeAsync();
    }

    [Benchmark(Description = "Prepared indexed equality, changing values")]
    public Task<long> IndexedEquality()
    {
        long value = 100L + 4 * _lookupIndex;
        _lookupIndex = (_lookupIndex + 1) % ValueSetCount;
        _lookupValue.Value = value;
        return ReadAsync(_lookup, value, 1);
    }

    [Benchmark(Description = "Prepared ordered range, changing values")]
    public Task<long> OrderedRange()
    {
        long value = 100L + 4 * _rangeIndex;
        _rangeIndex = (_rangeIndex + 1) % ValueSetCount;
        _lower.Value = value;
        _upper.Value = value + 4;
        return ReadAsync(_range, value, 4);
    }

    [Benchmark(Description = "Prepared primary-key equality, changing values")]
    public Task<long> PrimaryKeyEquality()
    {
        long value = 100L + 4 * _primaryKeyIndex;
        _primaryKeyIndex = (_primaryKeyIndex + 1) % ValueSetCount;
        _primaryKeyValue.Value = value;
        return ReadAsync(_primaryKey, value, 1);
    }

    [Benchmark(Description = "Prepared primary-key-only projection, changing values")]
    public Task<long> PrimaryKeyOnlyProjection()
    {
        long value = 100L + 4 * _primaryKeyOnlyIndex;
        _primaryKeyOnlyIndex = (_primaryKeyOnlyIndex + 1) % ValueSetCount;
        _primaryKeyOnlyValue.Value = value;
        return ReadAsync(_primaryKeyOnly, value, 1);
    }

    [Benchmark(Description = "Prepared primary-key range, changing values")]
    public Task<long> PrimaryKeyRange()
    {
        long value = 100L + 4 * _primaryKeyRangeIndex;
        _primaryKeyRangeIndex = (_primaryKeyRangeIndex + 1) % ValueSetCount;
        _primaryKeyRangeLower.Value = value;
        _primaryKeyRangeUpper.Value = value + 4;
        return ReadAsync(_primaryKeyRange, value, 4);
    }

    private static async Task<long> ReadAsync(CSharpDbCommand command, long value, int expectedRows)
    {
        await using var reader = await command.ExecuteReaderAsync();
        long checksum = 0;
        int row = 0;
        while (await reader.ReadAsync())
        {
            long id = reader.GetInt64(0);
            if (id != value + row || reader.GetInt64(1) != value + row)
                throw new InvalidOperationException("Prepared query returned an incorrect parameter value.");
            checksum += id;
            row++;
        }
        if (row != expectedRows) throw new InvalidOperationException("Prepared query returned an incorrect row count.");
        return checksum;
    }
}
