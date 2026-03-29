using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures the write and read cost of ordered SQL text indexes under the built-in collation set.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class CollationIndexBenchmarks
{
    private const int RangeStartId = 2_000;
    private const int RangeLength = 1_000;
    private const int LookupId = 1_234;

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    [Params("BINARY", "NOCASE", "NOCASE_AI", "ICU:en-US")]
    public string Collation { get; set; } = null!;

    private BenchmarkDatabase _lookupBench = null!;
    private BenchmarkDatabase _writeBench = null!;
    private string _lookupSql = null!;
    private string _rangeSql = null!;
    private string _orderBySql = null!;
    private int _nextInsertId;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _lookupBench.Dispose();
        _writeBench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "WHERE text eq (collated text index)")]
    public async Task LookupTextEquality()
    {
        await using var result = await _lookupBench.Db.ExecuteAsync(_lookupSql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "WHERE text range (ordered text index)")]
    public async Task RangeScanTextIndex()
    {
        await using var result = await _lookupBench.Db.ExecuteAsync(_rangeSql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY text + LIMIT 100 (ordered text index)")]
    public async Task OrderByTextIndexTopN()
    {
        await using var result = await _lookupBench.Db.ExecuteAsync(_orderBySql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "INSERT with collated text index (tx rollback)")]
    public async Task InsertWithTextIndex()
    {
        int id = Interlocked.Increment(ref _nextInsertId);
        string sql =
            $"INSERT INTO bench_collation_idx VALUES ({id}, '{EscapeSql(BuildStoredName(id))}', 'payload_{id:D6}')";

        await _writeBench.Db.BeginTransactionAsync();
        try
        {
            await _writeBench.Db.ExecuteAsync(sql);
        }
        finally
        {
            await _writeBench.Db.RollbackAsync();
        }
    }

    private async Task GlobalSetupAsync()
    {
        string createSql =
            $"CREATE TABLE bench_collation_idx (id INTEGER PRIMARY KEY, name TEXT COLLATE {Collation} NOT NULL, payload TEXT NOT NULL)";

        _lookupBench = await BenchmarkDatabase.CreateWithSchemaAsync(createSql);
        _writeBench = await BenchmarkDatabase.CreateWithSchemaAsync(createSql);

        await SeedBenchAsync(_lookupBench, RowCount);
        await SeedBenchAsync(_writeBench, 1_024);

        await _lookupBench.Db.ExecuteAsync(
            $"CREATE INDEX idx_bench_collation_name ON bench_collation_idx(name COLLATE {Collation})");
        await _writeBench.Db.ExecuteAsync(
            $"CREATE INDEX idx_bench_collation_name ON bench_collation_idx(name COLLATE {Collation})");

        _lookupSql =
            $"SELECT id FROM bench_collation_idx WHERE name = '{EscapeSql(BuildLookupProbeText(LookupId))}'";
        _rangeSql =
            $"SELECT id, name FROM bench_collation_idx WHERE name >= '{EscapeSql(BuildRangeBound(RangeStartId))}' " +
            $"AND name < '{EscapeSql(BuildRangeBound(RangeStartId + RangeLength))}' ORDER BY name";
        _orderBySql =
            "SELECT id, name FROM bench_collation_idx ORDER BY name LIMIT 100";
        _nextInsertId = 10_000_000;
    }

    private async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        await bench.SeedAsync("bench_collation_idx", rowCount, id =>
            $"INSERT INTO bench_collation_idx VALUES ({id}, '{EscapeSql(BuildStoredName(id))}', 'payload_{id:D6}')");
    }

    private string BuildLookupProbeText(int id)
    {
        string stored = BuildStoredName(id);
        return Collation switch
        {
            "BINARY" => stored,
            "NOCASE" => stored.ToUpperInvariant(),
            "NOCASE_AI" => RemoveAccents(stored).ToUpperInvariant(),
            "ICU:en-US" => stored.Normalize(NormalizationForm.FormD),
            _ => stored,
        };
    }

    private string BuildRangeBound(int id)
        => BuildLookupProbeText(id);

    private static string BuildStoredName(int id)
        => FormattableString.Invariant($"résumé_{id:D6}");

    private static string RemoveAccents(string text)
    {
        string decomposed = text.Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(decomposed.Length);

        for (int i = 0; i < decomposed.Length; i++)
        {
            char ch = decomposed[i];
            UnicodeCategory category = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (category is UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark or UnicodeCategory.EnclosingMark)
                continue;

            builder.Append(ch);
        }

        return builder.ToString().Normalize(NormalizationForm.FormC);
    }

    private static string EscapeSql(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);
}
