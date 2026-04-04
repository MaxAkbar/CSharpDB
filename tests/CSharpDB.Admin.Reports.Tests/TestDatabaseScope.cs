using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Tests;

public sealed class TestDatabaseScope : IAsyncDisposable
{
    private readonly ICSharpDbClient _client;

    private TestDatabaseScope(string databasePath, ICSharpDbClient client)
    {
        DatabasePath = databasePath;
        _client = client;
    }

    public string DatabasePath { get; }
    public ICSharpDbClient Client => _client;

    public static async Task<TestDatabaseScope> CreateAsync(string? name = null)
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"{name ?? "csharpdb_admin_reports"}_{Guid.NewGuid():N}.db");

        ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = databasePath,
        });

        await client.GetInfoAsync(TestContext.Current.CancellationToken);
        return new TestDatabaseScope(databasePath, client);
    }

    public async Task ExecuteAsync(string sql)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(sql, TestContext.Current.CancellationToken);
        Assert.Null(result.Error);
    }

    public async Task<IReadOnlyList<Dictionary<string, object?>>> QueryRowsAsync(string sql)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(sql, TestContext.Current.CancellationToken);
        Assert.Null(result.Error);

        if (result.ColumnNames is null || result.Rows is null)
            return [];

        var rows = new List<Dictionary<string, object?>>(result.Rows.Count);
        foreach (object?[] row in result.Rows)
        {
            var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < result.ColumnNames.Length && i < row.Length; i++)
                values[result.ColumnNames[i]] = row[i];

            rows.Add(values);
        }

        return rows;
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        TryDelete(DatabasePath);
        TryDelete(DatabasePath + ".wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
