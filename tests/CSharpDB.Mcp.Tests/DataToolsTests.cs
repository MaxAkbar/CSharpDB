using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Mcp.Tools;

namespace CSharpDB.Mcp.Tests;

public sealed class DataToolsTests
{
    [Fact]
    public async Task BrowseTable_ReportsLogicalAndPhysicalColumnTypes()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_mcp_{Guid.NewGuid():N}.db");

        try
        {
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions { DataSource = databasePath });
            SqlExecutionResult setup = await client.ExecuteSqlAsync(
                """
                CREATE TABLE logical_values (
                    id INTEGER PRIMARY KEY,
                    big_value BIGINT,
                    enabled BOOLEAN,
                    recorded_at DATETIME2(3),
                    version ROWVERSION
                );
                INSERT INTO logical_values
                    (id, big_value, enabled, recorded_at)
                VALUES
                    (1, 9223372036854775807, 1, '2026-08-06 12:34:56.789');
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(setup.Error);

            string json = await DataTools.BrowseTable(
                client,
                "logical_values",
                page: 1,
                pageSize: 10);

            using JsonDocument document = JsonDocument.Parse(json);
            Dictionary<string, JsonElement> columns = document.RootElement
                .GetProperty("columns")
                .EnumerateArray()
                .ToDictionary(
                    static column => column.GetProperty("name").GetString()!,
                    static column => column.Clone(),
                    StringComparer.Ordinal);

            AssertColumn(columns, "id", "INTEGER", "Integer");
            AssertColumn(columns, "big_value", "BIGINT", "Integer");
            AssertColumn(columns, "enabled", "BOOLEAN", "Integer");
            AssertColumn(columns, "recorded_at", "DATETIME2(3)", "Text");
            AssertColumn(columns, "version", "ROWVERSION", "Blob");

            JsonElement row = Assert.Single(
                document.RootElement.GetProperty("rows").EnumerateArray());
            Assert.True(row.GetProperty("enabled").GetBoolean());
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    private static void AssertColumn(
        IReadOnlyDictionary<string, JsonElement> columns,
        string name,
        string logicalType,
        string storageType)
    {
        JsonElement column = columns[name];
        Assert.Equal(logicalType, column.GetProperty("type").GetString());
        Assert.Equal(storageType, column.GetProperty("storageType").GetString());
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
