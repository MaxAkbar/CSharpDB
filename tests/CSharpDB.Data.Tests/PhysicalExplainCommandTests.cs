using System.Data;
using System.Data.Common;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public sealed class PhysicalExplainCommandTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_physical_explain_{Guid.NewGuid():N}.db");

    private CSharpDbConnection _connection = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _connection = new CSharpDbConnection($"Data Source={_dbPath}");
        await _connection.OpenAsync(Ct);

        using DbCommand command = _connection.CreateCommand();
        command.CommandText =
            "CREATE TABLE ado_plan (id INTEGER PRIMARY KEY, payload TEXT);";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText =
            """
            INSERT INTO ado_plan VALUES
                (1, 'phase7-prepared-secret-alpha'),
                (2, 'phase7-prepared-secret-beta');
            """;
        await command.ExecuteNonQueryAsync(Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        DeleteIfPresent(_dbPath);
        DeleteIfPresent(_dbPath + ".wal");
        DeleteIfPresent(_dbPath + ".shm");
    }

    [Fact]
    public async Task PreparedExplain_RebindsAndRedactsParameters()
    {
        using var command = (CSharpDbCommand)_connection.CreateCommand();
        command.CommandText =
            """
            EXPLAIN
            SELECT payload
            FROM ado_plan
            WHERE id = @id AND payload = @payload;
            """;
        CSharpDbParameter id = command.Parameters.AddWithValue("@id", 1);
        CSharpDbParameter payload = command.Parameters.AddWithValue(
            "@payload",
            "phase7-prepared-secret-alpha");
        command.Prepare();

        AdoPlan first = await ReadPlanAsync(command);
        AssertPreparedPlanIsRedacted(
            first,
            "phase7-prepared-secret-alpha");

        id.Value = 2;
        payload.Value = "phase7-prepared-secret-beta";

        AdoPlan second = await ReadPlanAsync(command);
        AssertPreparedPlanIsRedacted(
            second,
            "phase7-prepared-secret-beta");
    }

    [Fact]
    public async Task PlainExplain_NullableIntegerColumnsRetainIntegerSchema()
    {
        using DbCommand command = _connection.CreateCommand();
        command.CommandText =
            "EXPLAIN SELECT payload FROM ado_plan WHERE id = 1;";

        await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
        DataTable schemaTable = Assert.IsType<DataTable>(reader.GetSchemaTable());

        string[] nullableIntegerColumns =
        [
            "parent_node_id",
            "estimated_rows",
            "actual_rows",
            "actual_loops",
            "elapsed_microseconds",
        ];

        foreach (string columnName in nullableIntegerColumns)
        {
            int ordinal = reader.GetOrdinal(columnName);
            Assert.Equal("BIGINT", reader.GetDataTypeName(ordinal));
            Assert.Equal(typeof(long), reader.GetFieldType(ordinal));

            DataRow schemaRow = Assert.Single(
                schemaTable.Rows.Cast<DataRow>(),
                row => string.Equals(
                    row.Field<string>("ColumnName"),
                    columnName,
                    StringComparison.Ordinal));
            Assert.Equal(
                (int)System.Data.DbType.Int64,
                schemaRow.Field<int>("ProviderType"));
            Assert.Equal("BIGINT", schemaRow.Field<string>("DataTypeName"));
            Assert.True(schemaRow.Field<bool>("AllowDBNull"));
        }

        foreach (string columnName in
                 new[] { "node_id", "operator_type", "status" })
        {
            DataRow schemaRow = Assert.Single(
                schemaTable.Rows.Cast<DataRow>(),
                row => string.Equals(
                    row.Field<string>("ColumnName"),
                    columnName,
                    StringComparison.Ordinal));
            Assert.False(schemaRow.Field<bool>("AllowDBNull"));
        }

        int actualRowsOrdinal = reader.GetOrdinal("actual_rows");
        int actualLoopsOrdinal = reader.GetOrdinal("actual_loops");
        int elapsedOrdinal = reader.GetOrdinal("elapsed_microseconds");
        int rowCount = 0;
        while (await reader.ReadAsync(Ct))
        {
            rowCount++;
            Assert.True(reader.IsDBNull(actualRowsOrdinal));
            Assert.True(reader.IsDBNull(actualLoopsOrdinal));
            Assert.True(reader.IsDBNull(elapsedOrdinal));
        }

        Assert.True(rowCount >= 2);
    }

    [Fact]
    public async Task Explain_UsesConnectionLocalTemporaryTableState()
    {
        using DbCommand command = _connection.CreateCommand();
        command.CommandText =
            "CREATE TEMP TABLE ado_temp_plan (id INTEGER PRIMARY KEY, value TEXT);";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText =
            "INSERT INTO ado_temp_plan VALUES (1, 'before');";
        await command.ExecuteNonQueryAsync(Ct);

        command.CommandText =
            "EXPLAIN SELECT value FROM ado_temp_plan WHERE id = 1;";
        AdoPlan planned = await ReadPlanAsync((CSharpDbCommand)command);
        Assert.Contains(
            planned.Rows,
            row => planned.Text(row, "operator_type") is
                "primary_key_lookup" or "table_scan");

        command.CommandText =
            "EXPLAIN ANALYZE UPDATE ado_temp_plan SET value = 'after' WHERE id = 1;";
        AdoPlan profiled = await ReadPlanAsync((CSharpDbCommand)command);
        Assert.Contains(
            profiled.Rows,
            row =>
                profiled.Text(row, "operator_type") == "update" &&
                Convert.ToInt64(profiled.Value(row, "actual_rows")) == 1);

        command.CommandText =
            "SELECT value FROM ado_temp_plan WHERE id = 1;";
        Assert.Equal("after", await command.ExecuteScalarAsync(Ct));
    }

    private static async Task<AdoPlan> ReadPlanAsync(CSharpDbCommand command)
    {
        await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
        string[] columnNames = Enumerable.Range(0, reader.FieldCount)
            .Select(reader.GetName)
            .ToArray();
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(Ct))
        {
            var values = new object[reader.FieldCount];
            _ = reader.GetValues(values);
            rows.Add(values);
        }

        return new AdoPlan(columnNames, rows);
    }

    private static void AssertPreparedPlanIsRedacted(
        AdoPlan plan,
        string secret)
    {
        Assert.Contains(
            plan.Rows,
            row => plan.Text(row, "operator_type") == "primary_key_lookup");
        Assert.All(
            plan.Rows,
            row => Assert.Equal(DBNull.Value, plan.Value(row, "actual_rows")));

        string[] textValues = plan.Rows
            .SelectMany(static row => row)
            .OfType<string>()
            .ToArray();
        Assert.DoesNotContain(
            textValues,
            value => value.Contains(secret, StringComparison.Ordinal));
        Assert.DoesNotContain(
            textValues,
            value => value.Contains("@id", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(
            textValues,
            value => value.Contains("@payload", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(
            plan.Rows,
            row =>
                plan.Text(row, "predicate") is { } predicate &&
                predicate.Contains('?'));
    }

    private static void DeleteIfPresent(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class AdoPlan(
        string[] columnNames,
        List<object?[]> rows)
    {
        private readonly Dictionary<string, int> _ordinals = columnNames
            .Select((name, ordinal) => (name, ordinal))
            .ToDictionary(
                static item => item.name,
                static item => item.ordinal,
                StringComparer.OrdinalIgnoreCase);

        internal List<object?[]> Rows { get; } = rows;

        internal object? Value(object?[] row, string columnName)
            => row[Ordinal(columnName)];

        internal string? Text(object?[] row, string columnName)
            => Value(row, columnName) as string;

        private int Ordinal(string columnName)
            => _ordinals.TryGetValue(columnName, out int ordinal)
                ? ordinal
                : throw new Xunit.Sdk.XunitException(
                    $"Expected plan column '{columnName}' was not present.");
    }
}
