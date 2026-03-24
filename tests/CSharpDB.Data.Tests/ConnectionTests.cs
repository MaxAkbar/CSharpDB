using System.Data;
using System.Data.Common;
using CSharpDB.Data;

namespace CSharpDB.Data.Tests;

public class ConnectionTests : IDisposable
{
    private readonly string _dbPath;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ConnectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_data_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task OpenAsync_CreatesDatabase()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        Assert.Equal(ConnectionState.Closed, conn.State);

        await conn.OpenAsync(Ct);
        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.True(File.Exists(_dbPath));
    }

    [Fact]
    public async Task CloseAsync_SetsStateToClosed()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        await conn.CloseAsync();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public async Task OpenAsync_WhenAlreadyOpen_Throws()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => conn.OpenAsync(Ct));
    }

    [Fact]
    public async Task OpenAsync_WithEmptyDataSource_Throws()
    {
        await using var conn = new CSharpDbConnection("Data Source=");
        await Assert.ThrowsAsync<InvalidOperationException>(() => conn.OpenAsync(Ct));
    }

    [Fact]
    public void DataSource_ParsesConnectionString()
    {
        var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        Assert.Equal(_dbPath, conn.DataSource);
    }

    [Fact]
    public void ChangeDatabase_ThrowsNotSupported()
    {
        var conn = new CSharpDbConnection();
        Assert.Throws<NotSupportedException>(() => conn.ChangeDatabase("other"));
    }

    [Fact]
    public async Task CreateCommand_ReturnsCommandWithConnection()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        using var cmd = conn.CreateCommand();
        Assert.NotNull(cmd);
        Assert.IsType<CSharpDbCommand>(cmd);
        Assert.Same(conn, cmd.Connection);

        CSharpDbParameter parameter = cmd.Parameters.AddWithValue("@id", 1);
        Assert.Equal("@id", parameter.ParameterName);
        Assert.Equal(1, parameter.Value);
    }

    [Fact]
    public async Task GetTableNames_ReturnsCreatedTables()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = (CSharpDbCommand)conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE alpha (id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE TABLE beta (id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        var names = conn.GetTableNames();
        Assert.Contains("alpha", names);
        Assert.Contains("beta", names);
        Assert.Equal(2, names.Count);
    }

    [Fact]
    public async Task GetTableSchema_ReturnsColumns()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = (CSharpDbCommand)conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        var schema = conn.GetTableSchema("users");
        Assert.NotNull(schema);
        Assert.Equal("users", schema!.TableName);
        Assert.Equal(3, schema.Columns.Count);
        Assert.Equal("id", schema.Columns[0].Name);
        Assert.True(schema.Columns[0].IsPrimaryKey);
        Assert.Equal(CSharpDB.Primitives.DbType.Integer, schema.Columns[0].Type);
        Assert.Equal("name", schema.Columns[1].Name);
        Assert.Equal(CSharpDB.Primitives.DbType.Text, schema.Columns[1].Type);
    }

    [Fact]
    public async Task GetTableSchema_NonExistent_ReturnsNull()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);
        Assert.Null(conn.GetTableSchema("nonexistent"));
    }

    [Fact]
    public async Task GetSchema_WithoutArguments_ReturnsSupportedCollections()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        DataTable schema = conn.GetSchema();
        Assert.Equal(5, schema.Rows.Count);
        Assert.Equal(0, GetRequiredInt(schema, DbMetaDataCollectionNames.MetaDataCollections, "NumberOfRestrictions"));
        Assert.Equal(4, GetRequiredInt(schema, "Tables", "NumberOfRestrictions"));
        Assert.Equal(4, GetRequiredInt(schema, "Columns", "NumberOfRestrictions"));
        Assert.Equal(4, GetRequiredInt(schema, "Indexes", "NumberOfRestrictions"));
        Assert.Equal(3, GetRequiredInt(schema, "Views", "NumberOfRestrictions"));
    }

    [Fact]
    public async Task GetSchema_Tables_ReturnsTablesAndViews()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, message TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE VIEW adult_users AS SELECT id, name FROM users;";
        await cmd.ExecuteNonQueryAsync(Ct);

        DataTable schema = conn.GetSchema("Tables");
        Assert.Equal(3, schema.Rows.Count);
        Assert.Equal("BASE TABLE", GetRequiredString(schema, "users", "TABLE_TYPE"));
        Assert.Equal("BASE TABLE", GetRequiredString(schema, "audit_log", "TABLE_TYPE"));
        Assert.Equal("VIEW", GetRequiredString(schema, "adult_users", "TABLE_TYPE"));
    }

    [Fact]
    public async Task GetSchema_Tables_AppliesRestrictions()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE VIEW users_view AS SELECT id, name FROM users;";
        await cmd.ExecuteNonQueryAsync(Ct);

        DataTable tableOnly = conn.GetSchema("Tables", [null, null, "users", "BASE TABLE"]);
        DataRow tableRow = Assert.Single(tableOnly.Rows.Cast<DataRow>());
        Assert.Equal("users", tableRow["TABLE_NAME"]);

        DataTable viewOnly = conn.GetSchema("Tables", [null, null, "users_view", "VIEW"]);
        DataRow viewRow = Assert.Single(viewOnly.Rows.Cast<DataRow>());
        Assert.Equal("users_view", viewRow["TABLE_NAME"]);
    }

    [Fact]
    public async Task GetSchema_Columns_ReturnsColumnMetadata()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY IDENTITY, name TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);

        DataTable schema = conn.GetSchema("Columns");
        DataRow[] rows = schema.Rows.Cast<DataRow>()
            .Where(row => string.Equals((string)row["TABLE_NAME"], "users", StringComparison.OrdinalIgnoreCase))
            .OrderBy(row => (int)row["ORDINAL_POSITION"])
            .ToArray();

        Assert.Equal(3, rows.Length);
        Assert.Equal("id", rows[0]["COLUMN_NAME"]);
        Assert.Equal(1, rows[0]["ORDINAL_POSITION"]);
        Assert.Equal("INTEGER", rows[0]["DATA_TYPE"]);
        Assert.Equal("NO", rows[0]["IS_NULLABLE"]);
        Assert.True((bool)rows[0]["IS_PRIMARY_KEY"]);
        Assert.True((bool)rows[0]["IS_IDENTITY"]);

        DataTable filtered = conn.GetSchema("Columns", [null, null, "users", "name"]);
        DataRow filteredRow = Assert.Single(filtered.Rows.Cast<DataRow>());
        Assert.Equal("name", filteredRow["COLUMN_NAME"]);
    }

    [Fact]
    public async Task GetSchema_Indexes_ReturnsIndexMetadata()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE UNIQUE INDEX idx_users_email ON users(email);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE INDEX idx_users_age ON users(age);";
        await cmd.ExecuteNonQueryAsync(Ct);

        DataTable schema = conn.GetSchema("Indexes");
        Assert.Equal(2, schema.Rows.Count);
        Assert.True(GetRequiredBoolean(schema, "idx_users_email", "IS_UNIQUE"));
        Assert.Equal("email", GetRequiredString(schema, "idx_users_email", "COLUMN_LIST"));
        Assert.Equal("Sql", GetRequiredString(schema, "idx_users_email", "INDEX_TYPE"));

        DataTable filtered = conn.GetSchema("Indexes", [null, null, "users", "idx_users_age"]);
        DataRow filteredRow = Assert.Single(filtered.Rows.Cast<DataRow>());
        Assert.Equal("idx_users_age", filteredRow["INDEX_NAME"]);
    }

    [Fact]
    public async Task GetSchema_Views_ReturnsViewMetadata()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER);";
        await cmd.ExecuteNonQueryAsync(Ct);
        cmd.CommandText = "CREATE VIEW adults AS SELECT id FROM users WHERE age >= 18;";
        await cmd.ExecuteNonQueryAsync(Ct);

        DataTable schema = conn.GetSchema("Views");
        DataRow row = Assert.Single(schema.Rows.Cast<DataRow>());
        Assert.Equal("adults", row["TABLE_NAME"]);
        Assert.Equal("NONE", row["CHECK_OPTION"]);
        Assert.Equal("NO", row["IS_UPDATABLE"]);
        Assert.Contains("SELECT id FROM users", (string)row["VIEW_DEFINITION"], StringComparison.OrdinalIgnoreCase);

        DataTable filtered = conn.GetSchema("Views", [null, null, "adults"]);
        DataRow filteredRow = Assert.Single(filtered.Rows.Cast<DataRow>());
        Assert.Equal("adults", filteredRow["TABLE_NAME"]);
    }

    [Fact]
    public async Task GetSchema_UnsupportedCollection_Throws()
    {
        await using var conn = new CSharpDbConnection($"Data Source={_dbPath}");
        await conn.OpenAsync(Ct);

        Assert.Throws<ArgumentException>(() => conn.GetSchema("Procedures"));
    }

    private static int GetRequiredInt(DataTable table, string collectionName, string columnName)
        => (int)GetRequiredRow(table, collectionName)[columnName];

    private static bool GetRequiredBoolean(DataTable table, string objectName, string columnName)
        => (bool)GetRequiredRow(table, objectName)[columnName];

    private static string GetRequiredString(DataTable table, string objectName, string columnName)
        => (string)GetRequiredRow(table, objectName)[columnName];

    private static DataRow GetRequiredRow(DataTable table, string objectName)
    {
        string keyColumnName = table.Columns.Contains("CollectionName")
            ? "CollectionName"
            : table.Columns.Contains("INDEX_NAME")
                ? "INDEX_NAME"
                : "TABLE_NAME";

        return Assert.Single(
            table.Rows.Cast<DataRow>().Where(row =>
                string.Equals(row[keyColumnName] as string, objectName, StringComparison.OrdinalIgnoreCase)));
    }
}

