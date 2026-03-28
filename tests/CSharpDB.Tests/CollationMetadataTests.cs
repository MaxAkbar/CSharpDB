using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class CollationMetadataTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateTable_WithColumnCollation_PersistsSchemaMetadata()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);

        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE NOT NULL)", Ct);

        var schema = db.GetTableSchema("users");
        Assert.NotNull(schema);
        Assert.Equal(2, schema.Columns.Count);
        Assert.Null(schema.Columns[0].Collation);
        Assert.Equal("NOCASE", schema.Columns[1].Collation);
    }

    [Fact]
    public async Task AlterTable_AddColumn_WithCollation_PersistsSchemaMetadata()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY)", Ct);

        await db.ExecuteAsync("ALTER TABLE users ADD COLUMN email TEXT COLLATE NOCASE", Ct);

        var schema = db.GetTableSchema("users");
        Assert.NotNull(schema);
        Assert.Equal("NOCASE", schema.Columns.Single(column => column.Name == "email").Collation);
    }

    [Fact]
    public async Task CreateIndex_WithColumnCollation_PersistsIndexMetadata()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", Ct);

        await db.ExecuteAsync("CREATE INDEX idx_users_name ON users (name COLLATE NOCASE)", Ct);

        var index = Assert.Single(db.GetIndexes(), static item => string.Equals(item.IndexName, "idx_users_name", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(["name"], index.Columns);
        Assert.Equal(["NOCASE"], index.ColumnCollations);
    }

    [Fact]
    public async Task CreateTable_WithCollationOnNonTextColumn_Throws()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);

        var error = await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
            await db.ExecuteAsync("CREATE TABLE users (id INTEGER COLLATE NOCASE)", Ct));

        Assert.Equal(CSharpDB.Primitives.ErrorCode.TypeMismatch, error.Code);
    }
}
