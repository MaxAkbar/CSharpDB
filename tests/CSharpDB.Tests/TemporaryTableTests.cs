using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TemporaryTableTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TempTable_SupportsDmlAndCatalogVisibility()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);

        await db.ExecuteAsync("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY IDENTITY, name TEXT, note TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO scratch (name, note) VALUES ('Alice', NULL), ('Bob', 'old')", Ct);
        await db.ExecuteAsync("UPDATE scratch SET note = 'new' WHERE id = 2", Ct);
        await db.ExecuteAsync("DELETE FROM scratch WHERE id = 1", Ct);

        await using (var rowsResult = await db.ExecuteAsync("SELECT id, name, note FROM scratch", Ct))
        {
            var rows = await rowsResult.ToListAsync(Ct);
            Assert.Single(rows);
            Assert.Equal(2, rows[0][0].AsInteger);
            Assert.Equal("Bob", rows[0][1].AsText);
            Assert.Equal("new", rows[0][2].AsText);
        }

        Assert.Equal(0, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.tables WHERE table_name = 'scratch'"));
        Assert.Equal(1, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_tables WHERE table_name = 'scratch'"));
        Assert.Equal(3, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_columns WHERE table_name = 'scratch'"));
        Assert.Equal(0, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_columns WHERE is_row_version = 1"));

        await db.ExecuteAsync("DROP TEMP TABLE scratch", Ct);
        Assert.Equal(0, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_tables"));
    }

    [Fact]
    public async Task DropTable_DropsShadowingTempBeforeDurableTable()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);

        await db.ExecuteAsync("CREATE TABLE shadowed (id INTEGER PRIMARY KEY, value TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO shadowed VALUES (1, 'durable')", Ct);
        await db.ExecuteAsync("CREATE TEMP TABLE shadowed (id INTEGER PRIMARY KEY, value TEXT)", Ct);
        await db.ExecuteAsync("INSERT INTO shadowed VALUES (2, 'temp')", Ct);

        Assert.Equal("temp", await ExecuteTextScalarAsync(db, "SELECT value FROM shadowed"));

        await db.ExecuteAsync("DROP TABLE shadowed", Ct);

        Assert.Equal("durable", await ExecuteTextScalarAsync(db, "SELECT value FROM shadowed"));
        Assert.Equal(1, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.tables WHERE table_name = 'shadowed'"));
        Assert.Equal(0, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_tables WHERE table_name = 'shadowed'"));
    }

    [Fact]
    public async Task PersistTempTable_CreatesDurableTableAndLeavesTempTable()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_temp_persist_{Guid.NewGuid():N}.db");
        try
        {
            await using (Database db = await Database.OpenAsync(path, Ct))
            {
                await db.ExecuteAsync("CREATE TEMPORARY TABLE stage (id INTEGER PRIMARY KEY IDENTITY, value TEXT)", Ct);
                await db.ExecuteAsync("INSERT INTO stage (value) VALUES ('one'), ('two')", Ct);

                await using (var persistResult = await db.ExecuteAsync("PERSIST TEMPORARY TABLE stage AS persisted_stage", Ct))
                {
                    var rows = await persistResult.ToListAsync(Ct);
                    Assert.Single(rows);
                    Assert.Equal("stage", rows[0][0].AsText);
                    Assert.Equal("persisted_stage", rows[0][1].AsText);
                    Assert.Equal(2, rows[0][2].AsInteger);
                }

                Assert.Equal(2, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM persisted_stage"));
                Assert.Equal(1, await ExecuteCountAsync(db, "SELECT COUNT(*) FROM sys.temp_tables WHERE table_name = 'stage'"));
            }

            await using (Database reopened = await Database.OpenAsync(path, Ct))
            {
                Assert.Equal(2, await ExecuteCountAsync(reopened, "SELECT COUNT(*) FROM persisted_stage"));
                Assert.Equal(0, await ExecuteCountAsync(reopened, "SELECT COUNT(*) FROM sys.temp_tables"));
            }
        }
        finally
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task TempTables_AreNotSavedWithoutExplicitPersist()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_temp_save_{Guid.NewGuid():N}.db");
        try
        {
            await using (Database db = await Database.OpenInMemoryAsync(Ct))
            {
                await db.ExecuteAsync("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY)", Ct);
                await db.ExecuteAsync("INSERT INTO scratch VALUES (1)", Ct);
                await db.SaveToFileAsync(path, Ct);
            }

            await using Database reopened = await Database.OpenAsync(path, Ct);
            Assert.Equal(0, await ExecuteCountAsync(reopened, "SELECT COUNT(*) FROM sys.temp_tables"));
            var ex = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await reopened.ExecuteAsync("SELECT COUNT(*) FROM scratch", Ct));
            Assert.Equal(ErrorCode.TableNotFound, ex.Code);
        }
        finally
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task TempTable_RejectsUnsupportedSchemaFeatures()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);

        await db.ExecuteAsync("CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, value TEXT)", Ct);

        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("CREATE TEMP TABLE bad_fk (id INTEGER REFERENCES scratch(id))", Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("CREATE INDEX idx_scratch_value ON scratch(value)", Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("CREATE TRIGGER tr_scratch AFTER INSERT ON scratch BEGIN INSERT INTO scratch VALUES (2, 'x'); END", Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await db.ExecuteAsync("CREATE VALIDATION RULE scratch_rule ON scratch AS id > 0 MESSAGE 'bad'", Ct));
    }

    private static async Task<long> ExecuteCountAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql, Ct);
        var rows = await result.ToListAsync(Ct);
        Assert.Single(rows);
        return rows[0][0].AsInteger;
    }

    private static async Task<string> ExecuteTextScalarAsync(Database db, string sql)
    {
        await using var result = await db.ExecuteAsync(sql, Ct);
        var rows = await result.ToListAsync(Ct);
        Assert.Single(rows);
        return rows[0][0].AsText;
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temporary database files.
        }
    }
}
