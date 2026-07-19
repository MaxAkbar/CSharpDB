using System.Buffers.Binary;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class RowVersionIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_rowversion_{Guid.NewGuid():N}.db");
    private Database _db = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        DeleteIfExists(_dbPath);
        DeleteIfExists(_dbPath + ".wal");
    }

    [Fact]
    public async Task RowVersion_CreateTablePersistsMetadataAcrossReopen()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);

        ColumnDefinition version = Assert.Single(
            _db.GetTableSchema("items")!.Columns,
            column => column.IsRowVersion);
        Assert.Equal(DbType.Blob, version.Type);
        Assert.False(version.Nullable);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, Ct);

        version = Assert.Single(
            _db.GetTableSchema("items")!.Columns,
            column => column.IsRowVersion);
        Assert.Equal("version", version.Name);
        Assert.Equal(DbType.Blob, version.Type);
        Assert.False(version.Nullable);
    }

    [Fact]
    public async Task RowVersion_InsertRawUpdateAndNoOpUpdate_GenerateMonotonicTokens()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);

        await using (QueryResult insert = await _db.ExecuteAsync(
            "INSERT INTO items (id, name) VALUES (1, 'alpha')",
            Ct))
        {
            AssertGeneratedRowVersion(insert, 1);
        }
        await AssertStoredRowVersionAsync(1, 1);

        await using (QueryResult update = await _db.ExecuteAsync(
            "UPDATE items SET name = 'beta' WHERE id = 1",
            Ct))
        {
            AssertGeneratedRowVersion(update, 2);
        }
        await AssertStoredRowVersionAsync(1, 2);

        await using (QueryResult noOpUpdate = await _db.ExecuteAsync(
            "UPDATE items SET name = name WHERE id = 1",
            Ct))
        {
            AssertGeneratedRowVersion(noOpUpdate, 3);
        }
        await AssertStoredRowVersionAsync(1, 3);
    }

    [Fact]
    public async Task RowVersion_ExplicitInsertAndUpdateAssignmentsAreRejected()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);
        await _db.ExecuteAsync("INSERT INTO items (id, name) VALUES (1, 'alpha')", Ct);

        CSharpDbException insertException = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "INSERT INTO items (id, name, version) VALUES (2, 'beta', X'0000000000000001')",
                Ct));
        Assert.Equal(ErrorCode.ConstraintViolation, insertException.Code);

        CSharpDbException updateException = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "UPDATE items SET version = X'0000000000000002' WHERE id = 1",
                Ct));
        Assert.Equal(ErrorCode.ConstraintViolation, updateException.Code);

        await AssertStoredRowVersionAsync(1, 1);
    }

    [Fact]
    public async Task RowVersion_FailedUpdateAndRollbackPreserveToken()
    {
        await _db.ExecuteAsync(
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                version BLOB ROWVERSION NOT NULL
            )
            """,
            Ct);
        await _db.ExecuteAsync("INSERT INTO items (id, quantity) VALUES (1, 1)", Ct);

        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "UPDATE items SET quantity = 0 WHERE id = 1",
                Ct));
        await AssertStoredRowVersionAsync(1, 1);

        await _db.BeginTransactionAsync(Ct);
        try
        {
            await _db.ExecuteAsync("UPDATE items SET quantity = 2 WHERE id = 1", Ct);
            await AssertStoredRowVersionAsync(1, 2);
            await _db.RollbackAsync(Ct);
        }
        catch
        {
            await _db.RollbackAsync(CancellationToken.None);
            throw;
        }

        await AssertStoredRowVersionAsync(1, 1);
    }

    [Fact]
    public async Task RowVersion_TriggerIssuedUpdateAdvancesToken()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER normalize_item AFTER INSERT ON items
            BEGIN
                UPDATE items SET name = name WHERE id = NEW.id;
            END
            """,
            Ct);

        await using (QueryResult insert = await _db.ExecuteAsync(
            "INSERT INTO items (id, name) VALUES (1, 'alpha')",
            Ct))
        {
            AssertGeneratedRowVersion(insert, 2);
        }

        await AssertStoredRowVersionAsync(1, 2);
    }

    [Fact]
    public async Task RowVersion_AfterUpdateTriggerReturnsFinalPersistedToken()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);
        await _db.ExecuteAsync("INSERT INTO items (id, name) VALUES (1, 'alpha')", Ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER normalize_item AFTER UPDATE ON items
            BEGIN
                UPDATE items
                SET name = 'normalized'
                WHERE id = NEW.id AND name <> 'normalized';
            END
            """,
            Ct);

        await using (QueryResult update = await _db.ExecuteAsync(
            "UPDATE items SET name = 'beta' WHERE id = 1",
            Ct))
        {
            AssertGeneratedRowVersion(update, 3);
        }

        await AssertStoredRowVersionAsync(1, 3);
        await using QueryResult result =
            await _db.ExecuteAsync("SELECT name FROM items WHERE id = 1", Ct);
        Assert.Equal("normalized", Assert.Single(await result.ToListAsync(Ct))[0].AsText);
    }

    [Fact]
    public async Task RowVersion_BeforeUpdateTriggerAndOuterUpdateAdvanceIndependently()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);
        await _db.ExecuteAsync("INSERT INTO items (id, name) VALUES (1, 'alpha')", Ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER preprocess_item BEFORE UPDATE ON items
            BEGIN
                UPDATE items
                SET name = 'preprocessed'
                WHERE id = NEW.id AND NEW.name <> 'preprocessed';
            END
            """,
            Ct);

        await using (QueryResult update = await _db.ExecuteAsync(
            "UPDATE items SET name = 'final' WHERE id = 1",
            Ct))
        {
            AssertGeneratedRowVersion(update, 3);
        }

        await AssertStoredRowVersionAsync(1, 3);
        await using QueryResult result =
            await _db.ExecuteAsync("SELECT name FROM items WHERE id = 1", Ct);
        Assert.Equal("final", Assert.Single(await result.ToListAsync(Ct))[0].AsText);
    }

    [Fact]
    public async Task RowVersion_VacuumPreservesMetadataAndValueAcrossReopen()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);
        await _db.ExecuteAsync("INSERT INTO items (id, name) VALUES (1, 'alpha')", Ct);
        await _db.ExecuteAsync("UPDATE items SET name = 'beta' WHERE id = 1", Ct);

        await _db.DisposeAsync();
        await DatabaseMaintenanceCoordinator.VacuumAsync(_dbPath, Ct);
        _db = await Database.OpenAsync(_dbPath, Ct);

        ColumnDefinition version = Assert.Single(
            _db.GetTableSchema("items")!.Columns,
            column => column.IsRowVersion);
        Assert.Equal(DbType.Blob, version.Type);
        Assert.False(version.Nullable);
        await AssertStoredRowVersionAsync(1, 2);
    }

    [Fact]
    public async Task RowVersion_UnsupportedSchemaShapesAreRejected()
    {
        string[] invalidCreateStatements =
        [
            "CREATE TABLE wrong_type (id INTEGER PRIMARY KEY, version TEXT ROWVERSION NOT NULL)",
            "CREATE TABLE nullable_version (id INTEGER PRIMARY KEY, version BLOB ROWVERSION)",
            "CREATE TABLE defaulted_version (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL DEFAULT X'0000000000000001')",
            "CREATE TABLE primary_version (version BLOB ROWVERSION NOT NULL PRIMARY KEY)",
            "CREATE TABLE duplicate_version (id INTEGER PRIMARY KEY, first_version BLOB ROWVERSION NOT NULL, second_version BLOB ROWVERSION NOT NULL)",
            "CREATE TABLE unique_version (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL, UNIQUE (version))",
            "CREATE TEMP TABLE temporary_version (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL)",
        ];

        foreach (string sql in invalidCreateStatements)
            await Assert.ThrowsAsync<CSharpDbException>(async () => await _db.ExecuteAsync(sql, Ct));

        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", Ct);
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "CREATE TABLE foreign_version (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL REFERENCES parents(id))",
                Ct));

        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);

        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "CREATE INDEX idx_items_version ON items(version)",
                Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE items ADD COLUMN other_version BLOB ROWVERSION NOT NULL",
                Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE items ALTER COLUMN version DROP NOT NULL",
                Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE items ALTER COLUMN version SET DEFAULT X'0000000000000001'",
                Ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _db.ExecuteAsync(
                "ALTER TABLE items ALTER COLUMN version TYPE TEXT",
                Ct));
    }

    [Fact]
    public async Task RowVersion_ExternalTableRegistrationIsRejected()
    {
        string archivePath =
            Path.Combine(Path.GetTempPath(), $"csharpdb_rowversion_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "archived_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new ColumnDefinition
                {
                    Name = "version",
                    Type = DbType.Blob,
                    Nullable = false,
                    IsRowVersion = true,
                },
            ],
        };

        try
        {
            await TableArchiveWriter.WriteAsync(
                archivePath,
                schema,
                TableArchiveWriter.ToAsyncRows(
                    [
                        [
                            DbValue.FromInteger(1),
                            DbValue.FromBlob([0, 0, 0, 0, 0, 0, 0, 1]),
                        ],
                    ],
                    Ct),
                Ct);

            string escapedArchivePath = archivePath.Replace("'", "''", StringComparison.Ordinal);
            CSharpDbException exception = await Assert.ThrowsAsync<CSharpDbException>(
                async () => await _db.ExecuteAsync(
                    $"CREATE EXTERNAL TABLE archived_items FROM '{escapedArchivePath}'",
                    Ct));
            Assert.Equal(ErrorCode.SyntaxError, exception.Code);
            Assert.Contains("ROWVERSION", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteIfExists(archivePath);
        }
    }

    [Fact]
    public async Task RowVersion_PreparedInsertBatchOmitsGeneratedColumn()
    {
        await _db.ExecuteAsync(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, version BLOB ROWVERSION NOT NULL)",
            Ct);

        InsertBatch batch = _db.PrepareInsertBatch("items", initialCapacity: 2);
        Assert.Equal(2, batch.ColumnCount);
        batch.AddRow(DbValue.FromInteger(1), DbValue.FromText("alpha"));
        batch.AddRow(DbValue.FromInteger(2), DbValue.FromText("beta"));

        Assert.Equal(2, await batch.ExecuteAsync(Ct));
        await AssertStoredRowVersionAsync(1, 1);
        await AssertStoredRowVersionAsync(2, 1);

        await _db.ExecuteAsync(
            "CREATE TABLE generated_only (version BLOB ROWVERSION NOT NULL)",
            Ct);
        InsertBatch generatedOnlyBatch = _db.PrepareInsertBatch("generated_only");
        Assert.Equal(0, generatedOnlyBatch.ColumnCount);
        generatedOnlyBatch.AddRow(Array.Empty<DbValue>());
        Assert.Equal(1, await generatedOnlyBatch.ExecuteAsync(Ct));

        await using QueryResult generatedOnlyResult =
            await _db.ExecuteAsync("SELECT version FROM generated_only", Ct);
        DbValue[] generatedOnlyRow =
            Assert.Single(await generatedOnlyResult.ToListAsync(Ct));
        Assert.Equal(1UL, ReadRowVersion(generatedOnlyRow[0].AsBlob));
    }

    private async Task AssertStoredRowVersionAsync(long id, ulong expected)
    {
        await using QueryResult result = await _db.ExecuteAsync(
            $"SELECT version FROM items WHERE id = {id}",
            Ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(Ct));
        Assert.Equal(expected, ReadRowVersion(row[0].AsBlob));
    }

    private static void AssertGeneratedRowVersion(QueryResult result, ulong expected)
    {
        Assert.True(result.TryGetGeneratedRowVersion(out byte[] generatedRowVersion));
        Assert.Equal(expected, ReadRowVersion(generatedRowVersion));
    }

    private static ulong ReadRowVersion(byte[] value)
    {
        Assert.Equal(sizeof(ulong), value.Length);
        return BinaryPrimitives.ReadUInt64BigEndian(value);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
