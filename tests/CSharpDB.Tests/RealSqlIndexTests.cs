using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class RealSqlIndexTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_real_index_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync() =>
        _database = await Database.OpenAsync(
            _databasePath,
            TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
        if (File.Exists(_databasePath + ".wal"))
            File.Delete(_databasePath + ".wal");
    }

    [Fact]
    public async Task RealEqualityIndex_BackfillsMaintainsNumericValuesAndSurvivesReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE real_index_items (" +
            "id INTEGER PRIMARY KEY, score REAL, label TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO real_index_items VALUES " +
            "(1, 1, 'integer-tag'), " +
            "(2, 1.0, 'real-tag'), " +
            "(3, -0.0, 'zero'), " +
            "(4, NULL, 'null')",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_real_index_items_score " +
            "ON real_index_items (score)",
            ct);

        await AssertIdsAsync("score = 1.0", [1L, 2L], ct);
        await AssertIdsAsync("score = 1", [1L, 2L], ct);
        await AssertIdsAsync("score = 0.0", [3L], ct);

        await _database.ExecuteAsync(
            "INSERT INTO real_index_items VALUES (5, 2, 'inserted')",
            ct);
        await AssertIdsAsync("score = 2.0", [5L], ct);

        await _database.ExecuteAsync(
            "UPDATE real_index_items SET score = 2.5 WHERE id = 5",
            ct);
        await AssertIdsAsync("score = 2", [], ct);
        await AssertIdsAsync("score = 2.5", [5L], ct);

        await _database.ExecuteAsync(
            "DELETE FROM real_index_items WHERE id = 5",
            ct);
        await AssertIdsAsync("score = 2.5", [], ct);

        CSharpDbException precisionFailure =
            await Assert.ThrowsAsync<CSharpDbException>(
                async () => await _database.ExecuteAsync(
                    "INSERT INTO real_index_items VALUES " +
                    "(6, 9007199254740993, 'inexact')",
                    ct));
        Assert.Equal(ErrorCode.TypeMismatch, precisionFailure.Code);
        await AssertIdsAsync("id = 6", [], ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        await AssertIdsAsync("score = 1.0", [1L, 2L], ct);
        await AssertIdsAsync("score = -0.0", [3L], ct);
    }

    [Fact]
    public async Task UniqueRealIndex_TreatsIntegerRealAndSignedZeroAsEqual()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE unique_real_index_items (" +
            "id INTEGER PRIMARY KEY, score REAL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO unique_real_index_items VALUES (1, 1)",
            ct);
        await _database.ExecuteAsync(
            "CREATE UNIQUE INDEX ux_unique_real_index_items_score " +
            "ON unique_real_index_items (score)",
            ct);

        CSharpDbException numericCollision =
            await Assert.ThrowsAsync<CSharpDbException>(
                async () => await _database.ExecuteAsync(
                    "INSERT INTO unique_real_index_items VALUES (2, 1.0)",
                    ct));
        Assert.Equal(ErrorCode.ConstraintViolation, numericCollision.Code);
        await AssertIdsAsync("score = 1.0", [1L], ct, "unique_real_index_items");

        await _database.ExecuteAsync(
            "INSERT INTO unique_real_index_items VALUES (3, -0.0)",
            ct);
        CSharpDbException zeroCollision =
            await Assert.ThrowsAsync<CSharpDbException>(
                async () => await _database.ExecuteAsync(
                    "INSERT INTO unique_real_index_items VALUES (4, 0.0)",
                    ct));
        Assert.Equal(ErrorCode.ConstraintViolation, zeroCollision.Code);
        await AssertIdsAsync("score = 0.0", [3L], ct, "unique_real_index_items");
    }

    [Fact]
    public async Task RealIndex_BackfillFailureLeavesNoCatalogEntryAndRowsUsable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE failed_real_index (" +
            "id INTEGER PRIMARY KEY, score REAL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO failed_real_index VALUES " +
            "(1, 9007199254740993)",
            ct);

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "CREATE INDEX ix_failed_real_index_score " +
                "ON failed_real_index (score)",
                ct));

        Assert.Equal(ErrorCode.TypeMismatch, failure.Code);
        Assert.DoesNotContain(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_failed_real_index_score",
                StringComparison.OrdinalIgnoreCase));
        await AssertIdsAsync(
            "score = 9007199254740993",
            [1L],
            ct,
            "failed_real_index");

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        Assert.DoesNotContain(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_failed_real_index_score",
                StringComparison.OrdinalIgnoreCase));
        await AssertIdsAsync(
            "score = 9007199254740993",
            [1L],
            ct,
            "failed_real_index");
    }

    private async Task AssertIdsAsync(
        string predicate,
        long[] expectedIds,
        CancellationToken ct,
        string tableName = "real_index_items")
    {
        await using QueryResult result = await _database.ExecuteAsync(
            $"SELECT id FROM {tableName} WHERE {predicate} ORDER BY id",
            ct);
        List<DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Equal(
            expectedIds,
            rows.Select(row => row[0].AsInteger).ToArray());
    }
}
