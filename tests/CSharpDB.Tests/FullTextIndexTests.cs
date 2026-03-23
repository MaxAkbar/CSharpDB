using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class FullTextIndexTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public FullTextIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_fulltext_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, Ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task EnsureFullTextIndex_BackfillsAndMaintainsRowsAcrossWrites()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (1, 'Alpha', 'Quick brown fox jumps')", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (2, 'Beta', 'Runner''s high and steady pace')", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (3, 'Gamma', 'Brown bear near river')", Ct);

        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["title", "body"], ct: Ct);

        AssertHitRowIds(await _db.SearchAsync("fts_docs", "brown", Ct), 1, 3);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "brown fox", Ct), 1);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "RUNNER'S", Ct), 2);

        await _db.ExecuteAsync("INSERT INTO docs VALUES (4, 'Delta', 'Ocean breeze and harbor light')", Ct);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "harbor", Ct), 4);

        await _db.ExecuteAsync("UPDATE docs SET body = 'Ocean breeze and lighthouse' WHERE id = 4", Ct);
        Assert.Empty(await _db.SearchAsync("fts_docs", "harbor", Ct));
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "lighthouse", Ct), 4);

        await _db.ExecuteAsync("DELETE FROM docs WHERE id = 4", Ct);
        Assert.Empty(await _db.SearchAsync("fts_docs", "lighthouse", Ct));

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, Ct);

        AssertHitRowIds(await _db.SearchAsync("fts_docs", "brown", Ct), 1, 3);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "runner's", Ct), 2);
    }

    [Fact]
    public async Task EnsureFullTextIndex_CreatesOwnedStoresAndDropIndexCascades()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (1, 'hello world')", Ct);

        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);

        IndexSchema[] indexes = _db.GetIndexes()
            .Where(static index => string.Equals(index.TableName, "docs", StringComparison.OrdinalIgnoreCase))
            .OrderBy(static index => index.IndexName, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(5, indexes.Length);
        Assert.Contains(indexes, static index => index.IndexName == "fts_docs" && index.Kind == IndexKind.FullText);
        Assert.Equal(
            4,
            indexes.Count(static index => index.Kind == IndexKind.FullTextInternal && index.OwnerIndexName == "fts_docs"));

        await _db.ExecuteAsync("DROP INDEX fts_docs", Ct);

        Assert.DoesNotContain(
            _db.GetIndexes(),
            static index => string.Equals(index.TableName, "docs", StringComparison.OrdinalIgnoreCase));

        var ex = await Assert.ThrowsAsync<CSharpDbException>(() => _db.SearchAsync("fts_docs", "hello", Ct).AsTask());
        Assert.Equal(ErrorCode.TableNotFound, ex.Code);
    }

    [Fact]
    public async Task EnsureFullTextIndex_InsideExplicitTransaction_Throws()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.BeginTransactionAsync(Ct);

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct).AsTask());
        }
        finally
        {
            await _db.RollbackAsync(Ct);
        }
    }

    private static void AssertHitRowIds(IReadOnlyList<FullTextSearchHit> hits, params long[] expectedRowIds)
    {
        Assert.Equal(expectedRowIds, hits.Select(static hit => hit.RowId).ToArray());
    }
}
