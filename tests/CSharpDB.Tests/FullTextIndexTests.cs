using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;
using System.Reflection;

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

        Assert.Equal(6, indexes.Length);
        Assert.Contains(indexes, static index => index.IndexName == "fts_docs" && index.Kind == IndexKind.FullText);
        Assert.Equal(
            5,
            indexes.Count(static index => index.Kind == IndexKind.FullTextInternal && index.OwnerIndexName == "fts_docs"));

        await _db.ExecuteAsync("DROP INDEX fts_docs", Ct);

        Assert.DoesNotContain(
            _db.GetIndexes(),
            static index => string.Equals(index.TableName, "docs", StringComparison.OrdinalIgnoreCase));

        var ex = await Assert.ThrowsAsync<CSharpDbException>(() => _db.SearchAsync("fts_docs", "hello", Ct).AsTask());
        Assert.Equal(ErrorCode.TableNotFound, ex.Code);
    }

    [Fact]
    public async Task EnsureFullTextIndex_DirectDropOfOwnedStore_ThrowsAndLeavesLogicalIndexUsable()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (1, 'hello world')", Ct);

        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);

        var ex = await Assert.ThrowsAsync<CSharpDbException>(() =>
            _db.ExecuteAsync($"DROP INDEX {FullTextIndexNaming.GetPostingsIndexName("fts_docs")}", Ct).AsTask());

        Assert.Equal(ErrorCode.SyntaxError, ex.Code);
        Assert.Contains("cannot be dropped directly", ex.Message, StringComparison.OrdinalIgnoreCase);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "hello", Ct), 1);
    }

    [Fact]
    public async Task EnsureFullTextIndex_ExistingNameMustMatchDefinitionToBeIdempotent()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)", Ct);
        await _db.ExecuteAsync("CREATE TABLE posts (id INTEGER PRIMARY KEY, body TEXT)", Ct);

        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);
        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);

        Assert.Equal(6, _db.GetIndexes().Count(static index => index.IndexName.StartsWith("fts_docs", StringComparison.Ordinal)));

        var differentColumns = await Assert.ThrowsAsync<CSharpDbException>(() =>
            _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["title"], ct: Ct).AsTask());
        Assert.Equal(ErrorCode.TableAlreadyExists, differentColumns.Code);

        var differentTable = await Assert.ThrowsAsync<CSharpDbException>(() =>
            _db.EnsureFullTextIndexAsync("fts_docs", "posts", ["body"], ct: Ct).AsTask());
        Assert.Equal(ErrorCode.TableAlreadyExists, differentTable.Code);

        var differentOptions = await Assert.ThrowsAsync<CSharpDbException>(() =>
            _db.EnsureFullTextIndexAsync(
                "fts_docs",
                "docs",
                ["body"],
                new FullTextIndexOptions { LowercaseInvariant = false },
                Ct).AsTask());
        Assert.Equal(ErrorCode.TableAlreadyExists, differentOptions.Code);
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

    [Fact]
    public async Task FullTextIndex_HotTermPostingsGrowPastOneLeafCell()
    {
        // Regression: a term shared by thousands of rows must span multiple
        // bounded posting chunks instead of rewriting one growing postings
        // blob on every insert. Positions storage makes postings grow fastest.
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.EnsureFullTextIndexAsync(
            "fts_docs",
            "docs",
            ["body"],
            new FullTextIndexOptions { StorePositions = true },
            Ct);

        const int DocumentCount = 3000;
        for (int i = 0; i < DocumentCount; i++)
        {
            await _db.ExecuteAsync(
                FormattableString.Invariant($"INSERT INTO docs VALUES ({i}, 'needle_{i:D4} line')"),
                Ct);
        }

        IReadOnlyList<FullTextSearchHit> hits = await _db.SearchAsync("fts_docs", "line", Ct);
        Assert.Equal(DocumentCount, hits.Count);

        AssertHitRowIds(await _db.SearchAsync("fts_docs", "needle_1500", Ct), 1500);

        // Deleting a document must survive the overflow round-trip too.
        await _db.ExecuteAsync("DELETE FROM docs WHERE id = 1500", Ct);
        Assert.Empty(await _db.SearchAsync("fts_docs", "needle_1500", Ct));
        hits = await _db.SearchAsync("fts_docs", "line", Ct);
        Assert.Equal(DocumentCount - 1, hits.Count);

        // Reopen: the spilled postings must be durable and readable.
        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, Ct);

        hits = await _db.SearchAsync("fts_docs", "line", Ct);
        Assert.Equal(DocumentCount - 1, hits.Count);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "needle_2999", Ct), 2999);
    }

    [Fact]
    public async Task FullTextIndex_ChunkedPostingsHandleOutOfOrderRowIds()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);

        const int DocumentCount = 1100;
        for (int i = 0; i < DocumentCount; i++)
        {
            int id = 1000 + i;
            await _db.ExecuteAsync(
                FormattableString.Invariant($"INSERT INTO docs VALUES ({id}, 'line high_{id}')"),
                Ct);
        }

        await _db.ExecuteAsync("INSERT INTO docs VALUES (1, 'line low')", Ct);

        IReadOnlyList<FullTextSearchHit> hits = await _db.SearchAsync("fts_docs", "line", Ct);
        Assert.Equal(DocumentCount + 1, hits.Count);
        Assert.Equal(1, hits[0].RowId);
        Assert.Equal(1000, hits[1].RowId);

        await _db.ExecuteAsync("DELETE FROM docs WHERE id = 1", Ct);
        hits = await _db.SearchAsync("fts_docs", "line", Ct);
        Assert.Equal(DocumentCount, hits.Count);
        Assert.Equal(1000, hits[0].RowId);
    }

    [Fact]
    public async Task FullTextIndex_ChunkedMutationRollbackRestoresPostings()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await _db.EnsureFullTextIndexAsync("fts_docs", "docs", ["body"], ct: Ct);

        const int DocumentCount = 1100;
        for (int i = 1; i <= DocumentCount; i++)
        {
            await _db.ExecuteAsync(
                FormattableString.Invariant($"INSERT INTO docs VALUES ({i}, 'line token_{i}')"),
                Ct);
        }

        Assert.Equal(DocumentCount, (await _db.SearchAsync("fts_docs", "line", Ct)).Count);

        await _db.BeginTransactionAsync(Ct);
        try
        {
            await _db.ExecuteAsync("DELETE FROM docs WHERE id = 10", Ct);
            await _db.ExecuteAsync("UPDATE docs SET body = 'changed unique_20' WHERE id = 20", Ct);
            await _db.ExecuteAsync("INSERT INTO docs VALUES (2000, 'line inserted')", Ct);

            Assert.Equal(DocumentCount - 1, (await _db.SearchAsync("fts_docs", "line", Ct)).Count);
            Assert.Empty(await _db.SearchAsync("fts_docs", "token_20", Ct));
            AssertHitRowIds(await _db.SearchAsync("fts_docs", "unique_20", Ct), 20);

            await _db.RollbackAsync(Ct);
        }
        catch
        {
            await _db.RollbackAsync(CancellationToken.None);
            throw;
        }

        Assert.Equal(DocumentCount, (await _db.SearchAsync("fts_docs", "line", Ct)).Count);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "token_10", Ct), 10);
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "token_20", Ct), 20);
        Assert.Empty(await _db.SearchAsync("fts_docs", "unique_20", Ct));
        Assert.Empty(await _db.SearchAsync("fts_docs", "inserted", Ct));
    }

    [Fact]
    public async Task FullTextIndex_ExistingLegacyOwnedStoresAreUpgradedOnOpenAndMigratedOnWrite()
    {
        await _db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", Ct);
        await CreateLegacyFullTextIndexAsync(_db, "fts_docs", "docs", ["body"]);

        Assert.Equal(
            5,
            _db.GetIndexes().Count(static index => index.IndexName.StartsWith("fts_docs", StringComparison.Ordinal)));
        Assert.DoesNotContain(
            _db.GetIndexes(),
            static index => index.IndexName == FullTextIndexNaming.GetPostingChunksIndexName("fts_docs"));

        await _db.ExecuteAsync("INSERT INTO docs VALUES (1, 'line legacy_one')", Ct);
        await _db.ExecuteAsync("INSERT INTO docs VALUES (2, 'line legacy_two')", Ct);

        AssertHitRowIds(await _db.SearchAsync("fts_docs", "line", Ct), 1, 2);
        AssertLegacyPostingsPayload("fts_docs", "line");

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, Ct);

        Assert.Contains(
            _db.GetIndexes(),
            static index => index.IndexName == FullTextIndexNaming.GetPostingChunksIndexName("fts_docs"));
        AssertHitRowIds(await _db.SearchAsync("fts_docs", "line", Ct), 1, 2);
        AssertLegacyPostingsPayload("fts_docs", "line");

        await _db.ExecuteAsync("INSERT INTO docs VALUES (3, 'line legacy_three')", Ct);

        AssertHitRowIds(await _db.SearchAsync("fts_docs", "line", Ct), 1, 2, 3);
        AssertChunkedPostingsPayload("fts_docs", "line");
    }

    private static void AssertHitRowIds(IReadOnlyList<FullTextSearchHit> hits, params long[] expectedRowIds)
    {
        Assert.Equal(expectedRowIds, hits.Select(static hit => hit.RowId).ToArray());
    }

    private static async ValueTask CreateLegacyFullTextIndexAsync(
        Database db,
        string indexName,
        string tableName,
        IReadOnlyList<string> columns)
    {
        SchemaCatalog catalog = GetCatalog(db);
        var logicalIndex = FullTextIndexCatalog.CreateLogicalSchema(
            indexName,
            tableName,
            columns,
            new FullTextIndexOptions());

        await db.BeginTransactionAsync(Ct);
        try
        {
            await catalog.CreateIndexAsync(logicalIndex, Ct);
            foreach (IndexSchema internalIndex in FullTextIndexCatalog.CreateInternalSchemas(logicalIndex))
            {
                if (internalIndex.IndexName == FullTextIndexNaming.GetPostingChunksIndexName(indexName))
                    continue;

                await catalog.CreateIndexAsync(internalIndex, Ct);
            }

            await db.CommitAsync(Ct);
        }
        catch
        {
            await db.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private void AssertLegacyPostingsPayload(string indexName, string term)
    {
        byte[] payload = ReadPostingsPayload(indexName, term);
        Assert.True(FullTextPostingsPayloadCodec.IsEncoded(payload));
        Assert.False(FullTextPostingChunkManifestCodec.IsEncoded(payload));
    }

    private void AssertChunkedPostingsPayload(string indexName, string term)
    {
        byte[] payload = ReadPostingsPayload(indexName, term);
        Assert.True(FullTextPostingChunkManifestCodec.IsEncoded(payload));
        Assert.False(FullTextPostingsPayloadCodec.IsEncoded(payload));
    }

    private byte[] ReadPostingsPayload(string indexName, string term)
    {
        SchemaCatalog catalog = GetCatalog(_db);
        var store = catalog.GetIndexStore(FullTextIndexNaming.GetPostingsIndexName(indexName));
        byte[]? payload = store.FindAsync(FullTextTermKeyCodec.ComputeKey(term), Ct).AsTask().GetAwaiter().GetResult();
        Assert.NotNull(payload);
        return payload;
    }

    private static SchemaCatalog GetCatalog(Database db)
    {
        var field = typeof(Database).GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return Assert.IsType<SchemaCatalog>(field.GetValue(db));
    }
}
