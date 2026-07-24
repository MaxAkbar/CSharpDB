using System.Reflection;
using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class CollectionJsonElementTests
{
    private sealed record TypedCollectionDocument(
        string Name,
        int Count,
        bool Active,
        string[] Tags,
        TypedCollectionNested Nested);

    private sealed record TypedCollectionNested(string City);

    [Fact]
    public async Task JsonElementCollection_RoundTripsDirectPayloadDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        var collection = await db.GetCollectionAsync<JsonElement>("json_docs", ct);

        JsonElement document;
        using (var json = JsonDocument.Parse("""{"name":"json","meta":{"count":2}}"""))
            document = json.RootElement.Clone();

        await collection.PutAsync("doc-1", document, ct);
        JsonElement loaded = await collection.GetAsync("doc-1", ct);

        Assert.Equal("json", loaded.GetProperty("name").GetString());
        Assert.Equal(2, loaded.GetProperty("meta").GetProperty("count").GetInt32());
    }

    [Fact]
    public async Task JsonElementCollection_ReadsBinaryTypedDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        var typedCollection = await db.GetCollectionAsync<TypedCollectionDocument>("typed_docs", ct);

        await typedCollection.PutAsync(
            "doc-1",
            new TypedCollectionDocument(
                "scanner",
                2,
                true,
                ["alpha", "beta"],
                new TypedCollectionNested("Seattle")),
            ct);

        var jsonCollection = await db.GetCollectionAsync<JsonElement>("typed_docs", ct);

        JsonElement loaded = await jsonCollection.GetAsync("doc-1", ct);
        Assert.Equal(JsonValueKind.Object, loaded.ValueKind);
        Assert.Equal("scanner", loaded.GetProperty("name").GetString());
        Assert.Equal(2, loaded.GetProperty("count").GetInt32());
        Assert.True(loaded.GetProperty("active").GetBoolean());
        Assert.Equal("beta", loaded.GetProperty("tags")[1].GetString());
        Assert.Equal("Seattle", loaded.GetProperty("nested").GetProperty("city").GetString());

        var scanned = new List<KeyValuePair<string, JsonElement>>();
        await foreach (var item in jsonCollection.ScanAsync(ct))
            scanned.Add(item);

        var row = Assert.Single(scanned);
        Assert.Equal("doc-1", row.Key);
        Assert.Equal("scanner", row.Value.GetProperty("name").GetString());
    }

    [Fact]
    public async Task CanonicalMigrationInsert_PreservesExactBytesAndRoundTrips()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        byte[] canonical =
            """{"é":"<\b\t\n\f\r\u0000","nested":{"z":-0,"a":[1e+02,1.2300]}}"""u8.ToArray();

        await db.BeginTransactionAsync(ct);
        await db.EnsureJsonDocumentCollectionAsync("migrated_docs", ct);
        await db.InsertCanonicalJsonDocumentAsync(
            "migrated_docs",
            "doc:é",
            canonical,
            ct);
        await db.CommitAsync(ct);

        var collection =
            await db.GetCollectionAsync<JsonElement>("migrated_docs", ct);
        JsonElement loaded = await collection.GetAsync("doc:é", ct);

        Assert.Equal("<\b\t\n\f\r\0", loaded.GetProperty("é").GetString());
        JsonElement nested = loaded.GetProperty("nested");
        Assert.Equal(["z", "a"], nested.EnumerateObject().Select(static property => property.Name));
        Assert.Equal("-0", nested.GetProperty("z").GetRawText());
        Assert.Equal("1e+02", nested.GetProperty("a")[0].GetRawText());
        Assert.Equal("1.2300", nested.GetProperty("a")[1].GetRawText());
        Assert.Equal(canonical, Encoding.UTF8.GetBytes(loaded.GetRawText()));

        await using (var result = await db.ExecuteAsync(
                         "SELECT _doc FROM _col_migrated_docs WHERE _key = 'doc:é'",
                         ct))
        {
            var rows = await result.ToListAsync(ct);
            Assert.Single(rows);
            Assert.Equal(Encoding.UTF8.GetString(canonical), rows[0][0].AsText);
        }

        byte[] payload = await GetBackingPayloadAsync(collection, "doc:é", ct);
        Assert.True(CollectionPayloadCodec.IsDirectPayload(payload));
        Assert.False(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal(canonical, CollectionPayloadCodec.GetJsonUtf8(payload).ToArray());
    }

    [Fact]
    public async Task CanonicalMigrationInsert_RefusesDuplicateWithoutUpdating()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        byte[] first = """{"value":1}"""u8.ToArray();

        await db.BeginTransactionAsync(ct);
        await db.EnsureJsonDocumentCollectionAsync("duplicate_docs", ct);
        await db.InsertCanonicalJsonDocumentAsync(
            "duplicate_docs",
            "same-key",
            first,
            ct);

        CSharpDbException duplicate =
            await Assert.ThrowsAsync<CSharpDbException>(
                async () => await db.InsertCanonicalJsonDocumentAsync(
                    "duplicate_docs",
                    "same-key",
                    """{"value":2}"""u8.ToArray(),
                    ct));

        Assert.Equal(ErrorCode.DuplicateKey, duplicate.Code);
        await db.CommitAsync(ct);

        var collection =
            await db.GetCollectionAsync<JsonElement>("duplicate_docs", ct);
        Assert.Equal(1, await collection.CountAsync(ct));
        byte[] payload =
            await GetBackingPayloadAsync(collection, "same-key", ct);
        Assert.Equal(first, CollectionPayloadCodec.GetJsonUtf8(payload).ToArray());
    }

    [Fact]
    public async Task CanonicalMigrationInsert_RollbackRemovesNewCollectionAndAllowsRecreation()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);

        await db.BeginTransactionAsync(ct);
        await db.EnsureJsonDocumentCollectionAsync("rollback_docs", ct);
        await db.InsertCanonicalJsonDocumentAsync(
            "rollback_docs",
            "rolled-back",
            """{"state":"temporary"}"""u8.ToArray(),
            ct);
        await db.RollbackAsync(ct);

        Assert.DoesNotContain("rollback_docs", db.GetCollectionNames());

        await db.BeginTransactionAsync(ct);
        await db.EnsureJsonDocumentCollectionAsync("rollback_docs", ct);
        await db.InsertCanonicalJsonDocumentAsync(
            "rollback_docs",
            "committed",
            """{"state":"durable"}"""u8.ToArray(),
            ct);
        await db.CommitAsync(ct);

        var collection =
            await db.GetCollectionAsync<JsonElement>("rollback_docs", ct);
        Assert.Equal(1, await collection.CountAsync(ct));
        Assert.Equal(
            "durable",
            (await collection.GetAsync("committed", ct))
                .GetProperty("state")
                .GetString());
    }

    [Fact]
    public async Task CanonicalMigrationMethods_RequireExplicitTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await db.EnsureJsonDocumentCollectionAsync(
                "transaction_docs",
                ct));
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await db.InsertCanonicalJsonDocumentAsync(
                "transaction_docs",
                "key",
                "{}"u8.ToArray(),
                ct));

        Assert.DoesNotContain("transaction_docs", db.GetCollectionNames());
    }

    [Fact]
    public async Task CanonicalMigrationInsert_RejectsDocumentAboveAbsoluteLimit()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        byte[] oversized = GC.AllocateUninitializedArray<byte>(
            OrderedCanonicalJsonValidator.MaximumDocumentBytes + 1);

        await db.BeginTransactionAsync(ct);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await db.InsertCanonicalJsonDocumentAsync(
                "oversized_docs",
                "key",
                oversized,
                ct));

        Assert.DoesNotContain("oversized_docs", db.GetCollectionNames());
        await db.RollbackAsync(ct);
    }

    [Theory]
    [InlineData("{}{}")]
    [InlineData("{ }")]
    [InlineData(" [1]")]
    [InlineData("{\"x\":1,}")]
    [InlineData("{\"a\":1,\"\\u0061\":2}")]
    [InlineData("{\"x\":\"\\u00e9\"}")]
    [InlineData("{\"x\":\"\\u000A\"}")]
    [InlineData("\"\\ud800\"")]
    public async Task CanonicalMigrationInsert_RejectsMalformedOrNonCanonicalJson(
        string document)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);

        await db.BeginTransactionAsync(ct);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await db.InsertCanonicalJsonDocumentAsync(
                "invalid_docs",
                "key",
                Encoding.UTF8.GetBytes(document),
                ct));

        Assert.DoesNotContain("invalid_docs", db.GetCollectionNames());
        await db.RollbackAsync(ct);
    }

    [Fact]
    public void CanonicalMigrationValidation_EnforcesAbsoluteNodeAndPropertyBounds()
    {
        string atNodeLimit = BuildArray(elementCount: 65_535);
        OrderedCanonicalJsonValidator.Validate(
            Encoding.UTF8.GetBytes(atNodeLimit),
            TestContext.Current.CancellationToken);
        Assert.Throws<InvalidDataException>(() =>
            OrderedCanonicalJsonValidator.Validate(
                Encoding.UTF8.GetBytes(BuildArray(elementCount: 65_536)),
                TestContext.Current.CancellationToken));

        string atPropertyLimit = BuildObject(propertyCount: 16_384);
        OrderedCanonicalJsonValidator.Validate(
            Encoding.UTF8.GetBytes(atPropertyLimit),
            TestContext.Current.CancellationToken);
        Assert.Throws<InvalidDataException>(() =>
            OrderedCanonicalJsonValidator.Validate(
                Encoding.UTF8.GetBytes(BuildObject(propertyCount: 16_385)),
                TestContext.Current.CancellationToken));
    }

    private static string BuildArray(int elementCount) =>
        "[" + string.Join(',', Enumerable.Repeat("0", elementCount)) + "]";

    private static string BuildObject(int propertyCount)
    {
        var builder = new StringBuilder(propertyCount * 12);
        builder.Append('{');
        for (int index = 0; index < propertyCount; index++)
        {
            if (index != 0)
                builder.Append(',');
            builder.Append('"')
                .Append('p')
                .Append(index)
                .Append("\":0");
        }
        return builder.Append('}').ToString();
    }

    private static async Task<byte[]> GetBackingPayloadAsync(
        Collection<JsonElement> collection,
        string key,
        CancellationToken ct)
    {
        FieldInfo treeField =
            typeof(Collection<JsonElement>).GetField(
                "_tree",
                BindingFlags.Instance | BindingFlags.NonPublic) ??
            throw new InvalidOperationException(
                "Collection tree field not found.");
        var tree = (BTree?)treeField.GetValue(collection) ??
            throw new InvalidOperationException(
                "Collection tree was not initialized.");
        ReadOnlyMemory<byte> payload =
            await tree.FindMemoryAsync(
                Collection<JsonElement>.HashDocumentKey(key),
                ct) ??
            throw new InvalidOperationException(
                "Collection payload not found.");
        return payload.ToArray();
    }
}
