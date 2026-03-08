using System.Text.Json;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class CollectionIndexTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public CollectionIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_collection_index_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    private record User(string Name, int Age, string Email);

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForIntegerField()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 25, "bob@example.com"), ct);
        await users.PutAsync("u:3", new User("Charlie", 30, "charlie@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task EnsureIndex_BackfillsExistingDocuments_ForStringField()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alpha@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "beta@example.com"), ct);
        await users.PutAsync("u:3", new User("Cara", 32, "alpha@example.com"), ct);

        await users.EnsureIndexAsync(x => x.Email, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Email, "alpha@example.com", ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:3"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task FindByIndex_FallsBackToScan_WhenIndexDoesNotExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 31, "bob@example.com"), ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 31, ct), ct);

        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_UpdatesCollectionIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await users.PutAsync("u:1", new User("Alice", 31, "alice@example.com"), ct);

        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct));

        var updated = await CollectAsync(users.FindByIndexAsync(x => x.Age, 31, ct), ct);
        Assert.Single(updated);
        Assert.Equal("u:1", updated[0].Key);
    }

    [Fact]
    public async Task Delete_RemovesDocumentFromCollectionIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        Assert.True(await users.DeleteAsync("u:1", ct));
        Assert.Empty(await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct));
    }

    [Fact]
    public async Task CollectionIndexes_SurviveReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var matches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal(["u:1", "u:2"], matches.Select(x => x.Key).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Put_UpdateExistingDocument_PersistsIndexedWritesAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);
        await users.PutAsync("u:1", new User("Alice", 31, "alice@example.com"), ct);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        Assert.Empty(await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct));

        var updated = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 31, ct), ct);
        Assert.Single(updated);
        Assert.Equal("u:1", updated[0].Key);
    }

    [Fact]
    public async Task Delete_PersistsIndexedWritesAcrossReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);
        Assert.True(await users.DeleteAsync("u:1", ct));

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var matches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Single(matches);
        Assert.Equal("u:2", matches[0].Key);
    }

    [Fact]
    public async Task ExplicitTransaction_Rollback_RevertsCollectionIndexWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);

        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        await _db.BeginTransactionAsync(ct);
        await users.PutAsync("u:1", new User("Alice", 21, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await _db.RollbackAsync(ct);

        var age20 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 20, ct), ct);
        var age21 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 21, ct), ct);
        var age30 = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);

        Assert.Single(age20);
        Assert.Equal("u:1", age20[0].Key);
        Assert.Empty(age21);
        Assert.Empty(age30);
    }

    [Fact]
    public async Task EnsureIndex_CannotRunInsideExplicitTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        var users = await _db.GetCollectionAsync<User>("users", ct);
        await users.PutAsync("u:1", new User("Alice", 20, "alice@example.com"), ct);

        await _db.BeginTransactionAsync(ct);
        try
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await users.EnsureIndexAsync(x => x.Age, ct));
            Assert.Contains("explicit transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await _db.RollbackAsync(ct);
        }
    }

    [Fact]
    public async Task CollectionIndexes_WorkWithCustomRecordSerializer()
    {
        var ct = TestContext.Current.CancellationToken;
        var options = new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                SerializerProvider = new PrefixCollisionSerializerProvider(),
            },
        };

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, options, ct);

        var users = await _db.GetCollectionAsync<User>("users", ct);
        await users.PutAsync("u:1", new User("Alice", 30, "alice@example.com"), ct);
        await users.PutAsync("u:2", new User("Bob", 30, "bob@example.com"), ct);
        await users.EnsureIndexAsync(x => x.Age, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(2, matches.Count);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, options, ct);

        var reopened = await _db.GetCollectionAsync<User>("users", ct);
        var reopenedMatches = await CollectAsync(reopened.FindByIndexAsync(x => x.Age, 30, ct), ct);
        Assert.Equal(2, reopenedMatches.Count);
    }

    private static async Task<List<KeyValuePair<string, TDocument>>> CollectAsync<TDocument>(
        IAsyncEnumerable<KeyValuePair<string, TDocument>> source,
        CancellationToken ct)
    {
        var items = new List<KeyValuePair<string, TDocument>>();
        await foreach (var item in source.WithCancellation(ct))
            items.Add(item);
        return items;
    }

    private sealed class PrefixCollisionSerializerProvider : ISerializerProvider
    {
        public IRecordSerializer RecordSerializer { get; } = new PrefixCollisionRecordSerializer();

        public ISchemaSerializer SchemaSerializer { get; } = new DefaultSchemaSerializer();
    }

    private sealed class PrefixCollisionRecordSerializer : IRecordSerializer
    {
        private const byte Prefix = 0xC1;
        private readonly IRecordSerializer _inner = new DefaultRecordSerializer();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            byte[] encoded = _inner.Encode(values);
            byte[] prefixed = new byte[encoded.Length + 1];
            prefixed[0] = Prefix;
            encoded.CopyTo(prefixed.AsSpan(1));
            return prefixed;
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(StripPrefix(buffer));

        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
            => _inner.DecodeInto(StripPrefix(buffer), destination);

        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
            => _inner.DecodeUpTo(StripPrefix(buffer), maxColumnIndexInclusive);

        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.DecodeColumn(StripPrefix(buffer), columnIndex);

        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
            => _inner.TryColumnTextEquals(StripPrefix(buffer), columnIndex, expectedUtf8, out equals);

        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.IsColumnNull(StripPrefix(buffer), columnIndex);

        public bool TryDecodeNumericColumn(
            ReadOnlySpan<byte> buffer,
            int columnIndex,
            out long intValue,
            out double realValue,
            out bool isReal)
            => _inner.TryDecodeNumericColumn(StripPrefix(buffer), columnIndex, out intValue, out realValue, out isReal);

        private static ReadOnlySpan<byte> StripPrefix(ReadOnlySpan<byte> buffer)
        {
            if (buffer.IsEmpty || buffer[0] != Prefix)
                throw new InvalidOperationException("Expected prefixed row payload.");

            return buffer[1..];
        }
    }
}
