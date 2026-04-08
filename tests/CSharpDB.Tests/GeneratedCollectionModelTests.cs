using System.Text.Json;
using System.Text.Json.Serialization;
using System.Diagnostics.CodeAnalysis;
using CSharpDB.Engine;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class GeneratedCollectionModelTests
{
    [Fact]
    public async Task GeneratedCollectionModel_UsesGeneratedDescriptorIndexWithoutManualRegistration()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var users = await db.GetGeneratedCollectionAsync<GeneratedUser>("users", ct);

        await users.PutAsync("u1", new GeneratedUser("alpha@example.com", 30), ct);
        await users.PutAsync("u2", new GeneratedUser("beta@example.com", 41), ct);
        await users.EnsureIndexAsync(GeneratedUser.Collection.Email, ct);

        var matches = await CollectAsync(users.FindByIndexAsync(GeneratedUser.Collection.Email, "alpha@example.com", ct), ct);

        Assert.Single(matches);
        Assert.Equal("u1", matches[0].Key);
        Assert.Equal("alpha@example.com", matches[0].Value.Email);
    }

    [Fact]
    public async Task GeneratedCollectionModel_RangeQuery_WorksAfterReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb-generated-model-{Guid.NewGuid():N}.db");

        try
        {
            await using (var db = await Database.OpenAsync(path, ct))
            {
                var users = await db.GetGeneratedCollectionAsync<GeneratedUser>("users", ct);
                await users.PutAsync("u1", new GeneratedUser("alpha@example.com", 21), ct);
                await users.PutAsync("u2", new GeneratedUser("beta@example.com", 35), ct);
                await users.PutAsync("u3", new GeneratedUser("gamma@example.com", 27), ct);
                await users.EnsureIndexAsync(GeneratedUser.Collection.Age, ct);
            }

            await using var reopened = await Database.OpenAsync(path, ct);
            var reopenedUsers = await reopened.GetGeneratedCollectionAsync<GeneratedUser>("users", ct);
            var matches = await CollectAsync(reopenedUsers.FindByRangeAsync(GeneratedUser.Collection.Age, 20, 30, ct: ct), ct);

            Assert.Equal(2, matches.Count);
            Assert.Contains(matches, pair => pair.Key == "u1" && pair.Value.Age == 21);
            Assert.Contains(matches, pair => pair.Key == "u3" && pair.Value.Age == 27);
        }
        finally
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task GeneratedCollectionModel_DecodesLegacyBinaryPayloadsAfterOverrideRegistration()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb-generated-binary-compat-{Guid.NewGuid():N}.db");

        try
        {
            using (CollectionModelRegistry.Register<GeneratedUser>(new LegacyBinaryGeneratedUserCollectionModel()))
            {
                await using var db = await Database.OpenAsync(path, ct);
                var users = await db.GetGeneratedCollectionAsync<GeneratedUser>("users", ct);
                await users.PutAsync("u1", new GeneratedUser("alpha@example.com", 25), ct);
                await users.PutAsync("u2", new GeneratedUser("beta@example.com", 38), ct);
                await users.EnsureIndexAsync(GeneratedUser.Collection.Email, ct);
            }

            await using var reopened = await Database.OpenAsync(path, ct);
            var reopenedUsers = await reopened.GetGeneratedCollectionAsync<GeneratedUser>("users", ct);
            GeneratedUser? reopenedUser = await reopenedUsers.GetAsync("u1", ct);
            var matches = await CollectAsync(reopenedUsers.FindByIndexAsync(GeneratedUser.Collection.Email, "alpha@example.com", ct), ct);

            Assert.NotNull(reopenedUser);
            Assert.Equal(25, reopenedUser!.Age);
            Assert.Single(matches);
            Assert.Equal("u1", matches[0].Key);
            Assert.Equal("alpha@example.com", matches[0].Value.Email);
        }
        finally
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task GetGeneratedCollectionAsync_RequiresRegisteredModel()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            _ = await db.GetGeneratedCollectionAsync<UnannotatedGeneratedCollectionUser>("users", ct);
        });

        Assert.Contains(nameof(UnannotatedGeneratedCollectionUser), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GeneratedCollectionModel_HonorsJsonPropertyNameAttributes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var users = await db.GetGeneratedCollectionAsync<RenamedGeneratedUser>("renamed_users", ct);

        await users.PutAsync("u1", new RenamedGeneratedUser("alpha@example.com", 24), ct);
        await users.PutAsync("u2", new RenamedGeneratedUser("beta@example.com", 37), ct);
        await users.PutAsync("u3", new RenamedGeneratedUser("gamma@example.com", 28), ct);
        await users.EnsureIndexAsync(RenamedGeneratedUser.Collection.Email, ct);
        await users.EnsureIndexAsync(RenamedGeneratedUser.Collection.Age, ct);

        var emailMatches = await CollectAsync(
            users.FindByIndexAsync(RenamedGeneratedUser.Collection.Email, "alpha@example.com", ct),
            ct);
        var ageMatches = await CollectAsync(
            users.FindByRangeAsync(RenamedGeneratedUser.Collection.Age, 20, 30, ct: ct),
            ct);

        Assert.Single(emailMatches);
        Assert.Equal("u1", emailMatches[0].Key);
        Assert.Equal("alpha@example.com", emailMatches[0].Value.Email);

        Assert.Equal(2, ageMatches.Count);
        Assert.Contains(ageMatches, pair => pair.Key == "u1" && pair.Value.Age == 24);
        Assert.Contains(ageMatches, pair => pair.Key == "u3" && pair.Value.Age == 28);
    }

    [Fact]
    public async Task GeneratedCollectionModel_SupportsTopLevelScalarArrays()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var users = await db.GetGeneratedCollectionAsync<TaggedGeneratedUser>("tagged_users", ct);

        await users.PutAsync("u1", new TaggedGeneratedUser("Alice", ["alpha", "beta"]), ct);
        await users.PutAsync("u2", new TaggedGeneratedUser("Bob", ["gamma"]), ct);
        await users.PutAsync("u3", new TaggedGeneratedUser("Cara", ["beta", "delta"]), ct);
        await users.EnsureIndexAsync(TaggedGeneratedUser.Collection.Tags, ct);

        var matches = await CollectAsync(
            users.FindByIndexAsync(TaggedGeneratedUser.Collection.Tags, "beta", ct),
            ct);

        Assert.Equal(2, matches.Count);
        Assert.Contains(matches, pair => pair.Key == "u1");
        Assert.Contains(matches, pair => pair.Key == "u3");
    }

    [Fact]
    public async Task GeneratedCollectionModel_SupportsNestedObjectScalars()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var users = await db.GetGeneratedCollectionAsync<NestedGeneratedUser>("nested_users", ct);

        await users.PutAsync("u1", new NestedGeneratedUser("Alice", new NestedGeneratedAddress("Seattle", 98101)), ct);
        await users.PutAsync("u2", new NestedGeneratedUser("Bob", new NestedGeneratedAddress("Portland", 97201)), ct);
        await users.PutAsync("u3", new NestedGeneratedUser("Cara", new NestedGeneratedAddress("Seattle", 98109)), ct);
        await users.EnsureIndexAsync(NestedGeneratedUser.Collection.Address_City, ct);
        await users.EnsureIndexAsync(NestedGeneratedUser.Collection.Address_ZipCode, ct);

        var cityMatches = await CollectAsync(
            users.FindByIndexAsync(NestedGeneratedUser.Collection.Address_City, "Seattle", ct),
            ct);
        var zipMatches = await CollectAsync(
            users.FindByRangeAsync(NestedGeneratedUser.Collection.Address_ZipCode, 98100, 98105, ct: ct),
            ct);

        Assert.Equal(2, cityMatches.Count);
        Assert.Contains(cityMatches, pair => pair.Key == "u1");
        Assert.Contains(cityMatches, pair => pair.Key == "u3");

        Assert.Single(zipMatches);
        Assert.Equal("u1", zipMatches[0].Key);
        Assert.Equal(98101, zipMatches[0].Value.Address.ZipCode);
    }

    [Fact]
    public async Task GeneratedCollectionModel_SupportsNestedObjectArrays()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using var db = await Database.OpenInMemoryAsync(ct);
        var users = await db.GetGeneratedCollectionAsync<OrderedGeneratedUser>("ordered_users", ct);

        await users.PutAsync(
            "u1",
            new OrderedGeneratedUser("Alice", [new OrderedGeneratedOrder("sku-1", 2), new OrderedGeneratedOrder("sku-2", 1)]),
            ct);
        await users.PutAsync(
            "u2",
            new OrderedGeneratedUser("Bob", [new OrderedGeneratedOrder("sku-3", 1)]),
            ct);
        await users.PutAsync(
            "u3",
            new OrderedGeneratedUser("Cara", [new OrderedGeneratedOrder("sku-2", 5)]),
            ct);

        await users.EnsureIndexAsync(OrderedGeneratedUser.Collection.Orders_Sku, ct);

        var matches = await CollectAsync(
            users.FindByIndexAsync(OrderedGeneratedUser.Collection.Orders_Sku, "sku-2", ct),
            ct);

        Assert.Equal(2, matches.Count);
        Assert.Contains(matches, pair => pair.Key == "u1");
        Assert.Contains(matches, pair => pair.Key == "u3");
    }

    [Fact]
    public async Task GetGeneratedCollectionAsync_RejectsExistingReflectionOnlyIndexBindings()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using var registration = CollectionModelRegistry.Register<ManualLinkedUser>(new ManualLinkedUserCollectionModel());
        await using var db = await Database.OpenInMemoryAsync(ct);

        var reflectionCollection = await db.GetCollectionAsync<ManualLinkedUser>("manual_linked_users", ct);
        await reflectionCollection.PutAsync(
            "u1",
            new ManualLinkedUser("root", new ManualLinkedUser("child", null)),
            ct);
        await reflectionCollection.EnsureIndexAsync("Next.Name", ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            _ = await db.GetGeneratedCollectionAsync<ManualLinkedUser>("manual_linked_users", ct);
        });

        Assert.Contains("Next.Name", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(ManualLinkedUser), ex.Message, StringComparison.Ordinal);
    }

    private static async Task<List<KeyValuePair<string, TDocument>>> CollectAsync<TDocument>(
        IAsyncEnumerable<KeyValuePair<string, TDocument>> source,
        CancellationToken ct = default)
    {
        var items = new List<KeyValuePair<string, TDocument>>();
        await foreach (var item in source.WithCancellation(ct))
            items.Add(item);

        return items;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class LegacyBinaryGeneratedUserCollectionModel : ICollectionModel<GeneratedUser>
    {
        public ICollectionDocumentCodec<GeneratedUser> CreateCodec(IRecordSerializer recordSerializer)
            => new LegacyBinaryGeneratedUserCollectionCodec(recordSerializer);

        public bool TryGetField(string fieldPath, [NotNullWhen(true)] out CollectionField<GeneratedUser>? field)
            => GeneratedUser.Collection.TryGetField(fieldPath, out field);
    }

    private sealed class LegacyBinaryGeneratedUserCollectionCodec : ICollectionDocumentCodec<GeneratedUser>
    {
        private readonly IRecordSerializer _recordSerializer;
        private readonly bool _usesDirectPayloadFormat;

        public LegacyBinaryGeneratedUserCollectionCodec(IRecordSerializer recordSerializer)
        {
            _recordSerializer = recordSerializer;
            _usesDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
        }

        public byte[] Encode(string key, GeneratedUser document)
        {
            if (_usesDirectPayloadFormat)
            {
                byte[] binaryDocument = CollectionBinaryDocumentCodec.Encode(document);
                return CollectionPayloadCodec.EncodeBinary(key, binaryDocument);
            }

            string json = JsonSerializer.Serialize(document, GeneratedUserGeneratedJsonContext.Default.GeneratedUser);
            return _recordSerializer.Encode(
            [
                CSharpDB.Primitives.DbValue.FromText(key),
                CSharpDB.Primitives.DbValue.FromText(json),
            ]);
        }

        public (string Key, GeneratedUser Document) Decode(ReadOnlySpan<byte> payload)
            => (DecodeKey(payload), DecodeDocument(payload));

        public GeneratedUser DecodeDocument(ReadOnlySpan<byte> payload)
        {
            if (_usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
            {
                if (!CollectionPayloadCodec.IsBinaryPayload(payload))
                {
                    return JsonSerializer.Deserialize(
                               CollectionPayloadCodec.GetJsonUtf8(payload),
                               GeneratedUserGeneratedJsonContext.Default.GeneratedUser)
                           ?? throw new InvalidOperationException("Generated collection payload deserialized to null.");
                }

                string json = CollectionPayloadCodec.DecodeJson(payload);
                return JsonSerializer.Deserialize(json, GeneratedUserGeneratedJsonContext.Default.GeneratedUser)
                       ?? throw new InvalidOperationException("Generated collection payload deserialized to null.");
            }

            var values = _recordSerializer.Decode(payload);
            return JsonSerializer.Deserialize(values[1].AsText, GeneratedUserGeneratedJsonContext.Default.GeneratedUser)
                   ?? throw new InvalidOperationException("Generated collection payload deserialized to null.");
        }

        public string DecodeKey(ReadOnlySpan<byte> payload)
            => _usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload)
                ? CollectionPayloadCodec.DecodeKey(payload)
                : _recordSerializer.DecodeUpTo(payload, 0)[0].AsText;

        public bool TryDecodeDocumentForKey(ReadOnlySpan<byte> payload, string expectedKey, out GeneratedUser? document)
        {
            if (!PayloadMatchesKey(payload, expectedKey))
            {
                document = null;
                return false;
            }

            document = DecodeDocument(payload);
            return true;
        }

        public bool PayloadMatchesKey(ReadOnlySpan<byte> payload, string expectedKey)
            => _usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload)
                ? CollectionPayloadCodec.KeyEquals(payload, System.Text.Encoding.UTF8.GetBytes(expectedKey))
                : DecodeKey(payload) == expectedKey;
    }

    private sealed class ManualLinkedUserCollectionModel : ICollectionModel<ManualLinkedUser>
    {
        public ICollectionDocumentCodec<ManualLinkedUser> CreateCodec(IRecordSerializer recordSerializer)
            => new ManualLinkedUserCollectionCodec(recordSerializer);

        public bool TryGetField(string fieldPath, [NotNullWhen(true)] out CollectionField<ManualLinkedUser>? field)
        {
            if (StringComparer.OrdinalIgnoreCase.Equals(fieldPath, ManualLinkedUser.Collection.Name.FieldPath))
            {
                field = ManualLinkedUser.Collection.Name;
                return true;
            }

            field = null;
            return false;
        }
    }

    private sealed class ManualLinkedUserCollectionCodec : ICollectionDocumentCodec<ManualLinkedUser>
    {
        private readonly IRecordSerializer _recordSerializer;
        private readonly bool _usesDirectPayloadFormat;

        public ManualLinkedUserCollectionCodec(IRecordSerializer recordSerializer)
        {
            _recordSerializer = recordSerializer;
            _usesDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
        }

        public byte[] Encode(string key, ManualLinkedUser document)
        {
            ArgumentNullException.ThrowIfNull(key);
            ArgumentNullException.ThrowIfNull(document);

            if (_usesDirectPayloadFormat)
            {
                byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, ManualLinkedUserJsonContext.Default.ManualLinkedUser);
                return CollectionPayloadCodec.Encode(key, jsonUtf8);
            }

            string json = JsonSerializer.Serialize(document, ManualLinkedUserJsonContext.Default.ManualLinkedUser);
            return _recordSerializer.Encode(
            [
                CSharpDB.Primitives.DbValue.FromText(key),
                CSharpDB.Primitives.DbValue.FromText(json),
            ]);
        }

        public (string Key, ManualLinkedUser Document) Decode(ReadOnlySpan<byte> payload)
            => (DecodeKey(payload), DecodeDocument(payload));

        public ManualLinkedUser DecodeDocument(ReadOnlySpan<byte> payload)
        {
            if (_usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
            {
                if (!CollectionPayloadCodec.IsBinaryPayload(payload))
                {
                    return JsonSerializer.Deserialize(
                               CollectionPayloadCodec.GetJsonUtf8(payload),
                               ManualLinkedUserJsonContext.Default.ManualLinkedUser)
                           ?? throw new InvalidOperationException("Manual linked payload deserialized to null.");
                }

                string json = CollectionPayloadCodec.DecodeJson(payload);
                return JsonSerializer.Deserialize(json, ManualLinkedUserJsonContext.Default.ManualLinkedUser)
                       ?? throw new InvalidOperationException("Manual linked payload deserialized to null.");
            }

            var values = _recordSerializer.Decode(payload);
            return JsonSerializer.Deserialize(values[1].AsText, ManualLinkedUserJsonContext.Default.ManualLinkedUser)
                   ?? throw new InvalidOperationException("Manual linked payload deserialized to null.");
        }

        public string DecodeKey(ReadOnlySpan<byte> payload)
            => _usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload)
                ? CollectionPayloadCodec.DecodeKey(payload)
                : _recordSerializer.DecodeUpTo(payload, 0)[0].AsText;

        public bool TryDecodeDocumentForKey(ReadOnlySpan<byte> payload, string expectedKey, out ManualLinkedUser? document)
        {
            if (!PayloadMatchesKey(payload, expectedKey))
            {
                document = null;
                return false;
            }

            document = DecodeDocument(payload);
            return true;
        }

        public bool PayloadMatchesKey(ReadOnlySpan<byte> payload, string expectedKey)
            => _usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload)
                ? CollectionPayloadCodec.KeyEquals(payload, System.Text.Encoding.UTF8.GetBytes(expectedKey))
                : DecodeKey(payload) == expectedKey;
    }
}

[CollectionModel(typeof(GeneratedUserGeneratedJsonContext))]
internal sealed partial record GeneratedUser(string Email, int Age);

internal sealed record UnannotatedGeneratedCollectionUser(string Email);

[CollectionModel(typeof(RenamedGeneratedUserJsonContext))]
internal sealed partial record RenamedGeneratedUser(
    [property: JsonPropertyName("email_address")] string Email,
    [property: JsonPropertyName("age_years")] int Age);

[CollectionModel(typeof(TaggedGeneratedUserJsonContext))]
internal sealed partial record TaggedGeneratedUser(string Name, IReadOnlyList<string> Tags);

[CollectionModel(typeof(NestedGeneratedUserJsonContext))]
internal sealed partial record NestedGeneratedUser(string Name, NestedGeneratedAddress Address);

internal sealed partial record NestedGeneratedAddress(string City, int ZipCode);

[CollectionModel(typeof(OrderedGeneratedUserJsonContext))]
internal sealed partial record OrderedGeneratedUser(string Name, IReadOnlyList<OrderedGeneratedOrder> Orders);

internal sealed partial record OrderedGeneratedOrder(string Sku, int Quantity);

internal sealed partial record ManualLinkedUser(string Name, ManualLinkedUser? Next)
{
    public static class Collection
    {
        public static CollectionField<ManualLinkedUser, string> Name { get; } =
            new("Name", static document => document.Name, CollectionIndexDataKind.Text);
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(GeneratedUser))]
internal sealed partial class GeneratedUserGeneratedJsonContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(RenamedGeneratedUser))]
internal sealed partial class RenamedGeneratedUserJsonContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TaggedGeneratedUser))]
internal sealed partial class TaggedGeneratedUserJsonContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(NestedGeneratedUser))]
internal sealed partial class NestedGeneratedUserJsonContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(OrderedGeneratedUser))]
internal sealed partial class OrderedGeneratedUserJsonContext : JsonSerializerContext;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ManualLinkedUser))]
internal sealed partial class ManualLinkedUserJsonContext : JsonSerializerContext;
