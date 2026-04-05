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

    private static async Task<List<KeyValuePair<string, GeneratedUser>>> CollectAsync(
        IAsyncEnumerable<KeyValuePair<string, GeneratedUser>> source,
        CancellationToken ct = default)
    {
        var items = new List<KeyValuePair<string, GeneratedUser>>();
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
}

[CollectionModel(typeof(GeneratedUserGeneratedJsonContext))]
internal sealed partial record GeneratedUser(string Email, int Age);

internal sealed record UnannotatedGeneratedCollectionUser(string Email);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(GeneratedUser))]
internal sealed partial class GeneratedUserGeneratedJsonContext : JsonSerializerContext;
