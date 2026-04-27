using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class GeneratedCollectionEncodeBenchmarks : GeneratedCollectionCodecBenchmarkBase
{
    [Benchmark(Baseline = true, Description = "Generated collection encode (source-gen JSON payload)")]
    public byte[] Encode_SourceGeneratedJsonPayload()
        => JsonCodec.Encode(Key, JsonDocument);

    [Benchmark(Description = "Generated collection encode (source-gen binary payload)")]
    public byte[] Encode_SourceGeneratedBinaryPayload()
        => BinaryCodec.Encode(Key, BinaryDocument);
}

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class GeneratedCollectionIndexedFieldBenchmarks : GeneratedCollectionCodecBenchmarkBase
{
    private long _sink;

    [Benchmark(Baseline = true, Description = "Generated collection indexed field read (source-gen JSON payload)")]
    public void ReadIndexedField_SourceGeneratedJsonPayload()
    {
        if (CollectionIndexedFieldReader.TryReadValue(JsonPayload, ScoreAccessor, out var value) &&
            value.Type == DbType.Integer)
        {
            _sink = value.AsInteger;
        }
        else
        {
            _sink = -1;
        }
    }

    [Benchmark(Description = "Generated collection indexed field read (source-gen binary payload)")]
    public void ReadIndexedField_SourceGeneratedBinaryPayload()
    {
        if (ScoreField.TryReadPayloadInt64(BinaryPayload, out long score))
        {
            _sink = score;
        }
        else
        {
            _sink = -1;
        }
    }
}

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class GeneratedCollectionTextFieldBenchmarks : GeneratedCollectionCodecBenchmarkBase
{
    private int _sink;

    [Benchmark(Baseline = true, Description = "Generated collection text field read (source-gen JSON payload)")]
    public void ReadTextField_SourceGeneratedJsonPayload()
    {
        if (CollectionIndexedFieldReader.TryReadString(JsonPayload, EmailAccessor, out string? email) &&
            email is not null)
        {
            _sink = email.Length;
        }
        else
        {
            _sink = -1;
        }
    }

    [Benchmark(Description = "Generated collection text field read (source-gen binary UTF-8 payload)")]
    public void ReadTextField_SourceGeneratedBinaryPayload()
    {
        if (EmailField.TryReadPayloadStringUtf8(BinaryPayload, out ReadOnlySpan<byte> emailUtf8))
        {
            _sink = emailUtf8.Length;
        }
        else
        {
            _sink = -1;
        }
    }
}

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class GeneratedCollectionPayloadKeyBenchmarks : GeneratedCollectionCodecBenchmarkBase
{
    [Benchmark(Baseline = true, Description = "Generated collection key match (source-gen JSON payload)")]
    public bool PayloadMatchesKey_SourceGeneratedJsonPayload()
        => JsonCodec.PayloadMatchesKey(JsonPayload, Key);

    [Benchmark(Description = "Generated collection key match (source-gen binary payload)")]
    public bool PayloadMatchesKey_SourceGeneratedBinaryPayload()
        => BinaryCodec.PayloadMatchesKey(BinaryPayload, Key);
}

[MemoryDiagnoser]
[Config(typeof(CollectionInProcessBenchmarkConfig))]
public class GeneratedCollectionDecodeBenchmarks : GeneratedCollectionCodecBenchmarkBase
{
    [Benchmark(Baseline = true, Description = "Generated collection decode (source-gen JSON payload)")]
    public JsonSourceBenchDoc Decode_SourceGeneratedJsonPayload()
        => JsonCodec.DecodeDocument(JsonPayload);

    [Benchmark(Description = "Generated collection decode (source-gen binary payload)")]
    public GeneratedBinaryBenchDoc Decode_SourceGeneratedBinaryPayload()
        => BinaryCodec.DecodeDocument(BinaryPayload);
}

public abstract class GeneratedCollectionCodecBenchmarkBase
{
    protected const string Key = "doc:42";

    private CollectionModelRegistration? _jsonRegistration;

    private protected CollectionDocumentCodec<GeneratedBinaryBenchDoc> BinaryCodec { get; private set; } = null!;

    private protected CollectionDocumentCodec<JsonSourceBenchDoc> JsonCodec { get; private set; } = null!;

    private protected GeneratedBinaryBenchDoc BinaryDocument { get; private set; } = null!;

    private protected JsonSourceBenchDoc JsonDocument { get; private set; } = null!;

    private protected byte[] BinaryPayload { get; private set; } = null!;

    private protected byte[] JsonPayload { get; private set; } = null!;

    private protected CollectionFieldAccessor ScoreAccessor { get; private set; } = null!;

    private protected CollectionFieldAccessor EmailAccessor { get; private set; } = null!;

    private protected CollectionField<GeneratedBinaryBenchDoc, int> ScoreField { get; private set; } = null!;

    private protected CollectionField<GeneratedBinaryBenchDoc, string> EmailField { get; private set; } = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _jsonRegistration = CollectionModelRegistry.Register<JsonSourceBenchDoc>(new JsonSourceBenchDocCollectionModel());

        BinaryCodec = new CollectionDocumentCodec<GeneratedBinaryBenchDoc>(new DefaultRecordSerializer());
        JsonCodec = new CollectionDocumentCodec<JsonSourceBenchDoc>(new DefaultRecordSerializer());

        BinaryDocument = new GeneratedBinaryBenchDoc(
            "Alice Example",
            37,
            "Alpha",
            "alice@example.com",
            912,
            "west",
            "active",
            7,
            "gold",
            3,
            new GeneratedBinaryBenchProfile("enterprise", new GeneratedBinaryBenchAddress("Seattle", 98101)));
        JsonDocument = new JsonSourceBenchDoc(
            BinaryDocument.Name,
            BinaryDocument.Age,
            BinaryDocument.Category,
            BinaryDocument.Email,
            BinaryDocument.Score,
            BinaryDocument.Region,
            BinaryDocument.Status,
            BinaryDocument.Revision,
            BinaryDocument.Tier,
            BinaryDocument.Flags,
            new JsonSourceBenchProfile(
                BinaryDocument.Profile.Segment,
                new JsonSourceBenchAddress(
                    BinaryDocument.Profile.Address.City,
                    BinaryDocument.Profile.Address.ZipCode)));

        BinaryPayload = BinaryCodec.Encode(Key, BinaryDocument);
        JsonPayload = JsonCodec.Encode(Key, JsonDocument);
        ScoreAccessor = CollectionFieldAccessor.FromFieldPath("score");
        EmailAccessor = CollectionFieldAccessor.FromFieldPath("email");
        ScoreField = GeneratedBinaryBenchDoc.Collection.Score;
        EmailField = GeneratedBinaryBenchDoc.Collection.Email;

        if (!CollectionPayloadCodec.IsBinaryPayload(BinaryPayload))
            throw new InvalidOperationException("Generated binary benchmark payload did not use the generated binary path.");

        if (!CollectionPayloadCodec.IsDirectPayload(JsonPayload) || CollectionPayloadCodec.IsBinaryPayload(JsonPayload))
            throw new InvalidOperationException("Generated JSON benchmark payload did not use the direct JSON path.");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
        => _jsonRegistration?.Dispose();
}

[CollectionModel(typeof(GeneratedCollectionCodecJsonContext))]
public sealed partial record GeneratedBinaryBenchDoc(
    string Name,
    int Age,
    string Category,
    string Email,
    int Score,
    string Region,
    string Status,
    int Revision,
    string Tier,
    int Flags,
    GeneratedBinaryBenchProfile Profile);

public sealed partial record GeneratedBinaryBenchProfile(string Segment, GeneratedBinaryBenchAddress Address);

public sealed partial record GeneratedBinaryBenchAddress(string City, int ZipCode);

public sealed record JsonSourceBenchDoc(
    string Name,
    int Age,
    string Category,
    string Email,
    int Score,
    string Region,
    string Status,
    int Revision,
    string Tier,
    int Flags,
    JsonSourceBenchProfile Profile);

public sealed record JsonSourceBenchProfile(string Segment, JsonSourceBenchAddress Address);

public sealed record JsonSourceBenchAddress(string City, int ZipCode);

internal sealed class JsonSourceBenchDocCollectionModel : ICollectionModel<JsonSourceBenchDoc>
{
    public ICollectionDocumentCodec<JsonSourceBenchDoc> CreateCodec(IRecordSerializer recordSerializer)
        => new JsonSourceBenchDocCollectionCodec(recordSerializer);

    public bool TryGetField(
        string fieldPath,
        [NotNullWhen(true)] out CollectionField<JsonSourceBenchDoc>? field)
    {
        field = null;
        return false;
    }
}

internal sealed class JsonSourceBenchDocCollectionCodec : ICollectionDocumentCodec<JsonSourceBenchDoc>
{
    private const int StackallocKeyThreshold = 256;

    private readonly IRecordSerializer _recordSerializer;
    private readonly bool _usesDirectPayloadFormat;

    public JsonSourceBenchDocCollectionCodec(IRecordSerializer recordSerializer)
    {
        _recordSerializer = recordSerializer ?? throw new ArgumentNullException(nameof(recordSerializer));
        _usesDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
    }

    public byte[] Encode(string key, JsonSourceBenchDoc document)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(document);

        if (_usesDirectPayloadFormat)
        {
            byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(
                document,
                GeneratedCollectionCodecJsonContext.Default.JsonSourceBenchDoc);
            return CollectionPayloadCodec.Encode(key, jsonUtf8);
        }

        string json = JsonSerializer.Serialize(
            document,
            GeneratedCollectionCodecJsonContext.Default.JsonSourceBenchDoc);
        return _recordSerializer.Encode(
        [
            DbValue.FromText(key),
            DbValue.FromText(json),
        ]);
    }

    public (string Key, JsonSourceBenchDoc Document) Decode(ReadOnlySpan<byte> payload)
        => (DecodeKey(payload), DecodeDocument(payload));

    public JsonSourceBenchDoc DecodeDocument(ReadOnlySpan<byte> payload)
    {
        if (_usesDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
        {
            if (!CollectionPayloadCodec.IsBinaryPayload(payload))
            {
                return JsonSerializer.Deserialize(
                           CollectionPayloadCodec.GetJsonUtf8(payload),
                           GeneratedCollectionCodecJsonContext.Default.JsonSourceBenchDoc)
                       ?? throw new InvalidOperationException("Generated JSON benchmark payload deserialized to null.");
            }

            string json = CollectionPayloadCodec.DecodeJson(payload);
            return JsonSerializer.Deserialize(
                       json,
                       GeneratedCollectionCodecJsonContext.Default.JsonSourceBenchDoc)
                   ?? throw new InvalidOperationException("Generated JSON benchmark payload deserialized to null.");
        }

        var values = _recordSerializer.Decode(payload);
        return JsonSerializer.Deserialize(
                   values[1].AsText,
                   GeneratedCollectionCodecJsonContext.Default.JsonSourceBenchDoc)
               ?? throw new InvalidOperationException("Generated JSON benchmark payload deserialized to null.");
    }

    public string DecodeKey(ReadOnlySpan<byte> payload)
    {
        if (_usesDirectPayloadFormat && CollectionPayloadCodec.TryDecodeDirectPayloadKey(payload, out string key))
            return key;

        var values = _recordSerializer.DecodeUpTo(payload, 0);
        return values[0].AsText;
    }

    public bool TryDecodeDocumentForKey(
        ReadOnlySpan<byte> payload,
        string expectedKey,
        [NotNullWhen(true)] out JsonSourceBenchDoc? document)
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
    {
        ArgumentNullException.ThrowIfNull(expectedKey);

        if (_usesDirectPayloadFormat && CollectionPayloadCodec.TryDirectPayloadKeyEquals(payload, expectedKey, out bool directEquals))
            return directEquals;

        int byteCount = Encoding.UTF8.GetByteCount(expectedKey);
        byte[]? rented = null;
        Span<byte> utf8 = byteCount <= StackallocKeyThreshold
            ? stackalloc byte[StackallocKeyThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

        try
        {
            int written = Encoding.UTF8.GetBytes(expectedKey.AsSpan(), utf8);
            ReadOnlySpan<byte> expectedKeyUtf8 = utf8[..written];

            if (_recordSerializer.TryColumnTextEquals(payload, 0, expectedKeyUtf8, out bool equals))
                return equals;

            return DecodeKey(payload) == expectedKey;
        }
        finally
        {
            if (rented is not null)
            {
                utf8[..byteCount].Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(GeneratedBinaryBenchDoc))]
[JsonSerializable(typeof(JsonSourceBenchDoc))]
internal sealed partial class GeneratedCollectionCodecJsonContext : JsonSerializerContext;
