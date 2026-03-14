using BenchmarkDotNet.Attributes;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionFieldExtractionBenchmarks
{
    private readonly CollectionDocumentCodec<BenchDoc> _codec = new(new DefaultRecordSerializer());
    private CollectionFieldAccessor _scoreAccessor = null!;
    private CollectionFieldAccessor _flagsAccessor = null!;
    private CollectionFieldAccessor _tierAccessor = null!;
    private CollectionFieldAccessor _cityAccessor = null!;
    private byte[] _payload = null!;
    private string? _sinkText;
    private long _sinkLong;
    private bool _sinkBool;

    private sealed record BenchDoc(
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
        BenchProfile Profile);

    private sealed record BenchProfile(string Segment, BenchAddress Address);

    private sealed record BenchAddress(string City, int ZipCode);

    [GlobalSetup]
    public void GlobalSetup()
    {
        _scoreAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Score));
        _flagsAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Flags));
        _tierAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Tier));
        _cityAccessor = CollectionFieldAccessor.FromFieldPath(
            $"{nameof(BenchDoc.Profile)}.{nameof(BenchProfile.Address)}.{nameof(BenchAddress.City)}");

        _payload = _codec.Encode(
            "doc:42",
            new BenchDoc(
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
                new BenchProfile("enterprise", new BenchAddress("Seattle", 98101))));
    }

    [Benchmark(Baseline = true, Description = "Collection field read (early field)")]
    public void Read_EarlyField()
    {
        if (CollectionIndexedFieldReader.TryReadValue(_payload, "name", out var value) &&
            value.Type == DbType.Text)
        {
            _sinkText = value.AsText;
        }
        else
        {
            _sinkText = null;
        }
    }

    [Benchmark(Description = "Collection field read (middle field)")]
    public void Read_MiddleField()
    {
        if (CollectionIndexedFieldReader.TryReadValue(_payload, "score", out var value) &&
            value.Type == DbType.Integer)
        {
            _sinkLong = value.AsInteger;
        }
        else
        {
            _sinkLong = -1;
        }
    }

    [Benchmark(Description = "Collection field read (late field)")]
    public void Read_LateField()
    {
        if (CollectionIndexedFieldReader.TryReadValue(_payload, "flags", out var value) &&
            value.Type == DbType.Integer)
        {
            _sinkLong = value.AsInteger;
        }
        else
        {
            _sinkLong = -1;
        }
    }

    [Benchmark(Description = "Collection field read (late field, bound accessor)")]
    public void Read_LateField_BoundAccessor()
    {
        if (_flagsAccessor.TryReadValue(_payload, out var value) &&
            value.Type == DbType.Integer)
        {
            _sinkLong = value.AsInteger;
        }
        else
        {
            _sinkLong = -1;
        }
    }

    [Benchmark(Description = "Collection field read (missing field)")]
    public void Read_MissingField()
        => _sinkBool = CollectionIndexedFieldReader.TryReadValue(_payload, "missing", out _);

    [Benchmark(Description = "Collection field compare (late text field)")]
    public void Compare_LateTextField()
        => _sinkBool = CollectionIndexedFieldReader.TryTextEquals(_payload, "tier", "gold");

    [Benchmark(Description = "Collection field compare (late text field, bound accessor)")]
    public void Compare_LateTextField_BoundAccessor()
        => _sinkBool = _tierAccessor.TryTextEquals(_payload, "gold");

    [Benchmark(Description = "Collection field read (nested path, bound accessor)")]
    public void Read_NestedField_BoundAccessor()
    {
        if (_cityAccessor.TryReadValue(_payload, out var value) &&
            value.Type == DbType.Text)
        {
            _sinkText = value.AsText;
        }
        else
        {
            _sinkText = null;
        }
    }

    [Benchmark(Description = "Collection field read (middle field, bound accessor)")]
    public void Read_MiddleField_BoundAccessor()
    {
        if (_scoreAccessor.TryReadValue(_payload, out var value) &&
            value.Type == DbType.Integer)
        {
            _sinkLong = value.AsInteger;
        }
        else
        {
            _sinkLong = -1;
        }
    }

    [Benchmark(Description = "Collection hydrate document (comparison)")]
    public void Hydrate_FullDocument()
        => _sinkText = _codec.DecodeDocument(_payload).Tier;
}
