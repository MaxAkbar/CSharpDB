using System.Text;
using BenchmarkDotNet.Attributes;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SqlMaterializationBenchmarks
{
    private readonly IRecordSerializer _serializer = new DefaultRecordSerializer();
    private readonly int[] _selectedColumns = [0, 2, 5, 8];
    private readonly byte[] _expectedTierUtf8 = Encoding.UTF8.GetBytes("gold");
    private readonly RecordColumnAccessor _tierAccessor = new(2);
    private readonly RecordColumnAccessor _salaryAccessor = new(4);
    private readonly RecordColumnAccessor _ageAccessor = new(5);
    private byte[] _payload = null!;
    private DbValue[] _selectedBuffer = null!;
    private DbValue[]? _sinkRow;
    private DbValue _sinkValue;
    private long _sinkLong;
    private bool _sinkBool;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _payload = _serializer.Encode(
        [
            DbValue.FromInteger(42),
            DbValue.FromText("customer-42"),
            DbValue.FromText("gold"),
            DbValue.FromText("alice@example.com"),
            DbValue.FromInteger(125_000),
            DbValue.FromInteger(37),
            DbValue.FromReal(98.5),
            DbValue.FromText("west"),
            DbValue.FromText("Seattle"),
        ]);

        _selectedBuffer = new DbValue[9];
    }

    [Benchmark(Baseline = true, Description = "SQL decode (full row)")]
    public void Decode_FullRow()
        => _sinkRow = _serializer.Decode(_payload);

    [Benchmark(Description = "SQL decode (selected cols, reused buffer)")]
    public void Decode_SelectedColumns()
    {
        _serializer.DecodeSelectedInto(_payload, _selectedBuffer, _selectedColumns);
        _sinkValue = _selectedBuffer[8];
    }

    [Benchmark(Description = "SQL decode (prefix up to col 5)")]
    public void Decode_UpToProjectionTail()
        => _sinkRow = _serializer.DecodeUpTo(_payload, 5);

    [Benchmark(Description = "SQL decode (single column)")]
    public void Decode_SingleColumn()
        => _sinkValue = _serializer.DecodeColumn(_payload, 5);

    [Benchmark(Description = "SQL decode (single column, bound accessor)")]
    public void Decode_SingleColumn_BoundAccessor()
        => _sinkValue = _ageAccessor.Decode(_payload);

    [Benchmark(Description = "SQL payload text compare")]
    public void Compare_TextColumn_NoMaterialize()
        => _sinkBool = _serializer.TryColumnTextEquals(_payload, 2, _expectedTierUtf8, out bool equals) && equals;

    [Benchmark(Description = "SQL payload text compare (bound accessor)")]
    public void Compare_TextColumn_BoundAccessor()
        => _sinkBool = _tierAccessor.TryTextEquals(_payload, _expectedTierUtf8, out bool equals) && equals;

    [Benchmark(Description = "SQL payload numeric decode")]
    public void Decode_NumericColumn_NoMaterialize()
    {
        if (_serializer.TryDecodeNumericColumn(_payload, 4, out long intValue, out double realValue, out bool isReal))
            _sinkLong = isReal ? (long)realValue : intValue;
        else
            _sinkLong = -1;
    }

    [Benchmark(Description = "SQL payload numeric decode (bound accessor)")]
    public void Decode_NumericColumn_BoundAccessor()
    {
        if (_salaryAccessor.TryDecodeNumeric(_payload, out long intValue, out double realValue, out bool isReal))
            _sinkLong = isReal ? (long)realValue : intValue;
        else
            _sinkLong = -1;
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CollectionAccessBenchmarks
{
    private readonly CollectionDocumentCodec<BenchDoc> _codec = new(new DefaultRecordSerializer());
    private readonly string _expectedKey = "doc:42";
    private byte[] _payload = null!;
    private string? _sinkText;
    private long _sinkLong;
    private bool _sinkBool;

    private sealed record BenchDoc(
        string Name,
        int Age,
        string Category,
        string Email,
        string City,
        int Score);

    [GlobalSetup]
    public void GlobalSetup()
    {
        _payload = _codec.Encode(
            _expectedKey,
            new BenchDoc(
                "Alice Example",
                37,
                "Alpha",
                "alice@example.com",
                "Seattle",
                912));
    }

    [Benchmark(Baseline = true, Description = "Collection hydrate document")]
    public void Hydrate_Document()
        => _sinkText = _codec.DecodeDocument(_payload).Email;

    [Benchmark(Description = "Collection decode key only")]
    public void Decode_KeyOnly()
        => _sinkText = _codec.DecodeKey(_payload);

    [Benchmark(Description = "Collection payload key match")]
    public void Match_Key_NoHydration()
        => _sinkBool = _codec.PayloadMatchesKey(_payload, _expectedKey);

    [Benchmark(Description = "Collection indexed field read (int)")]
    public void Read_IndexedField_Integer()
    {
        if (CollectionIndexedFieldReader.TryReadValue(_payload, "age", out var value) &&
            value.Type == DbType.Integer)
        {
            _sinkLong = value.AsInteger;
        }
        else
        {
            _sinkLong = -1;
        }
    }

    [Benchmark(Description = "Collection indexed field text compare")]
    public void Compare_IndexedField_Text()
        => _sinkBool = CollectionIndexedFieldReader.TryTextEquals(_payload, "category", "Alpha");
}
