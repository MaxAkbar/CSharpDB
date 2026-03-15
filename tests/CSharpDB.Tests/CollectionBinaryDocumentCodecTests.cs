using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class CollectionBinaryDocumentCodecTests
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    private sealed record BenchDoc(
        string Name,
        int Age,
        bool Active,
        decimal Balance,
        BenchProfile Profile);

    private sealed record BenchProfile(string Segment, BenchAddress Address);

    private sealed record BenchAddress(string City, int ZipCode);

    [Fact]
    public void BinaryCollectionPayload_RoundTripsNestedDocument()
    {
        var codec = new CollectionDocumentCodec<BenchDoc>(new DefaultRecordSerializer());
        var expected = new BenchDoc(
            "Alice Example",
            37,
            true,
            125.50m,
            new BenchProfile("enterprise", new BenchAddress("Seattle", 98101)));

        byte[] payload = codec.Encode("doc:42", expected);
        var actual = codec.Decode(payload);

        Assert.True(CollectionPayloadCodec.IsDirectPayload(payload));
        Assert.True(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal("doc:42", actual.Key);
        Assert.Equal(expected, actual.Document);
    }

    [Fact]
    public void BinaryCollectionPayload_IndexedFieldReader_ReadsNestedPath()
    {
        var codec = new CollectionDocumentCodec<BenchDoc>(new DefaultRecordSerializer());
        var accessor = CollectionFieldAccessor.FromFieldPath(
            $"{nameof(BenchDoc.Profile)}.{nameof(BenchProfile.Address)}.{nameof(BenchAddress.City)}");
        byte[] payload = codec.Encode(
            "doc:42",
            new BenchDoc(
                "Alice Example",
                37,
                true,
                125.50m,
                new BenchProfile("enterprise", new BenchAddress("Seattle", 98101))));

        bool found = accessor.TryReadValue(payload, out DbValue value);

        Assert.True(found);
        Assert.Equal(DbType.Text, value.Type);
        Assert.Equal("Seattle", value.AsText);
    }

    [Fact]
    public void BinaryCollectionPayload_TypedExtraction_ReadsScalarFields()
    {
        var codec = new CollectionDocumentCodec<BenchDoc>(new DefaultRecordSerializer());
        byte[] payload = codec.Encode(
            "doc:42",
            new BenchDoc(
                "Alice Example",
                37,
                true,
                125.50m,
                new BenchProfile("enterprise", new BenchAddress("Seattle", 98101))));
        var ageAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Age));
        var activeAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Active));
        var balanceAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDoc.Balance));

        Assert.True(ageAccessor.TryReadInt64(payload, out long age));
        Assert.Equal(37, age);

        Assert.True(activeAccessor.TryReadBoolean(payload, out bool active));
        Assert.True(active);

        Assert.True(balanceAccessor.TryReadDecimal(payload, out decimal balance));
        Assert.Equal(125.50m, balance);
    }

    [Fact]
    public void BinaryCollectionPayload_CodecReader_ReadsNestedDocumentSlice()
    {
        var codec = new CollectionDocumentCodec<BenchDoc>(new DefaultRecordSerializer());
        byte[] payload = codec.Encode(
            "doc:42",
            new BenchDoc(
                "Alice Example",
                37,
                true,
                125.50m,
                new BenchProfile("enterprise", new BenchAddress("Seattle", 98101))));
        byte[][] pathSegments =
        [
            System.Text.Encoding.UTF8.GetBytes("profile"),
            System.Text.Encoding.UTF8.GetBytes("address")
        ];

        Assert.True(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.True(
            CollectionBinaryDocumentCodec.TryReadDocument(
                CollectionPayloadCodec.GetBinaryDocumentPayload(payload),
                pathSegments,
                out ReadOnlySpan<byte> nestedDocument));
        Assert.True(
            CollectionBinaryDocumentCodec.TryReadString(
                nestedDocument,
                [System.Text.Encoding.UTF8.GetBytes("city")],
                out string? city));
        Assert.Equal("Seattle", city);
    }

    [Fact]
    public void BinaryCollectionPayload_CollectionAwareSerializer_PresentsJsonCompatibilityView()
    {
        var codec = new CollectionDocumentCodec<BenchDoc>(new DefaultRecordSerializer());
        var serializer = new CollectionAwareRecordSerializer(new DefaultRecordSerializer());
        var document = new BenchDoc(
            "Alice Example",
            37,
            true,
            125.50m,
            new BenchProfile("enterprise", new BenchAddress("Seattle", 98101)));
        byte[] payload = codec.Encode("doc:42", document);

        DbValue[] row = serializer.Decode(payload);

        Assert.Equal("doc:42", row[0].AsText);
        Assert.Equal(JsonSerializer.Serialize(document, s_jsonOptions), row[1].AsText);
    }

    [Fact]
    public void CollectionAwareSerializer_DoesNotMisclassifyRowsPrefixedWithBinaryMarker()
    {
        IRecordSerializer serializer = new CollectionAwareRecordSerializer(new BinaryMarkerPrefixRecordSerializer());
        DbValue[] expected =
        [
            DbValue.FromText("doc:42"),
            DbValue.FromText("{\"name\":\"Alice Example\",\"age\":37}")
        ];

        byte[] payload = serializer.Encode(expected);
        DbValue[] actual = serializer.Decode(payload);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void CollectionAwareSerializer_DoesNotMisclassifyRowsPrefixedWithBinaryMarkerAndVersion()
    {
        IRecordSerializer serializer = new CollectionAwareRecordSerializer(new BinaryMarkerAndVersionPrefixRecordSerializer());
        DbValue[] expected =
        [
            DbValue.FromText("doc:42"),
            DbValue.FromText("{\"name\":\"Alice Example\",\"age\":37}")
        ];

        byte[] payload = serializer.Encode(expected);
        DbValue[] actual = serializer.Decode(payload);

        Assert.Equal(expected, actual);
    }

    private sealed class BinaryMarkerPrefixRecordSerializer : IRecordSerializer
    {
        private readonly IRecordSerializer _inner = new DefaultRecordSerializer();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            byte[] encoded = _inner.Encode(values);
            byte[] prefixed = new byte[encoded.Length + 1];
            prefixed[0] = CollectionPayloadCodec.BinaryFormatMarker;
            encoded.CopyTo(prefixed.AsSpan(1));
            return prefixed;
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(StripPrefix(buffer));

        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
            => _inner.DecodeInto(StripPrefix(buffer), destination);

        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedCompactInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
            => _inner.DecodeUpTo(StripPrefix(buffer), maxColumnIndexInclusive);

        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.DecodeColumn(StripPrefix(buffer), columnIndex);

        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
            => _inner.TryColumnTextEquals(StripPrefix(buffer), columnIndex, expectedUtf8, out equals);

        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.IsColumnNull(StripPrefix(buffer), columnIndex);

        public bool TryDecodeNumericColumn(ReadOnlySpan<byte> buffer, int columnIndex, out long intValue, out double realValue, out bool isReal)
            => _inner.TryDecodeNumericColumn(StripPrefix(buffer), columnIndex, out intValue, out realValue, out isReal);

        private static ReadOnlySpan<byte> StripPrefix(ReadOnlySpan<byte> buffer)
        {
            if (buffer.IsEmpty || buffer[0] != CollectionPayloadCodec.BinaryFormatMarker)
                throw new InvalidOperationException("Expected prefixed row payload.");

            return buffer[1..];
        }
    }

    private sealed class BinaryMarkerAndVersionPrefixRecordSerializer : IRecordSerializer
    {
        private readonly IRecordSerializer _inner = new DefaultRecordSerializer();

        public byte[] Encode(ReadOnlySpan<DbValue> values)
        {
            byte[] encoded = _inner.Encode(values);
            byte[] prefixed = new byte[encoded.Length + 2];
            prefixed[0] = CollectionPayloadCodec.BinaryFormatMarker;
            prefixed[1] = CollectionPayloadCodec.BinaryFormatVersion;
            encoded.CopyTo(prefixed.AsSpan(2));
            return prefixed;
        }

        public DbValue[] Decode(ReadOnlySpan<byte> buffer) => _inner.Decode(StripPrefix(buffer));

        public int DecodeInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination)
            => _inner.DecodeInto(StripPrefix(buffer), destination);

        public void DecodeSelectedInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public void DecodeSelectedCompactInto(ReadOnlySpan<byte> buffer, Span<DbValue> destination, ReadOnlySpan<int> selectedColumnIndices)
            => _inner.DecodeSelectedCompactInto(StripPrefix(buffer), destination, selectedColumnIndices);

        public DbValue[] DecodeUpTo(ReadOnlySpan<byte> buffer, int maxColumnIndexInclusive)
            => _inner.DecodeUpTo(StripPrefix(buffer), maxColumnIndexInclusive);

        public DbValue DecodeColumn(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.DecodeColumn(StripPrefix(buffer), columnIndex);

        public bool TryColumnTextEquals(ReadOnlySpan<byte> buffer, int columnIndex, ReadOnlySpan<byte> expectedUtf8, out bool equals)
            => _inner.TryColumnTextEquals(StripPrefix(buffer), columnIndex, expectedUtf8, out equals);

        public bool IsColumnNull(ReadOnlySpan<byte> buffer, int columnIndex)
            => _inner.IsColumnNull(StripPrefix(buffer), columnIndex);

        public bool TryDecodeNumericColumn(ReadOnlySpan<byte> buffer, int columnIndex, out long intValue, out double realValue, out bool isReal)
            => _inner.TryDecodeNumericColumn(StripPrefix(buffer), columnIndex, out intValue, out realValue, out isReal);

        private static ReadOnlySpan<byte> StripPrefix(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length < 2 ||
                buffer[0] != CollectionPayloadCodec.BinaryFormatMarker ||
                buffer[1] != CollectionPayloadCodec.BinaryFormatVersion)
            {
                throw new InvalidOperationException("Expected prefixed row payload.");
            }

            return buffer[2..];
        }
    }
}
