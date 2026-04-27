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

    private sealed record BenchDocWithArrays(
        string Name,
        string[] Tags,
        List<int> Scores);
    private sealed record BenchDocWithNestedArray(
        string Name,
        BenchOrder[] Orders);
    private sealed record BenchTemporalDoc(
        Guid SessionId,
        DateOnly EventDate,
        TimeOnly StartTime,
        string Name);
    private sealed record BenchUInt64Doc(
        string Name,
        ulong Sequence);
    private sealed record BenchDateTimeDoc(
        string Name,
        DateTime UpdatedAt);

    private sealed record BenchProfile(string Segment, BenchAddress Address);

    private sealed record BenchAddress(string City, int ZipCode);
    private sealed record BenchOrder(string Sku, int Quantity);

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
    public void BinaryCollectionPayload_RoundTripsGuidAndTemporalScalars()
    {
        var codec = new CollectionDocumentCodec<BenchTemporalDoc>(new DefaultRecordSerializer());
        var expected = new BenchTemporalDoc(
            Guid.Parse("2f8c4c7e-0d1b-4d26-9b70-61c5d9342ef0"),
            new DateOnly(2026, 3, 15),
            new TimeOnly(14, 30, 45, 123),
            "Alice Example");

        byte[] payload = codec.Encode("doc:temporal", expected);
        var actual = codec.Decode(payload);

        Assert.True(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal("doc:temporal", actual.Key);
        Assert.Equal(expected, actual.Document);
    }

    [Fact]
    public void BinaryCollectionPayload_RoundTripsUInt64PastSignedRange()
    {
        var codec = new CollectionDocumentCodec<BenchUInt64Doc>(new DefaultRecordSerializer());
        var expected = new BenchUInt64Doc("Alice Example", ulong.MaxValue - 7);

        byte[] payload = codec.Encode("doc:uint64", expected);
        var actual = codec.Decode(payload);

        Assert.True(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal("doc:uint64", actual.Key);
        Assert.Equal(expected, actual.Document);
    }

    [Fact]
    public void BinaryCollectionPayload_PathReader_ExposesGuidAndTemporalScalars()
    {
        var codec = new CollectionDocumentCodec<BenchTemporalDoc>(new DefaultRecordSerializer());
        var expected = new BenchTemporalDoc(
            Guid.Parse("2f8c4c7e-0d1b-4d26-9b70-61c5d9342ef0"),
            new DateOnly(2026, 3, 15),
            new TimeOnly(14, 30, 45, 123),
            "Alice Example");
        byte[] payload = codec.Encode("doc:temporal", expected);

        Assert.True(CollectionIndexedFieldReader.TryReadValue(payload, "sessionId", out DbValue sessionId));
        Assert.Equal(DbType.Text, sessionId.Type);
        Assert.Equal(expected.SessionId.ToString("D"), sessionId.AsText);

        Assert.True(CollectionIndexedFieldReader.TryReadValue(payload, "eventDate", out DbValue eventDate));
        Assert.Equal(DbType.Text, eventDate.Type);
        Assert.Equal(expected.EventDate.ToString("O"), eventDate.AsText);

        Assert.True(CollectionIndexedFieldReader.TryReadValue(payload, "startTime", out DbValue startTime));
        Assert.Equal(DbType.Text, startTime.Type);
        Assert.Equal(expected.StartTime.ToString("O"), startTime.AsText);
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
        Assert.True(
            CollectionBinaryDocumentCodec.TryReadStringUtf8(
                nestedDocument,
                [System.Text.Encoding.UTF8.GetBytes("city")],
                out ReadOnlySpan<byte> cityUtf8));
        Assert.Equal("Seattle", System.Text.Encoding.UTF8.GetString(cityUtf8));
    }

    [Fact]
    public void BinaryCollectionPayload_RoundTripsScalarArrays()
    {
        var codec = new CollectionDocumentCodec<BenchDocWithArrays>(new DefaultRecordSerializer());
        var expected = new BenchDocWithArrays("Alice Example", ["alpha", "beta"], [10, 20, 30]);

        byte[] payload = codec.Encode("doc:arrays", expected);
        var actual = codec.Decode(payload);

        Assert.True(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal(expected.Name, actual.Document.Name);
        Assert.Equal(expected.Tags, actual.Document.Tags);
        Assert.Equal(expected.Scores, actual.Document.Scores);
    }

    [Fact]
    public void BinaryCollectionPayload_ArrayExtraction_ReadsScalarElements()
    {
        var codec = new CollectionDocumentCodec<BenchDocWithArrays>(new DefaultRecordSerializer());
        byte[] payload = codec.Encode(
            "doc:arrays",
            new BenchDocWithArrays("Alice Example", ["alpha", "beta", "alpha"], [10, 20, 30]));
        var tagAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDocWithArrays.Tags) + "[]");
        var scoreAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDocWithArrays.Scores) + "[]");
        var tags = new List<DbValue>();
        var scores = new List<DbValue>();

        Assert.True(tagAccessor.TryReadIndexValues(payload, tags));
        Assert.True(scoreAccessor.TryReadIndexValues(payload, scores));
        Assert.True(tagAccessor.TryValueEquals(payload, DbValue.FromText("beta")));
        Assert.True(scoreAccessor.TryValueEquals(payload, DbValue.FromInteger(20)));
        Assert.False(tagAccessor.TryValueEquals(payload, DbValue.FromText("gamma")));

        Assert.Equal(["alpha", "beta", "alpha"], tags.Select(static value => value.AsText).ToArray());
        Assert.Equal([10L, 20L, 30L], scores.Select(static value => value.AsInteger).ToArray());
    }

    [Fact]
    public void BinaryCollectionPayload_ArrayExtraction_ReadsNestedObjectScalarElements()
    {
        var codec = new CollectionDocumentCodec<BenchDocWithNestedArray>(new DefaultRecordSerializer());
        byte[] payload = codec.Encode(
            "doc:orders",
            new BenchDocWithNestedArray(
                "Alice Example",
                [
                    new BenchOrder("sku-alpha", 1),
                    new BenchOrder("sku-beta", 2),
                    new BenchOrder("sku-alpha", 3)
                ]));
        var skuAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDocWithNestedArray.Orders) + "[]." + nameof(BenchOrder.Sku));
        var quantityAccessor = CollectionFieldAccessor.FromFieldPath(nameof(BenchDocWithNestedArray.Orders) + "[]." + nameof(BenchOrder.Quantity));
        var skus = new List<DbValue>();
        var quantities = new List<DbValue>();

        Assert.True(skuAccessor.TryReadIndexValues(payload, skus));
        Assert.True(quantityAccessor.TryReadIndexValues(payload, quantities));
        Assert.True(skuAccessor.TryValueEquals(payload, DbValue.FromText("sku-beta")));
        Assert.True(quantityAccessor.TryValueEquals(payload, DbValue.FromInteger(2)));
        Assert.False(skuAccessor.TryValueEquals(payload, DbValue.FromText("sku-gamma")));

        Assert.Equal(["sku-alpha", "sku-beta", "sku-alpha"], skus.Select(static value => value.AsText).ToArray());
        Assert.Equal([1L, 2L, 3L], quantities.Select(static value => value.AsInteger).ToArray());
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

    [Fact]
    public void CollectionDocumentCodec_FallsBackToJsonForUnsupportedTypedDocuments()
    {
        var codec = new CollectionDocumentCodec<BenchDateTimeDoc>(new DefaultRecordSerializer());
        var expected = new BenchDateTimeDoc(
            "Alice Example",
            new DateTime(2026, 3, 20, 15, 45, 12, DateTimeKind.Utc));

        byte[] payload = codec.Encode("doc:datetime", expected);
        var actual = codec.Decode(payload);

        Assert.True(CollectionPayloadCodec.IsDirectPayload(payload));
        Assert.False(CollectionPayloadCodec.IsBinaryPayload(payload));
        Assert.Equal("doc:datetime", actual.Key);
        Assert.Equal(expected, actual.Document);
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
