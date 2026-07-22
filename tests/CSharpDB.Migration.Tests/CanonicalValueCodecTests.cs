using System.Buffers.Binary;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.Tests;

public sealed class CanonicalValueCodecTests
{
    private const int EnvelopeHeaderLength = 8 + 32 + sizeof(uint);

    [Fact]
    public void GoldenVectors_AreIndependentlyValidAndMatchStreamingCodec()
    {
        GoldenFixture fixture = LoadFixture();
        Assert.Equal(CanonicalRowCodec.CanonicalizationId, fixture.CanonicalizationId);
        Assert.Equal(
            Convert.ToHexString(Encoding.ASCII.GetBytes(CanonicalRowCodec.RowDomain)).ToLowerInvariant(),
            fixture.RowMagicHex);
        Assert.Equal(
            Convert.ToHexString(Encoding.ASCII.GetBytes(CanonicalRowCodec.KeyDomain)).ToLowerInvariant(),
            fixture.KeyMagicHex);
        Assert.Equal(CanonicalRowCodec.ContractHashHex, fixture.ContractHash);
        Assert.Equal(
            fixture.ContractHash,
            Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(fixture.CanonicalizationId)))
                .ToLowerInvariant());
        Assert.Equal(
            Enum.GetValues<CanonicalType>(),
            fixture.Vectors
                .SelectMany(vector => FieldsFor(vector.Name))
                .Select(field => field.Type)
                .Distinct()
                .OrderBy(type => type));

        foreach (GoldenVector vector in fixture.Vectors)
        {
            byte[] expectedRow = ComposeGoldenRow(fixture, vector);
            string independentlyComputed = Convert.ToHexString(SHA256.HashData(expectedRow)).ToLowerInvariant();
            Assert.Equal(vector.RowSha256, independentlyComputed);

            IReadOnlyList<CanonicalValue> fields = FieldsFor(vector.Name);
            Assert.Equal(expectedRow, CanonicalRowCodec.EncodeRow(fields));
            Assert.Equal(vector.RowSha256, CanonicalRowCodec.ComputeRowHash(fields));
        }
    }

    [Fact]
    public void RemainingValueTags_HaveExactNetworkOrderFrames()
    {
        AssertFrame("0100000000000000000101", CanonicalValue.Boolean(true));
        AssertFrame("03000000000000000008ffffffffffffffff", CanonicalValue.UInt64(ulong.MaxValue));
        AssertFrame("08000000000000000003deadbe", CanonicalValue.Blob(new byte[] { 0xDE, 0xAD, 0xBE }));
        AssertFrame("0a000000000000000004ffffffff", CanonicalValue.Date(-1));
        AssertFrame("0b000000000000000008000000003b9aca00", CanonicalValue.Time(1_000_000_000));
    }

    [Fact]
    public void Decimal_NormalizesScaleCoefficientZeroAndSignExtension()
    {
        AssertFrame(
            "04000000000000000006000000023039",
            CanonicalValue.Decimal(new BigInteger(1_234_500), 4));
        AssertFrame("040000000000000000050000000000", CanonicalValue.Decimal(BigInteger.Zero, uint.MaxValue));
        AssertFrame("0400000000000000000500000000ff", CanonicalValue.Decimal(new BigInteger(-1), 0));
        AssertFrame("04000000000000000006000000000080", CanonicalValue.Decimal(new BigInteger(128), 0));
        AssertFrame("0400000000000000000600000000ff7f", CanonicalValue.Decimal(new BigInteger(-129), 0));
        AssertFrame("04000000000000000006000000023039", CanonicalValue.Decimal(123.4500m));
    }

    [Fact]
    public void RealValues_RejectNonFiniteAndNormalizeBothNegativeZeros()
    {
        AssertFrame("0500000000000000000400000000", CanonicalValue.Binary32(-0F));
        AssertFrame("060000000000000000080000000000000000", CanonicalValue.Binary64(-0D));
        Assert.Throws<ArgumentOutOfRangeException>(() => CanonicalValue.Binary32(float.NaN));
        Assert.Throws<ArgumentOutOfRangeException>(() => CanonicalValue.Binary32(float.PositiveInfinity));
        Assert.Throws<ArgumentOutOfRangeException>(() => CanonicalValue.Binary64(double.NegativeInfinity));
    }

    [Fact]
    public void Text_IsStrictUtf8AndDoesNotApplyUnicodeNormalization()
    {
        AssertFrame("0700000000000000000341cc8a", CanonicalValue.Text("A\u030A"));
        AssertFrame("07000000000000000002c385", CanonicalValue.Text("\u00C5"));

        var invalid = CanonicalValue.Text("\uD800");
        Assert.Throws<InvalidDataException>(() => CanonicalRowCodec.EncodeRow([invalid]));
        Assert.Throws<InvalidDataException>(() => CanonicalRowCodec.ComputeRowHash([invalid]));
    }

    [Fact]
    public void TemporalFactories_EnforceRangesAndPreEpochFloorSemantics()
    {
        CanonicalValue beforeEpoch = CanonicalValue.UtcInstant(
            new DateTimeOffset(1969, 12, 31, 23, 59, 59, 500, TimeSpan.Zero));
        AssertFrame(
            "0d00000000000000000cffffffffffffffff1dcd6500",
            beforeEpoch);

        Assert.Throws<ArgumentOutOfRangeException>(() => CanonicalValue.Time(86_400_000_000_000));
        Assert.Throws<ArgumentOutOfRangeException>(() => CanonicalValue.UtcInstant(0, 1_000_000_000));
        Assert.Throws<ArgumentException>(
            () => CanonicalValue.WallDateTime(new DateTime(2026, 7, 21, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void NullTypeAndRegisteredExclusionRemainPartOfTheHash()
    {
        Assert.NotEqual(
            CanonicalRowCodec.ComputeRowHash([CanonicalValue.Null(CanonicalType.Text)]),
            CanonicalRowCodec.ComputeRowHash([CanonicalValue.Null(CanonicalType.Blob)]));
        AssertFrame("0802000000000000000101", CanonicalValue.RegeneratedRowVersion());
    }

    [Fact]
    public void KeyHash_UsesIndependentDomainBeforeTheCompleteCanonicalRow()
    {
        CanonicalValue[] fields = [CanonicalValue.Int64(42), CanonicalValue.Text("key")];
        byte[] row = CanonicalRowCodec.EncodeRow(fields);
        byte[] keyMagic = "CSDBKEY1"u8.ToArray();
        byte[] domainAndRow = new byte[keyMagic.Length + row.Length];
        keyMagic.CopyTo(domainAndRow, 0);
        row.CopyTo(domainAndRow, keyMagic.Length);
        string expected = Convert.ToHexString(SHA256.HashData(domainAndRow)).ToLowerInvariant();

        Assert.Equal(expected, CanonicalRowCodec.ComputeKeyHash(fields));
        Assert.NotEqual(CanonicalRowCodec.ComputeRowHash(fields), CanonicalRowCodec.ComputeKeyHash(fields));
    }

    [Fact]
    public void BinaryHashApis_ReturnIndependentDigestArrays()
    {
        CanonicalValue[] fields = [CanonicalValue.Int64(42)];
        byte[] first = CanonicalRowCodec.ComputeRowHashBytes(fields);
        byte[] second = CanonicalRowCodec.ComputeRowHashBytes(fields);

        Assert.NotSame(first, second);
        Assert.Equal(first, second);
        first[0] ^= 0xFF;
        Assert.NotEqual(first, second);
        Assert.Equal(CanonicalRowCodec.ComputeRowHash(fields), Convert.ToHexString(second).ToLowerInvariant());
    }

    [Fact]
    public void StreamingHash_MatchesIndependentHashForMultiBufferPayloads()
    {
        byte[] blob = new byte[20_000];
        for (int index = 0; index < blob.Length; index++)
            blob[index] = (byte)(index * 31);
        CanonicalValue[] fields = [CanonicalValue.Text(new string('\u03BB', 5_000)), CanonicalValue.Blob(blob)];

        byte[] encoded = CanonicalRowCodec.EncodeRow(fields);
        string expected = Convert.ToHexString(SHA256.HashData(encoded)).ToLowerInvariant();

        Assert.Equal(expected, CanonicalRowCodec.ComputeRowHash(fields));
    }

    private static void AssertFrame(string expectedHex, CanonicalValue value)
    {
        byte[] encoded = CanonicalRowCodec.EncodeRow([value]);
        Assert.Equal(expectedHex, Convert.ToHexString(encoded.AsSpan(EnvelopeHeaderLength)).ToLowerInvariant());
    }

    private static byte[] ComposeGoldenRow(GoldenFixture fixture, GoldenVector vector)
    {
        byte[] magic = Convert.FromHexString(fixture.RowMagicHex);
        byte[] contractHash = Convert.FromHexString(fixture.ContractHash);
        byte[] frame = Convert.FromHexString(vector.FrameHex);
        byte[] row = new byte[EnvelopeHeaderLength + frame.Length];
        magic.CopyTo(row, 0);
        contractHash.CopyTo(row, magic.Length);
        BinaryPrimitives.WriteUInt32BigEndian(
            row.AsSpan(magic.Length + contractHash.Length, sizeof(uint)),
            vector.FieldCount);
        frame.CopyTo(row, EnvelopeHeaderLength);
        return row;
    }

    private static IReadOnlyList<CanonicalValue> FieldsFor(string name) => name switch
    {
        "empty" => [],
        "null-text" => [CanonicalValue.Null(CanonicalType.Text)],
        "boolean-true" => [CanonicalValue.Boolean(true)],
        "int64-minus-one" => [CanonicalValue.Int64(-1)],
        "uint64-max" => [CanonicalValue.UInt64(ulong.MaxValue)],
        "decimal-123-4500" => [CanonicalValue.Decimal(new BigInteger(1_234_500), 4)],
        "binary32-negative-zero" => [CanonicalValue.Binary32(-0F)],
        "binary64-one-point-five" => [CanonicalValue.Binary64(1.5D)],
        "text-a-combining-ring" => [CanonicalValue.Text("A\u030A")],
        "blob-deadbe" => [CanonicalValue.Blob(new byte[] { 0xDE, 0xAD, 0xBE })],
        "guid-rfc-network-order" => [CanonicalValue.Guid(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"))],
        "date-minus-one" => [CanonicalValue.Date(-1)],
        "time-one-second" => [CanonicalValue.Time(1_000_000_000)],
        "wall-date-time" => [CanonicalValue.WallDateTime(20_655, 29_350_123_456_700)],
        "utc-instant" => [CanonicalValue.UtcInstant(1_784_646_550, 123_456_700)],
        "offset-date-time" => [CanonicalValue.OffsetDateTime(20_655, 29_350_123_456_700, -420)],
        "excluded-rowversion" => [CanonicalValue.RegeneratedRowVersion()],
        _ => throw new InvalidDataException($"Unknown canonical golden vector '{name}'."),
    };

    private static GoldenFixture LoadFixture()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "csharpdb-canon-v1.golden.json");
        return JsonSerializer.Deserialize<GoldenFixture>(
                   File.ReadAllText(path),
                   new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ??
               throw new InvalidDataException("The canonical golden fixture is empty.");
    }

    private sealed record GoldenFixture
    {
        public required string CanonicalizationId { get; init; }

        public required string RowMagicHex { get; init; }

        public required string KeyMagicHex { get; init; }

        public required string ContractHash { get; init; }

        public required IReadOnlyList<GoldenVector> Vectors { get; init; }
    }

    private sealed record GoldenVector
    {
        public required string Name { get; init; }

        public uint FieldCount { get; init; }

        public required string FrameHex { get; init; }

        public required string RowSha256 { get; init; }
    }
}
