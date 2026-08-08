using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class DecimalStorageTests
{
    [Fact]
    public void DbValue_NormalizesDecimalCoefficientAndScale()
    {
        DbValue value = DbValue.FromDecimal(123.4500m);

        Assert.Equal(DbType.Decimal, value.Type);
        Assert.Equal(12_345L, value.DecimalCoefficient);
        Assert.Equal(2, value.DecimalScale);
        Assert.Equal(123.45m, value.AsDecimal);
        Assert.True(value.IsTruthy);
        Assert.False(DbValue.FromDecimal(0.000m).IsTruthy);
    }

    [Fact]
    public void DbValue_RejectsDecimalOutsidePrecisionEighteen()
    {
        Assert.Throws<OverflowException>(
            () => DbValue.FromDecimal(1_000_000_000_000_000_000m));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => DbValue.FromDecimalParts(1, 19));
    }

    [Fact]
    public void NumericEqualityComparisonAndHashing_AgreeAcrossRepresentations()
    {
        DbValue integer = DbValue.FromInteger(42);
        DbValue real = DbValue.FromReal(42.0);
        DbValue exact = DbValue.FromDecimal(42.000m);

        Assert.Equal(0, DbValue.Compare(integer, real));
        Assert.Equal(0, DbValue.Compare(real, exact));
        Assert.Equal(integer, real);
        Assert.Equal(real, exact);
        Assert.Equal(integer.GetHashCode(), real.GetHashCode());
        Assert.Equal(real.GetHashCode(), exact.GetHashCode());
    }

    [Fact]
    public void RecordEncoder_RoundTripsDecimalThroughAllProjectionPaths()
    {
        DbValue expected = DbValue.FromDecimal(-1234567890.012300m);
        DbValue[] values =
        [
            DbValue.FromInteger(7),
            expected,
            DbValue.FromText("tail"),
        ];
        byte[] encoded = RecordEncoder.Encode(values);

        Assert.Equal(values, RecordEncoder.Decode(encoded));

        var decodedInto = new DbValue[values.Length];
        Assert.Equal(values.Length, RecordEncoder.DecodeInto(encoded, decodedInto));
        Assert.Equal(values, decodedInto);

        var selected = new DbValue[values.Length];
        RecordEncoder.DecodeSelectedInto(encoded, selected, [1]);
        Assert.Equal(expected, selected[1]);

        var compact = new DbValue[1];
        RecordEncoder.DecodeSelectedCompactInto(encoded, compact, [1]);
        Assert.Equal(expected, compact[0]);

        Assert.Equal(expected, RecordEncoder.DecodeUpTo(encoded, 1)[1]);
        Assert.Equal(expected, RecordEncoder.DecodeColumn(encoded, 1));
        Assert.True(RecordEncoder.TryDecodeNumericColumn(
            encoded,
            1,
            out _,
            out double numeric,
            out bool isReal));
        Assert.True(isReal);
        Assert.Equal((double)expected.AsDecimal, numeric);
    }

    [Fact]
    public void RecordEncoder_UsesNineByteDecimalPayload()
    {
        DbValue value = DbValue.FromDecimalParts(-12345, 3);
        byte[] encoded = RecordEncoder.Encode([value]);

        Assert.Equal(11, encoded.Length); // count + tag + payload
        Assert.Equal((byte)DbType.Decimal, encoded[1]);
        Assert.Equal(3, encoded[^1]);
    }

    [Fact]
    public void RecordEncoder_RejectsUnknownTagsInDecodeAndSkipPaths()
    {
        byte[] malformed = [1, 0xFF];

        Assert.Throws<InvalidDataException>(() => RecordEncoder.Decode(malformed));
        Assert.Throws<InvalidDataException>(() => RecordEncoder.DecodeColumn(malformed, 0));
        Assert.Throws<InvalidDataException>(() =>
            RecordEncoder.DecodeSelectedInto(malformed, new DbValue[1], [0]));
        Assert.Throws<InvalidDataException>(() => RecordEncoder.IsColumnNull(malformed, 0));

        byte[] malformedSkippedColumn = [2, 0xFF, (byte)DbType.Null];
        Assert.Throws<InvalidDataException>(() =>
            RecordEncoder.DecodeColumn(malformedSkippedColumn, 1));
    }

    [Fact]
    public void RecordEncoder_RejectsMalformedDecimalPayload()
    {
        byte[] truncated = [1, (byte)DbType.Decimal, 0, 0, 0];
        byte[] unsupportedScale =
        [
            1,
            (byte)DbType.Decimal,
            1, 0, 0, 0, 0, 0, 0, 0,
            19,
        ];

        Assert.Throws<InvalidDataException>(() => RecordEncoder.Decode(truncated));
        Assert.Throws<InvalidDataException>(() => RecordEncoder.Decode(unsupportedScale));
    }
}
