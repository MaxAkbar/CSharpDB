using System.Globalization;
using CSharpDB.Migration;
using CSharpDB.Migration.MySql;
using MySqlConnector;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlScalarCodecTests
{
    [Theory]
    [InlineData("tinyint", false, "SignedInteger")]
    [InlineData("tinyint", true, "UnsignedInteger")]
    [InlineData("bigint", false, "SignedInteger")]
    [InlineData("bigint", true, "UnsignedInteger")]
    [InlineData("decimal", false, "Decimal")]
    [InlineData("float", false, "Binary32")]
    [InlineData("double", false, "Binary64")]
    [InlineData("varchar", false, "Text")]
    [InlineData("longblob", false, "Binary")]
    [InlineData("date", false, "Date")]
    [InlineData("datetime", false, "DateTime")]
    public void SupportedTypesResolveToExactCodecs(
        string dataType,
        bool unsigned,
        string expected)
    {
        Assert.True(
            MySqlScalarCodec.TryResolve(
                dataType,
                unsigned,
                out MySqlScalarCodecKind actual));
        Assert.Equal(expected, actual.ToString());
    }

    [Fact]
    public void IntegerProjectionPreservesExactSignedAndUnsignedRanges()
    {
        Assert.Equal(
            long.MinValue.ToString(
                CultureInfo.InvariantCulture),
            Project(
                MySqlScalarCodecKind.SignedInteger,
                long.MinValue));
        Assert.Equal(
            long.MaxValue.ToString(
                CultureInfo.InvariantCulture),
            Project(
                MySqlScalarCodecKind.SignedInteger,
                long.MaxValue));
        Assert.Equal(
            ulong.MaxValue.ToString(
                CultureInfo.InvariantCulture),
            Project(
                MySqlScalarCodecKind.UnsignedInteger,
                ulong.MaxValue));
        Assert.Equal(
            "1",
            Project(
                MySqlScalarCodecKind.SignedInteger,
                (sbyte)1));
    }

    [Fact]
    public void DecimalProjectionSupportsMysqlSixtyFiveThirtyAndCanonicalizes()
    {
        const string maximum =
            "12345678901234567890123456789012345." +
            "123456789012345678901234567891";

        Assert.Equal(
            maximum,
            MySqlScalarCodec.CanonicalDecimal(
                maximum));
        Assert.Equal(
            "-12.34",
            MySqlScalarCodec.CanonicalDecimal(
                "-00012.34000"));
        Assert.Equal(
            "0",
            MySqlScalarCodec.CanonicalDecimal(
                "-000.000"));
        Assert.Equal(
            maximum,
            Project(
                MySqlScalarCodecKind.Decimal,
                maximum));
    }

    [Theory]
    [InlineData("123456789012345678901234567890123456789012345678901234567890123456")]
    [InlineData("1.1234567890123456789012345678901")]
    [InlineData("+1")]
    [InlineData("1e2")]
    [InlineData(".1")]
    [InlineData("1.")]
    [InlineData("--1")]
    public void DecimalProjectionRejectsOutOfContractText(
        string value)
    {
        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec
                .CanonicalDecimal(value));
    }

    [Fact]
    public void Binary32ProjectionEncodesExactWidenedValue()
    {
        const float source = 0.1f;
        string expected =
            ((double)source).ToString(
                "R",
                CultureInfo.InvariantCulture);
        string text = Project(
            MySqlScalarCodecKind.Binary32,
            source);

        Assert.Equal(
            "0.10000000149011612",
            expected);
        Assert.Equal(expected, text);
        Assert.Equal(
            (double)source,
            double.Parse(
                text,
                CultureInfo.InvariantCulture));
    }

    [Fact]
    public void FloatingPointProjectionRejectsNonfiniteValues()
    {
        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Binary32,
                float.PositiveInfinity,
                100));
        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Binary64,
                double.NaN,
                100));
    }

    [Fact]
    public void TextAndBinaryProjectionAreStrictAndBounded()
    {
        MySqlProjectedScalar text =
            MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Text,
                "snowman \u2603",
                100);
        MySqlProjectedScalar binary =
            MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Binary,
                new byte[] { 0, 1, 255 },
                100);

        Assert.Equal(11, text.PayloadBytes);
        Assert.Equal(3, binary.PayloadBytes);
        Assert.Equal(
            new byte[] { 0, 1, 255 },
            binary.Value.BinaryValue.ToArray());
        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Text,
                "invalid\uD800text",
                100));
        Assert.Throws<MySqlRetainedCaptureLimitException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Binary,
                new byte[] { 0, 1, 2 },
                2));
    }

    [Fact]
    public void ValidDateAndDatetimeUseCanonicalWallClockText()
    {
        var date =
            new MySqlDateTime(
                2026,
                7,
                25,
                0,
                0,
                0,
                0);
        var dateTime =
            new MySqlDateTime(
                2026,
                7,
                25,
                14,
                3,
                2,
                123_456);

        Assert.Equal(
            "2026-07-25",
            Project(
                MySqlScalarCodecKind.Date,
                date));
        Assert.Equal(
            "2026-07-25 14:03:02.123456",
            Project(
                MySqlScalarCodecKind.DateTime,
                dateTime));
    }

    [Theory]
    [InlineData(0, 0, 0)]
    [InlineData(2026, 0, 25)]
    [InlineData(2026, 7, 0)]
    public void ZeroOrPartialDatesAreRejected(
        int year,
        int month,
        int day)
    {
        var value = new MySqlDateTime(
            year,
            month,
            day,
            0,
            0,
            0,
            0);

        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Date,
                value,
                100));
    }

    [Fact]
    public void DateCodecRejectsUnexpectedTimeComponents()
    {
        var value = new MySqlDateTime(
            2026,
            7,
            25,
            1,
            0,
            0,
            0);

        Assert.Throws<MySqlMigrationException>(
            () => MySqlScalarCodec.Project(
                MySqlScalarCodecKind.Date,
                value,
                100));
    }

    private static string Project(
        MySqlScalarCodecKind codec,
        object value) =>
        MySqlScalarCodec.Project(
                codec,
                value,
                1024)
            .Value.CanonicalText ??
        throw new InvalidOperationException(
            "Expected a retained text scalar.");
}
