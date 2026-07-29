using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class SqlLiteralRulesTests
{
    [Theory]
    [InlineData(7d, "7.0")]
    [InlineData(0.00001d, "0.00001")]
    [InlineData(
        1.2345678901234568E+29d,
        "123456789012345680000000000000.0")]
    public void FormatReal_UsesCanonicalExponentFreeSql(
        double value,
        string expected)
    {
        string sql = SqlLiteralRules.FormatReal(value);

        Assert.Equal(expected, sql);
        Assert.DoesNotContain("E", sql, StringComparison.OrdinalIgnoreCase);
        double reparsed = double.Parse(
            sql,
            NumberStyles.AllowLeadingSign |
            NumberStyles.AllowDecimalPoint,
            CultureInfo.InvariantCulture);
        Assert.Equal(
            BitConverter.DoubleToInt64Bits(value),
            BitConverter.DoubleToInt64Bits(reparsed));
    }

    [Fact]
    public void FormatReal_PreservesNegativeZeroAsReal()
    {
        double negativeZero =
            BitConverter.Int64BitsToDouble(long.MinValue);

        string sql = SqlLiteralRules.FormatReal(negativeZero);

        Assert.Equal("-0.0", sql);
        Assert.Equal(
            long.MinValue,
            BitConverter.DoubleToInt64Bits(
                double.Parse(
                    sql,
                    NumberStyles.AllowLeadingSign |
                    NumberStyles.AllowDecimalPoint,
                    CultureInfo.InvariantCulture)));
    }

    [Fact]
    public void FormatReal_RoundTripsFiniteExtremesWithoutExponent()
    {
        double[] values =
        [
            double.Epsilon,
            -double.Epsilon,
            double.MaxValue,
            double.MinValue,
        ];

        foreach (double value in values)
        {
            string sql = SqlLiteralRules.FormatReal(value);

            Assert.Contains(".", sql, StringComparison.Ordinal);
            Assert.DoesNotContain(
                "E",
                sql,
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                BitConverter.DoubleToInt64Bits(value),
                BitConverter.DoubleToInt64Bits(
                    double.Parse(
                        sql,
                        NumberStyles.AllowLeadingSign |
                        NumberStyles.AllowDecimalPoint,
                        CultureInfo.InvariantCulture)));
        }
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void FormatReal_RejectsNonFiniteValues(double value)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => SqlLiteralRules.FormatReal(value));
    }
}
