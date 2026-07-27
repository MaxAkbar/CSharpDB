using System.Data.SqlTypes;
using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerScalarCodecTests
{
    [Fact]
    public void DecimalProjectionPreservesPrecisionThirtyEight()
    {
        const string expected =
            "1234567890123456789012345678901234.5678";
        SqlDecimal value = SqlDecimal.Parse(expected);

        SqlServerProjectedScalar projected =
            SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Decimal,
                value,
                1024);

        Assert.Equal(
            MigrationSourceValueKind.Decimal,
            projected.Value.Kind);
        Assert.Equal(
            expected,
            projected.Value.CanonicalText);
        Assert.Equal(
            expected,
            SqlServerScalarCodec.FormatSqlDecimal(
                value));
    }

    [Fact]
    public void BinaryWidthsAndCanonicalScalarKindsAreExplicit()
    {
        Assert.True(
            SqlServerScalarCodec.TryResolve(
                "real",
                24,
                out SqlServerScalarCodecKind real,
                out int? realWidth));
        Assert.Equal(
            SqlServerScalarCodecKind.Binary32,
            real);
        Assert.Equal(32, realWidth);
        Assert.True(
            SqlServerScalarCodec.TryResolve(
                "float",
                53,
                out SqlServerScalarCodecKind float53,
                out int? floatWidth));
        Assert.Equal(
            SqlServerScalarCodecKind.Binary64,
            float53);
        Assert.Equal(64, floatWidth);

        SqlServerProjectedScalar binary32 =
            SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Binary32,
                (double)1.25f,
                1024);
        Assert.Equal(
            MigrationSourceValueKind.FloatingPoint,
            binary32.Value.Kind);
        Assert.Equal(
            "1.25",
            binary32.Value.CanonicalText);

        SqlServerProjectedScalar widenedBinary32 =
            SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Binary32,
                0.1f,
                1024);
        Assert.Equal(
            ((double)0.1f).ToString(
                "R",
                System.Globalization.CultureInfo.InvariantCulture),
            widenedBinary32.Value.CanonicalText);
        Assert.NotEqual(
            0.1f.ToString(
                "R",
                System.Globalization.CultureInfo.InvariantCulture),
            widenedBinary32.Value.CanonicalText);

        byte[] bytes = [0x00, 0xff, 0x2a];
        SqlServerProjectedScalar binary =
            SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Binary,
                bytes,
                1024);
        Assert.Equal(
            MigrationSourceValueKind.Binary,
            binary.Value.Kind);
        Assert.Equal(
            bytes,
            binary.Value.BinaryValue.ToArray());

        Guid guid =
            Guid.Parse(
                "00112233-4455-6677-8899-aabbccddeeff");
        Assert.Equal(
            "00112233-4455-6677-8899-aabbccddeeff",
            SqlServerScalarCodec.Project(
                    SqlServerScalarCodecKind.Guid,
                    guid,
                    1024)
                .Value.CanonicalText);
        Assert.Equal(
            "2026-07-25",
            SqlServerScalarCodec.Project(
                    SqlServerScalarCodecKind.Date,
                    new DateTime(
                        2026,
                        7,
                        25,
                        0,
                        0,
                        0,
                        DateTimeKind.Unspecified),
                    1024)
                .Value.CanonicalText);
    }

    [Fact]
    public void ProjectionFailsClosedForInvalidTextNonfiniteAndBounds()
    {
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Text,
                "invalid\uD800text",
                1024));
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Binary64,
                double.PositiveInfinity,
                1024));
        Assert.Throws<
            SqlServerRetainedCaptureLimitException>(
            () => SqlServerScalarCodec.Project(
                SqlServerScalarCodecKind.Text,
                "too large",
                2));
    }
}
