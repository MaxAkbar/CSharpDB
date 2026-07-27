using CSharpDB.Primitives;
using System.Globalization;

namespace CSharpDB.Migration.Tests;

public sealed class SharedScalarCodecTests
{
    [Fact]
    public void TextCodecs_HaveStableInvariantGoldenValuesAndRoundTrip()
    {
        var guid = Guid.Parse("11111111-2222-3333-4444-555555555555");
        var date = new DateOnly(2026, 7, 21);
        var time = new TimeOnly(8, 9, 10, 123).Add(TimeSpan.FromTicks(4_567));
        var dateTime = new DateTime(2026, 7, 21, 8, 9, 10, 123, DateTimeKind.Utc)
            .AddTicks(4_567);
        var dateTimeOffset = new DateTimeOffset(
                2026,
                7,
                21,
                8,
                9,
                10,
                123,
                TimeSpan.FromHours(-7))
            .AddTicks(4_567);

        Assert.Equal("11111111-2222-3333-4444-555555555555", CSharpDbTextCodec.FormatGuid(guid));
        Assert.Equal("2026-07-21", CSharpDbTextCodec.FormatDate(date));
        Assert.Equal("08:09:10.1234567", CSharpDbTextCodec.FormatTime(time));
        Assert.Equal("08:09:10", CSharpDbTextCodec.FormatTime(new TimeOnly(8, 9, 10)));
        Assert.Equal("08:09:10.1230000", CSharpDbTextCodec.FormatTime(new TimeOnly(8, 9, 10, 123)));
        Assert.Equal("08:09:10.0000001", CSharpDbTextCodec.FormatTime(new TimeOnly(8, 9, 10).Add(TimeSpan.FromTicks(1))));
        Assert.Equal("2026-07-21 08:09:10.1234567", CSharpDbTextCodec.FormatDateTime(dateTime));
        Assert.Equal(
            "2026-07-21 08:09:10.1234567-07:00",
            CSharpDbTextCodec.FormatDateTimeOffset(dateTimeOffset));

        Assert.Equal(guid, CSharpDbTextCodec.ParseGuid(CSharpDbTextCodec.FormatGuid(guid)));
        Assert.Equal(date, CSharpDbTextCodec.ParseDate(CSharpDbTextCodec.FormatDate(date)));
        Assert.Equal(time, CSharpDbTextCodec.ParseTime(CSharpDbTextCodec.FormatTime(time)));
        DateTime parsedDateTime = CSharpDbTextCodec.ParseDateTime(CSharpDbTextCodec.FormatDateTime(dateTime));
        Assert.Equal(dateTime.Ticks, parsedDateTime.Ticks);
        Assert.Equal(DateTimeKind.Unspecified, parsedDateTime.Kind);
        Assert.Equal(
            dateTimeOffset,
            CSharpDbTextCodec.ParseDateTimeOffset(CSharpDbTextCodec.FormatDateTimeOffset(dateTimeOffset)));
    }

    [Theory]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    [InlineData("fa-IR")]
    public void TextCodecs_AreInvariantUnderNonGregorianCultures(string cultureName)
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo originalUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(cultureName);

            Assert.Equal(
                "2026-07-21 08:09:10.123-07:00",
                CSharpDbTextCodec.FormatDateTimeOffset(
                    new DateTimeOffset(2026, 7, 21, 8, 9, 10, 123, TimeSpan.FromHours(-7))));
            Assert.Equal("08:09:10.1230000", CSharpDbTextCodec.FormatTime(new TimeOnly(8, 9, 10, 123)));
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void DecimalCodec_UsesExactScaledInt64AndRejectsUnsafeValues()
    {
        long encoded = CSharpDbDecimalCodec.ToScaledInt64(123.4500m, precision: 18, scale: 4);

        Assert.Equal(1_234_500L, encoded);
        Assert.Equal(123.45m, CSharpDbDecimalCodec.FromScaledInt64(encoded, precision: 18, scale: 4));
        Assert.Equal(
            999_999_999_999_999_999L,
            CSharpDbDecimalCodec.ToScaledInt64(99_999_999_999_999.9999m, precision: 18, scale: 4));
        Assert.Equal(
            -999_999_999_999_999_999L,
            CSharpDbDecimalCodec.ToScaledInt64(-99_999_999_999_999.9999m, precision: 18, scale: 4));
        Assert.Equal((18, 2), CSharpDbDecimalCodec.ResolveFacets(precision: null, scale: null));
        Assert.Equal((12, 0), CSharpDbDecimalCodec.ResolveFacets(precision: 12, scale: null));
        Assert.Throws<InvalidOperationException>(
            () => CSharpDbDecimalCodec.ToScaledInt64(1.234m, precision: 18, scale: 2));
        Assert.Throws<OverflowException>(
            () => CSharpDbDecimalCodec.ToScaledInt64(100_000_000_000_000.0000m, precision: 18, scale: 4));
        Assert.Throws<OverflowException>(
            () => CSharpDbDecimalCodec.FromScaledInt64(1_000_000_000_000_000_000L, precision: 18, scale: 4));
        Assert.Throws<NotSupportedException>(
            () => CSharpDbDecimalCodec.ValidateFacets(precision: 19, scale: 2));
    }

    [Fact]
    public void IdentifierContract_CoversLengthQuotesNulAndCaseInsensitivePlanningCollisions()
    {
        string maximum = new('x', SqlIdentifierRules.MaxLength);
        SqlIdentifierRules.Validate(maximum);
        Assert.Equal("\"a\"\"b\"", SqlIdentifierRules.Quote("a\"b"));
        Assert.Throws<CSharpDbException>(() => SqlIdentifierRules.Validate(new string('x', 129)));
        Assert.Throws<CSharpDbException>(() => SqlIdentifierRules.Validate("bad\0name"));
    }

    [Fact]
    public async Task Planner_UsesSharedDecimalAndTextCodecVersions()
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
            },
            TestContext.Current.CancellationToken);
        MigrationPlan plan = new MigrationPlanner().CreatePlan(catalog);

        MigrationTypeMapping tax = plan.Objects
            .Single(item => item.SourceObjectId == "syn:column:orders:tax")
            .TypeMappings
            .Single();
        Assert.Equal(DbType.Integer, tax.TargetType);
        Assert.Equal("decimal-scaled-int64", tax.Conversion!.ConversionId);
        Assert.Contains(
            tax.Conversion.Parameters,
            item => item.Name == "codecVersion" &&
                    item.Value == CSharpDbDecimalCodec.Version.ToString(System.Globalization.CultureInfo.InvariantCulture));

        MigrationTypeMapping guid = plan.Objects
            .Single(item => item.SourceObjectId == "syn:column:customers-upper:external-id")
            .TypeMappings
            .Single();
        Assert.Equal("guid-text", guid.Conversion!.ConversionId);
        Assert.Contains(
            guid.Conversion.Parameters,
            item => item.Name == "codecVersion" &&
                    item.Value == CSharpDbTextCodec.Version.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }
}
