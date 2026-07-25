using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlInspectionLimitsTests
{
    [Fact]
    public void DefaultsAreValidAndCallersCannotRaiseQualifiedCeilings()
    {
        MySqlInspectionLimits.Default.Validate();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with { MaxTables = 0 }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxColumns = MySqlInspectionLimits.MaximumColumns + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxViews = MySqlInspectionLimits.MaximumViews + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxNameBytes = MySqlInspectionLimits.MaximumNameBytes + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxColumnTypeBytes =
                    MySqlInspectionLimits.MaximumColumnTypeBytes + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxExpressionBytes =
                    MySqlInspectionLimits.MaximumExpressionBytes + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxExpressionBytesTotal =
                    MySqlInspectionLimits.MaximumExpressionBytesTotal + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxMetadataBytes =
                    MySqlInspectionLimits.MaximumMetadataBytes + 1,
            }).Validate());
    }
}
