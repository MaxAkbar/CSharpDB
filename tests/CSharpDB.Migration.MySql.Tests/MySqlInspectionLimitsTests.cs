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
                MaxTableDefinitions =
                    MySqlInspectionLimits.MaximumTableDefinitions + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxKeys = MySqlInspectionLimits.MaximumKeys + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxKeyColumns =
                    MySqlInspectionLimits.MaximumKeyColumns + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxForeignKeys =
                    MySqlInspectionLimits.MaximumForeignKeys + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxForeignKeyColumns =
                    MySqlInspectionLimits.MaximumForeignKeyColumns + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxChecks = MySqlInspectionLimits.MaximumChecks + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxIndexes = MySqlInspectionLimits.MaximumIndexes + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxIndexParts =
                    MySqlInspectionLimits.MaximumIndexParts + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxViews = MySqlInspectionLimits.MaximumViews + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxStructuralRowsTotal =
                    MySqlInspectionLimits.MaximumStructuralRowsTotal + 1,
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
                MaxDefinitionBytes =
                    MySqlInspectionLimits.MaximumDefinitionBytes + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxDefinitionBytesTotal =
                    MySqlInspectionLimits.MaximumDefinitionBytesTotal + 1,
            }).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(
            () => (MySqlInspectionLimits.Default with
            {
                MaxMetadataBytes =
                    MySqlInspectionLimits.MaximumMetadataBytes + 1,
            }).Validate());
    }
}
