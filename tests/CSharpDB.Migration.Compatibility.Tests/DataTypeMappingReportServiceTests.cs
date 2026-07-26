using CSharpDB.Primitives;

namespace CSharpDB.Migration.Compatibility.Tests;

public sealed class DataTypeMappingReportServiceTests
{
    [Fact]
    public async Task PreserveReport_UsesPlannerMappingPolicyAndIsDeterministic()
    {
        MigrationCatalog catalog = await InspectAsync();
        var service = new DataTypeMappingReportService();

        DataTypeMappingReport first = service.Create(catalog);
        DataTypeMappingReport second = service.Create(catalog);

        Assert.Equal(DataTypeMappingReportFormats.V1, first.Format);
        Assert.Equal(
            StandardDataTypeMappingProvider.StandardPolicyId,
            first.MappingPolicyId);
        Assert.Equal(
            first.Entries.OrderBy(
                static entry => entry.SourceObjectId,
                StringComparer.Ordinal),
            first.Entries);
        Assert.Equal(
            catalog.Objects.Count(static item => item.NativeType is not null),
            first.Summary.Total);
        Assert.Equal(
            first.Summary.Total,
            first.Summary.Exact +
            first.Summary.LosslessReencoded +
            first.Summary.Lossy +
            first.Summary.Unsupported);
        Assert.Equal(0, first.Summary.Lossy);
        Assert.True(first.Summary.Exact > 0);
        Assert.True(first.Summary.LosslessReencoded > 0);
        Assert.True(first.Summary.Unsupported > 0);

        DataTypeMappingReportEntry amount = Assert.Single(
            first.Entries,
            static item =>
                item.SourceObjectId == "syn:column:orders:amount");
        Assert.Equal("DECIMAL(38,9)", amount.SourceNativeType);
        Assert.Equal("decimal", amount.SourceLogicalType);
        Assert.Equal(DbType.Text, amount.TargetType);
        Assert.Equal(
            MigrationMappingClassification.LosslessReencoded,
            amount.Classification);
        Assert.Equal("decimal-text", amount.Conversion?.ConversionId);
        Assert.Equal(
            ["format", "precision", "scale"],
            amount.Conversion?.Parameters.Select(static item => item.Name));
        Assert.Equal(MigrationCoverageKind.Full, amount.Coverage.Kind);
        Assert.False(amount.Coverage.RequiresFullStreamValidation);

        Assert.Equal(
            CompatibilityReportFormatter.ToJson(first),
            CompatibilityReportFormatter.ToJson(second));
        Assert.Equal(
            CompatibilityReportFormatter.ToText(first),
            CompatibilityReportFormatter.ToText(second));
    }

    [Fact]
    public async Task QueryableReport_ExposesLossyDiagnosticAndCoverage()
    {
        MigrationCatalog catalog = await InspectAsync();
        DataTypeMappingReport report = new DataTypeMappingReportService().Create(
            catalog,
            new DataTypeMappingReportOptions
            {
                Profile = MigrationMappingProfile.Queryable,
            });

        DataTypeMappingReportEntry amount = Assert.Single(
            report.Entries,
            static item =>
                item.SourceObjectId == "syn:column:orders:amount");
        Assert.Equal(DbType.Real, amount.TargetType);
        Assert.Equal(
            MigrationMappingClassification.Lossy,
            amount.Classification);
        Assert.Equal("decimal-binary64", amount.Conversion?.ConversionId);
        Assert.NotNull(amount.Diagnostic);
        Assert.Equal(
            MigrationCompatibilityStatus.Conditional,
            amount.Diagnostic.Status);
        Assert.True(amount.Diagnostic.CanOverride);
        Assert.True(report.Summary.Lossy > 0);

        string json = CompatibilityReportFormatter.ToJson(report);
        string text = CompatibilityReportFormatter.ToText(report);
        Assert.Contains("\"classification\": \"lossy\"", json);
        Assert.Contains("classification: lossy", text);
        Assert.Contains(amount.Diagnostic.DiagnosticId, text);
    }

    [Fact]
    public async Task CustomReport_RequiresKnownScalarOverrides()
    {
        MigrationCatalog catalog = await InspectAsync();
        var service = new DataTypeMappingReportService();

        Assert.Throws<ArgumentException>(() =>
            service.Create(
                catalog,
                new DataTypeMappingReportOptions
                {
                    Profile = MigrationMappingProfile.Preserve,
                    CustomTargetTypes = new Dictionary<string, DbType>
                    {
                        ["syn:column:orders:amount"] = DbType.Text,
                    },
                }));

        Assert.Throws<ArgumentException>(() =>
            service.Create(
                catalog,
                new DataTypeMappingReportOptions
                {
                    Profile = MigrationMappingProfile.Custom,
                    CustomTargetTypes = new Dictionary<string, DbType>
                    {
                        ["missing:column"] = DbType.Text,
                    },
                }));

        DataTypeMappingReport custom = service.Create(
            catalog,
            new DataTypeMappingReportOptions
            {
                Profile = MigrationMappingProfile.Custom,
                CustomTargetTypes = new Dictionary<string, DbType>
                {
                    ["syn:column:orders:source-counter"] = DbType.Real,
                },
            });
        DataTypeMappingReportEntry counter = Assert.Single(
            custom.Entries,
            static item =>
                item.SourceObjectId ==
                "syn:column:orders:source-counter");
        Assert.Equal(DbType.Real, counter.RequestedTargetType);
        Assert.Equal(
            MigrationMappingClassification.Lossy,
            counter.Classification);
    }

    private static async Task<MigrationCatalog> InspectAsync() =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });
}
