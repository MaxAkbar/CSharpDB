using CSharpDB.Migration;

namespace CSharpDB.Cli.Tests;

public sealed class MigrationRejectContractCliTests
{
    private const string CsvOnlyExecutionMessage =
        "Deterministic-reject CLI execution is supported only for retained CSV migrations.";

    [Fact]
    public async Task Apply_CraftedDeterministicSyntheticPlanFailsClosedAsCsvOnlyUsage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_reject_cli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        string targetPath = Path.Combine(directory, "staged.csdb");
        string reportPath = Path.Combine(directory, "run.json");

        try
        {
            MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                    IncludeProfile = true,
                },
                ct);
            MigrationPlan ready = new MigrationPlanner().CreatePlan(
                catalog,
                new MigrationPlanningOptions { AcceptAllExclusions = true });
            MigrationPlan unsupported = ready with
            {
                Load = ready.Load with
                {
                    RejectMode = MigrationRejectMode.DeterministicRejects,
                    RejectPolicy = ValidDeterministicRejectPolicy(),
                },
            };
            await File.WriteAllTextAsync(
                catalogPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            await File.WriteAllTextAsync(
                planPath,
                MigrationArtifactSerializer.SerializePlan(unsupported, catalog),
                ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", reportPath,
                    "--format", "json",
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, exitCode);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(CsvOnlyExecutionMessage, error.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(targetPath));
            Assert.False(File.Exists(targetPath + ".wal"));
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    [Fact]
    public async Task Validate_CraftedDeterministicSyntheticPlanFailsClosedAsCsvOnlyUsage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_reject_validate_cli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        string targetPath = Path.Combine(directory, "missing-staged.csdb");
        string reportPath = Path.Combine(directory, "validation.json");

        try
        {
            MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                    IncludeProfile = true,
                },
                ct);
            MigrationPlan ready = new MigrationPlanner().CreatePlan(
                catalog,
                new MigrationPlanningOptions { AcceptAllExclusions = true });
            MigrationPlan unsupported = ready with
            {
                Load = ready.Load with
                {
                    RejectMode = MigrationRejectMode.DeterministicRejects,
                    RejectPolicy = ValidDeterministicRejectPolicy(),
                },
            };
            await File.WriteAllTextAsync(
                catalogPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);
            await File.WriteAllTextAsync(
                planPath,
                MigrationArtifactSerializer.SerializePlan(unsupported, catalog),
                ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "validate", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", reportPath,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, exitCode);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(CsvOnlyExecutionMessage, error.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(targetPath));
            Assert.False(File.Exists(targetPath + ".wal"));
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    private static MigrationDeterministicRejectPolicy ValidDeterministicRejectPolicy() => new()
    {
        ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
        AllowedRuleIds = ["MIG-TEST-001"],
        MaxRejectedRowsPerBatch = 1,
        MaxRejectedRowsPerRun = 10,
        MaxRawValueBytes = 1_024,
        MaxRawValueBytesPerBatch = 8_192,
        MaxRawValueBytesPerRun = 65_536,
        MaxArtifactBytes = 131_072,
    };
}
