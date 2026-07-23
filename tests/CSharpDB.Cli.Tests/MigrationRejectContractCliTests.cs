using System.Text.Json;
using CSharpDB.Migration;

namespace CSharpDB.Cli.Tests;

public sealed class MigrationRejectContractCliTests
{
    [Fact]
    public async Task Apply_DurableRejectModeFailsBeforeTargetCreationWithStableReport()
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

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(MigrationRejectContract.DeterministicFailFastV1, error.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(targetPath));
            Assert.False(File.Exists(targetPath + ".wal"));
            Assert.False(File.Exists(targetPath + ".migration.lock"));

            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(reportPath, ct));
            JsonElement root = report.RootElement;
            Assert.Equal("csharpdb-migration-run/v1", root.GetProperty("format").GetString());
            Assert.Equal("failed", root.GetProperty("status").GetString());
            Assert.Equal("MIG-APPLY-POLICY-REJECT-001", root.GetProperty("errorCode").GetString());
            Assert.Equal(
                MigrationRejectContract.DeterministicFailFastV1,
                root.GetProperty("rejectContractVersion").GetString());
            Assert.Equal(0, root.GetProperty("rejectedRows").GetInt32());
            Assert.False(root.TryGetProperty("firstRejectedRow", out _));
            Assert.False(root.TryGetProperty("message", out _));
            Assert.False(root.TryGetProperty("targetPath", out _));
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
