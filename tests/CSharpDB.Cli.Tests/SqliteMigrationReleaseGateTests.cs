using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Cli.Tests;

public sealed partial class SqliteMigrationCommandRunnerTests
{
    private const int ReleaseGateRowCount = 257;

    [Fact]
    public async Task
        ReleaseGate_RetainedBackupCompletesCliLifecycleAndPreservesModerateTable()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("release-gate-source.sqlite");
        string packagePath = workspace.PathFor("release-gate.csdbsqlite");
        string catalogPath = workspace.PathFor("release-gate-catalog.json");
        string planPath = workspace.PathFor("release-gate-plan.json");
        string targetPath = workspace.PathFor("release-gate-target.csdb");
        string applyReportPath = workspace.PathFor("release-gate-run.json");

        await CreateReleaseGateDatabaseAsync(sourcePath);

        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
            ],
            inspectOutput,
            inspectError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(inspectCode, inspectError);
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));
        string manifestDigest = ReadManifestDigest(
            inspectOutput.ToString());
        byte[] retainedPackage = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);
        Assert.NotEmpty(retainedPackage);

        var planError = new StringWriter();
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            TextWriter.Null,
            planError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(planCode, planError);
        Assert.True(File.Exists(planPath));

        var previewOutput = new StringWriter();
        var previewError = new StringWriter();
        int previewCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "preview", planPath,
                "--catalog", catalogPath,
                "--format", "json",
            ],
            previewOutput,
            previewError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(previewCode, previewError);
        using (JsonDocument preview = JsonDocument.Parse(
            previewOutput.ToString()))
        {
            Assert.Equal(
                "csharpdb-migration-preview/v1",
                preview.RootElement.GetProperty("format").GetString());
        }

        File.Delete(sourcePath);

        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "apply", planPath,
                "--catalog", catalogPath,
                "--source-package", packagePath,
                "--expected-manifest-digest", manifestDigest,
                "--workspace", workspace.Root,
                "--target", targetPath,
                "--out", applyReportPath,
                "--format", "json",
            ],
            applyOutput,
            applyError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(applyCode, applyError);
        using JsonDocument applied = JsonDocument.Parse(
            applyOutput.ToString());
        long batchesWritten = applied.RootElement
            .GetProperty("batchesWritten")
            .GetInt64();
        Assert.True(batchesWritten > 0);
        Assert.Equal(
            ReleaseGateRowCount,
            applied.RootElement.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(
            "csharpdb-sqlite-backup-v1",
            applied.RootElement
                .GetProperty("sourcePackageFormat")
                .GetString());
        Assert.Equal(
            "awaiting-validation",
            await ReadReleaseGateLifecycleAsync(targetPath));

        string resumeReportPath =
            workspace.PathFor("release-gate-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "apply", planPath,
                "--catalog", catalogPath,
                "--source-package", packagePath,
                "--expected-manifest-digest", manifestDigest,
                "--workspace", workspace.Root,
                "--target", targetPath,
                "--out", resumeReportPath,
                "--resume",
                "--format", "json",
            ],
            resumeOutput,
            resumeError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(resumeCode, resumeError);
        using JsonDocument resumed = JsonDocument.Parse(
            resumeOutput.ToString());
        Assert.Equal(
            0,
            resumed.RootElement.GetProperty("batchesWritten").GetInt64());
        Assert.Equal(
            batchesWritten,
            resumed.RootElement.GetProperty("batchesSkipped").GetInt64());
        Assert.Equal(
            0,
            resumed.RootElement.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(
            ReleaseGateRowCount,
            resumed.RootElement.GetProperty("rowsSkipped").GetInt64());
        Assert.Equal(
            "awaiting-validation",
            await ReadReleaseGateLifecycleAsync(targetPath));

        string validationPath =
            workspace.PathFor("release-gate-validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", planPath,
                "--catalog", catalogPath,
                "--source-package", packagePath,
                "--expected-manifest-digest", manifestDigest,
                "--workspace", workspace.Root,
                "--target", targetPath,
                "--out", validationPath,
                "--level", "checksum",
                "--spill-dir", workspace.Root,
            ],
            validationOutput,
            validationError,
            Cancellation);

        AssertSuccessfulReleaseGateCommand(validationCode, validationError);
        Assert.Contains(
            "Activation: activated",
            validationOutput.ToString(),
            StringComparison.Ordinal);
        MigrationValidationReport validation =
            MigrationValidationReportSerializer.Deserialize(
                await File.ReadAllTextAsync(
                    validationPath,
                    Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, validation.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, validation.Level);
        Assert.Equal(
            "activated",
            await ReadReleaseGateLifecycleAsync(targetPath));
        Assert.Equal(
            retainedPackage,
            await File.ReadAllBytesAsync(packagePath, Cancellation));

        await AssertReleaseGateTargetRowsAsync(targetPath);
    }

    private static async ValueTask CreateReleaseGateDatabaseAsync(
        string path)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false,
        };
        await using var connection = new SqliteConnection(
            builder.ConnectionString);
        await connection.OpenAsync(Cancellation);

        await using (SqliteCommand create = connection.CreateCommand())
        {
            create.CommandText =
                """
                CREATE TABLE release_gate_items (
                    id INTEGER PRIMARY KEY,
                    label TEXT NOT NULL,
                    amount INTEGER NOT NULL
                );
                """;
            await create.ExecuteNonQueryAsync(Cancellation);
        }

        await using var transaction =
            await connection.BeginTransactionAsync(Cancellation);
        await using SqliteCommand insert = connection.CreateCommand();
        insert.Transaction = (SqliteTransaction)transaction;
        insert.CommandText =
            """
            INSERT INTO release_gate_items(id, label, amount)
            VALUES ($id, $label, $amount);
            """;
        SqliteParameter id = insert.Parameters.Add("$id", SqliteType.Integer);
        SqliteParameter label = insert.Parameters.Add("$label", SqliteType.Text);
        SqliteParameter amount =
            insert.Parameters.Add("$amount", SqliteType.Integer);

        for (int index = 1; index <= ReleaseGateRowCount; index++)
        {
            id.Value = index;
            label.Value = ReleaseGateLabel(index);
            amount.Value = ReleaseGateAmount(index);
            Assert.Equal(
                1,
                await insert.ExecuteNonQueryAsync(Cancellation));
        }

        await transaction.CommitAsync(Cancellation);
    }

    private static async ValueTask<string>
        ReadReleaseGateLifecycleAsync(string targetPath)
    {
        await using Database database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var result = await database.ExecuteAsync(
            """
            SELECT "lifecycle_state"
            FROM "__csharpdb_migration_state"
            WHERE "singleton" = 1;
            """,
            Cancellation);
        Assert.True(await result.MoveNextAsync(Cancellation));
        string lifecycle = result.Current[0].AsText;
        Assert.False(await result.MoveNextAsync(Cancellation));
        return lifecycle;
    }

    private static async ValueTask AssertReleaseGateTargetRowsAsync(
        string targetPath)
    {
        await using Database database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var query = await database.ExecuteAsync(
            """
            SELECT id, label, amount
            FROM release_gate_items
            ORDER BY id;
            """,
            Cancellation);
        var rows = await query.ToListAsync(Cancellation);
        Assert.Equal(ReleaseGateRowCount, rows.Count);
        for (int index = 1; index <= ReleaseGateRowCount; index++)
        {
            var row = rows[index - 1];
            Assert.Equal(index, row[0].AsInteger);
            Assert.Equal(ReleaseGateLabel(index), row[1].AsText);
            Assert.Equal(ReleaseGateAmount(index), row[2].AsInteger);
        }
    }

    private static string ReleaseGateLabel(int index) =>
        $"release-item-{index:D4}";

    private static long ReleaseGateAmount(int index) =>
        checked(index * 17L - 3L);

    private static void AssertSuccessfulReleaseGateCommand(
        int exitCode,
        StringWriter error)
    {
        Assert.True(
            exitCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            error.ToString());
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()),
            error.ToString());
    }
}
