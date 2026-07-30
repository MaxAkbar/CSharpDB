using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class MigrationDdlPreviewCommandRunnerTests
{
    private const string CollectionActionPrefix =
        "csharpdb-migration-json-collection-action/v1:";

    [Fact]
    public async Task DefaultJsonPreview_RemainsDeterministicAndDoesNotExposeDdl()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);

            (int firstCode, string first, string firstError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--format", "json"],
                    ct);
            (int repeatedCode, string repeated, string repeatedError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--format", "json"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, firstCode);
            Assert.Equal(firstCode, repeatedCode);
            Assert.Equal(first, repeated);
            string jsonDigest = Sha256(first);
            Assert.True(
                string.Equals(
                    "c5076ffa7a6b35bf04e17be9ece68d93c7c319a15963dd001b3f250c4bc7d94c",
                    jsonDigest,
                    StringComparison.Ordinal),
                $"JSON preview digest: {jsonDigest}");
            Assert.True(string.IsNullOrWhiteSpace(firstError));
            Assert.True(string.IsNullOrWhiteSpace(repeatedError));

            using JsonDocument document = JsonDocument.Parse(first);
            JsonElement root = document.RootElement;
            Assert.Equal(
                "csharpdb-migration-preview/v1",
                root.GetProperty("format").GetString());
            Assert.Equal(
                [
                    "blockingDiagnosticIds",
                    "diagnostics",
                    "excludedObjects",
                    "format",
                    "mappingProfile",
                    "mappings",
                    "objects",
                    "pendingDiagnosticIds",
                    "pendingExclusionObjectIds",
                    "status",
                    "targetCSharpDbVersion",
                ],
                root.EnumerateObject()
                    .Select(property => property.Name)
                    .Order(StringComparer.Ordinal)
                    .ToArray());
            Assert.False(root.TryGetProperty("generatedDdlDigest", out _));
            Assert.False(root.TryGetProperty("stages", out _));
            Assert.DoesNotContain(
                "CREATE TABLE",
                first,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                CollectionActionPrefix,
                first,
                StringComparison.Ordinal);

            (int textCode, string text, string textError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    [],
                    ct);
            Assert.Equal(InspectorCommandRunner.ExitWarn, textCode);
            string textDigest = Sha256(text);
            Assert.True(
                string.Equals(
                    "0a5c6952d88688e4057a44af806664e39f6c5b8613fec23f61fbadf60f389dbc",
                    textDigest,
                    StringComparison.Ordinal),
                $"Text preview digest: {textDigest}");
            Assert.Contains(
                "Status: REVIEW REQUIRED",
                text,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "Generated DDL digest:",
                text,
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(textError));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task DdlPreview_IsExplicitTypedDeterministicAndNeverEmitsSentinel()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);

            (int firstCode, string first, string firstError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--ddl", "--format", "json"],
                    ct);
            (int repeatedCode, string repeated, string repeatedError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--ddl", "--format", "json"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, firstCode);
            Assert.Equal(firstCode, repeatedCode);
            Assert.Equal(first, repeated);
            Assert.True(string.IsNullOrWhiteSpace(firstError));
            Assert.True(string.IsNullOrWhiteSpace(repeatedError));
            Assert.DoesNotContain(
                CollectionActionPrefix,
                first,
                StringComparison.Ordinal);

            using JsonDocument document = JsonDocument.Parse(first);
            JsonElement root = document.RootElement;
            Assert.Equal(
                "csharpdb-ddl-preview/v1",
                root.GetProperty("format").GetString());
            Assert.Equal(5, root.GetProperty("stages").GetArrayLength());
            Assert.Matches(
                "^[0-9a-f]{64}$",
                root.GetProperty("generatedDdlDigest").GetString());
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(
                    await File.ReadAllTextAsync(catalogPath, ct));
            MigrationPlan plan =
                MigrationArtifactSerializer.DeserializePlan(
                    await File.ReadAllTextAsync(planPath, ct),
                    catalog);
            Assert.Equal(
                plan.GeneratedDdlDigest,
                root.GetProperty("generatedDdlDigest").GetString());
            Assert.Contains(
                root.GetProperty("stages")
                    .EnumerateArray()
                    .SelectMany(stage =>
                        stage.GetProperty("actions").EnumerateArray()),
                action =>
                    action.GetProperty("kind").GetString() == "sql" &&
                    action.GetProperty("sql").GetString()!
                        .Contains(
                            "CREATE TABLE",
                            StringComparison.OrdinalIgnoreCase));

            (int textCode, string text, string textError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--ddl"],
                    ct);
            Assert.Equal(InspectorCommandRunner.ExitWarn, textCode);
            Assert.Contains(
                "Format: csharpdb-ddl-preview/v1",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "Action 0: sql",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "CREATE TABLE",
                text,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                CollectionActionPrefix,
                text,
                StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(textError));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Theory]
    [InlineData("--ddl", "csharpdb-ddl-preview/v1")]
    [InlineData(
        "--scratch",
        "csharpdb-ddl-scratch-validation/v1")]
    public async Task ExplicitPreview_LegacyUnsealedPlanRemainsReadable(
        string mode,
        string expectedFormat)
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(
                    await File.ReadAllTextAsync(catalogPath, ct));
            MigrationPlan sealedPlan =
                MigrationArtifactSerializer.DeserializePlan(
                    await File.ReadAllTextAsync(planPath, ct),
                    catalog);
            Assert.NotNull(sealedPlan.GeneratedDdlDigest);
            MigrationPlan legacyPlan = sealedPlan with
            {
                GeneratedDdlDigest = null,
            };
            await File.WriteAllTextAsync(
                planPath,
                MigrationArtifactSerializer.SerializePlan(
                    legacyPlan,
                    catalog),
                ct);

            (int code, string output, string error) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    [mode, "--format", "json"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            using JsonDocument document = JsonDocument.Parse(output);
            Assert.Equal(
                expectedFormat,
                document.RootElement.GetProperty("format").GetString());
            Assert.Matches(
                "^[0-9a-f]{64}$",
                document.RootElement
                    .GetProperty("generatedDdlDigest")
                    .GetString());
            Assert.True(string.IsNullOrWhiteSpace(error));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task DdlPreview_RejectsTamperedAttachedDigestBeforeOutput()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(
                    await File.ReadAllTextAsync(catalogPath, ct));
            MigrationPlan plan =
                MigrationArtifactSerializer.DeserializePlan(
                    await File.ReadAllTextAsync(planPath, ct),
                    catalog);
            Assert.NotNull(plan.GeneratedDdlDigest);
            string tamperedDigest =
                string.Equals(
                    plan.GeneratedDdlDigest,
                    new string('0', 64),
                    StringComparison.Ordinal)
                    ? new string('1', 64)
                    : new string('0', 64);
            MigrationPlan tampered = plan with
            {
                GeneratedDdlDigest = tamperedDigest,
            };
            await File.WriteAllTextAsync(
                planPath,
                MigrationArtifactSerializer.SerializePlan(
                    tampered,
                    catalog),
                ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    "--ddl",
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "CREATE TABLE",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task DdlPreview_RejectsMismatchedRenderedBindingBeforeOutput()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            MigrationCatalog catalog =
                MigrationArtifactSerializer.DeserializeCatalog(
                    await File.ReadAllTextAsync(catalogPath, ct));
            MigrationPlan plan =
                MigrationArtifactSerializer.DeserializePlan(
                    await File.ReadAllTextAsync(planPath, ct),
                    catalog);
            CSharpDbDdlPreview preview =
                CSharpDbDdlPreviewBuilder.BuildBounded(
                    plan,
                    catalog,
                    cancellationToken: ct);
            string mismatchedDigest =
                string.Equals(
                    preview.GeneratedDdlDigest,
                    new string('0', 64),
                    StringComparison.Ordinal)
                    ? new string('1', 64)
                    : new string('0', 64);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    BuildCSharpDbDdlPreview =
                        (_, _, _) => preview with
                        {
                            GeneratedDdlDigest = mismatchedDigest,
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    "--ddl",
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task ScratchPreview_IsSanitizedAndDoesNotPromotePlanReadiness()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateBlockedSqlServerArtifactsAsync(directory, ct);

            (int firstCode, string first, string firstError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch", "--format", "json"],
                    ct);
            (int repeatedCode, string repeated, string repeatedError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch", "--format", "json"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, firstCode);
            Assert.Equal(firstCode, repeatedCode);
            Assert.Equal(first, repeated);
            Assert.True(string.IsNullOrWhiteSpace(firstError));
            Assert.True(string.IsNullOrWhiteSpace(repeatedError));
            AssertScratchOutputIsSanitized(first);

            using JsonDocument document = JsonDocument.Parse(first);
            JsonElement root = document.RootElement;
            Assert.Equal(
                "csharpdb-ddl-scratch-validation/v1",
                root.GetProperty("format").GetString());
            Assert.Equal("passed", root.GetProperty("status").GetString());
            Assert.Equal(
                "scratchExecuted",
                root.GetProperty("highestEvidence").GetString());
            Assert.Equal(
                "blocked",
                root.GetProperty("readinessStatus").GetString());
            Assert.Equal(
                "csharpdb.scratch.schema.equal",
                root.GetProperty("ruleId").GetString());

            (int textCode, string text, string textError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch"],
                    ct);
            Assert.Equal(InspectorCommandRunner.ExitWarn, textCode);
            Assert.Contains(
                "Format: csharpdb-ddl-scratch-validation/v1",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "Status: passed",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                "Readiness: blocked",
                text,
                StringComparison.Ordinal);
            AssertScratchOutputIsSanitized(text);
            Assert.True(string.IsNullOrWhiteSpace(textError));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task ScratchPreview_ReadyPassReturnsOk()
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateCatalogArtifactsAsync(
                    ScratchTableCatalog(),
                    directory,
                    "ready",
                    InspectorCommandRunner.ExitOk,
                    ct);

            (int code, string output, string error) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch", "--format", "json"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, code);
            using JsonDocument document = JsonDocument.Parse(output);
            Assert.Equal(
                "passed",
                document.RootElement.GetProperty("status").GetString());
            Assert.Equal(
                "ready",
                document.RootElement
                    .GetProperty("readinessStatus")
                    .GetString());
            AssertScratchOutputIsSanitized(output);
            Assert.True(string.IsNullOrWhiteSpace(error));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Theory]
    [InlineData(
        "different",
        "private difference table 7412",
        "private difference check 7412",
        "value = 1 ",
        "different",
        "csharpdb.scratch.schema.different")]
    [InlineData(
        "rejected",
        "private rejected table 9182",
        "private rejected check 9182",
        "private_engine_column_9182 = 1",
        "rejected",
        "csharpdb.scratch.sql.execute")]
    public async Task ScratchPreview_FailureReturnsErrorAndSanitizedEvidence(
        string fixtureId,
        string privateTableName,
        string privateCheckName,
        string privateTargetSql,
        string expectedStatus,
        string expectedRule)
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateCatalogArtifactsAsync(
                    ScratchCheckCatalog(
                        fixtureId,
                        privateTableName,
                        privateCheckName,
                        privateTargetSql),
                    directory,
                    fixtureId,
                    InspectorCommandRunner.ExitOk,
                    ct);

            (int jsonCode, string json, string jsonError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch", "--format", "json"],
                    ct);
            (int textCode, string text, string textError) =
                await RunPreviewAsync(
                    planPath,
                    catalogPath,
                    ["--scratch"],
                    ct);

            Assert.Equal(InspectorCommandRunner.ExitError, jsonCode);
            Assert.Equal(InspectorCommandRunner.ExitError, textCode);
            using JsonDocument document = JsonDocument.Parse(json);
            Assert.Equal(
                expectedStatus,
                document.RootElement.GetProperty("status").GetString());
            Assert.Equal(
                expectedRule,
                document.RootElement.GetProperty("ruleId").GetString());
            Assert.Contains(
                $"Status: {expectedStatus}",
                text,
                StringComparison.Ordinal);
            Assert.Contains(
                $"Rule: {expectedRule}",
                text,
                StringComparison.Ordinal);
            foreach (string published in new[] { json, text })
            {
                AssertScratchOutputIsSanitized(published);
                Assert.DoesNotContain(
                    privateTableName,
                    published,
                    StringComparison.Ordinal);
                Assert.DoesNotContain(
                    privateCheckName,
                    published,
                    StringComparison.Ordinal);
                Assert.DoesNotContain(
                    privateTargetSql,
                    published,
                    StringComparison.Ordinal);
            }
            Assert.True(string.IsNullOrWhiteSpace(jsonError));
            Assert.True(string.IsNullOrWhiteSpace(textError));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Theory]
    [InlineData("--ddl")]
    [InlineData("--scratch")]
    public async Task ExplicitPreview_RenderFailureIsSanitized(
        string mode)
    {
        string directory = NewTempDirectory();
        const string privateObjectId =
            "private_customer_table_name_6249";
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateCatalogArtifactsAsync(
                    EmptyTableCatalog(privateObjectId),
                    directory,
                    "unrenderable",
                    InspectorCommandRunner.ExitOk,
                    ct,
                    sealPlan: false);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    mode,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "could not be produced safely",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                privateObjectId,
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Theory]
    [InlineData("--ddl")]
    [InlineData("--scratch")]
    public async Task ExplicitPreview_MalformedArtifactFailureIsSanitized(
        string mode)
    {
        string directory = NewTempDirectory();
        const string privateProperty =
            "private_password_token_5407";
        const string privateValue =
            "Server=private.example;Password=artifact-secret";
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            string plan = await File.ReadAllTextAsync(planPath, ct);
            int objectStart = plan.IndexOf('{');
            Assert.True(objectStart >= 0);
            string malformed = plan.Insert(
                objectStart + 1,
                $"\"{privateProperty}\":\"{privateValue}\",");
            await File.WriteAllTextAsync(planPath, malformed, ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    mode,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "could not be produced safely",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                privateProperty,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                privateValue,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "artifact-secret",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Theory]
    [InlineData("--ddl")]
    [InlineData("--scratch")]
    public async Task ExplicitPreview_OversizedArtifactFailsBeforeReading(
        string mode)
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            await using (var stream = new FileStream(
                planPath,
                FileMode.Open,
                FileAccess.Write,
                FileShare.None))
            {
                stream.SetLength(64L * 1024 * 1024 + 1);
            }
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    mode,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "could not be produced safely",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                planPath,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task ExplicitPreview_MalformedOptionsNeverEchoSensitiveToken()
    {
        const string privateToken =
            "Server=private.example;Password=preview-secret";
        var positionalOutput = new StringWriter();
        var positionalError = new StringWriter();
        int positionalCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "preview",
                "plan.json",
                "--catalog",
                "catalog.json",
                "--scratch",
                privateToken,
            ],
            positionalOutput,
            positionalError,
            TestContext.Current.CancellationToken);

        var formatOutput = new StringWriter();
        var formatError = new StringWriter();
        int formatCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "preview",
                "plan.json",
                "--catalog",
                "catalog.json",
                "--ddl",
                "--format",
                privateToken,
            ],
            formatOutput,
            formatError,
            TestContext.Current.CancellationToken);

        var assignedOutput = new StringWriter();
        var assignedError = new StringWriter();
        int assignedCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "preview",
                "plan.json",
                "--catalog",
                "catalog.json",
                $"--scratch={privateToken}",
            ],
            assignedOutput,
            assignedError,
            TestContext.Current.CancellationToken);

        Assert.Equal(InspectorCommandRunner.ExitUsage, positionalCode);
        Assert.Equal(InspectorCommandRunner.ExitUsage, formatCode);
        Assert.Equal(InspectorCommandRunner.ExitUsage, assignedCode);
        Assert.True(
            string.IsNullOrWhiteSpace(positionalOutput.ToString()));
        Assert.True(string.IsNullOrWhiteSpace(formatOutput.ToString()));
        Assert.True(
            string.IsNullOrWhiteSpace(assignedOutput.ToString()));
        string errors =
            positionalError.ToString() +
            formatError +
            assignedError;
        Assert.Contains(
            "explicit CSharpDB preview options are invalid",
            errors,
            StringComparison.Ordinal);
        Assert.Contains(
            "Unsupported explicit preview format",
            errors,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            privateToken,
            errors,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "preview-secret",
            errors,
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--ddl")]
    [InlineData("--scratch")]
    public async Task ExplicitPreview_LimitFailureUsesStableSafeError(
        string mode)
    {
        string directory = NewTempDirectory();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) =
                await CreateArtifactsAsync(directory, ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    BuildCSharpDbDdlPreview =
                        (plan, catalog, cancellationToken) =>
                            CSharpDbDdlPreviewBuilder.BuildBounded(
                                plan,
                                catalog,
                                CSharpDbDdlPreviewBuildOptions.Default with
                                {
                                    MaxActionCount = 1,
                                },
                                cancellationToken: cancellationToken),
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate",
                    "preview",
                    planPath,
                    "--catalog",
                    catalogPath,
                    mode,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-CSHARPDB-DDL-PREVIEW-LIMIT-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "exceeded a production safety limit",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "CREATE TABLE",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                "customers",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task DdlAndScratchModes_CannotBeCombined()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "preview",
                "plan.json",
                "--catalog",
                "catalog.json",
                "--DDL",
                "--SCRATCH",
            ],
            output,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "Options --ddl and --scratch cannot be combined.",
            error.ToString(),
            StringComparison.Ordinal);
    }

    private static async Task<(int Code, string Output, string Error)>
        RunPreviewAsync(
            string planPath,
            string catalogPath,
            IReadOnlyList<string> options,
            CancellationToken ct)
    {
        var args = new List<string>
        {
            "migrate",
            "preview",
            planPath,
            "--catalog",
            catalogPath,
        };
        args.AddRange(options);
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [.. args],
            output,
            error,
            ct);
        return (code, output.ToString(), error.ToString());
    }

    private static async Task<(string CatalogPath, string PlanPath)>
        CreateArtifactsAsync(
            string directory,
            CancellationToken ct)
    {
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "inspect",
                "--source",
                "synthetic",
                "--out",
                catalogPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            ct);
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "plan",
                catalogPath,
                "--out",
                planPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            ct);

        Assert.Equal(InspectorCommandRunner.ExitWarn, inspectCode);
        Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);
        return (catalogPath, planPath);
    }

    private static async Task<(string CatalogPath, string PlanPath)>
        CreateBlockedSqlServerArtifactsAsync(
            string directory,
            CancellationToken ct)
    {
        MigrationCatalog synthetic =
            await new SyntheticMigrationSourceInspector().InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                    IncludeProfile = true,
                },
                ct);
        var blocker = new MigrationDiagnostic
        {
            DiagnosticId = "sqlserver:cli-preview:blocker",
            RuleId = "MIG-SQLSERVER-CLI-PREVIEW-BLOCK-001",
            Severity = MigrationDiagnosticSeverity.Error,
            Status = MigrationCompatibilityStatus.Unsupported,
            Evidence = MigrationEvidenceLevel.Parsed,
            Summary = "SQL Server CLI preview test blocker.",
            Explanation =
                "Keeps readiness blocked after successful scratch execution.",
            ObjectId = "syn:table:customers-upper",
            Remediation = "Resolve before migration.",
            CanOverride = false,
        };
        MigrationCatalog catalog = synthetic with
        {
            Source = synthetic.Source with
            {
                Kind = MigrationSourceKind.SqlServer,
                Identity = "sqlserver:cli-ddl-preview-test-v1",
                Fingerprint =
                    "sha256:4628bfa0d115e92d6b6348a1245f1370ee2f35ed039a7d48e9be39878bf48dbf",
                ProviderVersion = "test-provider-v1",
                SourceVersion = "test-source-v1",
            },
            Diagnostics = synthetic.Diagnostics.Append(blocker).ToArray(),
        };
        string catalogPath =
            Path.Combine(directory, "sqlserver-catalog.json");
        string planPath =
            Path.Combine(directory, "sqlserver-plan.json");
        await File.WriteAllTextAsync(
            catalogPath,
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            ct);
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "plan",
                catalogPath,
                "--out",
                planPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            ct);

        Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);
        return (catalogPath, planPath);
    }

    private static async Task<(string CatalogPath, string PlanPath)>
        CreateCatalogArtifactsAsync(
            MigrationCatalog catalog,
            string directory,
            string prefix,
            int expectedPlanCode,
            CancellationToken ct,
            bool sealPlan = true)
    {
        string catalogPath =
            Path.Combine(directory, $"{prefix}-catalog.json");
        string planPath =
            Path.Combine(directory, $"{prefix}-plan.json");
        await File.WriteAllTextAsync(
            catalogPath,
            MigrationArtifactSerializer.SerializeCatalog(catalog),
            ct);
        MigrationCommandDependencies dependencies =
            sealPlan
                ? MigrationCommandDependencies.Default
                : MigrationCommandDependencies.Default with
                {
                    SealCSharpDbMigrationPlan =
                        static (plan, _, _) => plan,
                };
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate",
                "plan",
                catalogPath,
                "--out",
                planPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            dependencies,
            ct);
        Assert.Equal(expectedPlanCode, planCode);
        if (!sealPlan)
        {
            MigrationPlan legacyPlan =
                MigrationArtifactSerializer.DeserializePlan(
                    await File.ReadAllTextAsync(planPath, ct),
                    catalog);
            Assert.Null(legacyPlan.GeneratedDdlDigest);
        }
        return (catalogPath, planPath);
    }

    private static MigrationCatalog ScratchCheckCatalog(
        string fixtureId,
        string tableName,
        string checkName,
        string targetSql) => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = $"synthetic:cli-scratch-{fixtureId}",
            Fingerprint =
                "9ad3a1f3bd9ad017a71cf4a9e69a36af00bdb5cd2fc6e51fc53f94549475256b",
            ProviderVersion = "test-provider-v1",
            SourceVersion = "test-source-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable CLI scratch evidence fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = $"{fixtureId}:table",
                Kind = MigrationObjectKind.Table,
                SourceName = tableName,
            },
            new MigrationCatalogObject
            {
                ObjectId = $"{fixtureId}:column",
                Kind = MigrationObjectKind.Column,
                ParentObjectId = $"{fixtureId}:table",
                SourceName = "value",
                NativeType = "INT64",
                Facets =
                [
                    new MigrationCatalogFacet
                    {
                        Name = "logicalType",
                        Value = "signedInteger",
                    },
                    new MigrationCatalogFacet
                    {
                        Name = "nullable",
                        Value = "false",
                    },
                ],
            },
            new MigrationCatalogObject
            {
                ObjectId = $"{fixtureId}:check",
                Kind = MigrationObjectKind.CheckConstraint,
                ParentObjectId = $"{fixtureId}:table",
                SourceName = checkName,
                DependsOn = [$"{fixtureId}:column"],
                Facets =
                [
                    new MigrationCatalogFacet
                    {
                        Name = "deterministic",
                        Value = "true",
                    },
                    new MigrationCatalogFacet
                    {
                        Name = "rowLocal",
                        Value = "true",
                    },
                    new MigrationCatalogFacet
                    {
                        Name = "targetSql",
                        Value = targetSql,
                    },
                ],
            },
        ],
    };

    private static MigrationCatalog ScratchTableCatalog() => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:cli-scratch-ready",
            Fingerprint =
                "a1d4743ead885217e0f902399ee1a7e09b7185961d619c86fece70c23774b1f6",
            ProviderVersion = "test-provider-v1",
            SourceVersion = "test-source-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable ready CLI scratch fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = "ready:table",
                Kind = MigrationObjectKind.Table,
                SourceName = "ready_table",
            },
            new MigrationCatalogObject
            {
                ObjectId = "ready:column",
                Kind = MigrationObjectKind.Column,
                ParentObjectId = "ready:table",
                SourceName = "value",
                NativeType = "INT64",
                Facets =
                [
                    new MigrationCatalogFacet
                    {
                        Name = "logicalType",
                        Value = "signedInteger",
                    },
                    new MigrationCatalogFacet
                    {
                        Name = "nullable",
                        Value = "false",
                    },
                ],
            },
        ],
    };

    private static MigrationCatalog EmptyTableCatalog(
        string privateObjectId) => new()
    {
        TargetCSharpDbVersion =
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        Source = new MigrationSourceIdentity
        {
            Kind = MigrationSourceKind.Synthetic,
            Identity = "synthetic:cli-unrenderable-preview",
            Fingerprint =
                "44830f325780dbf5fd0b40327da4659d353787028f7bb352e6a088973d38d1bd",
            ProviderVersion = "test-provider-v1",
            SourceVersion = "test-source-v1",
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Immutable,
                Description = "Immutable unrenderable CLI preview fixture.",
            },
        },
        Objects =
        [
            new MigrationCatalogObject
            {
                ObjectId = privateObjectId,
                Kind = MigrationObjectKind.Table,
                SourceName = "private customer table 6249",
            },
        ],
    };

    private static void AssertScratchOutputIsSanitized(string output)
    {
        Assert.DoesNotContain(
            "CREATE TABLE",
            output,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            CollectionActionPrefix,
            output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "\"sql\"",
            output,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "\"targetName\"",
            output,
            StringComparison.OrdinalIgnoreCase);
    }

    private static string Sha256(string value) =>
        Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(
                    value.Replace(
                        "\r\n",
                        "\n",
                        StringComparison.Ordinal))))
            .ToLowerInvariant();

    private static string NewTempDirectory()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_ddl_cli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
