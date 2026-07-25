using CSharpDB.Migration;

namespace CSharpDB.Cli.Tests;

public sealed class MySqlMigrationCommandRunnerTests
{
    private const string EnvironmentVariableName =
        "CSHARPDB_TEST_MYSQL_CONNECTION";
    private const string SecretConnectionString =
        "Server=private.example;Database=source;User ID=reader;Password=never-print-this";

    [Theory]
    [InlineData(false, InspectorCommandRunner.ExitOk, "Status: OK")]
    [InlineData(true, InspectorCommandRunner.ExitWarn, "Status: REVIEW")]
    public async Task Inspect_PublishesSchemaOnlyCatalogWithoutDisclosingConnection(
        bool includeDiagnostic,
        int expectedExitCode,
        string expectedStatus)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string? capturedEnvironmentName = null;
        string? capturedTargetVersion = null;

        try
        {
            MigrationCatalog catalog =
                await CreateMySqlCatalogAsync(includeDiagnostic, ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (
                    environmentName,
                    targetVersion,
                    cancellationToken) =>
                {
                    capturedEnvironmentName = environmentName;
                    capturedTargetVersion = targetVersion;
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        MySqlWorkerResult.Success(catalog));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(expectedExitCode, exitCode);
            Assert.Equal(EnvironmentVariableName, capturedEnvironmentName);
            Assert.Equal(
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                capturedTargetVersion);
            Assert.True(File.Exists(catalogPath));

            string artifact = await File.ReadAllTextAsync(catalogPath, ct);
            MigrationCatalog published =
                MigrationArtifactSerializer.DeserializeCatalog(artifact);
            Assert.Equal(MigrationSourceKind.MySql, published.Source.Kind);
            Assert.Equal("mysql:test-schema-v1", published.Source.Identity);
            Assert.Equal(catalog.Objects.Count, published.Objects.Count);
            Assert.Equal(catalog.Diagnostics.Count, published.Diagnostics.Count);
            Assert.Contains(expectedStatus, output.ToString(), StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
            AssertSecretAbsent(output.ToString(), error.ToString(), artifact);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task Inspect_UnsetOrBlankConnectionEnvironmentFailsClosed(
        string? connectionString)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        bool workerCalled = false;

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (
                    name,
                    targetVersion,
                    cancellationToken) =>
                {
                    Assert.Equal(EnvironmentVariableName, name);
                    Assert.Equal(
                        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                        targetVersion);
                    Assert.True(string.IsNullOrWhiteSpace(connectionString));
                    cancellationToken.ThrowIfCancellationRequested();
                    workerCalled = true;
                    return ValueTask.FromResult(
                        MySqlWorkerResult.Failure(
                            MySqlWorkerStatus.ConnectionUnavailable));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.True(workerCalled);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-MYSQL-CLI-CONNECTION-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                EnvironmentVariableName,
                error.ToString(),
                StringComparison.Ordinal);
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("9CONNECTION")]
    [InlineData("BAD-NAME")]
    [InlineData("BAD.NAME")]
    [InlineData("BAD NAME")]
    public async Task Inspect_InvalidEnvironmentVariableNameIsRejectedBeforeLookup(
        string environmentVariableName)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        bool workerCalled = false;

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, _) =>
                {
                    workerCalled = true;
                    throw new InvalidOperationException(
                        "The worker must not be called.");
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", environmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, exitCode);
            Assert.False(workerCalled);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "environment variable name is invalid",
                error.ToString(),
                StringComparison.Ordinal);
            if (environmentVariableName.Length > 0)
            {
                Assert.DoesNotContain(
                    environmentVariableName,
                    error.ToString(),
                    StringComparison.Ordinal);
            }
            AssertSecretAbsent(output.ToString(), error.ToString());
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_OverlongEnvironmentVariableNameIsRejectedBeforeLookup()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string overlongName = "A" + new string('B', 128);
        bool workerCalled = false;

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, _) =>
                {
                    workerCalled = true;
                    throw new InvalidOperationException(
                        "The worker must not be called.");
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", overlongName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, exitCode);
            Assert.False(workerCalled);
            Assert.False(File.Exists(catalogPath));
            Assert.DoesNotContain(
                overlongName,
                error.ToString(),
                StringComparison.Ordinal);
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_MalformedArgumentsNeverEchoRawConnectionMaterial()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        const string rawConnection =
            "Server=private.example;Password=review-secret";
        bool workerCalled = false;
        var dependencies = new MigrationCommandDependencies
        {
            InspectMySqlAsync = (_, _, _) =>
            {
                workerCalled = true;
                throw new InvalidOperationException(
                    "The worker must not be called.");
            },
        };

        try
        {
            var positionalOutput = new StringWriter();
            var positionalError = new StringWriter();
            int positionalCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    rawConnection,
                    "--out", catalogPath,
                ],
                positionalOutput,
                positionalError,
                dependencies,
                ct);

            var optionOutput = new StringWriter();
            var optionError = new StringWriter();
            int optionCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--Password=review-secret", "unused",
                    "--out", catalogPath,
                ],
                optionOutput,
                optionError,
                dependencies,
                ct);

            var sourceOutput = new StringWriter();
            var sourceError = new StringWriter();
            int sourceCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", rawConnection,
                    "--out", catalogPath,
                ],
                sourceOutput,
                sourceError,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, positionalCode);
            Assert.Equal(InspectorCommandRunner.ExitUsage, optionCode);
            Assert.Equal(InspectorCommandRunner.ExitUsage, sourceCode);
            Assert.False(workerCalled);
            Assert.True(
                string.IsNullOrWhiteSpace(positionalOutput.ToString()));
            Assert.True(
                string.IsNullOrWhiteSpace(optionOutput.ToString()));
            Assert.True(
                string.IsNullOrWhiteSpace(sourceOutput.ToString()));
            string publishedErrors =
                positionalError.ToString() +
                optionError +
                sourceError;
            Assert.Contains(
                "unexpected positional argument",
                publishedErrors,
                StringComparison.Ordinal);
            Assert.Contains(
                "unsupported option",
                publishedErrors,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                rawConnection,
                publishedErrors,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "review-secret",
                publishedErrors,
                StringComparison.Ordinal);
            Assert.False(File.Exists(catalogPath));
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_MaliciousInspectorFailureIsSanitized()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        const string maliciousMessage =
            "provider failure: Password=stolen; SELECT secret FROM credentials";

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, _) =>
                    throw new InvalidOperationException(maliciousMessage),
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-MYSQL-CLI-INSPECT-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "could not be inspected or published safely",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                maliciousMessage,
                error.ToString(),
                StringComparison.Ordinal);
            AssertSecretAbsent(output.ToString(), error.ToString());
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("connection", "MIG-MYSQL-CLI-CONNECTION-001")]
    [InlineData("inspection", "MIG-MYSQL-CLI-INSPECT-001")]
    [InlineData("incompatible", "MIG-MYSQL-CLI-ADAPTER-001")]
    public async Task Inspect_SecretBearingSetupFailureIsSanitized(
        string workerState,
        string expectedCode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string maliciousMessage =
            $"setup failed with {SecretConnectionString}";

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _ = maliciousMessage;
                    MySqlWorkerStatus workerStatus = workerState switch
                    {
                        "connection" =>
                            MySqlWorkerStatus.ConnectionUnavailable,
                        "inspection" =>
                            MySqlWorkerStatus.InspectionFailed,
                        "incompatible" =>
                            MySqlWorkerStatus.Incompatible,
                        _ => throw new InvalidOperationException(
                            "Unexpected test worker state."),
                    };
                    return ValueTask.FromResult(
                        MySqlWorkerResult.Failure(workerStatus));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                expectedCode,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                maliciousMessage,
                error.ToString(),
                StringComparison.Ordinal);
            AssertSecretAbsent(output.ToString(), error.ToString());
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_DestinationRaceDoesNotOverwriteAndCleansTemporaryFile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        byte[] sentinel = "unrelated-existing-artifact"u8.ToArray();

        try
        {
            MigrationCatalog catalog =
                await CreateMySqlCatalogAsync(
                    includeDiagnostic: false,
                    ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    File.WriteAllBytes(catalogPath, sentinel);
                    return ValueTask.FromResult(
                        MySqlWorkerResult.Success(catalog));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                output,
                error,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, exitCode);
            Assert.Equal(
                sentinel,
                await File.ReadAllBytesAsync(catalogPath, ct));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "MIG-MYSQL-CLI-INSPECT-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "could not be inspected or published safely",
                error.ToString(),
                StringComparison.Ordinal);
            AssertSecretAbsent(output.ToString(), error.ToString());
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_CallerCancellationPropagatesWithoutPublishing()
    {
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        bool workerCalled = false;
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        try
        {
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, _) =>
                {
                    workerCalled = true;
                    throw new InvalidOperationException(
                        "The worker must not be called.");
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "inspect",
                        "--source", "mysql",
                        "--connection-env", EnvironmentVariableName,
                        "--out", catalogPath,
                    ],
                    output,
                    error,
                    dependencies,
                    cancellation.Token));

            Assert.False(workerCalled);
            Assert.False(File.Exists(catalogPath));
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task Inspect_ExistingDestinationIsNotOverwrittenAndLeavesNoTemporaryFile()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        int workerCalls = 0;

        try
        {
            MigrationCatalog catalog =
                await CreateMySqlCatalogAsync(includeDiagnostic: false, ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectMySqlAsync = (_, _, cancellationToken) =>
                {
                    workerCalls++;
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        MySqlWorkerResult.Success(catalog));
                },
            };

            int firstExitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                new StringWriter(),
                new StringWriter(),
                dependencies,
                ct);
            byte[] firstArtifact = await File.ReadAllBytesAsync(catalogPath, ct);
            var retryOutput = new StringWriter();
            var retryError = new StringWriter();

            int retryExitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "mysql",
                    "--connection-env", EnvironmentVariableName,
                    "--out", catalogPath,
                ],
                retryOutput,
                retryError,
                dependencies,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, firstExitCode);
            Assert.Equal(InspectorCommandRunner.ExitUsage, retryExitCode);
            Assert.Equal(1, workerCalls);
            Assert.Equal(
                firstArtifact,
                await File.ReadAllBytesAsync(catalogPath, ct));
            Assert.True(string.IsNullOrWhiteSpace(retryOutput.ToString()));
            Assert.Contains(
                "destination already exists",
                retryError.ToString(),
                StringComparison.Ordinal);
            AssertSecretAbsent(
                retryOutput.ToString(),
                retryError.ToString(),
                await File.ReadAllTextAsync(catalogPath, ct));
            AssertNoTemporaryArtifacts(directory);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task PlanAndPreview_MySqlCatalogRemainBlocked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");

        try
        {
            MigrationCatalog catalog =
                await CreateMySqlCatalogAsync(
                    includeDiagnostic: true,
                    ct);
            await File.WriteAllTextAsync(
                catalogPath,
                MigrationArtifactSerializer.SerializeCatalog(catalog),
                ct);

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan",
                    catalogPath,
                    "--out", planPath,
                ],
                TextWriter.Null,
                TextWriter.Null,
                ct);

            string artifact = await File.ReadAllTextAsync(planPath, ct);
            MigrationPlan plan =
                MigrationArtifactSerializer.DeserializePlan(
                    artifact,
                    catalog);
            MigrationPlanReadiness readiness =
                MigrationPlanReadinessValidator.Evaluate(plan, catalog);

            Assert.Equal(InspectorCommandRunner.ExitWarn, exitCode);
            Assert.Matches(
                "^[0-9a-f]{64}$",
                plan.GeneratedDdlDigest);
            Assert.Equal(
                MigrationPlanReadinessStatus.Blocked,
                readiness.Status);
            Assert.DoesNotContain(
                "CREATE TABLE",
                artifact,
                StringComparison.OrdinalIgnoreCase);

            var previewOutput = new StringWriter();
            var previewError = new StringWriter();
            int previewExitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "preview",
                    planPath,
                    "--catalog", catalogPath,
                    "--ddl",
                    "--format", "json",
                ],
                previewOutput,
                previewError,
                ct);

            Assert.Equal(
                InspectorCommandRunner.ExitWarn,
                previewExitCode);
            Assert.Contains(
                "csharpdb-ddl-preview/v1",
                previewOutput.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "CREATE TABLE",
                previewOutput.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.True(
                string.IsNullOrWhiteSpace(previewError.ToString()));
            AssertSecretAbsent(artifact);
            AssertSecretAbsent(previewOutput.ToString());
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    private static async ValueTask<MigrationCatalog> CreateMySqlCatalogAsync(
        bool includeDiagnostic,
        CancellationToken ct)
    {
        MigrationCatalog synthetic =
            await new SyntheticMigrationSourceInspector().InspectAsync(
                new MigrationInspectionRequest
                {
                    TargetCSharpDbVersion =
                        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                    IncludeProfile = false,
                },
                ct);
        MigrationCatalog candidate = synthetic with
        {
            Source = new MigrationSourceIdentity
            {
                Kind = MigrationSourceKind.MySql,
                Identity = "mysql:test-schema-v1",
                Fingerprint = "sha256:" + new string('a', 64),
                ProviderVersion = "test-provider-v1",
                SourceVersion = "test-source-v1",
                Consistency = new MigrationConsistencyStrategy
                {
                    Kind = MigrationConsistencyKind.Snapshot,
                    Description = "Deterministic schema-only test snapshot.",
                },
            },
            Diagnostics = includeDiagnostic
                ? synthetic.Diagnostics
                    .Append(
                        new MigrationDiagnostic
                        {
                            DiagnosticId =
                                "mysql:inventory:partial",
                            RuleId =
                                "MIG-MYSQL-INVENTORY-PARTIAL-001",
                            Severity =
                                MigrationDiagnosticSeverity.Error,
                            Status =
                                MigrationCompatibilityStatus.Unknown,
                            Evidence = MigrationEvidenceLevel.Parsed,
                            Summary =
                                "This test catalog is an intentionally partial MySQL inventory.",
                            Explanation =
                                "The CLI must keep a partial MySQL inventory blocked while allowing deterministic planning and preview.",
                            Remediation =
                                "Complete source qualification before migration approval.",
                            CanOverride = false,
                        })
                    .ToArray()
                : [],
        };

        return MigrationArtifactSerializer.DeserializeCatalog(
            MigrationArtifactSerializer.SerializeCatalog(candidate));
    }

    private static string CreateTempDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_mysql_cli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static void AssertNoTemporaryArtifacts(string directory) =>
        Assert.Empty(
            Directory.EnumerateFiles(
                directory,
                ".csharpdb-migration-*.tmp",
                SearchOption.TopDirectoryOnly));

    private static void AssertSecretAbsent(params string[] values)
    {
        foreach (string value in values)
        {
            Assert.DoesNotContain(
                SecretConnectionString,
                value,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "never-print-this",
                value,
                StringComparison.Ordinal);
        }
    }

    private static void TryDeleteDirectory(string directory)
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
