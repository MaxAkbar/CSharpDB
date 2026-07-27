using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli.Tests;

public sealed class SqlServerMigrationCommandRunnerTests
{
    private const string EnvironmentVariableName =
        "CSHARPDB_TEST_SQLSERVER_CONNECTION";
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
                await CreateSqlServerCatalogAsync(includeDiagnostic, ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectSqlServerAsync = (
                    environmentName,
                    targetVersion,
                    cancellationToken) =>
                {
                    capturedEnvironmentName = environmentName;
                    capturedTargetVersion = targetVersion;
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        SqlServerWorkerResult.Success(catalog));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
            Assert.Equal(MigrationSourceKind.SqlServer, published.Source.Kind);
            Assert.Equal("sqlserver:test-schema-v1", published.Source.Identity);
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
                InspectSqlServerAsync = (
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
                        SqlServerWorkerResult.Failure(
                            SqlServerWorkerStatus.ConnectionUnavailable));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
                "MIG-SQLSERVER-CLI-CONNECTION-001",
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
                InspectSqlServerAsync = (_, _, _) =>
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
                    "--source", "sqlserver",
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
                InspectSqlServerAsync = (_, _, _) =>
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
                    "--source", "sqlserver",
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
            InspectSqlServerAsync = (_, _, _) =>
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
                    "--source", "sqlserver",
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
                    "--source", "sqlserver",
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
                InspectSqlServerAsync = (_, _, _) =>
                    throw new InvalidOperationException(maliciousMessage),
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
                "MIG-SQLSERVER-CLI-INSPECT-001",
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
    [InlineData("connection", "MIG-SQLSERVER-CLI-CONNECTION-001")]
    [InlineData("inspection", "MIG-SQLSERVER-CLI-INSPECT-001")]
    [InlineData("incompatible", "MIG-SQLSERVER-CLI-ADAPTER-001")]
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
                InspectSqlServerAsync = (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    _ = maliciousMessage;
                    SqlServerWorkerStatus workerStatus = workerState switch
                    {
                        "connection" =>
                            SqlServerWorkerStatus.ConnectionUnavailable,
                        "inspection" =>
                            SqlServerWorkerStatus.InspectionFailed,
                        "incompatible" =>
                            SqlServerWorkerStatus.Incompatible,
                        _ => throw new InvalidOperationException(
                            "Unexpected test worker state."),
                    };
                    return ValueTask.FromResult(
                        SqlServerWorkerResult.Failure(workerStatus));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
                await CreateSqlServerCatalogAsync(
                    includeDiagnostic: false,
                    ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectSqlServerAsync = (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    File.WriteAllBytes(catalogPath, sentinel);
                    return ValueTask.FromResult(
                        SqlServerWorkerResult.Success(catalog));
                },
            };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
                "MIG-SQLSERVER-CLI-INSPECT-001",
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
                InspectSqlServerAsync = (_, _, _) =>
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
                        "--source", "sqlserver",
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
                await CreateSqlServerCatalogAsync(includeDiagnostic: false, ct);
            var dependencies = new MigrationCommandDependencies
            {
                InspectSqlServerAsync = (_, _, cancellationToken) =>
                {
                    workerCalls++;
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        SqlServerWorkerResult.Success(catalog));
                },
            };

            int firstExitCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlserver",
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
                    "--source", "sqlserver",
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
    public async Task Plan_SqlServerCatalogIsSealedWithoutPromotingReadiness()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");

        try
        {
            MigrationCatalog catalog =
                await CreateSqlServerCatalogAsync(
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
                MigrationPlanReadinessStatus.RequiresApproval,
                readiness.Status);
            Assert.DoesNotContain(
                "CREATE TABLE",
                artifact,
                StringComparison.OrdinalIgnoreCase);
            AssertSecretAbsent(artifact);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task
        CaptureInspect_ForwardsTimeoutVerifiesPackageAndRemovesWorkspace()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string packagePath =
            Path.Combine(
                directory,
                "captured.csdbsqlserver");
        string catalogPath =
            Path.Combine(
                directory,
                "captured.json");
        int? capturedTimeout = null;
        try
        {
            var dependencies =
                new MigrationCommandDependencies
                {
                    CaptureSqlServerAsync =
                        async (
                            environmentName,
                            targetVersion,
                            temporaryPath,
                            maximumBytes,
                            tableTimeoutSeconds,
                            cancellationToken) =>
                        {
                            Assert.Equal(
                                EnvironmentVariableName,
                                environmentName);
                            Assert.Equal(
                                4 * 1024 * 1024,
                                maximumBytes);
                            capturedTimeout =
                                tableTimeoutSeconds;
                            CapturedRetainedPackage
                                captured =
                                await WriteRetainedPackageAsync(
                                    temporaryPath,
                                    targetVersion,
                                    maximumBytes,
                                    cancellationToken);
                            return SqlServerCaptureWorkerResult
                                .Success(
                                    Receipt(
                                        temporaryPath,
                                        captured.Result));
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "inspect",
                        "--source", "sqlserver",
                        "--connection-env",
                        EnvironmentVariableName,
                        "--package", packagePath,
                        "--out", catalogPath,
                        "--max-source-bytes",
                        (4 * 1024 * 1024)
                            .ToString(
                                System.Globalization
                                    .CultureInfo
                                    .InvariantCulture),
                        "--table-timeout-seconds",
                        "42",
                    ],
                    output,
                    error,
                    dependencies,
                    ct);

            Assert.Equal(
                InspectorCommandRunner.ExitOk,
                exitCode);
            Assert.Equal(42, capturedTimeout);
            Assert.True(File.Exists(packagePath));
            Assert.True(File.Exists(catalogPath));
            Assert.True(
                string.IsNullOrWhiteSpace(
                    error.ToString()));
            Assert.Empty(
                Directory.EnumerateDirectories(
                    directory,
                    SqlServerWorkerClient
                        .CaptureWorkspacePrefix +
                    "*",
                    SearchOption.TopDirectoryOnly));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public void
        CaptureWorkspace_UnixCreationIsExclusiveAndOwnerPrivate()
    {
        if (OperatingSystem.IsWindows())
            return;

        string directory = CreateTempDirectory();
        string candidate = Path.Combine(
            directory,
            SqlServerWorkerClient.CaptureWorkspacePrefix +
            Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(candidate);
            Assert.Throws<IOException>(
                () => MigrationCommandRunner
                    .SqlServerCaptureWorkspace
                    .CreatePrivateDirectoryExclusive(
                        candidate));
            Directory.Delete(candidate);

            MigrationCommandRunner
                .SqlServerCaptureWorkspace
                .CreatePrivateDirectoryExclusive(candidate);
            Assert.Equal(
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute,
                File.GetUnixFileMode(candidate));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task
        CaptureInspect_CatalogRacePreservesVerifiedPackageAndCleansWorkspace()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string packagePath =
            Path.Combine(
                directory,
                "preserved.csdbsqlserver");
        string catalogPath =
            Path.Combine(
                directory,
                "raced-catalog.json");
        byte[] sentinel =
            "do-not-overwrite"u8.ToArray();
        try
        {
            var dependencies =
                new MigrationCommandDependencies
                {
                    CaptureSqlServerAsync =
                        async (
                            _,
                            targetVersion,
                            temporaryPath,
                            maximumBytes,
                            _,
                            cancellationToken) =>
                        {
                            CapturedRetainedPackage
                                captured =
                                await WriteRetainedPackageAsync(
                                    temporaryPath,
                                    targetVersion,
                                    maximumBytes,
                                    cancellationToken);
                            await File.WriteAllBytesAsync(
                                catalogPath,
                                sentinel,
                                cancellationToken);
                            return SqlServerCaptureWorkerResult
                                .Success(
                                    Receipt(
                                        temporaryPath,
                                        captured.Result));
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "inspect",
                        "--source", "sqlserver",
                        "--connection-env",
                        EnvironmentVariableName,
                        "--package", packagePath,
                        "--out", catalogPath,
                    ],
                    output,
                    error,
                    dependencies,
                    ct);

            Assert.Equal(
                InspectorCommandRunner.ExitError,
                exitCode);
            Assert.True(File.Exists(packagePath));
            Assert.Equal(
                sentinel,
                await File.ReadAllBytesAsync(
                    catalogPath,
                    ct));
            Assert.Contains(
                "MIG-SQLSERVER-CLI-CATALOG-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.Contains(
                "package was preserved",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.True(
                string.IsNullOrWhiteSpace(
                    output.ToString()));
            Assert.Empty(
                Directory.EnumerateDirectories(
                    directory,
                    SqlServerWorkerClient
                        .CaptureWorkspacePrefix +
                    "*",
                    SearchOption.TopDirectoryOnly));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("0")]
    [InlineData("86401")]
    [InlineData("-1")]
    [InlineData("not-a-number")]
    public async Task
        CaptureInspect_InvalidTableTimeoutIsRejectedBeforeWorker(
        string timeoutValue)
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        int workerCalls = 0;
        try
        {
            var dependencies =
                new MigrationCommandDependencies
                {
                    CaptureSqlServerAsync =
                        (_, _, _, _, _, _) =>
                        {
                            workerCalls++;
                            throw new InvalidOperationException();
                        },
                };
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "inspect",
                        "--source", "sqlserver",
                        "--connection-env",
                        EnvironmentVariableName,
                        "--package",
                        Path.Combine(
                            directory,
                            "source.csdbsqlserver"),
                        "--out",
                        Path.Combine(
                            directory,
                            "catalog.json"),
                        "--table-timeout-seconds",
                        timeoutValue,
                    ],
                    TextWriter.Null,
                    error,
                    dependencies,
                    ct);

            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                exitCode);
            Assert.Equal(0, workerCalls);
            Assert.Contains(
                "1 through 86400",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task
        RetainedPackage_CompletesOfflineApplyResumeAndChecksumValidation()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string? originalConnection =
            Environment.GetEnvironmentVariable(
                EnvironmentVariableName);
        try
        {
            SqlServerRetainedArtifacts artifacts =
                await CreateRetainedArtifactsAsync(
                    directory,
                    ct);
            string planPath =
                Path.Combine(directory, "plan.json");
            var planError = new StringWriter();
            int planCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "plan",
                        artifacts.CatalogPath,
                        "--out", planPath,
                        "--accept-exclusions", "all",
                    ],
                    TextWriter.Null,
                    planError,
                    ct);
            Assert.True(
                planCode is
                    InspectorCommandRunner.ExitOk or
                    InspectorCommandRunner.ExitWarn,
                planError.ToString());

            Environment.SetEnvironmentVariable(
                EnvironmentVariableName,
                null);

            string wrongTarget =
                Path.Combine(
                    directory,
                    "wrong-digest.csdb");
            var wrongError = new StringWriter();
            int wrongCode =
                await RunRetainedApplyAsync(
                    artifacts,
                    planPath,
                    wrongTarget,
                    Path.Combine(
                        directory,
                        "wrong-digest-run.json"),
                    "sha256:" + new string('0', 64),
                    resume: false,
                    TextWriter.Null,
                    wrongError,
                    ct);
            Assert.Equal(
                InspectorCommandRunner.ExitError,
                wrongCode);
            Assert.False(File.Exists(wrongTarget));
            Assert.False(
                File.Exists(wrongTarget + ".wal"));

            string tamperedPackage =
                Path.Combine(
                    directory,
                    "tampered.csdbsqlserver");
            File.Copy(
                artifacts.PackagePath,
                tamperedPackage);
            await using (
                FileStream tampered =
                    new(
                        tamperedPackage,
                        FileMode.Open,
                        FileAccess.ReadWrite,
                        FileShare.None))
            {
                tampered.Position =
                    tampered.Length - 1;
                int value = tampered.ReadByte();
                tampered.Position =
                    tampered.Length - 1;
                tampered.WriteByte(
                    unchecked((byte)(value ^ 0xff)));
                await tampered.FlushAsync(ct);
            }
            SqlServerRetainedArtifacts tamperedArtifacts =
                artifacts with
                {
                    PackagePath =
                        tamperedPackage,
                };
            string tamperedTarget =
                Path.Combine(
                    directory,
                    "tampered-target.csdb");
            var tamperedError = new StringWriter();
            int tamperedCode =
                await RunRetainedApplyAsync(
                    tamperedArtifacts,
                    planPath,
                    tamperedTarget,
                    Path.Combine(
                        directory,
                        "tampered-run.json"),
                    artifacts.PackageDigest,
                    resume: false,
                    TextWriter.Null,
                    tamperedError,
                    ct);
            Assert.Equal(
                InspectorCommandRunner.ExitError,
                tamperedCode);
            Assert.False(
                File.Exists(tamperedTarget));
            Assert.False(
                File.Exists(
                    tamperedTarget + ".wal"));

            string targetPath =
                Path.Combine(
                    directory,
                    "staged.csdb");
            string applyReport =
                Path.Combine(
                    directory,
                    "run.json");
            var applyOutput = new StringWriter();
            var applyError = new StringWriter();
            int applyCode =
                await RunRetainedApplyAsync(
                    artifacts,
                    planPath,
                    targetPath,
                    applyReport,
                    artifacts.PackageDigest,
                    resume: false,
                    applyOutput,
                    applyError,
                    ct);
            Assert.True(
                applyCode is
                    InspectorCommandRunner.ExitOk or
                    InspectorCommandRunner.ExitWarn,
                applyError.ToString());
            using JsonDocument applied =
                JsonDocument.Parse(
                    applyOutput.ToString());
            Assert.Equal(
                3,
                applied.RootElement
                    .GetProperty("rowsWritten")
                    .GetInt64());
            Assert.Equal(
                RetainedMigrationPackageContract
                    .Format,
                applied.RootElement
                    .GetProperty(
                        "sourcePackageFormat")
                    .GetString());

            var resumeOutput = new StringWriter();
            var resumeError = new StringWriter();
            int resumeCode =
                await RunRetainedApplyAsync(
                    artifacts,
                    planPath,
                    targetPath,
                    Path.Combine(
                        directory,
                        "resume.json"),
                    artifacts.PackageDigest,
                    resume: true,
                    resumeOutput,
                    resumeError,
                    ct);
            Assert.True(
                resumeCode is
                    InspectorCommandRunner.ExitOk or
                    InspectorCommandRunner.ExitWarn,
                resumeError.ToString());
            using JsonDocument resumed =
                JsonDocument.Parse(
                    resumeOutput.ToString());
            Assert.Equal(
                0,
                resumed.RootElement
                    .GetProperty("rowsWritten")
                    .GetInt64());
            Assert.Equal(
                3,
                resumed.RootElement
                    .GetProperty("rowsSkipped")
                    .GetInt64());

            string validationPath =
                Path.Combine(
                    directory,
                    "validation.json");
            var validationOutput =
                new StringWriter();
            var validationError =
                new StringWriter();
            int validationCode =
                await MigrationCommandRunner
                    .RunAsync(
                        [
                            "migrate", "validate",
                            planPath,
                            "--catalog",
                            artifacts.CatalogPath,
                            "--source-package",
                            artifacts.PackagePath,
                            "--expected-manifest-digest",
                            artifacts.PackageDigest,
                            "--workspace", directory,
                            "--target", targetPath,
                            "--out", validationPath,
                            "--level", "checksum",
                            "--spill-dir", directory,
                        ],
                        validationOutput,
                        validationError,
                        ct);
            Assert.True(
                validationCode is
                    InspectorCommandRunner.ExitOk or
                    InspectorCommandRunner.ExitWarn,
                validationError.ToString());
            Assert.Contains(
                "Activation: activated",
                validationOutput.ToString(),
                StringComparison.Ordinal);
            MigrationValidationReport report =
                MigrationValidationReportSerializer
                    .Deserialize(
                        await File.ReadAllTextAsync(
                            validationPath,
                            ct));
            Assert.Equal(
                MigrationValidationStatus.Passed,
                report.Outcome);
            Assert.Equal(
                MigrationValidationLevel.Checksum,
                report.Level);
            MigrationObjectValidationEvidence
                tableEvidence =
                Assert.Single(report.Objects);
            Assert.Equal(
                3,
                tableEvidence.SourceRowCount);
            Assert.Equal(
                3,
                tableEvidence.TargetRowCount);
            Assert.NotNull(
                tableEvidence.SourceChecksum);
            Assert.Equal(
                tableEvidence.SourceChecksum,
                tableEvidence.TargetChecksum);
            Assert.Null(
                Environment.GetEnvironmentVariable(
                    EnvironmentVariableName));
        }
        finally
        {
            Environment.SetEnvironmentVariable(
                EnvironmentVariableName,
                originalConnection);
            TryDeleteDirectory(directory);
        }
    }

    [Theory]
    [InlineData("apply")]
    [InlineData("validate")]
    public async Task
        SchemaOnlyCatalogRejectsDataCommandsBeforeTargetMutation(
        string command)
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        try
        {
            MigrationCatalog catalog =
                await CreateSqlServerCatalogAsync(
                    includeDiagnostic: false,
                    ct);
            string catalogPath =
                Path.Combine(
                    directory,
                    "schema-only.json");
            string planPath =
                Path.Combine(
                    directory,
                    "schema-only-plan.json");
            await File.WriteAllTextAsync(
                catalogPath,
                MigrationArtifactSerializer
                    .SerializeCatalog(catalog),
                ct);
            int planCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "plan",
                        catalogPath,
                        "--out", planPath,
                        "--accept-exclusions", "all",
                    ],
                    TextWriter.Null,
                    TextWriter.Null,
                    ct);
            Assert.True(
                planCode is
                    InspectorCommandRunner.ExitOk or
                    InspectorCommandRunner.ExitWarn);

            string targetPath =
                Path.Combine(
                    directory,
                    "must-not-exist.csdb");
            string reportPath =
                Path.Combine(
                    directory,
                    command + ".json");
            string[] arguments =
                string.Equals(
                    command,
                    "apply",
                    StringComparison.Ordinal)
                    ?
                    [
                        "migrate", "apply", planPath,
                        "--catalog", catalogPath,
                        "--target", targetPath,
                        "--out", reportPath,
                    ]
                    :
                    [
                        "migrate", "validate",
                        planPath,
                        "--catalog", catalogPath,
                        "--target", targetPath,
                        "--out", reportPath,
                        "--level", "checksum",
                        "--spill-dir", directory,
                    ];
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    arguments,
                    output,
                    error,
                    ct);

            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                exitCode);
            Assert.Contains(
                "schema-only",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "Inspect the source again with --package",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.False(File.Exists(targetPath));
            Assert.False(
                File.Exists(targetPath + ".wal"));
            Assert.False(File.Exists(reportPath));
        }
        finally
        {
            TryDeleteDirectory(directory);
        }
    }

    private static async ValueTask<
        SqlServerRetainedArtifacts>
        CreateRetainedArtifactsAsync(
        string directory,
        CancellationToken ct)
    {
        string packagePath =
            Path.Combine(
                directory,
                "source.csdbsqlserver");
        string catalogPath =
            Path.Combine(
                directory,
                "retained-catalog.json");
        CapturedRetainedPackage captured =
            await WriteRetainedPackageAsync(
                packagePath,
                CSharpDbCapabilityCatalogLoader
                    .CurrentTargetVersion,
                new RetainedMigrationPackageWriteOptions()
                    .MaxPackageBytes,
                ct);
        await File.WriteAllTextAsync(
            catalogPath,
            MigrationArtifactSerializer
                .SerializeCatalog(captured.Catalog),
            ct);
        return new SqlServerRetainedArtifacts(
            packagePath,
            catalogPath,
            captured.Result.PackageDigest);
    }

    private static async ValueTask<
        CapturedRetainedPackage>
        WriteRetainedPackageAsync(
        string outputPath,
        string targetVersion,
        long maxPackageBytes,
        CancellationToken ct)
    {
        const string tableId =
            "sqlserver:table:dbo:items";
        const string columnId =
            "sqlserver:column:dbo:items:id";
        MigrationCatalog? catalog = null;
        RetainedMigrationPackageWriteResult
            result =
            await RetainedMigrationPackageWriter
                .WriteAsync(
                    new RetainedMigrationPackageCaptureRequest
                    {
                        OutputPath = outputPath,
                        Tables =
                        [
                            new RetainedMigrationTableWrite
                            {
                                Descriptor =
                                    new RetainedMigrationTableDescriptor
                                    {
                                        SourceObjectId =
                                            tableId,
                                        ColumnObjectIds =
                                            [columnId],
                                        OrderingKeyColumnObjectIds =
                                            [columnId],
                                    },
                                Rows =
                                    RetainedRows(),
                            },
                        ],
                        CatalogFactory =
                            (summary, cancellationToken) =>
                            {
                                cancellationToken
                                    .ThrowIfCancellationRequested();
                                catalog =
                                    CreateRetainedSqlServerCatalog(
                                        targetVersion,
                                        summary
                                            .ContentDigest);
                                return ValueTask.FromResult(
                                    new RetainedMigrationCatalogBinding
                                    {
                                        Catalog = catalog,
                                        SnapshotIdentity =
                                            "sqlserver-retained:" +
                                            summary
                                                .ContentDigest,
                                    });
                            },
                        Options =
                            new RetainedMigrationPackageWriteOptions
                            {
                                MaxPackageBytes =
                                    maxPackageBytes,
                            },
                    },
                    ct);
        return new CapturedRetainedPackage(
            result,
            catalog ??
                throw new InvalidOperationException(
                    "The retained SQL Server test catalog was not created."));
    }

    private static MigrationCatalog
        CreateRetainedSqlServerCatalog(
        string targetVersion,
        string fingerprint) =>
        new()
        {
            TargetCSharpDbVersion =
                targetVersion,
            Source =
                new MigrationSourceIdentity
                {
                    Kind =
                        MigrationSourceKind
                            .SqlServer,
                    Identity =
                        "sqlserver:test-retained-v1",
                    Fingerprint =
                        fingerprint,
                    ProviderVersion =
                        "test-provider-v1",
                    SourceVersion =
                        "test-source-v1",
                    Consistency =
                        new MigrationConsistencyStrategy
                        {
                            Kind =
                                MigrationConsistencyKind
                                    .Snapshot,
                            Description =
                                "Deterministic retained SQL Server test snapshot.",
                        },
                },
            Objects =
            [
                new MigrationCatalogObject
                {
                    ObjectId =
                        "sqlserver:database:test",
                    Kind =
                        MigrationObjectKind.Database,
                    SourceName = "test",
                    Facets =
                    [
                        Facet(
                            "sqlServerAnalyzerCatalogContract",
                            "csharpdb-sqlserver-catalog/v6"),
                        Facet(
                            "sqlServerCatalogContract",
                            "csharpdb-sqlserver-retained-catalog/v1"),
                        Facet(
                            "sqlServerDataContract",
                            "csharpdb-sqlserver-retained-data/v1"),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "sqlserver:namespace:dbo",
                    Kind =
                        MigrationObjectKind.Namespace,
                    ParentObjectId =
                        "sqlserver:database:test",
                    SourceName = "dbo",
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "sqlserver:table:dbo:items",
                    Kind =
                        MigrationObjectKind.Table,
                    ParentObjectId =
                        "sqlserver:namespace:dbo",
                    SourceNamespace = "dbo",
                    SourceName = "items",
                    Facets =
                    [
                        Facet(
                            MigrationDataAvailabilityContract
                                .AvailableFacet,
                            "true"),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "sqlserver:column:dbo:items:id",
                    Kind =
                        MigrationObjectKind.Column,
                    ParentObjectId =
                        "sqlserver:table:dbo:items",
                    SourceNamespace = "dbo",
                    SourceName = "id",
                    NativeType = "bigint",
                    Facets =
                    [
                        Facet(
                            "logicalType",
                            "signedInteger"),
                        Facet(
                            "nullable",
                            "false"),
                        Facet(
                            "primaryKey",
                            "true"),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "sqlserver:key:dbo:items:pk",
                    Kind =
                        MigrationObjectKind.Key,
                    ParentObjectId =
                        "sqlserver:table:dbo:items",
                    SourceNamespace = "dbo",
                    SourceName = "PK_items",
                    Facets =
                    [
                        Facet("kind", "primary"),
                    ],
                    Members =
                    [
                        new MigrationObjectReference
                        {
                            ObjectId =
                                "sqlserver:column:dbo:items:id",
                            Role =
                                MigrationObjectReferenceRoles
                                    .Column,
                            Ordinal = 0,
                        },
                    ],
                    DependsOn =
                    [
                        "sqlserver:column:dbo:items:id",
                    ],
                },
            ],
            Diagnostics = [],
        };

    private static MigrationCatalogFacet Facet(
        string name,
        string value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static async IAsyncEnumerable<
        MigrationDataRow> RetainedRows()
    {
        await Task.Yield();
        for (int index = 1;
             index <= 3;
             index++)
        {
            yield return new MigrationDataRow
            {
                StableKey =
                    $"id:{index}",
                Values =
                [
                    new MigrationSourceValue
                    {
                        Kind =
                            MigrationSourceValueKind
                                .SignedInteger,
                        CanonicalText =
                            index.ToString(
                                System.Globalization
                                    .CultureInfo
                                    .InvariantCulture),
                    },
                ],
            };
        }
    }

    private static SqlServerCaptureReceipt Receipt(
        string packagePath,
        RetainedMigrationPackageWriteResult result) =>
        new()
        {
            Format =
                SqlServerCaptureReceipt
                    .CurrentFormat,
            PackageDigest =
                result.PackageDigest,
            CatalogDigest =
                result.Manifest.CatalogDigest,
            SnapshotIdentity =
                result.Manifest.SnapshotIdentity,
            PackageBytes =
                new FileInfo(packagePath).Length,
            TableCount =
                result.Manifest.Tables.Count,
            RowCount =
                result.Manifest.Tables.Sum(
                    static table =>
                        table.RowCount),
        };

    private static ValueTask<int>
        RunRetainedApplyAsync(
        SqlServerRetainedArtifacts artifacts,
        string planPath,
        string targetPath,
        string reportPath,
        string packageDigest,
        bool resume,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        var arguments =
            new List<string>
            {
                "migrate", "apply", planPath,
                "--catalog",
                artifacts.CatalogPath,
                "--source-package",
                artifacts.PackagePath,
                "--expected-manifest-digest",
                packageDigest,
                "--workspace",
                Path.GetDirectoryName(
                    artifacts.PackagePath)!,
                "--target", targetPath,
                "--out", reportPath,
                "--format", "json",
            };
        if (resume)
            arguments.Add("--resume");
        return MigrationCommandRunner.RunAsync(
            arguments.ToArray(),
            output,
            error,
            ct);
    }

    private static async ValueTask<MigrationCatalog> CreateSqlServerCatalogAsync(
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
                Kind = MigrationSourceKind.SqlServer,
                Identity = "sqlserver:test-schema-v1",
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
                : [],
        };

        return MigrationArtifactSerializer.DeserializeCatalog(
            MigrationArtifactSerializer.SerializeCatalog(candidate));
    }

    private static string CreateTempDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_sqlserver_cli_{Guid.NewGuid():N}");
        MigrationCommandRunner
            .SqlServerCaptureWorkspace
            .CreatePrivateDirectoryExclusive(directory);
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

    private sealed record CapturedRetainedPackage(
        RetainedMigrationPackageWriteResult Result,
        MigrationCatalog Catalog);

    private sealed record SqlServerRetainedArtifacts(
        string PackagePath,
        string CatalogPath,
        string PackageDigest);

}
