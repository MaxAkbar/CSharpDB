using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;
using CSharpDB.Migration.Validation;

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
    public async Task
        CaptureInspect_ForwardsLimitsVerifiesPackageAndRemovesWorkspace()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string packagePath =
            Path.Combine(
                directory,
                "captured.csdbmysql");
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
                    CaptureMySqlAsync =
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
                            return MySqlCaptureWorkerResult
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
                        "--source", "mysql",
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
                InspectorCommandRunner.ExitWarn,
                exitCode);
            Assert.Equal(42, capturedTimeout);
            Assert.True(File.Exists(packagePath));
            Assert.True(File.Exists(catalogPath));
            MigrationCatalog published =
                MigrationArtifactSerializer
                    .DeserializeCatalog(
                        await File.ReadAllTextAsync(
                            catalogPath,
                            ct));
            Assert.Equal(
                MigrationSourceKind.MySql,
                published.Source.Kind);
            Assert.True(
                string.IsNullOrWhiteSpace(
                    error.ToString()));
            Assert.Empty(
                Directory.EnumerateDirectories(
                    directory,
                    MySqlWorkerClient
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
    public async Task
        CaptureInspect_RejectsSelfConsistentRowSubstitutionBeforePublication()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string directory = CreateTempDirectory();
        string packagePath =
            Path.Combine(
                directory,
                "must-not-publish.csdbmysql");
        string catalogPath =
            Path.Combine(
                directory,
                "must-not-publish.json");
        string baselinePath =
            Path.Combine(
                directory,
                "substitution-baseline.csdbmysql");
        try
        {
            var dependencies =
                new MigrationCommandDependencies
                {
                    CaptureMySqlAsync =
                        async (
                            _,
                            targetVersion,
                            temporaryPath,
                            maximumBytes,
                            _,
                            cancellationToken) =>
                        {
                            CapturedRetainedPackage baseline =
                                await WriteRetainedPackageAsync(
                                    baselinePath,
                                    targetVersion,
                                   maximumBytes,
                                   cancellationToken);
                            RetainedMigrationPackageWriteResult
                                substituted =
                                await WriteSubstitutedRetainedPackageAsync(
                                    temporaryPath,
                                    baseline.Catalog,
                                    maximumBytes,
                                    cancellationToken);
                            return MySqlCaptureWorkerResult
                                .Success(
                                    Receipt(
                                        temporaryPath,
                                        substituted));
                        },
                };
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "inspect",
                        "--source", "mysql",
                        "--connection-env",
                        EnvironmentVariableName,
                        "--package", packagePath,
                        "--out", catalogPath,
                    ],
                    TextWriter.Null,
                    error,
                    dependencies,
                    ct);

            Assert.Equal(
                InspectorCommandRunner.ExitError,
                exitCode);
            Assert.False(File.Exists(packagePath));
            Assert.False(File.Exists(catalogPath));
            Assert.Empty(
                Directory.EnumerateDirectories(
                    directory,
                    MySqlWorkerClient
                        .CaptureWorkspacePrefix +
                    "*",
                    SearchOption.TopDirectoryOnly));
            AssertSecretAbsent(error.ToString());
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
                "preserved.csdbmysql");
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
                    CaptureMySqlAsync =
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
                            return MySqlCaptureWorkerResult
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
                        "--source", "mysql",
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
                "MIG-MYSQL-CLI-CATALOG-001",
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
                    MySqlWorkerClient
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
                    CaptureMySqlAsync =
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
                        "--source", "mysql",
                        "--connection-env",
                        EnvironmentVariableName,
                        "--package",
                        Path.Combine(
                            directory,
                            "source.csdbmysql"),
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
            MySqlRetainedArtifacts artifacts =
                await CreateRetainedArtifactsAsync(
                    directory,
                    ct);
            MigrationCatalog retainedCatalog =
                MigrationArtifactSerializer
                    .DeserializeCatalog(
                        await File.ReadAllTextAsync(
                            artifacts.CatalogPath,
                            ct));
            string[] retainedRules =
                retainedCatalog.Diagnostics
                    .Select(static item =>
                        item.RuleId)
                    .ToArray();
            Assert.DoesNotContain(
                "MIG-MYSQL-INVENTORY-PARTIAL-001",
                retainedRules);
            Assert.DoesNotContain(
                "MIG-MYSQL-METADATA-COMPLETENESS-UNKNOWN-001",
                retainedRules);
            Assert.DoesNotContain(
                "MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001",
                retainedRules);
            Assert.Contains(
                "MIG-MYSQL-RETAINED-SCOPE-001",
                retainedRules);
            Assert.Contains(
                "MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001",
                retainedRules);
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
                    new StringWriter(),
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
                    "tampered.csdbmysql");
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
            MySqlRetainedArtifacts tamperedArtifacts =
                artifacts with
                {
                    PackagePath =
                        tamperedPackage,
                };
            string tamperedTarget =
                Path.Combine(
                    directory,
                    "tampered-target.csdb");
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
                    new StringWriter(),
                    ct);
            Assert.Equal(
                InspectorCommandRunner.ExitError,
                tamperedCode);
            Assert.False(
                File.Exists(tamperedTarget));
            Assert.False(
                File.Exists(
                    tamperedTarget + ".wal"));

            string substitutedPackage =
                Path.Combine(
                    directory,
                    "substituted.csdbmysql");
            RetainedMigrationPackageWriteResult
                substitutedResult =
                await WriteSubstitutedRetainedPackageAsync(
                    substitutedPackage,
                    retainedCatalog,
                    new RetainedMigrationPackageWriteOptions()
                        .MaxPackageBytes,
                    ct);
            Assert.NotEqual(
                retainedCatalog.Source.Fingerprint,
                substitutedResult.Manifest
                    .ContentDigest);
            MySqlRetainedArtifacts substitutedArtifacts =
                artifacts with
                {
                    PackagePath =
                        substitutedPackage,
                };
            string substitutedTarget =
                Path.Combine(
                    directory,
                    "substituted-target.csdb");
            int substitutedCode =
                await RunRetainedApplyAsync(
                    substitutedArtifacts,
                    planPath,
                    substitutedTarget,
                    Path.Combine(
                        directory,
                        "substituted-run.json"),
                    substitutedResult.PackageDigest,
                    resume: false,
                    TextWriter.Null,
                    new StringWriter(),
                    ct);
            Assert.Equal(
                InspectorCommandRunner.ExitError,
                substitutedCode);
            Assert.False(
                File.Exists(substitutedTarget));
            Assert.False(
                File.Exists(
                    substitutedTarget + ".wal"));

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
                await CreateMySqlCatalogAsync(
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
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    arguments,
                    TextWriter.Null,
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

    private static async ValueTask<
        MySqlRetainedArtifacts>
        CreateRetainedArtifactsAsync(
        string directory,
        CancellationToken ct)
    {
        string packagePath =
            Path.Combine(
                directory,
                "source.csdbmysql");
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
        return new MySqlRetainedArtifacts(
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
            "mysql:table:test:items";
        const string columnId =
            "mysql:column:test:items:id";
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
                                    CreateRetainedMySqlCatalog(
                                        targetVersion,
                                        summary);
                                return ValueTask.FromResult(
                                    new RetainedMigrationCatalogBinding
                                    {
                                        Catalog = catalog,
                                        SnapshotIdentity =
                                            "mysql-retained:" +
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
                    "The retained MySQL test catalog was not created."));
    }

    private static MigrationCatalog
        CreateRetainedMySqlCatalog(
        string targetVersion,
        RetainedMigrationContentSummary summary) =>
        new()
        {
            TargetCSharpDbVersion =
                targetVersion,
            Source =
                new MigrationSourceIdentity
                {
                    Kind =
                        MigrationSourceKind
                            .MySql,
                    Identity =
                        "mysql:test-retained-v1",
                    Fingerprint =
                        summary.ContentDigest,
                    ProviderVersion =
                        "test-provider-v1",
                    SourceVersion =
                        "8.4",
                    Consistency =
                        new MigrationConsistencyStrategy
                        {
                            Kind =
                                MigrationConsistencyKind
                                    .Snapshot,
                            Description =
                                "Deterministic retained MySQL test snapshot.",
                        },
                },
            Objects =
            [
                new MigrationCatalogObject
                {
                    ObjectId =
                        "mysql:database:test",
                    Kind =
                        MigrationObjectKind.Database,
                    SourceName = "test",
                    Facets =
                    [
                        Facet(
                            "mysqlAnalyzerCatalogContract",
                            "csharpdb-mysql-catalog/v3"),
                        Facet(
                            "mysqlCatalogContract",
                            "csharpdb-mysql-retained-catalog/v1"),
                        Facet(
                            "mysqlDataContract",
                            "csharpdb-mysql-retained-data/v1"),
                        Facet(
                            "mysqlRetainedContentDigest",
                            summary.ContentDigest),
                        Facet(
                            "mysqlRetainedSnapshotIdentity",
                            "mysql-retained:" +
                            summary.ContentDigest),
                        Facet(
                            "mysqlRetainedMetadataScope",
                            "ordinary-base-tables"),
                        Facet(
                            "mysqlRetainedDirectSchemaSelectProven",
                            "true"),
                        Facet(
                            "mysqlMetadataVisibilityProofAttempted",
                            "true"),
                        Facet(
                            "mysqlMetadataVisibilityAccountFormatSupported",
                            "true"),
                        Facet(
                            "mysqlMetadataVisibilityGranteeMatched",
                            "true"),
                        Facet(
                            "mysqlDirectSchemaSelect",
                            "true"),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "mysql:namespace:test",
                    Kind =
                        MigrationObjectKind.Namespace,
                    ParentObjectId =
                        "mysql:database:test",
                    SourceName = "test",
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "mysql:table:test:items",
                    Kind =
                        MigrationObjectKind.Table,
                    ParentObjectId =
                        "mysql:namespace:test",
                    SourceNamespace = "test",
                    SourceName = "items",
                    Facets =
                    [
                        Facet(
                            "mysqlTableType",
                            "BASE TABLE"),
                        Facet(
                            "mysqlEngine",
                            "InnoDB"),
                        Facet(
                            "mysqlPartitioned",
                            "false"),
                        Facet(
                            MigrationDataAvailabilityContract
                                .AvailableFacet,
                            "true"),
                        Facet(
                            "mysqlRowOrderContract",
                            "csharpdb-mysql-integer-key-order/v1"),
                        Facet(
                            "mysqlRowOrderKind",
                            "primary"),
                        Facet(
                            "mysqlRowOrderObjectId",
                            "mysql:key:test:items:pk"),
                        Facet(
                            "mysqlRetainedRowCount",
                            summary.Tables
                                .Single()
                                .RowCount
                                .ToString(
                                    System.Globalization
                                        .CultureInfo
                                        .InvariantCulture)),
                        Facet(
                            "mysqlRetainedSectionDigest",
                            summary.Tables
                                .Single()
                                .SectionDigest),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "mysql:column:test:items:id",
                    Kind =
                        MigrationObjectKind.Column,
                    ParentObjectId =
                        "mysql:table:test:items",
                    SourceNamespace = "test",
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
                        Facet(
                            "mysqlOrdinalPosition",
                            "1"),
                        Facet(
                            "mysqlDataType",
                            "bigint"),
                        Facet(
                            "mysqlUnsigned",
                            "false"),
                        Facet(
                            "mysqlGenerated",
                            "false"),
                        Facet(
                            "mysqlInvisible",
                            "false"),
                        Facet(
                            "mysqlZerofill",
                            "false"),
                        Facet(
                            "mysqlColumnDataAvailable",
                            "true"),
                        Facet(
                            "mysqlScalarCodecContract",
                            "csharpdb-mysql-scalar/v1"),
                        Facet(
                            "mysqlScalarCodec",
                            "signed-integer"),
                    ],
                },
                new MigrationCatalogObject
                {
                    ObjectId =
                        "mysql:key:test:items:pk",
                    Kind =
                        MigrationObjectKind.Key,
                    ParentObjectId =
                        "mysql:table:test:items",
                    SourceNamespace = "test",
                    SourceName = "PRIMARY",
                    Facets =
                    [
                        Facet("kind", "primary"),
                        Facet(
                            "mysqlMembershipComplete",
                            "true"),
                        Facet(
                            "mysqlBackingIndexMatched",
                            "true"),
                    ],
                    Members =
                    [
                        new MigrationObjectReference
                        {
                            ObjectId =
                                "mysql:column:test:items:id",
                            Role =
                                MigrationObjectReferenceRoles
                                    .Column,
                            Ordinal = 0,
                        },
                    ],
                    DependsOn =
                    [
                        "mysql:column:test:items:id",
                    ],
                },
            ],
            Diagnostics =
            [
                RetainedDiagnostic(
                    "mysql:diag:retained-scope",
                    "MIG-MYSQL-RETAINED-SCOPE-001",
                    "Retained MySQL v1 is scoped to ordinary base tables and rows."),
                RetainedDiagnostic(
                    "mysql:diag:retained-live-qualification-deferred",
                    "MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001",
                    "The retained MySQL package has not completed live qualification."),
            ],
        };

    private static MigrationDiagnostic RetainedDiagnostic(
        string diagnosticId,
        string ruleId,
        string summary) =>
        new()
        {
            DiagnosticId = diagnosticId,
            RuleId = ruleId,
            Severity =
                MigrationDiagnosticSeverity.Warning,
            Status =
                MigrationCompatibilityStatus.Conditional,
            Evidence = MigrationEvidenceLevel.Bound,
            Summary = summary,
            Explanation =
                "The retained package binds its supported table-and-row scope without claiming broader programmable-object or live-server qualification.",
            ObjectId = "mysql:database:test",
            Remediation =
                "Review the retained scope and complete any separately applicable qualification.",
            CanOverride = false,
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

    private static async ValueTask<
        RetainedMigrationPackageWriteResult>
        WriteSubstitutedRetainedPackageAsync(
        string outputPath,
        MigrationCatalog catalog,
        long maxPackageBytes,
        CancellationToken ct) =>
        await RetainedMigrationPackageWriter
            .WriteAsync(
                new RetainedMigrationPackageWriteRequest
                {
                    OutputPath = outputPath,
                    Catalog = catalog,
                    SnapshotIdentity =
                        "mysql-retained:" +
                        catalog.Source.Fingerprint,
                    Tables =
                    [
                        new RetainedMigrationTableWrite
                        {
                            Descriptor =
                                RetainedDescriptor(),
                            Rows =
                                SubstitutedRetainedRows(),
                        },
                    ],
                    Options =
                        new RetainedMigrationPackageWriteOptions
                        {
                            MaxPackageBytes =
                                maxPackageBytes,
                        },
                },
                ct);

    private static async IAsyncEnumerable<
        MigrationDataRow> SubstitutedRetainedRows()
    {
        await Task.Yield();
        yield return new MigrationDataRow
        {
            StableKey = "id:999",
            Values =
            [
                new MigrationSourceValue
                {
                    Kind =
                        MigrationSourceValueKind
                            .SignedInteger,
                    CanonicalText = "999",
                },
            ],
        };
    }

    private static RetainedMigrationTableDescriptor
        RetainedDescriptor() => new()
        {
            SourceObjectId =
                "mysql:table:test:items",
            ColumnObjectIds =
                ["mysql:column:test:items:id"],
            OrderingKeyColumnObjectIds =
                ["mysql:column:test:items:id"],
        };

    private static MySqlCaptureReceipt Receipt(
        string packagePath,
        RetainedMigrationPackageWriteResult result) =>
        new()
        {
            Format =
                MySqlCaptureReceipt
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
        MySqlRetainedArtifacts artifacts,
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

    private sealed record MySqlRetainedArtifacts(
        string PackagePath,
        string CatalogPath,
        string PackageDigest);

}
