using CSharpDB.Migration.MySql.Worker;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlWorkerRunnerTests
{
    private const string EnvironmentVariableName =
        "CSHARPDB_MYSQL_WORKER_TEST";
    private const string SecretConnectionString =
        "Server=private.example;Database=source;User ID=reader;Password=never-print-this";

    [Fact]
    public async Task Success_EmitsExactHeaderAndCanonicalCatalogOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = BuildCatalog(ct);
        string? observedEnvironmentName = null;
        string? observedConnectionString = null;
        string? clearedEnvironmentName = null;
        MigrationInspectionRequest? observedRequest = null;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = name =>
            {
                observedEnvironmentName = name;
                return SecretConnectionString;
            },
            ClearEnvironmentVariable =
                name => clearedEnvironmentName = name,
            CreateInspector = connectionString =>
            {
                Assert.Equal(
                    EnvironmentVariableName,
                    clearedEnvironmentName);
                observedConnectionString = connectionString;
                return new FakeInspector(
                    MigrationSourceKind.MySql,
                    (request, cancellationToken) =>
                    {
                        observedRequest = request;
                        cancellationToken.ThrowIfCancellationRequested();
                        return ValueTask.FromResult(catalog);
                    });
            },
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(MySqlWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        Assert.Equal(
            MySqlWorkerRunner.SuccessHeader +
                MigrationArtifactSerializer.SerializeCatalog(
                    catalog,
                    writeIndented: false),
            result.Output);
        Assert.Equal(EnvironmentVariableName, observedEnvironmentName);
        Assert.Equal(EnvironmentVariableName, clearedEnvironmentName);
        Assert.Equal(SecretConnectionString, observedConnectionString);
        Assert.NotNull(observedRequest);
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            observedRequest.TargetCSharpDbVersion);
        Assert.False(observedRequest.IncludeProfile);

        string payload =
            result.Output[MySqlWorkerRunner.SuccessHeader.Length..];
        MigrationCatalog emitted =
            MigrationArtifactSerializer.DeserializeCatalog(payload);
        Assert.Equal(MigrationSourceKind.MySql, emitted.Source.Kind);
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            emitted.TargetCSharpDbVersion);
        AssertSecretAbsent(result.Output, result.Error);
    }

    [Fact]
    public async Task Invocation_MustMatchExactOrderedProtocol()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string[][] invalidArguments =
        [
            [],
            ["--protocol", MySqlWorkerRunner.Protocol],
            [
                "--protocol", "csharpdb-mysql-worker/v2",
                "--connection-env", EnvironmentVariableName,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--connection-env", EnvironmentVariableName,
                "--protocol", MySqlWorkerRunner.Protocol,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", MySqlWorkerRunner.Protocol,
                "--connection-env", "9INVALID",
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", MySqlWorkerRunner.Protocol,
                "--connection-env", "INVALID-NAME",
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", MySqlWorkerRunner.Protocol,
                "--connection-env", EnvironmentVariableName,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                "--extra",
            ],
        ];

        foreach (string[] args in invalidArguments)
        {
            bool environmentRead = false;
            bool inspectorCreated = false;
            var dependencies = new MySqlWorkerDependencies
            {
                ReadEnvironmentVariable = _ =>
                {
                    environmentRead = true;
                    return SecretConnectionString;
                },
                CreateInspector = _ =>
                {
                    inspectorCreated = true;
                    throw new InvalidOperationException();
                },
            };

            WorkerResult result = await RunAsync(args, dependencies, ct);

            Assert.Equal(
                MySqlWorkerRunner.ExitIncompatible,
                result.ExitCode);
            Assert.Equal(string.Empty, result.Output);
            Assert.Equal(
                MySqlWorkerRunner.Protocol +
                    ":error:incompatible" +
                    "\n",
                result.Error);
            Assert.False(environmentRead);
            Assert.False(inspectorCreated);
        }
    }

    [Fact]
    public async Task UnsupportedTarget_IsRejectedBeforeEnvironmentAccess()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool environmentRead = false;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = _ =>
            {
                environmentRead = true;
                return SecretConnectionString;
            },
        };
        string[] args = ValidArguments();
        args[5] = "999.0.0";

        WorkerResult result = await RunAsync(args, dependencies, ct);

        Assert.Equal(
            MySqlWorkerRunner.ExitIncompatible,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:incompatible" +
                "\n",
            result.Error);
        Assert.False(environmentRead);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task MissingConnection_ReturnsStableSecretFreeFailure(
        string? connectionString)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool inspectorCreated = false;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = name =>
            {
                Assert.Equal(EnvironmentVariableName, name);
                return connectionString;
            },
            CreateInspector = _ =>
            {
                inspectorCreated = true;
                throw new InvalidOperationException();
            },
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(
            MySqlWorkerRunner.ExitConnectionUnavailable,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:connection-unavailable" +
                "\n",
            result.Error);
        Assert.False(inspectorCreated);
        AssertSecretAbsent(result.Output, result.Error);
    }

    [Fact]
    public async Task EnvironmentFailure_DoesNotPublishExceptionOrSecret()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = _ =>
                throw new InvalidOperationException(SecretConnectionString),
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(
            MySqlWorkerRunner.ExitConnectionUnavailable,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:connection-unavailable" +
                "\n",
            result.Error);
        AssertSecretAbsent(result.Output, result.Error);
    }

    [Fact]
    public async Task InspectionFailure_DoesNotPublishProviderDetails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = _ => SecretConnectionString,
            CreateInspector = _ =>
                new FakeInspector(
                    MigrationSourceKind.MySql,
                    (_, _) => throw new InvalidOperationException(
                        SecretConnectionString)),
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(
            MySqlWorkerRunner.ExitInspectionFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:inspection-failed" +
                "\n",
            result.Error);
        AssertSecretAbsent(result.Output, result.Error);
    }

    [Fact]
    public async Task Cancellation_UsesStableInspectionFailure()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        bool environmentRead = false;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = _ =>
            {
                environmentRead = true;
                return SecretConnectionString;
            },
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            cancellation.Token);

        Assert.Equal(
            MySqlWorkerRunner.ExitInspectionFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:inspection-failed" +
                "\n",
            result.Error);
        Assert.False(environmentRead);
    }

    [Fact]
    public async Task InspectorWithWrongSourceKind_FailsInternalContract()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dependencies = new MySqlWorkerDependencies
        {
            ReadEnvironmentVariable = _ => SecretConnectionString,
            CreateInspector = _ =>
                new FakeInspector(
                    MigrationSourceKind.Sqlite,
                    (_, _) => throw new InvalidOperationException()),
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        AssertInternalFailure(result);
    }

    [Fact]
    public async Task CatalogWithWrongSourceKindOrTarget_FailsInternalContract()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MigrationCatalog valid = BuildCatalog(ct);
        MigrationCatalog wrongSource = valid with
        {
            Source = valid.Source with
            {
                Kind = MigrationSourceKind.Sqlite,
            },
        };
        MigrationCatalog wrongTarget = valid with
        {
            TargetCSharpDbVersion = "999.0.0",
        };

        foreach (MigrationCatalog catalog in new[] { wrongSource, wrongTarget })
        {
            var dependencies = new MySqlWorkerDependencies
            {
                ReadEnvironmentVariable = _ => SecretConnectionString,
                CreateInspector = _ =>
                    new FakeInspector(
                        MigrationSourceKind.MySql,
                        (_, _) => ValueTask.FromResult(catalog)),
            };

            WorkerResult result = await RunAsync(
                ValidArguments(),
                dependencies,
                ct);

            AssertInternalFailure(result);
        }
    }

    [Fact]
    public async Task OversizedOrInvalidSerialization_FailsBeforeStdout()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = BuildCatalog(ct);
        foreach (Func<MigrationCatalog, string> serializer in new Func<
                     MigrationCatalog,
                     string>[]
                 {
                     _ => "payload",
                     _ => throw new InvalidDataException(
                         SecretConnectionString),
                 })
        {
            var dependencies = new MySqlWorkerDependencies
            {
                ReadEnvironmentVariable = _ => SecretConnectionString,
                CreateInspector = _ =>
                    new FakeInspector(
                        MigrationSourceKind.MySql,
                        (_, _) => ValueTask.FromResult(catalog)),
                SerializeCatalog = serializer,
                MeasureUtf8Bytes = _ =>
                    MySqlWorkerRunner.MaxCatalogBytes + 1,
            };

            WorkerResult result = await RunAsync(
                ValidArguments(),
                dependencies,
                ct);

            AssertInternalFailure(result);
            AssertSecretAbsent(result.Output, result.Error);
        }
    }

    private static MigrationCatalog BuildCatalog(
        CancellationToken cancellationToken = default) =>
        MySqlCatalogBuilder.Build(
            MySqlTestSnapshot.Create(),
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = false,
            },
            MySqlInspectionLimits.Default,
            cancellationToken);

    private static string[] ValidArguments() =>
    [
        "--protocol", MySqlWorkerRunner.Protocol,
        "--connection-env", EnvironmentVariableName,
        "--target-version",
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
    ];

    private static async ValueTask<WorkerResult> RunAsync(
        string[] args,
        MySqlWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int exitCode = await MySqlWorkerRunner.RunAsync(
            args,
            output,
            error,
            dependencies,
            cancellationToken);
        return new WorkerResult(
            exitCode,
            output.ToString(),
            error.ToString());
    }

    private static void AssertInternalFailure(WorkerResult result)
    {
        Assert.Equal(
            MySqlWorkerRunner.ExitInternalFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            MySqlWorkerRunner.Protocol +
                ":error:internal-failure" +
                "\n",
            result.Error);
    }

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

    private sealed record WorkerResult(
        int ExitCode,
        string Output,
        string Error);

    private sealed class FakeInspector(
        MigrationSourceKind sourceKind,
        Func<
            MigrationInspectionRequest,
            CancellationToken,
            ValueTask<MigrationCatalog>> inspect)
        : IMigrationSourceInspector
    {
        public MigrationSourceKind SourceKind => sourceKind;

        public ValueTask<MigrationCatalog> InspectAsync(
            MigrationInspectionRequest request,
            CancellationToken cancellationToken = default) =>
            inspect(request, cancellationToken);
    }
}
