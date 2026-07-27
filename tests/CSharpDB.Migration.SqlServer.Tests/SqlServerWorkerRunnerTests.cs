using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Compatibility;
using CSharpDB.Migration.SqlServer.Worker;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerWorkerRunnerTests
{
    private const string EnvironmentVariableName =
        "CSHARPDB_SQLSERVER_WORKER_TEST";
    private const string SecretConnectionString =
        "Server=private.example;Database=source;User ID=reader;Password=never-print-this";

    [Fact]
    public async Task Success_EmitsExactHeaderAndCanonicalCatalogOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MigrationCatalog catalog = BuildCatalog(ct);
        string? observedEnvironmentName = null;
        string? observedConnectionString = null;
        MigrationInspectionRequest? observedRequest = null;
        var dependencies = new SqlServerWorkerDependencies
        {
            ReadEnvironmentVariable = name =>
            {
                observedEnvironmentName = name;
                return SecretConnectionString;
            },
            CreateInspector = connectionString =>
            {
                observedConnectionString = connectionString;
                return new FakeInspector(
                    MigrationSourceKind.SqlServer,
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

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        Assert.Equal(
            SqlServerWorkerRunner.SuccessHeader +
                MigrationArtifactSerializer.SerializeCatalog(
                    catalog,
                    writeIndented: false),
            result.Output);
        Assert.Equal(EnvironmentVariableName, observedEnvironmentName);
        Assert.Equal(SecretConnectionString, observedConnectionString);
        Assert.NotNull(observedRequest);
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            observedRequest.TargetCSharpDbVersion);
        Assert.False(observedRequest.IncludeProfile);

        string payload =
            result.Output[SqlServerWorkerRunner.SuccessHeader.Length..];
        MigrationCatalog emitted =
            MigrationArtifactSerializer.DeserializeCatalog(payload);
        Assert.Equal(MigrationSourceKind.SqlServer, emitted.Source.Kind);
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
            ["--protocol", SqlServerWorkerRunner.Protocol],
            [
                "--protocol", "csharpdb-sqlserver-worker/v2",
                "--connection-env", EnvironmentVariableName,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--connection-env", EnvironmentVariableName,
                "--protocol", SqlServerWorkerRunner.Protocol,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", SqlServerWorkerRunner.Protocol,
                "--connection-env", "9INVALID",
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", SqlServerWorkerRunner.Protocol,
                "--connection-env", "INVALID-NAME",
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol", SqlServerWorkerRunner.Protocol,
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
            var dependencies = new SqlServerWorkerDependencies
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
                SqlServerWorkerRunner.ExitIncompatible,
                result.ExitCode);
            Assert.Equal(string.Empty, result.Output);
            Assert.Equal(
                SqlServerWorkerRunner.Protocol +
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
        var dependencies = new SqlServerWorkerDependencies
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
            SqlServerWorkerRunner.ExitIncompatible,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
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
        var dependencies = new SqlServerWorkerDependencies
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
            SqlServerWorkerRunner.ExitConnectionUnavailable,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
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
        var dependencies = new SqlServerWorkerDependencies
        {
            ReadEnvironmentVariable = _ =>
                throw new InvalidOperationException(SecretConnectionString),
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(
            SqlServerWorkerRunner.ExitConnectionUnavailable,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
                ":error:connection-unavailable" +
                "\n",
            result.Error);
        AssertSecretAbsent(result.Output, result.Error);
    }

    [Fact]
    public async Task InspectionFailure_DoesNotPublishProviderDetails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dependencies = new SqlServerWorkerDependencies
        {
            ReadEnvironmentVariable = _ => SecretConnectionString,
            CreateInspector = _ =>
                new FakeInspector(
                    MigrationSourceKind.SqlServer,
                    (_, _) => throw new InvalidOperationException(
                        SecretConnectionString)),
        };

        WorkerResult result = await RunAsync(
            ValidArguments(),
            dependencies,
            ct);

        Assert.Equal(
            SqlServerWorkerRunner.ExitInspectionFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
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
        var dependencies = new SqlServerWorkerDependencies
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
            SqlServerWorkerRunner.ExitInspectionFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
                ":error:inspection-failed" +
                "\n",
            result.Error);
        Assert.False(environmentRead);
    }

    [Fact]
    public async Task InspectorWithWrongSourceKind_FailsInternalContract()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var dependencies = new SqlServerWorkerDependencies
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
            var dependencies = new SqlServerWorkerDependencies
            {
                ReadEnvironmentVariable = _ => SecretConnectionString,
                CreateInspector = _ =>
                    new FakeInspector(
                        MigrationSourceKind.SqlServer,
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
            var dependencies = new SqlServerWorkerDependencies
            {
                ReadEnvironmentVariable = _ => SecretConnectionString,
                CreateInspector = _ =>
                    new FakeInspector(
                        MigrationSourceKind.SqlServer,
                        (_, _) => ValueTask.FromResult(catalog)),
                SerializeCatalog = serializer,
                MeasureUtf8Bytes = _ =>
                    SqlServerWorkerRunner.MaxCatalogBytes + 1,
            };

            WorkerResult result = await RunAsync(
                ValidArguments(),
                dependencies,
                ct);

            AssertInternalFailure(result);
            AssertSecretAbsent(result.Output, result.Error);
        }
    }

    [Fact]
    public async Task QueryDefaultDependencies_ProduceValidatedTsqlReport()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        const string query =
            "SELECT TOP (10) id FROM widgets ORDER BY id;";

        WorkerResult result = await RunQueryAsync(
            Encoding.UTF8.GetBytes(query),
            SqlServerWorkerDependencies.Default,
            ct);

        Assert.Equal(
            SqlServerWorkerRunner.ExitSuccess,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        Assert.StartsWith(
            SqlServerWorkerRunner.QuerySuccessHeader,
            result.Output,
            StringComparison.Ordinal);

        string json = result.Output[
            SqlServerWorkerRunner.QuerySuccessHeader.Length..];
        using JsonDocument document =
            JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        Assert.Equal(
            QueryCompatibilityReportFormats.V1,
            root.GetProperty("format").GetString());
        JsonElement queryResult =
            root.GetProperty("results")[0];
        Assert.Equal(
            "sqlserver-query",
            queryResult.GetProperty("queryId").GetString());
        Assert.Equal(
            "sqlServerTsql",
            queryResult
                .GetProperty("sourceDialect")
                .GetString());
        Assert.Equal(
            "tsql-top-integer-to-csharpdb-limit/v1",
            queryResult
                .GetProperty("rewrite")
                .GetProperty("rewriteId")
                .GetString());
    }

    [Fact]
    public async Task QueryInvalidCompatibilityLevel_FailsClosed()
    {
        string[] args = ValidQueryArguments();
        args[^1] = "140";

        WorkerResult result = await RunQueryAsync(
            args,
            new MemoryStream(
                "SELECT 1;"u8.ToArray(),
                writable: false),
            SqlServerWorkerDependencies.Default,
            TestContext.Current.CancellationToken);

        Assert.Equal(
            SqlServerWorkerRunner.ExitIncompatible,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.QueryProtocol +
                ":error:incompatible\n",
            result.Error);
    }

    [Fact]
    public async Task DdlSuccess_ReadsRawStdinStripsBomAndEmitsCanonicalReport()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string script =
            "CREATE TABLE [dbo].[widgets] ([id] int NOT NULL);";
        string? observedScript = null;
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (source, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                observedScript = source;
                return ValueTask.FromResult(BuildDdlReport(source));
            },
        };

        WorkerResult first = await RunDdlAsync(
            Utf8WithBom(script),
            dependencies,
            ct);
        WorkerResult repeated = await RunDdlAsync(
            Utf8WithBom(script),
            dependencies,
            ct);

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, first.ExitCode);
        Assert.Equal(string.Empty, first.Error);
        Assert.Equal(script, observedScript);
        Assert.Equal(first.Output, repeated.Output);
        Assert.StartsWith(
            SqlServerWorkerRunner.DdlSuccessHeader,
            first.Output,
            StringComparison.Ordinal);

        string json = first.Output[
            SqlServerWorkerRunner.DdlSuccessHeader.Length..];
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        Assert.Equal(
            CSharpDbDdlCompatibilityReport.CurrentFormat,
            root.GetProperty("format").GetString());
        Assert.Equal(
            "tsql",
            root.GetProperty("dialect").GetString());
        Assert.Equal(
            "tsql160",
            root.GetProperty("sourceGrammar").GetString());
        Assert.Equal(
            "compatibleWithRewrite",
            root.GetProperty("status").GetString());
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            root.GetProperty("targetCSharpDbVersion").GetString());
        AssertSecretAbsent(first.Output, first.Error);
    }

    [Fact]
    public async Task DdlDefaultDependencies_ProduceValidatedTsql160Report()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string privateIdentifier =
            "PrivateWorkerTable_48D2";
        string script =
            $"CREATE TABLE [dbo].[{privateIdentifier}] " +
            "([id] int NOT NULL PRIMARY KEY);";

        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            SqlServerWorkerDependencies.Default,
            ct);

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        Assert.StartsWith(
            SqlServerWorkerRunner.DdlSuccessHeader,
            result.Output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            privateIdentifier,
            result.Output,
            StringComparison.Ordinal);

        using JsonDocument document = JsonDocument.Parse(
            result.Output[
                SqlServerWorkerRunner.DdlSuccessHeader.Length..]);
        Assert.Equal(
            "tsql160",
            document.RootElement
                .GetProperty("sourceGrammar")
                .GetString());
        Assert.Equal(
            "compatibleWithRewrite",
            document.RootElement
                .GetProperty("status")
                .GetString());
    }

    [Theory]
    [InlineData(
        "DROP TABLE dbo.widgets;",
        "unsupported",
        SqlServerTsqlDdlCompatibilityAnalyzer
            .UnsupportedStatementRuleId)]
    [InlineData(
        "CREATE TABLE dbo.widgets (label nvarchar(20) NOT NULL);",
        "conditional",
        SqlServerTsqlDdlCompatibilityAnalyzer
            .TextCollationRuleId)]
    [InlineData(
        "CREATE TABLE dbo.widgets (",
        "unsupported",
        SqlServerTsqlDdlCompatibilityAnalyzer.ParseRuleId)]
    public async Task DdlDefaultDependencies_AcceptCanonicalAnalyzerShapes(
        string script,
        string expectedStatus,
        string expectedRuleId)
    {
        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            SqlServerWorkerDependencies.Default,
            TestContext.Current.CancellationToken);

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        using JsonDocument document = JsonDocument.Parse(
            result.Output[
                SqlServerWorkerRunner.DdlSuccessHeader.Length..]);
        Assert.Equal(
            expectedStatus,
            document.RootElement.GetProperty("status").GetString());
        Assert.Equal(
            expectedRuleId,
            document.RootElement.GetProperty("ruleId").GetString());
    }

    [Fact]
    public async Task DdlCanonicalLoweringLimit_IsAccepted()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL);";
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (source, cancellationToken) =>
                SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                    source,
                    new SqlServerTsqlDdlCompatibilityOptions
                    {
                        MaxCatalogObjectCount = 1,
                    },
                    cancellationToken),
        };

        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            dependencies,
            TestContext.Current.CancellationToken);

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        using JsonDocument document = JsonDocument.Parse(
            result.Output[
                SqlServerWorkerRunner.DdlSuccessHeader.Length..]);
        Assert.Equal(
            SqlServerTsqlDdlCompatibilityAnalyzer.LimitRuleId,
            document.RootElement.GetProperty("ruleId").GetString());
        Assert.Equal(
            "parsed",
            document.RootElement
                .GetProperty("highestEvidence")
                .GetString());
    }

    [Fact]
    public async Task DdlDiagnosticProse_IsReplacedWithHostOwnedText()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL);";
        const string privateDiagnostic =
            "private-worker-diagnostic-7F4C";
        CSharpDbDdlCompatibilityReport report =
            BuildDdlReport(script);
        report = report with
        {
            Diagnostics =
            [
                report.Diagnostics[0] with
                {
                    Summary =
                        "\u001b[31m" +
                        privateDiagnostic +
                        "\r\nforged-summary",
                    Remediation =
                        "\u001b[0m" +
                        SecretConnectionString +
                        "\nforged-remediation",
                },
            ],
        };
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.FromResult(report);
            },
        };

        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            dependencies,
            TestContext.Current.CancellationToken);

        Assert.Equal(SqlServerWorkerRunner.ExitSuccess, result.ExitCode);
        Assert.Equal(string.Empty, result.Error);
        Assert.DoesNotContain(
            privateDiagnostic,
            result.Output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            SecretConnectionString,
            result.Output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "\u001b",
            result.Output,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "forged-",
            result.Output,
            StringComparison.Ordinal);

        using JsonDocument document = JsonDocument.Parse(
            result.Output[
                SqlServerWorkerRunner.DdlSuccessHeader.Length..]);
        JsonElement diagnostic = document.RootElement
            .GetProperty("diagnostics")[0];
        Assert.Equal(
            "The proven candidate requires a deterministic canonical rewrite.",
            diagnostic.GetProperty("summary").GetString());
        Assert.Equal(
            "Review the generated migration plan before any apply workflow.",
            diagnostic.GetProperty("remediation").GetString());
    }

    [Fact]
    public async Task DdlLoweringDiagnostics_MustRetainCanonicalOrderAndRoot()
    {
        const string script =
            "DROP TABLE dbo.widgets; " +
            "CREATE TABLE dbo.bad (payload xml NOT NULL);";
        CSharpDbDdlCompatibilityReport canonical =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken:
                    TestContext.Current.CancellationToken);
        Assert.Equal(2, canonical.Diagnostics.Count);
        CSharpDbDdlCompatibilityDiagnostic[] reversed =
            canonical.Diagnostics
                .Reverse()
                .Select((diagnostic, ordinal) =>
                    diagnostic with
                    {
                        Ordinal = ordinal,
                        DiagnosticId = string.Concat(
                            "tsql-ddl/",
                            ordinal.ToString(
                                "D6",
                                System.Globalization
                                    .CultureInfo.InvariantCulture),
                            "/",
                            diagnostic.RuleId),
                    })
                .ToArray();
        CSharpDbDdlCompatibilityReport forged = canonical with
        {
            RuleId = reversed[0].RuleId,
            Diagnostics = reversed,
        };
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.FromResult(forged);
            },
        };

        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            dependencies,
            TestContext.Current.CancellationToken);

        AssertDdlFailure(
            result,
            SqlServerWorkerRunner.ExitInternalFailure,
            "internal-failure");
    }

    [Fact]
    public async Task DdlParseDiagnostic_MustRemainAPointLocation()
    {
        const string script = "CREATE TABLE dbo.widgets (";
        CSharpDbDdlCompatibilityReport canonical =
            await SqlServerTsqlDdlCompatibilityAnalyzer.AnalyzeAsync(
                script,
                cancellationToken:
                    TestContext.Current.CancellationToken);
        CSharpDbDdlCompatibilityReport forged = canonical with
        {
            Diagnostics =
            [
                canonical.Diagnostics[0] with
                {
                    SourceSpan =
                        canonical.Diagnostics[0].SourceSpan! with
                        {
                            Length = 0,
                        },
                },
            ],
        };
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.FromResult(forged);
            },
        };

        WorkerResult result = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            dependencies,
            TestContext.Current.CancellationToken);

        AssertDdlFailure(
            result,
            SqlServerWorkerRunner.ExitInternalFailure,
            "internal-failure");
    }

    [Fact]
    public async Task DdlScratchDifferences_MustBeUniqueAndChanged()
    {
        const string script =
            "CREATE TABLE dbo.widgets (id int NOT NULL);";
        CSharpDbDdlCompatibilityReport canonical =
            BuildScratchDifferentReport(script);
        WorkerResult accepted = await RunDdlAsync(
            Encoding.UTF8.GetBytes(script),
            new SqlServerWorkerDependencies
            {
                AnalyzeDdlAsync = (_, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(canonical);
                },
            },
            TestContext.Current.CancellationToken);
        Assert.Equal(
            SqlServerWorkerRunner.ExitSuccess,
            accepted.ExitCode);

        CSharpDbDdlScratchValidationDifference difference =
            canonical.Differences[0];
        CSharpDbDdlCompatibilityReport[] forgedReports =
        [
            canonical with
            {
                Differences =
                [
                    difference,
                    difference with { Ordinal = 1 },
                ],
            },
            canonical with
            {
                Differences =
                [
                    difference with
                    {
                        ActualDefinitionDigest =
                            difference
                                .ExpectedDefinitionDigest,
                    },
                ],
            },
        ];
        foreach (CSharpDbDdlCompatibilityReport forged
                 in forgedReports)
        {
            WorkerResult result = await RunDdlAsync(
                Encoding.UTF8.GetBytes(script),
                new SqlServerWorkerDependencies
                {
                    AnalyzeDdlAsync = (_, cancellationToken) =>
                    {
                        cancellationToken
                            .ThrowIfCancellationRequested();
                        return ValueTask.FromResult(forged);
                    },
                },
                TestContext.Current.CancellationToken);

            AssertDdlFailure(
                result,
                SqlServerWorkerRunner.ExitInternalFailure,
                "internal-failure");
        }
    }

    [Fact]
    public async Task DdlInvocation_MustMatchExactOrderedProtocol()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string[][] invalidArguments =
        [
            [
                "--protocol",
                SqlServerWorkerRunner.DdlProtocol,
            ],
            [
                "--protocol",
                SqlServerWorkerRunner.DdlProtocol,
                "--connection-env",
                EnvironmentVariableName,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            ],
            [
                "--protocol",
                SqlServerWorkerRunner.DdlProtocol,
                "--target-version",
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                "--extra",
            ],
            [
                "--protocol",
                SqlServerWorkerRunner.DdlProtocol,
                "--target-version",
                "999.0.0",
            ],
        ];

        foreach (string[] args in invalidArguments)
        {
            bool analyzerInvoked = false;
            var dependencies = new SqlServerWorkerDependencies
            {
                AnalyzeDdlAsync = (_, _) =>
                {
                    analyzerInvoked = true;
                    throw new InvalidOperationException();
                },
            };

            WorkerResult result = await RunDdlAsync(
                args,
                new ThrowingReadStream(),
                dependencies,
                ct);

            AssertDdlFailure(
                result,
                SqlServerWorkerRunner.ExitIncompatible,
                "incompatible");
            Assert.False(analyzerInvoked);
        }
    }

    [Fact]
    public async Task DdlInput_MustBeStrictUtf8()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool analyzerInvoked = false;
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, _) =>
            {
                analyzerInvoked = true;
                throw new InvalidOperationException();
            },
        };

        WorkerResult result = await RunDdlAsync(
            [0x43, 0xC3, 0x28],
            dependencies,
            ct);

        AssertDdlFailure(
            result,
            SqlServerWorkerRunner.ExitIncompatible,
            "incompatible");
        Assert.False(analyzerInvoked);
    }

    [Fact]
    public async Task DdlInput_EnforcesByteAndCharacterBoundsBeforeAnalysis()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool analyzerInvoked = false;
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, _) =>
            {
                analyzerInvoked = true;
                throw new InvalidOperationException();
            },
        };
        Stream[] inputs =
        [
            new FixedLengthStream(
                (long)SqlServerWorkerRunner.MaxDdlInputBytes + 1,
                (byte)'x'),
            new FixedLengthStream(
                (long)SqlServerWorkerRunner.MaxDdlInputCharacters + 1,
                (byte)'x'),
        ];

        foreach (Stream input in inputs)
        {
            WorkerResult result = await RunDdlAsync(
                ValidDdlArguments(),
                input,
                dependencies,
                ct);

            AssertDdlFailure(
                result,
                SqlServerWorkerRunner.ExitIncompatible,
                "incompatible");
        }

        Assert.False(analyzerInvoked);
    }

    [Fact]
    public async Task DdlReport_MustMatchWorkerContract()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string script = "CREATE TABLE t (id int NOT NULL);";
        CSharpDbDdlCompatibilityReport valid =
            BuildDdlReport(script);
        CSharpDbDdlCompatibilityReport[] invalidReports =
        [
            valid with { Format = "csharpdb-ddl-compatibility/v2" },
            valid with { Dialect = "csharpdb" },
            valid with { SourceGrammar = "tsql150" },
            valid with { TargetCSharpDbVersion = "999.0.0" },
            valid with { CapabilityDigest = new string('A', 64) },
            valid with { ScriptDigest = new string('0', 64) },
            valid with { RuleId = "tsql.ddl.test" },
            valid with
            {
                Status = MigrationCompatibilityStatus.Compatible,
            },
            valid with
            {
                Status = (MigrationCompatibilityStatus)int.MaxValue,
            },
            valid with { StatementCount = -1 },
            valid with { StatementCount = 2 },
            valid with { ProvenStatementCount = 0 },
            valid with { CandidateActionCount = 0 },
            valid with { CatalogDigest = null },
            valid with
            {
                ExpectedSchemaDigest = new string('e', 64),
                ActualSchemaDigest = new string('f', 64),
            },
            valid with
            {
                Statements =
                [
                    valid.Statements[0] with { Index = 1 },
                ],
            },
            valid with
            {
                Statements =
                [
                    valid.Statements[0] with
                    {
                        Kind = "hostile-kind",
                    },
                ],
            },
            valid with
            {
                Statements =
                [
                    valid.Statements[0] with
                    {
                        Span = valid.Statements[0].Span with
                        {
                            Length = script.Length + 1,
                        },
                    },
                ],
            },
            valid with
            {
                Statements =
                [
                    valid.Statements[0] with
                    {
                        Span = valid.Statements[0].Span with
                        {
                            Line = 2,
                        },
                    },
                ],
            },
            valid with
            {
                Diagnostics =
                [
                    valid.Diagnostics[0] with
                    {
                        DiagnosticId =
                            "csharpdb-ddl/000000/hostile",
                    },
                ],
            },
            valid with
            {
                Diagnostics =
                [
                    valid.Diagnostics[0] with
                    {
                        Evidence = MigrationEvidenceLevel.Parsed,
                    },
                ],
            },
        ];

        foreach (CSharpDbDdlCompatibilityReport report
                 in invalidReports)
        {
            bool serialized = false;
            var dependencies = new SqlServerWorkerDependencies
            {
                AnalyzeDdlAsync = (_, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(report);
                },
                SerializeDdlReport = _ =>
                {
                    serialized = true;
                    return "must-not-serialize";
                },
            };

            WorkerResult result = await RunDdlAsync(
                Encoding.UTF8.GetBytes(script),
                dependencies,
                ct);

            AssertDdlFailure(
                result,
                SqlServerWorkerRunner.ExitInternalFailure,
                "internal-failure");
            Assert.False(serialized);
        }
    }

    [Fact]
    public async Task DdlOversizedOrInvalidSerialization_FailsBeforeStdout()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string script = "CREATE TABLE t (id int NOT NULL);";
        foreach (Func<CSharpDbDdlCompatibilityReport, string> serializer
                 in new Func<CSharpDbDdlCompatibilityReport, string>[]
                 {
                     _ => "payload",
                     _ => throw new InvalidDataException(
                         SecretConnectionString),
                 })
        {
            var dependencies = new SqlServerWorkerDependencies
            {
                AnalyzeDdlAsync = (source, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return ValueTask.FromResult(
                        BuildDdlReport(source));
                },
                SerializeDdlReport = serializer,
                MeasureUtf8Bytes = _ =>
                    SqlServerWorkerRunner.MaxDdlReportBytes + 1,
            };

            WorkerResult result = await RunDdlAsync(
                Encoding.UTF8.GetBytes(script),
                dependencies,
                ct);

            AssertDdlFailure(
                result,
                SqlServerWorkerRunner.ExitInternalFailure,
                "internal-failure");
            AssertSecretAbsent(result.Output, result.Error);
        }
    }

    [Fact]
    public async Task DdlReadAndAnalysisFailures_AreStableAndSanitized()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var analysisFailureDependencies =
            new SqlServerWorkerDependencies
            {
                AnalyzeDdlAsync = (_, _) =>
                    throw new InvalidOperationException(
                        SecretConnectionString),
            };

        WorkerResult readFailure = await RunDdlAsync(
            ValidDdlArguments(),
            new ThrowingReadStream(),
            new SqlServerWorkerDependencies(),
            ct);
        WorkerResult analysisFailure = await RunDdlAsync(
            Encoding.UTF8.GetBytes("SELECT 1;"),
            analysisFailureDependencies,
            ct);

        AssertDdlFailure(
            readFailure,
            SqlServerWorkerRunner.ExitInspectionFailure,
            "analysis-failed");
        AssertDdlFailure(
            analysisFailure,
            SqlServerWorkerRunner.ExitInspectionFailure,
            "analysis-failed");
        AssertSecretAbsent(
            readFailure.Output,
            readFailure.Error,
            analysisFailure.Output,
            analysisFailure.Error);
    }

    [Fact]
    public async Task DdlCancellation_UsesStableAnalysisFailure()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        bool analyzerInvoked = false;
        var dependencies = new SqlServerWorkerDependencies
        {
            AnalyzeDdlAsync = (_, _) =>
            {
                analyzerInvoked = true;
                throw new InvalidOperationException();
            },
        };

        WorkerResult result = await RunDdlAsync(
            ValidDdlArguments(),
            new ThrowingReadStream(),
            dependencies,
            cancellation.Token);

        AssertDdlFailure(
            result,
            SqlServerWorkerRunner.ExitInspectionFailure,
            "analysis-failed");
        Assert.False(analyzerInvoked);
    }

    private static MigrationCatalog BuildCatalog(
        CancellationToken cancellationToken = default) =>
        SqlServerCatalogBuilder.Build(
            SqlServerTestSnapshot.Create(),
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = false,
            },
            SqlServerInspectionLimits.Default,
            cancellationToken);

    private static string[] ValidArguments() =>
    [
        "--protocol", SqlServerWorkerRunner.Protocol,
        "--connection-env", EnvironmentVariableName,
        "--target-version",
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
    ];

    private static string[] ValidDdlArguments() =>
    [
        "--protocol", SqlServerWorkerRunner.DdlProtocol,
        "--target-version",
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
    ];

    private static string[] ValidQueryArguments() =>
    [
        "--protocol", SqlServerWorkerRunner.QueryProtocol,
        "--target-version",
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        "--query-id", "sqlserver-query",
        "--compatibility-level", "160",
    ];

    private static CSharpDbDdlCompatibilityReport BuildDdlReport(
        string script) =>
        new()
        {
            Dialect = "tsql",
            SourceGrammar =
                SqlServerTsqlDdlCompatibilityAnalyzer.SourceGrammar,
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            CapabilityDigest =
                CSharpDbCapabilityCatalogLoader.LoadEmbedded().Digest,
            ScriptDigest = ComputeDdlSourceDigest(
                Encoding.UTF8.GetBytes(script)),
            Status =
                MigrationCompatibilityStatus.CompatibleWithRewrite,
            HighestEvidence = MigrationEvidenceLevel.ScratchExecuted,
            RuleId =
                CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
            StatementCount = 1,
            ProvenStatementCount = 1,
            CandidateActionCount = 1,
            CatalogDigest = new string('b', 64),
            PlanContractDigest = new string('c', 64),
            GeneratedDdlDigest = new string('d', 64),
            ExpectedSchemaDigest = new string('e', 64),
            ActualSchemaDigest = new string('e', 64),
            Statements =
            [
                new CSharpDbDdlCompatibilityStatement
                {
                    Index = 0,
                    Kind = "create-table",
                    Span = new MigrationSourceSpan
                    {
                        SourceId = "input",
                        Start = 0,
                        Length = script.Length,
                        Line = 1,
                        Column = 1,
                    },
                    Status =
                        MigrationCompatibilityStatus
                            .CompatibleWithRewrite,
                    Evidence =
                        MigrationEvidenceLevel.ScratchExecuted,
                    RuleId =
                        CSharpDbDdlCompatibilityAnalyzer
                            .RewriteRuleId,
                },
            ],
            Diagnostics =
            [
                new CSharpDbDdlCompatibilityDiagnostic
                {
                    Ordinal = 0,
                    DiagnosticId =
                        "csharpdb-ddl/000000/" +
                        CSharpDbDdlCompatibilityAnalyzer
                            .RewriteRuleId,
                    RuleId =
                        CSharpDbDdlCompatibilityAnalyzer
                            .RewriteRuleId,
                    Severity =
                        MigrationDiagnosticSeverity.Warning,
                    Status =
                        MigrationCompatibilityStatus
                            .CompatibleWithRewrite,
                    Evidence =
                        MigrationEvidenceLevel.ScratchExecuted,
                    Summary = "Untrusted worker narrative.",
                    Remediation = "Untrusted worker remediation.",
                },
            ],
        };

    private static CSharpDbDdlCompatibilityReport
        BuildScratchDifferentReport(string script)
    {
        CSharpDbDdlCompatibilityReport canonical =
            BuildDdlReport(script);
        return canonical with
        {
            Status = MigrationCompatibilityStatus.Unknown,
            HighestEvidence =
                MigrationEvidenceLevel.ScratchExecuted,
            RuleId =
                CSharpDbDdlCompatibilityAnalyzer
                    .ScratchDifferentRuleId,
            ProvenStatementCount = 0,
            ActualSchemaDigest = new string('f', 64),
            Statements =
            [
                canonical.Statements[0] with
                {
                    Status =
                        MigrationCompatibilityStatus.Unknown,
                    Evidence =
                        MigrationEvidenceLevel.ScratchExecuted,
                    RuleId =
                        CSharpDbDdlCompatibilityAnalyzer
                            .ScratchDifferentRuleId,
                },
            ],
            Diagnostics =
            [
                new CSharpDbDdlCompatibilityDiagnostic
                {
                    Ordinal = 0,
                    DiagnosticId =
                        "csharpdb-ddl/000000/" +
                        CSharpDbDdlCompatibilityAnalyzer
                            .ScratchDifferentRuleId,
                    RuleId =
                        CSharpDbDdlCompatibilityAnalyzer
                            .ScratchDifferentRuleId,
                    Severity =
                        MigrationDiagnosticSeverity.Error,
                    Status =
                        MigrationCompatibilityStatus.Unknown,
                    Evidence =
                        MigrationEvidenceLevel.ScratchExecuted,
                    Summary = "Untrusted worker narrative.",
                    Remediation =
                        "Untrusted worker remediation.",
                },
            ],
            Differences =
            [
                new CSharpDbDdlScratchValidationDifference
                {
                    Ordinal = 0,
                    ObjectIdentityDigest = new string('a', 64),
                    Kind = MigrationObjectKind.Table,
                    ExpectedDefinitionDigest =
                        new string('b', 64),
                    ActualDefinitionDigest =
                        new string('c', 64),
                },
            ],
        };
    }

    private static string ComputeDdlSourceDigest(
        ReadOnlySpan<byte> source)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(
            Encoding.UTF8.GetBytes(
                SqlServerTsqlDdlCompatibilityAnalyzer
                    .InputDigestDomain));
        hash.AppendData([0]);
        hash.AppendData(source);
        return Convert.ToHexString(hash.GetHashAndReset())
            .ToLowerInvariant();
    }

    private static byte[] Utf8WithBom(string value)
    {
        byte[] encoded = Encoding.UTF8.GetBytes(value);
        byte[] result = new byte[3 + encoded.Length];
        result[0] = 0xEF;
        result[1] = 0xBB;
        result[2] = 0xBF;
        encoded.CopyTo(result.AsSpan(3));
        return result;
    }

    private static async ValueTask<WorkerResult> RunAsync(
        string[] args,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int exitCode = await SqlServerWorkerRunner.RunAsync(
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

    private static ValueTask<WorkerResult> RunDdlAsync(
        byte[] input,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default) =>
        RunDdlAsync(
            ValidDdlArguments(),
            new MemoryStream(input, writable: false),
            dependencies,
            cancellationToken);

    private static ValueTask<WorkerResult> RunQueryAsync(
        byte[] input,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default) =>
        RunQueryAsync(
            ValidQueryArguments(),
            new MemoryStream(input, writable: false),
            dependencies,
            cancellationToken);

    private static async ValueTask<WorkerResult> RunQueryAsync(
        string[] args,
        Stream input,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        await using (input)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int exitCode =
                await SqlServerWorkerRunner.RunAsync(
                    args,
                    input,
                    output,
                    error,
                    dependencies,
                    cancellationToken);
            return new WorkerResult(
                exitCode,
                output.ToString(),
                error.ToString());
        }
    }

    private static async ValueTask<WorkerResult> RunDdlAsync(
        string[] args,
        Stream input,
        SqlServerWorkerDependencies dependencies,
        CancellationToken cancellationToken = default)
    {
        await using (input)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int exitCode = await SqlServerWorkerRunner.RunAsync(
                args,
                input,
                output,
                error,
                dependencies,
                cancellationToken);
            return new WorkerResult(
                exitCode,
                output.ToString(),
                error.ToString());
        }
    }

    private static void AssertInternalFailure(WorkerResult result)
    {
        Assert.Equal(
            SqlServerWorkerRunner.ExitInternalFailure,
            result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.Protocol +
                ":error:internal-failure" +
                "\n",
            result.Error);
    }

    private static void AssertDdlFailure(
        WorkerResult result,
        int expectedExitCode,
        string expectedError)
    {
        Assert.Equal(expectedExitCode, result.ExitCode);
        Assert.Equal(string.Empty, result.Output);
        Assert.Equal(
            SqlServerWorkerRunner.DdlProtocol +
                ":error:" +
                expectedError +
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

    private sealed class FixedLengthStream(
        long length,
        byte value) : Stream
    {
        private readonly long _length = length;
        private long _remaining = length;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => _length;

        public override long Position
        {
            get => _length - _remaining;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ObjectDisposedException.ThrowIf(
                !CanRead,
                this);
            int read = (int)Math.Min(count, _remaining);
            buffer.AsSpan(offset, read).Fill(value);
            _remaining -= read;
            return read;
        }

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int read = (int)Math.Min(buffer.Length, _remaining);
            buffer.Span[..read].Fill(value);
            _remaining -= read;
            return ValueTask.FromResult(read);
        }

        public override void Flush()
        {
        }

        public override long Seek(
            long offset,
            SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();
    }

    private sealed class ThrowingReadStream : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length =>
            throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            throw new IOException(SecretConnectionString);

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default) =>
            throw new IOException(SecretConnectionString);

        public override void Flush()
        {
        }

        public override long Seek(
            long offset,
            SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();
    }

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
