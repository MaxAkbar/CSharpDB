using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Sql;

namespace CSharpDB.Cli.Tests;

public sealed class MigrationDdlCheckCommandRunnerTests
{
    private const string HostileContractSecret =
        "Password=hostile-runner-contract-secret";

    [Fact]
    public async Task InvalidGrammar_ReturnsUsageWithoutEchoingValues()
    {
        string[][] arguments =
        [
            ["migrate", "ddl-check"],
            ["migrate", "ddl-check", "--dialect", "csharpdb"],
            ["migrate", "ddl-check", "input.sql"],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect", "csharpdb",
                "--dialect", "csharpdb",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect", "csharpdb",
                "--unknown", "private-option-value",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "private-extra-position",
                "--dialect", "csharpdb",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect", "mysql",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect", "csharpdb",
                "--format", "yaml",
            ],
            [
                "migrate", "ddl-check", "input.sql",
                "--dialect=csharpdb",
            ],
        ];

        foreach (string[] args in arguments)
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                args,
                output,
                error,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "csharpdb migrate ddl-check",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "private-option-value",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "private-extra-position",
                error.ToString(),
                StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("text")]
    [InlineData("json")]
    public async Task SupportedScript_ProducesProofAndExitOk(
        string format)
    {
        string path = CreateTempPath("supported");
        try
        {
            await File.WriteAllTextAsync(
                path,
                "CREATE TABLE widgets (id INTEGER NOT NULL);",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "csharpdb",
                    "--format", format,
                ],
                output,
                error,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitOk, code);
            Assert.Empty(error.ToString());
            if (format == "json")
            {
                using JsonDocument document =
                    JsonDocument.Parse(output.ToString());
                JsonElement root = document.RootElement;
                Assert.Equal(
                    "csharpdb-ddl-compatibility/v1",
                    root.GetProperty("format").GetString());
                Assert.Equal(
                    "compatible",
                    root.GetProperty("status").GetString());
                Assert.Equal(
                    "scratchExecuted",
                    root.GetProperty("highestEvidence").GetString());
                Assert.Equal(
                    "csharpdb",
                    root.GetProperty("dialect").GetString());
                Assert.Equal(
                    1,
                    root.GetProperty("statementCount").GetInt32());
            }
            else
            {
                Assert.Contains(
                    "Format: csharpdb-ddl-compatibility/v1",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "Status: compatible",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "Highest evidence: scratch-executed",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "Statements: total=1 | proven=1",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "span=start:",
                    output.ToString(),
                    StringComparison.Ordinal);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Utf8Bom_IsAcceptedAndStripped()
    {
        string path = CreateTempPath("bom");
        try
        {
            await File.WriteAllTextAsync(
                path,
                "CREATE TABLE widgets (id INTEGER NOT NULL);",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: true),
                Ct);

            (int code, string stdout, string stderr) =
                await RunAsync(path);

            Assert.Equal(InspectorCommandRunner.ExitOk, code);
            Assert.Contains(
                "Status: compatible",
                stdout,
                StringComparison.Ordinal);
            Assert.Empty(stderr);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RewriteScript_ReturnsReviewExit()
    {
        string path = CreateTempPath("rewrite");
        try
        {
            await File.WriteAllTextAsync(
                path,
                "CREATE TABLE widgets (id INTEGER PRIMARY KEY);",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);

            (int code, string stdout, string stderr) =
                await RunAsync(path);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            Assert.Contains(
                "Status: compatible-with-rewrite",
                stdout,
                StringComparison.Ordinal);
            Assert.Empty(stderr);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(
        "DROP TABLE private_table;",
        "unsupported")]
    [InlineData(
        "CREATE TABLE private_table (",
        "unsupported")]
    public async Task UnprovenScript_ReturnsErrorExitAndSafeReport(
        string script,
        string expectedStatus)
    {
        string path = CreateTempPath("unproven");
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);

            (int code, string stdout, string stderr) =
                await RunAsync(path);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Contains(
                $"Status: {expectedStatus}",
                stdout,
                StringComparison.Ordinal);
            Assert.Contains(
                "Diagnostic:",
                stdout,
                StringComparison.Ordinal);
            Assert.Empty(stderr);
            Assert.DoesNotContain(
                "private_table",
                stdout,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task InvalidUtf8_ReturnsSanitizedReadError()
    {
        string path = CreateTempPath("private-path-token");
        try
        {
            await File.WriteAllBytesAsync(
                path,
                [0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x20, 0xC3, 0x28],
                Ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "csharpdb",
                ],
                output,
                error,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-CSHARPDB-DDL-CHECK-ENCODING-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                path,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                "private-path-token",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task OversizedFile_IsRejectedBeforeAnalyzerInvocation()
    {
        string path = CreateTempPath("oversized");
        bool analyzerInvoked = false;
        try
        {
            await using (FileStream stream = new(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None))
            {
                stream.SetLength(
                    (long)SqlScriptParserOptions.HardMaxScriptUtf8Bytes +
                    1);
            }
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeCSharpDbDdlAsync =
                        (_, _) =>
                        {
                            analyzerInvoked = true;
                            throw new InvalidOperationException(
                                "The analyzer must not be invoked.");
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "csharpdb",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.False(analyzerInvoked);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-CSHARPDB-DDL-CHECK-LIMIT-001",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                path,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task OverCharacterLimitFile_IsRejectedBeforeAnalyzerInvocation()
    {
        string path = CreateTempPath("over-character-limit");
        bool analyzerInvoked = false;
        try
        {
            await using (FileStream stream = new(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None))
            {
                stream.SetLength(
                    (long)SqlScriptParserOptions
                        .HardMaxScriptCharacters +
                    1);
            }
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeCSharpDbDdlAsync =
                        (_, _) =>
                        {
                            analyzerInvoked = true;
                            throw new InvalidOperationException(
                                "The analyzer must not be invoked.");
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "csharpdb",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.False(analyzerInvoked);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-CSHARPDB-DDL-CHECK-LIMIT-001",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task UnsupportedReport_DoesNotExposePathOrSqlNames()
    {
        const string privateName =
            "CustomerPasswordVault_Secret_4D29";
        string path = CreateTempPath(privateName);
        try
        {
            await File.WriteAllTextAsync(
                path,
                $"DROP TABLE {privateName};",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);

            (int code, string stdout, string stderr) =
                await RunAsync(path);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            string combined = stdout + stderr;
            Assert.DoesNotContain(
                privateName,
                combined,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                path,
                combined,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                "DROP TABLE",
                combined,
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RepeatedAnalysis_ProducesIdenticalOutput()
    {
        string path = CreateTempPath("deterministic");
        try
        {
            await File.WriteAllTextAsync(
                path,
                "CREATE TABLE widgets (id INTEGER NOT NULL);",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);

            (int firstCode, string firstOutput, string firstError) =
                await RunAsync(path, "json");
            (int secondCode, string secondOutput, string secondError) =
                await RunAsync(path, "json");

            Assert.Equal(firstCode, secondCode);
            Assert.Equal(firstOutput, secondOutput);
            Assert.Equal(firstError, secondError);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("text")]
    [InlineData("json")]
    public async Task TsqlDialect_DispatchesWorkerAndRendersReport(
        string format)
    {
        const string script =
            "CREATE TABLE [dbo].[widgets] ([id] int NOT NULL);";
        string path = CreateTempPath("tsql-dispatch");
        bool csharpDbAnalyzerInvoked = false;
        bool tsqlAnalyzerInvoked = false;
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            CSharpDbDdlCompatibilityReport report =
                CreateTsqlReport(
                    MigrationCompatibilityStatus
                        .CompatibleWithRewrite,
                    script);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeCSharpDbDdlAsync =
                        (_, _) =>
                        {
                            csharpDbAnalyzerInvoked = true;
                            throw new InvalidOperationException(
                                "The CSharpDB analyzer must not run.");
                        },
                    AnalyzeTsqlDdlAsync =
                        (source, targetVersion, cancellationToken) =>
                        {
                            cancellationToken
                                .ThrowIfCancellationRequested();
                            Assert.Equal(script, source);
                            Assert.Equal(
                                CSharpDbCapabilityCatalogLoader
                                    .CurrentTargetVersion,
                                targetVersion);
                            tsqlAnalyzerInvoked = true;
                            return ValueTask.FromResult(
                                SqlServerDdlWorkerResult.Success(
                                    report));
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "TSQL",
                    "--format", format,
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            Assert.False(csharpDbAnalyzerInvoked);
            Assert.True(tsqlAnalyzerInvoked);
            Assert.Empty(error.ToString());
            if (format == "json")
            {
                using JsonDocument document =
                    JsonDocument.Parse(output.ToString());
                Assert.Equal(
                    "tsql",
                    document.RootElement
                        .GetProperty("dialect")
                        .GetString());
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
            else
            {
                Assert.Contains(
                    "Dialect: tsql",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "Source grammar: tsql160",
                    output.ToString(),
                    StringComparison.Ordinal);
                Assert.Contains(
                    "Status: compatible-with-rewrite",
                    output.ToString(),
                    StringComparison.Ordinal);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(
        MigrationCompatibilityStatus.CompatibleWithRewrite,
        InspectorCommandRunner.ExitWarn)]
    [InlineData(
        MigrationCompatibilityStatus.Conditional,
        InspectorCommandRunner.ExitWarn)]
    [InlineData(
        MigrationCompatibilityStatus.Unsupported,
        InspectorCommandRunner.ExitError)]
    [InlineData(
        MigrationCompatibilityStatus.Unknown,
        InspectorCommandRunner.ExitError)]
    public async Task TsqlReportStatus_UsesSharedExitContract(
        MigrationCompatibilityStatus status,
        int expectedExitCode)
    {
        const string script = "CREATE TABLE t (id int);";
        string path = CreateTempPath("tsql-status");
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) => ValueTask.FromResult(
                            SqlServerDdlWorkerResult.Success(
                                CreateTsqlReport(status, script))),
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(expectedExitCode, code);
            Assert.Contains(
                $"Status: {StatusToken(status)}",
                output.ToString(),
                StringComparison.Ordinal);
            Assert.Empty(error.ToString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TsqlUtf8Bom_IsStrippedBeforeWorkerInvocation()
    {
        const string script = "CREATE TABLE t (id int);";
        string path = CreateTempPath("tsql-bom");
        bool workerInvoked = false;
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: true),
                Ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (source, _, _) =>
                        {
                            Assert.Equal(script, source);
                            workerInvoked = true;
                            return ValueTask.FromResult(
                                SqlServerDdlWorkerResult.Success(
                                    CreateTsqlReport(
                                        MigrationCompatibilityStatus
                                            .CompatibleWithRewrite,
                                        script)));
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            Assert.True(workerInvoked);
            Assert.Empty(error.ToString());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TsqlCompatibleOverclaim_IsRejected()
    {
        const string script = "CREATE TABLE t (id int);";
        string path = CreateTempPath("tsql-overclaim");
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) => ValueTask.FromResult(
                            SqlServerDdlWorkerResult.Success(
                                CreateTsqlReport(
                                    MigrationCompatibilityStatus
                                        .Compatible,
                                    script))),
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-TSQL-CLI-ADAPTER-001",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("wrong-capability")]
    [InlineData("contradictory-diagnostic")]
    public async Task TsqlContradictoryWorkerContract_IsRejectedWithoutProse(
        string mode)
    {
        const string script = "CREATE TABLE t (id int);";
        string path = CreateTempPath("tsql-contradictory-contract");
        try
        {
            await File.WriteAllTextAsync(
                path,
                script,
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            CSharpDbDdlCompatibilityReport report =
                CreateTsqlReport(
                    MigrationCompatibilityStatus
                        .CompatibleWithRewrite,
                    script);
            CSharpDbDdlCompatibilityDiagnostic hostile =
                report.Diagnostics[0] with
                {
                    Summary = string.Concat(
                        HostileContractSecret,
                        "\r\nDROP TABLE private_data;",
                        "\u001b[31mINJECTED-CONTROL"),
                    Remediation = string.Concat(
                        HostileContractSecret,
                        "\0hostile-remediation"),
                };
            report = mode switch
            {
                "wrong-capability" => report with
                {
                    CapabilityDigest = report.CapabilityDigest[0] == '0'
                        ? new string('1', 64)
                        : new string('0', 64),
                    Diagnostics = [hostile],
                },
                "contradictory-diagnostic" => report with
                {
                    Diagnostics =
                    [
                        hostile with
                        {
                            DiagnosticId =
                                "tsql-ddl/000000/tsql.ddl.statement.unsupported",
                            RuleId =
                                "tsql.ddl.statement.unsupported",
                            Severity =
                                MigrationDiagnosticSeverity.Error,
                            Status =
                                MigrationCompatibilityStatus
                                    .Unsupported,
                            Evidence =
                                MigrationEvidenceLevel.Parsed,
                            StatementIndex = 0,
                            SourceSpan = report.Statements[0].Span,
                        },
                    ],
                },
                _ => throw new ArgumentOutOfRangeException(
                    nameof(mode)),
            };
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) => ValueTask.FromResult(
                            SqlServerDdlWorkerResult.Success(
                                report)),
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Empty(output.ToString());
            string renderedError = error.ToString();
            Assert.Contains(
                "MIG-TSQL-CLI-ADAPTER-001",
                renderedError,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                HostileContractSecret,
                renderedError,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "DROP TABLE",
                renderedError,
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                "\u001b",
                renderedError,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "\0",
                renderedError,
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(
        (int)SqlServerDdlWorkerStatus.Missing,
        "MIG-TSQL-CLI-ADAPTER-001")]
    [InlineData(
        (int)SqlServerDdlWorkerStatus.Incompatible,
        "MIG-TSQL-CLI-ADAPTER-001")]
    [InlineData(
        (int)SqlServerDdlWorkerStatus.AnalysisFailed,
        "MIG-TSQL-CLI-DDL-CHECK-001")]
    public async Task TsqlWorkerFailure_ReturnsSanitizedError(
        int statusValue,
        string expectedCode)
    {
        SqlServerDdlWorkerStatus status =
            (SqlServerDdlWorkerStatus)statusValue;
        const string secret = "PrivateTsqlIdentifier_7A2F";
        string path = CreateTempPath(secret);
        try
        {
            await File.WriteAllTextAsync(
                path,
                $"CREATE TABLE [{secret}] (id int);",
                new UTF8Encoding(
                    encoderShouldEmitUTF8Identifier: false),
                Ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) => ValueTask.FromResult(
                            SqlServerDdlWorkerResult.Failure(
                                status)),
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Empty(output.ToString());
            Assert.Contains(
                expectedCode,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                secret,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(
                path,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TsqlInvalidUtf8_IsRejectedBeforeWorkerInvocation()
    {
        string path = CreateTempPath("tsql-invalid-utf8");
        bool workerInvoked = false;
        try
        {
            await File.WriteAllBytesAsync(
                path,
                [0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x20, 0xC3, 0x28],
                Ct);
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) =>
                        {
                            workerInvoked = true;
                            throw new InvalidOperationException(
                                "The worker must not run.");
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.False(workerInvoked);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-CSHARPDB-DDL-CHECK-ENCODING-001",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task TsqlOversizedFile_IsRejectedBeforeWorkerInvocation()
    {
        string path = CreateTempPath("tsql-oversized");
        bool workerInvoked = false;
        try
        {
            await using (FileStream stream = new(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None))
            {
                stream.SetLength(
                    (long)SqlScriptParserOptions
                        .HardMaxScriptUtf8Bytes +
                    1);
            }
            MigrationCommandDependencies dependencies =
                MigrationCommandDependencies.Default with
                {
                    AnalyzeTsqlDdlAsync =
                        (_, _, _) =>
                        {
                            workerInvoked = true;
                            throw new InvalidOperationException(
                                "The worker must not run.");
                        },
                };
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", path,
                    "--dialect", "tsql",
                ],
                output,
                error,
                dependencies,
                Ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.False(workerInvoked);
            Assert.Empty(output.ToString());
            Assert.Contains(
                "MIG-CSHARPDB-DDL-CHECK-LIMIT-001",
                error.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Cancellation_Propagates()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var output = new StringWriter();
        var error = new StringWriter();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "ddl-check", "unopened-private-path.sql",
                    "--dialect", "csharpdb",
                ],
                output,
                error,
                cancellation.Token));
        Assert.Empty(output.ToString());
        Assert.Empty(error.ToString());
    }

    private static async ValueTask<(
        int Code,
        string Stdout,
        string Stderr)> RunAsync(
        string path,
        string format = "text")
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "ddl-check", path,
                "--dialect", "csharpdb",
                "--format", format,
            ],
            output,
            error,
            Ct);
        return (code, output.ToString(), error.ToString());
    }

    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    private static CSharpDbDdlCompatibilityReport CreateTsqlReport(
        MigrationCompatibilityStatus status,
        string script)
    {
        bool rewrite =
            status is MigrationCompatibilityStatus.Compatible or
                MigrationCompatibilityStatus.CompatibleWithRewrite;
        bool conditional =
            status == MigrationCompatibilityStatus.Conditional;
        bool scratch = rewrite || conditional;
        string ruleId = status switch
        {
            MigrationCompatibilityStatus.Compatible or
            MigrationCompatibilityStatus.CompatibleWithRewrite =>
                CSharpDbDdlCompatibilityAnalyzer.RewriteRuleId,
            MigrationCompatibilityStatus.Conditional =>
                "tsql.ddl.collation.unresolved",
            MigrationCompatibilityStatus.Unsupported =>
                "tsql.ddl.statement.unsupported",
            MigrationCompatibilityStatus.Unknown =>
                "tsql.ddl.proof.unavailable",
            _ => throw new ArgumentOutOfRangeException(
                nameof(status)),
        };
        MigrationEvidenceLevel evidence = scratch
            ? MigrationEvidenceLevel.ScratchExecuted
            : MigrationEvidenceLevel.Parsed;
        var statementSpan = new MigrationSourceSpan
        {
            SourceId = "input",
            Start = 0,
            Length = script.Length,
            Line = 1,
            Column = 1,
        };
        IReadOnlyList<CSharpDbDdlCompatibilityDiagnostic> diagnostics =
            conditional
                ?
                [
                    new CSharpDbDdlCompatibilityDiagnostic
                    {
                        Ordinal = 0,
                        DiagnosticId =
                            "tsql-ddl/000000/tsql.ddl.collation.unresolved",
                        RuleId =
                            "tsql.ddl.collation.unresolved",
                        Severity =
                            MigrationDiagnosticSeverity.Warning,
                        Status =
                            MigrationCompatibilityStatus.Conditional,
                        Evidence = MigrationEvidenceLevel.Parsed,
                        Summary = "Untrusted fixture prose.",
                    },
                    new CSharpDbDdlCompatibilityDiagnostic
                    {
                        Ordinal = 1,
                        DiagnosticId =
                            "csharpdb-ddl/000001/csharpdb.ddl.canonical-rewrite",
                        RuleId =
                            CSharpDbDdlCompatibilityAnalyzer
                                .RewriteRuleId,
                        Severity =
                            MigrationDiagnosticSeverity.Warning,
                        Status =
                            MigrationCompatibilityStatus.Conditional,
                        Evidence =
                            MigrationEvidenceLevel.ScratchExecuted,
                        Summary = "Untrusted fixture prose.",
                    },
                ]
                :
                [
                    new CSharpDbDdlCompatibilityDiagnostic
                    {
                        Ordinal = 0,
                        DiagnosticId = string.Concat(
                            ruleId.StartsWith(
                                "tsql.",
                                StringComparison.Ordinal)
                                ? "tsql-ddl/"
                                : "csharpdb-ddl/",
                            "000000/",
                            ruleId),
                        RuleId = ruleId,
                        Severity = rewrite
                            ? MigrationDiagnosticSeverity.Warning
                            : MigrationDiagnosticSeverity.Error,
                        Status = status,
                        Evidence = evidence,
                        StatementIndex =
                            status ==
                                MigrationCompatibilityStatus
                                    .Unsupported
                                ? 0
                                : null,
                        SourceSpan =
                            status ==
                                MigrationCompatibilityStatus
                                    .Unsupported
                                ? statementSpan
                                : null,
                        Summary = "Untrusted fixture prose.",
                    },
                ];
        return new CSharpDbDdlCompatibilityReport
        {
            Dialect = "tsql",
            SourceGrammar = "tsql160",
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            CapabilityDigest =
                CSharpDbCapabilityCatalogLoader.LoadEmbedded().Digest,
            ScriptDigest = TsqlSourceDigest(script),
            Status = status,
            HighestEvidence = evidence,
            RuleId = ruleId,
            StatementCount = 1,
            ProvenStatementCount =
                status ==
                    MigrationCompatibilityStatus
                        .CompatibleWithRewrite
                    ? 1
                    : 0,
            CandidateActionCount = scratch ? 1 : 0,
            CatalogDigest = scratch ? new string('c', 64) : null,
            PlanContractDigest = scratch ? new string('d', 64) : null,
            GeneratedDdlDigest = scratch ? new string('e', 64) : null,
            ExpectedSchemaDigest = scratch ? new string('f', 64) : null,
            ActualSchemaDigest = scratch ? new string('f', 64) : null,
            Statements =
            [
                new CSharpDbDdlCompatibilityStatement
                {
                    Index = 0,
                    Kind = status is
                            MigrationCompatibilityStatus.Unsupported
                        ? "unsupported"
                        : status is
                            MigrationCompatibilityStatus.Unknown
                            ? "unproven"
                            : "create-table",
                    Span = statementSpan,
                    Status = status,
                    Evidence = evidence,
                    RuleId = ruleId,
                },
            ],
            Diagnostics = diagnostics,
        };
    }

    private static string TsqlSourceDigest(string script)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData("tsql-ddl-input/v1"u8);
        hash.AppendData([0]);
        hash.AppendData(Encoding.UTF8.GetBytes(script));
        return Convert.ToHexString(hash.GetHashAndReset())
            .ToLowerInvariant();
    }

    private static string StatusToken(
        MigrationCompatibilityStatus status) =>
        status switch
        {
            MigrationCompatibilityStatus.Compatible =>
                "compatible",
            MigrationCompatibilityStatus.CompatibleWithRewrite =>
                "compatible-with-rewrite",
            MigrationCompatibilityStatus.Conditional =>
                "conditional",
            MigrationCompatibilityStatus.Unsupported =>
                "unsupported",
            MigrationCompatibilityStatus.Unknown =>
                "unknown",
            _ => throw new ArgumentOutOfRangeException(
                nameof(status)),
        };

    private static string CreateTempPath(string label) =>
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-ddl-check-{label}-{Guid.NewGuid():N}.sql");
}
