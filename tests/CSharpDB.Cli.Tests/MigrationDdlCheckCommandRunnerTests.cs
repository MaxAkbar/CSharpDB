using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Sql;

namespace CSharpDB.Cli.Tests;

public sealed class MigrationDdlCheckCommandRunnerTests
{
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
                "--dialect", "tsql",
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

    private static string CreateTempPath(string label) =>
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-ddl-check-{label}-{Guid.NewGuid():N}.sql");
}
