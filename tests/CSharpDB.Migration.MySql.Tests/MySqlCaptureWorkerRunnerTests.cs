using System.Collections.ObjectModel;
using CSharpDB.Migration;
using CSharpDB.Migration.MySql.Worker;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlCaptureWorkerRunnerTests
{
    private const string EnvironmentName =
        "CSHARPDB_TEST_MYSQL_CAPTURE";

    [Fact]
    public async Task
        Success_UsesExactProtocolClearsEnvironmentAndEmitsReceiptOnly()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            MySqlCaptureWorkerRunner.OutputFileName);
        string? clearedName = null;
        string secret =
            "Server=private;Password=must-not-escape";
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            var dependencies =
                new MySqlWorkerDependencies
                {
                    ReadEnvironmentVariable =
                        name => name == EnvironmentName
                            ? secret
                            : null,
                    ClearEnvironmentVariable =
                        name => clearedName = name,
                    CaptureRetainedAsync =
                        async (
                            connectionString,
                            path,
                            maximum,
                            tableTimeout,
                            cancellationToken) =>
                        {
                            Assert.Equal(
                                secret,
                                connectionString);
                            Assert.Equal(
                                EnvironmentName,
                                clearedName);
                            Assert.Equal(
                                outputPath,
                                path);
                            Assert.Equal(
                                1024 * 1024,
                                maximum);
                            Assert.Equal(
                                MySqlCaptureWorkerRunner
                                    .DefaultTableTimeoutSeconds,
                                tableTimeout);
                            await File.WriteAllBytesAsync(
                                path,
                                [1, 2, 3],
                                cancellationToken);
                            return Result();
                        },
                };

            int exitCode =
                await MySqlWorkerRunner.RunAsync(
                    Arguments(
                        outputPath,
                        1024 * 1024),
                    stdout,
                    stderr,
                    dependencies,
                    ct);

            Assert.Equal(
                MySqlCaptureWorkerRunner.ExitSuccess,
                exitCode);
            Assert.Equal(
                EnvironmentName,
                clearedName);
            Assert.StartsWith(
                MySqlCaptureWorkerRunner.SuccessHeader,
                stdout.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                secret,
                stdout.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                secret,
                stderr.ToString(),
                StringComparison.Ordinal);
            Assert.True(
                string.IsNullOrWhiteSpace(
                    stderr.ToString()));
            Assert.True(File.Exists(outputPath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task
        Invocation_MustMatchExactOrderedProtocol()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        int reads = 0;
        int clears = 0;
        var dependencies =
            new MySqlWorkerDependencies
            {
                ReadEnvironmentVariable =
                    _ =>
                    {
                        reads++;
                        return "secret";
                    },
                ClearEnvironmentVariable =
                    _ => clears++,
                CaptureRetainedAsync =
                    (_, _, _, _, _) =>
                        throw new InvalidOperationException(),
            };

        foreach (string[] invocation in new[]
                 {
                     Array.Empty<string>(),
                     new[]
                     {
                         "--protocol",
                         MySqlCaptureWorkerRunner
                             .Protocol,
                     },
                     new[]
                     {
                         "--protocol",
                         MySqlCaptureWorkerRunner
                             .Protocol,
                         "--connection-env",
                         EnvironmentName,
                         "--target-version",
                         CSharpDbCapabilityCatalogLoader
                             .CurrentTargetVersion,
                         "--max-source-bytes",
                         "1024",
                         "--output",
                         "capture.csdbmysql",
                     },
                 })
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await MySqlCaptureWorkerRunner.RunAsync(
                    invocation,
                    stdout,
                    stderr,
                    dependencies,
                    ct);
            Assert.Equal(
                MySqlCaptureWorkerRunner
                    .ExitIncompatible,
                exitCode);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                MySqlCaptureWorkerRunner.Protocol +
                ":error:incompatible\n",
                stderr.ToString());
        }

        Assert.Equal(0, reads);
        Assert.Equal(0, clears);
    }

    [Fact]
    public async Task
        OutputPathMustBeNewFixedFileInExactCaptureWorkspace()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string ordinaryDirectory = Path.Combine(
            Path.GetTempPath(),
            "csharpdb_mysql_capture_test_" +
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(
            ordinaryDirectory);
        string existingPath = Path.Combine(
            workspace,
            MySqlCaptureWorkerRunner
                .OutputFileName);
        await File.WriteAllBytesAsync(
            existingPath,
            [1],
            ct);
        int reads = 0;
        int clears = 0;
        try
        {
            var dependencies =
                new MySqlWorkerDependencies
                {
                    ReadEnvironmentVariable =
                        _ =>
                        {
                            reads++;
                            return "secret";
                        },
                    ClearEnvironmentVariable =
                        _ => clears++,
                    CaptureRetainedAsync =
                        (_, _, _, _, _) =>
                            throw new
                                InvalidOperationException(),
                };
            string[] invalidOutputs =
            [
                Path.Combine(
                    ordinaryDirectory,
                    MySqlCaptureWorkerRunner
                        .OutputFileName),
                Path.Combine(
                    workspace,
                    "wrong.csdbmysql"),
                existingPath,
                MySqlCaptureWorkerRunner
                    .OutputFileName,
            ];

            foreach (string invalidOutput
                     in invalidOutputs)
            {
                var stdout = new StringWriter();
                var stderr = new StringWriter();
                int exitCode =
                    await MySqlCaptureWorkerRunner
                        .RunAsync(
                            Arguments(
                                invalidOutput,
                                1024),
                            stdout,
                            stderr,
                            dependencies,
                            ct);

                Assert.Equal(
                    MySqlCaptureWorkerRunner
                        .ExitIncompatible,
                    exitCode);
                Assert.Empty(stdout.ToString());
                Assert.Equal(
                    MySqlCaptureWorkerRunner
                        .Protocol +
                    ":error:incompatible\n",
                    stderr.ToString());
            }

            Assert.Equal(0, reads);
            Assert.Equal(0, clears);
        }
        finally
        {
            TryDelete(workspace);
            TryDelete(ordinaryDirectory);
        }
    }

    [Fact]
    public async Task
        EnvironmentClearFailureFailsBeforeCaptureWithoutPublishingDetails()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            MySqlCaptureWorkerRunner.OutputFileName);
        int captures = 0;
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await MySqlWorkerRunner.RunAsync(
                    Arguments(outputPath, 1024),
                    stdout,
                    stderr,
                    new MySqlWorkerDependencies
                    {
                        ReadEnvironmentVariable =
                            _ => "Password=secret",
                        ClearEnvironmentVariable =
                            _ => throw new
                                InvalidOperationException(
                                    "Password=secret"),
                        CaptureRetainedAsync =
                            (_, _, _, _, _) =>
                            {
                                captures++;
                                throw new
                                    InvalidOperationException();
                            },
                    },
                    ct);

            Assert.Equal(
                MySqlCaptureWorkerRunner
                    .ExitConnectionUnavailable,
                exitCode);
            Assert.Equal(0, captures);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                MySqlCaptureWorkerRunner.Protocol +
                ":error:connection-unavailable\n",
                stderr.ToString());
            Assert.DoesNotContain(
                "secret",
                stderr.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(outputPath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task
        CaptureLimitUsesDedicatedSanitizedExit()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            MySqlCaptureWorkerRunner.OutputFileName);
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await MySqlWorkerRunner.RunAsync(
                    Arguments(outputPath, 1024),
                    stdout,
                    stderr,
                    new MySqlWorkerDependencies
                    {
                        ReadEnvironmentVariable =
                            _ => "Password=secret",
                        ClearEnvironmentVariable =
                            _ => { },
                        CaptureRetainedAsync =
                            (_, _, _, _, _) =>
                                throw new
                                    MySqlRetainedCaptureLimitException(
                                        "Password=secret"),
                    },
                    ct);

            Assert.Equal(
                MySqlCaptureWorkerRunner
                    .ExitLimitExceeded,
                exitCode);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                MySqlCaptureWorkerRunner.Protocol +
                ":error:limit-exceeded\n",
                stderr.ToString());
            Assert.DoesNotContain(
                "secret",
                stderr.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(outputPath));
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    [Fact]
    public async Task
        UnsafeSnapshotIdentityIsNeverEmitted()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            MySqlCaptureWorkerRunner.OutputFileName);
        const string secret =
            "mysql:private-database-name";
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await MySqlWorkerRunner.RunAsync(
                    Arguments(outputPath, 1024),
                    stdout,
                    stderr,
                    new MySqlWorkerDependencies
                    {
                        ReadEnvironmentVariable =
                            _ => "Password=secret",
                        ClearEnvironmentVariable =
                            _ => { },
                        CaptureRetainedAsync =
                            async (
                                _,
                                path,
                                _,
                                _,
                                cancellationToken) =>
                            {
                                await File
                                    .WriteAllBytesAsync(
                                        path,
                                        [1],
                                        cancellationToken);
                                return Result(secret);
                            },
                    },
                    ct);

            Assert.Equal(
                MySqlCaptureWorkerRunner
                    .ExitInternalFailure,
                exitCode);
            Assert.Empty(stdout.ToString());
            Assert.Equal(
                MySqlCaptureWorkerRunner.Protocol +
                ":error:internal-failure\n",
                stderr.ToString());
            Assert.DoesNotContain(
                secret,
                stderr.ToString(),
                StringComparison.Ordinal);
        }
        finally
        {
            TryDelete(workspace);
        }
    }

    private static string[] Arguments(
        string outputPath,
        long maxPackageBytes) =>
        [
            "--protocol",
            MySqlCaptureWorkerRunner.Protocol,
            "--connection-env",
            EnvironmentName,
            "--target-version",
            CSharpDbCapabilityCatalogLoader
                .CurrentTargetVersion,
            "--output",
            outputPath,
            "--max-source-bytes",
            maxPackageBytes.ToString(
                System.Globalization.CultureInfo
                    .InvariantCulture),
            "--table-timeout-seconds",
            MySqlCaptureWorkerRunner
                .DefaultTableTimeoutSeconds
                .ToString(
                    System.Globalization.CultureInfo
                        .InvariantCulture),
        ];

    private static RetainedMigrationPackageWriteResult
        Result(
        string? snapshotIdentity = null)
    {
        var descriptor =
            new RetainedMigrationTableDescriptor
            {
                SourceObjectId = "table:1",
                ColumnObjectIds = ["column:1"],
                OrderingKeyColumnObjectIds =
                    ["column:1"],
            };
        var table =
            new RetainedMigrationPackageTableManifest
            {
                Descriptor = descriptor,
                RowCount = 2,
                SectionLength = 1,
                SectionDigest =
                    "sha256:" +
                    new string('c', 64),
            };
        var manifest =
            new RetainedMigrationPackageManifest
            {
                Format =
                    RetainedMigrationPackageContract
                        .Format,
                CatalogDigest =
                    new string('b', 64),
                SourceKind =
                    MigrationSourceKind.MySql,
                SourceIdentity =
                    "mysql:test",
                SourceFingerprint =
                    "sha256:" +
                    new string('d', 64),
                SnapshotIdentity =
                    snapshotIdentity ??
                    "mysql-retained:sha256:" +
                    new string('e', 64),
                ContentDigest =
                    "sha256:" +
                    new string('f', 64),
                Tables =
                    new ReadOnlyCollection<
                        RetainedMigrationPackageTableManifest>(
                        [table]),
            };
        var summary =
            new RetainedMigrationContentSummary
            {
                DigestAlgorithm =
                    RetainedMigrationPackageContract
                        .ContentDigestAlgorithm,
                ContentDigest =
                    manifest.ContentDigest,
                Tables =
                [
                    new RetainedMigrationContentTableSummary
                    {
                        Descriptor = descriptor,
                        RowCount = table.RowCount,
                        SectionDigest =
                            table.SectionDigest,
                    },
                ],
            };
        return new RetainedMigrationPackageWriteResult
        {
            Manifest = manifest,
            PackageDigest =
                "sha256:" +
                new string('a', 64),
            ContentSummary = summary,
            RowCounts =
                new ReadOnlyDictionary<string, long>(
                    new Dictionary<string, long>
                    {
                        [descriptor.SourceObjectId] =
                            table.RowCount,
                    }),
        };
    }

    private static string CreateWorkspace()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            MySqlCaptureWorkerRunner
                .WorkspacePrefix +
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            Directory.Delete(
                path,
                recursive: true);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException)
        {
        }
    }
}
