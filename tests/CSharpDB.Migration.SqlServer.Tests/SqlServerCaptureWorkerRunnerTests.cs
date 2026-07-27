using System.Collections.ObjectModel;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;
using CSharpDB.Migration.SqlServer.Worker;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerCaptureWorkerRunnerTests
{
    private const string EnvironmentName =
        "CSHARPDB_TEST_SQLSERVER_CAPTURE";

    [Fact]
    public async Task Success_UsesExactProtocolClearsEnvironmentAndEmitsReceiptOnly()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            SqlServerCaptureWorkerRunner.OutputFileName);
        string? clearedName = null;
        string secret =
            "Server=private;Password=must-not-escape";
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            var dependencies =
                new SqlServerWorkerDependencies
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
                                outputPath,
                                path);
                            Assert.Equal(
                                1024 * 1024,
                                maximum);
                            Assert.Equal(
                                SqlServerCaptureWorkerRunner
                                    .DefaultTableTimeoutSeconds,
                                tableTimeout);
                            await File.WriteAllBytesAsync(
                                path,
                                [1, 2, 3],
                                cancellationToken);
                            return Result(
                                packageBytes: 3);
                        },
                };

            int exitCode =
                await SqlServerWorkerRunner.RunAsync(
                    Arguments(
                        outputPath,
                        1024 * 1024),
                    stdout,
                    stderr,
                    dependencies,
                    ct);

            Assert.Equal(
                SqlServerCaptureWorkerRunner.ExitSuccess,
                exitCode);
            Assert.Equal(
                EnvironmentName,
                clearedName);
            Assert.StartsWith(
                SqlServerCaptureWorkerRunner.SuccessHeader,
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
    public async Task Invocation_MustMatchExactOrderedProtocol()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        int reads = 0;
        int clears = 0;
        var dependencies =
            new SqlServerWorkerDependencies
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
                         SqlServerCaptureWorkerRunner.Protocol,
                     },
                     new[]
                     {
                         "--protocol",
                         SqlServerCaptureWorkerRunner.Protocol,
                         "--connection-env",
                         EnvironmentName,
                         "--target-version",
                         CSharpDbCapabilityCatalogLoader
                             .CurrentTargetVersion,
                         "--max-source-bytes",
                         "1024",
                         "--output",
                         "capture.csdbsqlserver",
                     },
                 })
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await SqlServerCaptureWorkerRunner.RunAsync(
                    invocation,
                    stdout,
                    stderr,
                    dependencies,
                    ct);
            Assert.Equal(
                SqlServerCaptureWorkerRunner
                    .ExitIncompatible,
                exitCode);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                SqlServerCaptureWorkerRunner.Protocol +
                ":error:incompatible\n",
                stderr.ToString());
        }

        Assert.Equal(0, reads);
        Assert.Equal(0, clears);
    }

    [Fact]
    public async Task EnvironmentClearFailureFailsBeforeCaptureWithoutPublishingDetails()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            SqlServerCaptureWorkerRunner.OutputFileName);
        int captures = 0;
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await SqlServerWorkerRunner.RunAsync(
                    Arguments(outputPath, 1024),
                    stdout,
                    stderr,
                    new SqlServerWorkerDependencies
                    {
                        ReadEnvironmentVariable =
                            _ => "Password=secret",
                        ClearEnvironmentVariable =
                            _ => throw new InvalidOperationException(
                                "Password=secret"),
                        CaptureRetainedAsync =
                            (_, _, _, _, _) =>
                            {
                                captures++;
                                throw new InvalidOperationException();
                            },
                    },
                    ct);

            Assert.Equal(
                SqlServerCaptureWorkerRunner
                    .ExitConnectionUnavailable,
                exitCode);
            Assert.Equal(0, captures);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                SqlServerCaptureWorkerRunner.Protocol +
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
    public async Task CaptureLimitUsesDedicatedSanitizedExit()
    {
        CancellationToken ct =
            TestContext.Current.CancellationToken;
        string workspace = CreateWorkspace();
        string outputPath = Path.Combine(
            workspace,
            SqlServerCaptureWorkerRunner.OutputFileName);
        try
        {
            var stdout = new StringWriter();
            var stderr = new StringWriter();
            int exitCode =
                await SqlServerWorkerRunner.RunAsync(
                    Arguments(outputPath, 1024),
                    stdout,
                    stderr,
                    new SqlServerWorkerDependencies
                    {
                        ReadEnvironmentVariable =
                            _ => "Password=secret",
                        ClearEnvironmentVariable =
                            _ => { },
                        CaptureRetainedAsync =
                            (_, _, _, _, _) =>
                                throw new
                                    SqlServerRetainedCaptureLimitException(
                                        "Password=secret"),
                    },
                    ct);

            Assert.Equal(
                SqlServerCaptureWorkerRunner
                    .ExitLimitExceeded,
                exitCode);
            Assert.True(
                string.IsNullOrEmpty(
                    stdout.ToString()));
            Assert.Equal(
                SqlServerCaptureWorkerRunner.Protocol +
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

    private static string[] Arguments(
        string outputPath,
        long maxPackageBytes) =>
        [
            "--protocol",
            SqlServerCaptureWorkerRunner.Protocol,
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
            SqlServerCaptureWorkerRunner
                .DefaultTableTimeoutSeconds
                .ToString(
                    System.Globalization.CultureInfo
                        .InvariantCulture),
        ];

    private static RetainedMigrationPackageWriteResult Result(
        long packageBytes)
    {
        _ = packageBytes;
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
                    "sha256:" + new string('c', 64),
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
                    MigrationSourceKind.SqlServer,
                SourceIdentity =
                    "sqlserver:test",
                SourceFingerprint =
                    "sha256:" + new string('d', 64),
                SnapshotIdentity =
                    "sqlserver-retained:sha256:" +
                    new string('e', 64),
                ContentDigest =
                    "sha256:" + new string('f', 64),
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
                "sha256:" + new string('a', 64),
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
            SqlServerCaptureWorkerRunner
                .WorkspacePrefix +
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException)
        {
        }
    }
}
