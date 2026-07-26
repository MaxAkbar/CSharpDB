using System.Text.RegularExpressions;
using CSharpDB.Migration;
using CSharpDB.Migration.Access;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Cli.Tests;

public sealed partial class AccessMigrationCommandRunnerTests
{
    private const string LiveFixtureEnvironmentVariable =
        "CSHARPDB_ACCESS_LIVE_FIXTURE";

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task Inspect_RejectsUnsafeOptionShapesBeforeSourceAccess()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath =
            workspace.PathFor("private-source.accdb");
        string packagePath =
            workspace.PathFor("retained.csdbaccess");
        string catalogPath =
            workspace.PathFor("catalog.json");
        await File.WriteAllBytesAsync(
            sourcePath,
            [0x01, 0x02, 0x03],
            Cancellation);

        string[][] invalidArguments =
        [
            [
                "migrate", "inspect",
                "--source", "access",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--provider", "jet4",
            ],
            [
                "migrate", "inspect",
                "--source", "access",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--provider", "ace12",
                "--allow-ace12-fallback",
            ],
            [
                "migrate", "inspect",
                "--source", "access",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--command-timeout-seconds", "0",
            ],
            [
                "migrate", "inspect",
                "--source", "access",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--password", "never-print-this",
            ],
        ];

        foreach (string[] arguments in invalidArguments)
        {
            var output = new StringWriter();
            var error = new StringWriter();

            int exitCode =
                await MigrationCommandRunner.RunAsync(
                    arguments,
                    output,
                    error,
                    Cancellation);

            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                exitCode);
            Assert.DoesNotContain(
                "never-print-this",
                error.ToString(),
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                sourcePath,
                error.ToString(),
                StringComparison.Ordinal);
            Assert.False(File.Exists(packagePath));
            Assert.False(File.Exists(catalogPath));
        }
    }

    [Fact]
    public async Task
        Inspect_ProviderOrInvalidSourceFailureIsSanitizedAndPublishesNothing()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath =
            workspace.PathFor(
                "private-customer-name.accdb");
        string packagePath =
            workspace.PathFor("retained.csdbaccess");
        string catalogPath =
            workspace.PathFor("catalog.json");
        const string PrivatePayload =
            "PRIVATE-ACCESS-ROW-VALUE";
        await File.WriteAllTextAsync(
            sourcePath,
            PrivatePayload,
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int exitCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "access",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                    "--provider", "ace16",
                ],
                output,
                error,
                Dependencies(
                    AccessCaptureWorkerStatus
                        .CaptureFailed),
                Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitError,
            exitCode);
        Assert.Contains(
            "MIG-ACCESS-CLI-INSPECT-001",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sourcePath,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            Path.GetFileName(sourcePath),
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PrivatePayload,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.True(
            string.IsNullOrWhiteSpace(
                output.ToString()));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
    }

    [Fact]
    public async Task
        Inspect_UnavailableProviderFailureIsSanitizedAndPublishesNothing()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath =
            workspace.PathFor(
                "private-provider-probe.accdb");
        string packagePath =
            workspace.PathFor("retained.csdbaccess");
        string catalogPath =
            workspace.PathFor("catalog.json");
        await File.WriteAllBytesAsync(
            sourcePath,
            [0x01],
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int exitCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "access",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                    "--provider", "ace16",
                ],
                output,
                error,
                Dependencies(
                    AccessCaptureWorkerStatus
                        .ProviderUnavailable),
                Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitError,
            exitCode);
        Assert.Contains(
            "selected process-matched ACE OLE DB provider is unavailable",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sourcePath,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            Path.GetFileName(sourcePath),
            error.ToString(),
            StringComparison.Ordinal);
        Assert.True(
            string.IsNullOrWhiteSpace(
                output.ToString()));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
    }

    [Fact]
    public async Task
        Inspect_OptInFixturePublishesAProviderNeutralBoundPackage()
    {
        string? fixture =
            Environment.GetEnvironmentVariable(
                LiveFixtureEnvironmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(fixture),
            $"Set {LiveFixtureEnvironmentVariable} to a trusted, unencrypted .mdb or .accdb fixture.");
        Assert.True(File.Exists(fixture));

        using var workspace = new TemporaryDirectory();
        string packagePath =
            workspace.PathFor("retained.csdbaccess");
        string catalogPath =
            workspace.PathFor("catalog.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int exitCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "access",
                    "--input", fixture,
                    "--package", packagePath,
                    "--out", catalogPath,
                    "--provider", "ace16",
                    "--max-package-bytes",
                    (64L * 1024 * 1024).ToString(
                        System.Globalization
                            .CultureInfo.InvariantCulture),
                ],
                output,
                error,
                new MigrationCommandDependencies
                {
                    CaptureAccessAsync =
                        CaptureDirectAsync,
                },
                Cancellation);

        Assert.Equal(
            InspectorCommandRunner.ExitWarn,
            exitCode);
        Assert.True(
            string.IsNullOrWhiteSpace(
                error.ToString()),
            error.ToString());
        Assert.DoesNotContain(
            fixture,
            output.ToString(),
            StringComparison.Ordinal);
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));

        Match digestMatch =
            PackageDigestRegex().Match(
                output.ToString());
        Assert.True(
            digestMatch.Success,
            output.ToString());
        string packageDigest =
            digestMatch.Groups["digest"].Value;
        MigrationCatalog publishedCatalog =
            MigrationArtifactSerializer
                .DeserializeCatalog(
                    await File.ReadAllTextAsync(
                        catalogPath,
                        Cancellation));

        await using
            RetainedMigrationPackageSession session =
            await RetainedMigrationPackageSession
                .OpenAsync(
                    packagePath,
                    new RetainedMigrationPackageOpenOptions
                    {
                        ExpectedPackageDigest =
                            packageDigest,
                        WorkspacePath =
                            workspace.Path,
                        MaxPackageBytes =
                            64L * 1024 * 1024,
                    },
                    Cancellation);
        AccessRetainedPackageBindingValidator
            .Validate(
                publishedCatalog,
                session.Manifest);
        Assert.Equal(
            MigrationArtifactSerializer
                .ComputeCatalogDigest(
                    publishedCatalog),
            MigrationArtifactSerializer
                .ComputeCatalogDigest(
                    session.Catalog));
        Assert.Equal(
            MigrationSourceKind.Access,
            session.Manifest.SourceKind);
        Assert.Contains(
            publishedCatalog.Diagnostics,
            static diagnostic =>
                diagnostic.RuleId ==
                    "MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001" &&
                diagnostic.Status ==
                    MigrationCompatibilityStatus.Unknown &&
                !diagnostic.CanOverride);
    }

    [GeneratedRegex(
        @"packageDigest=(?<digest>sha256:[0-9a-f]{64})(?:\s|\||$)",
        RegexOptions.CultureInvariant)]
    private static partial Regex PackageDigestRegex();

    private static MigrationCommandDependencies
        Dependencies(
        AccessCaptureWorkerStatus status) =>
        new()
        {
            CaptureAccessAsync =
                (_, _, _, _, _, _, _, _, _) =>
                    ValueTask.FromResult(
                        AccessCaptureWorkerResult
                            .Failure(status)),
        };

    private static async ValueTask<
        AccessCaptureWorkerResult>
        CaptureDirectAsync(
        string sourcePath,
        string targetVersion,
        string outputPath,
        string provider,
        bool allowAce12Fallback,
        int commandTimeoutSeconds,
        long maxSourceBytes,
        long maxPackageBytes,
        CancellationToken cancellationToken)
    {
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader
                .CurrentTargetVersion,
            targetVersion);
        AccessOleDbProvider selectedProvider =
            provider switch
            {
                "ace16" =>
                    AccessOleDbProvider.Ace16,
                "ace12" =>
                    AccessOleDbProvider.Ace12,
                _ => throw new
                    InvalidOperationException(),
            };
        RetainedMigrationPackageWriteResult result =
            await AccessRetainedCapture.CaptureAsync(
                sourcePath,
                outputPath,
                new AccessRetainedCaptureOptions
                {
                    Source = new AccessSourceOptions
                    {
                        Provider =
                            selectedProvider,
                        AllowAce12Fallback =
                            allowAce12Fallback,
                        CommandTimeoutSeconds =
                            commandTimeoutSeconds,
                        MaxSourceBytes =
                            maxSourceBytes,
                    },
                    MaxPackageBytes =
                        maxPackageBytes,
                },
                cancellationToken);
        long rows = result.Manifest.Tables.Sum(
            static table => table.RowCount);
        return AccessCaptureWorkerResult.Success(
            new AccessCaptureReceipt
            {
                Format =
                    AccessCaptureReceipt
                        .CurrentFormat,
                PackageDigest =
                    result.PackageDigest,
                CatalogDigest =
                    result.Manifest.CatalogDigest,
                SnapshotIdentity =
                    result.Manifest
                        .SnapshotIdentity,
                PackageBytes =
                    new FileInfo(outputPath).Length,
                TableCount =
                    result.Manifest.Tables.Count,
                RowCount = rows,
            });
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "csharpdb-access-cli-tests-" +
                Guid.NewGuid().ToString("N"));
            MigrationCommandRunner
                .SqlServerCaptureWorkspace
                .CreatePrivateDirectoryExclusive(
                    Path);
        }

        internal string Path { get; }

        internal string PathFor(string name) =>
            System.IO.Path.Combine(Path, name);

        public void Dispose()
        {
            try
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(
                        Path,
                        recursive: true);
                }
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
