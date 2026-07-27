using CSharpDB.Migration.Access.Worker;

namespace CSharpDB.Migration.Access.Tests;

public sealed class AccessWorkerRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task InvalidInvocationFailsClosed()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        int exitCode =
            await AccessWorkerRunner.RunAsync(
                ["--protocol", "wrong"],
                output,
                error,
                AccessWorkerDependencies.Default,
                Cancellation);

        Assert.Equal(
            AccessWorkerRunner.ExitIncompatible,
            exitCode);
        Assert.True(
            string.IsNullOrEmpty(
                output.ToString()));
        Assert.Equal(
            AccessWorkerRunner.Protocol +
                ":error:incompatible\n",
            error.ToString());
    }

    [Fact]
    public async Task ProviderFailureUsesOnlyStableProtocolState()
    {
        Assert.SkipWhen(
            !OperatingSystem.IsWindows(),
            "The Access worker is intentionally Windows-only.");
        using var workspace =
            new TemporaryDirectory();
        string input =
            workspace.PathFor("private.accdb");
        string outputPath =
            workspace.PathFor("capture.csdbaccess");
        await File.WriteAllBytesAsync(
            input,
            [0x01],
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();
        var dependencies =
            new AccessWorkerDependencies
            {
                CaptureRetainedAsync =
                    (_, _, _, _) =>
                        throw new
                            AccessMigrationException(
                                AccessMigrationErrorCode
                                    .ProviderUnavailable,
                                "private provider detail"),
            };

        int exitCode =
            await AccessWorkerRunner.RunAsync(
                [
                    "--protocol",
                    AccessWorkerRunner.Protocol,
                    "--input", input,
                    "--target-version",
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion,
                    "--output", outputPath,
                    "--provider", "ace16",
                    "--allow-ace12-fallback",
                    "false",
                    "--command-timeout-seconds",
                    "30",
                    "--max-input-bytes",
                    "1048576",
                    "--max-package-bytes",
                    "1048576",
                ],
                output,
                error,
                dependencies,
                Cancellation);

        Assert.Equal(
            AccessWorkerRunner
                .ExitProviderUnavailable,
            exitCode);
        Assert.True(
            string.IsNullOrEmpty(
                output.ToString()));
        Assert.Equal(
            AccessWorkerRunner.Protocol +
                ":error:provider-unavailable\n",
            error.ToString());
        Assert.DoesNotContain(
            "private provider detail",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            input,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(outputPath));
    }

    private sealed class TemporaryDirectory
        : IDisposable
    {
        internal TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "csharpdb-access-worker-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Path);
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
