using System.Diagnostics;

namespace CSharpDB.Api.Tests;

internal static class ApiTestFileCleanup
{
    private static readonly TimeSpan CleanupTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(50);

    // Call only after the client and factory have finished asynchronous disposal.
    // Windows can still hold temporary file locks; permanent locks must fail cleanup.
    internal static async ValueTask DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var stopwatch = Stopwatch.StartNew();
        Exception? lastException = null;
        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex)
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex)
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;

            TimeSpan remaining = CleanupTimeout - stopwatch.Elapsed;
            if (remaining <= TimeSpan.Zero)
                break;

            // Teardown must run even if the test's cancellation token was cancelled.
            await Task.Delay(remaining < RetryDelay ? remaining : RetryDelay, CancellationToken.None);
        }

        throw new IOException(
            $"Failed to delete temporary database file '{path}' within the cleanup timeout ({CleanupTimeout}).",
            lastException);
    }
}
