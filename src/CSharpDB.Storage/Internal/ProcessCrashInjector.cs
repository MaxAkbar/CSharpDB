using System.Threading;

namespace CSharpDB.Storage.Internal;

/// <summary>
/// Internal crash injection hook used by process-level durability tests to terminate
/// the current process at specific storage-engine phases.
/// </summary>
internal static class ProcessCrashInjector
{
    private const string CrashPointKey = "CSharpDB.TestCrashPoint";
    private const string MarkerPathKey = "CSharpDB.TestCrashMarkerPath";
    private static int _triggered;

    public static void TripIfRequested(string pointName, string markerText)
    {
        string? configuredPoint = AppContext.GetData(CrashPointKey) as string;
        if (!string.Equals(configuredPoint, pointName, StringComparison.Ordinal))
            return;

        if (Interlocked.Exchange(ref _triggered, 1) != 0)
            return;

        string? markerPath = AppContext.GetData(MarkerPathKey) as string;
        if (!string.IsNullOrWhiteSpace(markerPath))
        {
            string fullMarkerPath = Path.GetFullPath(markerPath);
            string? directory = Path.GetDirectoryName(fullMarkerPath);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);

            File.WriteAllText(fullMarkerPath, markerText);
        }

        Environment.FailFast($"CSharpDB process crash injector triggered at '{pointName}'.");
    }
}
