namespace EfCoreProviderSample;

internal static class SamplePaths
{
    public static string GetDatabasePath(string[] args, string? baseDirectory = null)
    {
        string effectiveBaseDirectory = baseDirectory ?? Directory.GetCurrentDirectory();

        for (int i = 0; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], "--database-path", StringComparison.OrdinalIgnoreCase))
                return NormalizePath(args[i + 1], effectiveBaseDirectory);
        }

        return Path.Combine(effectiveBaseDirectory, "sample.db");
    }

    private static string NormalizePath(string path, string baseDirectory)
        => Path.IsPathRooted(path)
            ? Path.GetFullPath(path)
            : Path.GetFullPath(Path.Combine(baseDirectory, path));
}
