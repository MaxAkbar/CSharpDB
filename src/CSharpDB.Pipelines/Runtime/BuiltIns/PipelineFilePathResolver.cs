namespace CSharpDB.Pipelines.Runtime.BuiltIns;

internal static class PipelineFilePathResolver
{
    public static string ResolveExistingFile(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        foreach (string candidate in EnumerateCandidatePaths(path))
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return GetDefaultPath(path);
    }

    public static string ResolveOutputPath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return GetDefaultPath(path);
    }

    private static IEnumerable<string> EnumerateCandidatePaths(string path)
    {
        if (Path.IsPathRooted(path))
        {
            yield return Path.GetFullPath(path);
            yield break;
        }

        foreach (string root in GetSearchRoots())
        {
            yield return Path.GetFullPath(Path.Combine(root, path));
        }
    }

    private static string GetDefaultPath(string path) =>
        Path.IsPathRooted(path)
            ? Path.GetFullPath(path)
            : Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), path));

    private static IEnumerable<string> GetSearchRoots()
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string root in EnumerateBaseRoots())
        {
            foreach (string candidate in EnumerateAncestors(root))
            {
                if (seen.Add(candidate))
                {
                    yield return candidate;
                }
            }
        }
    }

    private static IEnumerable<string> EnumerateBaseRoots()
    {
        yield return Directory.GetCurrentDirectory();
        yield return AppContext.BaseDirectory;
    }

    private static IEnumerable<string> EnumerateAncestors(string path)
    {
        DirectoryInfo? directory = new(Path.GetFullPath(path));
        while (directory is not null)
        {
            yield return directory.FullName;
            directory = directory.Parent;
        }
    }
}
