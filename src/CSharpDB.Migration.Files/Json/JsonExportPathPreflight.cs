namespace CSharpDB.Migration.Files.Json;

internal static class JsonExportPathPreflight
{
    internal const string ReservedPrivatePathPrefix =
        ".csharpdb-json-export-";

    internal static void ValidateSourcePath(
        string sourcePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            sourcePath);
        if (sourcePath.Contains('\0'))
        {
            throw new ArgumentException(
                "JSON export source paths cannot contain NUL characters.",
                nameof(sourcePath));
        }
        RejectInvalidUnicode(
            sourcePath,
            nameof(sourcePath));
        if (!Path.IsPathFullyQualified(
                sourcePath))
        {
            throw new ArgumentException(
                "JSON export source paths must be fully qualified.",
                nameof(sourcePath));
        }
        RejectDotSegments(
            sourcePath,
            nameof(sourcePath));
        RejectWindowsSpecialPath(
            sourcePath,
            nameof(sourcePath));
        if (Path.EndsInDirectorySeparator(
                sourcePath))
        {
            throw new ArgumentException(
                "JSON export source paths must name files.",
                nameof(sourcePath));
        }

        string fullPath =
            Path.GetFullPath(sourcePath);
        if (!string.Equals(
                sourcePath,
                fullPath,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "JSON export source paths must already be normalized.",
                nameof(sourcePath));
        }
        string leaf =
            Path.GetFileName(fullPath);
        if (string.IsNullOrWhiteSpace(leaf) ||
            leaf is "." or "..")
        {
            throw new ArgumentException(
                "JSON export source file names are invalid.",
                nameof(sourcePath));
        }
        RejectReservedPrivateLeaf(
            fullPath,
            nameof(sourcePath));
    }

    internal static void RejectReservedPrivateLeaf(
        string path,
        string parameterName)
    {
        if (Path.GetFileName(path).StartsWith(
                ReservedPrivatePathPrefix,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Caller-chosen JSON export paths cannot occupy the reserved private namespace.",
                parameterName);
        }
    }

    private static void RejectDotSegments(
        string path,
        string parameterName)
    {
        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        foreach (
            string segment in
            path[root.Length..].Split(
                [
                    Path.DirectorySeparatorChar,
                    Path.AltDirectorySeparatorChar,
                ],
                StringSplitOptions.None))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    "JSON export source paths cannot contain traversal segments.",
                    parameterName);
            }
        }
    }

    private static void RejectInvalidUnicode(
        string path,
        string parameterName)
    {
        for (
            int index = 0;
            index < path.Length;
            index++)
        {
            char value = path[index];
            if (!char.IsSurrogate(value))
                continue;
            if (char.IsHighSurrogate(value) &&
                index + 1 < path.Length &&
                char.IsLowSurrogate(
                    path[index + 1]))
            {
                index++;
                continue;
            }

            throw new ArgumentException(
                "JSON export source paths must contain valid Unicode scalar data.",
                parameterName);
        }
    }

    private static void RejectWindowsSpecialPath(
        string path,
        string parameterName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        if (path.StartsWith(
                @"\\?\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                @"\\.\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                @"\\",
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths cannot be JSON export sources.",
                parameterName);
        }

        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        if (path.AsSpan(root.Length)
            .Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be JSON export sources.",
                parameterName);
        }
        foreach (
            string segment in
            path[root.Length..].Split(
                [
                    Path.DirectorySeparatorChar,
                    Path.AltDirectorySeparatorChar,
                ],
                StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment.IndexOfAny(
                    Path.GetInvalidFileNameChars()) >= 0)
            {
                throw new ArgumentException(
                    "Windows JSON export source path segments contain invalid file-name characters.",
                    parameterName);
            }
            if (segment.Contains('~'))
            {
                throw new ArgumentException(
                    "Windows DOS short-name aliases cannot be JSON export source paths.",
                    parameterName);
            }
            if (segment.EndsWith(' ') ||
                segment.EndsWith('.'))
            {
                throw new ArgumentException(
                    "Windows JSON export source path segments cannot end in spaces or dots.",
                    parameterName);
            }
            RejectReservedDeviceName(
                segment,
                parameterName);
        }
    }

    private static void RejectReservedDeviceName(
        string segment,
        string parameterName)
    {
        int firstDot =
            segment.IndexOf('.');
        string stem =
            (firstDot < 0
                ? segment
                : segment[..firstDot])
            .TrimEnd(' ', '.');
        if (stem.Equals(
                "CON",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "PRN",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "AUX",
                StringComparison.OrdinalIgnoreCase) ||
            stem.Equals(
                "NUL",
                StringComparison.OrdinalIgnoreCase) ||
            (stem.Length == 4 &&
             (stem.StartsWith(
                  "COM",
                  StringComparison.OrdinalIgnoreCase) ||
              stem.StartsWith(
                  "LPT",
                  StringComparison.OrdinalIgnoreCase)) &&
             stem[3] is
                 >= '1' and <= '9' or
                 '\u00b9' or
                 '\u00b2' or
                 '\u00b3'))
        {
            throw new ArgumentException(
                "Windows reserved device names cannot be JSON export source paths.",
                parameterName);
        }
    }
}
