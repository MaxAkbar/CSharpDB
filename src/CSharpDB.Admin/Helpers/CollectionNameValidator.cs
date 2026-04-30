using System.Text.RegularExpressions;

namespace CSharpDB.Admin.Helpers;

public static partial class CollectionNameValidator
{
    public static bool IsValid(string? name)
        => !string.IsNullOrWhiteSpace(name) && CollectionNamePattern().IsMatch(name.Trim());

    public static string Normalize(string name) => name.Trim();

    public const string HelpText = "Use letters, numbers, and underscores. The first character must be a letter or underscore.";

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex CollectionNamePattern();
}
