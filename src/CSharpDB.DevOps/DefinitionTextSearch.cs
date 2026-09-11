using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public static class DefinitionTextSearch
{
    public static IReadOnlyList<DefinitionSearchResult> Search(IEnumerable<DefinitionCatalogRecord> definitions,
        string text, string? kind = null, bool matchCase = false, bool wholeWord = false, CancellationToken ct = default)
    {
        var results = new List<DefinitionSearchResult>();
        foreach (var definition in definitions)
        {
            ct.ThrowIfCancellationRequested();
            if (!string.IsNullOrEmpty(kind) && definition.Kind != kind) continue;
            var matches = Find(definition.Source, text, matchCase, wholeWord, ct);
            bool nameMatched = Find(definition.Name, text, matchCase, wholeWord, ct).Count > 0;
            if (text.Length == 0 || matches.Count > 0 || nameMatched) results.Add(new(definition, matches, nameMatched));
        }
        return results.OrderByDescending(r => r.NameMatched).ThenBy(r => r.Definition.Name, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public static IReadOnlyList<DefinitionTextMatch> Find(string source, string text, bool matchCase = false,
        bool wholeWord = false, CancellationToken ct = default)
    {
        if (text.Length == 0) return [];
        var matches = new List<DefinitionTextMatch>();
        var comparison = matchCase ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
        int offset = 0, line = 1, lineStart = 0, scanned = 0;
        while (offset <= source.Length - text.Length)
        {
            ct.ThrowIfCancellationRequested();
            int start = source.IndexOf(text, offset, comparison);
            if (start < 0) break;
            int end = start + text.Length;
            if (!wholeWord || ((start == 0 || !IdentifierPart(source[start - 1])) && (end == source.Length || !IdentifierPart(source[end]))))
            {
                for (; scanned < start; scanned++)
                    if (source[scanned] == '\n' || (source[scanned] == '\r' && (scanned + 1 == source.Length || source[scanned + 1] != '\n')))
                    { line++; lineStart = scanned + 1; }
                matches.Add(new(start, text.Length, line, start - lineStart + 1));
            }
            offset = start + Math.Max(1, text.Length);
        }
        return matches;
    }

    public static string Snippet(string source, DefinitionTextMatch? match, int context = 70)
    {
        int start = Math.Max(0, (match?.Start ?? 0) - context);
        int end = Math.Min(source.Length, (match?.Start ?? 0) + (match?.Length ?? 0) + context);
        return (start > 0 ? "…" : "") + source[start..end].Replace('\r', ' ').Replace('\n', ' ') + (end < source.Length ? "…" : "");
    }
    private static bool IdentifierPart(char c) => char.IsLetterOrDigit(c) || c is '_' or '$' || char.GetUnicodeCategory(c) is
        System.Globalization.UnicodeCategory.NonSpacingMark or System.Globalization.UnicodeCategory.SpacingCombiningMark or System.Globalization.UnicodeCategory.ConnectorPunctuation;
}
