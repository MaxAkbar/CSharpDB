using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

internal readonly record struct FullTextToken(string Text, int Position);

internal sealed class FullTextTokenizer
{
    private readonly FullTextIndexOptions _options;

    public FullTextTokenizer(FullTextIndexOptions? options = null)
    {
        _options = options ?? new FullTextIndexOptions();
    }

    public IEnumerable<FullTextToken> Tokenize(string? input)
    {
        if (string.IsNullOrEmpty(input))
            yield break;

        string normalized = input.Normalize(_options.Normalization);
        if (_options.LowercaseInvariant)
            normalized = normalized.ToLowerInvariant();

        int position = 0;
        var sb = new StringBuilder();

        for (int i = 0; i < normalized.Length;)
        {
            if (!Rune.TryGetRuneAt(normalized, i, out var rune))
                break;

            if (IsWordRune(rune))
            {
                sb.Append(rune.ToString());
            }
            else if (sb.Length > 0)
            {
                yield return new FullTextToken(sb.ToString(), position++);
                sb.Clear();
            }

            i += rune.Utf16SequenceLength;
        }

        if (sb.Length > 0)
            yield return new FullTextToken(sb.ToString(), position);
    }

    private static bool IsWordRune(Rune rune)
        => Rune.IsLetterOrDigit(rune) || rune.Value == '\'';
}
