using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Primitives;

/// <summary>
/// Creates stable schema identities and deterministic compatibility identities
/// for metadata written before persisted identities were introduced.
/// </summary>
public static class SchemaIdentity
{
    private static readonly Guid LegacyNamespace =
        new("db684490-d447-4ff2-a285-4e68b7f68431");

    public static Guid Create() => Guid.NewGuid();

    public static Guid ForLegacyTable(string tableName) =>
        Derive(
            LegacyNamespace,
            $"table:{CanonicalizeOrdinalIgnoreCase(tableName)}");

    public static Guid ForLegacyColumn(
        Guid tableId,
        string columnName,
        int ordinal) =>
        Derive(
            tableId,
            $"column:{ordinal}:{CanonicalizeOrdinalIgnoreCase(columnName)}");

    public static Guid ForLegacyConstraint(
        Guid tableId,
        string kind,
        string? name,
        int ordinal) =>
        Derive(
            tableId,
            $"{kind}:{ordinal}:{(name is null ? "<unnamed>" : CanonicalizeOrdinalIgnoreCase(name))}");

    /// <summary>
    /// Produces a deterministic representative for the same equivalence relation
    /// used by <see cref="StringComparer.OrdinalIgnoreCase"/>.
    /// </summary>
    /// <remarks>
    /// Unconditionally calling <see cref="string.ToUpperInvariant"/> is not safe
    /// here: invariant casing folds some characters (for example, the long s)
    /// that ordinal-ignore-case deliberately keeps distinct. Apply a casing
    /// transform only when the comparer itself confirms that it is equivalent.
    /// </remarks>
    public static string CanonicalizeOrdinalIgnoreCase(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var canonical = new StringBuilder(value.Length);
        foreach (Rune rune in value.EnumerateRunes())
        {
            Rune upper = Rune.ToUpperInvariant(rune);
            string originalText = rune.ToString();
            string upperText = upper.ToString();
            canonical.Append(
                string.Equals(
                    originalText,
                    upperText,
                    StringComparison.OrdinalIgnoreCase)
                    ? upperText
                    : originalText);
        }

        return canonical.ToString();
    }

    private static Guid Derive(Guid namespaceId, string value)
    {
        byte[] namespaceBytes = namespaceId.ToByteArray();
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        byte[] input = new byte[namespaceBytes.Length + valueBytes.Length];
        namespaceBytes.CopyTo(input, 0);
        valueBytes.CopyTo(input, namespaceBytes.Length);
        byte[] digest = SHA256.HashData(input);
        return new Guid(digest.AsSpan(0, 16));
    }
}
