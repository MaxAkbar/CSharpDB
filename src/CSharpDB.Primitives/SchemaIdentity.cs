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
        Derive(LegacyNamespace, $"table:{tableName.ToUpperInvariant()}");

    public static Guid ForLegacyColumn(
        Guid tableId,
        string columnName,
        int ordinal) =>
        Derive(
            tableId,
            $"column:{ordinal}:{columnName.ToUpperInvariant()}");

    public static Guid ForLegacyConstraint(
        Guid tableId,
        string kind,
        string? name,
        int ordinal) =>
        Derive(
            tableId,
            $"{kind}:{ordinal}:{name?.ToUpperInvariant() ?? "<unnamed>"}");

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
