using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public static class DeterministicMigrationNameMapper
{
    public const string AlgorithmVersion = "csharpdb-name-v1";

    private const int HashCharacters = 16;
    private const int MaxCollisionResolutionPasses = 8;

    public static IReadOnlyDictionary<string, string> Map(MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        MigrationContractValidator.ValidateCatalog(catalog);

        IReadOnlyDictionary<string, MigrationCatalogObject> byId = catalog.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);

        var candidates = catalog.Objects
            .Select(item => new NameCandidate(
                item.ObjectId,
                GetTargetNameScope(item, byId),
                CreateReadableName(item, byId)))
            .ToArray();

        var collisions = FindCollisionGroups(candidates, item => item.Name)
            .SelectMany(group => group.Select(item => item.ObjectId))
            .ToHashSet(StringComparer.Ordinal);

        var resolved = candidates.ToDictionary(
            item => item.ObjectId,
            item => FinalizeName(
                item.Name,
                byId[item.ObjectId],
                forceSuffix: collisions.Contains(item.ObjectId),
                collisionResolutionPass: 0),
            StringComparer.Ordinal);

        for (int pass = 1; ; pass++)
        {
            IReadOnlyList<NameCandidate[]> finalizedCollisions = FindCollisionGroups(
                candidates,
                item => resolved[item.ObjectId]);
            if (finalizedCollisions.Count == 0)
            {
                return resolved
                    .OrderBy(item => item.Key, StringComparer.Ordinal)
                    .ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal);
            }

            if (pass > MaxCollisionResolutionPasses)
                break;

            foreach (NameCandidate candidate in finalizedCollisions.SelectMany(group => group))
            {
                resolved[candidate.ObjectId] = FinalizeName(
                    candidate.Name,
                    byId[candidate.ObjectId],
                    forceSuffix: true,
                    collisionResolutionPass: pass);
            }
        }

        throw new InvalidOperationException(
            $"Target names could not be made unique after {MaxCollisionResolutionPasses} deterministic passes.");
    }

    public static string? GetTargetParentObjectId(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById)
    {
        if (item.ParentObjectId is null)
            return null;

        return catalogObjectsById.TryGetValue(item.ParentObjectId, out MigrationCatalogObject? parent) &&
               parent.Kind == MigrationObjectKind.Namespace
            ? null
            : item.ParentObjectId;
    }

    internal static TargetNameScope GetTargetNameScope(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationCatalogObject> catalogObjectsById)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(catalogObjectsById);

        string? targetParentObjectId = GetTargetParentObjectId(item, catalogObjectsById);
        return item.Kind switch
        {
            MigrationObjectKind.Database => new TargetNameScope("database", null),
            MigrationObjectKind.Namespace => new TargetNameScope("namespace", targetParentObjectId),
            MigrationObjectKind.Table or MigrationObjectKind.Collection or MigrationObjectKind.View =>
                new TargetNameScope("relation", null),
            MigrationObjectKind.Column => new TargetNameScope("column", targetParentObjectId),
            MigrationObjectKind.Key or MigrationObjectKind.ForeignKey or MigrationObjectKind.CheckConstraint =>
                new TargetNameScope("constraint", targetParentObjectId),
            MigrationObjectKind.Index => new TargetNameScope("index", null),
            MigrationObjectKind.Trigger => new TargetNameScope("trigger", null),
            MigrationObjectKind.Sequence => new TargetNameScope("sequence", null),
            MigrationObjectKind.Routine => new TargetNameScope("routine", null),
            MigrationObjectKind.Other => new TargetNameScope("other", targetParentObjectId),
            _ => throw new ArgumentOutOfRangeException(nameof(item), item.Kind, "Unknown migration object kind."),
        };
    }

    private static IReadOnlyList<NameCandidate[]> FindCollisionGroups(
        IEnumerable<NameCandidate> candidates,
        Func<NameCandidate, string> getName) =>
        candidates
            .GroupBy(
                item => new TargetNameCollisionKey(item.Scope, getName(item)),
                TargetNameCollisionKeyComparer.Instance)
            .Where(group => group.Skip(1).Any())
            .Select(group => group.OrderBy(item => item.ObjectId, StringComparer.Ordinal).ToArray())
            .OrderBy(group => group[0].ObjectId, StringComparer.Ordinal)
            .ToArray();

    private static string CreateReadableName(
        MigrationCatalogObject item,
        IReadOnlyDictionary<string, MigrationCatalogObject> byId)
    {
        if (item.ParentObjectId is not null &&
            byId.TryGetValue(item.ParentObjectId, out MigrationCatalogObject? parent) &&
            parent.Kind == MigrationObjectKind.Namespace &&
            !IsDefaultNamespace(parent))
        {
            return $"{parent.SourceName}__{item.SourceName}";
        }

        return item.SourceName;
    }

    private static bool IsDefaultNamespace(MigrationCatalogObject item) =>
        item.Facets.Any(facet =>
            string.Equals(facet.Name, "isDefault", StringComparison.Ordinal) &&
            string.Equals(facet.Value, "true", StringComparison.OrdinalIgnoreCase));

    private static string FinalizeName(
        string readableName,
        MigrationCatalogObject item,
        bool forceSuffix,
        int collisionResolutionPass)
    {
        bool containsNul = readableName.IndexOf('\0') >= 0;
        bool reserved = MigrationContractValidator.IsReservedTargetName(readableName);
        bool overLength = readableName.Length > SqlIdentifierRules.MaxLength;
        if (!containsNul && !reserved && !overLength && !forceSuffix)
            return readableName;

        string prefix = readableName.Replace('\0', '_');
        if (reserved)
            prefix = $"migrated_{prefix}";
        if (string.IsNullOrWhiteSpace(prefix))
            prefix = item.Kind.ToString();

        return AppendHash(prefix, StableHashInput(item, collisionResolutionPass));
    }

    private static string AppendHash(string prefix, string hashInput)
    {
        string suffix = $"__{StableHash(hashInput)}";
        int maximumPrefixLength = SqlIdentifierRules.MaxLength - suffix.Length;
        int prefixLength = Math.Min(prefix.Length, maximumPrefixLength);
        if (prefixLength > 0 &&
            prefixLength < prefix.Length &&
            char.IsHighSurrogate(prefix[prefixLength - 1]))
        {
            prefixLength--;
        }

        return prefix[..prefixLength] + suffix;
    }

    private static string StableHashInput(MigrationCatalogObject item, int collisionResolutionPass)
    {
        var builder = new StringBuilder();
        AppendField(builder, AlgorithmVersion);
        AppendField(builder, item.Kind.ToString());
        AppendField(builder, item.ParentObjectId ?? string.Empty);
        AppendField(builder, item.SourceNamespace ?? string.Empty);
        AppendField(builder, item.ObjectId);
        AppendField(builder, item.SourceName);
        if (collisionResolutionPass > 0)
            AppendField(builder, collisionResolutionPass.ToString(System.Globalization.CultureInfo.InvariantCulture));
        return builder.ToString();
    }

    private static void AppendField(StringBuilder builder, string value) =>
        builder.Append(Encoding.UTF8.GetByteCount(value))
            .Append(':')
            .Append(value)
            .Append('|');

    private static string StableHash(string value)
    {
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(digest)[..HashCharacters].ToLowerInvariant();
    }

    internal readonly record struct TargetNameScope(string Namespace, string? OwnerObjectId);

    private readonly record struct TargetNameCollisionKey(TargetNameScope Scope, string Name);

    private sealed class TargetNameCollisionKeyComparer : IEqualityComparer<TargetNameCollisionKey>
    {
        public static TargetNameCollisionKeyComparer Instance { get; } = new();

        public bool Equals(TargetNameCollisionKey left, TargetNameCollisionKey right) =>
            left.Scope == right.Scope &&
            string.Equals(left.Name, right.Name, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(TargetNameCollisionKey key) =>
            HashCode.Combine(key.Scope, StringComparer.OrdinalIgnoreCase.GetHashCode(key.Name));
    }

    private sealed record NameCandidate(string ObjectId, TargetNameScope Scope, string Name);
}
