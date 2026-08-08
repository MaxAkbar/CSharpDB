using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Validation;

/// <summary>
/// One deterministic, target-inspectable attribute of a normalized schema object.
/// </summary>
public sealed record MigrationNormalizedSchemaAttribute
{
    public required string Name { get; init; }

    public required string Value { get; init; }
}

/// <summary>
/// One ordered structural reference in a normalized schema definition.
/// </summary>
public sealed record MigrationNormalizedSchemaMember
{
    public required string Role { get; init; }

    public int Ordinal { get; init; }

    public required string ObjectId { get; init; }
}

/// <summary>
/// Provider-neutral schema evidence. The source object id remains the stable
/// comparison identity while target names and definitions are read from the
/// concrete validation snapshot.
/// </summary>
public sealed record MigrationNormalizedSchemaObject
{
    public required string ObjectId { get; init; }

    public required MigrationObjectKind Kind { get; init; }

    public string? ParentObjectId { get; init; }

    public required string TargetName { get; init; }

    public IReadOnlyList<MigrationNormalizedSchemaAttribute> Attributes { get; init; } = [];

    public IReadOnlyList<MigrationNormalizedSchemaMember> Members { get; init; } = [];

    public required string DefinitionDigest { get; init; }
}

public sealed record MigrationNormalizedSchema
{
    public const string ContractVersion = "csharpdb-migration-schema/v1";

    public required string Digest { get; init; }

    public IReadOnlyList<MigrationNormalizedSchemaObject> Objects { get; init; } = [];
}

public sealed record MigrationNormalizedSchemaDifference
{
    public required string ObjectId { get; init; }

    public required MigrationObjectKind Kind { get; init; }

    public string? SourceDefinitionDigest { get; init; }

    public string? TargetDefinitionDigest { get; init; }
}

/// <summary>
/// Builds and compares deterministic normalized schema evidence. Definition
/// digests contain only structural metadata; source SQL text is represented by
/// its SHA-256 digest so validation reports never disclose it.
/// </summary>
public static class MigrationNormalizedSchemaContract
{
    private static readonly byte[] s_definitionDomain = Encoding.ASCII.GetBytes("CSDBSCHDEF1");
    private static readonly byte[] s_schemaDomain = Encoding.ASCII.GetBytes("CSDBSCHEMA1");

    public static MigrationNormalizedSchema CreateExpected(MigrationPlan plan, MigrationCatalog catalog)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(catalog);

        IReadOnlyDictionary<string, MigrationPlanObject> planned = plan.Objects
            .ToDictionary(item => item.SourceObjectId, StringComparer.Ordinal);
        HashSet<string> implicitIdentityColumns = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Key &&
                IsPrimaryKey(item) &&
                planned.TryGetValue(item.ObjectId, out MigrationPlanObject? keyPlan) &&
                keyPlan.Included)
            .Select(item => item.Members
                .Where(member => string.Equals(
                    member.Role,
                    MigrationObjectReferenceRoles.Column,
                    StringComparison.Ordinal))
                .OrderBy(member => member.Ordinal)
                .ToArray())
            .Where(members => members.Length == 1)
            .Select(members => members[0].ObjectId)
            .Where(columnId =>
                planned.TryGetValue(columnId, out MigrationPlanObject? columnPlan) &&
                columnPlan.Included &&
                columnPlan.TypeMappings.SingleOrDefault()?.TargetType == DbType.Integer)
            .ToHashSet(StringComparer.Ordinal);
        var definitions = new List<MigrationNormalizedSchemaObject>();

        foreach (MigrationCatalogObject item in catalog.Objects
                     .Where(item => IsPersistedKind(item.Kind))
                     .Where(item => planned.TryGetValue(item.ObjectId, out MigrationPlanObject? value) && value.Included)
                     .OrderBy(item => item.ObjectId, StringComparer.Ordinal))
        {
            MigrationPlanObject target = planned[item.ObjectId];
            var attributes = ExpectedAttributes(
                item,
                target,
                implicitIdentityColumns.Contains(item.ObjectId));
            var members = item.Members
                .OrderBy(member => member.Role, StringComparer.Ordinal)
                .ThenBy(member => member.Ordinal)
                .ThenBy(member => member.ObjectId, StringComparer.Ordinal)
                .Select(member => new MigrationNormalizedSchemaMember
                {
                    Role = member.Role,
                    Ordinal = member.Ordinal,
                    ObjectId = member.ObjectId,
                })
                .ToArray();

            definitions.Add(CreateObject(
                item.ObjectId,
                item.Kind,
                item.ParentObjectId,
                target.TargetName ?? throw new InvalidDataException(
                    $"Included migration object '{item.ObjectId}' has no target name."),
                attributes,
                members));
        }

        return Create(definitions);
    }

    public static MigrationNormalizedSchemaObject CreateObject(
        string objectId,
        MigrationObjectKind kind,
        string? parentObjectId,
        string targetName,
        IReadOnlyList<MigrationNormalizedSchemaAttribute>? attributes = null,
        IReadOnlyList<MigrationNormalizedSchemaMember>? members = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(objectId);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetName);

        MigrationNormalizedSchemaAttribute[] normalizedAttributes = (attributes ?? [])
            .OrderBy(attribute => attribute.Name, StringComparer.Ordinal)
            .ThenBy(attribute => attribute.Value, StringComparer.Ordinal)
            .ToArray();
        MigrationNormalizedSchemaMember[] normalizedMembers = (members ?? [])
            .OrderBy(member => member.Role, StringComparer.Ordinal)
            .ThenBy(member => member.Ordinal)
            .ThenBy(member => member.ObjectId, StringComparer.Ordinal)
            .ToArray();
        ValidateAttributes(normalizedAttributes, objectId);
        ValidateMembers(normalizedMembers, objectId);

        var definition = new MigrationNormalizedSchemaObject
        {
            ObjectId = objectId,
            Kind = kind,
            ParentObjectId = parentObjectId,
            TargetName = targetName,
            Attributes = normalizedAttributes,
            Members = normalizedMembers,
            DefinitionDigest = string.Empty,
        };
        return definition with { DefinitionDigest = ComputeDefinitionDigest(definition) };
    }

    public static MigrationNormalizedSchema Create(
        IEnumerable<MigrationNormalizedSchemaObject> objects)
    {
        ArgumentNullException.ThrowIfNull(objects);
        MigrationNormalizedSchemaObject[] normalized = objects
            .Select(NormalizeAndVerify)
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (normalized.Select(item => item.ObjectId).Distinct(StringComparer.Ordinal).Count() != normalized.Length)
            throw new InvalidDataException("Normalized validation schema contains duplicate object identities.");

        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(s_schemaDomain);
        AppendString(hash, MigrationNormalizedSchema.ContractVersion);
        AppendUInt32(hash, checked((uint)normalized.Length));
        foreach (MigrationNormalizedSchemaObject item in normalized)
        {
            AppendString(hash, item.ObjectId);
            hash.AppendData(Convert.FromHexString(item.DefinitionDigest));
        }

        return new MigrationNormalizedSchema
        {
            Digest = Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant(),
            Objects = normalized,
        };
    }

    public static IReadOnlyList<MigrationNormalizedSchemaDifference> Compare(
        MigrationNormalizedSchema source,
        MigrationNormalizedSchema target)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        MigrationNormalizedSchema verifiedSource = Create(source.Objects);
        MigrationNormalizedSchema verifiedTarget = Create(target.Objects);
        if (!string.Equals(source.Digest, verifiedSource.Digest, StringComparison.Ordinal) ||
            !string.Equals(target.Digest, verifiedTarget.Digest, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Normalized validation schema digest does not match its definitions.");
        }

        IReadOnlyDictionary<string, MigrationNormalizedSchemaObject> sourceById = verifiedSource.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);
        IReadOnlyDictionary<string, MigrationNormalizedSchemaObject> targetById = verifiedTarget.Objects
            .ToDictionary(item => item.ObjectId, StringComparer.Ordinal);

        return sourceById.Keys.Concat(targetById.Keys)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(id => id, StringComparer.Ordinal)
            .Where(id => !sourceById.TryGetValue(id, out MigrationNormalizedSchemaObject? left) ||
                !targetById.TryGetValue(id, out MigrationNormalizedSchemaObject? right) ||
                !string.Equals(left.DefinitionDigest, right.DefinitionDigest, StringComparison.Ordinal))
            .Select(id =>
            {
                sourceById.TryGetValue(id, out MigrationNormalizedSchemaObject? left);
                targetById.TryGetValue(id, out MigrationNormalizedSchemaObject? right);
                return new MigrationNormalizedSchemaDifference
                {
                    ObjectId = id,
                    Kind = left?.Kind ?? right!.Kind,
                    SourceDefinitionDigest = left?.DefinitionDigest,
                    TargetDefinitionDigest = right?.DefinitionDigest,
                };
            })
            .ToArray();
    }

    private static MigrationNormalizedSchemaObject NormalizeAndVerify(
        MigrationNormalizedSchemaObject item)
    {
        ArgumentNullException.ThrowIfNull(item);
        MigrationNormalizedSchemaObject normalized = CreateObject(
            item.ObjectId,
            item.Kind,
            item.ParentObjectId,
            item.TargetName,
            item.Attributes,
            item.Members);
        if (!string.IsNullOrEmpty(item.DefinitionDigest) &&
            !string.Equals(item.DefinitionDigest, normalized.DefinitionDigest, StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"Normalized schema definition digest for '{item.ObjectId}' does not match its metadata.");
        }
        return normalized;
    }

    private static string ComputeDefinitionDigest(MigrationNormalizedSchemaObject item)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(s_definitionDomain);
        AppendString(hash, item.ObjectId);
        AppendString(hash, item.Kind.ToString());
        AppendNullableString(hash, item.ParentObjectId);
        AppendString(hash, item.TargetName);
        AppendUInt32(hash, checked((uint)item.Attributes.Count));
        foreach (MigrationNormalizedSchemaAttribute attribute in item.Attributes)
        {
            AppendString(hash, attribute.Name);
            AppendString(hash, attribute.Value);
        }
        AppendUInt32(hash, checked((uint)item.Members.Count));
        foreach (MigrationNormalizedSchemaMember member in item.Members)
        {
            AppendString(hash, member.Role);
            AppendInt32(hash, member.Ordinal);
            AppendString(hash, member.ObjectId);
        }
        return Convert.ToHexString(hash.GetHashAndReset()).ToLowerInvariant();
    }

    private static IReadOnlyList<MigrationNormalizedSchemaAttribute> ExpectedAttributes(
        MigrationCatalogObject item,
        MigrationPlanObject planned,
        bool implicitIdentity)
    {
        var attributes = new List<MigrationNormalizedSchemaAttribute>();
        void Add(string name, string? value)
        {
            if (value is not null)
                attributes.Add(new MigrationNormalizedSchemaAttribute { Name = name, Value = value });
        }

        switch (item.Kind)
        {
            case MigrationObjectKind.Column:
                MigrationTypeMapping mapping = planned.TypeMappings.Single();
                Add("targetType", mapping.TargetType?.ToString() ?? "none");
                Add("targetSqlType", mapping.TargetSqlType);
                if (CSharpDbDeclaredTypeContract.TryRead(
                        item,
                        out SqlTypeDescriptor declaredType) &&
                    declaredType.StorageType == mapping.TargetType)
                {
                    Add("declaredType", declaredType.ToSql());
                }
                Add("nullable", Facet(item, "nullable") is string nullable
                    ? bool.Parse(nullable).ToString().ToLowerInvariant()
                    : "true");
                // CSharpDB normalizes a single-column INTEGER primary key to
                // an identity column when the primary-key constraint is added.
                // Expected evidence describes the persisted target shape, not
                // only the source facet, so account for that engine invariant.
                Add("identity", implicitIdentity || IsTrueFacet(item, "identity")
                    ? "true"
                    : "false");
                Add("rowVersion", BooleanFacet(item, "rowVersion"));
                Add("collation", Facet(item, "collation"));
                Add("defaultSqlDigest", SqlDigest(item, "defaultExpression"));
                break;

            case MigrationObjectKind.Index:
                Add("unique", BooleanFacet(item, "unique"));
                break;

            case MigrationObjectKind.Key:
                Add("kind", NormalizeToken(Facet(item, "kind")));
                break;

            case MigrationObjectKind.ForeignKey:
                Add("onDelete", NormalizeToken(Facet(item, "onDelete"), "restrict"));
                Add("onUpdate", NormalizeToken(Facet(item, "onUpdate"), "restrict"));
                break;

            case MigrationObjectKind.CheckConstraint:
            case MigrationObjectKind.View:
            case MigrationObjectKind.Trigger:
                Add("targetSqlDigest", SqlDigest(item, "targetSql"));
                break;
        }

        return attributes;
    }

    private static void ValidateAttributes(
        IReadOnlyList<MigrationNormalizedSchemaAttribute> attributes,
        string objectId)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (MigrationNormalizedSchemaAttribute attribute in attributes)
        {
            if (attribute is null || string.IsNullOrWhiteSpace(attribute.Name) || attribute.Value is null)
                throw new InvalidDataException($"Normalized schema attributes for '{objectId}' are invalid.");
            if (!names.Add(attribute.Name))
                throw new InvalidDataException($"Normalized schema object '{objectId}' has duplicate attribute '{attribute.Name}'.");
        }
    }

    private static void ValidateMembers(
        IReadOnlyList<MigrationNormalizedSchemaMember> members,
        string objectId)
    {
        var identities = new HashSet<string>(StringComparer.Ordinal);
        foreach (MigrationNormalizedSchemaMember member in members)
        {
            if (member is null || string.IsNullOrWhiteSpace(member.Role) ||
                string.IsNullOrWhiteSpace(member.ObjectId) || member.Ordinal < 0)
            {
                throw new InvalidDataException($"Normalized schema members for '{objectId}' are invalid.");
            }
            if (!identities.Add($"{member.Role}\0{member.Ordinal}"))
            {
                throw new InvalidDataException(
                    $"Normalized schema object '{objectId}' has duplicate role/ordinal members.");
            }
        }
    }

    private static string? Facet(MigrationCatalogObject item, string name) => item.Facets
        .FirstOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static string BooleanFacet(MigrationCatalogObject item, string name) =>
        bool.TryParse(Facet(item, name), out bool value) && value ? "true" : "false";

    private static bool IsTrueFacet(MigrationCatalogObject item, string name) =>
        bool.TryParse(Facet(item, name), out bool value) && value;

    private static bool IsPrimaryKey(MigrationCatalogObject item) =>
        NormalizeToken(Facet(item, "kind")) is "primary" or "primary-key";

    private static string? SqlDigest(MigrationCatalogObject item, string facetName) =>
        Facet(item, facetName) is string sql
            ? Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql))).ToLowerInvariant()
            : null;

    private static string NormalizeToken(string? value, string? fallback = null)
    {
        string normalized = (value ?? fallback ?? string.Empty)
            .Trim()
            .Replace('_', '-')
            .ToLowerInvariant();
        if (normalized.StartsWith("on-delete-", StringComparison.Ordinal))
            normalized = normalized["on-delete-".Length..];
        if (normalized.StartsWith("on-update-", StringComparison.Ordinal))
            normalized = normalized["on-update-".Length..];
        return normalized;
    }

    private static bool IsPersistedKind(MigrationObjectKind kind) => kind is
        MigrationObjectKind.Table or
        MigrationObjectKind.Collection or
        MigrationObjectKind.Column or
        MigrationObjectKind.Index or
        MigrationObjectKind.Key or
        MigrationObjectKind.ForeignKey or
        MigrationObjectKind.CheckConstraint or
        MigrationObjectKind.View or
        MigrationObjectKind.Trigger;

    private static void AppendNullableString(IncrementalHash hash, string? value)
    {
        hash.AppendData(value is null ? [0] : [1]);
        if (value is not null)
            AppendString(hash, value);
    }

    private static void AppendString(IncrementalHash hash, string value)
    {
        byte[] bytes = new UTF8Encoding(false, true).GetBytes(value);
        AppendUInt32(hash, checked((uint)bytes.Length));
        hash.AppendData(bytes);
    }

    private static void AppendUInt32(IncrementalHash hash, uint value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }

    private static void AppendInt32(IncrementalHash hash, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        hash.AppendData(bytes);
    }
}

/// <summary>
/// Optional extension implemented by validation snapshots that can expose
/// actual normalized schema evidence from the same coherent view as rows.
/// </summary>
public interface IMigrationSchemaValidationSnapshot : IValidationSnapshot
{
    ValueTask<MigrationNormalizedSchema> ReadSchemaAsync(
        CancellationToken cancellationToken = default);
}
