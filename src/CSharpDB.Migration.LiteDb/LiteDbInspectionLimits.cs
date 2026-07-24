namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// Fixed safety limits used by the LiteDB inspection and tagged-BSON
/// contracts. Test-only constructors can select lower values but never values
/// above these production ceilings.
/// </summary>
internal sealed record LiteDbInspectionLimits
{
    public const int MaximumCollections = 4_096;

    public const int MaximumIndexes = 32_768;

    public const long MaximumDocuments = 100_000_000;

    public const int MaximumFieldPaths = 262_144;

    public const int MaximumDepth = 64;

    public const int MaximumFieldsPerDocument = 65_536;

    public const int MaximumPropertyNameBytes = 64 * 1024;

    public const int MaximumStringBytes = 16 * 1024 * 1024;

    public const int MaximumBinaryBytes = 12 * 1024 * 1024;

    public const int MaximumCanonicalOutputBytes = 64 * 1024 * 1024;

    public const int MaximumTypedKeyBytes = 16 * 1024 * 1024;

    public const int MaximumJsonNodes = 65_536;

    public const int MaximumJsonContainerDepth = 128;

    public const int MaximumPathBytes = 256 * 1024;

    public const int MaximumMetadataBytes = 64 * 1024 * 1024;

    public static LiteDbInspectionLimits Default { get; } = new();

    public int MaxCollections { get; init; } = MaximumCollections;

    public int MaxIndexes { get; init; } = MaximumIndexes;

    public long MaxDocuments { get; init; } = MaximumDocuments;

    public int MaxFieldPaths { get; init; } = MaximumFieldPaths;

    public int MaxDepth { get; init; } = MaximumDepth;

    public int MaxFieldsPerDocument { get; init; } = MaximumFieldsPerDocument;

    public int MaxPropertyNameBytes { get; init; } = MaximumPropertyNameBytes;

    public int MaxStringBytes { get; init; } = MaximumStringBytes;

    public int MaxBinaryBytes { get; init; } = MaximumBinaryBytes;

    public int MaxCanonicalOutputBytes { get; init; } = MaximumCanonicalOutputBytes;

    public int MaxTypedKeyBytes { get; init; } = MaximumTypedKeyBytes;

    public int MaxJsonNodes { get; init; } = MaximumJsonNodes;

    public int MaxJsonContainerDepth { get; init; } = MaximumJsonContainerDepth;

    public int MaxPathBytes { get; init; } = MaximumPathBytes;

    public int MaxMetadataBytes { get; init; } = MaximumMetadataBytes;

    public void Validate()
    {
        Validate(nameof(MaxCollections), MaxCollections, MaximumCollections);
        Validate(nameof(MaxIndexes), MaxIndexes, MaximumIndexes);
        Validate(nameof(MaxDocuments), MaxDocuments, MaximumDocuments);
        Validate(nameof(MaxFieldPaths), MaxFieldPaths, MaximumFieldPaths);
        Validate(nameof(MaxDepth), MaxDepth, MaximumDepth);
        Validate(nameof(MaxFieldsPerDocument), MaxFieldsPerDocument, MaximumFieldsPerDocument);
        Validate(
            nameof(MaxPropertyNameBytes),
            MaxPropertyNameBytes,
            MaximumPropertyNameBytes);
        Validate(nameof(MaxStringBytes), MaxStringBytes, MaximumStringBytes);
        Validate(nameof(MaxBinaryBytes), MaxBinaryBytes, MaximumBinaryBytes);
        Validate(
            nameof(MaxCanonicalOutputBytes),
            MaxCanonicalOutputBytes,
            MaximumCanonicalOutputBytes);
        Validate(nameof(MaxTypedKeyBytes), MaxTypedKeyBytes, MaximumTypedKeyBytes);
        Validate(nameof(MaxJsonNodes), MaxJsonNodes, MaximumJsonNodes);
        Validate(
            nameof(MaxJsonContainerDepth),
            MaxJsonContainerDepth,
            MaximumJsonContainerDepth);
        Validate(nameof(MaxPathBytes), MaxPathBytes, MaximumPathBytes);
        Validate(nameof(MaxMetadataBytes), MaxMetadataBytes, MaximumMetadataBytes);
    }

    private static void Validate(string name, long value, long maximum)
    {
        if (value <= 0 || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                value,
                $"The limit must be between 1 and {maximum}.");
        }
    }
}
