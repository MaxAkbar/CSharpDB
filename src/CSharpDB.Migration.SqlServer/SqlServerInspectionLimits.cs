namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Fixed safety limits for SQL Server catalog analysis. Tests may select lower
/// values but no caller can raise a limit above the qualified ceiling.
/// </summary>
internal sealed record SqlServerInspectionLimits
{
    public const int MaximumSchemas = 4_096;
    public const int MaximumTables = 10_000;
    public const int MaximumColumns = 20_000;
    public const int MaximumKeys = 20_000;
    public const int MaximumIndexes = 40_000;
    public const int MaximumIndexColumns = 160_000;
    public const int MaximumForeignKeys = 20_000;
    public const int MaximumForeignKeyColumns = 80_000;
    public const int MaximumChecks = 20_000;
    public const int MaximumSequences = 4_096;
    public const int MaximumUserTokens = 4_096;
    public const int MaximumPermissionDenials = 32_768;
    public const int MaximumStructuralRowsTotal = 250_000;
    public const int MaximumPermissionRowsTotal = 65_536;
    public const int MaximumNameBytes = 128 * 4;
    public const int MaximumExpressionBytes = 1024 * 1024;
    public const long MaximumExpressionBytesTotal = 64L * 1024 * 1024;
    public const long MaximumMetadataBytes = 128L * 1024 * 1024;

    public static SqlServerInspectionLimits Default { get; } = new();

    public int MaxSchemas { get; init; } = MaximumSchemas;
    public int MaxTables { get; init; } = MaximumTables;
    public int MaxColumns { get; init; } = MaximumColumns;
    public int MaxKeys { get; init; } = MaximumKeys;
    public int MaxIndexes { get; init; } = MaximumIndexes;
    public int MaxIndexColumns { get; init; } = MaximumIndexColumns;
    public int MaxForeignKeys { get; init; } = MaximumForeignKeys;
    public int MaxForeignKeyColumns { get; init; } = MaximumForeignKeyColumns;
    public int MaxChecks { get; init; } = MaximumChecks;
    public int MaxSequences { get; init; } = MaximumSequences;
    public int MaxUserTokens { get; init; } = MaximumUserTokens;
    public int MaxPermissionDenials { get; init; } = MaximumPermissionDenials;
    public int MaxStructuralRowsTotal { get; init; } = MaximumStructuralRowsTotal;
    public int MaxPermissionRowsTotal { get; init; } = MaximumPermissionRowsTotal;
    public int MaxNameBytes { get; init; } = MaximumNameBytes;
    public int MaxExpressionBytes { get; init; } = MaximumExpressionBytes;
    public long MaxExpressionBytesTotal { get; init; } = MaximumExpressionBytesTotal;
    public long MaxMetadataBytes { get; init; } = MaximumMetadataBytes;

    public void Validate()
    {
        Validate(nameof(MaxSchemas), MaxSchemas, MaximumSchemas);
        Validate(nameof(MaxTables), MaxTables, MaximumTables);
        Validate(nameof(MaxColumns), MaxColumns, MaximumColumns);
        Validate(nameof(MaxKeys), MaxKeys, MaximumKeys);
        Validate(nameof(MaxIndexes), MaxIndexes, MaximumIndexes);
        Validate(nameof(MaxIndexColumns), MaxIndexColumns, MaximumIndexColumns);
        Validate(nameof(MaxForeignKeys), MaxForeignKeys, MaximumForeignKeys);
        Validate(
            nameof(MaxForeignKeyColumns),
            MaxForeignKeyColumns,
            MaximumForeignKeyColumns);
        Validate(nameof(MaxChecks), MaxChecks, MaximumChecks);
        Validate(nameof(MaxSequences), MaxSequences, MaximumSequences);
        Validate(nameof(MaxUserTokens), MaxUserTokens, MaximumUserTokens);
        Validate(
            nameof(MaxPermissionDenials),
            MaxPermissionDenials,
            MaximumPermissionDenials);
        Validate(
            nameof(MaxStructuralRowsTotal),
            MaxStructuralRowsTotal,
            MaximumStructuralRowsTotal);
        Validate(
            nameof(MaxPermissionRowsTotal),
            MaxPermissionRowsTotal,
            MaximumPermissionRowsTotal);
        Validate(nameof(MaxNameBytes), MaxNameBytes, MaximumNameBytes);
        Validate(nameof(MaxExpressionBytes), MaxExpressionBytes, MaximumExpressionBytes);
        Validate(
            nameof(MaxExpressionBytesTotal),
            MaxExpressionBytesTotal,
            MaximumExpressionBytesTotal);
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
