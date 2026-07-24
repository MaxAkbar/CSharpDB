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
    public const int MaximumNameBytes = 128 * 4;
    public const int MaximumExpressionBytes = 1024 * 1024;
    public const long MaximumExpressionBytesTotal = 64L * 1024 * 1024;
    public const long MaximumMetadataBytes = 128L * 1024 * 1024;

    public static SqlServerInspectionLimits Default { get; } = new();

    public int MaxSchemas { get; init; } = MaximumSchemas;
    public int MaxTables { get; init; } = MaximumTables;
    public int MaxColumns { get; init; } = MaximumColumns;
    public int MaxNameBytes { get; init; } = MaximumNameBytes;
    public int MaxExpressionBytes { get; init; } = MaximumExpressionBytes;
    public long MaxExpressionBytesTotal { get; init; } = MaximumExpressionBytesTotal;
    public long MaxMetadataBytes { get; init; } = MaximumMetadataBytes;

    public void Validate()
    {
        Validate(nameof(MaxSchemas), MaxSchemas, MaximumSchemas);
        Validate(nameof(MaxTables), MaxTables, MaximumTables);
        Validate(nameof(MaxColumns), MaxColumns, MaximumColumns);
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
