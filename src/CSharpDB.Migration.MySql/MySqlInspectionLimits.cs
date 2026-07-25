namespace CSharpDB.Migration.MySql;

/// <summary>
/// Fixed safety limits for MySQL catalog analysis. Tests may select lower
/// values, but callers cannot raise a limit above the qualified ceiling.
/// </summary>
internal sealed record MySqlInspectionLimits
{
    public const int MaximumTables = 10_000;
    public const int MaximumColumns = 100_000;
    public const int MaximumViews = 10_000;
    public const int MaximumNameBytes = 64 * 4;
    public const int MaximumColumnTypeBytes = 1024 * 1024;
    public const int MaximumExpressionBytes = 1024 * 1024;
    public const long MaximumExpressionBytesTotal = 64L * 1024 * 1024;
    public const long MaximumMetadataBytes = 128L * 1024 * 1024;

    public static MySqlInspectionLimits Default { get; } = new();

    public int MaxTables { get; init; } = MaximumTables;

    public int MaxColumns { get; init; } = MaximumColumns;

    public int MaxViews { get; init; } = MaximumViews;

    public int MaxNameBytes { get; init; } = MaximumNameBytes;

    public int MaxColumnTypeBytes { get; init; } = MaximumColumnTypeBytes;

    public int MaxExpressionBytes { get; init; } = MaximumExpressionBytes;

    public long MaxExpressionBytesTotal { get; init; } =
        MaximumExpressionBytesTotal;

    public long MaxMetadataBytes { get; init; } = MaximumMetadataBytes;

    public void Validate()
    {
        Validate(nameof(MaxTables), MaxTables, MaximumTables);
        Validate(nameof(MaxColumns), MaxColumns, MaximumColumns);
        Validate(nameof(MaxViews), MaxViews, MaximumViews);
        Validate(nameof(MaxNameBytes), MaxNameBytes, MaximumNameBytes);
        Validate(
            nameof(MaxColumnTypeBytes),
            MaxColumnTypeBytes,
            MaximumColumnTypeBytes);
        Validate(
            nameof(MaxExpressionBytes),
            MaxExpressionBytes,
            MaximumExpressionBytes);
        Validate(
            nameof(MaxExpressionBytesTotal),
            MaxExpressionBytesTotal,
            MaximumExpressionBytesTotal);
        Validate(
            nameof(MaxMetadataBytes),
            MaxMetadataBytes,
            MaximumMetadataBytes);
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
