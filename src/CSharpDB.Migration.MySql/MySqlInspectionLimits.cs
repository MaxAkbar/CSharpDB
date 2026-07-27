namespace CSharpDB.Migration.MySql;

/// <summary>
/// Fixed safety limits for MySQL catalog analysis. Tests may select lower
/// values, but callers cannot raise a limit above the qualified ceiling.
/// </summary>
internal sealed record MySqlInspectionLimits
{
    public const int MaximumTables = 10_000;
    public const int MaximumColumns = 100_000;
    public const int MaximumTableDefinitions = MaximumTables;
    public const int MaximumKeys = 20_000;
    public const int MaximumKeyColumns = 80_000;
    public const int MaximumForeignKeys = 20_000;
    public const int MaximumForeignKeyColumns = 80_000;
    public const int MaximumChecks = 20_000;
    public const int MaximumIndexes = 40_000;
    public const int MaximumIndexParts = 160_000;
    public const int MaximumViews = 10_000;
    public const int MaximumViewColumns = 100_000;
    public const int MaximumTriggers = 20_000;
    public const int MaximumRoutines = 20_000;
    public const int MaximumRoutineParameters = 80_000;
    public const int MaximumStructuralRowsTotal = 500_000;
    public const int MaximumNameBytes = 64 * 4;
    public const int MaximumColumnTypeBytes = 1024 * 1024;
    public const int MaximumExpressionBytes = 1024 * 1024;
    public const long MaximumExpressionBytesTotal = 64L * 1024 * 1024;
    public const int MaximumDefinitionBytes = 1024 * 1024;
    public const long MaximumDefinitionBytesTotal = 64L * 1024 * 1024;
    public const long MaximumMetadataBytes = 128L * 1024 * 1024;

    public static MySqlInspectionLimits Default { get; } = new();

    public int MaxTables { get; init; } = MaximumTables;

    public int MaxColumns { get; init; } = MaximumColumns;

    public int MaxTableDefinitions { get; init; } = MaximumTableDefinitions;

    public int MaxKeys { get; init; } = MaximumKeys;

    public int MaxKeyColumns { get; init; } = MaximumKeyColumns;

    public int MaxForeignKeys { get; init; } = MaximumForeignKeys;

    public int MaxForeignKeyColumns { get; init; } = MaximumForeignKeyColumns;

    public int MaxChecks { get; init; } = MaximumChecks;

    public int MaxIndexes { get; init; } = MaximumIndexes;

    public int MaxIndexParts { get; init; } = MaximumIndexParts;

    public int MaxViews { get; init; } = MaximumViews;

    public int MaxViewColumns { get; init; } = MaximumViewColumns;

    public int MaxTriggers { get; init; } = MaximumTriggers;

    public int MaxRoutines { get; init; } = MaximumRoutines;

    public int MaxRoutineParameters { get; init; } =
        MaximumRoutineParameters;

    public int MaxStructuralRowsTotal { get; init; } =
        MaximumStructuralRowsTotal;

    public int MaxNameBytes { get; init; } = MaximumNameBytes;

    public int MaxColumnTypeBytes { get; init; } = MaximumColumnTypeBytes;

    public int MaxExpressionBytes { get; init; } = MaximumExpressionBytes;

    public long MaxExpressionBytesTotal { get; init; } =
        MaximumExpressionBytesTotal;

    public int MaxDefinitionBytes { get; init; } = MaximumDefinitionBytes;

    public long MaxDefinitionBytesTotal { get; init; } =
        MaximumDefinitionBytesTotal;

    public long MaxMetadataBytes { get; init; } = MaximumMetadataBytes;

    public void Validate()
    {
        Validate(nameof(MaxTables), MaxTables, MaximumTables);
        Validate(nameof(MaxColumns), MaxColumns, MaximumColumns);
        Validate(
            nameof(MaxTableDefinitions),
            MaxTableDefinitions,
            MaximumTableDefinitions);
        Validate(nameof(MaxKeys), MaxKeys, MaximumKeys);
        Validate(nameof(MaxKeyColumns), MaxKeyColumns, MaximumKeyColumns);
        Validate(nameof(MaxForeignKeys), MaxForeignKeys, MaximumForeignKeys);
        Validate(
            nameof(MaxForeignKeyColumns),
            MaxForeignKeyColumns,
            MaximumForeignKeyColumns);
        Validate(nameof(MaxChecks), MaxChecks, MaximumChecks);
        Validate(nameof(MaxIndexes), MaxIndexes, MaximumIndexes);
        Validate(nameof(MaxIndexParts), MaxIndexParts, MaximumIndexParts);
        Validate(nameof(MaxViews), MaxViews, MaximumViews);
        Validate(
            nameof(MaxViewColumns),
            MaxViewColumns,
            MaximumViewColumns);
        Validate(nameof(MaxTriggers), MaxTriggers, MaximumTriggers);
        Validate(nameof(MaxRoutines), MaxRoutines, MaximumRoutines);
        Validate(
            nameof(MaxRoutineParameters),
            MaxRoutineParameters,
            MaximumRoutineParameters);
        Validate(
            nameof(MaxStructuralRowsTotal),
            MaxStructuralRowsTotal,
            MaximumStructuralRowsTotal);
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
            nameof(MaxDefinitionBytes),
            MaxDefinitionBytes,
            MaximumDefinitionBytes);
        Validate(
            nameof(MaxDefinitionBytesTotal),
            MaxDefinitionBytesTotal,
            MaximumDefinitionBytesTotal);
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
