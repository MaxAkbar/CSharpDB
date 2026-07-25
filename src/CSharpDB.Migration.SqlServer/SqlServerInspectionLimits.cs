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
    public const int MaximumViews = 10_000;
    public const int MaximumViewColumns = 20_000;
    public const int MaximumTriggers = 20_000;
    public const int MaximumTriggerEvents = 80_000;
    public const int MaximumRoutines = 20_000;
    public const int MaximumModules = 40_000;
    public const int MaximumParameters = 80_000;
    public const int MaximumExpressionDependencies = 160_000;
    public const int MaximumFullTextCatalogs = 4_096;
    public const int MaximumFullTextStoplists = 4_096;
    public const int MaximumSearchPropertyLists = 4_096;
    public const int MaximumFullTextIndexes = 10_000;
    public const int MaximumFullTextIndexColumns = 20_000;
    public const int MaximumDataSpaces = 4_096;
    public const int MaximumPartitionSchemes = 4_096;
    public const int MaximumPartitionSchemeDestinations = 160_000;
    public const int MaximumPartitionFunctions = 4_096;
    public const int MaximumPartitionParameters = 4_096;
    public const int MaximumPartitionRangeValues = 160_000;
    public const int MaximumIndexPartitions = 160_000;
    public const int MaximumUserTokens = 4_096;
    public const int MaximumPermissionDenials = 32_768;
    public const int MaximumStructuralRowsTotal = 500_000;
    public const int MaximumPermissionRowsTotal = 65_536;
    public const int MaximumNameBytes = 128 * 4;
    public const int MaximumPartitionBoundaryBytes = 8_016;
    public const int MaximumExpressionBytes = 1024 * 1024;
    public const long MaximumExpressionBytesTotal = 64L * 1024 * 1024;
    public const long MaximumMetadataBytes = 128L * 1024 * 1024;
    public const int MaximumScriptDomTokensPerDefinition = 100_000;
    public const long MaximumScriptDomTokensTotal = 1_000_000;
    public const int MaximumScriptDomNodesPerDefinition = 100_000;
    public const long MaximumScriptDomNodesTotal = 1_000_000;
    public const int MaximumScriptDomParseErrorsPerDefinition = 64;
    public const long MaximumScriptDomParseErrorsTotal = 4_096;
    public const int MaximumScriptDomNestingPerDefinition = 256;
    public const int MaximumScriptDomStatementsPerDefinition = 10_000;
    public const long MaximumScriptDomStatementsTotal = 100_000;

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
    public int MaxViews { get; init; } = MaximumViews;
    public int MaxViewColumns { get; init; } = MaximumViewColumns;
    public int MaxTriggers { get; init; } = MaximumTriggers;
    public int MaxTriggerEvents { get; init; } = MaximumTriggerEvents;
    public int MaxRoutines { get; init; } = MaximumRoutines;
    public int MaxModules { get; init; } = MaximumModules;
    public int MaxParameters { get; init; } = MaximumParameters;
    public int MaxExpressionDependencies { get; init; } =
        MaximumExpressionDependencies;
    public int MaxFullTextCatalogs { get; init; } = MaximumFullTextCatalogs;
    public int MaxFullTextStoplists { get; init; } = MaximumFullTextStoplists;
    public int MaxSearchPropertyLists { get; init; } = MaximumSearchPropertyLists;
    public int MaxFullTextIndexes { get; init; } = MaximumFullTextIndexes;
    public int MaxFullTextIndexColumns { get; init; } =
        MaximumFullTextIndexColumns;
    public int MaxDataSpaces { get; init; } = MaximumDataSpaces;
    public int MaxPartitionSchemes { get; init; } = MaximumPartitionSchemes;
    public int MaxPartitionSchemeDestinations { get; init; } =
        MaximumPartitionSchemeDestinations;
    public int MaxPartitionFunctions { get; init; } = MaximumPartitionFunctions;
    public int MaxPartitionParameters { get; init; } = MaximumPartitionParameters;
    public int MaxPartitionRangeValues { get; init; } =
        MaximumPartitionRangeValues;
    public int MaxIndexPartitions { get; init; } = MaximumIndexPartitions;
    public int MaxUserTokens { get; init; } = MaximumUserTokens;
    public int MaxPermissionDenials { get; init; } = MaximumPermissionDenials;
    public int MaxStructuralRowsTotal { get; init; } = MaximumStructuralRowsTotal;
    public int MaxPermissionRowsTotal { get; init; } = MaximumPermissionRowsTotal;
    public int MaxNameBytes { get; init; } = MaximumNameBytes;
    public int MaxPartitionBoundaryBytes { get; init; } =
        MaximumPartitionBoundaryBytes;
    public int MaxExpressionBytes { get; init; } = MaximumExpressionBytes;
    public long MaxExpressionBytesTotal { get; init; } = MaximumExpressionBytesTotal;
    public long MaxMetadataBytes { get; init; } = MaximumMetadataBytes;
    public int MaxScriptDomTokensPerDefinition { get; init; } =
        MaximumScriptDomTokensPerDefinition;
    public long MaxScriptDomTokensTotal { get; init; } =
        MaximumScriptDomTokensTotal;
    public int MaxScriptDomNodesPerDefinition { get; init; } =
        MaximumScriptDomNodesPerDefinition;
    public long MaxScriptDomNodesTotal { get; init; } =
        MaximumScriptDomNodesTotal;
    public int MaxScriptDomParseErrorsPerDefinition { get; init; } =
        MaximumScriptDomParseErrorsPerDefinition;
    public long MaxScriptDomParseErrorsTotal { get; init; } =
        MaximumScriptDomParseErrorsTotal;
    public int MaxScriptDomNestingPerDefinition { get; init; } =
        MaximumScriptDomNestingPerDefinition;
    public int MaxScriptDomStatementsPerDefinition { get; init; } =
        MaximumScriptDomStatementsPerDefinition;
    public long MaxScriptDomStatementsTotal { get; init; } =
        MaximumScriptDomStatementsTotal;

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
        Validate(nameof(MaxViews), MaxViews, MaximumViews);
        Validate(nameof(MaxViewColumns), MaxViewColumns, MaximumViewColumns);
        Validate(nameof(MaxTriggers), MaxTriggers, MaximumTriggers);
        Validate(
            nameof(MaxTriggerEvents),
            MaxTriggerEvents,
            MaximumTriggerEvents);
        Validate(nameof(MaxRoutines), MaxRoutines, MaximumRoutines);
        Validate(nameof(MaxModules), MaxModules, MaximumModules);
        Validate(nameof(MaxParameters), MaxParameters, MaximumParameters);
        Validate(
            nameof(MaxExpressionDependencies),
            MaxExpressionDependencies,
            MaximumExpressionDependencies);
        Validate(
            nameof(MaxFullTextCatalogs),
            MaxFullTextCatalogs,
            MaximumFullTextCatalogs);
        Validate(
            nameof(MaxFullTextStoplists),
            MaxFullTextStoplists,
            MaximumFullTextStoplists);
        Validate(
            nameof(MaxSearchPropertyLists),
            MaxSearchPropertyLists,
            MaximumSearchPropertyLists);
        Validate(
            nameof(MaxFullTextIndexes),
            MaxFullTextIndexes,
            MaximumFullTextIndexes);
        Validate(
            nameof(MaxFullTextIndexColumns),
            MaxFullTextIndexColumns,
            MaximumFullTextIndexColumns);
        Validate(nameof(MaxDataSpaces), MaxDataSpaces, MaximumDataSpaces);
        Validate(
            nameof(MaxPartitionSchemes),
            MaxPartitionSchemes,
            MaximumPartitionSchemes);
        Validate(
            nameof(MaxPartitionSchemeDestinations),
            MaxPartitionSchemeDestinations,
            MaximumPartitionSchemeDestinations);
        Validate(
            nameof(MaxPartitionFunctions),
            MaxPartitionFunctions,
            MaximumPartitionFunctions);
        Validate(
            nameof(MaxPartitionParameters),
            MaxPartitionParameters,
            MaximumPartitionParameters);
        Validate(
            nameof(MaxPartitionRangeValues),
            MaxPartitionRangeValues,
            MaximumPartitionRangeValues);
        Validate(
            nameof(MaxIndexPartitions),
            MaxIndexPartitions,
            MaximumIndexPartitions);
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
        Validate(
            nameof(MaxPartitionBoundaryBytes),
            MaxPartitionBoundaryBytes,
            MaximumPartitionBoundaryBytes);
        Validate(nameof(MaxExpressionBytes), MaxExpressionBytes, MaximumExpressionBytes);
        Validate(
            nameof(MaxExpressionBytesTotal),
            MaxExpressionBytesTotal,
            MaximumExpressionBytesTotal);
        Validate(nameof(MaxMetadataBytes), MaxMetadataBytes, MaximumMetadataBytes);
        Validate(
            nameof(MaxScriptDomTokensPerDefinition),
            MaxScriptDomTokensPerDefinition,
            MaximumScriptDomTokensPerDefinition);
        Validate(
            nameof(MaxScriptDomTokensTotal),
            MaxScriptDomTokensTotal,
            MaximumScriptDomTokensTotal);
        Validate(
            nameof(MaxScriptDomNodesPerDefinition),
            MaxScriptDomNodesPerDefinition,
            MaximumScriptDomNodesPerDefinition);
        Validate(
            nameof(MaxScriptDomNodesTotal),
            MaxScriptDomNodesTotal,
            MaximumScriptDomNodesTotal);
        Validate(
            nameof(MaxScriptDomParseErrorsPerDefinition),
            MaxScriptDomParseErrorsPerDefinition,
            MaximumScriptDomParseErrorsPerDefinition);
        Validate(
            nameof(MaxScriptDomParseErrorsTotal),
            MaxScriptDomParseErrorsTotal,
            MaximumScriptDomParseErrorsTotal);
        Validate(
            nameof(MaxScriptDomNestingPerDefinition),
            MaxScriptDomNestingPerDefinition,
            MaximumScriptDomNestingPerDefinition);
        Validate(
            nameof(MaxScriptDomStatementsPerDefinition),
            MaxScriptDomStatementsPerDefinition,
            MaximumScriptDomStatementsPerDefinition);
        Validate(
            nameof(MaxScriptDomStatementsTotal),
            MaxScriptDomStatementsTotal,
            MaximumScriptDomStatementsTotal);
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
