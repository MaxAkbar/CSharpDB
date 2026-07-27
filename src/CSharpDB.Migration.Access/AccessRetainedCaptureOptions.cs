using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Access;

public sealed record AccessRetainedCaptureOptions
{
    public const long MaximumPackageBytes =
        256L * 1024 * 1024 * 1024;

    public const int MaximumTables = 8_192;

    public const int MaximumColumnsPerTable =
        2_048;

    public const long MaximumRowsPerTable =
        10_000_000_000;

    public const long MaximumRowsTotal =
        10_000_000_000;

    public const int MaximumValueBytes =
        64 * 1024 * 1024;

    public const int MaximumRowBytes =
        256 * 1024 * 1024;

    public const long MinimumPackageBytes = 13;

    public const int MinimumRowBytes =
        1 + sizeof(int);

    public AccessSourceOptions Source { get; init; } =
        new();

    public long MaxPackageBytes { get; init; } =
        MaximumPackageBytes;

    public int MaxTables { get; init; } =
        MaximumTables;

    public int MaxColumnsPerTable { get; init; } =
        MaximumColumnsPerTable;

    public long MaxRowsPerTable { get; init; } =
        MaximumRowsPerTable;

    public long MaxRowsTotal { get; init; } =
        MaximumRowsTotal;

    public int MaxValueBytes { get; init; } =
        MaximumValueBytes;

    public int MaxRowBytes { get; init; } =
        MaximumRowBytes;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Source);
        Source.Validate();
        if (MaxPackageBytes is <
                MinimumPackageBytes or
                > MaximumPackageBytes ||
            MaxTables is < 1 or
                > MaximumTables ||
            MaxColumnsPerTable is < 1 or
                > MaximumColumnsPerTable ||
            MaxRowsPerTable is < 1 or
                > MaximumRowsPerTable ||
            MaxRowsTotal is < 1 or
                > MaximumRowsTotal ||
            MaxValueBytes is < 1 or
                > MaximumValueBytes ||
            MaxRowBytes is < MinimumRowBytes or
                > MaximumRowBytes ||
            MaxValueBytes > MaxRowBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AccessRetainedCaptureOptions));
        }
    }

    internal RetainedMigrationPackageWriteOptions
        ToRetainedOptions() =>
        new()
        {
            MaxPackageBytes = MaxPackageBytes,
            MaxTables = MaxTables,
            MaxColumnsPerTable =
                MaxColumnsPerTable,
            MaxRowsPerTable = MaxRowsPerTable,
            MaxValueBytes = MaxValueBytes,
            MaxRowBytes = MaxRowBytes,
        };
}
