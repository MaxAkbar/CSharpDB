using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Versioned SQL Server facts added to catalogs that are bound to one
/// provider-neutral retained row package.
/// </summary>
public static class SqlServerRetainedDataContract
{
    public const string CatalogContract =
        "csharpdb-sqlserver-retained-catalog/v1";

    public const string DataContract =
        "csharpdb-sqlserver-retained-data/v1";

    public const string RowOrderContract =
        "csharpdb-sqlserver-integer-key-order/v1";

    public const string ScalarCodecContract =
        "csharpdb-sqlserver-scalar/v1";

    public const string SnapshotIdentityPrefix =
        "sqlserver-retained:";

    public const string DataAvailableFacet =
        MigrationDataAvailabilityContract.AvailableFacet;

    public const string DataUnavailableReasonFacet =
        MigrationDataAvailabilityContract.UnavailableReasonFacet;
}

/// <summary>
/// Caller-selectable bounds for a retained SQL Server capture. Values can
/// narrow the fixed implementation ceilings but cannot raise them.
/// </summary>
public sealed record SqlServerRetainedCaptureOptions
{
    public const int MaximumTables = 10_000;
    public const int MaximumColumnsPerTable = 4_096;
    public const long MaximumRowsPerTable = 10_000_000_000;
    public const long MaximumRowsTotal = 10_000_000_000;
    public const int MaximumValueBytes = 64 * 1024 * 1024;
    public const int MinimumRowBytes = 1 + sizeof(int);
    public const int MaximumRowBytes = 256 * 1024 * 1024;
    public const long MinimumPackageBytes = 13;
    public const int MaximumRowCommandTimeoutSeconds =
        24 * 60 * 60;
    public const long MaximumPackageBytes =
        256L * 1024 * 1024 * 1024;

    public int MaxTables { get; init; } = MaximumTables;

    public int MaxColumnsPerTable { get; init; } =
        MaximumColumnsPerTable;

    public long MaxRowsPerTable { get; init; } =
        MaximumRowsPerTable;

    public long MaxRowsTotal { get; init; } =
        MaximumRowsTotal;

    public int MaxValueBytes { get; init; } =
        16 * 1024 * 1024;

    public int MaxRowBytes { get; init; } =
        64 * 1024 * 1024;

    public long MaxPackageBytes { get; init; } =
        MaximumPackageBytes;

    /// <summary>
    /// Per-table SQL command timeout. Cancellation remains the primary
    /// operator-controlled stop mechanism.
    /// </summary>
    public int RowCommandTimeoutSeconds { get; init; } =
        30 * 60;

    internal void Validate()
    {
        ValidateBound(
            nameof(MaxTables),
            MaxTables,
            MaximumTables);
        ValidateBound(
            nameof(MaxColumnsPerTable),
            MaxColumnsPerTable,
            MaximumColumnsPerTable);
        ValidateBound(
            nameof(MaxRowsPerTable),
            MaxRowsPerTable,
            MaximumRowsPerTable);
        ValidateBound(
            nameof(MaxRowsTotal),
            MaxRowsTotal,
            MaximumRowsTotal);
        ValidateBound(
            nameof(MaxValueBytes),
            MaxValueBytes,
            MaximumValueBytes);
        ValidateBound(
            nameof(MaxRowBytes),
            MaxRowBytes,
            MaximumRowBytes);
        ValidateBound(
            nameof(MaxPackageBytes),
            MaxPackageBytes,
            MaximumPackageBytes);
        ValidateBound(
            nameof(RowCommandTimeoutSeconds),
            RowCommandTimeoutSeconds,
            MaximumRowCommandTimeoutSeconds);
        if (MaxRowBytes < MinimumRowBytes)
        {
            throw new SqlServerRetainedCaptureLimitException(
                $"The configured row-byte bound must be at least {MinimumRowBytes} bytes for the retained row envelope.");
        }
        if (MaxPackageBytes < MinimumPackageBytes)
        {
            throw new SqlServerRetainedCaptureLimitException(
                $"The configured package-byte bound must be at least {MinimumPackageBytes} bytes for the retained package envelope.");
        }
        if (MaxValueBytes > MaxRowBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxValueBytes),
                "The value bound cannot exceed the row bound.");
        }
    }

    internal RetainedMigrationPackageWriteOptions
        ToRetainedOptions() => new()
        {
            MaxPackageBytes = MaxPackageBytes,
            MaxTables = MaxTables,
            MaxColumnsPerTable =
                MaxColumnsPerTable,
            MaxRowsPerTable = MaxRowsPerTable,
            MaxValueBytes = MaxValueBytes,
            MaxRowBytes = MaxRowBytes,
        };

    private static void ValidateBound(
        string name,
        int value,
        int maximum)
    {
        if (value <= 0 || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                $"The value must be from 1 through {maximum}.");
        }
    }

    private static void ValidateBound(
        string name,
        long value,
        long maximum)
    {
        if (value <= 0 || value > maximum)
        {
            throw new ArgumentOutOfRangeException(
                name,
                $"The value must be from 1 through {maximum}.");
        }
    }
}
