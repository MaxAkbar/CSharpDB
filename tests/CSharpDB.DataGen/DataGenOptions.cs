using System.Globalization;

namespace CSharpDB.DataGen;

public enum DatasetKind
{
    Relational,
    Documents,
    TimeSeries,
    FromDatabase,
}

public sealed class DataGenOptions
{
    public required DatasetKind Dataset { get; init; }
    public int Seed { get; init; } = 42;
    public long RowCount { get; init; }
    public int BatchSize { get; init; } = 1000;
    public bool DirectLoad { get; init; }
    public bool WriteFiles { get; init; } = true;
    public bool OverwriteDatabase { get; init; }
    public bool BuildIndexes { get; init; }
    public string OutputPath { get; init; } = default!;
    public string? DatabasePath { get; init; }
    public string? SpecPath { get; init; }
    public string? SourceDatabasePath { get; init; }
    public double NullRate { get; init; } = 0.05;
    public double HotKeyRate { get; init; } = 0.20;
    public double RecentRate { get; init; } = 0.80;
    public int AvgDocSizeBytes { get; init; } = 1024;
    public int TenantCount { get; init; } = 250;
    public int DeviceCount { get; init; } = 100_000;
    public int OrdersPerCustomer { get; init; } = 5;
    public int ItemsPerOrder { get; init; } = 4;

    public string DatasetLabel => Dataset switch
    {
        DatasetKind.Relational => "relational",
        DatasetKind.Documents => "documents",
        DatasetKind.TimeSeries => "timeseries",
        DatasetKind.FromDatabase => "fromdb",
        _ => throw new InvalidOperationException($"Unsupported dataset kind '{Dataset}'."),
    };

    public string ResolvedDatabasePath =>
        Path.GetFullPath(DatabasePath ?? Path.Combine(OutputPath, $"{DatasetLabel}.db"));

    public static DataGenOptions Parse(string[] args)
    {
        if (args.Length == 0)
            throw new DataGenUsageException("Missing dataset command.");

        DatasetKind dataset = args[0].ToLowerInvariant() switch
        {
            "relational" => DatasetKind.Relational,
            "docs" or "documents" => DatasetKind.Documents,
            "timeseries" or "time-series" => DatasetKind.TimeSeries,
            "fromdb" or "from-db" or "from-database" => DatasetKind.FromDatabase,
            "--help" or "-h" or "help" => throw new DataGenUsageException(null),
            _ => throw new DataGenUsageException($"Unknown dataset command '{args[0]}'."),
        };

        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var flags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 1; i < args.Length; i++)
        {
            string arg = args[i];
            if (arg is "--help" or "-h")
                throw new DataGenUsageException(null);

            if (!arg.StartsWith("--", StringComparison.Ordinal))
                throw new DataGenUsageException($"Unexpected argument '{arg}'.");

            string key = arg[2..].ToLowerInvariant();
            if (IsFlag(key))
            {
                flags.Add(key);
                continue;
            }

            if (i + 1 >= args.Length)
                throw new DataGenUsageException($"Missing value for option '{arg}'.");

            values[key] = args[++i];
        }

        long defaultRows = dataset switch
        {
            DatasetKind.Relational => 100_000,
            DatasetKind.Documents => 100_000,
            DatasetKind.TimeSeries => 1_000_000,
            DatasetKind.FromDatabase => 100_000,
            _ => 100_000,
        };

        int defaultDeviceCount = dataset == DatasetKind.TimeSeries ? 100_000 : 1_000;
        string defaultOutputPath = Path.GetFullPath(Path.Combine("artifacts", "data-gen", DatasetFolderName(dataset)));

        var options = new DataGenOptions
        {
            Dataset = dataset,
            Seed = ParseInt(values, "seed", 42),
            RowCount = ParseLong(values, "rows", defaultRows),
            BatchSize = ParseInt(values, "batch-size", 1000),
            DirectLoad = HasFlag(flags, "load-direct"),
            WriteFiles = !HasFlag(flags, "no-files"),
            OverwriteDatabase = HasFlag(flags, "overwrite-db"),
            BuildIndexes = HasFlag(flags, "build-indexes"),
            OutputPath = Path.GetFullPath(GetValue(values, "output-path", "output", defaultOutputPath)),
            DatabasePath = GetOptionalFullPath(values, "database-path", "db-path"),
            SpecPath = GetOptionalFullPath(values, "spec-path", "schema-path"),
            SourceDatabasePath = GetOptionalFullPath(values, "source-database", "source-db"),
            NullRate = ParseDouble(values, "null-rate", 0.05),
            HotKeyRate = ParseDouble(values, "hot-key-rate", 0.20),
            RecentRate = ParseDouble(values, "recent-rate", 0.80),
            AvgDocSizeBytes = ParseInt(values, "avg-size", ParseInt(values, "avg-doc-size", 1024)),
            TenantCount = ParseInt(values, "tenant-count", 250),
            DeviceCount = ParseInt(values, "device-count", defaultDeviceCount),
            OrdersPerCustomer = ParseInt(values, "orders-per-customer", 5),
            ItemsPerOrder = ParseInt(values, "items-per-order", 4),
        };

        Validate(options);
        return options;
    }

    private static bool IsFlag(string key)
        => key is "load-direct" or "no-files" or "overwrite-db" or "build-indexes";

    private static bool HasFlag(HashSet<string> flags, string key)
        => flags.Contains(key);

    private static string DatasetFolderName(DatasetKind dataset)
        => dataset switch
        {
            DatasetKind.Relational => "relational",
            DatasetKind.Documents => "documents",
            DatasetKind.TimeSeries => "timeseries",
            DatasetKind.FromDatabase => "fromdb",
            _ => throw new InvalidOperationException($"Unsupported dataset kind '{dataset}'."),
        };

    private static string GetValue(
        IReadOnlyDictionary<string, string> values,
        string primaryKey,
        string alternateKey,
        string fallback)
    {
        if (values.TryGetValue(primaryKey, out string? value))
            return value;

        return values.TryGetValue(alternateKey, out value) ? value : fallback;
    }

    private static string? GetOptionalFullPath(
        IReadOnlyDictionary<string, string> values,
        string primaryKey,
        string alternateKey)
    {
        if (values.TryGetValue(primaryKey, out string? value) ||
            values.TryGetValue(alternateKey, out value))
        {
            return Path.GetFullPath(value);
        }

        return null;
    }

    private static int ParseInt(IReadOnlyDictionary<string, string> values, string key, int fallback)
    {
        if (!values.TryGetValue(key, out string? raw))
            return fallback;

        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
            throw new DataGenUsageException($"Option '--{key}' expects an integer value.");

        return value;
    }

    private static long ParseLong(IReadOnlyDictionary<string, string> values, string key, long fallback)
    {
        if (!values.TryGetValue(key, out string? raw))
            return fallback;

        if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
            throw new DataGenUsageException($"Option '--{key}' expects an integer value.");

        return value;
    }

    private static double ParseDouble(IReadOnlyDictionary<string, string> values, string key, double fallback)
    {
        if (!values.TryGetValue(key, out string? raw))
            return fallback;

        if (!double.TryParse(raw, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double value))
            throw new DataGenUsageException($"Option '--{key}' expects a numeric value.");

        return value;
    }

    private static void Validate(DataGenOptions options)
    {
        if (options.RowCount <= 0)
            throw new DataGenUsageException("--rows must be a positive integer.");

        if (options.BatchSize <= 0)
            throw new DataGenUsageException("--batch-size must be a positive integer.");

        if (options.AvgDocSizeBytes <= 0)
            throw new DataGenUsageException("--avg-size must be a positive integer.");

        if (options.TenantCount <= 0)
            throw new DataGenUsageException("--tenant-count must be a positive integer.");

        if (options.DeviceCount <= 0)
            throw new DataGenUsageException("--device-count must be a positive integer.");

        if (options.OrdersPerCustomer <= 0)
            throw new DataGenUsageException("--orders-per-customer must be a positive integer.");

        if (options.ItemsPerOrder <= 0)
            throw new DataGenUsageException("--items-per-order must be a positive integer.");

        ValidateRate(options.NullRate, "--null-rate");
        ValidateRate(options.HotKeyRate, "--hot-key-rate");
        ValidateRate(options.RecentRate, "--recent-rate");

        if (options.Dataset == DatasetKind.FromDatabase && string.IsNullOrWhiteSpace(options.SourceDatabasePath))
            throw new DataGenUsageException("The fromdb command requires --source-database <path> to read schema from.");

        if (options.Dataset == DatasetKind.FromDatabase && !File.Exists(options.SourceDatabasePath))
            throw new DataGenUsageException($"Source database not found: '{options.SourceDatabasePath}'.");

        if (!options.WriteFiles && !options.DirectLoad)
            throw new DataGenUsageException("Choose at least one output path: files or --load-direct.");
    }

    private static void ValidateRate(double value, string optionName)
    {
        if (value < 0 || value > 1)
            throw new DataGenUsageException($"{optionName} must be between 0 and 1.");
    }
}

public sealed class DataGenUsageException : Exception
{
    public DataGenUsageException(string? message)
        : base(message)
    {
    }
}
