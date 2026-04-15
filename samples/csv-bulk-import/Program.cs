using System.Diagnostics;
using System.Globalization;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Primitives;

return await CsvBulkImportSampleProgram.MainAsync(args);

internal static class CsvBulkImportSampleProgram
{
    private static readonly string[] ExpectedHeader =
    [
        "id",
        "timestamp_utc",
        "source",
        "severity",
        "category",
        "payload_size",
        "ingest_ms",
    ];

    public static async Task<int> MainAsync(string[] args)
    {
        try
        {
            SampleOptions options = ParseArgs(args);
            string sampleDirectory = AppContext.BaseDirectory;
            string csvPath = Path.GetFullPath(options.CsvPath ?? Path.Combine(sampleDirectory, "events.csv"));
            string databasePath = Path.GetFullPath(options.DatabasePath ?? Path.Combine(sampleDirectory, "csv-bulk-import-demo.db"));

            PrepareDatabasePath(databasePath);

            var dbOptions = new DatabaseOptions()
                .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

            await using var db = await Database.OpenAsync(databasePath, dbOptions);
            await CreateSchemaAsync(db);

            var stopwatch = Stopwatch.StartNew();
            int importedRows = await ImportAsync(db, csvPath, options.BatchSize);
            await CreateIndexesAsync(db);
            stopwatch.Stop();

            double rowsPerSecond = stopwatch.Elapsed.TotalSeconds <= 0
                ? importedRows
                : importedRows / stopwatch.Elapsed.TotalSeconds;

            Console.WriteLine("CSV Bulk Import Sample");
            Console.WriteLine();
            Console.WriteLine($"CSV path:        {csvPath}");
            Console.WriteLine($"Database path:   {databasePath}");
            Console.WriteLine($"Batch size:      {options.BatchSize}");
            Console.WriteLine($"Rows imported:   {importedRows}");
            Console.WriteLine($"Elapsed:         {stopwatch.Elapsed.TotalMilliseconds:F1} ms");
            Console.WriteLine($"Throughput:      {rowsPerSecond:F1} rows/sec");
            Console.WriteLine();
            Console.WriteLine("Inspect the generated database with the CLI:");
            Console.WriteLine($"  dotnet run --project src/CSharpDB.Cli -- \"{databasePath}\"");
            Console.WriteLine("  csdb> SELECT severity, COUNT(*) FROM events GROUP BY severity ORDER BY severity;");
            Console.WriteLine("  csdb> SELECT source, AVG(ingest_ms) FROM events GROUP BY source ORDER BY source;");

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Import failed: {ex.Message}");
            return 1;
        }
    }

    private static SampleOptions ParseArgs(string[] args)
    {
        string? csvPath = null;
        string? databasePath = null;
        int batchSize = 1000;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--csv-path":
                    csvPath = ReadRequiredValue(args, ref i, "--csv-path");
                    break;
                case "--database-path":
                    databasePath = ReadRequiredValue(args, ref i, "--database-path");
                    break;
                case "--batch-size":
                    string rawBatchSize = ReadRequiredValue(args, ref i, "--batch-size");
                    if (!int.TryParse(rawBatchSize, NumberStyles.Integer, CultureInfo.InvariantCulture, out batchSize) || batchSize <= 0)
                        throw new ArgumentException($"Invalid value for --batch-size: '{rawBatchSize}'. Expected a positive integer.");
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown option '{args[i]}'. Supported options: --csv-path <path>, --database-path <path>, --batch-size <n>.");
            }
        }

        return new SampleOptions(csvPath, databasePath, batchSize);
    }

    private static string ReadRequiredValue(string[] args, ref int index, string optionName)
    {
        if (index + 1 >= args.Length)
            throw new ArgumentException($"Missing value for {optionName}.");

        index++;
        return args[index];
    }

    private static void PrepareDatabasePath(string databasePath)
    {
        string? directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        DeleteIfExists(databasePath);
        DeleteIfExists(databasePath + ".wal");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static async Task CreateSchemaAsync(Database db)
    {
        await db.ExecuteAsync("""
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                timestamp_utc TEXT NOT NULL,
                source TEXT NOT NULL,
                severity TEXT NOT NULL,
                category TEXT NOT NULL,
                payload_size INTEGER NOT NULL,
                ingest_ms REAL NOT NULL
            )
            """);
    }

    private static async Task CreateIndexesAsync(Database db)
    {
        await db.ExecuteAsync("CREATE INDEX idx_events_timestamp_utc ON events (timestamp_utc)");
        await db.ExecuteAsync("CREATE INDEX idx_events_severity ON events (severity)");
        await db.ExecuteAsync("CREATE INDEX idx_events_source ON events (source)");
    }

    private static async Task<int> ImportAsync(Database db, string csvPath, int batchSize)
    {
        if (!File.Exists(csvPath))
            throw new FileNotFoundException("Input CSV file was not found.", csvPath);

        using var stream = File.OpenRead(csvPath);
        using var reader = new StreamReader(stream);

        string? headerLine = await reader.ReadLineAsync();
        if (headerLine is null)
            throw new FormatException("CSV file is empty.");

        ValidateHeader(headerLine);

        var batch = db.PrepareInsertBatch("events", batchSize);
        int lineNumber = 1;
        int importedRows = 0;

        while (await reader.ReadLineAsync() is { } line)
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
                continue;

            string[] fields = ParseCsvLine(line, lineNumber);
            batch.AddRow(ConvertRow(fields, lineNumber));

            if (batch.Count >= batchSize)
                importedRows += await FlushBatchAsync(db, batch);
        }

        if (batch.Count > 0)
            importedRows += await FlushBatchAsync(db, batch);

        return importedRows;
    }

    private static void ValidateHeader(string headerLine)
    {
        string[] actualHeader = ParseCsvLine(headerLine, lineNumber: 1);
        if (actualHeader.Length != ExpectedHeader.Length)
        {
            throw new FormatException(
                $"Line 1: expected {ExpectedHeader.Length} header columns, got {actualHeader.Length}.");
        }

        for (int i = 0; i < ExpectedHeader.Length; i++)
        {
            if (!string.Equals(actualHeader[i], ExpectedHeader[i], StringComparison.OrdinalIgnoreCase))
            {
                throw new FormatException(
                    $"Line 1: expected header '{ExpectedHeader[i]}' at column {i + 1}, got '{actualHeader[i]}'.");
            }
        }
    }

    private static DbValue[] ConvertRow(string[] fields, int lineNumber)
    {
        if (fields.Length != ExpectedHeader.Length)
            throw new FormatException($"Line {lineNumber}: expected {ExpectedHeader.Length} columns, got {fields.Length}.");

        long id = ParseInt64(fields[0], "id", lineNumber);
        string timestamp = ParseTimestamp(fields[1], lineNumber);
        string source = ParseRequiredText(fields[2], "source", lineNumber);
        string severity = ParseRequiredText(fields[3], "severity", lineNumber);
        string category = ParseRequiredText(fields[4], "category", lineNumber);
        long payloadSize = ParseInt64(fields[5], "payload_size", lineNumber);
        double ingestMs = ParseDouble(fields[6], "ingest_ms", lineNumber);

        return
        [
            DbValue.FromInteger(id),
            DbValue.FromText(timestamp),
            DbValue.FromText(source),
            DbValue.FromText(severity),
            DbValue.FromText(category),
            DbValue.FromInteger(payloadSize),
            DbValue.FromReal(ingestMs),
        ];
    }

    private static string ParseTimestamp(string rawValue, int lineNumber)
    {
        string value = ParseRequiredText(rawValue, "timestamp_utc", lineNumber);
        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTimeOffset parsed))
            throw new FormatException($"Line {lineNumber}: invalid timestamp_utc '{rawValue}'.");

        return parsed.ToString("O", CultureInfo.InvariantCulture);
    }

    private static string ParseRequiredText(string rawValue, string columnName, int lineNumber)
    {
        string value = rawValue.Trim();
        if (string.IsNullOrEmpty(value))
            throw new FormatException($"Line {lineNumber}: column '{columnName}' is required.");

        return value;
    }

    private static long ParseInt64(string rawValue, string columnName, int lineNumber)
    {
        if (!long.TryParse(rawValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
            throw new FormatException($"Line {lineNumber}: invalid {columnName} '{rawValue}'.");

        return value;
    }

    private static double ParseDouble(string rawValue, string columnName, int lineNumber)
    {
        if (!double.TryParse(rawValue.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double value))
            throw new FormatException($"Line {lineNumber}: invalid {columnName} '{rawValue}'.");

        return value;
    }

    private static async Task<int> FlushBatchAsync(Database db, InsertBatch batch)
    {
        await db.BeginTransactionAsync();
        try
        {
            int rows = await batch.ExecuteAsync();
            await db.CommitAsync();
            return rows;
        }
        catch
        {
            await db.RollbackAsync();
            throw;
        }
    }

    private static string[] ParseCsvLine(string line, int lineNumber)
    {
        var fields = new List<string>();
        var current = new StringBuilder(line.Length);
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.Length && line[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                    }
                    else
                    {
                        inQuotes = false;
                    }
                }
                else
                {
                    current.Append(c);
                }
            }
            else
            {
                switch (c)
                {
                    case ',':
                        fields.Add(current.ToString().Trim());
                        current.Clear();
                        break;
                    case '"':
                        inQuotes = true;
                        break;
                    default:
                        current.Append(c);
                        break;
                }
            }
        }

        if (inQuotes)
            throw new FormatException($"Line {lineNumber}: unmatched quote in CSV row.");

        fields.Add(current.ToString().Trim());
        return [.. fields];
    }

    private sealed record SampleOptions(string? CsvPath, string? DatabasePath, int BatchSize);
}
