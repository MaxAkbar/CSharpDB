using CSharpDB.DataGen.Generators;
using CSharpDB.DataGen.Output;
using CSharpDB.DataGen.Specs;

namespace CSharpDB.DataGen;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        DataGenOptions options;
        try
        {
            options = DataGenOptions.Parse(args);
        }
        catch (DataGenUsageException ex)
        {
            if (!string.IsNullOrWhiteSpace(ex.Message))
                Console.Error.WriteLine(ex.Message);

            PrintHelp();
            return 1;
        }

        try
        {
            if (options.WriteFiles)
                Directory.CreateDirectory(options.OutputPath);

            Console.WriteLine($"Dataset     : {options.DatasetLabel}");
            Console.WriteLine($"Rows        : {options.RowCount:N0}");
            Console.WriteLine($"Seed        : {options.Seed}");
            Console.WriteLine($"Batch size  : {options.BatchSize:N0}");
            Console.WriteLine($"Write files : {options.WriteFiles}");
            Console.WriteLine($"Direct load : {options.DirectLoad}");

            RunSummary summary = options.Dataset switch
            {
                DatasetKind.Relational => await RunRelationalAsync(options),
                DatasetKind.Documents => await RunDocumentsAsync(options),
                DatasetKind.TimeSeries => await RunTimeSeriesAsync(options),
                DatasetKind.FromDatabase => await RunFromDatabaseAsync(options),
                _ => throw new InvalidOperationException($"Unsupported dataset kind '{options.Dataset}'."),
            };

            if (options.WriteFiles)
            {
                string summaryPath = Path.Combine(options.OutputPath, "summary.json");
                await JsonlWriter.WriteJsonAsync(summaryPath, summary);
                Console.WriteLine($"Summary     : {summaryPath}");
            }

            if (summary.DatabasePath != null)
                Console.WriteLine($"Database    : {summary.DatabasePath}");

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
    }

    private static async Task<RunSummary> RunRelationalAsync(DataGenOptions options)
    {
        LoadedDatasetSpec loadedSpec = DatasetSpecLoader.Load(options);
        DatasetGenerationPlan plan = SpecDataGenerator.CreatePlan(options, loadedSpec.Spec);

        Console.WriteLine(
            "Relational  : " +
            string.Join(
                ", ",
                loadedSpec.Spec.Tables.Select(table =>
                    $"{table.Name}={GetRequiredTableSource(plan.SqlSources, table.GeneratorKey).RowCount:N0}")));

        return await RunSqlDatasetAsync(options, loadedSpec, plan.SqlSources);
    }

    private static async Task<RunSummary> RunDocumentsAsync(DataGenOptions options)
    {
        LoadedDatasetSpec loadedSpec = DatasetSpecLoader.Load(options);
        DatasetGenerationPlan plan = SpecDataGenerator.CreatePlan(options, loadedSpec.Spec);
        var files = new List<string>();

        if (options.WriteFiles)
        {
            foreach (CollectionSpec collectionSpec in loadedSpec.Spec.Collections)
            {
                GeneratedCollectionSource source = GetRequiredCollectionSource(plan.CollectionSources, collectionSpec.GeneratorKey);
                string outputPath = Path.Combine(options.OutputPath, collectionSpec.OutputFileName);
                await JsonlWriter.WriteJsonLinesAsync(outputPath, source.CreateDocuments().Select(static document => document.Document));
                files.Add(outputPath);
            }
        }

        string? databasePath = null;
        if (options.DirectLoad)
            databasePath = await BinaryDirectLoader.LoadCollectionsAsync(options, loadedSpec.Spec.Collections, plan.CollectionSources);

        var counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (CollectionSpec collectionSpec in loadedSpec.Spec.Collections)
            counts[collectionSpec.Name] = GetRequiredCollectionSource(plan.CollectionSources, collectionSpec.GeneratorKey).RowCount;

        return new RunSummary(
            Dataset: options.DatasetLabel,
            Counts: counts,
            Files: files,
            DatabasePath: databasePath,
            Options: CreateSerializableOptions(options, loadedSpec.SourcePath));
    }

    private static async Task<RunSummary> RunTimeSeriesAsync(DataGenOptions options)
    {
        LoadedDatasetSpec loadedSpec = DatasetSpecLoader.Load(options);
        DatasetGenerationPlan plan = SpecDataGenerator.CreatePlan(options, loadedSpec.Spec);
        return await RunSqlDatasetAsync(options, loadedSpec, plan.SqlSources);
    }

    private static async Task<RunSummary> RunFromDatabaseAsync(DataGenOptions options)
    {
        Console.WriteLine($"Source DB   : {options.SourceDatabasePath}");

        DatasetSpec spec = await SchemaInferredSpecBuilder.BuildFromDatabaseAsync(options.SourceDatabasePath!);

        Console.WriteLine(
            "Tables      : " +
            string.Join(", ", spec.Tables.Select(t => t.Name)));

        var loadedSpec = new LoadedDatasetSpec(options.SourceDatabasePath!, spec);
        DatasetGenerationPlan plan = SpecDataGenerator.CreatePlan(options, loadedSpec.Spec);

        Console.WriteLine(
            "Generating  : " +
            string.Join(
                ", ",
                spec.Tables.Select(table =>
                    $"{table.Name}={GetRequiredTableSource(plan.SqlSources, table.GeneratorKey).RowCount:N0}")));

        return await RunSqlDatasetAsync(options, loadedSpec, plan.SqlSources);
    }

    private static async Task<RunSummary> RunSqlDatasetAsync(
        DataGenOptions options,
        LoadedDatasetSpec loadedSpec,
        IReadOnlyDictionary<string, GeneratedSqlTableSource> sources,
        IReadOnlyDictionary<string, long>? extraCounts = null)
    {
        var files = new List<string>();

        if (options.WriteFiles)
        {
            string schemaPath = Path.Combine(options.OutputPath, "schema.sql");
            await CsvWriter.WriteTextAsync(
                schemaPath,
                SqlSpecBuilder.BuildSchemaScript(loadedSpec.Spec.Tables, includeIndexes: true));
            files.Add(schemaPath);

            foreach (SqlTableSpec table in loadedSpec.Spec.Tables)
            {
                GeneratedSqlTableSource source = GetRequiredTableSource(sources, table.GeneratorKey);
                string outputPath = Path.Combine(options.OutputPath, table.OutputFileName);
                await CsvWriter.WriteRowsAsync(outputPath, table, source.CreateRows());
                files.Add(outputPath);
            }
        }

        string? databasePath = null;
        if (options.DirectLoad)
            databasePath = await BinaryDirectLoader.LoadSqlTablesAsync(options, loadedSpec.Spec.Tables, sources);

        var counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (SqlTableSpec table in loadedSpec.Spec.Tables)
            counts[table.Name] = GetRequiredTableSource(sources, table.GeneratorKey).RowCount;

        if (extraCounts != null)
        {
            foreach ((string key, long value) in extraCounts)
                counts[key] = value;
        }

        return new RunSummary(
            Dataset: options.DatasetLabel,
            Counts: counts,
            Files: files,
            DatabasePath: databasePath,
            Options: CreateSerializableOptions(options, loadedSpec.SourcePath));
    }

    private static GeneratedSqlTableSource GetRequiredTableSource(
        IReadOnlyDictionary<string, GeneratedSqlTableSource> sources,
        string generatorKey)
    {
        if (sources.TryGetValue(generatorKey, out GeneratedSqlTableSource? source))
            return source;

        throw new InvalidOperationException(
            $"No generated table source was registered for generatorKey '{generatorKey}'.");
    }

    private static GeneratedCollectionSource GetRequiredCollectionSource(
        IReadOnlyDictionary<string, GeneratedCollectionSource> sources,
        string generatorKey)
    {
        if (sources.TryGetValue(generatorKey, out GeneratedCollectionSource? source))
            return source;

        throw new InvalidOperationException(
            $"No generated collection source was registered for generatorKey '{generatorKey}'.");
    }

    private static SerializableOptions CreateSerializableOptions(DataGenOptions options, string resolvedSpecPath) => new(
        Seed: options.Seed,
        RowCount: options.RowCount,
        BatchSize: options.BatchSize,
        DirectLoad: options.DirectLoad,
        WriteFiles: options.WriteFiles,
        OverwriteDatabase: options.OverwriteDatabase,
        BuildIndexes: options.BuildIndexes,
        OutputPath: options.OutputPath,
        DatabasePath: options.DatabasePath,
        SpecPath: resolvedSpecPath,
        SourceDatabasePath: options.SourceDatabasePath,
        NullRate: options.NullRate,
        HotKeyRate: options.HotKeyRate,
        RecentRate: options.RecentRate,
        AvgDocSizeBytes: options.AvgDocSizeBytes,
        TenantCount: options.TenantCount,
        DeviceCount: options.DeviceCount,
        OrdersPerCustomer: options.OrdersPerCustomer,
        ItemsPerOrder: options.ItemsPerOrder);

    private static void PrintHelp()
    {
        Console.WriteLine("CSharpDB Data Generator");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational [options]");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- docs [options]");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- timeseries [options]");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- fromdb --source-database <path> [options]");
        Console.WriteLine();
        Console.WriteLine("Common options:");
        Console.WriteLine("  --rows <n>                 Row or document count");
        Console.WriteLine("  --seed <n>                 Deterministic seed (default 42)");
        Console.WriteLine("  --batch-size <n>           Direct-load batch size (default 1000)");
        Console.WriteLine("  --output-path <path>       File output directory");
        Console.WriteLine("  --spec-path <path>         Dataset JSON spec file");
        Console.WriteLine("  --load-direct              Load directly into a CSharpDB database");
        Console.WriteLine("  --database-path <path>     Database path for direct load");
        Console.WriteLine("  --overwrite-db             Replace an existing direct-load database");
        Console.WriteLine("  --build-indexes            Build secondary indexes after direct load");
        Console.WriteLine("  --no-files                 Skip CSV/JSONL file output");
        Console.WriteLine("  --null-rate <0..1>         Sparse/null field rate");
        Console.WriteLine("  --hot-key-rate <0..1>      Fraction of traffic hitting the hot key band");
        Console.WriteLine("  --recent-rate <0..1>       Fraction of rows skewed toward recent timestamps");
        Console.WriteLine();
        Console.WriteLine("Dataset-specific options:");
        Console.WriteLine("  relational: --orders-per-customer <n> --items-per-order <n> --tenant-count <n>");
        Console.WriteLine("  docs      : --avg-size <bytes> --tenant-count <n>");
        Console.WriteLine("  timeseries: --device-count <n>");
        Console.WriteLine("  fromdb    : --source-database <path>  (required)");
        Console.WriteLine();
        Console.WriteLine("Examples:");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 1000000 --seed 42");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- docs --rows 500000 --avg-size 1024 --seed 42");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- timeseries --rows 10000000 --seed 42");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 250000 --load-direct --database-path artifacts/data-gen/relational.db --overwrite-db --build-indexes");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- relational --rows 50000 --spec-path artifacts/data-gen/custom-relational.dataset.json");
        Console.WriteLine("  dotnet run --project tests/CSharpDB.DataGen/CSharpDB.DataGen.csproj -- fromdb --source-database myapp.db --rows 50000 --load-direct --database-path artifacts/data-gen/populated.db --overwrite-db --build-indexes");
    }

    private sealed record RunSummary(
        string Dataset,
        IReadOnlyDictionary<string, long> Counts,
        IReadOnlyList<string> Files,
        string? DatabasePath,
        SerializableOptions Options);

    private sealed record SerializableOptions(
        int Seed,
        long RowCount,
        int BatchSize,
        bool DirectLoad,
        bool WriteFiles,
        bool OverwriteDatabase,
        bool BuildIndexes,
        string OutputPath,
        string? DatabasePath,
        string SpecPath,
        string? SourceDatabasePath,
        double NullRate,
        double HotKeyRate,
        double RecentRate,
        int AvgDocSizeBytes,
        int TenantCount,
        int DeviceCount,
        int OrdersPerCustomer,
        int ItemsPerOrder);
}
