namespace StorageStudyExamples;

internal static class Program
{
    private sealed record ExampleCommand(
        string Name,
        string Description,
        Func<Task> RunAsync);

    private static readonly IReadOnlyList<ExampleCommand> Commands =
    [
        new("default-config", "Open the database with default storage settings.", ConfigurationExamples.DefaultConfigurationAsync),
        new("production-config", "Open the database with bounded cache, caching indexes, and custom checksums.", ConfigurationExamples.ProductionConfigurationAsync),
        new("debug-config", "Run a verbose example with interceptor logging.", ConfigurationExamples.DebugConfigurationAsync),
        new("batch-import", "Run the heavier bulk-import example with auto-checkpoint disabled.", ConfigurationExamples.BatchImportConfigurationAsync),
        new("metrics-cache", "Run a cache-instrumented workload and print cache stats.", ConfigurationExamples.MetricsCacheConfigurationAsync),
        new("multiple-interceptors", "Run an example with more than one page interceptor.", ConfigurationExamples.MultipleInterceptorsAsync),
        new("crash-recovery-test", "Simulate a write failure and reopen the database.", TestingExamples.CrashRecoveryTestAsync),
        new("checkpoint-policy-test", "Print deterministic output for the time-based checkpoint policy.", () =>
        {
            TestingExamples.CheckpointPolicyUnitTest();
            return Task.CompletedTask;
        }),
        new("wal-size-policy-test", "Print deterministic output for the WAL-size checkpoint policy.", () =>
        {
            TestingExamples.WalSizePolicyTest();
            return Task.CompletedTask;
        })
    ];

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || IsHelpCommand(args[0]))
        {
            PrintUsage();
            return 0;
        }

        string commandName = args[0];
        if (string.Equals(commandName, "list", StringComparison.OrdinalIgnoreCase))
        {
            PrintUsage();
            return 0;
        }

        ExampleCommand? command = Commands.FirstOrDefault(x =>
            string.Equals(x.Name, commandName, StringComparison.OrdinalIgnoreCase));

        if (command is null)
        {
            Console.Error.WriteLine($"Unknown example '{commandName}'.");
            Console.Error.WriteLine();
            PrintUsage();
            return 1;
        }

        string runDirectory = CreateRunDirectory(command.Name);
        Directory.CreateDirectory(runDirectory);
        Environment.CurrentDirectory = runDirectory;

        Console.WriteLine($"Running: {command.Name}");
        Console.WriteLine(command.Description);
        Console.WriteLine($"Working directory: {runDirectory}");
        Console.WriteLine();

        await command.RunAsync();

        Console.WriteLine();
        Console.WriteLine("Completed.");
        return 0;
    }

    private static bool IsHelpCommand(string arg) =>
        string.Equals(arg, "help", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase);

    private static void PrintUsage()
    {
        Console.WriteLine("CSharpDB.Storage study examples");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples/StorageStudyExamples.csproj -- list");
        Console.WriteLine("  dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples/StorageStudyExamples.csproj -- <example-name>");
        Console.WriteLine();
        Console.WriteLine("Examples:");

        foreach (ExampleCommand command in Commands.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase))
            Console.WriteLine($"  {command.Name,-22} {command.Description}");
    }

    private static string CreateRunDirectory(string commandName)
    {
        string safeCommandName = string.Concat(commandName.Select(ch =>
            Path.GetInvalidFileNameChars().Contains(ch) ? '_' : ch));

        string folderName = $"{safeCommandName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";
        return Path.Combine(Path.GetTempPath(), "CSharpDB", "StorageStudyExamples", folderName);
    }
}
