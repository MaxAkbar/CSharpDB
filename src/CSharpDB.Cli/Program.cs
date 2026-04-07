using CSharpDB.Cli;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using Spectre.Console;

CliConsole.ConfigureTerminal();

var stdout = CliConsole.Create(Console.Out, interactive: true);
var stderr = CliConsole.Create(Console.Error);

if (args.Length > 0 && InspectorCommandRunner.IsKnownCommand(args[0]))
    return await InspectorCommandRunner.RunAsync(args, Console.Out, Console.Error);

if (args.Length > 0 && MaintenanceCommandRunner.IsKnownCommand(args[0]))
    return await MaintenanceCommandRunner.RunAsync(args, Console.Out, Console.Error);

if (args.Length > 0 && PipelineCommandRunner.IsKnownCommand(args[0]))
    return await PipelineCommandRunner.RunAsync(args, Console.Out, Console.Error);

if (!CliShellOptions.TryParse(args, out var shellOptions, out var parseError))
{
    CliConsole.WriteError(stderr, parseError ?? "Invalid command line.");
    stderr.MarkupLine($"[yellow]{CliConsole.Escape(CliShellOptions.Usage)}[/]");
    return 1;
}

string displayTarget = shellOptions!.DisplayTarget;

CliConsole.WriteBanner(stdout, displayTarget);

ICSharpDbClient client;
Database? localDatabase = null;
try
{
    client = CSharpDbClient.Create(shellOptions.ClientOptions);

    if (shellOptions.EnableLocalDirectFeatures && client is IEngineBackedClient engineBacked)
        localDatabase = await engineBacked.TryGetDatabaseAsync();
}
catch (Exception ex)
{
    CliConsole.WriteError(stderr, $"Fatal: Could not open database: {ex.Message}");
    return 1;
}

await using (client)
{
    // Build the command registry.
    var commands = new List<IMetaCommand>();
    var help = new HelpCommand(commands);
    commands.Add(help);
    commands.Add(new InfoCommand());
    commands.Add(new TablesCommand());
    commands.Add(new SchemaCommand());
    commands.Add(new IndexesCommand());
    commands.Add(new ViewsCommand());
    commands.Add(new ViewCommand());
    commands.Add(new TriggersCommand());
    commands.Add(new TriggerCommand());
    commands.Add(new CollectionsCommand());
    commands.Add(new BeginTransactionCommand());
    commands.Add(new CommitCommand());
    commands.Add(new RollbackCommand());
    commands.Add(new CheckpointCommand());
    commands.Add(new BackupCommand());
    commands.Add(new RestoreCommand());
    commands.Add(new MigrateForeignKeysCommand());
    commands.Add(new ReindexCommand());
    commands.Add(new VacuumCommand());
    commands.Add(new SnapshotCommand());
    commands.Add(new SyncPointCommand());
    commands.Add(new TimingCommand());
    commands.Add(new ReadCommand());

    using var repl = new Repl(client, localDatabase, displayTarget, Console.Out, commands);
    await repl.RunAsync();
}

return 0;
