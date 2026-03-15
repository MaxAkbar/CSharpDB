using CSharpDB.Cli;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;

Ansi.EnableVirtualTerminal();

if (args.Length > 0 && InspectorCommandRunner.IsKnownCommand(args[0]))
    return await InspectorCommandRunner.RunAsync(args, Console.Out, Console.Error);

if (args.Length > 0 && MaintenanceCommandRunner.IsKnownCommand(args[0]))
    return await MaintenanceCommandRunner.RunAsync(args, Console.Out, Console.Error);

if (!CliShellOptions.TryParse(args, out var shellOptions, out var parseError))
{
    Console.Error.WriteLine(Ansi.Colorize($"Error: {parseError}", Ansi.Red));
    Console.Error.WriteLine(Ansi.Colorize(CliShellOptions.Usage, Ansi.Yellow));
    return 1;
}

string displayTarget = shellOptions!.DisplayTarget;

Console.WriteLine($"{Ansi.Bold}{Ansi.Cyan}CSharpDB{Ansi.Reset} - Interactive SQL Shell");
Console.WriteLine($"{Ansi.Dim}Database: {displayTarget}{Ansi.Reset}");
Console.WriteLine($"{Ansi.Dim}Type .help for commands, .quit to exit.{Ansi.Reset}");
Console.WriteLine();

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
    Console.Error.WriteLine(Ansi.Colorize($"Fatal: Could not open database: {ex.Message}", Ansi.Red));
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
