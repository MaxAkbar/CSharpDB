using CSharpDB.Cli;
using CSharpDB.Engine;

Ansi.EnableVirtualTerminal();

if (args.Length > 0 && InspectorCommandRunner.IsKnownCommand(args[0]))
    return await InspectorCommandRunner.RunAsync(args, Console.Out, Console.Error);

string dbPath = args.Length > 0 ? args[0] : "csharpdb.db";

Console.WriteLine($"{Ansi.Bold}{Ansi.Cyan}CSharpDB{Ansi.Reset} - Interactive SQL Shell");
Console.WriteLine($"{Ansi.Dim}Database: {dbPath}{Ansi.Reset}");
Console.WriteLine($"{Ansi.Dim}Type .help for commands, .quit to exit.{Ansi.Reset}");
Console.WriteLine();

Database db;
try
{
    db = await Database.OpenAsync(dbPath);
}
catch (Exception ex)
{
    Console.Error.WriteLine(Ansi.Colorize($"Fatal: Could not open database: {ex.Message}", Ansi.Red));
    return 1;
}

await using (db)
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
    commands.Add(new SnapshotCommand());
    commands.Add(new SyncPointCommand());
    commands.Add(new TimingCommand());
    commands.Add(new ReadCommand());

    using var repl = new Repl(db, dbPath, Console.Out, commands);
    await repl.RunAsync();
}

return 0;
