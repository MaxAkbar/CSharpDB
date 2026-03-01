namespace CSharpDB.Cli;

/// <summary>
/// A dot-command that the REPL can execute (e.g. .help, .tables).
/// </summary>
internal interface IMetaCommand
{
    IReadOnlyList<string> Aliases { get; }
    string Name { get; }
    string Description { get; }
    ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default);
}
