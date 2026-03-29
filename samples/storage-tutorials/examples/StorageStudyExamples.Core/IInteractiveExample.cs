namespace StorageStudyExamples.Core;

/// <summary>
/// Metadata for a single command available within an interactive example.
/// </summary>
public sealed record CommandInfo(string Name, string Usage, string Description);

/// <summary>
/// Extended interface for examples that support interactive domain-specific commands.
/// The REPL delegates all command dispatch to the example itself.
/// </summary>
public interface IInteractiveExample : IExample
{
    /// <summary>Get the list of domain-specific commands this example supports.</summary>
    IReadOnlyList<CommandInfo> GetCommands();

    /// <summary>
    /// Execute a domain-specific command.
    /// </summary>
    /// <returns><c>true</c> if the command was recognized, <c>false</c> otherwise.</returns>
    Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output);
}
