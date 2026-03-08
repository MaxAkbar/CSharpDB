namespace StorageStudyExamples.Core;

/// <summary>
/// Base interface for all storage study examples.
/// Provides metadata and a scripted demo that showcases the example's features.
/// </summary>
public interface IExample : IAsyncDisposable
{
    /// <summary>Display name shown in the REPL (e.g. "Virtual Drive").</summary>
    string Name { get; }

    /// <summary>CLI command name used to load the example (e.g. "virtual-drive").</summary>
    string CommandName { get; }

    /// <summary>Short description of what this example demonstrates.</summary>
    string Description { get; }

    /// <summary>
    /// Initialize the example in the given working directory.
    /// For data store examples this creates the schema and seeds sample data.
    /// </summary>
    Task InitializeAsync(string workingDirectory);

    /// <summary>
    /// Run the scripted demo that exercises all features of this example.
    /// Output is written to <paramref name="output"/> so the REPL can capture or redirect it.
    /// </summary>
    Task RunDemoAsync(TextWriter output);
}
