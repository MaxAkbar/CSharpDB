using StorageStudyExamples.Core;

namespace StorageStudyExamples.Repl;

/// <summary>
/// Interactive REPL host with two states: main menu (example selection)
/// and example mode (domain-specific command dispatch).
/// </summary>
public sealed class ReplHost
{
    private readonly IReadOnlyList<IExample> _examples;
    private IExample? _current;
    private string? _workingDirectory;

    public ReplHost(IReadOnlyList<IExample> examples)
    {
        _examples = examples;
    }

    public async Task RunAsync()
    {
        PrintWelcome();

        while (true)
        {
            Console.Write(GetPrompt());
            var input = Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(input)) continue;

            try
            {
                var shouldExit = await ProcessCommandAsync(input);
                if (shouldExit) break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine();
        }

        if (_current != null)
            await _current.DisposeAsync();
    }

    private string GetPrompt() => _current != null
        ? $"{_current.CommandName}> "
        : "> ";

    /// <returns><c>true</c> if the REPL should exit.</returns>
    private async Task<bool> ProcessCommandAsync(string input)
    {
        var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
        var command = parts[0].ToLowerInvariant();
        var args = parts.Length > 1 ? parts[1].Trim() : "";

        // ── Global commands (work in any state) ─────────────────────────
        switch (command)
        {
            case "clear":
                Console.Clear();
                return false;

            case "quit":
            case "exit":
                Console.WriteLine("Goodbye!");
                return true;
        }

        // ── Example mode ────────────────────────────────────────────────
        if (_current != null)
            return await ProcessExampleCommandAsync(command, args);

        // ── Main menu mode ──────────────────────────────────────────────
        return await ProcessMainMenuCommandAsync(command, args);
    }

    // ── Main menu ───────────────────────────────────────────────────────

    private async Task<bool> ProcessMainMenuCommandAsync(string command, string args)
    {
        switch (command)
        {
            case "list":
                PrintExampleList();
                break;

            case "load":
                await LoadExampleAsync(args);
                break;

            case "help":
                PrintMainMenuHelp();
                break;

            default:
                Console.WriteLine($"Unknown command: {command}. Type 'help' or 'list' to see options.");
                break;
        }

        return false;
    }

    // ── Example mode ────────────────────────────────────────────────────

    private async Task<bool> ProcessExampleCommandAsync(string command, string args)
    {
        switch (command)
        {
            case "back":
                await UnloadExampleAsync();
                PrintExampleList();
                return false;

            case "demo":
                await RunDemoAsync();
                return false;

            case "help":
                PrintExampleHelp();
                return false;

            case "sql":
                await ExecuteSqlAsync(args);
                return false;

            case "load":
                Console.WriteLine("Type 'back' to return to the main menu first.");
                return false;
        }

        // Delegate to the example's domain-specific commands
        if (_current is IInteractiveExample interactive)
        {
            var handled = await interactive.ExecuteCommandAsync(command, args, Console.Out);
            if (!handled)
                Console.WriteLine($"Unknown command: {command}. Type 'help' for available commands.");
        }
        else
        {
            // Demo-only example — no domain commands
            Console.WriteLine($"Unknown command: {command}. This example only supports 'demo' and 'back'.");
        }

        return false;
    }

    // ── Load / Unload ───────────────────────────────────────────────────

    private async Task LoadExampleAsync(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            Console.WriteLine("Usage: load <example-name>");
            return;
        }

        var example = _examples.FirstOrDefault(e =>
            string.Equals(e.CommandName, name, StringComparison.OrdinalIgnoreCase));

        if (example == null)
        {
            Console.WriteLine($"Unknown example: {name}");
            Console.WriteLine("Type 'list' to see available examples.");
            return;
        }

        // Unload current if any
        if (_current != null)
            await _current.DisposeAsync();

        _workingDirectory = CreateWorkingDirectory(example.CommandName);
        Directory.CreateDirectory(_workingDirectory);

        await example.InitializeAsync(_workingDirectory);
        _current = example;

        PrintExampleBanner();
    }

    private async Task UnloadExampleAsync()
    {
        if (_current == null) return;

        await _current.DisposeAsync();
        _current = null;
        _workingDirectory = null;
    }

    // ── Demo ────────────────────────────────────────────────────────────

    private async Task RunDemoAsync()
    {
        if (_current == null) return;

        // For demo-only examples, re-initialize with fresh directory
        if (_current is not IInteractiveExample)
        {
            _workingDirectory = CreateWorkingDirectory(_current.CommandName);
            Directory.CreateDirectory(_workingDirectory);
            await _current.DisposeAsync();
            await _current.InitializeAsync(_workingDirectory);
        }

        await _current.RunDemoAsync(Console.Out);
    }

    // ── Raw SQL ─────────────────────────────────────────────────────────

    private async Task ExecuteSqlAsync(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            Console.WriteLine("Usage: sql <query>");
            return;
        }

        if (_current is not DataStoreBase storeBase)
        {
            Console.WriteLine("Raw SQL is not available for this example.");
            return;
        }

        try
        {
            await storeBase.ExecuteRawSqlAsync(sql, Console.Out);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SQL error: {ex.Message}");
        }
    }

    // ── Output formatting ───────────────────────────────────────────────

    private void PrintWelcome()
    {
        Console.WriteLine("===========================================================");
        Console.WriteLine("  CSharpDB Storage Study Examples");
        Console.WriteLine("===========================================================");
        Console.WriteLine();
        PrintExampleList();
    }

    private void PrintExampleList()
    {
        var interactive = _examples.Where(e => e is IInteractiveExample).ToList();
        var demoOnly = _examples.Where(e => e is not IInteractiveExample).ToList();

        if (interactive.Count > 0)
        {
            Console.WriteLine("  Application Examples:");
            foreach (var ex in interactive)
                Console.WriteLine($"    {ex.CommandName,-24} {ex.Description}");
            Console.WriteLine();
        }

        if (demoOnly.Count > 0)
        {
            Console.WriteLine("  Storage Internals (demo only):");
            foreach (var ex in demoOnly)
                Console.WriteLine($"    {ex.CommandName,-24} {ex.Description}");
            Console.WriteLine();
        }

        Console.WriteLine("  Type 'load <name>' to start an example.");
    }

    private void PrintExampleBanner()
    {
        if (_current == null) return;

        Console.WriteLine();
        Console.WriteLine($"  Loaded: {_current.Name}");
        Console.WriteLine($"  {_current.Description}");
        Console.WriteLine();

        if (_current is IInteractiveExample interactive)
        {
            Console.WriteLine("  Commands:");
            var commands = interactive.GetCommands();
            foreach (var cmd in commands)
                Console.WriteLine($"    {cmd.Usage,-32} {cmd.Description}");

            Console.WriteLine();
            Console.WriteLine($"    {"demo",-32} Run the scripted demo");
            Console.WriteLine($"    {"sql <query>",-32} Execute raw SQL");
            Console.WriteLine($"    {"back",-32} Return to main menu");
            Console.WriteLine($"    {"help",-32} Show this help");
        }
        else
        {
            Console.WriteLine("  This is a demo-only example.");
            Console.WriteLine("  Type 'demo' to run it, or 'back' to return to the main menu.");
        }
    }

    private void PrintExampleHelp()
    {
        PrintExampleBanner();
    }

    private static void PrintMainMenuHelp()
    {
        Console.WriteLine("  Commands:");
        Console.WriteLine($"    {"load <name>",-32} Load an example");
        Console.WriteLine($"    {"list",-32} Show available examples");
        Console.WriteLine($"    {"clear",-32} Clear the screen");
        Console.WriteLine($"    {"quit",-32} Exit the REPL");
    }

    // ── Utility ─────────────────────────────────────────────────────────

    private static string CreateWorkingDirectory(string commandName)
    {
        var safeCommandName = string.Concat(commandName.Select(ch =>
            Path.GetInvalidFileNameChars().Contains(ch) ? '_' : ch));

        var folderName = $"{safeCommandName}_{DateTime.UtcNow:yyyyMMdd_HHmmss}";
        return Path.Combine(Path.GetTempPath(), "CSharpDB", "StorageStudyExamples", folderName);
    }
}
