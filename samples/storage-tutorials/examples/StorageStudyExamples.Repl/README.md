# StorageStudyExamples.Repl

Interactive REPL host for all storage study examples.

## Run

```bash
dotnet run --project samples/storage-tutorials/examples/StorageStudyExamples.Repl/StorageStudyExamples.Repl.csproj
```

## Two-State Flow

**Main menu** — lists available examples grouped by type. Type `load <name>` to enter an example.

```
> list                     List all available examples
> load <name>              Load an example
> help                     Show help
> clear                    Clear the screen
> quit                     Exit the REPL
```

**Example mode** — once loaded, the example's domain-specific commands are active. Type `back` to return.

```
example> demo              Run the scripted demo
example> sql <query>       Execute raw SQL
example> back              Return to main menu
example> help              Show commands for this example
example> clear             Clear the screen
example> quit              Exit the REPL
```

## Contents

| File | Description |
|------|-------------|
| `Program.cs` | Entry point — registers all examples and launches the REPL |
| `ReplHost.cs` | Command dispatch with main menu / example mode state machine |

## Working Directories

The REPL creates a temp directory under `%TEMP%/CSharpDB/StorageStudyExamples/` for each loaded example so no `.cdb` or `.wal` files are left in the repository.

See the [root README](../README.md) for full documentation.
