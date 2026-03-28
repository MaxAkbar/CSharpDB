# StorageStudyExamples.Core

Shared interfaces and base classes for all storage study examples.

## Contents

| File | Description |
|------|-------------|
| `IExample.cs` | Base interface for all examples — metadata (`Name`, `CommandName`, `Description`) and lifecycle (`InitializeAsync`, `RunDemoAsync`) |
| `IInteractiveExample.cs` | Extended interface for examples with domain-specific commands (`GetCommands`, `ExecuteCommandAsync`) |
| `DataStoreBase.cs` | Abstract base class that provides CSharpDB `Database` lifecycle, schema/seed hooks, raw SQL helper, and common utilities (`Esc`, `Truncate`) |

## Interface Hierarchy

```
IAsyncDisposable
  └── IExample
        ├── Name, CommandName, Description
        ├── InitializeAsync(workingDirectory)
        └── RunDemoAsync(output)
              └── IInteractiveExample
                    ├── GetCommands() → IReadOnlyList<CommandInfo>
                    └── ExecuteCommandAsync(command, args, output)
```

Application-pattern examples (virtual-drive, config-store, event-log, task-queue, graph-store) implement `IInteractiveExample` via `DataStoreBase`. Storage-internals examples implement only `IExample`.

See the [root README](../README.md) for full documentation.
