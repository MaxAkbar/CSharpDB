# Contributing to CSharpDB

Thanks for your interest in contributing.

## Ways to Contribute

- Report bugs
- Propose features and improvements
- Improve documentation
- Submit code changes and tests

## Before You Start

1. Search existing issues and pull requests to avoid duplicates.
2. If the change is large, open an issue first to align on scope.

## Development Setup

Prerequisite: .NET 10 SDK

```bash
dotnet --version
dotnet restore CSharpDB.slnx
dotnet build CSharpDB.slnx
```

## Running Tests

Build validation:

```bash
dotnet build CSharpDB.slnx
```

Run test executables:

```bash
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj --
dotnet run --project tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj --
dotnet run --project tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --
```

## Pull Request Guidelines

1. Keep PRs focused and reasonably small.
2. Add or update tests for behavior changes.
3. Update docs when user-facing behavior changes.
4. Ensure the solution builds cleanly before opening the PR.
5. Describe:
   - what changed
   - why it changed
   - how it was tested

## Coding Guidelines

- Follow existing code style and naming conventions.
- Prefer clear, maintainable changes over clever shortcuts.
- Avoid adding dependencies unless clearly justified.
- Preserve backward compatibility unless the PR explicitly documents a breaking change.

## Commit Messages

Use clear, imperative messages, for example:

- `Fix query parser for dotted table names`
- `Add system catalog tests for sys.tables`
- `Update admin README screenshots`

## Questions

If anything is unclear, open an issue or discussion and ask before implementing large changes.
