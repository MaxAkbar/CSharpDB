# Storage Study Examples

The storage study examples are now a runnable console project instead of a standalone `.cs` file.

## Project

- [StorageStudyExamples.csproj](./StorageStudyExamples/StorageStudyExamples.csproj)
- [Program.cs](./StorageStudyExamples/Program.cs)
- [StorageStudyExamples.cs](./StorageStudyExamples/StorageStudyExamples.cs)

## Run

List the available demos:

```bash
dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples/StorageStudyExamples.csproj -- list
```

Run a specific example:

```bash
dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples/StorageStudyExamples.csproj -- debug-config
dotnet run --project docs/tutorials/storage/examples/StorageStudyExamples/StorageStudyExamples.csproj -- crash-recovery-test
```

The launcher creates a temp working directory for each run so the examples do not litter the repository root with `.cdb` and `.wal` files.

## Suggested starting points

- `default-config`
- `debug-config`
- `metrics-cache`
- `checkpoint-policy-test`
- `wal-size-policy-test`
