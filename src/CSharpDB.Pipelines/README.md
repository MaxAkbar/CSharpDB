# CSharpDB.Pipelines

Package contracts and runtime foundation for CSharpDB ETL pipelines.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Pipelines)](https://www.nuget.org/packages/CSharpDB.Pipelines)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Pipelines` defines portable pipeline packages and a small orchestration
runtime for batch ETL work. A pipeline package describes the source,
transformations, destination, execution options, and optional incremental state.
The built-in runtime can validate packages, serialize them to JSON, execute them
in batches, capture checkpoints, and report rejects and run metrics.
Packages can also name trusted host commands for lifecycle hooks; command bodies
are registered by the process that runs the pipeline and are not serialized into
the package. Package JSON includes generated automation metadata that lists the
trusted commands and scalar functions a host must register.

Current boundary:
- Built-in runtime components currently support CSV and JSON file sources/destinations
- Built-in transforms support `Select`, `Rename`, `Cast`, `Filter`, `Derive`, and `Deduplicate`
- `CSharpDB` table sources/destinations and SQL query sources are modeled in the contracts but are not implemented by `DefaultPipelineComponentFactory` yet

## Features

- **Pipeline package model**: strongly typed source, transform, destination, and execution settings
- **Validation**: schema-level validation before execution
- **Serialization**: save/load pipeline packages as JSON
- **Runtime orchestration**: `Validate`, `DryRun`, `Run`, and `Resume` modes
- **Built-in connectors**: CSV and JSON file readers/writers
- **Built-in transforms**: select, rename, cast, filter, derive, deduplicate
- **Checkpointing hooks**: pluggable checkpoint store and run logger abstractions
- **Trusted command hooks**: host-registered commands for run started, batch completed, run succeeded, and run failed events
- **Automation metadata**: generated import/export manifest for trusted command and scalar function names
- **Batch metrics**: rows read/written/rejected plus batch counts

## Usage

### End-to-End Example

```csharp
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Runtime.BuiltIns;
using CSharpDB.Pipelines.Serialization;
using CSharpDB.Pipelines.Validation;
using CSharpDB.Primitives;

Directory.CreateDirectory("data");

await File.WriteAllLinesAsync("data/customers.csv",
[
    "id,name,status",
    "1,Alice,active",
    "1,Alice,active",
    "2,Bob,inactive",
    "3,Carol,active",
]);

var package = new PipelinePackageDefinition
{
    Name = "customers-csv-to-json",
    Version = "1.0.0",
    Description = "Import customers from CSV, clean them, and emit JSON.",
    Source = new PipelineSourceDefinition
    {
        Kind = PipelineSourceKind.CsvFile,
        Path = "data/customers.csv",
        HasHeaderRow = true,
    },
    Transforms =
    [
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Select,
            SelectColumns = ["id", "name", "status"],
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Rename,
            RenameMappings =
            [
                new PipelineRenameMapping { Source = "name", Target = "full_name" },
            ],
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Cast,
            CastMappings =
            [
                new PipelineCastMapping { Column = "id", TargetType = DbType.Integer },
            ],
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Filter,
            FilterExpression = "status == 'active'",
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Derive,
            DerivedColumns =
            [
                new PipelineDerivedColumn { Name = "import_source", Expression = "'csv'" },
            ],
        },
        new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Deduplicate,
            DeduplicateKeys = ["id"],
        },
    ],
    Destination = new PipelineDestinationDefinition
    {
        Kind = PipelineDestinationKind.JsonFile,
        Path = "data/customers.cleaned.json",
    },
    Options = new PipelineExecutionOptions
    {
        BatchSize = 2,
        CheckpointInterval = 1,
        ErrorMode = PipelineErrorMode.FailFast,
    },
    Hooks =
    [
        new PipelineCommandHookDefinition
        {
            Event = PipelineCommandHookEvent.OnRunSucceeded,
            CommandName = "NotifyPipeline",
            Arguments = new Dictionary<string, object?>
            {
                ["channel"] = "ops",
            },
        },
    ],
};

PipelineValidationResult validation = PipelinePackageValidator.Validate(package);
if (!validation.IsValid)
{
    throw new InvalidOperationException(string.Join(
        Environment.NewLine,
        validation.Errors.Select(error => $"{error.Path}: {error.Message}")));
}

await PipelinePackageSerializer.SaveToFileAsync(package, "data/customers.pipeline.json");
PipelinePackageDefinition loadedPackage =
    await PipelinePackageSerializer.LoadFromFileAsync("data/customers.pipeline.json");

var orchestrator = new PipelineOrchestrator(
    new DefaultPipelineComponentFactory(),
    new NullPipelineCheckpointStore(),
    new NullPipelineRunLogger(),
    DbCommandRegistry.Create(commands =>
    {
        commands.AddCommand("NotifyPipeline", static context =>
        {
            string pipelineName = context.Metadata["pipelineName"];
            long rowsWritten = context.Arguments["rowsWritten"].AsInteger;
            Console.WriteLine($"{pipelineName}: {rowsWritten} row(s) written.");
            return DbCommandResult.Success();
        });
    }));

PipelineRunResult result = await orchestrator.ExecuteAsync(new PipelineRunRequest
{
    Package = loadedPackage,
    Mode = PipelineExecutionMode.Run,
});

Console.WriteLine(
    $"{result.Status}: {result.Metrics.RowsRead} read, " +
    $"{result.Metrics.RowsWritten} written, " +
    $"{result.Metrics.RowsRejected} rejected");

string outputJson = await File.ReadAllTextAsync("data/customers.cleaned.json");
Console.WriteLine(outputJson);
```

The output file contains the active customers only, with duplicate IDs removed:

```json
[
  {
    "id": 1,
    "status": "active",
    "full_name": "Alice",
    "import_source": "csv"
  },
  {
    "id": 3,
    "status": "active",
    "full_name": "Carol",
    "import_source": "csv"
  }
]
```

### Execution Modes

- `Validate`: validate the package only, without creating components
- `DryRun`: open the source and run transforms, but skip destination writes
- `Run`: execute the full pipeline and persist checkpoints
- `Resume`: continue a previous run from the checkpoint returned by your `IPipelineCheckpointStore`

### Notes

- `DefaultPipelineComponentFactory` is the ready-to-run built-in factory for file pipelines
- Use `NullPipelineCheckpointStore` and `NullPipelineRunLogger` when you want a minimal in-process setup
- Relative source file paths are searched from the current directory and app base directory; relative output paths are written relative to the current directory
- `Derive` expressions are intentionally simple today: use a source column name or a literal such as `'csv'`, `123`, `true`, or `null`
- Trusted command hooks are skipped in `Validate` mode. Missing command registration or a failing hook with `StopOnFailure = true` fails the run through `PipelineRunResult`.
- `PipelinePackageSerializer` regenerates `Automation` during save/load and string serialization. Validation accepts legacy packages without automation metadata, but reports stale manifests when present.

## Installation

```
dotnet add package CSharpDB.Pipelines
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Primitives` - shared type system used by cast mappings and pipeline contracts

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB](https://www.nuget.org/packages/CSharpDB) | Recommended all-in-one package including pipelines, engine, storage, and client APIs |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine for SQL and collection access |
| [CSharpDB.Client](https://www.nuget.org/packages/CSharpDB.Client) | Client SDK for database, pipeline, and maintenance operations |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
