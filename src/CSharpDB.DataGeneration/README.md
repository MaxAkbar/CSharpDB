# CSharpDB.DataGeneration

Repeatable test-data generation and relationship-aware schema planning for the
[CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.DataGeneration` provides the shared generation engine used by CSharpDB
Studio and the developer DataGen CLI. It creates typed synthetic rows from
versioned profiles, validates constraints and relationships, and supports
repeatable previews with a fixed seed and reference date.

The library accepts schema/key snapshots from its caller. Database connections,
transaction execution, Studio UI and file output belong to the consuming tools.
The full workflow, configuration and support matrix are documented in the
[Test Data Generator guide](https://csharpdb.com/docs/test-data-generator.html).

## Features

- Realistic person, contact, address, company, product and text generators.
- Typed numbers, decimals, booleans, dates/times, UUIDs, binary values, sequences,
  constants and value lists.
- Uniform, weighted, bounded normal, recent-date and hot-key distributions.
- Generated and existing parent-key sources, ordered composite mappings,
  overlapping relationships and one-to-one capacity validation.
- Full-run validation of supported types, constraints, keys and configured limits.
- Stable random streams, versioned JSON profiles and repeatable preview pages.
- Shared specification evaluator for the CLI's SQL and document datasets.

## Key Types

| Type | Description |
|------|-------------|
| `GenerationProfile` | Versioned table, field and relationship settings with JSON serialization and a profile hash |
| `GenerationTableSnapshot` | Caller-supplied schema, indexes, row counts, existing keys, triggers and fingerprint |
| `GeneratorCatalog` | Compatible providers, suggested rules, validation and field generation |
| `GenerationPlan` | Resolved dependency order, key allocation, full validation, row generation and referenced-key lookup |
| `GenerationLimits` | Row, value-size, key-size, statement, preview and duration settings |
| `GenerationValues` | Engine-compatible typed assignment, collation-aware keys and SQL value formatting |
| `StableRandom` | Versioned deterministic seed derivation and independent random streams |
| `SpecDataGenerator` | Lazy SQL/document generation from the developer dataset specification format |

The profile/planning API uses `CSharpDB.DataGeneration`. Extracted CLI types retain
`CSharpDB.DataGen`, `CSharpDB.DataGen.Generators` and `CSharpDB.DataGen.Specs`.

## Usage

For a table without parent relationships, use a complete snapshot from the caller's
schema adapter to suggest rules and prepare a preview:

```csharp
using CSharpDB.DataGeneration;

static GenerationPlan PreparePreview(GenerationTableSnapshot snapshot)
{
    var table = GenerationPlan.Suggest(snapshot);
    table.Rows = 100;

    var profile = new GenerationProfile
    {
        Seed = 42,
        ReferenceUtc = new DateTime(2026, 9, 11, 0, 0, 0, DateTimeKind.Utc),
        Tables = [table],
    };

    // Build validates the full requested run.
    return GenerationPlan.Build(profile, [snapshot]);
}

// Read a page from the returned plan:
// plan.Rows(tableName, offset: 0, count: 25)
```

For related tables, include snapshots for each referenced parent and configure
its generated or existing key source. Snapshots must reflect the actual schema,
identity state and existing keys; the adapter revalidates them before an append.

Repeatability depends on the profile, seed, UTC reference date, algorithm/provider
version, schema identities and existing key state. Generated cycles,
INSERT-trigger tables, rowversion keys and unsupported types/expressions are
rejected. See the [support matrix](https://csharpdb.com/docs/test-data-generator.html#scope)
and [repeatability contract](https://csharpdb.com/docs/test-data-generator.html#preview).

## Package Status

This project currently sets `IsPackable=false`. It is consumed through project
references and is not included in the NuGet release list or the `CSharpDB`
umbrella package.

The repository's shared package configuration already designates each project's
`README.md` as its NuGet README. This file follows the library README format;
NuGet distribution still requires enabling packaging and including the project
in the release package graph. There is no published-package installation command
for this project in the current configuration.

## Dependencies

- `CSharpDB.Primitives` — values, SQL type descriptors and schema metadata.
- `CSharpDB.Sql` — parsing and expression models.
- `CSharpDB.Execution` — database assignment, expression and collation semantics.
- `Bogus` 35.6.5 — realistic field providers.

## Build and Test

From the repository root:

```powershell
dotnet build src/CSharpDB.DataGeneration/CSharpDB.DataGeneration.csproj
dotnet test tests/CSharpDB.DataGeneration.Tests/CSharpDB.DataGeneration.Tests.csproj
```

## Related Projects

| Project | Description |
|---------|-------------|
| [CSharpDB.Admin](https://github.com/MaxAkbar/CSharpDB/tree/main/src/CSharpDB.Admin) | Studio workflow, consistent snapshots and transactional generation |
| [CSharpDB.DataGen](https://github.com/MaxAkbar/CSharpDB/tree/main/tests/CSharpDB.DataGen) | Developer dataset specifications, schema inference, file output and direct loading |
| [CSharpDB.Client](https://github.com/MaxAkbar/CSharpDB/tree/main/src/CSharpDB.Client) | Connected database metadata and transaction APIs used by Studio |

## License

MIT — see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
