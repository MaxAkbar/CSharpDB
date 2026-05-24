# CSharpDB.DevOps.Tests

Focused unit tests for the `CSharpDB.DevOps` compare, drift, and script
rendering services.

This project verifies the shared DevOps layer directly, before the same
services are exercised through the CLI and Admin integration tests.

## What It Covers

- Schema comparison across tables, columns, indexes, views, triggers, foreign
  keys, and procedure metadata
- Table archive schema loading through `TableArchiveSchemaCompareTarget`
- Data comparison through `ClientDataCompareTarget` and archive-backed targets
- Key-column resolution, including primary-key fallback behavior
- Data sync preview script rendering
- Drift report summary generation
- Whole-database schema script-out
- Selected-object schema script-out for tables and related objects
- SQL identifier and literal escaping behavior that matters to generated scripts

## Test Shape

The tests use small in-memory or temporary-file fixtures. Temporary `.csdbtable`
archives are written through the production table archive writer so the archive
targets are tested against real archive files rather than mocked input.

The project intentionally keeps assertions close to the generated report
contracts. Admin and CLI tests then cover their own mapping, command parsing,
and UI/service behavior on top of these contracts.

## Running The Tests

Run this project by itself:

```powershell
dotnet test .\tests\CSharpDB.DevOps.Tests\CSharpDB.DevOps.Tests.csproj -m:1
```

Run the full repository suite serially:

```powershell
dotnet test .\CSharpDB.slnx -m:1
```

## Maintenance Notes

- Add tests here when changing `SchemaDiffReport`, `DataDiffReport`, or
  `DriftReport` semantics.
- Add renderer tests here when changing generated SQL shape.
- Keep result schemas stable unless the CLI and Admin consumers are updated in
  the same change.
- Prefer realistic table/archive fixtures over broad mocks so target loading
  behavior stays covered.
