# CSharpDB.Core (Compatibility Package)

Compatibility package for existing projects that still reference `CSharpDB.Core`.

## Status

`CSharpDB.Core` is now a compatibility package that forwards to `CSharpDB.Primitives`.

- New development should use `CSharpDB.Primitives`.
- Existing `v1.x` consumers can keep using this compatibility package during migration.
- Planned removal target: `v2.0.0`

- For application development, install:
  - `CSharpDB`
- For low-level/shared type usage, install:
  - `CSharpDB.Primitives`

## Installation

```bash
dotnet add package CSharpDB.Core
```

## Migration

Replace:

```bash
dotnet add package CSharpDB.Core
```

With:

```bash
dotnet add package CSharpDB.Primitives
```

## Removal Plan

The `CSharpDB.Core` compatibility package is planned for removal in `v2.0.0`.

Before `v2.0.0`, consumers should:

- replace `CSharpDB.Core` package references with `CSharpDB.Primitives`
- validate any package-management scripts, templates, or docs that still mention `CSharpDB.Core`
