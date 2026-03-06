# CSharpDB.Core (Compatibility Package)

Compatibility package for existing projects that still reference `CSharpDB.Core`.

## Status

`CSharpDB.Core` is now a compatibility package that forwards to `CSharpDB.Primitives`.

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
