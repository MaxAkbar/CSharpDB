# `CSharpDB.Core` to `CSharpDB.Primitives`

`v2.0` makes the primitives rename permanent. The old `CSharpDB.Core` project, assembly, and namespace identities are removed from the active repo surface. There is no compatibility shim, no forwarding assembly, and no dual-namespace support.

## Source Migration

Update namespace imports:

```csharp
// Before
using CSharpDB.Core;

// After
using CSharpDB.Primitives;
```

Update fully-qualified symbols:

```csharp
// Before
CSharpDB.Core.DbType.Integer
CSharpDB.Core.CSharpDbException

// After
CSharpDB.Primitives.DbType.Integer
CSharpDB.Primitives.CSharpDbException
```

## Project Migration

Update project references:

```xml
<!-- Before -->
<ProjectReference Include="..\..\src\CSharpDB.Core\CSharpDB.Core.csproj" />

<!-- After -->
<ProjectReference Include="..\..\src\CSharpDB.Primitives\CSharpDB.Primitives.csproj" />
```

Update package usage:

```bash
# Before
dotnet add package CSharpDB.Core

# After
dotnet add package CSharpDB.Primitives
```

## Binary Impact

- The assembly name is now `CSharpDB.Primitives.dll`.
- Any code compiled against `CSharpDB.Core.dll` must be rebuilt against `CSharpDB.Primitives.dll`.
- Reflection, `typeof(...)` comparisons, and serialized type names must be updated if they referenced the old namespace or assembly name.
