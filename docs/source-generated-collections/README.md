# Source-Generated Collection Fast Path

## Summary

`Collection<T>` is currently the main reflection-heavy runtime surface in CSharpDB.

That reflection is concentrated in:

- typed document serialization and deserialization
- collection index field binding
- constructor/member discovery for binary document hydration

The SQL engine is not the main reflection source today, so a "remove reflection from the whole engine" effort should actually be treated as a collection/runtime optimization project.

This roadmap item introduces a new source-generated collection path that removes runtime reflection from the performance-critical typed collection flow while keeping the current on-disk collection payload format compatible.

The existing `Collection<T>` API remains as the legacy compatibility path. The new generated path becomes the recommended fast path for:

- NativeAOT / trim-sensitive applications
- collection-heavy applications using typed POCO documents
- users who want lower collection write/materialization overhead without moving to SQL tables

## Why This Exists

Recent investigation on the branch showed that the main runtime reflection sites are:

- `CollectionDocumentCodec<T>` JSON serialization/deserialization
- `CollectionBinaryDocumentCodec` constructor/member discovery and object materialization
- `CollectionIndexBinding<T>` member-path resolution and member reads

Those costs do not meaningfully affect the SQL engine, but they do matter for:

- collection writes
- collection decode/materialization
- collection index backfill and maintenance
- trim/AOT warnings and compatibility

This means the right optimization is not a broad engine rewrite. The right optimization is a generated typed-collection path.

## Problem Statement

The current typed collection pipeline depends on runtime reflection and dynamic code generation:

- member enumeration via `GetProperties()` / `GetFields()`
- constructor discovery via `GetConstructors()`
- runtime object activation and setter/getter creation
- member-path value reads for collection indexes
- serializer metadata discovery for POCO document models

That creates four problems:

1. it adds first-use and per-document overhead on typed collection workloads
2. it makes the typed collection surface trim-unsafe and NativeAOT-hostile
3. it ties the fastest collection path to runtime metadata discovery
4. it makes future performance work harder because the codec and index path are driven by `MemberInfo` rather than generated static metadata

## Goals

- add a no-reflection fast path for typed collections
- preserve current collection binary payload compatibility
- make the fast path trim-safe and NativeAOT-friendly
- remove runtime member discovery from typed collection serialization, deserialization, and index binding
- keep the generated fast path explicit and easy to understand

## Non-Goals

- changing the SQL engine execution model
- replacing `Collection<T>` immediately
- removing `JsonElement` collection support
- introducing a new on-disk collection file format in v1
- reworking remote client collection APIs in the same phase

## Proposed Shape

### New Public Surface

Add a new generated collection API rather than overloading the existing reflection-based one:

- `Database.GetGeneratedCollectionAsync<T>(string name, GeneratedCollectionModel<T> model, CancellationToken ct = default)`
- `GeneratedCollection<T>`
- `GeneratedCollectionModel<T>`

The generated model is produced by a new source generator package, tentatively `CSharpDB.Generators`.

Document types opt in with an attribute such as:

```csharp
[CSharpDbCollectionModel]
public partial class UserDocument
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
}
```

The generator emits a companion model such as:

```csharp
UserDocumentCSharpDbCollectionModel.Instance
```

### Generated Responsibilities

The generated model should provide:

- binary encode/decode for the current collection payload format
- stable constructor binding with no runtime constructor discovery
- precomputed getters/setters with no `MemberInfo.GetValue` / `SetValue`
- generated field descriptors/tokens for supported collection index paths
- generated metadata needed for direct index backfill/maintenance

### Legacy Compatibility

The current `Collection<T>` path stays supported and remains the compatibility/legacy path.

It should continue to support:

- existing POCO collection callers
- `JsonElement`
- reflection-based schema-less usage

But it should stop being the main place where performance work lands.

## Storage / Format Direction

V1 keeps the current collection payload format compatible.

That means:

- generated encoders must write payloads readable by the existing runtime codec
- generated decoders must read payloads written by the existing runtime codec
- no migration is required for existing collection data

This roadmap item is about removing runtime reflection, not about introducing a new document wire format.

## Engine Design Changes

### Codec Layer

Refactor the existing reflection-heavy codec into two layers:

1. low-level collection wire-format reader/writer primitives
2. two metadata providers on top:
   - legacy reflection metadata provider
   - generated metadata provider

The generated path should call the low-level primitives directly and avoid `MemberInfo`, `ConstructorInfo`, and runtime expression compilation.

### Index Binding

Replace reflection-based field binding on the generated path with generated descriptors:

- generated collections use compile-time field descriptors for index create/backfill/write maintenance
- supported array-element field paths should also be generated where possible
- transient string field-path lookups can remain on the legacy path

### Query / Find Surface

For the generated path, favor generated field tokens over expression parsing or reflective member-path resolution.

Example direction:

```csharp
await users.EnsureIndexAsync(UserDocumentFields.Name, ct);
var matches = await users.FindByIndexAsync(UserDocumentFields.Name, "ada", ct);
```

That keeps the fast path static and avoids rebuilding field bindings from expressions or strings.

## Performance Expectations

The main expected gains are:

- lower first-use overhead for typed collections
- faster document encode/decode
- faster index backfill and write maintenance
- better NativeAOT and trim behavior

This work should not be expected to materially change SQL benchmarks, because SQL is not currently driven by the same reflection-heavy pipeline.

## Test Plan

- generated encoder can write documents readable by legacy decoders
- generated decoder can read documents written by legacy encoders
- generated collection CRUD matches legacy collection semantics
- generated collection indexes behave the same as legacy indexes
- generated collections survive reopen/recovery in file-backed, hybrid, and in-memory modes
- generated APIs build cleanly without `RequiresUnreferencedCode` / `RequiresDynamicCode`
- NativeAOT smoke test exercises a generated collection model
- focused benchmarks compare legacy vs generated collection paths for:
  - single put
  - batch put
  - get
  - indexed lookup
  - index backfill

## Status

This should be treated as a **Planned** roadmap item.

It is a better fit than trying to remove reflection from "the whole engine" because it targets the actual hot path and the actual trim/AOT risk in the current codebase.

## See Also

- [Roadmap](../roadmap.md)
- [Advanced Collection Storage Plan](../advanced-collection-storage/README.md)
- [Performance Phasing Plan](../performance-phasing/README.md)
