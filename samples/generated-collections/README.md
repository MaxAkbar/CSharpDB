# Generated Collections Sample

This sample is a runnable C# console project that demonstrates the source-generated collection fast path end to end.

## What It Shows

- `Database.GetGeneratedCollectionAsync<T>(...)`
- generated `CollectionField<,>` descriptors such as `CustomerRecord.Collection.Email`
- top-level scalar indexes such as `Email` and `LoyaltyPoints`
- nested scalar indexes such as `Address.City` and `Address.ZipCode`
- terminal array-element indexes such as `Tags[]`
- nested array-object indexes such as `Orders[].Sku`
- `JsonPropertyName` payload renames while keeping public descriptor names on CLR member names

## Files

- `GeneratedCollectionsSample.csproj` - sample project
- `Program.cs` - declares generated collection models, seeds data, creates descriptor-based indexes, and runs example queries

## Run

```bash
dotnet run --project samples/generated-collections/GeneratedCollectionsSample.csproj
```

The sample creates a small demo database next to the compiled output, writes a few customer documents, creates generated collection indexes, then prints matches for equality, membership, and range queries.

## Publish Trimmed

```bash
dotnet publish samples/generated-collections/GeneratedCollectionsSample.csproj -c Release -r win-x64 --self-contained true
```

## Publish NativeAOT

```bash
dotnet publish samples/generated-collections/GeneratedCollectionsSample.csproj -c Release -r win-x64 -p:PublishAot=true
```

## Related Docs

- [CSharpDB.Engine README](../../src/CSharpDB.Engine/README.md)
- [CSharpDB.Generators README](../../src/CSharpDB.Generators/README.md)
- [Samples Overview](../README.md)
