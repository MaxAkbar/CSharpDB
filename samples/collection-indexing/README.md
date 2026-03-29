# Collection Indexing Sample

This sample is a runnable C# console project that demonstrates the `Collection<T>` indexing APIs end to end.

## What It Shows

- top-level scalar indexes such as `Email`
- scalar range indexes such as `LoyaltyPoints`
- nested object path indexes such as `$.address.city`
- terminal array-element indexes such as `$.tags[]`
- nested array-object indexes such as `$.orders[].sku`

## Files

- `CollectionIndexingSample.csproj` - sample project
- `Program.cs` - seeds a document collection, creates indexes, and runs example queries

## Run

```bash
dotnet run --project samples/collection-indexing/CollectionIndexingSample.csproj
```

The sample creates a small demo database next to the compiled output, writes a few user documents, creates collection indexes, then prints matches for equality, membership, and range queries.

## Related Docs

- [Collection Indexing Guide](https://csharpdb.com/docs/collection-indexing.html)
- [Samples Overview](../README.md)
