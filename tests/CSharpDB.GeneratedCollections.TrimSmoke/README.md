# Generated Collection Trim Smoke

Small publish-time validation app for the source-generated collection path.

It exists to prove that:

- `GetGeneratedCollectionAsync<T>(...)` can be published trimmed
- generated codecs and descriptors work without reflection-based `System.Text.Json`
- nested generated descriptors like `Orders[].Sku` and `Address.City` still work in a published app

Run:

```powershell
dotnet publish .\tests\CSharpDB.GeneratedCollections.TrimSmoke\CSharpDB.GeneratedCollections.TrimSmoke.csproj -c Release -r win-x64 --self-contained true
.\tests\CSharpDB.GeneratedCollections.TrimSmoke\bin\Release\net10.0\win-x64\publish\CSharpDB.GeneratedCollections.TrimSmoke.exe
```

Optional NativeAOT probe:

```powershell
dotnet publish .\tests\CSharpDB.GeneratedCollections.TrimSmoke\CSharpDB.GeneratedCollections.TrimSmoke.csproj -c Release -r win-x64 -p:PublishAot=true
```
