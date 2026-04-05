# CSharpDB.Generators

Source generator for CSharpDB generated collection models.

Use this package with `CSharpDB.Engine` when you want:

- generated collection codecs backed by a `System.Text.Json` source-generated context
- generated `CollectionField<,>` descriptors such as `User.Collection.Email`
- flattened nested descriptors such as `User.Collection.Address_City` and `User.Collection.Orders_Sku`
- trim-safe typed collection access through `Database.GetGeneratedCollectionAsync<T>(...)`

Example:

```csharp
using System.Text.Json.Serialization;
using CSharpDB.Engine;

[CollectionModel(typeof(UserJsonContext))]
public sealed partial record User(string Email, int Age);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(User))]
internal sealed partial class UserJsonContext : JsonSerializerContext;

await using var db = await Database.OpenAsync("app.db");
var users = await db.GetGeneratedCollectionAsync<User>("users");

await users.PutAsync("alice", new User("alice@example.com", 30));
await users.EnsureIndexAsync(User.Collection.Email);
```

The generator currently emits descriptors for:

- top-level scalar members
- top-level scalar collections like `Tags[]`
- nested scalar members like `Address.City`
- nested collection scalar members like `Orders[].Sku`

Descriptor names stay based on CLR member names even when payload names are
renamed with `JsonPropertyName`.

Unsupported public members are ignored with a build warning (`CDBGEN007`) so
generator coverage gaps fail loudly instead of silently omitting descriptors.

At runtime, `GetGeneratedCollectionAsync<T>(...)` also expects existing
collection indexes for that document type to bind through registered generated
descriptors. Reflection-only path indexes should stay on `GetCollectionAsync<T>(...)`
until the generated model covers those paths.
