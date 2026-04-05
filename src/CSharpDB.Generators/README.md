# CSharpDB.Generators

Source generator for CSharpDB generated collection models.

Use this package with `CSharpDB.Engine` when you want:

- generated collection codecs backed by a `System.Text.Json` source-generated context
- generated `CollectionField<,>` descriptors such as `User.Collection.Email`
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
