# Collection Indexing and Query Examples

This guide covers the document-style `Collection<T>` API, not SQL `CREATE INDEX`.

If you are working with SQL tables, use SQL indexes. If you are storing typed documents through `GetCollectionAsync<T>()`, this is the indexing model you want.

## What Collection Indexes Support

Collection indexes can be created over:

- scalar members such as `Email` or `LoyaltyPoints`
- nested object paths such as `$.address.city`
- terminal array-element paths such as `$.tags[]`
- nested array-object paths such as `$.orders[].sku`

Collection queries use an index when present and fall back to a scan when the index does not exist. For hot paths, create the index first.

Range queries are supported only for scalar paths. Array-element paths are equality/contains only.

## End-to-End Example

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("collections-demo.db");
var users = await db.GetCollectionAsync<UserDocument>("users");

await users.PutAsync("user:ada", new UserDocument(
    Email: "ada@acme.io",
    LoyaltyPoints: 1200,
    Address: new AddressDocument("Seattle", "WA"),
    Tags: ["premium", "beta"],
    Orders:
    [
        new OrderDocument("SKU-1001", 2),
        new OrderDocument("SKU-2005", 1)
    ]));

await users.PutAsync("user:grace", new UserDocument(
    Email: "grace@acme.io",
    LoyaltyPoints: 420,
    Address: new AddressDocument("Portland", "OR"),
    Tags: ["standard"],
    Orders:
    [
        new OrderDocument("SKU-2005", 3)
    ]));

await users.PutAsync("user:linus", new UserDocument(
    Email: "linus@acme.io",
    LoyaltyPoints: 2450,
    Address: new AddressDocument("Seattle", "WA"),
    Tags: ["premium"],
    Orders:
    [
        new OrderDocument("SKU-9000", 1)
    ]));

// Top-level scalar indexes
await users.EnsureIndexAsync("Email");
await users.EnsureIndexAsync("LoyaltyPoints");

// Nested object path
await users.EnsureIndexAsync("$.address.city");

// Terminal array element path: "contains this tag"
await users.EnsureIndexAsync("$.tags[]");

// Nested array-object path: "any order has this SKU"
await users.EnsureIndexAsync("$.orders[].sku");

// Equality lookup on a top-level field
await foreach (var match in users.FindByIndexAsync("Email", "ada@acme.io"))
    Console.WriteLine($"email => {match.Key}");

// Equality lookup on a nested object path
await foreach (var match in users.FindByPathAsync("Address.City", "Seattle"))
    Console.WriteLine($"city => {match.Key}");

// Array-element membership lookup
await foreach (var match in users.FindByPathAsync("$.tags[]", "premium"))
    Console.WriteLine($"tag => {match.Key}");

// Nested array-object lookup
await foreach (var match in users.FindByPathAsync("$.orders[].sku", "SKU-2005"))
    Console.WriteLine($"sku => {match.Key}");

// Numeric range query on a scalar path
await foreach (var match in users.FindByPathRangeAsync("LoyaltyPoints", 1000, 3000))
    Console.WriteLine($"points => {match.Key}");

// Ordered text range query on a scalar text index
await foreach (var match in users.FindByPathRangeAsync("Email", "a", "h", upperInclusive: false))
    Console.WriteLine($"email range => {match.Key}");

public sealed record UserDocument(
    string Email,
    int LoyaltyPoints,
    AddressDocument Address,
    string[] Tags,
    OrderDocument[] Orders);

public sealed record AddressDocument(string City, string State);

public sealed record OrderDocument(string Sku, int Quantity);
```

## Path Shapes

Use these path forms with `EnsureIndexAsync`, `FindByIndexAsync`, `FindByPathAsync`, and `FindByPathRangeAsync`:

| Path shape | Example | Meaning |
|---|---|---|
| Top-level scalar member | `Email` | Index/query one property on the document |
| Nested object member | `$.address.city` | Index/query a nested scalar value |
| Terminal array element | `$.tags[]` | Match documents where any array element equals the lookup value |
| Nested array-object member | `$.orders[].sku` | Match documents where any object inside the array has a matching member |

Both `Address.City` and `$.address.city` normalize to the same path shape for typed collections.

## Expression-Based Variants

For simple member paths, you can use strongly typed selectors instead of strings:

```csharp
await users.EnsureIndexAsync(u => u.Email);
await users.EnsureIndexAsync(u => u.LoyaltyPoints);

await foreach (var match in users.FindByIndexAsync(u => u.Email, "ada@acme.io"))
    Console.WriteLine(match.Key);

await foreach (var match in users.FindByPathRangeAsync(u => u.LoyaltyPoints, 1000, 3000))
    Console.WriteLine(match.Key);
```

Use string paths when you need array-element forms such as `$.tags[]` or `$.orders[].sku`.

## Practical Rules

- Create the index before the query becomes performance-sensitive.
- Use scalar paths for range queries.
- Use `[]` only for array membership paths.
- Prefer top-level names such as `Email` when a simple member is enough.
- Use `FindByIndexAsync(...)` or `FindByPathAsync(...)` for equality lookups.
- Use `FindByPathRangeAsync(...)` for ordered integer, temporal, or text ranges.

## Related Docs

- [Samples Overview](../../samples/README.md)
- [Getting Started](../getting-started.md)
- [v2.2.0 Release Notes](../releases/v2.2.0-pr-notes.md)
