# API-Level Sharding Sample

This sample shows CSharpDB sharding through a concrete e-commerce order-history scenario.

Orders are write-once/read-many. A shopper usually sees recent orders first, so the app reads the current month route and pages those rows. When the shopper chooses an older month, the app supplies that older month as the route key and reads the correct shard for that month.

Run it from the repository root:

```bash
dotnet run --project samples/api-level-sharding/ApiLevelShardingSample.csproj
```

The program creates a local master database plus four local shard database files under the build output directory, writes the active shard map into the master database, applies the same `orders` schema to every shard through the shard-admin surface, pins several month route keys to specific shards for clear output, and shows how the app explicitly routes current and older order-history pages.

## Creating The Master Sharding DB

The sample intentionally uses the same master-catalog pattern that Admin,
daemon, and API hosts use. There is no sharding section in appsettings and no
JSON catalog file. The normal database path points at the master database; the
master database contains the active shard map.

The sample creates the master DB under the generated shard directory:

```csharp
string dataDirectory = Path.Combine(AppContext.BaseDirectory, "shards");
string masterDbPath = Path.Combine(dataDirectory, "master.db");
```

It then builds the shard map payload and writes it into `master.db`:

```csharp
CSharpDbShardingOptions activeMap = CreateShardingOptions(dataDirectory);

await CSharpDbShardedClient.SeedMasterCatalogAsync(
    new CSharpDbClientOptions
    {
        DataSource = masterDbPath,
    },
    activeMap);
```

After that, the application opens only the master database and lets CSharpDB
discover whether sharding is active:

```csharp
await using CSharpDbShardedClient sharded =
    await CSharpDbShardedClient.TryCreateFromMasterCatalogAsync(new CSharpDbClientOptions
    {
        DataSource = masterDbPath,
    })
    ?? throw new InvalidOperationException("The sample master database did not contain an active shard map.");
```

For a daemon or Admin host, the equivalent configuration is only the normal
database locator:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=path/to/master.db"
  }
}
```

The master database is not a data shard and is not part of fan-out. It owns the
shard map, directory metadata, catalog history, migration checkpoints, and
future failover metadata. The sample recreates it on each run so the output is
deterministic; a real deployment would create it once, then use catalog update
and migration APIs to evolve the map.

## Existing Large Databases

This sample is a new sharded setup. It does not convert an existing monolithic
database into shards.

For an existing large DB, do not seed a shard map and assume the old rows are
sharded. Routed queries read the shard DBs, not the original monolithic DB, and
the master DB is not part of fan-out. The existing data must be copied into
shard DBs by route key, verified, and cut over before the master catalog becomes
the active entry point.

The required flow is documented in
[`docs/sharding-existing-database-migration.md`](../../docs/sharding-existing-database-migration.md).

## Scenario

The route context is:

```csharp
new CSharpDbRouteContext
{
    Keyspace = "orders_by_month",
    Key = "2026-06"
}
```

The route key is the order month in `yyyy-MM` form. Normal application code does not choose `shard-0` or `shard-1`; it chooses the business key, and CSharpDB resolves that key through the virtual-bucket map and exact pins.

The sample models these user flows:

- Customer opens order history: read page 1 from route key `2026-06`.
- Customer chooses May 2026 from an older-history selector: read page 1 from route key `2026-05`.
- Customer requests a 10-row order-history page that spans months: read 6 rows from `2026-06`, then continue into `2026-05` for the remaining 4 rows.
- Customer requests a multi-month summary: caller explicitly iterates `2026-06`, `2026-05`, `2026-04`, and `2025-12`.
- A new June order is committed through a transaction whose id is prefixed with the resolved shard.
- A missing route key fails in sharded mode.

## What It Demonstrates

- A master database that stores the active shard map for four direct/local shard files.
- Stable virtual-bucket ownership through `BucketRanges`.
- Month-based route keys for order-history partitioning.
- `ExactKeyPins` for operator-controlled placement of hot or archival months.
- `ICSharpDbShardAdminClient` for map snapshots, route preview, shard status, and schema setup.
- `ExecuteSqlOnAllShardsAsync(...)` for explicit schema setup across shards.
- `ExecuteReadOnlySqlOnAllShardsAsync(...)` for diagnostic read-only fan-out with one result per shard.
- `ForRoute(...)` for normal application requests.
- `ForShardId(...)` for admin/debug inspection.
- Application-level page filling across route keys when one UI page spans more than one month.
- Transaction ids prefixed as `csdbshard:{mapVersion}:{shardId}:{innerId}` so commit/rollback can route without extra headers.
- Sharded mode rejecting SQL execution when no route context is available.

## Expected Output Shape

```text
API-Level Sharding Sample: E-Commerce Order History

Keyspace:        orders_by_month
Route key shape: yyyy-MM order month
Customer:        customer-1001
Virtual buckets: 16
Master DB:       ...
Shard files:     ...

Month route map
---------------
2026-06 bucket=.. shard=shard-0 token=0x...
2026-05 bucket=.. shard=shard-1 token=0x...
2026-04 bucket=.. shard=shard-2 token=0x...
2025-12 bucket=.. shard=shard-3 token=0x...

Shard admin snapshot
--------------------
Map version:       1
Shard definitions: 4
Bucket ranges:     4
Exact pins:        4
Directory indexes: 0 (read-only placeholder for future global lookups)

Recent orders page
------------------
Route key: 2026-06
SO-202606-...  ...

Older history page selected by user
-----------------------------------
Route key: 2026-05
SO-202605-...  ...

Filled cross-month page
-----------------------
Requested 10 orders. Query months newest-to-oldest until the page is full.
Route 2026-06 supplied 6 row(s).
Route 2026-05 supplied 4 row(s).
```

## Important Pattern

The route key gets the request to the right shard. SQL should still include the route-key column for clarity and safety:

```sql
SELECT order_number, order_date, amount, status
FROM orders
WHERE order_month = '2026-05'
  AND customer_id = 'customer-1001'
ORDER BY order_date DESC
LIMIT 3 OFFSET 0;
```

This matters because multiple route keys can live on the same physical shard. Routing narrows the database file; filtering narrows the rows inside that file.

## Page Fill Across Route Keys

When a page can span more than one route key, the application owns the aggregation loop. For page 1 of a descending order-history view, the sample tries each month in newest-to-oldest order until the page has enough rows:

```text
remaining = 10

query 2026-06 LIMIT 10 OFFSET 0
  -> 6 rows
  -> remaining = 4

query 2026-05 LIMIT 4 OFFSET 0
  -> 4 rows
  -> page is full
```

For later pages, the sample uses the same principle with a global offset:

1. Count rows in the current route key.
2. If the global offset is larger than that count, skip the whole route.
3. Otherwise query that route with a route-local `OFFSET`.
4. Continue through older route keys until the page is full.

## Alternative Aggregation Patterns

The sample implements a precise page-fill algorithm because it reads only the rows needed for an arbitrary page. Some applications can use simpler patterns when the UI only spans a small, known route window.

### Bounded Over-Fetch, Then Merge And Limit

For a recent-orders page, the UI may only need the current month and previous month. In that case, the app can query both routes, accept a small amount of extra data, merge by the same sort order, and trim the result to the requested page size.

```text
pageSize = 10
candidateMonths = ["2026-06", "2026-05"]
rows = []

for month in candidateMonths:
    rows += query route month
        where customer_id = "customer-1001"
        order by order_date desc, id desc
        limit pageSize

page = rows
    order by order_date desc, id desc
    take pageSize
```

This avoids route counts and route-local offset math. The tradeoff is that it can read extra rows and only works well when the route window is intentionally small. Use a deterministic tie-breaker such as `id` with `order_date` so merged pages are stable.

### Date-Range Window Reads

For filters such as "last 90 days" or "orders from April through June", compute the month route keys from the date range and query each route with both the route key and the date predicate.

```text
months = monthsBetween(startDate, endDate)
rows = []

for month in months newest-to-oldest:
    rows += query route month
        where order_month = month
          and order_date >= startDate
          and order_date < endDate
        order by order_date desc, id desc
        limit perRouteLimit

page = merge rows by order_date desc, id desc
page = page take pageSize
```

This is useful for user-selected windows and reports. Keep `perRouteLimit` bounded so a broad date filter does not accidentally become an expensive fan-out.

### Cursor-Based Continuation

Instead of counting every route for later pages, the API can return a continuation token that records where the next page should resume. For month-sharded order history, that token can contain the ordered route list plus either per-route offsets or the last `(order_date, id)` cursor returned from each route.

```text
request:
  pageSize = 10
  token = previous response token, or empty

state = decode token
rows = []

for month in state.remainingMonths:
    rows += query route month
        where row is after the month cursor
        order by order_date desc, id desc
        limit rows still needed

    update month cursor
    if rows has pageSize:
        break

return rows plus updated token
```

This usually fits infinite scroll better than numbered pages. It avoids global counts, but the token must be treated as part of the API contract.

This is application-level aggregation. CSharpDB V1 does not run a distributed query plan for this.

## Diagnostic Read-Only Fan-Out

The shard-admin surface also has a read-only fan-out helper for diagnostics and
Admin screens. For example, an operator can ask every shard for an order count
and display the result by shard:

```csharp
IReadOnlyList<CSharpDbShardSqlExecutionResult> counts =
    await shardAdmin.ExecuteReadOnlySqlOnAllShardsAsync(
        "SELECT COUNT(*) FROM orders;");

foreach (CSharpDbShardSqlExecutionResult shard in counts)
{
    Console.WriteLine($"{shard.ShardId}: {shard.Error ?? "ok"}");
}
```

This is intentionally not the same as the customer order-history flow. The
customer flow should keep using route-aware calls so the app controls which
months are read, how much data is fetched, and how pages are filled. The
read-only fan-out helper returns per-shard results and leaves any merge, rollup,
or presentation decision to the caller.

## Trying The Same Routes In Admin

Open the sample `master.db` in CSharpDB Studio. The sample prints the full
`Master DB:` path in its output. When the master database contains the sample
shard map, the Query, table data, and collection tabs show a route selector.
Enter:

```text
keyspace: orders_by_month
route key: 2026-06
```

Then run the same order-history SQL for June. To inspect older history, change
only the route key to `2026-05` and keep the SQL filtered by
`order_month = '2026-05'`. Admin does not infer the shard from the `WHERE`
clause; the selected route controls the database file, and the SQL predicate
controls the rows inside that file.

## Operational Notes

This is API-level routing, not distributed SQL. A normal request supplies one application-owned route key and lands on one shard. V1 does not infer the shard from arbitrary `WHERE` clauses, run cross-shard joins, move data automatically when bucket ownership changes, or coordinate cross-shard transactions.

For order history, the application owns the history navigation model. Recent orders can use the current month route. Older history can use a month picker, archive selector, or known list of route keys. If a view spans multiple months or shards, the caller must run multiple routed requests and combine the results.

The important invariant is that bucket ownership is stable. Adding a shard should be a migration project: copy the relevant data, update the bucket map version, deploy the new map, and verify ownership. Do not replace this with `hash(key) % shardCount`; changing the shard count would silently remap existing keys.

Phase 4 starts that migration project with exact route-key movement and bucket-range movement. In catalog mode, an operator can call `MigrateExactRouteKeyAsync(...)` for one known route key or `MigrateBucketRangeAsync(...)` for a virtual-bucket interval. Both APIs use a manifest for route-owned tables and collections, fence affected writes, copy matching rows/documents, verify counts and checksums, then write a pending map change that takes effect after the sharded client or daemon is restarted. The same catalog records movement history that can be read with `GetShardMigrationHistoryAsync()`. Automatic SQL ownership inference remains outside this sample.
