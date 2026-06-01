# API-Level Sharding Sample

This sample shows CSharpDB sharding through a concrete e-commerce order-history scenario.

Orders are write-once/read-many. A shopper usually sees recent orders first, so the app reads the current month route and pages those rows. When the shopper chooses an older month, the app supplies that older month as the route key and reads the correct shard for that month.

Run it from the repository root:

```bash
dotnet run --project samples/api-level-sharding/ApiLevelShardingSample.csproj
```

The program creates four local shard database files under the build output directory, applies the same `orders` schema to every shard through the shard-admin surface, pins several month route keys to specific shards for clear output, and shows how the app explicitly routes current and older order-history pages.

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

- `CSharpDbShardingOptions` with four direct/local shard files.
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

## Operational Notes

This is API-level routing, not distributed SQL. A normal request supplies one application-owned route key and lands on one shard. V1 does not infer the shard from arbitrary `WHERE` clauses, run cross-shard joins, move data automatically when bucket ownership changes, or coordinate cross-shard transactions.

For order history, the application owns the history navigation model. Recent orders can use the current month route. Older history can use a month picker, archive selector, or known list of route keys. If a view spans multiple months or shards, the caller must run multiple routed requests and combine the results.

The important invariant is that bucket ownership is stable. Adding a shard should be a migration project: copy the relevant data, update the bucket map version, deploy the new map, and verify ownership. Do not replace this with `hash(key) % shardCount`; changing the shard count would silently remap existing keys.

Phase 4 starts that migration project with exact route-key movement. In catalog mode, an operator can call `MigrateExactRouteKeyAsync(...)` with a manifest for the route-owned tables and collections. CSharpDB fences writes for that route key, copies the matching rows/documents, verifies counts and checksums, then writes a pending exact-key pin that takes effect after the sharded client or daemon is restarted. The same catalog records migration history that can be read with `GetShardMigrationHistoryAsync()`. Bucket-range movement and automatic SQL ownership inference remain outside this sample.
