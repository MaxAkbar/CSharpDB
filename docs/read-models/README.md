# Materialized Join Read Models

This document is a concrete design note, not an implementation.
It describes how to handle a hot joined read path in CSharpDB by precomputing a read model and serving it through the existing `Collection<T>` API instead of live SQL joins.

## Why Use This Pattern

Use a materialized read model when all of the following are true:

- the query shape is fixed and heavily reused
- the read path is more important than write simplicity
- denormalized or snapshot-style fields are acceptable
- you want to avoid repeated SQL parse, plan, join, and aggregate work

Stay on SQL when the query is ad hoc, the filter shape changes constantly, or the caller needs live fully-normalized semantics on every read.

## Recommended First Example

Use `orders + customers` as the first read-model target.

Assume the hot queries look like this:

```sql
SELECT
    o.id,
    o.customer_id,
    o.created_utc,
    o.status,
    o.total_amount,
    c.name,
    c.region,
    c.tier
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.id = @orderId;
```

```sql
SELECT
    o.id,
    o.created_utc,
    o.status,
    o.total_amount,
    c.name,
    c.region
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = @customerId
  AND o.created_utc BETWEEN @fromUtc AND @toUtc
ORDER BY o.created_utc DESC;
```

```sql
SELECT
    o.customer_id,
    date(o.created_utc) AS day,
    COUNT(*) AS order_count,
    SUM(o.total_amount) AS total_amount
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.customer_id = @customerId
  AND o.created_utc BETWEEN @fromUtc AND @toUtc
GROUP BY o.customer_id, date(o.created_utc);
```

## Recommended Read Models

Use two collections, each optimized for a different hot path.

### 1. `order_customer_read`

Purpose:
Serve joined order-detail and customer-order listing screens without a live join.

Suggested document shape:

```csharp
public sealed class OrderCustomerReadModel
{
    public long OrderId { get; set; }
    public long CustomerId { get; set; }
    public DateTime CreatedUtc { get; set; }
    public long CreatedUtcTicks { get; set; }
    public string CustomerCreatedKey { get; set; } = "";
    public string Status { get; set; } = "";
    public decimal TotalAmount { get; set; }

    public string CustomerNameSnapshot { get; set; } = "";
    public string CustomerRegionSnapshot { get; set; } = "";
    public string CustomerTierSnapshot { get; set; } = "";
}
```

Collection name:

```text
order_customer_read
```

Primary key:

```text
ord:{OrderId}
```

Indexes to create:

- `CustomerId`
- `Status`
- `CreatedUtcTicks`
- `CustomerCreatedKey`

### 2. `customer_day_sales`

Purpose:
Serve joined aggregate queries as direct lookups or bounded range scans over precomputed rows.

Suggested document shape:

```csharp
public sealed class CustomerDaySalesSummary
{
    public long CustomerId { get; set; }
    public DateOnly Day { get; set; }
    public string CustomerDayKey { get; set; } = "";

    public long OrderCount { get; set; }
    public decimal TotalAmount { get; set; }

    public string CustomerRegionSnapshot { get; set; } = "";
    public string CustomerTierSnapshot { get; set; } = "";
}
```

Collection name:

```text
customer_day_sales
```

Primary key:

```text
cust:{CustomerId}|day:{yyyyMMdd}
```

Indexes to create:

- `CustomerId`
- `Day`
- `CustomerDayKey`

## Key Strategy

The current collection API supports single-path equality and ordered range indexes well.
It does not expose a general composite secondary-index API, so the design should use encoded composite fields for the hot multi-parameter paths.

### `CustomerCreatedKey`

Use a fixed-width text key:

```text
cust:{CustomerId:D20}|ticks:{CreatedUtcTicks:D19}
```

Why:

- supports `CustomerId = @customerId` plus `CreatedUtc BETWEEN @from AND @to`
- works with a single ordered text index
- keeps ordering stable because the numeric segments are fixed width

Example range:

```text
lower = cust:00000000000000000042|ticks:000638460000000000
upper = cust:00000000000000000042|ticks:000638546399999999
```

### `CustomerDayKey`

Use:

```text
cust:{CustomerId:D20}|day:{yyyyMMdd}
```

Why:

- supports day-bucket aggregate lookups per customer
- supports ordered range scans over a bounded date window
- avoids needing a native composite secondary index for v1

## Parameter Support

The read model is locked to a fixed query shape, but literal values stay parameterized.

Supported parameterized filters:

- `OrderId = @orderId`
- `CustomerId = @customerId`
- `Status = @status`
- `CreatedUtc BETWEEN @fromUtc AND @toUtc`
- `CustomerId = @customerId AND CreatedUtc BETWEEN @fromUtc AND @toUtc`
- `CustomerId = @customerId AND Day BETWEEN @fromDay AND @toDay`

Not supported as first-class fast paths in this design:

- arbitrary `OR` trees
- ad hoc joins to unrelated tables
- many optional filters across unrelated dimensions
- predicates on fields not stored in the read model

## Query Mapping

### Order By Order Id

Use:

```csharp
await ordersRead.GetAsync($"ord:{orderId}", ct);
```

This is the direct point-read path.

### Orders For Customer In Time Range

Use:

```csharp
await foreach (var row in ordersRead.FindByPathRangeAsync(
    x => x.CustomerCreatedKey,
    lowerKey,
    upperKey,
    ct: ct))
{
}
```

This replaces:

- join on `orders.customer_id = customers.id`
- `WHERE customer_id = @customerId`
- `AND created_utc BETWEEN @fromUtc AND @toUtc`
- `ORDER BY created_utc`

### Customer Sales Summary In Date Range

Use:

```csharp
await foreach (var row in customerDaySales.FindByPathRangeAsync(
    x => x.CustomerDayKey,
    lowerDayKey,
    upperDayKey,
    ct: ct))
{
}
```

This replaces:

- join on `orders.customer_id = customers.id`
- group by `customer_id, day`
- aggregate `COUNT(*)` and `SUM(total_amount)`

## Write Maintenance Rules

Maintain the read models in the same transaction as the base-table write.

### On Order Insert

1. Load the customer row once.
2. Build the `order_customer_read` document.
3. Upsert `ord:{OrderId}`.
4. Compute the customer day bucket.
5. Upsert `customer_day_sales` for that bucket by incrementing:
   - `OrderCount += 1`
   - `TotalAmount += order.TotalAmount`

### On Order Update

Treat this as `remove old contribution` then `apply new contribution`.

If any of these fields change:

- `CustomerId`
- `CreatedUtc`
- `Status`
- `TotalAmount`

Then:

1. load the previous order version
2. decrement the old summary bucket
3. rewrite or move the detail document if the key fields changed
4. increment the new summary bucket

### On Order Delete

1. load the existing order
2. delete `ord:{OrderId}`
3. decrement the matching `customer_day_sales` bucket
4. if the bucket becomes zeroed, delete it

### On Customer Update

Use snapshot semantics in v1.

Rules:

- `CustomerNameSnapshot`
- `CustomerRegionSnapshot`
- `CustomerTierSnapshot`

are captured when the order read model is written.

Do not fan out updates into historical order rows in v1.
If a screen needs the live current customer master record, fetch the customer row separately.

This avoids an expensive rewrite of every historical order when a customer profile changes.

## Expected Performance

These are ballpark expectations based on the current benchmark data already in the repo.

- point read from a materialized collection: around `1.3x` faster than SQL PK lookup
- indexed equality/range lookup over a materialized collection: around `4x` to `5x` faster than generic SQL indexed lookup
- precomputed joined aggregate lookup: often `100x+`, and in some narrow cases much more, because the read path becomes a fetch instead of a live join plus aggregate

Treat these as directional targets, not guarantees.
Actual wins will depend on row width, fan-out, cache locality, and how much post-filtering still happens after the lookup.

## Tradeoffs

Benefits:

- no SQL parse on the hot path
- no planner work on the hot path
- no live join on the hot path
- aggregate work moved from read time to write time
- stable latency for fixed query shapes

Costs:

- more write complexity
- more storage
- denormalized data
- explicit rebuild/backfill logic
- snapshot semantics for some parent-table fields

## When Read Models Break Down

Large row counts by themselves are not the main problem.
The real problem is high-change fan-out.

### What Stays Cheap

Incremental maintenance is still efficient at very large scale when the changed row only affects a small number of read-model rows.

Examples:

- inserting one order usually writes one detail read row and updates one summary bucket
- deleting one order usually deletes one detail read row and updates one summary bucket
- updating one order amount or timestamp usually removes one old contribution and adds one new contribution

In these cases, the cost is tied to the changed fact row, not to the total table size.
That is why a read model can still work well even when the base tables contain millions of rows.

### What Gets Expensive

The dangerous case is a parent or dimension update that fans out into a very large number of dependent rows.

Example:

- `CustomerName` is denormalized into every order read row
- one customer owns millions of orders
- updating that customer name now requires rewriting millions of read-model rows if fully-current semantics are required

That is where synchronous materialized joins become a bad fit.

### Practical Failure Modes

Read models usually stop being attractive when one or more of these are true:

- parent-table attributes change frequently
- one parent row fans out to a very large child set
- the caller requires fully current joined values everywhere
- many query dimensions are optional and ad hoc
- write latency is more important than read latency

### How To Avoid The Worst Cases

Use one of these mitigation patterns:

- keep denormalized fields limited to stable attributes
- use snapshot semantics for descriptive parent fields
- materialize aggregates but not every joined detail row
- keep volatile parent data live and fetch it with one extra lookup
- move projection refresh to eventual/asynchronous maintenance when exact immediate freshness is not required
- keep the workload on live SQL when the fan-out rewrite cost is too high

### Full Rebuild Guidance

A full rebuild should be treated as an operational tool, not the normal write path.

Use full rebuild only for:

- initial backfill
- schema evolution
- repair after a bug
- validation and consistency recovery

Do not design the system around full rematerialization on ordinary data changes.

## Rebuild And Validation

Implement a full rebuild path for both collections.

Rebuild rules:

1. scan `orders`
2. join each order to `customers`
3. rewrite `order_customer_read`
4. rewrite `customer_day_sales`
5. swap into service only after validation completes

Validation checks:

- materialized point read matches the SQL join result
- materialized customer-range read matches the SQL join result
- materialized daily summary matches the SQL aggregate result
- insert, update, and delete all keep the read model correct
- rollback leaves the read model unchanged

## When To Keep SQL Instead

Do not materialize the query if:

- the join shape is not stable
- the caller needs arbitrary filters
- the parent-table attributes change often and must be reflected everywhere immediately
- the result set is rarely read

For those cases, keep using SQL and let the planner pick the best available path.

## Recommended V1 Scope

If this is implemented, keep v1 narrow:

1. build `order_customer_read`
2. build `customer_day_sales`
3. support the exact parameterized paths defined in this document
4. use snapshot semantics for denormalized customer fields
5. keep everything else on SQL

That gives a clear first cut with measurable performance upside and bounded write-side complexity.
