# Procurement Analytics Lab

Procurement Analytics Lab is a focused workbook sample for the newer SQL surface area in CSharpDB: set operations, scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, `ANALYZE`, and the persisted `sys.table_stats` / `sys.column_stats` catalogs.

## Files

- `schema.sql` - procurement schema plus seed data
- `procedures.json` - `RefreshProcurementStats`, `GetSupplierActionQueue`, `GetWarehouseReorderWatch`
- `queries.sql` - read-only workbook for query expansion and statistics inspection

## What It Showcases

- Set operations with `UNION`, `INTERSECT`, and `EXCEPT`
- Scalar subqueries in projection, views, and correlated filters
- `IN (SELECT ...)` and `EXISTS (SELECT ...)` against operational data
- `ANALYZE`, `sys.table_stats`, `sys.column_stats`, and a commented stale-stats demo
- Compact supplier, warehouse, inventory, purchase-order, and incident reporting shapes

## Good Starting Points

- `csdb> .read samples/procurement-analytics/schema.sql`
- `csdb> .read samples/procurement-analytics/queries.sql`
- `EXEC RefreshProcurementStats;`
- `EXEC GetSupplierActionQueue;`
- `EXEC GetWarehouseReorderWatch warehouseId=3;`

## Developer Notes

- `queries.sql` starts with `ANALYZE;` because `sys.column_stats` is only populated after a refresh.
- The workbook keeps all write examples commented out so you can run it repeatedly on the same local database.
- The stale-stats demo at the end of `queries.sql` is intended for disposable local runs where mutating the sample is acceptable.
