# Atlas Platform Showcase

Atlas Platform Showcase is the broadest single-database sample in the repo. It combines commercial orders, subscription renewals, support operations, knowledge-base content, inventory pressure, procedures, and optional API-only features in one cohesive snapshot.

## Files

- `schema.sql` - end-to-end schema plus seed data
- `procedures.json` - `RefreshPlatformStats`, `GetCustomer360`, `EscalateSupportTicket`, `ReceiveInventory`
- `queries.sql` - read-only workbook for joins, collations, views, triggers, subqueries, set operations, and system catalogs
- `PlatformShowcaseSample.csproj` / `Program.cs` - optional runnable demo that loads the schema, builds a full-text index, and exercises the Collection API

## What It Showcases

- Foreign keys with `ON DELETE CASCADE` plus default restrict behavior
- `COLLATE NOCASE`, unique indexes, and composite indexes
- Views for customer, support, inventory, and order reporting
- Triggers for insert, update, comment, and delete activity logging
- `IDENTITY` event rows through `platform_events`
- JOINs, `LEFT` / `RIGHT` joins, `WITH` clauses, scalar subqueries, `IN`, `EXISTS`, `UNION`, `INTERSECT`, `EXCEPT`, `DISTINCT`, and `TEXT(...)`
- `ANALYZE`, `sys.table_stats`, `sys.column_stats`, `sys.foreign_keys`, `sys.views`, `sys.triggers`, and `sys.objects`
- Procedure-driven operational flows
- Optional API-only add-ons: full-text search and typed collections

## Good Starting Points

- `csdb> .read samples/platform-showcase/schema.sql`
- `csdb> .read samples/platform-showcase/queries.sql`
- `EXEC RefreshPlatformStats;`
- `EXEC GetCustomer360 customerId=1001;`
- `EXEC EscalateSupportTicket ticketId=7003, newPriority='high', newStatus='in_progress', assignedAgentId=4001, changedBy='Ops Console', changedDate='2026-04-08', note='Escalated after finance review.';`
- `EXEC ReceiveInventory warehouseId=6003, productId=2006, quantity=3, receivedDate='2026-04-08', referenceCode='PO-4408';`
- `dotnet run --project samples/platform-showcase/PlatformShowcaseSample.csproj`

## Notes

- The SQL workbook is intentionally read-only. Mutating examples are kept as procedures or commented lines so you can re-run the workbook safely on a fresh local copy.
- The runnable demo uses the same `schema.sql`, then layers on features that are exposed through the C# engine API rather than SQL DDL, specifically full-text search and `Collection<T>`.
- The snapshot date is fixed to early April 2026 so the query workbook stays deterministic.
