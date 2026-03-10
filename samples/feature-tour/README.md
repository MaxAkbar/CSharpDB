# Northstar Field Services

Northstar Field Services is the richest sample in the repo: a fictitious multi-region field service company snapshot with customers, sites, contracts, dispatch, inventory, and billing. Use it when you want a fuller end-to-end example instead of a small vertical slice.

## Files

- `schema.sql` - full operations schema plus seed data
- `procedures.json` - `GetCustomerOperationsSnapshot`, `DispatchWorkOrder`, `ReceiveInventory`
- `queries.sql` - read-only workbook for dispatch, inventory, billing, CTEs, catalogs, and `TEXT(...)`

## What It Showcases

- A larger operational schema with customer hierarchies, work orders, warehouses, and invoices
- Composite indexes, views, triggers, and IDENTITY-based audit/event tables
- Reporting views: `ticket_command_center`, `inventory_position`, `billing_snapshot`
- Server-side `TEXT(...)` filtering against numeric identifiers
- Procedure-driven operational flows for dispatching work and receiving inventory

## Good Starting Points

- `csdb> .read samples/feature-tour/schema.sql`
- `csdb> .read samples/feature-tour/queries.sql`
- `EXEC GetCustomerOperationsSnapshot customerId=2;`
- `EXEC DispatchWorkOrder workOrderId=9002, technicianId=504, scheduledDate='2026-03-14', newStatus='scheduled';`
- `EXEC ReceiveInventory warehouseId=803, partId=10002, quantity=6, movedDate='2026-03-11', referenceCode='PO-4201';`

## Developer Notes

- The sample is a fixed snapshot dated `2026-03-10`, so trigger-generated audit rows also use that snapshot date.
- `queries.sql` is intentionally read-only; procedure calls are kept as commented examples there and spelled out above.
