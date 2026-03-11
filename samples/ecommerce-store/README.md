# Northwind Electronics

Northwind Electronics is a compact online retail sample for exercising the CSharpDB basics against a familiar commerce domain: customers, products, orders, reviews, and shipping addresses.

## Files

- `schema.sql` - schema plus seed data
- `procedures.json` - `GetCustomerOrderHistory`, `AdjustProductStock`

## What It Showcases

- Straightforward relational joins across customers, catalog, orders, and line items
- Secondary indexes for common lookup paths
- Read-oriented views: `order_summary`, `product_catalog`
- Trigger-driven inventory updates via `trg_update_stock`
- Procedure-backed read and write flows for the Admin UI or API

## Good Starting Points

- `SELECT * FROM order_summary ORDER BY order_date DESC;`
- `SELECT * FROM product_catalog ORDER BY price DESC;`
- `EXEC GetCustomerOrderHistory customerId=1;`
- `EXEC AdjustProductStock productId=10, delta=-3;`

## Load

```text
csdb> .read samples/ecommerce-store/schema.sql
```
