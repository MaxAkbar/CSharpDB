-- Query workbook for Procurement Analytics Lab (procurement-analytics/schema.sql)
-- Load the sample first, then run these queries in the CLI, Admin Query tab, or via CSharpDB.Client.

-- 1. Refresh exact table and column stats for this snapshot.
ANALYZE;

-- 2. Inspect persisted table row counts and stale markers.
SELECT table_name, row_count, has_stale_columns
FROM sys.table_stats
ORDER BY row_count DESC, table_name;

-- 3. Inspect selected column stats after ANALYZE.
SELECT table_name, column_name, distinct_count, non_null_count, min_value, max_value, is_stale
FROM sys.column_stats
WHERE table_name IN ('suppliers', 'products', 'purchase_orders', 'quality_incidents')
  AND column_name IN ('region', 'category', 'status', 'total_amount')
ORDER BY table_name, column_name;

-- 4. View-backed supplier scorecard using scalar subqueries.
SELECT *
FROM supplier_scorecard
ORDER BY open_incident_count DESC, open_po_count DESC, supplier_name;

-- 5. Scalar subqueries in projection.
SELECT p.id, p.sku, p.name, p.category, p.unit_cost,
       (SELECT s.name FROM suppliers s WHERE s.id = p.preferred_supplier_id) AS preferred_supplier,
       (SELECT s.max_lead_days FROM suppliers s WHERE s.id = p.preferred_supplier_id) AS max_lead_days
FROM products p
ORDER BY p.category, p.name;

-- 6. Correlated scalar subquery filter: products priced above their category average.
SELECT p.id, p.name, p.category, p.unit_cost
FROM products p
WHERE p.unit_cost > (
    SELECT AVG(p2.unit_cost)
    FROM products p2
    WHERE p2.category = p.category
)
ORDER BY p.category, p.unit_cost DESC;

-- 7. IN (SELECT ...): products tied to suppliers with open incidents.
SELECT p.sku, p.name, p.category
FROM products p
WHERE p.preferred_supplier_id IN (
    SELECT supplier_id
    FROM quality_incidents
    WHERE status <> 'closed'
)
ORDER BY p.category, p.name;

-- 8. EXISTS (SELECT ...): suppliers with active purchase-order pipeline.
SELECT s.id, s.name, s.region
FROM suppliers s
WHERE EXISTS (
    SELECT 1
    FROM purchase_orders po
    WHERE po.supplier_id = s.id
      AND po.status IN ('open', 'approved', 'partial')
)
ORDER BY s.name;

-- 9. INTERSECT: suppliers that have both active purchase orders and open incidents.
SELECT supplier_id
FROM purchase_orders
WHERE status IN ('open', 'approved', 'partial')
INTERSECT
SELECT supplier_id
FROM quality_incidents
WHERE status <> 'closed'
ORDER BY supplier_id;

-- 10. EXCEPT: active suppliers without a live purchase-order pipeline.
SELECT id, name
FROM suppliers
WHERE status = 'active'
EXCEPT
SELECT s.id, s.name
FROM suppliers s
INNER JOIN purchase_orders po ON po.supplier_id = s.id
WHERE po.status IN ('open', 'approved', 'partial')
ORDER BY 2;

-- 11. UNION: combine incident queue and low-stock reorder queue.
SELECT 'incident' AS queue_type, s.name AS supplier_name, qi.summary AS detail
FROM suppliers s
INNER JOIN quality_incidents qi ON qi.supplier_id = s.id
WHERE qi.status <> 'closed'
UNION
SELECT 'reorder' AS queue_type, s.name AS supplier_name, p.name AS detail
FROM suppliers s
INNER JOIN products p ON p.preferred_supplier_id = s.id
INNER JOIN stock_levels sl ON sl.product_id = p.id
WHERE sl.on_hand_qty < sl.reorder_point
ORDER BY queue_type, supplier_name, detail;

-- 12. View-backed reorder watch.
SELECT *
FROM warehouse_reorder_watch
ORDER BY warehouse_code, product_name;

-- 13. Procedure calls for Admin Query tab examples.
-- EXEC RefreshProcurementStats;
-- EXEC GetSupplierActionQueue;
-- EXEC GetWarehouseReorderWatch warehouseId=3;

-- 14. Optional stale-stats demo. Run only on a disposable local copy.
-- INSERT INTO purchase_orders VALUES (5009, 6, 3, 'Riley Grant', '2026-03-12', '2026-03-19', 'open', 440.00);
-- SELECT table_name, has_stale_columns FROM sys.table_stats WHERE table_name = 'purchase_orders';
-- SELECT column_name, is_stale FROM sys.column_stats WHERE table_name = 'purchase_orders' ORDER BY ordinal_position;
-- ANALYZE purchase_orders;
