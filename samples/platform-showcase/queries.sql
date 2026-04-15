-- Query workbook for Atlas Platform Showcase (platform-showcase/schema.sql)
-- Load the sample first, then run these queries in the CLI, Admin Query tab, or via CSharpDB.Client.

-- 1. Customer 360 rollup view.
SELECT *
FROM customer_360
ORDER BY lifetime_revenue DESC, customer_id;

-- 2. Support queue with customer + order context.
SELECT *
FROM support_queue
ORDER BY severity_score DESC, ticket_id;

-- 3. Inventory pressure across warehouses.
SELECT warehouse_code, sku, product_name, on_hand_qty, reserved_qty, available_qty, reorder_point
FROM inventory_watch
WHERE available_qty < reorder_point
ORDER BY available_qty ASC, warehouse_code, sku;

-- 4. NOCASE collation on contact email.
SELECT customer_id, full_name, email
FROM customer_contacts
WHERE email = 'ava.morgan@deltalearning.edu';

-- 5. NOCASE collation on SKU lookups.
SELECT id, sku, name, category, price
FROM products
WHERE sku = 'edge-gateway';

-- 6. CTE + scalar subqueries for cross-functional customer attention.
WITH customer_attention AS (
    SELECT
        c.id AS customer_id,
        c.name AS customer_name,
        (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count,
        (SELECT SUM(o.total_amount) FROM orders o WHERE o.customer_id = c.id) AS booked_revenue,
        (SELECT COUNT(*) FROM support_tickets t WHERE t.customer_id = c.id AND t.status <> 'closed') AS open_tickets
    FROM customers c
)
SELECT *
FROM customer_attention
ORDER BY open_tickets DESC, booked_revenue DESC, customer_name;

-- 7. Scalar subquery filter: products priced above their category average.
SELECT p.id, p.sku, p.name, p.category, p.price
FROM products p
WHERE p.price > (
    SELECT AVG(p2.price)
    FROM products p2
    WHERE p2.category = p.category
)
ORDER BY p.category, p.price DESC;

-- 8. IN (SELECT ...): customers with critical or high-priority open tickets.
SELECT c.id, c.name, c.segment
FROM customers c
WHERE c.id IN (
    SELECT t.customer_id
    FROM support_tickets t
    WHERE t.priority IN ('critical', 'high')
      AND t.status <> 'closed'
)
ORDER BY c.name;

-- 9. EXISTS (SELECT ...): customers with active subscription revenue and open orders.
SELECT c.id, c.name
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM subscriptions s
    WHERE s.customer_id = c.id
      AND s.status = 'active'
)
AND EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.id
      AND o.status IN ('open', 'processing')
)
ORDER BY c.name;

-- 10. RIGHT JOIN: all agents even if they currently have no active ticket row.
SELECT a.name AS agent_name, sq.ticket_id, sq.ticket_status, sq.priority
FROM support_queue sq
RIGHT JOIN support_agents a ON a.id = sq.assigned_agent_id AND sq.ticket_status <> 'closed'
ORDER BY a.name, sq.ticket_id;

-- 11. UNION: combine renewal queue and critical support queue.
SELECT 'renewal' AS queue_type, c.name AS customer_name, s.plan_code AS detail
FROM customers c
INNER JOIN subscriptions s ON s.customer_id = c.id
WHERE s.status IN ('active', 'past_due')
  AND s.renewal_date <= '2026-04-20'
UNION
SELECT 'support' AS queue_type, c.name AS customer_name, t.subject AS detail
FROM customers c
INNER JOIN support_tickets t ON t.customer_id = c.id
WHERE t.priority = 'critical'
  AND t.status <> 'closed'
ORDER BY queue_type, customer_name, detail;

-- 12. INTERSECT: customers with both active subscriptions and open support tickets.
SELECT customer_id
FROM subscriptions
WHERE status = 'active'
INTERSECT
SELECT customer_id
FROM support_tickets
WHERE status <> 'closed'
ORDER BY customer_id;

-- 13. EXCEPT: active customers without any open support ticket.
SELECT id, name
FROM customers
WHERE status = 'active'
EXCEPT
SELECT c.id, c.name
FROM customers c
INNER JOIN support_tickets t ON t.customer_id = c.id
WHERE t.status <> 'closed'
ORDER BY 2;

-- 14. DISTINCT categories currently in the support queue.
SELECT DISTINCT category
FROM support_tickets
ORDER BY category;

-- 15. TEXT(...) filtering against numeric identifiers.
SELECT ticket_id, customer_name, order_id, subject
FROM support_queue
WHERE TEXT(order_id) LIKE '%500%'
ORDER BY ticket_id;

-- 16. Refresh planner statistics for the sample.
ANALYZE;

-- 17. Inspect exact table row counts and stale markers.
SELECT table_name, row_count, row_count_is_exact, has_stale_columns
FROM sys.table_stats
ORDER BY row_count DESC, table_name;

-- 18. Inspect selected column stats.
SELECT table_name, column_name, distinct_count, non_null_count, min_value, max_value, is_stale
FROM sys.column_stats
WHERE table_name IN ('customers', 'subscriptions', 'orders', 'support_tickets', 'products')
  AND column_name IN ('region', 'status', 'priority', 'category', 'price')
ORDER BY table_name, column_name;

-- 19. System catalog inventory.
SELECT object_name, object_type, parent_table_name
FROM sys.objects
ORDER BY object_type, object_name;

-- 20. Foreign-key metadata.
SELECT constraint_name, table_name, column_name, referenced_table_name, referenced_column_name, on_delete
FROM sys.foreign_keys
ORDER BY table_name, column_name;

-- 21. Views and triggers.
SELECT *
FROM sys.views
ORDER BY view_name;

SELECT *
FROM sys.triggers
ORDER BY trigger_name;

-- 22. Procedure calls for Admin Query tab examples.
-- EXEC RefreshPlatformStats;
-- EXEC GetCustomer360 customerId=1001;
-- EXEC EscalateSupportTicket ticketId=7003, newPriority='high', newStatus='in_progress', assignedAgentId=4001, changedBy='Ops Console', changedDate='2026-04-08', note='Escalated after finance review.';
-- EXEC ReceiveInventory warehouseId=6003, productId=2006, quantity=3, receivedDate='2026-04-08', referenceCode='PO-4408';

-- 23. Optional trigger/cascade demo on a disposable local copy.
-- DELETE FROM orders WHERE id = 5007;
-- SELECT COUNT(*) FROM order_items WHERE order_id = 5007;
-- SELECT * FROM platform_events WHERE entity_type = 'order' AND entity_id = 5007 ORDER BY id DESC;
