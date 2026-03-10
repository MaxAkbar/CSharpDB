-- Query workbook for Northstar Field Services (feature-tour/schema.sql)
-- Load the sample first, then run these queries in the CLI, Admin Query tab, or via CSharpDB.Client.

-- 1. Dispatch board / command center.
SELECT *
FROM ticket_command_center
ORDER BY severity_score DESC, ticket_id;

-- 2. Inventory position by warehouse and part.
SELECT *
FROM inventory_position
ORDER BY on_hand_qty ASC, warehouse_code, part_id;

-- 3. Server-side text filtering across numeric site IDs.
SELECT ticket_id, customer_name, site_code, summary
FROM ticket_command_center
WHERE TEXT(site_id) LIKE '%02%'
ORDER BY site_id, ticket_id;

-- 4. Aggregate + HAVING over warehouse stock.
SELECT warehouse_code, COUNT(*) AS stocked_parts, SUM(on_hand_qty) AS on_hand_units
FROM inventory_position
GROUP BY warehouse_code
HAVING SUM(on_hand_qty) >= 20
ORDER BY on_hand_units DESC;

-- 5. CTE for current technician workload.
WITH technician_load AS (
    SELECT tech.name AS technician_name, COUNT(w.id) AS active_orders, SUM(w.labor_hours) AS scheduled_hours
    FROM technicians tech
    INNER JOIN work_orders w ON w.technician_id = tech.id
    WHERE w.status <> 'completed'
    GROUP BY tech.name
)
SELECT *
FROM technician_load
ORDER BY active_orders DESC, scheduled_hours DESC;

-- 6. Billing rows that still need action.
SELECT customer_name, contract_code, invoice_id, invoice_status, total, due_date
FROM billing_snapshot
WHERE invoice_status <> 'paid'
ORDER BY customer_name, invoice_id;

-- 7. System catalog inspection.
SELECT object_type, object_name
FROM sys.objects
ORDER BY object_type, object_name;

-- 8. Procedure calls for Admin Query tab examples.
-- EXEC GetCustomerOperationsSnapshot customerId=2;
-- EXEC DispatchWorkOrder workOrderId=9002, technicianId=504, scheduledDate='2026-03-14', newStatus='scheduled';
-- EXEC ReceiveInventory warehouseId=803, partId=10002, quantity=6, movedDate='2026-03-11', referenceCode='PO-4201';
