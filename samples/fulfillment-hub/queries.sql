-- Fulfillment Hub workbook
-- Read-only exploration queries for the seeded operational snapshot.

SELECT
    order_number,
    customer_name,
    warehouse_code,
    order_status,
    priority_code,
    total_amount
FROM order_fulfillment_board
ORDER BY required_ship_date, priority_code DESC, order_number;

SELECT
    warehouse_code,
    sku,
    product_name,
    available_qty,
    inbound_qty,
    reorder_point,
    shortage_qty
FROM low_stock_watch
WHERE shortage_qty > 0
ORDER BY shortage_qty DESC, warehouse_code, sku;

SELECT
    po_number,
    supplier_name,
    warehouse_code,
    expected_date,
    po_status,
    sku,
    ordered_qty,
    received_qty,
    outstanding_qty
FROM purchase_order_receiving_board
WHERE outstanding_qty > 0
ORDER BY expected_date, po_number, sku;

SELECT
    shipment_number,
    shipment_status,
    shipped_date,
    carrier_name,
    order_number,
    customer_name,
    sku,
    quantity_shipped
FROM shipment_manifest_report_source
ORDER BY shipment_number, sku;

SELECT
    return_number,
    return_status,
    requested_date,
    customer_name,
    sku,
    product_name,
    warehouse_code,
    quantity,
    reason
FROM return_queue
ORDER BY requested_date DESC, return_number;

SELECT
    entity_type,
    entity_id,
    event_type,
    event_date,
    actor_name,
    details
FROM ops_events
ORDER BY id DESC
LIMIT 20;

SELECT
    table_name,
    row_count,
    row_count_is_exact,
    has_stale_columns
FROM sys.table_stats
ORDER BY row_count DESC, table_name;

SELECT
    object_name,
    object_type
FROM sys.objects
ORDER BY object_type, object_name;

-- Pipeline-run target tables:
SELECT *
FROM supplier_receipts_stage
ORDER BY id;

SELECT *
FROM marketplace_orders_stage
ORDER BY id;
