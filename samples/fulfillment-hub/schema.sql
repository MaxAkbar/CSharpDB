-- Fulfillment Hub
-- Fixed snapshot dated 2026-04-24.
-- Highlights: tables, views, indexes, triggers, procedures, saved queries,
-- forms, reports, pipelines, collections, and full-text search layered in
-- by the runnable sample seeder.

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    customer_code TEXT COLLATE NOCASE NOT NULL,
    name TEXT COLLATE NOCASE NOT NULL,
    tier TEXT NOT NULL,
    region TEXT NOT NULL,
    email TEXT COLLATE NOCASE NOT NULL,
    phone TEXT,
    is_priority INTEGER NOT NULL
);

CREATE TABLE suppliers (
    id INTEGER PRIMARY KEY,
    supplier_code TEXT COLLATE NOCASE NOT NULL,
    name TEXT COLLATE NOCASE NOT NULL,
    contact_name TEXT NOT NULL,
    email TEXT COLLATE NOCASE NOT NULL,
    lead_time_days INTEGER NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE warehouses (
    id INTEGER PRIMARY KEY,
    warehouse_code TEXT COLLATE NOCASE NOT NULL,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    is_default INTEGER NOT NULL
);

CREATE TABLE carriers (
    id INTEGER PRIMARY KEY,
    carrier_code TEXT COLLATE NOCASE NOT NULL,
    name TEXT NOT NULL,
    service_level TEXT NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT COLLATE NOCASE NOT NULL,
    name TEXT COLLATE NOCASE NOT NULL,
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    preferred_supplier_id INTEGER NOT NULL REFERENCES suppliers(id),
    reorder_point INTEGER NOT NULL,
    standard_cost REAL NOT NULL,
    sale_price REAL NOT NULL,
    is_active INTEGER NOT NULL
);

CREATE TABLE inventory_positions (
    id INTEGER PRIMARY KEY,
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    on_hand_qty INTEGER NOT NULL,
    allocated_qty INTEGER NOT NULL,
    inbound_qty INTEGER NOT NULL,
    cycle_count_date TEXT NOT NULL
);

CREATE TABLE purchase_orders (
    id INTEGER PRIMARY KEY,
    po_number TEXT COLLATE NOCASE NOT NULL,
    supplier_id INTEGER NOT NULL REFERENCES suppliers(id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    ordered_date TEXT NOT NULL,
    expected_date TEXT NOT NULL,
    status TEXT NOT NULL,
    buyer_name TEXT NOT NULL,
    priority_receiving INTEGER NOT NULL,
    notes TEXT
);

CREATE TABLE purchase_order_lines (
    id INTEGER PRIMARY KEY IDENTITY,
    purchase_order_id INTEGER NOT NULL REFERENCES purchase_orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    ordered_qty INTEGER NOT NULL,
    received_qty INTEGER NOT NULL,
    unit_cost REAL NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    order_number TEXT COLLATE NOCASE NOT NULL,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    order_date TEXT NOT NULL,
    required_ship_date TEXT NOT NULL,
    status TEXT NOT NULL,
    channel TEXT NOT NULL,
    priority_code TEXT NOT NULL,
    is_expedited INTEGER NOT NULL,
    total_amount REAL NOT NULL,
    notes TEXT
);

CREATE TABLE order_lines (
    id INTEGER PRIMARY KEY IDENTITY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id),
    ordered_qty INTEGER NOT NULL,
    allocated_qty INTEGER NOT NULL,
    shipped_qty INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    line_total REAL NOT NULL
);

CREATE TABLE shipments (
    id INTEGER PRIMARY KEY,
    shipment_number TEXT COLLATE NOCASE NOT NULL,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    carrier_id INTEGER NOT NULL REFERENCES carriers(id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    shipped_date TEXT NOT NULL,
    status TEXT NOT NULL,
    tracking_number TEXT,
    picked_by TEXT NOT NULL,
    packed_by TEXT NOT NULL
);

CREATE TABLE shipment_lines (
    id INTEGER PRIMARY KEY IDENTITY,
    shipment_id INTEGER NOT NULL REFERENCES shipments(id) ON DELETE CASCADE,
    order_line_id INTEGER NOT NULL REFERENCES order_lines(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity_shipped INTEGER NOT NULL,
    line_total REAL NOT NULL
);

CREATE TABLE returns (
    id INTEGER PRIMARY KEY,
    return_number TEXT COLLATE NOCASE NOT NULL,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    requested_date TEXT NOT NULL,
    received_date TEXT,
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    disposition TEXT NOT NULL,
    requires_qc INTEGER NOT NULL,
    notes TEXT
);

CREATE TABLE ops_playbooks (
    id INTEGER PRIMARY KEY,
    title TEXT COLLATE NOCASE NOT NULL,
    body TEXT NOT NULL,
    tags TEXT NOT NULL,
    owner_team TEXT NOT NULL,
    updated_date TEXT NOT NULL
);

CREATE TABLE ops_events (
    id INTEGER PRIMARY KEY IDENTITY,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE TABLE supplier_receipts_stage (
    id INTEGER PRIMARY KEY IDENTITY,
    receipt_batch TEXT NOT NULL,
    supplier_code TEXT NOT NULL,
    warehouse_code TEXT NOT NULL,
    sku TEXT NOT NULL,
    received_qty INTEGER NOT NULL,
    unit_cost REAL NOT NULL,
    received_date TEXT NOT NULL,
    reference_number TEXT NOT NULL,
    ingest_status TEXT NOT NULL
);

CREATE TABLE marketplace_orders_stage (
    id INTEGER PRIMARY KEY IDENTITY,
    external_order_id TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    warehouse_code TEXT NOT NULL,
    order_date TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    sales_channel TEXT NOT NULL,
    import_status TEXT NOT NULL,
    channel_snapshot TEXT
);

CREATE UNIQUE INDEX idx_customers_code_unique ON customers (customer_code);
CREATE UNIQUE INDEX idx_customers_email_unique ON customers (email);
CREATE UNIQUE INDEX idx_suppliers_code_unique ON suppliers (supplier_code);
CREATE UNIQUE INDEX idx_warehouses_code_unique ON warehouses (warehouse_code);
CREATE UNIQUE INDEX idx_carriers_code_unique ON carriers (carrier_code);
CREATE UNIQUE INDEX idx_products_sku_unique ON products (sku);
CREATE UNIQUE INDEX idx_inventory_positions_unique ON inventory_positions (warehouse_id, product_id);
CREATE UNIQUE INDEX idx_purchase_orders_number_unique ON purchase_orders (po_number);
CREATE UNIQUE INDEX idx_orders_number_unique ON orders (order_number);
CREATE UNIQUE INDEX idx_order_lines_order_line_unique ON order_lines (order_id, line_number);
CREATE UNIQUE INDEX idx_shipments_number_unique ON shipments (shipment_number);
CREATE UNIQUE INDEX idx_returns_number_unique ON returns (return_number);

CREATE INDEX idx_products_supplier_category ON products (preferred_supplier_id, category);
CREATE INDEX idx_inventory_positions_watch ON inventory_positions (warehouse_id, product_id, on_hand_qty, allocated_qty, inbound_qty);
CREATE INDEX idx_purchase_orders_status_expected ON purchase_orders (status, expected_date);
CREATE INDEX idx_purchase_order_lines_purchase_order ON purchase_order_lines (purchase_order_id, product_id);
CREATE INDEX idx_orders_status_required ON orders (status, required_ship_date, priority_code);
CREATE INDEX idx_orders_customer_date ON orders (customer_id, order_date);
CREATE INDEX idx_order_lines_product ON order_lines (product_id, order_id);
CREATE INDEX idx_shipments_order_status ON shipments (order_id, status, shipped_date);
CREATE INDEX idx_returns_status_requested ON returns (status, requested_date);
CREATE INDEX idx_ops_events_entity_date ON ops_events (entity_type, entity_id, event_date);
CREATE INDEX idx_supplier_receipts_stage_lookup ON supplier_receipts_stage (receipt_batch, sku, reference_number);
CREATE INDEX idx_marketplace_orders_stage_lookup ON marketplace_orders_stage (external_order_id, sku);

CREATE VIEW order_fulfillment_board AS
SELECT
    o.id AS order_id,
    o.order_number,
    c.customer_code,
    c.name AS customer_name,
    w.warehouse_code,
    o.order_date,
    o.required_ship_date,
    o.status AS order_status,
    o.channel,
    o.priority_code,
    o.is_expedited,
    o.total_amount,
    o.notes
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
INNER JOIN warehouses w ON w.id = o.warehouse_id;

CREATE VIEW low_stock_watch AS
SELECT
    ip.id AS inventory_position_id,
    w.id AS warehouse_id,
    w.warehouse_code,
    w.name AS warehouse_name,
    p.id AS product_id,
    p.sku,
    p.name AS product_name,
    p.category,
    s.name AS supplier_name,
    ip.on_hand_qty,
    ip.allocated_qty,
    ip.inbound_qty,
    ip.on_hand_qty - ip.allocated_qty AS available_qty,
    p.reorder_point,
    p.reorder_point - ip.on_hand_qty + ip.allocated_qty - ip.inbound_qty AS shortage_qty,
    ip.cycle_count_date
FROM inventory_positions ip
INNER JOIN warehouses w ON w.id = ip.warehouse_id
INNER JOIN products p ON p.id = ip.product_id
INNER JOIN suppliers s ON s.id = p.preferred_supplier_id;

CREATE VIEW purchase_order_receiving_board AS
SELECT
    po.id AS purchase_order_id,
    po.po_number,
    s.name AS supplier_name,
    w.warehouse_code,
    po.ordered_date,
    po.expected_date,
    po.status AS po_status,
    po.priority_receiving,
    p.sku,
    p.name AS product_name,
    pol.ordered_qty,
    pol.received_qty,
    pol.ordered_qty - pol.received_qty AS outstanding_qty,
    pol.unit_cost
FROM purchase_orders po
INNER JOIN suppliers s ON s.id = po.supplier_id
INNER JOIN warehouses w ON w.id = po.warehouse_id
INNER JOIN purchase_order_lines pol ON pol.purchase_order_id = po.id
INNER JOIN products p ON p.id = pol.product_id;

CREATE VIEW shipment_manifest_report_source AS
SELECT
    sh.id AS shipment_id,
    sh.shipment_number,
    sh.status AS shipment_status,
    sh.shipped_date,
    sh.tracking_number,
    c2.name AS carrier_name,
    o.order_number,
    cu.name AS customer_name,
    w.warehouse_code,
    p.sku,
    p.name AS product_name,
    sl.quantity_shipped,
    sl.line_total
FROM shipments sh
INNER JOIN carriers c2 ON c2.id = sh.carrier_id
INNER JOIN orders o ON o.id = sh.order_id
INNER JOIN customers cu ON cu.id = o.customer_id
INNER JOIN warehouses w ON w.id = sh.warehouse_id
INNER JOIN shipment_lines sl ON sl.shipment_id = sh.id
INNER JOIN products p ON p.id = sl.product_id;

CREATE VIEW return_queue AS
SELECT
    r.id AS return_id,
    r.return_number,
    r.status AS return_status,
    r.requested_date,
    r.received_date,
    r.reason,
    r.disposition,
    r.requires_qc,
    o.order_number,
    cu.name AS customer_name,
    p.sku,
    p.name AS product_name,
    w.warehouse_code,
    r.quantity,
    r.notes
FROM returns r
INNER JOIN orders o ON o.id = r.order_id
INNER JOIN customers cu ON cu.id = o.customer_id
INNER JOIN products p ON p.id = r.product_id
INNER JOIN warehouses w ON w.id = r.warehouse_id;

CREATE TRIGGER trg_orders_insert_event AFTER INSERT ON orders
BEGIN
    INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('order', NEW.id, 'created', NEW.order_date, 'order-router', 'Sales order created.');
END;

CREATE TRIGGER trg_purchase_orders_status_event AFTER UPDATE ON purchase_orders
BEGIN
    INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('purchase_order', NEW.id, 'status_changed', NEW.expected_date, 'receiving-console', 'Purchase order status updated.');
END;

CREATE TRIGGER trg_shipments_insert_event AFTER INSERT ON shipments
BEGIN
    INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('shipment', NEW.id, 'created', NEW.shipped_date, NEW.picked_by, 'Shipment record created.');
END;

CREATE TRIGGER trg_returns_insert_event AFTER INSERT ON returns
BEGIN
    INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('return', NEW.id, 'requested', NEW.requested_date, 'returns-portal', 'Return request recorded.');
END;

INSERT INTO customers VALUES (1001, 'ALP-001', 'Alpine Outdoor', 'Enterprise', 'West', 'ops@alpineoutdoor.example', '206-555-0100', 1);
INSERT INTO customers VALUES (1002, 'BEA-014', 'Beacon Health', 'Strategic', 'Mountain', 'supply@beaconhealth.example', '303-555-0140', 1);
INSERT INTO customers VALUES (1003, 'CED-222', 'Cedar Robotics', 'Growth', 'Central', 'warehouse@cedarrobotics.example', '312-555-0122', 0);
INSERT INTO customers VALUES (1004, 'DRI-090', 'Drift Coffee', 'Growth', 'West', 'buyers@driftcoffee.example', '415-555-0190', 0);
INSERT INTO customers VALUES (1005, 'ELM-119', 'Elm Learning', 'Midmarket', 'Southeast', 'campus@elmlearning.example', '404-555-0119', 0);

INSERT INTO suppliers VALUES (201, 'NOR-01', 'North Ridge Supply', 'Tara Knox', 'tara@northridge.example', 5, 'active');
INSERT INTO suppliers VALUES (202, 'PAC-07', 'Pacific Assembly', 'Jon Reeve', 'jon@pacificassembly.example', 7, 'active');
INSERT INTO suppliers VALUES (203, 'GLA-10', 'Glacier Print Systems', 'Monica Yu', 'monica@glacierprint.example', 6, 'active');
INSERT INTO suppliers VALUES (204, 'DEL-11', 'Delta Safety Goods', 'Luis Vega', 'luis@deltasafety.example', 4, 'active');

INSERT INTO warehouses VALUES (1, 'SEA-FC', 'Seattle Fulfillment Center', 'Seattle', 'West', 1);
INSERT INTO warehouses VALUES (2, 'DEN-DC', 'Denver Distribution Center', 'Denver', 'Mountain', 0);
INSERT INTO warehouses VALUES (3, 'ATL-RTN', 'Atlanta Returns Hub', 'Atlanta', 'Southeast', 0);

INSERT INTO carriers VALUES (1, 'UPS', 'United Parcel Service', 'Ground');
INSERT INTO carriers VALUES (2, 'FDX', 'FedEx', '2Day');
INSERT INTO carriers VALUES (3, 'RLF', 'Regional Line Freight', 'LTL');

INSERT INTO products VALUES (501, 'TRJ-100', 'Trail Jacket', 'Apparel', 'Weatherproof trail jacket with packable hood.', 201, 20, 48.50, 89.00, 1);
INSERT INTO products VALUES (502, 'BOT-220', 'Insulated Bottle', 'Gear', 'Double-wall insulated bottle for route drivers.', 201, 25, 11.75, 24.00, 1);
INSERT INTO products VALUES (503, 'RFS-310', 'RFID Scanner', 'Hardware', 'Handheld RFID scanner for pick-pack workflows.', 202, 10, 189.00, 270.00, 1);
INSERT INTO products VALUES (504, 'TAP-410', 'Packing Tape', 'Consumables', 'Heavy-duty tape roll used in every outbound pack station.', 201, 40, 2.30, 6.50, 1);
INSERT INTO products VALUES (505, 'MPR-510', 'Mobile Printer', 'Hardware', 'Portable printer for mobile receiving and cycle counts.', 203, 8, 229.00, 255.00, 1);
INSERT INTO products VALUES (506, 'HZL-610', 'Hazmat Label Kit', 'Safety', 'Hazmat label starter kit for regulated outbound shipments.', 204, 6, 6.25, 10.50, 1);
INSERT INTO products VALUES (507, 'BIN-710', 'Return Bin Large', 'Returns', 'Large bin used in return inspection and refurbishment lanes.', 204, 10, 12.00, 18.80, 1);
INSERT INTO products VALUES (508, 'CCS-810', 'Cold Chain Sensor', 'IoT', 'Temperature sensor used in cold chain shipments and alerts.', 202, 5, 38.50, 48.30, 1);

INSERT INTO inventory_positions VALUES (1, 1, 501, 18, 6, 18, '2026-04-18');
INSERT INTO inventory_positions VALUES (2, 1, 502, 64, 8, 0, '2026-04-17');
INSERT INTO inventory_positions VALUES (3, 1, 503, 9, 4, 10, '2026-04-19');
INSERT INTO inventory_positions VALUES (4, 1, 504, 140, 10, 50, '2026-04-16');
INSERT INTO inventory_positions VALUES (5, 2, 501, 7, 2, 10, '2026-04-15');
INSERT INTO inventory_positions VALUES (6, 2, 505, 4, 1, 8, '2026-04-18');
INSERT INTO inventory_positions VALUES (7, 2, 506, 2, 0, 20, '2026-04-14');
INSERT INTO inventory_positions VALUES (8, 2, 507, 21, 3, 0, '2026-04-18');
INSERT INTO inventory_positions VALUES (9, 3, 503, 5, 0, 3, '2026-04-20');
INSERT INTO inventory_positions VALUES (10, 3, 507, 44, 2, 25, '2026-04-19');
INSERT INTO inventory_positions VALUES (11, 3, 508, 3, 1, 6, '2026-04-21');

INSERT INTO purchase_orders VALUES (9001, 'PO-9001', 201, 1, '2026-04-18', '2026-04-25', 'open', 'Nina Shah', 1, 'Expedite trail jacket replenishment before weekend promotion.');
INSERT INTO purchase_orders VALUES (9002, 'PO-9002', 203, 2, '2026-04-16', '2026-04-23', 'open', 'Marco Bell', 0, 'Printer allocation for Denver receiving team.');
INSERT INTO purchase_orders VALUES (9003, 'PO-9003', 204, 3, '2026-04-20', '2026-04-28', 'open', 'Inez Park', 1, 'Returns lane safety restock.');

INSERT INTO purchase_order_lines VALUES (1, 9001, 501, 18, 0, 48.50);
INSERT INTO purchase_order_lines VALUES (2, 9001, 503, 10, 0, 189.00);
INSERT INTO purchase_order_lines VALUES (3, 9002, 505, 8, 3, 229.00);
INSERT INTO purchase_order_lines VALUES (4, 9002, 508, 6, 2, 38.50);
INSERT INTO purchase_order_lines VALUES (5, 9003, 506, 20, 0, 6.25);
INSERT INTO purchase_order_lines VALUES (6, 9003, 507, 25, 0, 12.00);

INSERT INTO orders VALUES (7001, 'SO-7001', 1001, 1, '2026-04-21', '2026-04-24', 'allocated', 'web', 'high', 1, 626.00, 'Priority launch order for west coast event inventory.');
INSERT INTO orders VALUES (7002, 'SO-7002', 1002, 2, '2026-04-22', '2026-04-25', 'released', 'edi', 'medium', 0, 496.50, 'Hospital replenishment order awaiting shipment creation.');
INSERT INTO orders VALUES (7003, 'SO-7003', 1004, 1, '2026-04-20', '2026-04-22', 'shipped', 'marketplace', 'high', 1, 458.00, 'Marketplace order that already shipped from Seattle.');
INSERT INTO orders VALUES (7004, 'SO-7004', 1003, 3, '2026-04-23', '2026-04-27', 'picking', 'rest-api', 'medium', 0, 307.60, 'Robotics demo order being picked in Atlanta.');
INSERT INTO orders VALUES (7005, 'SO-7005', 1005, 1, '2026-04-24', '2026-04-29', 'released', 'grpc', 'low', 0, 457.00, 'Campus store replenishment queued for allocation.');

INSERT INTO order_lines VALUES (1, 7001, 1, 501, 4, 4, 0, 89.00, 356.00);
INSERT INTO order_lines VALUES (2, 7001, 2, 503, 1, 1, 0, 270.00, 270.00);
INSERT INTO order_lines VALUES (3, 7002, 1, 505, 1, 1, 0, 255.00, 255.00);
INSERT INTO order_lines VALUES (4, 7002, 2, 508, 5, 0, 0, 48.30, 241.50);
INSERT INTO order_lines VALUES (5, 7003, 1, 502, 8, 8, 8, 24.00, 192.00);
INSERT INTO order_lines VALUES (6, 7003, 2, 504, 12, 12, 12, 6.50, 78.00);
INSERT INTO order_lines VALUES (7, 7003, 3, 507, 10, 10, 10, 18.80, 188.00);
INSERT INTO order_lines VALUES (8, 7004, 1, 503, 1, 1, 0, 270.00, 270.00);
INSERT INTO order_lines VALUES (9, 7004, 2, 507, 2, 2, 0, 18.80, 37.60);
INSERT INTO order_lines VALUES (10, 7005, 1, 502, 10, 0, 0, 24.00, 240.00);
INSERT INTO order_lines VALUES (11, 7005, 2, 504, 6, 0, 0, 6.50, 39.00);
INSERT INTO order_lines VALUES (12, 7005, 3, 501, 2, 0, 0, 89.00, 178.00);

INSERT INTO shipments VALUES (8001, 'SHP-8001', 7003, 1, 1, '2026-04-22', 'shipped', '1Z999AA10123456784', 'Priya Mendez', 'Jordan Hale');

INSERT INTO shipment_lines VALUES (1, 8001, 5, 502, 8, 192.00);
INSERT INTO shipment_lines VALUES (2, 8001, 6, 504, 12, 78.00);
INSERT INTO shipment_lines VALUES (3, 8001, 7, 507, 10, 188.00);

INSERT INTO returns VALUES (8501, 'RMA-8501', 7003, 502, 3, '2026-04-23', '2026-04-24', 2, 'received', 'Damaged insulation cap', 'inspect', 1, 'Inspect before restock because the cap seal failed in transit.');
INSERT INTO returns VALUES (8502, 'RMA-8502', 7001, 501, 1, '2026-04-24', NULL, 1, 'requested', 'Size swap', 'exchange', 0, 'Customer requested a different size before shipment cut-off.');

INSERT INTO ops_playbooks VALUES (1, 'Hazmat Partial Receipt Runbook', 'When a hazmat label kit receipt lands short, stage the pallet, log the partial receipt, and notify the safety lead before allocation resumes.', 'receiving,hazmat,partial', 'Warehouse Ops', '2026-04-18');
INSERT INTO ops_playbooks VALUES (2, 'Cold Chain Sensor Escalation', 'If a cold chain sensor arrives below reorder coverage, move the open order queue to manual review and issue a supplier escalation within one hour.', 'cold-chain,inventory,escalation', 'Inventory Control', '2026-04-20');
INSERT INTO ops_playbooks VALUES (3, 'Returns QC Exception Flow', 'Use the return intake form, assign inspection status, and restock only after QC clears any damaged bottle or electronics return.', 'returns,qc,restock', 'Returns Ops', '2026-04-22');

INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
VALUES ('inventory_position', 7, 'cycle_count_alert', '2026-04-24', 'inventory-bot', 'Hazmat labels fell below reorder threshold during morning audit.');

INSERT INTO ops_events (entity_type, entity_id, event_type, event_date, actor_name, details)
VALUES ('inventory_position', 11, 'cold_chain_watch', '2026-04-24', 'inventory-bot', 'Cold chain sensors require transfer or rush replenishment.');

UPDATE purchase_orders
SET status = 'partial'
WHERE id = 9002;
