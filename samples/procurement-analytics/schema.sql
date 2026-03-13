-- Procurement Analytics Lab
-- Fixed snapshot dated 2026-03-12.
-- Highlights: set-operation workbook support, scalar subqueries, IN/EXISTS filters,
-- ANALYZE, sys.table_stats, sys.column_stats, and view-backed query inspection.

CREATE TABLE suppliers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    tier TEXT NOT NULL,
    status TEXT NOT NULL,
    max_lead_days INTEGER NOT NULL
);

CREATE TABLE warehouses (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    region TEXT NOT NULL,
    manager TEXT NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_cost REAL NOT NULL,
    preferred_supplier_id INTEGER NOT NULL
);

CREATE TABLE stock_levels (
    id INTEGER PRIMARY KEY,
    warehouse_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    on_hand_qty INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    last_counted TEXT NOT NULL
);

CREATE TABLE purchase_orders (
    id INTEGER PRIMARY KEY,
    supplier_id INTEGER NOT NULL,
    warehouse_id INTEGER NOT NULL,
    buyer_name TEXT NOT NULL,
    order_date TEXT NOT NULL,
    expected_date TEXT NOT NULL,
    status TEXT NOT NULL,
    total_amount REAL NOT NULL
);

CREATE TABLE purchase_order_lines (
    id INTEGER PRIMARY KEY,
    purchase_order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    ordered_qty INTEGER NOT NULL,
    received_qty INTEGER NOT NULL,
    unit_price REAL NOT NULL
);

CREATE TABLE quality_incidents (
    id INTEGER PRIMARY KEY,
    supplier_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    opened_date TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NOT NULL
);

CREATE INDEX idx_products_category_supplier ON products (category, preferred_supplier_id);
CREATE INDEX idx_stock_levels_warehouse_product ON stock_levels (warehouse_id, product_id);
CREATE INDEX idx_purchase_orders_supplier_status ON purchase_orders (supplier_id, status);
CREATE INDEX idx_purchase_order_lines_po_product ON purchase_order_lines (purchase_order_id, product_id);
CREATE INDEX idx_quality_incidents_supplier_status ON quality_incidents (supplier_id, status);

CREATE VIEW supplier_scorecard AS
SELECT
    s.id AS supplier_id,
    s.name AS supplier_name,
    s.region,
    s.tier,
    s.status,
    (SELECT COUNT(*) FROM products p WHERE p.preferred_supplier_id = s.id) AS preferred_product_count,
    (SELECT COUNT(*) FROM purchase_orders po WHERE po.supplier_id = s.id AND po.status IN ('open', 'approved', 'partial')) AS open_po_count,
    (SELECT COUNT(*) FROM quality_incidents qi WHERE qi.supplier_id = s.id AND qi.status <> 'closed') AS open_incident_count
FROM suppliers s;

CREATE VIEW warehouse_reorder_watch AS
SELECT
    sl.id AS stock_level_id,
    w.code AS warehouse_code,
    p.sku,
    p.name AS product_name,
    sl.on_hand_qty,
    sl.reorder_point,
    (SELECT s.name FROM suppliers s WHERE s.id = p.preferred_supplier_id) AS supplier_name
FROM stock_levels sl
INNER JOIN warehouses w ON w.id = sl.warehouse_id
INNER JOIN products p ON p.id = sl.product_id
WHERE sl.on_hand_qty < sl.reorder_point;

INSERT INTO suppliers VALUES (1, 'Atlas Cold Systems', 'West', 'strategic', 'active', 5);
INSERT INTO suppliers VALUES (2, 'Beacon Controls', 'Mountain', 'core', 'active', 7);
INSERT INTO suppliers VALUES (3, 'Cedar Safety Labs', 'Southwest', 'core', 'active', 8);
INSERT INTO suppliers VALUES (4, 'Delta Fasteners', 'Midwest', 'approved', 'active', 14);
INSERT INTO suppliers VALUES (5, 'Evergreen Power', 'Northwest', 'strategic', 'active', 6);
INSERT INTO suppliers VALUES (6, 'Frontier Lighting', 'East', 'watch', 'paused', 12);

INSERT INTO warehouses VALUES (1, 'SEA-HUB', 'Pacific', 'Morgan Hale');
INSERT INTO warehouses VALUES (2, 'DEN-HUB', 'Mountain', 'Caleb Wong');
INSERT INTO warehouses VALUES (3, 'PHX-DEP', 'Southwest', 'Brianna Soto');

INSERT INTO products VALUES (1001, 'EVAP-VALVE', 'Evaporator Valve', 'Refrigeration', 82.00, 1);
INSERT INTO products VALUES (1002, 'DOOR-HEATER', 'Dock Door Heater', 'Refrigeration', 74.00, 1);
INSERT INTO products VALUES (1003, 'CTRL-BOARD', 'Smart Control Board', 'Controls', 215.00, 2);
INSERT INTO products VALUES (1004, 'TEMP-PROBE', 'Wireless Temp Probe', 'Controls', 129.00, 2);
INSERT INTO products VALUES (1005, 'SAFE-SENSOR', 'Pressure Safety Sensor', 'Safety', 188.00, 3);
INSERT INTO products VALUES (1006, 'ANCHOR-KIT', 'Anchor Bolt Kit', 'Hardware', 16.50, 4);
INSERT INTO products VALUES (1007, 'UPS-MODULE', 'UPS Battery Module', 'Electrical', 165.00, 5);
INSERT INTO products VALUES (1008, 'SURGE-GUARD', 'Surge Suppressor', 'Electrical', 98.00, 5);
INSERT INTO products VALUES (1009, 'LED-DRIVER', 'LED Driver Assembly', 'Lighting', 44.00, 6);

INSERT INTO stock_levels VALUES (2001, 1, 1001, 18, 12, '2026-03-10');
INSERT INTO stock_levels VALUES (2002, 2, 1002, 4, 8, '2026-03-10');
INSERT INTO stock_levels VALUES (2003, 2, 1003, 7, 6, '2026-03-10');
INSERT INTO stock_levels VALUES (2004, 3, 1004, 3, 9, '2026-03-10');
INSERT INTO stock_levels VALUES (2005, 3, 1005, 2, 5, '2026-03-10');
INSERT INTO stock_levels VALUES (2006, 1, 1006, 50, 20, '2026-03-10');
INSERT INTO stock_levels VALUES (2007, 1, 1007, 6, 10, '2026-03-10');
INSERT INTO stock_levels VALUES (2008, 2, 1008, 11, 6, '2026-03-10');
INSERT INTO stock_levels VALUES (2009, 3, 1009, 9, 12, '2026-03-10');

INSERT INTO purchase_orders VALUES (5001, 1, 1, 'Riley Grant', '2026-03-01', '2026-03-12', 'open', 2232.00);
INSERT INTO purchase_orders VALUES (5002, 2, 3, 'Jordan Ames', '2026-03-02', '2026-03-11', 'approved', 1720.00);
INSERT INTO purchase_orders VALUES (5003, 3, 3, 'Jordan Ames', '2026-02-25', '2026-03-07', 'partial', 1880.00);
INSERT INTO purchase_orders VALUES (5004, 4, 1, 'Casey Shaw', '2026-02-20', '2026-02-28', 'received', 660.00);
INSERT INTO purchase_orders VALUES (5005, 5, 1, 'Riley Grant', '2026-03-04', '2026-03-15', 'open', 2140.00);
INSERT INTO purchase_orders VALUES (5006, 6, 2, 'Casey Shaw', '2026-02-15', '2026-02-25', 'cancelled', 440.00);
INSERT INTO purchase_orders VALUES (5007, 2, 2, 'Riley Grant', '2026-01-30', '2026-02-09', 'received', 774.00);
INSERT INTO purchase_orders VALUES (5008, 1, 2, 'Jordan Ames', '2026-02-10', '2026-02-22', 'received', 888.00);

INSERT INTO purchase_order_lines VALUES (6001, 5001, 1001, 20, 0, 82.00);
INSERT INTO purchase_order_lines VALUES (6002, 5001, 1002, 8, 0, 74.00);
INSERT INTO purchase_order_lines VALUES (6003, 5002, 1003, 5, 0, 215.00);
INSERT INTO purchase_order_lines VALUES (6004, 5002, 1004, 5, 0, 129.00);
INSERT INTO purchase_order_lines VALUES (6005, 5003, 1005, 10, 6, 188.00);
INSERT INTO purchase_order_lines VALUES (6006, 5004, 1006, 40, 40, 16.50);
INSERT INTO purchase_order_lines VALUES (6007, 5005, 1007, 10, 0, 165.00);
INSERT INTO purchase_order_lines VALUES (6008, 5005, 1008, 5, 0, 98.00);
INSERT INTO purchase_order_lines VALUES (6009, 5006, 1009, 10, 0, 44.00);
INSERT INTO purchase_order_lines VALUES (6010, 5007, 1004, 6, 6, 129.00);
INSERT INTO purchase_order_lines VALUES (6011, 5008, 1002, 12, 12, 74.00);

INSERT INTO quality_incidents VALUES (9001, 1, 1002, '2026-03-05', 'high', 'open', 'Heater gasket failure on first install batch.');
INSERT INTO quality_incidents VALUES (9002, 2, 1004, '2026-03-01', 'medium', 'investigating', 'Probe calibration drift on warm dock deployments.');
INSERT INTO quality_incidents VALUES (9003, 3, 1005, '2026-02-26', 'critical', 'open', 'Pressure sensor alarm intermittently sticks high.');
INSERT INTO quality_incidents VALUES (9004, 5, 1007, '2026-02-20', 'low', 'closed', 'Battery label mismatch on one carton.');
INSERT INTO quality_incidents VALUES (9005, 6, 1009, '2026-03-06', 'medium', 'open', 'LED driver housings were scratched on receipt.');
