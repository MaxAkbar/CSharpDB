-- Northstar Field Services
-- Fictitious multi-region field service company snapshot for 2026-03-10.
-- Highlights: customer sites, contracts, dispatch, inventory, billing, composite indexes,
-- views, triggers, IDENTITY columns, procedures, CTE-friendly reporting, and TEXT(...) filtering.

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    segment TEXT NOT NULL,
    region TEXT NOT NULL,
    account_manager TEXT NOT NULL,
    annual_value REAL NOT NULL,
    is_active INTEGER NOT NULL
);

CREATE TABLE customer_sites (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    site_code TEXT NOT NULL,
    site_name TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    go_live_date TEXT NOT NULL,
    service_tier TEXT NOT NULL
);

CREATE TABLE service_contracts (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    contract_code TEXT NOT NULL,
    service_line TEXT NOT NULL,
    status TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    monthly_recurring_revenue REAL NOT NULL,
    sla_hours INTEGER NOT NULL
);

CREATE TABLE technicians (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    home_region TEXT NOT NULL,
    specialty TEXT NOT NULL,
    level TEXT NOT NULL,
    is_on_call INTEGER NOT NULL,
    active_work_orders INTEGER NOT NULL
);

CREATE TABLE service_tickets (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL,
    contract_id INTEGER NOT NULL,
    opened_date TEXT NOT NULL,
    category TEXT NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT NOT NULL,
    severity_score INTEGER NOT NULL
);

CREATE TABLE work_orders (
    id INTEGER PRIMARY KEY,
    ticket_id INTEGER NOT NULL,
    technician_id INTEGER,
    dispatch_date TEXT NOT NULL,
    scheduled_date TEXT,
    completed_date TEXT,
    status TEXT NOT NULL,
    labor_hours REAL NOT NULL,
    parts_cost REAL NOT NULL
);

CREATE TABLE ticket_events (
    id INTEGER PRIMARY KEY IDENTITY,
    ticket_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE TABLE work_order_status_history (
    id INTEGER PRIMARY KEY IDENTITY,
    work_order_id INTEGER NOT NULL,
    old_status TEXT,
    new_status TEXT NOT NULL,
    changed_date TEXT NOT NULL
);

CREATE TABLE warehouses (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    manager TEXT NOT NULL
);

CREATE TABLE parts (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost REAL NOT NULL,
    preferred_warehouse_id INTEGER NOT NULL
);

CREATE TABLE inventory_movements (
    id INTEGER PRIMARY KEY IDENTITY,
    warehouse_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    movement_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    moved_date TEXT NOT NULL,
    reference_code TEXT NOT NULL
);

CREATE TABLE invoices (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    contract_id INTEGER NOT NULL,
    invoice_date TEXT NOT NULL,
    due_date TEXT NOT NULL,
    status TEXT NOT NULL,
    subtotal REAL NOT NULL,
    tax REAL NOT NULL,
    total REAL NOT NULL
);

CREATE TABLE invoice_lines (
    id INTEGER PRIMARY KEY,
    invoice_id INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    description TEXT NOT NULL,
    revenue_category TEXT NOT NULL,
    amount REAL NOT NULL
);

CREATE INDEX idx_customer_sites_customer_tier ON customer_sites (customer_id, service_tier);
CREATE INDEX idx_service_contracts_customer_status ON service_contracts (customer_id, status);
CREATE INDEX idx_service_tickets_site_status ON service_tickets (site_id, status);
CREATE INDEX idx_work_orders_ticket_status ON work_orders (ticket_id, status);
CREATE INDEX idx_inventory_movements_wh_part_date ON inventory_movements (warehouse_id, part_id, moved_date);
CREATE INDEX idx_invoices_customer_status ON invoices (customer_id, status);

INSERT INTO customers VALUES (1, 'Apex Grocery Partners', 'Enterprise', 'West', 'Dana Kim', 1450000.00, 1);
INSERT INTO customers VALUES (2, 'Blue Harbor Logistics', 'Enterprise', 'Mountain', 'Marcus Lee', 980000.00, 1);
INSERT INTO customers VALUES (3, 'Solstice Health Network', 'Strategic', 'Southwest', 'Nina Patel', 1325000.00, 1);
INSERT INTO customers VALUES (4, 'Northwind Campus Services', 'Growth', 'Northwest', 'Omar Reed', 410000.00, 1);

INSERT INTO customer_sites VALUES (101, 1, 'AGP-SEA-01', 'Downtown Fresh Market', 'Seattle', 'WA', '2023-03-15', 'Platinum');
INSERT INTO customer_sites VALUES (102, 1, 'AGP-PDX-02', 'Riverfront Distribution', 'Portland', 'OR', '2024-01-10', 'Gold');
INSERT INTO customer_sites VALUES (201, 2, 'BHL-RNO-01', 'Sparks Fulfillment', 'Reno', 'NV', '2022-11-01', 'Platinum');
INSERT INTO customer_sites VALUES (202, 2, 'BHL-SLC-02', 'Wasatch Crossdock', 'Salt Lake City', 'UT', '2025-02-01', 'Gold');
INSERT INTO customer_sites VALUES (301, 3, 'SHN-PHX-01', 'Desert Care Pavilion', 'Phoenix', 'AZ', '2023-08-20', 'Platinum');
INSERT INTO customer_sites VALUES (302, 3, 'SHN-TUC-02', 'Tucson Specialty Clinic', 'Tucson', 'AZ', '2024-06-14', 'Gold');
INSERT INTO customer_sites VALUES (401, 4, 'NCS-BOI-01', 'Riverbend Campus', 'Boise', 'ID', '2025-01-05', 'Silver');
INSERT INTO customer_sites VALUES (402, 4, 'NCS-SPO-02', 'Spokane Learning Center', 'Spokane', 'WA', '2024-09-12', 'Gold');

INSERT INTO service_contracts VALUES (1001, 1, 'AGP-HVAC-PLAT', 'HVAC', 'active', '2025-01-01', '2026-12-31', 48000.00, 4);
INSERT INTO service_contracts VALUES (1002, 2, 'BHL-COLD-OPS', 'Refrigeration', 'active', '2025-04-01', '2027-03-31', 62000.00, 2);
INSERT INTO service_contracts VALUES (1003, 3, 'SHN-LIFE-SAFE', 'Life Safety', 'active', '2025-07-01', '2026-06-30', 54000.00, 2);
INSERT INTO service_contracts VALUES (1004, 4, 'NCS-FAC-GOLD', 'Facilities', 'active', '2025-01-01', '2025-12-31', 18000.00, 8);
INSERT INTO service_contracts VALUES (1005, 1, 'AGP-ENERGY-PILOT', 'Energy Analytics', 'pilot', '2026-01-01', '2026-06-30', 12000.00, 12);

INSERT INTO technicians VALUES (501, 'Maya Chen', 'Pacific', 'HVAC', 'Senior', 1, 1);
INSERT INTO technicians VALUES (502, 'Luis Romero', 'Mountain', 'Refrigeration', 'Lead', 1, 2);
INSERT INTO technicians VALUES (503, 'Harper Singh', 'Southwest', 'Controls', 'Senior', 0, 2);
INSERT INTO technicians VALUES (504, 'Elena Park', 'Pacific', 'Electrical', 'Mid', 0, 2);
INSERT INTO technicians VALUES (505, 'Theo Brooks', 'Northwest', 'Facilities', 'Mid', 1, 1);
INSERT INTO technicians VALUES (506, 'Isaac Bennett', 'Mountain', 'Refrigeration', 'Senior', 0, 1);

INSERT INTO warehouses VALUES (801, 'SEA-DC', 'Seattle', 'WA', 'Morgan Hale');
INSERT INTO warehouses VALUES (802, 'PHX-DEP', 'Phoenix', 'AZ', 'Brianna Soto');
INSERT INTO warehouses VALUES (803, 'DEN-HUB', 'Denver', 'CO', 'Caleb Wong');

INSERT INTO parts VALUES (10001, 'HVAC-FLTR-20', 'MERV-13 Filter Pack', 'HVAC', 24, 18.50, 801);
INSERT INTO parts VALUES (10002, 'COOL-DOOR-HTR', 'Freezer Door Heater', 'Refrigeration', 8, 74.00, 803);
INSERT INTO parts VALUES (10003, 'CTRL-PROBE-TEMP', 'Wireless Temperature Probe', 'Controls', 15, 129.00, 803);
INSERT INTO parts VALUES (10004, 'LIFE-PRESS-SEN', 'Pressure Sensor Kit', 'Life Safety', 6, 210.00, 802);
INSERT INTO parts VALUES (10005, 'ELEC-UPS-BATT', 'UPS Battery Module', 'Electrical', 10, 165.00, 801);
INSERT INTO parts VALUES (10006, 'FAC-CIRC-PUMP', 'Circulator Pump', 'Facilities', 4, 540.00, 801);
INSERT INTO parts VALUES (10007, 'LIGHT-DRV-LED', 'LED Driver Assembly', 'Lighting', 12, 44.00, 801);
INSERT INTO parts VALUES (10008, 'NET-GW-LTE', 'LTE Gateway', 'Network', 5, 380.00, 803);

INSERT INTO service_tickets VALUES (7001, 101, 1001, '2026-02-27', 'HVAC', 'critical', 'dispatching', 'Walk-in cooler alarm repeating overnight.', 95);
INSERT INTO service_tickets VALUES (7002, 102, 1005, '2026-02-18', 'Energy', 'high', 'open', 'Energy dashboard shows sustained peak demand spike.', 72);
INSERT INTO service_tickets VALUES (7003, 201, 1002, '2026-03-01', 'Refrigeration', 'high', 'scheduled', 'Dock freezer door heater failure.', 84);
INSERT INTO service_tickets VALUES (7004, 202, 1002, '2026-02-21', 'Network', 'medium', 'monitoring', 'Temperature probe intermittently offline.', 48);
INSERT INTO service_tickets VALUES (7005, 301, 1003, '2026-03-03', 'LifeSafety', 'critical', 'in_progress', 'Pharmacy clean room differential pressure alert.', 98);
INSERT INTO service_tickets VALUES (7006, 302, 1003, '2026-02-11', 'Access', 'medium', 'resolved', 'Badge reader firmware rollback needed.', 35);
INSERT INTO service_tickets VALUES (7007, 401, 1004, '2026-03-05', 'Facilities', 'high', 'scheduled', 'Boiler short cycling during morning warmup.', 77);
INSERT INTO service_tickets VALUES (7008, 402, 1004, '2026-01-29', 'Lighting', 'low', 'resolved', 'Gymnasium LED driver replacement.', 22);
INSERT INTO service_tickets VALUES (7009, 201, 1002, '2026-03-07', 'Refrigeration', 'medium', 'open', 'Battery room temperature trending upward.', 63);
INSERT INTO service_tickets VALUES (7010, 101, 1001, '2026-03-08', 'Electrical', 'high', 'dispatching', 'Checkout lane UPS transfer issue.', 81);

INSERT INTO work_orders VALUES (9001, 7001, 501, '2026-02-27', '2026-03-10', NULL, 'assigned', 2.50, 350.00);
INSERT INTO work_orders VALUES (9002, 7002, 504, '2026-02-18', '2026-03-12', NULL, 'triaged', 1.00, 0.00);
INSERT INTO work_orders VALUES (9003, 7003, 502, '2026-03-01', '2026-03-10', NULL, 'scheduled', 3.00, 180.00);
INSERT INTO work_orders VALUES (9004, 7004, 506, '2026-02-21', '2026-03-14', NULL, 'monitoring', 1.50, 0.00);
INSERT INTO work_orders VALUES (9005, 7005, 503, '2026-03-03', '2026-03-10', NULL, 'in_progress', 4.50, 420.00);
INSERT INTO work_orders VALUES (9006, 7006, 503, '2026-02-11', '2026-02-12', '2026-02-12', 'completed', 2.00, 60.00);
INSERT INTO work_orders VALUES (9007, 7007, 505, '2026-03-05', '2026-03-11', NULL, 'scheduled', 2.75, 110.00);
INSERT INTO work_orders VALUES (9008, 7008, 505, '2026-01-29', '2026-01-30', '2026-01-30', 'completed', 1.25, 95.00);
INSERT INTO work_orders VALUES (9009, 7009, 502, '2026-03-07', '2026-03-13', NULL, 'assigned', 1.25, 0.00);
INSERT INTO work_orders VALUES (9010, 7010, 504, '2026-03-08', '2026-03-11', NULL, 'assigned', 2.00, 140.00);

INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7001, 'created', '2026-02-27', 'Auto-dispatched from overnight alarm feed.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7001, 'parts_reserved', '2026-02-28', 'Filter pack and contactor reserved from SEA-DC.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7002, 'customer_update', '2026-02-19', 'Energy analyst confirmed utility spike pattern.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7003, 'site_call', '2026-03-01', 'Site manager reported freezer icing near dock door.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7005, 'escalated', '2026-03-03', 'Clinical operations escalated due to pharmacy clean room impact.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7006, 'resolved', '2026-02-12', 'Firmware rollback validated by clinic staff.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7007, 'dispatch_note', '2026-03-05', 'Campus engineer requested earliest morning arrival.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7008, 'resolved', '2026-01-30', 'LED driver replaced and fixture bank retested.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7009, 'monitoring', '2026-03-08', 'Battery room trend is above baseline but below emergency threshold.');
INSERT INTO ticket_events (ticket_id, event_type, event_date, details) VALUES (7010, 'dispatch_note', '2026-03-08', 'Store leadership requested work before evening rush.');

INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9001, 'new', 'assigned', '2026-02-27');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9002, 'new', 'triaged', '2026-02-18');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9003, 'new', 'scheduled', '2026-03-01');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9005, 'scheduled', 'in_progress', '2026-03-03');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9006, 'scheduled', 'completed', '2026-02-12');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9007, 'new', 'scheduled', '2026-03-05');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9008, 'scheduled', 'completed', '2026-01-30');
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date) VALUES (9010, 'new', 'assigned', '2026-03-08');

INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10001, 'receipt', 60, '2026-01-05', 'PO-4101');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10001, 'issue', -18, '2026-02-27', 'WO-9001');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (803, 10002, 'receipt', 20, '2026-01-18', 'PO-4115');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (803, 10002, 'issue', -2, '2026-03-01', 'WO-9003');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (803, 10003, 'receipt', 24, '2026-02-02', 'PO-4130');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (803, 10003, 'issue', -1, '2026-02-21', 'WO-9004');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (802, 10004, 'receipt', 12, '2026-01-22', 'PO-4144');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (802, 10004, 'issue', -2, '2026-03-03', 'WO-9005');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10005, 'receipt', 18, '2026-02-10', 'PO-4152');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10005, 'issue', -1, '2026-03-08', 'WO-9010');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10006, 'receipt', 6, '2026-02-12', 'PO-4160');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10006, 'issue', -1, '2026-03-05', 'WO-9007');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10007, 'receipt', 25, '2026-01-12', 'PO-4098');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (801, 10007, 'issue', -3, '2026-01-29', 'WO-9008');
INSERT INTO inventory_movements (warehouse_id, part_id, movement_type, quantity, moved_date, reference_code) VALUES (803, 10008, 'receipt', 10, '2026-02-24', 'PO-4177');

INSERT INTO invoices VALUES (11001, 1, 1001, '2026-02-01', '2026-02-28', 'paid', 4000.00, 360.00, 4360.00);
INSERT INTO invoices VALUES (11002, 1, 1005, '2026-02-01', '2026-02-28', 'open', 1000.00, 90.00, 1090.00);
INSERT INTO invoices VALUES (11003, 2, 1002, '2026-02-01', '2026-03-02', 'overdue', 5166.67, 465.00, 5631.67);
INSERT INTO invoices VALUES (11004, 3, 1003, '2026-02-01', '2026-03-02', 'open', 4500.00, 405.00, 4905.00);
INSERT INTO invoices VALUES (11005, 4, 1004, '2026-02-01', '2026-03-02', 'paid', 1500.00, 135.00, 1635.00);
INSERT INTO invoices VALUES (11006, 2, 1002, '2026-03-01', '2026-03-31', 'draft', 5166.67, 465.00, 5631.67);

INSERT INTO invoice_lines VALUES (20001, 11001, 1, 'Preventive maintenance retainer - February', 'recurring', 3200.00);
INSERT INTO invoice_lines VALUES (20002, 11001, 2, 'After-hours dispatch coverage', 'services', 800.00);
INSERT INTO invoice_lines VALUES (20003, 11002, 1, 'Energy analytics pilot subscription - February', 'subscription', 1000.00);
INSERT INTO invoice_lines VALUES (20004, 11003, 1, 'Cold-chain coverage retainer - February', 'recurring', 4600.00);
INSERT INTO invoice_lines VALUES (20005, 11003, 2, 'Emergency dock freezer visit', 'services', 566.67);
INSERT INTO invoice_lines VALUES (20006, 11004, 1, 'Life safety monitoring retainer - February', 'recurring', 4000.00);
INSERT INTO invoice_lines VALUES (20007, 11004, 2, 'Clean room escalation readiness', 'services', 500.00);
INSERT INTO invoice_lines VALUES (20008, 11005, 1, 'Campus facilities support - February', 'recurring', 1500.00);
INSERT INTO invoice_lines VALUES (20009, 11006, 1, 'Cold-chain coverage retainer - March', 'recurring', 4600.00);
INSERT INTO invoice_lines VALUES (20010, 11006, 2, 'Battery room diagnostic hold', 'services', 566.67);

CREATE VIEW ticket_command_center AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    ct.contract_code,
    s.id AS site_id,
    s.site_code,
    s.site_name,
    s.city,
    s.state,
    t.id AS ticket_id,
    t.category,
    t.priority,
    t.status AS ticket_status,
    t.summary,
    w.id AS work_order_id,
    w.status AS work_order_status,
    w.scheduled_date,
    w.labor_hours,
    w.parts_cost,
    tech.name AS technician_name,
    t.severity_score
FROM customers c
INNER JOIN customer_sites s ON s.customer_id = c.id
INNER JOIN service_tickets t ON t.site_id = s.id
INNER JOIN service_contracts ct ON ct.id = t.contract_id
LEFT JOIN work_orders w ON w.ticket_id = t.id
LEFT JOIN technicians tech ON tech.id = w.technician_id;

CREATE VIEW inventory_position AS
SELECT
    w.id AS warehouse_id,
    w.code AS warehouse_code,
    p.id AS part_id,
    p.sku,
    p.name AS part_name,
    p.category,
    SUM(m.quantity) AS on_hand_qty,
    p.reorder_point,
    p.unit_cost
FROM warehouses w
INNER JOIN inventory_movements m ON m.warehouse_id = w.id
INNER JOIN parts p ON p.id = m.part_id
GROUP BY w.id, w.code, p.id, p.sku, p.name, p.category, p.reorder_point, p.unit_cost;

CREATE VIEW billing_snapshot AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    ct.contract_code,
    i.id AS invoice_id,
    i.invoice_date,
    i.due_date,
    i.status AS invoice_status,
    i.total
FROM customers c
INNER JOIN service_contracts ct ON ct.customer_id = c.id
INNER JOIN invoices i ON i.contract_id = ct.id;

CREATE TRIGGER trg_service_tickets_insert_event AFTER INSERT ON service_tickets
BEGIN
INSERT INTO ticket_events (ticket_id, event_type, event_date, details)
VALUES (NEW.id, 'created', '2026-03-10', 'Auto-created event for a new service ticket.');
END;

CREATE TRIGGER trg_work_orders_status_audit AFTER UPDATE ON work_orders
BEGIN
INSERT INTO work_order_status_history (work_order_id, old_status, new_status, changed_date)
VALUES (NEW.id, OLD.status, NEW.status, '2026-03-10');
END;
