-- Atlas Platform Showcase
-- Fixed snapshot dated 2026-04-08.
-- Highlights: foreign keys, collations, unique + composite indexes, views, triggers,
-- IDENTITY event logs, procedures, joins, CTEs, subqueries, set operations, catalogs,
-- and optional API-only add-ons through Program.cs (full-text + Collection<T>).

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT COLLATE NOCASE NOT NULL,
    segment TEXT NOT NULL,
    region TEXT NOT NULL,
    status TEXT NOT NULL,
    lifecycle_stage TEXT NOT NULL,
    created_date TEXT NOT NULL
);

CREATE TABLE customer_contacts (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    full_name TEXT NOT NULL,
    email TEXT COLLATE NOCASE NOT NULL,
    role_name TEXT NOT NULL,
    phone TEXT,
    is_primary INTEGER NOT NULL
);

CREATE TABLE subscriptions (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    plan_code TEXT NOT NULL,
    start_date TEXT NOT NULL,
    renewal_date TEXT NOT NULL,
    status TEXT NOT NULL,
    monthly_amount REAL NOT NULL,
    seats INTEGER NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT COLLATE NOCASE NOT NULL,
    name TEXT COLLATE NOCASE NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL,
    active INTEGER NOT NULL
);

CREATE TABLE warehouses (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    city TEXT NOT NULL,
    region TEXT NOT NULL,
    manager_name TEXT NOT NULL
);

CREATE TABLE inventory_positions (
    id INTEGER PRIMARY KEY,
    warehouse_id INTEGER NOT NULL REFERENCES warehouses(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    on_hand_qty INTEGER NOT NULL,
    reserved_qty INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    last_received_date TEXT NOT NULL
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    ordered_date TEXT NOT NULL,
    status TEXT NOT NULL,
    channel TEXT NOT NULL,
    total_amount REAL NOT NULL,
    account_owner TEXT NOT NULL,
    shipped_date TEXT
);

CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL
);

CREATE TABLE support_agents (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    specialty TEXT NOT NULL,
    is_on_call INTEGER NOT NULL
);

CREATE TABLE support_tickets (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    contact_id INTEGER REFERENCES customer_contacts(id),
    order_id INTEGER REFERENCES orders(id),
    assigned_agent_id INTEGER REFERENCES support_agents(id),
    opened_date TEXT NOT NULL,
    category TEXT NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    subject TEXT NOT NULL,
    severity_score INTEGER NOT NULL,
    resolution_due TEXT NOT NULL,
    last_updated_date TEXT NOT NULL
);

CREATE TABLE ticket_comments (
    id INTEGER PRIMARY KEY IDENTITY,
    ticket_id INTEGER NOT NULL REFERENCES support_tickets(id) ON DELETE CASCADE,
    author_name TEXT NOT NULL,
    comment_date TEXT NOT NULL,
    body TEXT NOT NULL
);

CREATE TABLE knowledge_articles (
    id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    title TEXT COLLATE NOCASE NOT NULL,
    body TEXT NOT NULL,
    status TEXT NOT NULL,
    published_date TEXT NOT NULL
);

CREATE TABLE platform_events (
    id INTEGER PRIMARY KEY IDENTITY,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_date TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_customer_contacts_email_unique ON customer_contacts (email);
CREATE UNIQUE INDEX idx_products_sku_unique ON products (sku);
CREATE UNIQUE INDEX idx_inventory_positions_wh_product_unique ON inventory_positions (warehouse_id, product_id);

CREATE INDEX idx_subscriptions_customer_status ON subscriptions (customer_id, status);
CREATE INDEX idx_orders_customer_status_date ON orders (customer_id, status, ordered_date);
CREATE INDEX idx_order_items_order_product ON order_items (order_id, product_id);
CREATE INDEX idx_support_tickets_customer_status_priority ON support_tickets (customer_id, status, priority);
CREATE INDEX idx_support_tickets_agent_status ON support_tickets (assigned_agent_id, status);
CREATE INDEX idx_inventory_positions_reorder ON inventory_positions (warehouse_id, reorder_point, on_hand_qty);
CREATE INDEX idx_knowledge_articles_category_status ON knowledge_articles (category, status);

CREATE VIEW customer_360 AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    c.segment,
    c.region,
    c.status AS customer_status,
    c.lifecycle_stage,
    (SELECT cc.full_name FROM customer_contacts cc WHERE cc.customer_id = c.id AND cc.is_primary = 1) AS primary_contact_name,
    (SELECT cc.email FROM customer_contacts cc WHERE cc.customer_id = c.id AND cc.is_primary = 1) AS primary_contact_email,
    (SELECT COUNT(*) FROM subscriptions s WHERE s.customer_id = c.id AND s.status = 'active') AS active_subscription_count,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS total_orders,
    (SELECT COUNT(*) FROM support_tickets t WHERE t.customer_id = c.id AND t.status <> 'closed') AS open_ticket_count,
    (SELECT SUM(o.total_amount) FROM orders o WHERE o.customer_id = c.id) AS lifetime_revenue
FROM customers c;

CREATE VIEW support_queue AS
SELECT
    t.id AS ticket_id,
    c.id AS customer_id,
    c.name AS customer_name,
    cc.full_name AS contact_name,
    cc.email AS requester_email,
    t.order_id,
    t.assigned_agent_id,
    o.status AS order_status,
    t.category,
    t.priority,
    t.status AS ticket_status,
    t.subject,
    t.severity_score,
    a.name AS assigned_agent,
    t.opened_date,
    t.resolution_due,
    t.last_updated_date
FROM support_tickets t
INNER JOIN customers c ON c.id = t.customer_id
LEFT JOIN customer_contacts cc ON cc.id = t.contact_id
LEFT JOIN orders o ON o.id = t.order_id
LEFT JOIN support_agents a ON a.id = t.assigned_agent_id;

CREATE VIEW inventory_watch AS
SELECT
    ip.id AS inventory_position_id,
    w.id AS warehouse_id,
    w.code AS warehouse_code,
    p.id AS product_id,
    p.sku,
    p.name AS product_name,
    p.category,
    ip.on_hand_qty,
    ip.reserved_qty,
    ip.on_hand_qty - ip.reserved_qty AS available_qty,
    ip.reorder_point,
    ip.last_received_date
FROM inventory_positions ip
INNER JOIN warehouses w ON w.id = ip.warehouse_id
INNER JOIN products p ON p.id = ip.product_id;

CREATE VIEW order_revenue_rollup AS
SELECT
    o.id AS order_id,
    c.id AS customer_id,
    c.name AS customer_name,
    o.ordered_date,
    o.status AS order_status,
    o.channel,
    o.total_amount,
    (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id = o.id) AS line_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id;

CREATE TRIGGER trg_orders_insert_event AFTER INSERT ON orders
BEGIN
    INSERT INTO platform_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('order', NEW.id, 'created', NEW.ordered_date, 'system', 'Order created.');
END;

CREATE TRIGGER trg_support_tickets_status_event AFTER UPDATE ON support_tickets
BEGIN
    INSERT INTO platform_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('ticket', NEW.id, 'status_changed', NEW.last_updated_date, 'system', 'Ticket status changed.');
END;

CREATE TRIGGER trg_ticket_comments_insert_event AFTER INSERT ON ticket_comments
BEGIN
    INSERT INTO platform_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('ticket', NEW.ticket_id, 'comment_added', NEW.comment_date, NEW.author_name, 'Ticket comment added.');
END;

CREATE TRIGGER trg_order_items_delete_event AFTER DELETE ON order_items
BEGIN
    INSERT INTO platform_events (entity_type, entity_id, event_type, event_date, actor_name, details)
    VALUES ('order', OLD.order_id, 'line_deleted', '2026-04-08', 'system', 'Order line deleted.');
END;

INSERT INTO customers VALUES (1001, 'Apex Outfitters', 'Enterprise', 'West', 'active', 'expansion', '2024-05-14');
INSERT INTO customers VALUES (1002, 'Bluebird Health', 'Strategic', 'Mountain', 'active', 'steady_state', '2023-11-03');
INSERT INTO customers VALUES (1003, 'Cinder Logistics', 'Enterprise', 'Central', 'active', 'renewal', '2024-02-19');
INSERT INTO customers VALUES (1004, 'Delta Learning Labs', 'Growth', 'Northwest', 'active', 'onboarding', '2025-08-27');
INSERT INTO customers VALUES (1005, 'Evergreen Capital', 'Growth', 'West', 'at_risk', 'renewal', '2024-09-08');

INSERT INTO customer_contacts VALUES (1, 1001, 'Jordan Lee', 'jordan@apexoutfitters.com', 'VP Operations', '206-555-0110', 1);
INSERT INTO customer_contacts VALUES (2, 1001, 'Mina Park', 'finance@apexoutfitters.com', 'Finance Lead', '206-555-0191', 0);
INSERT INTO customer_contacts VALUES (3, 1002, 'Dr. Sam Ortiz', 'sam.ortiz@bluebirdhealth.org', 'Clinical Systems Director', '303-555-0144', 1);
INSERT INTO customer_contacts VALUES (4, 1003, 'Riley Chen', 'rchen@cinderlogistics.io', 'Revenue Operations', '312-555-0160', 1);
INSERT INTO customer_contacts VALUES (5, 1004, 'Ava Morgan', 'Ava.Morgan@DeltaLearning.edu', 'Program Manager', '509-555-0118', 1);
INSERT INTO customer_contacts VALUES (6, 1005, 'Chris Patel', 'chris@evergreencapital.com', 'Controller', '415-555-0186', 1);
INSERT INTO customer_contacts VALUES (7, 1003, 'Operations Desk', 'ops@cinderlogistics.io', 'Shared Inbox', NULL, 0);

INSERT INTO subscriptions VALUES (3001, 1001, 'PLAN-ENTERPRISE', '2025-05-01', '2026-05-01', 'active', 1200.00, 40);
INSERT INTO subscriptions VALUES (3002, 1002, 'PLAN-GROWTH', '2025-04-20', '2026-04-20', 'active', 850.00, 22);
INSERT INTO subscriptions VALUES (3003, 1003, 'PLAN-ENTERPRISE', '2025-06-15', '2026-06-15', 'active', 1500.00, 60);
INSERT INTO subscriptions VALUES (3004, 1004, 'PLAN-STARTER', '2025-10-18', '2026-04-18', 'active', 399.00, 8);
INSERT INTO subscriptions VALUES (3005, 1005, 'PLAN-GROWTH', '2025-04-12', '2026-04-12', 'past_due', 850.00, 20);

INSERT INTO products VALUES (2001, 'CORE-ANNUAL', 'Core Annual Platform License', 'Subscription', 1200.00, 1);
INSERT INTO products VALUES (2002, 'PLUS-ANNUAL', 'Plus Annual Platform License', 'Subscription', 2400.00, 1);
INSERT INTO products VALUES (2003, 'AI-ASSIST', 'AI Assistant Add-On', 'AddOn', 900.00, 1);
INSERT INTO products VALUES (2004, 'EDGE-GATEWAY', 'Edge Gateway Appliance', 'Hardware', 650.00, 1);
INSERT INTO products VALUES (2005, 'SENSOR-KIT', 'Environmental Sensor Kit', 'Hardware', 180.00, 1);
INSERT INTO products VALUES (2006, 'TRAINING-DAY', 'Dedicated Training Day', 'Services', 1500.00, 1);

INSERT INTO warehouses VALUES (6001, 'SEA-HUB', 'Seattle', 'West', 'Morgan Hale');
INSERT INTO warehouses VALUES (6002, 'DEN-HUB', 'Denver', 'Mountain', 'Caleb Wong');
INSERT INTO warehouses VALUES (6003, 'PHX-DEP', 'Phoenix', 'Southwest', 'Brianna Soto');

INSERT INTO inventory_positions VALUES (9001, 6001, 2004, 12, 4, 6, '2026-03-25');
INSERT INTO inventory_positions VALUES (9002, 6001, 2005, 18, 10, 12, '2026-03-29');
INSERT INTO inventory_positions VALUES (9003, 6002, 2004, 3, 1, 5, '2026-03-18');
INSERT INTO inventory_positions VALUES (9004, 6002, 2005, 40, 6, 15, '2026-03-30');
INSERT INTO inventory_positions VALUES (9005, 6003, 2004, 7, 5, 4, '2026-03-21');
INSERT INTO inventory_positions VALUES (9006, 6003, 2006, 2, 0, 3, '2026-03-15');
INSERT INTO inventory_positions VALUES (9007, 6001, 2003, 20, 8, 10, '2026-03-27');
INSERT INTO inventory_positions VALUES (9008, 6002, 2001, 50, 12, 20, '2026-03-11');
INSERT INTO inventory_positions VALUES (9009, 6003, 2002, 6, 2, 5, '2026-03-20');

INSERT INTO orders VALUES (5001, 1001, '2026-03-01', 'shipped', 'direct', 3300.00, 'Maya Chen', '2026-03-03');
INSERT INTO orders VALUES (5002, 1002, '2026-03-26', 'processing', 'partner', 1910.00, 'Derrick Cole', NULL);
INSERT INTO orders VALUES (5003, 1003, '2026-04-01', 'open', 'direct', 2400.00, 'Maya Chen', NULL);
INSERT INTO orders VALUES (5004, 1004, '2026-03-15', 'shipped', 'field', 2700.00, 'Leo Martinez', '2026-03-18');
INSERT INTO orders VALUES (5005, 1001, '2026-04-01', 'open', 'direct', 650.00, 'Maya Chen', NULL);
INSERT INTO orders VALUES (5006, 1005, '2026-04-03', 'processing', 'direct', 1680.00, 'Nora Blake', NULL);
INSERT INTO orders VALUES (5007, 1004, '2026-03-11', 'cancelled', 'self_serve', 900.00, 'Leo Martinez', NULL);

INSERT INTO order_items VALUES (9101, 5001, 2002, 1, 2400.00);
INSERT INTO order_items VALUES (9102, 5001, 2003, 1, 900.00);
INSERT INTO order_items VALUES (9103, 5002, 2004, 1, 650.00);
INSERT INTO order_items VALUES (9104, 5002, 2005, 2, 180.00);
INSERT INTO order_items VALUES (9105, 5002, 2003, 1, 900.00);
INSERT INTO order_items VALUES (9106, 5003, 2002, 1, 2400.00);
INSERT INTO order_items VALUES (9107, 5004, 2001, 1, 1200.00);
INSERT INTO order_items VALUES (9108, 5004, 2006, 1, 1500.00);
INSERT INTO order_items VALUES (9109, 5005, 2004, 1, 650.00);
INSERT INTO order_items VALUES (9110, 5006, 2006, 1, 1500.00);
INSERT INTO order_items VALUES (9111, 5006, 2005, 1, 180.00);
INSERT INTO order_items VALUES (9112, 5007, 2003, 1, 900.00);

INSERT INTO support_agents VALUES (4001, 'Nora Blake', 'West', 'Billing', 1);
INSERT INTO support_agents VALUES (4002, 'Isaac Cole', 'Mountain', 'Hardware', 0);
INSERT INTO support_agents VALUES (4003, 'Priya Raman', 'National', 'Platform', 1);
INSERT INTO support_agents VALUES (4004, 'Leo Martinez', 'West', 'Onboarding', 0);

INSERT INTO support_tickets VALUES (7001, 1001, 1, 5005, 4002, '2026-04-01', 'hardware', 'high', 'open', 'Gateway on latest expansion order has not checked in.', 82, '2026-04-10', '2026-04-01');
INSERT INTO support_tickets VALUES (7002, 1002, 3, 5002, 4003, '2026-03-28', 'analytics', 'critical', 'in_progress', 'AI assistant recommendations are timing out for nurses.', 96, '2026-04-09', '2026-04-03');
INSERT INTO support_tickets VALUES (7003, 1003, 4, 5003, NULL, '2026-04-02', 'billing', 'medium', 'new', 'Invoice total does not match contracted seat count.', 55, '2026-04-15', '2026-04-02');
INSERT INTO support_tickets VALUES (7004, 1004, 5, 5004, 4004, '2026-03-20', 'onboarding', 'low', 'closed', 'Need admin training recording for new coordinators.', 20, '2026-03-25', '2026-03-22');
INSERT INTO support_tickets VALUES (7005, 1005, 6, 5006, 4001, '2026-04-04', 'payments', 'high', 'open', 'Past due renewal needs manual payment arrangement.', 74, '2026-04-08', '2026-04-04');
INSERT INTO support_tickets VALUES (7006, 1001, 2, 5001, 4003, '2026-03-18', 'platform', 'medium', 'monitoring', 'Webhook deliveries spike latency during nightly imports.', 48, '2026-04-12', '2026-03-21');
INSERT INTO support_tickets VALUES (7007, 1004, 5, NULL, 4004, '2026-04-05', 'feature', 'medium', 'new', 'Requesting template cloning for district rollouts.', 43, '2026-04-19', '2026-04-05');

INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7001, 'Isaac Cole', '2026-04-01', 'Customer confirmed the gateway power light is on but no heartbeat is visible.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7002, 'Priya Raman', '2026-04-03', 'Latency reproduced during the 7 AM clinic shift overlap.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7002, 'Bluebird Health', '2026-04-03', 'The timeout pattern is concentrated on medication recommendation prompts.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7003, 'Riley Chen', '2026-04-02', 'Finance flagged a mismatch between contracted seats and invoice quantity.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7004, 'Leo Martinez', '2026-03-22', 'Training recording delivered and acknowledged by district admins.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7005, 'Nora Blake', '2026-04-04', 'Controller requested a short extension while bank approval clears.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7006, 'Priya Raman', '2026-03-21', 'Webhook batch size reduced and customer is monitoring import latency.');
INSERT INTO ticket_comments (ticket_id, author_name, comment_date, body) VALUES (7007, 'Ava Morgan', '2026-04-05', 'Template cloning would reduce manual rollout work for new campuses.');

INSERT INTO knowledge_articles VALUES (8001, 'hardware', 'How to Reset an Edge Gateway After Failed Activation', 'Use this article when an edge gateway powers on but never checks into the control plane. Verify network reachability, LED state, and cached activation token before replacement.', 'published', '2026-02-14');
INSERT INTO knowledge_articles VALUES (8002, 'billing', 'Refund Workflow for Duplicate Annual Renewal Charges', 'This article covers the refund and credit workflow for duplicate annual renewal charges, invoice mismatches, and manual finance approval steps.', 'published', '2026-01-30');
INSERT INTO knowledge_articles VALUES (8003, 'hardware', 'Warranty Replacement Checklist for Sensor Kits', 'Follow the warranty replacement checklist before shipping a new sensor kit. Confirm serial number, proof of failure, and spare kit availability in the nearest warehouse.', 'published', '2026-03-05');
INSERT INTO knowledge_articles VALUES (8004, 'analytics', 'Reducing AI Assistant Latency During Peak Clinic Hours', 'Tune prompt routing, cache hot recommendations, and shift heavy analytics jobs outside the busiest clinic window to reduce assistant latency.', 'published', '2026-03-19');
INSERT INTO knowledge_articles VALUES (8005, 'onboarding', 'Template Cloning Guide for District Rollouts', 'District teams can clone workspace templates, copy role mappings, and validate launch checklists before each campus rollout.', 'published', '2026-04-01');

UPDATE support_tickets
SET status = 'in_progress',
    last_updated_date = '2026-04-05'
WHERE id = 7003;
