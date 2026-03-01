-- ============================================================
-- Northwind Electronics — E-Commerce Store
-- ============================================================
-- An online electronics retailer with customers, product
-- catalog, orders, reviews, and shipping addresses.
-- ============================================================

-- ─── Tables ─────────────────────────────────────────────────

CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL,
    phone       TEXT,
    city        TEXT,
    joined_year INTEGER NOT NULL
);

CREATE TABLE categories (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT
);

CREATE TABLE products (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    price       REAL NOT NULL,
    stock       INTEGER NOT NULL,
    is_active   INTEGER NOT NULL
);

CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date  TEXT NOT NULL,
    status      TEXT NOT NULL,
    total       REAL NOT NULL
);

CREATE TABLE order_items (
    id          INTEGER PRIMARY KEY,
    order_id    INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  REAL NOT NULL
);

CREATE TABLE reviews (
    id          INTEGER PRIMARY KEY,
    product_id  INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    rating      INTEGER NOT NULL,
    comment     TEXT,
    review_date TEXT NOT NULL
);

CREATE TABLE shipping_addresses (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    street      TEXT NOT NULL,
    city        TEXT NOT NULL,
    state       TEXT NOT NULL,
    zip         TEXT NOT NULL
);

-- ─── Customers ──────────────────────────────────────────────

INSERT INTO customers VALUES (1, 'Alice Johnson', 'alice@example.com', '555-0101', 'Seattle', 2021);
INSERT INTO customers VALUES (2, 'Bob Martinez', 'bob.m@example.com', '555-0102', 'Portland', 2022);
INSERT INTO customers VALUES (3, 'Carol Chen', 'carol.chen@example.com', '555-0103', 'San Francisco', 2021);
INSERT INTO customers VALUES (4, 'David Kim', 'dkim@example.com', '555-0104', 'Los Angeles', 2023);
INSERT INTO customers VALUES (5, 'Emma Wilson', 'emma.w@example.com', '555-0105', 'Denver', 2022);
INSERT INTO customers VALUES (6, 'Frank Patel', 'frank.p@example.com', '555-0106', 'Austin', 2023);
INSERT INTO customers VALUES (7, 'Grace Lee', 'grace.lee@example.com', '555-0107', 'Chicago', 2024);
INSERT INTO customers VALUES (8, 'Henry Nguyen', 'henry.n@example.com', '555-0108', 'Miami', 2024);

-- ─── Categories ─────────────────────────────────────────────

INSERT INTO categories VALUES (1, 'Laptops', 'Notebook computers and ultrabooks');
INSERT INTO categories VALUES (2, 'Phones', 'Smartphones and accessories');
INSERT INTO categories VALUES (3, 'Audio', 'Headphones, speakers, and earbuds');
INSERT INTO categories VALUES (4, 'Accessories', 'Cables, cases, and peripherals');
INSERT INTO categories VALUES (5, 'Monitors', 'Desktop displays and portable monitors');

-- ─── Products ───────────────────────────────────────────────

INSERT INTO products VALUES (1,  'ProBook 15',         1, 1299.99, 24, 1);
INSERT INTO products VALUES (2,  'UltraSlim Air',      1,  999.99, 18, 1);
INSERT INTO products VALUES (3,  'WorkStation X1',     1, 2199.99,  7, 1);
INSERT INTO products VALUES (4,  'Galaxy Pro 14',      2,  899.99, 45, 1);
INSERT INTO products VALUES (5,  'iPhone Ultra',       2, 1199.99, 32, 1);
INSERT INTO products VALUES (6,  'Budget Phone SE',    2,  349.99, 60, 1);
INSERT INTO products VALUES (7,  'SoundWave Pro',      3,  249.99, 38, 1);
INSERT INTO products VALUES (8,  'BassBuds Elite',     3,  179.99, 55, 1);
INSERT INTO products VALUES (9,  'StudioMonitor 50',   3,  399.99, 12, 1);
INSERT INTO products VALUES (10, 'USB-C Hub 7-in-1',   4,   59.99, 80, 1);
INSERT INTO products VALUES (11, 'Wireless Charger',   4,   39.99, 95, 1);
INSERT INTO products VALUES (12, 'ClearView 27 4K',    5,  549.99, 15, 1);

-- ─── Orders ─────────────────────────────────────────────────

INSERT INTO orders VALUES (1,  1, '2025-01-15', 'delivered',  1349.98);
INSERT INTO orders VALUES (2,  2, '2025-01-22', 'delivered',   899.99);
INSERT INTO orders VALUES (3,  3, '2025-02-03', 'delivered',  2259.98);
INSERT INTO orders VALUES (4,  1, '2025-02-14', 'delivered',   289.98);
INSERT INTO orders VALUES (5,  4, '2025-03-01', 'shipped',   1199.99);
INSERT INTO orders VALUES (6,  5, '2025-03-10', 'shipped',    649.98);
INSERT INTO orders VALUES (7,  6, '2025-03-18', 'processing', 999.99);
INSERT INTO orders VALUES (8,  7, '2025-03-25', 'processing', 429.98);
INSERT INTO orders VALUES (9,  3, '2025-04-02', 'pending',    179.99);
INSERT INTO orders VALUES (10, 8, '2025-04-10', 'pending',   1849.98);

-- ─── Order Items ────────────────────────────────────────────

INSERT INTO order_items VALUES (1,  1,  1,  1, 1299.99);
INSERT INTO order_items VALUES (2,  1,  10, 1,   59.99);
INSERT INTO order_items VALUES (3,  2,  4,  1,  899.99);
INSERT INTO order_items VALUES (4,  3,  3,  1, 2199.99);
INSERT INTO order_items VALUES (5,  3,  10, 1,   59.99);
INSERT INTO order_items VALUES (6,  4,  7,  1,  249.99);
INSERT INTO order_items VALUES (7,  4,  11, 1,   39.99);
INSERT INTO order_items VALUES (8,  5,  5,  1, 1199.99);
INSERT INTO order_items VALUES (9,  6,  12, 1,  549.99);
INSERT INTO order_items VALUES (10, 6,  10, 1,   59.99);
INSERT INTO order_items VALUES (11, 6,  11, 1,   39.99);
INSERT INTO order_items VALUES (12, 7,  2,  1,  999.99);
INSERT INTO order_items VALUES (13, 8,  7,  1,  249.99);
INSERT INTO order_items VALUES (14, 8,  8,  1,  179.99);
INSERT INTO order_items VALUES (15, 9,  8,  1,  179.99);
INSERT INTO order_items VALUES (16, 10, 1,  1, 1299.99);
INSERT INTO order_items VALUES (17, 10, 12, 1,  549.99);

-- ─── Reviews ────────────────────────────────────────────────

INSERT INTO reviews VALUES (1, 1, 1, 5, 'Incredible laptop, fast and lightweight.',          '2025-02-01');
INSERT INTO reviews VALUES (2, 4, 2, 4, 'Great phone for the price. Camera could be better.', '2025-02-10');
INSERT INTO reviews VALUES (3, 7, 1, 5, 'Best headphones I have ever owned.',                '2025-03-05');
INSERT INTO reviews VALUES (4, 3, 3, 5, 'Perfect for software development.',                 '2025-02-20');
INSERT INTO reviews VALUES (5, 5, 4, 4, 'Beautiful display, battery life is decent.',         '2025-03-15');
INSERT INTO reviews VALUES (6, 8, 5, 3, 'Sound is okay, fit is a bit loose.',                '2025-03-20');
INSERT INTO reviews VALUES (7, 10, 3, 5, 'Exactly what I needed for my desk setup.',          '2025-02-18');
INSERT INTO reviews VALUES (8, 12, 6, 4, 'Sharp display, colors are accurate.',               '2025-04-01');

-- ─── Shipping Addresses ─────────────────────────────────────

INSERT INTO shipping_addresses VALUES (1,  1, '742 Evergreen Terrace',   'Seattle',       'WA', '98101');
INSERT INTO shipping_addresses VALUES (2,  1, '100 Market Street',       'Seattle',       'WA', '98102');
INSERT INTO shipping_addresses VALUES (3,  2, '221B Baker Street',       'Portland',      'OR', '97201');
INSERT INTO shipping_addresses VALUES (4,  3, '1600 Amphitheatre Pkwy',  'San Francisco', 'CA', '94102');
INSERT INTO shipping_addresses VALUES (5,  4, '350 Fifth Avenue',        'Los Angeles',   'CA', '90001');
INSERT INTO shipping_addresses VALUES (6,  5, '1 Infinite Loop',         'Denver',        'CO', '80201');
INSERT INTO shipping_addresses VALUES (7,  6, '2000 Congress Ave',       'Austin',        'TX', '78701');
INSERT INTO shipping_addresses VALUES (8,  7, '233 S Wacker Drive',      'Chicago',       'IL', '60606');
INSERT INTO shipping_addresses VALUES (9,  8, '1000 Ocean Drive',        'Miami',         'FL', '33139');
INSERT INTO shipping_addresses VALUES (10, 8, '500 Brickell Ave',        'Miami',         'FL', '33131');

-- ─── Indexes ────────────────────────────────────────────────

CREATE INDEX idx_products_category ON products (category_id);
CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_order_items_order ON order_items (order_id);
CREATE INDEX idx_reviews_product ON reviews (product_id);

-- ─── Views ──────────────────────────────────────────────────

CREATE VIEW order_summary AS
SELECT o.id, c.name, o.order_date, o.status, o.total
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id;

CREATE VIEW product_catalog AS
SELECT p.id, p.name, cat.name, p.price, p.stock
FROM products p
INNER JOIN categories cat ON cat.id = p.category_id
WHERE p.is_active = 1;

-- ─── Triggers ───────────────────────────────────────────────

CREATE TRIGGER trg_update_stock AFTER INSERT ON order_items
BEGIN
    UPDATE products SET stock = stock - NEW.quantity WHERE id = NEW.product_id;
END;
