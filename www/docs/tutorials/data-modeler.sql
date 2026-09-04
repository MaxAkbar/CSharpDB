-- Data Modeler tutorial: run once in a new, disposable database.
-- These are fictional records. This script creates objects; it is not a migration.
CREATE TABLE customers (
    tenant_id INTEGER NOT NULL,
    id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    email VARCHAR(120),
    CONSTRAINT pk_customers PRIMARY KEY (tenant_id, id)
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku VARCHAR(30) NOT NULL,
    name VARCHAR(80) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT uq_products_sku UNIQUE (sku)
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    order_number VARCHAR(30) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'new',
    CONSTRAINT uq_orders_number UNIQUE (order_number),
    CONSTRAINT fk_orders_customer FOREIGN KEY (tenant_id, customer_id)
        REFERENCES customers (tenant_id, id)
);

CREATE TABLE order_lines (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT ck_order_lines_quantity CHECK (quantity > 0),
    CONSTRAINT fk_order_lines_order FOREIGN KEY (order_id) REFERENCES orders (id),
    CONSTRAINT fk_order_lines_product FOREIGN KEY (product_id) REFERENCES products (id)
);

CREATE TABLE shipments (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    tracking_code VARCHAR(50),
    CONSTRAINT fk_shipments_order FOREIGN KEY (order_id) REFERENCES orders (id)
);

INSERT INTO customers VALUES (1, 101, 'Northwind Workshop', 'orders@example.test');
INSERT INTO products VALUES (10, 'DESK-LAMP', 'Desk lamp', 39.50);
INSERT INTO products VALUES (11, 'NOTEBOOK', 'Notebook', 8.00);
INSERT INTO orders VALUES (1001, 1, 101, 'ORD-1001', 'new');
INSERT INTO orders VALUES (1002, 1, 101, 'ORD-1002', 'packed');
INSERT INTO order_lines VALUES (1, 1001, 10, 2);
INSERT INTO order_lines VALUES (2, 1001, 11, 4);
INSERT INTO order_lines VALUES (3, 1002, 11, 1);
INSERT INTO shipments VALUES (501, 1002, 'DEMO-TRACK-501');
