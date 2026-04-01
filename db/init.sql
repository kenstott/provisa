-- Demo schema for Provisa development
-- Two schemas: public (sales data) and analytics (reporting views)
-- Used to test cross-source routing through Trino

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    category VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    amount NUMERIC(10, 2) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    region VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Table and column descriptions (sourced into GraphQL SDL)
COMMENT ON TABLE customers IS 'Registered customer accounts';
COMMENT ON COLUMN customers.id IS 'Unique customer identifier';
COMMENT ON COLUMN customers.name IS 'Full name of the customer';
COMMENT ON COLUMN customers.email IS 'Primary email address';
COMMENT ON COLUMN customers.region IS 'Geographic region code (e.g., us-east, eu-west)';

COMMENT ON TABLE products IS 'Product catalog';
COMMENT ON COLUMN products.name IS 'Product display name';
COMMENT ON COLUMN products.price IS 'Unit price in USD';
COMMENT ON COLUMN products.category IS 'Product category (widgets, gadgets, etc.)';

COMMENT ON TABLE orders IS 'Customer purchase orders';
COMMENT ON COLUMN orders.customer_id IS 'FK to customers table';
COMMENT ON COLUMN orders.product_id IS 'FK to products table';
COMMENT ON COLUMN orders.amount IS 'Total order amount in USD';
COMMENT ON COLUMN orders.quantity IS 'Number of units ordered';
COMMENT ON COLUMN orders.region IS 'Region where the order was placed';
COMMENT ON COLUMN orders.status IS 'Order status: pending, shipped, delivered, cancelled';

-- Seed customers
INSERT INTO customers (name, email, region) VALUES
    ('Alice Johnson', 'alice@example.com', 'us-east'),
    ('Bob Smith', 'bob@example.com', 'us-west'),
    ('Carol White', 'carol@example.com', 'eu-west'),
    ('David Brown', 'david@example.com', 'us-east'),
    ('Eve Davis', 'eve@example.com', 'eu-west'),
    ('Frank Miller', 'frank@example.com', 'us-west'),
    ('Grace Wilson', 'grace@example.com', 'ap-south'),
    ('Henry Moore', 'henry@example.com', 'us-east'),
    ('Iris Taylor', 'iris@example.com', 'eu-west'),
    ('Jack Anderson', 'jack@example.com', 'ap-south'),
    ('Karen Thomas', 'karen@example.com', 'us-west'),
    ('Leo Jackson', 'leo@example.com', 'us-east'),
    ('Mia Harris', 'mia@example.com', 'eu-west'),
    ('Nathan Clark', 'nathan@example.com', 'ap-south'),
    ('Olivia Lewis', 'olivia@example.com', 'us-east'),
    ('Paul Robinson', 'paul@example.com', 'us-west'),
    ('Quinn Walker', 'quinn@example.com', 'eu-west'),
    ('Rachel Hall', 'rachel@example.com', 'ap-south'),
    ('Sam Allen', 'sam@example.com', 'us-east'),
    ('Tina Young', 'tina@example.com', 'us-west');

-- Seed products
INSERT INTO products (name, price, category) VALUES
    ('Widget A', 19.99, 'widgets'),
    ('Widget B', 29.99, 'widgets'),
    ('Gadget X', 49.99, 'gadgets'),
    ('Gadget Y', 79.99, 'gadgets'),
    ('Tool Alpha', 14.99, 'tools'),
    ('Tool Beta', 24.99, 'tools'),
    ('Part 001', 9.99, 'parts'),
    ('Part 002', 4.99, 'parts'),
    ('Premium Widget', 99.99, 'widgets'),
    ('Micro Gadget', 149.99, 'gadgets'),
    ('Power Tool', 59.99, 'tools'),
    ('Nano Part', 2.49, 'parts'),
    ('Super Widget', 199.99, 'widgets'),
    ('Ultra Gadget', 249.99, 'gadgets'),
    ('Pro Tool', 89.99, 'tools');

-- Seed orders
INSERT INTO orders (customer_id, product_id, amount, quantity, region, status, created_at) VALUES
    (1, 1, 19.99, 1, 'us-east', 'completed', '2025-01-15 10:30:00'),
    (1, 3, 99.98, 2, 'us-east', 'completed', '2025-01-20 14:00:00'),
    (2, 2, 29.99, 1, 'us-west', 'completed', '2025-02-01 09:15:00'),
    (3, 5, 44.97, 3, 'eu-west', 'shipped', '2025-02-10 11:00:00'),
    (4, 1, 39.98, 2, 'us-east', 'completed', '2025-02-15 16:45:00'),
    (5, 4, 79.99, 1, 'eu-west', 'pending', '2025-03-01 08:00:00'),
    (6, 6, 49.98, 2, 'us-west', 'shipped', '2025-03-05 12:30:00'),
    (7, 3, 49.99, 1, 'ap-south', 'completed', '2025-03-10 07:00:00'),
    (8, 9, 99.99, 1, 'us-east', 'completed', '2025-03-12 15:20:00'),
    (9, 10, 299.98, 2, 'eu-west', 'pending', '2025-03-15 10:00:00'),
    (10, 7, 29.97, 3, 'ap-south', 'shipped', '2025-03-18 13:00:00'),
    (2, 13, 199.99, 1, 'us-west', 'completed', '2025-03-20 09:00:00'),
    (1, 14, 249.99, 1, 'us-east', 'pending', '2025-03-22 11:30:00'),
    (3, 11, 59.99, 1, 'eu-west', 'completed', '2025-03-23 14:15:00'),
    (4, 8, 9.98, 2, 'us-east', 'shipped', '2025-03-24 08:45:00'),
    (11, 2, 59.98, 2, 'us-west', 'completed', '2025-03-25 10:00:00'),
    (12, 15, 89.99, 1, 'us-east', 'pending', '2025-03-26 16:00:00'),
    (13, 1, 19.99, 1, 'eu-west', 'completed', '2025-03-27 09:30:00'),
    (14, 12, 7.47, 3, 'ap-south', 'shipped', '2025-03-28 11:00:00'),
    (15, 3, 149.97, 3, 'us-east', 'completed', '2025-03-28 14:00:00'),
    (16, 4, 159.98, 2, 'us-west', 'pending', '2025-03-29 08:00:00'),
    (17, 9, 199.98, 2, 'eu-west', 'shipped', '2025-03-29 12:00:00'),
    (18, 10, 149.99, 1, 'ap-south', 'completed', '2025-03-30 07:30:00'),
    (19, 5, 14.99, 1, 'us-east', 'completed', '2025-03-30 10:00:00'),
    (20, 6, 74.95, 3, 'us-west', 'pending', '2025-03-30 15:00:00');

-- Second schema: analytics (simulates a separate data source)
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.customer_segments (
    customer_id INTEGER NOT NULL,
    segment VARCHAR(50) NOT NULL,
    score NUMERIC(5, 2) NOT NULL,
    assigned_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO analytics.customer_segments (customer_id, segment, score) VALUES
    (1, 'high-value', 92.5),
    (2, 'medium-value', 65.0),
    (3, 'high-value', 88.3),
    (4, 'low-value', 32.1),
    (5, 'medium-value', 55.7),
    (6, 'high-value', 91.0),
    (7, 'low-value', 28.4),
    (8, 'medium-value', 60.2),
    (9, 'high-value', 85.9),
    (10, 'low-value', 30.0);
