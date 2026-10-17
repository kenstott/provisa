DROP TABLE IF EXISTS order_items, orders, products, customers CASCADE;
CREATE TABLE customers (
  id integer PRIMARY KEY,
  first_name text NOT NULL,
  last_name text NOT NULL,
  email text NOT NULL,
  sf_account_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE products (
  id integer PRIMARY KEY,
  sku text NOT NULL,
  name text NOT NULL,
  price_cents integer NOT NULL,
  active boolean NOT NULL DEFAULT true
);
CREATE TABLE orders (
  id serial PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers(id),
  status text NOT NULL DEFAULT 'placed',
  ship_to text,
  placed_at timestamptz NOT NULL DEFAULT now(),
  total_cents integer NOT NULL DEFAULT 0
);
CREATE TABLE order_items (
  id integer PRIMARY KEY,
  order_id integer NOT NULL REFERENCES orders(id),
  product_id integer NOT NULL REFERENCES products(id),
  quantity integer NOT NULL,
  unit_price_cents integer NOT NULL
);
CREATE OR REPLACE FUNCTION customer_full_name(customer_row customers) RETURNS text
  LANGUAGE sql STABLE AS $$ SELECT customer_row.first_name || ' ' || customer_row.last_name $$;
CREATE OR REPLACE FUNCTION search_products(search text) RETURNS SETOF products
  LANGUAGE sql STABLE AS $$ SELECT * FROM products WHERE name ILIKE '%' || search || '%' $$;
INSERT INTO customers VALUES
  (1, 'Ana', 'Ruiz', 'ana@example.com', '001A000001', '2026-01-04T10:00:00Z'),
  (2, 'Ben', 'Okafor', 'ben@example.com', '001A000002', '2026-02-11T15:30:00Z'),
  (3, 'Chloe', 'Martin', 'chloe@example.com', NULL, '2026-03-20T09:15:00Z');
INSERT INTO products VALUES
  (1, 'TNT-2P', 'Two-person tent', 18900, true),
  (2, 'BAG-20', 'Down sleeping bag', 24900, true),
  (3, 'STV-01', 'Camp stove', 6900, true),
  (4, 'TNT-OLD', 'Old tent (discontinued)', 9900, false);
INSERT INTO orders (id, customer_id, status, ship_to, placed_at, total_cents) VALUES
  (1, 1, 'shipped', '12 Elm St', '2026-09-01T12:00:00Z', 25800),
  (2, 1, 'placed', '12 Elm St', '2026-10-10T08:00:00Z', 24900),
  (3, 2, 'packed', '9 Oak Ave', '2026-10-12T17:45:00Z', 18900),
  (4, 3, 'cancelled', '4 Pine Rd', '2026-10-13T11:20:00Z', 6900);
SELECT setval('orders_id_seq', 4);
INSERT INTO order_items VALUES
  (1, 1, 1, 1, 18900), (2, 1, 3, 1, 6900), (3, 2, 2, 1, 24900),
  (4, 3, 1, 1, 18900), (5, 4, 3, 1, 6900);
