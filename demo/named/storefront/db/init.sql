-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The storefront's order database: its customers, their orders and each order's lines. The
-- products the lines name are in the catalog (MongoDB, db/catalog.js) and each order's shipment in
-- the shipping service (shipping/). Every value is a function of its row's id, so every start of
-- the demo holds the same rows -- and the shipping service computes the same order dates.

CREATE TABLE customers (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  email varchar NOT NULL,
  region varchar NOT NULL
);
INSERT INTO customers
SELECT n,
       (ARRAY['Ada', 'Ben', 'Chloe', 'Dev', 'Elena', 'Femi', 'Grace', 'Hiro', 'Ines', 'Jonas',
              'Kofi', 'Lena', 'Mateo', 'Nora', 'Omar', 'Priya'])[1 + n % 16]
         || ' ' ||
       (ARRAY['Adams', 'Baker', 'Costa', 'Dubois', 'Evans', 'Fischer', 'Garcia', 'Huang', 'Ito',
              'Jensen', 'Khan', 'Lopez', 'Moreau', 'Nakamura', 'Okafor'])[1 + (n * 7) % 15],
       'customer' || n || '@example.com',
       -- East has the most customers, Central the fewest.
       (ARRAY['East', 'East', 'East', 'West', 'West', 'South', 'South', 'North', 'Central',
              'East'])[1 + (n * 3) % 10]
FROM generate_series(1, 400) AS n;

-- An order is placed every 40 minutes back from 2026-09-30; its status follows from its age.
CREATE TABLE orders (
  id integer PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers (id),
  placed_at timestamp NOT NULL,
  status varchar NOT NULL
);
INSERT INTO orders
SELECT n,
       1 + (n * 37) % 400,
       TIMESTAMP '2026-09-30 12:00' - n * INTERVAL '40 minutes',
       CASE WHEN n % 41 = 0 THEN 'cancelled' WHEN n < 60 THEN 'processing' ELSE 'shipped' END
FROM generate_series(1, 3000) AS n;

-- One to three lines an order, each a product of the catalog (ids 1..24).
CREATE TABLE order_items (
  order_id integer NOT NULL REFERENCES orders (id),
  line integer NOT NULL,
  product_id integer NOT NULL,
  quantity integer NOT NULL,
  unit_price numeric(10, 2) NOT NULL,
  PRIMARY KEY (order_id, line)
);
INSERT INTO order_items
SELECT o, l,
       1 + (o * 5 + l * 11) % 24,
       1 + (o + l) % 3,
       (ARRAY[12.50, 18.00, 24.00, 32.50, 45.00, 59.00, 79.00, 120.00])[1 + (o * 5 + l * 11) % 8]
FROM generate_series(1, 3000) AS o, generate_series(1, 3) AS l
WHERE l <= 1 + o % 3;
