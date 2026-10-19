-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The dataroom demo's database: Northfield Supply's own order and finance system. 2,400 customers
-- (who they are: name, email, phone), their orders since 2023 and the invoice of each. Every value
-- is a function of the row's number, so every build holds the same rows. The data carries the
-- defects a diligence team looks for: customers with no email, phone numbers that are not ten
-- digits, and invoices never paid.

CREATE TABLE customers (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  email varchar,
  phone varchar NOT NULL,
  segment varchar NOT NULL,
  region varchar NOT NULL,
  first_order_date date NOT NULL
);

CREATE TABLE orders (
  id integer PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers (id),
  ordered_at date NOT NULL,
  channel varchar NOT NULL,
  amount numeric(12, 2) NOT NULL
);

CREATE TABLE invoices (
  id integer PRIMARY KEY,
  order_id integer NOT NULL REFERENCES orders (id),
  issued_at date NOT NULL,
  paid_at date,
  amount numeric(12, 2) NOT NULL
);

WITH firsts AS (
  SELECT ARRAY['Avery', 'Blake', 'Casey', 'Dana', 'Elliot', 'Frances', 'Glen', 'Harper', 'Iris',
               'Jordan', 'Kendall', 'Logan', 'Morgan', 'Noel', 'Owen', 'Parker', 'Quinn', 'Reese',
               'Sawyer', 'Taylor'] AS a
), lasts AS (
  SELECT ARRAY['Abbott', 'Barnes', 'Castillo', 'Dawson', 'Ellis', 'Fischer', 'Garcia', 'Holt',
               'Ingram', 'Jensen', 'Kim', 'Lowe', 'Moreno', 'Nash', 'Ortega', 'Patel', 'Quint',
               'Rivera', 'Shah', 'Tran', 'Underwood', 'Vance', 'Walsh', 'Young', 'Zimmer'] AS a
), people AS (
  SELECT i,
         firsts.a[(i % 20) + 1] AS first_name,
         lasts.a[((i * 7) % 25) + 1] AS last_name
  FROM generate_series(1, 2400) AS i, firsts, lasts
)
INSERT INTO customers
SELECT i,
       first_name || ' ' || last_name,
       CASE
         WHEN i % 37 = 0 THEN NULL  -- no email on file
         ELSE lower(first_name || '.' || last_name) || i || '@example.com'
       END,
       CASE
         WHEN i % 43 = 0 THEN '555-' || lpad((i % 1000)::text, 3, '0')  -- too short to dial
         ELSE '(' || (200 + i % 700) || ') 555-' || lpad((i % 10000)::text, 4, '0')
       END,
       CASE WHEN i % 10 < 6 THEN 'consumer' WHEN i % 10 < 9 THEN 'small business' ELSE 'enterprise' END,
       (ARRAY['northeast', 'south', 'midwest', 'west'])[(i % 4) + 1],
       DATE '2023-01-01' + ((i * 37) % 1095)
FROM people;

INSERT INTO orders
SELECT row_number() OVER (ORDER BY c.id, n),
       c.id,
       c.first_order_date + n * (20 + c.id % 60),
       (ARRAY['web', 'store', 'marketplace'])[((c.id + n) % 3) + 1],
       CASE c.segment
         WHEN 'consumer' THEN 40 + ((c.id * 31 + n * 17) % 200)
         WHEN 'small business' THEN 300 + ((c.id * 53 + n * 29) % 2000)
         ELSE 5000 + ((c.id * 97 + n * 41) % 25000)
       END
FROM customers AS c, generate_series(0, 11) AS n
WHERE n <= (c.id * 13) % 12
  AND c.first_order_date + n * (20 + c.id % 60) <= DATE '2026-09-30';

INSERT INTO invoices
SELECT o.id,
       o.id,
       o.ordered_at,
       CASE WHEN o.id % 53 = 0 THEN NULL ELSE o.ordered_at + (o.id % 45) END,  -- never paid
       o.amount
FROM orders AS o;
