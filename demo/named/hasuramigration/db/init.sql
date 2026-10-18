-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The hasuramigration demo's database: the outfitter's store its Hasura app served, with the
-- Salesforce account each business customer belongs to (customers.sf_account_id).

-- The outfitter's store database, as its Hasura app read and wrote it.
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
  customer_id integer NOT NULL REFERENCES customers (id),
  status text NOT NULL DEFAULT 'placed',
  ship_to text,
  placed_at timestamptz NOT NULL DEFAULT now(),
  total_cents integer NOT NULL DEFAULT 0
);
CREATE TABLE order_items (
  id integer PRIMARY KEY,
  order_id integer NOT NULL REFERENCES orders (id),
  product_id integer NOT NULL REFERENCES products (id),
  quantity integer NOT NULL,
  unit_price_cents integer NOT NULL
);
-- The Hasura computed field customers.full_name and tracked function search_products.
CREATE FUNCTION customer_full_name(customer_row customers) RETURNS text
  LANGUAGE sql STABLE AS $$ SELECT customer_row.first_name || ' ' || customer_row.last_name $$;
CREATE FUNCTION search_products(search text) RETURNS SETOF products
  LANGUAGE sql STABLE AS $$ SELECT * FROM products WHERE name ILIKE '%' || search || '%' $$;

INSERT INTO customers (id, first_name, last_name, email, sf_account_id, created_at) VALUES
  (1, 'Ana', 'Ruiz', 'ana.ruiz@grandhotels.example', '001gK00001Za2LGQAZ', '2026-02-11T10:00:00Z'),
  (2, 'Ben', 'Okafor', 'ben.okafor@edgecomm.example', '001gK00001Za2LCQAZ', '2026-03-12T10:00:00Z'),
  (3, 'Chloe', 'Martin', 'chloe@example.com', NULL, '2026-01-13T10:00:00Z'),
  (4, 'Dev', 'Patel', 'dev.patel@uog.example', '001gK00001Za2LHQAZ', '2026-02-14T10:00:00Z'),
  (5, 'Elena', 'Sokolova', 'elena@burlington.example', '001gK00001Za2LDQAZ', '2026-03-15T10:00:00Z'),
  (6, 'Farid', 'Haddad', 'farid@example.com', NULL, '2026-01-16T10:00:00Z'),
  (7, 'Grace', 'Lin', 'grace.lin@genepoint.example', '001gK00001Za2LMQAZ', '2026-02-17T10:00:00Z'),
  (8, 'Hugo', 'Bernard', 'hugo@pyramid.example', '001gK00001Za2LEQAZ', '2026-03-18T10:00:00Z'),
  (9, 'Ines', 'Costa', 'ines@expresslogistics.example', '001gK00001Za2LIQAZ', '2026-01-19T10:00:00Z'),
  (10, 'Jonas', 'Weber', 'jonas@dickenson.example', '001gK00001Za2LFQAZ', '2026-02-20T10:00:00Z'),
  (11, 'Kemi', 'Adeyemi', 'kemi@example.com', NULL, '2026-03-21T10:00:00Z'),
  (12, 'Liam', 'Murphy', 'liam.murphy@arizona.example', '001gK00001Za2LJQAZ', '2026-01-22T10:00:00Z');
INSERT INTO products VALUES
  (1, 'TNT-2P', 'Two-person tent', 18900, true),
  (2, 'TNT-4P', 'Four-person tent', 32900, true),
  (3, 'BAG-20', 'Down sleeping bag', 24900, true),
  (4, 'BAG-40', 'Synthetic sleeping bag', 12900, true),
  (5, 'STV-01', 'Camp stove', 6900, true),
  (6, 'PAD-01', 'Sleeping pad', 8900, true),
  (7, 'PCK-55', '55 L backpack', 21900, true),
  (8, 'LMP-01', 'Headlamp', 3900, true),
  (9, 'CHR-01', 'Camp chair', 5900, true),
  (10, 'TNT-OLD', 'Old tent (discontinued)', 9900, false);
INSERT INTO orders (id, customer_id, status, ship_to, placed_at, total_cents) VALUES
  (1, 6, 'shipped', '69 Elm St', '2026-07-12T19:00:00Z', 32900),
  (2, 6, 'placed', '56 Main St', '2026-08-09T12:00:00Z', 32900),
  (3, 2, 'shipped', '74 Lake Dr', '2026-04-25T17:00:00Z', 70700),
  (4, 7, 'placed', '72 Oak Ave', '2026-05-28T09:00:00Z', 95400),
  (5, 1, 'shipped', '41 Main St', '2026-07-06T10:00:00Z', 169000),
  (6, 10, 'cancelled', '11 Lake Dr', '2026-07-03T13:00:00Z', 49800),
  (7, 5, 'cancelled', '97 Pine Rd', '2026-06-28T16:00:00Z', 143400),
  (8, 3, 'cancelled', '89 Pine Rd', '2026-07-18T09:00:00Z', 66700),
  (9, 10, 'cancelled', '61 Elm St', '2026-08-28T16:00:00Z', 98700),
  (10, 1, 'shipped', '60 Pine Rd', '2026-09-14T18:00:00Z', 45200),
  (11, 3, 'packed', '17 Oak Ave', '2026-08-06T09:00:00Z', 6900),
  (12, 1, 'delivered', '71 Pine Rd', '2026-08-07T10:00:00Z', 15600),
  (13, 3, 'delivered', '49 Oak Ave', '2026-08-20T13:00:00Z', 34700),
  (14, 3, 'packed', '2 Main St', '2026-05-17T11:00:00Z', 12900),
  (15, 10, 'shipped', '69 Pine Rd', '2026-06-08T13:00:00Z', 99600),
  (16, 10, 'delivered', '51 Elm St', '2026-05-04T17:00:00Z', 178800),
  (17, 8, 'delivered', '21 Elm St', '2026-04-17T12:00:00Z', 51600),
  (18, 6, 'placed', '79 Elm St', '2026-04-28T09:00:00Z', 63700),
  (19, 2, 'shipped', '78 Pine Rd', '2026-09-06T15:00:00Z', 20700),
  (20, 8, 'packed', '19 Elm St', '2026-05-01T16:00:00Z', 17600),
  (21, 12, 'delivered', '98 Lake Dr', '2026-06-08T16:00:00Z', 145400),
  (22, 5, 'packed', '82 Oak Ave', '2026-06-07T17:00:00Z', 76500),
  (23, 1, 'shipped', '4 Pine Rd', '2026-06-02T15:00:00Z', 76200),
  (24, 8, 'shipped', '29 Elm St', '2026-05-21T18:00:00Z', 20600),
  (25, 4, 'cancelled', '62 Pine Rd', '2026-05-22T14:00:00Z', 3900),
  (26, 11, 'packed', '23 Main St', '2026-09-18T10:00:00Z', 16800),
  (27, 11, 'delivered', '93 Oak Ave', '2026-04-24T15:00:00Z', 54800),
  (28, 3, 'shipped', '77 Main St', '2026-04-09T11:00:00Z', 35700),
  (29, 11, 'delivered', '93 Elm St', '2026-05-11T17:00:00Z', 47700),
  (30, 9, 'shipped', '28 Pine Rd', '2026-07-22T12:00:00Z', 56700),
  (31, 9, 'shipped', '95 Pine Rd', '2026-08-30T14:00:00Z', 33700),
  (32, 8, 'delivered', '20 Oak Ave', '2026-08-08T11:00:00Z', 159300),
  (33, 3, 'cancelled', '67 Lake Dr', '2026-09-07T10:00:00Z', 31700),
  (34, 9, 'cancelled', '36 Elm St', '2026-04-29T17:00:00Z', 25800),
  (35, 2, 'cancelled', '79 Lake Dr', '2026-08-23T09:00:00Z', 11700),
  (36, 10, 'shipped', '58 Oak Ave', '2026-06-11T16:00:00Z', 37300),
  (37, 7, 'packed', '28 Pine Rd', '2026-07-11T16:00:00Z', 144500),
  (38, 2, 'shipped', '29 Elm St', '2026-09-13T19:00:00Z', 77400),
  (39, 7, 'cancelled', '66 Main St', '2026-05-13T19:00:00Z', 99600),
  (40, 6, 'delivered', '3 Pine Rd', '2026-05-22T14:00:00Z', 41800);
SELECT setval('orders_id_seq', 40);
INSERT INTO order_items VALUES
  (1, 1, 2, 1, 32900),
  (2, 2, 2, 1, 32900),
  (3, 3, 1, 2, 18900),
  (4, 3, 2, 1, 32900),
  (5, 4, 3, 2, 24900),
  (6, 4, 5, 1, 6900),
  (7, 4, 4, 3, 12900),
  (8, 5, 2, 2, 32900),
  (9, 5, 1, 4, 18900),
  (10, 5, 5, 4, 6900),
  (11, 6, 3, 2, 24900),
  (12, 7, 2, 4, 32900),
  (13, 7, 9, 2, 5900),
  (14, 8, 2, 1, 32900),
  (15, 8, 6, 1, 8900),
  (16, 8, 3, 1, 24900),
  (17, 9, 2, 3, 32900),
  (18, 10, 8, 4, 3900),
  (19, 10, 5, 3, 6900),
  (20, 10, 6, 1, 8900),
  (21, 11, 5, 1, 6900),
  (22, 12, 8, 4, 3900),
  (23, 13, 7, 1, 21900),
  (24, 13, 6, 1, 8900),
  (25, 13, 8, 1, 3900),
  (26, 14, 4, 1, 12900),
  (27, 15, 3, 4, 24900),
  (28, 16, 1, 4, 18900),
  (29, 16, 8, 4, 3900),
  (30, 16, 7, 4, 21900),
  (31, 17, 4, 4, 12900),
  (32, 18, 3, 1, 24900),
  (33, 18, 2, 1, 32900),
  (34, 18, 9, 1, 5900),
  (35, 19, 5, 3, 6900),
  (36, 20, 8, 3, 3900),
  (37, 20, 9, 1, 5900),
  (38, 21, 3, 3, 24900),
  (39, 21, 1, 2, 18900),
  (40, 21, 2, 1, 32900),
  (41, 22, 3, 2, 24900),
  (42, 22, 6, 3, 8900),
  (43, 23, 4, 4, 12900),
  (44, 23, 9, 3, 5900),
  (45, 23, 5, 1, 6900),
  (46, 24, 8, 3, 3900),
  (47, 24, 6, 1, 8900),
  (48, 25, 8, 1, 3900),
  (49, 26, 4, 1, 12900),
  (50, 26, 8, 1, 3900),
  (51, 27, 7, 1, 21900),
  (52, 27, 2, 1, 32900),
  (53, 28, 8, 1, 3900),
  (54, 28, 3, 1, 24900),
  (55, 28, 5, 1, 6900),
  (56, 29, 3, 1, 24900),
  (57, 29, 1, 1, 18900),
  (58, 29, 8, 1, 3900),
  (59, 30, 1, 3, 18900),
  (60, 31, 9, 2, 5900),
  (61, 31, 7, 1, 21900),
  (62, 32, 3, 4, 24900),
  (63, 32, 1, 2, 18900),
  (64, 32, 7, 1, 21900),
  (65, 33, 1, 1, 18900),
  (66, 33, 6, 1, 8900),
  (67, 33, 8, 1, 3900),
  (68, 34, 4, 2, 12900),
  (69, 35, 8, 3, 3900),
  (70, 36, 9, 2, 5900),
  (71, 36, 8, 3, 3900),
  (72, 36, 5, 2, 6900),
  (73, 37, 2, 4, 32900),
  (74, 37, 4, 1, 12900),
  (75, 38, 3, 2, 24900),
  (76, 38, 5, 4, 6900),
  (77, 39, 3, 4, 24900),
  (78, 40, 2, 1, 32900),
  (79, 40, 6, 1, 8900);
