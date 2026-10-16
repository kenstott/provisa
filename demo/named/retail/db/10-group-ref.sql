-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The group's shared reference data: what it sells and where. No customer data.
\connect group_ref

CREATE TABLE products (
  product_id integer PRIMARY KEY,
  name varchar NOT NULL,
  category varchar NOT NULL,
  list_price numeric(10, 2) NOT NULL
);
INSERT INTO products
SELECT n,
       (ARRAY['Linen shirt','Wool coat','Trail shoe','Leather bag','Silk scarf','Denim jacket','Rain shell','Knit hat'])[1 + n % 8] || ' ' || n,
       (ARRAY['apparel','outerwear','footwear','accessories'])[1 + n % 4],
       round((19 + (abs(hashtext('price:' || n)) % 281))::numeric, 2)
FROM generate_series(1, 120) AS n;

CREATE TABLE stores (
  store_id integer PRIMARY KEY,
  name varchar NOT NULL,
  city varchar NOT NULL,
  country char(2) NOT NULL,
  region varchar NOT NULL  -- eu | us: where the store's subsidiary keeps its data
);
INSERT INTO stores VALUES
  (1, 'Berlin Mitte', 'Berlin', 'DE', 'eu'), (2, 'Paris Marais', 'Paris', 'FR', 'eu'),
  (3, 'Madrid Sol', 'Madrid', 'ES', 'eu'), (4, 'Milan Brera', 'Milan', 'IT', 'eu'),
  (5, 'Amsterdam Centrum', 'Amsterdam', 'NL', 'eu'), (6, 'Dublin Grafton', 'Dublin', 'IE', 'eu'),
  (7, 'New York SoHo', 'New York', 'US', 'us'), (8, 'Chicago Loop', 'Chicago', 'US', 'us'),
  (9, 'Austin Domain', 'Austin', 'US', 'us'), (10, 'Seattle Pike', 'Seattle', 'US', 'us'),
  (11, 'Denver LoDo', 'Denver', 'US', 'us'), (12, 'Miami Brickell', 'Miami', 'US', 'us');

CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;

-- The group's register of data subject requests: who asked, for what, when. What the access
-- view and the erasure view are driven by.
CREATE TABLE subject_requests (
  request_id integer PRIMARY KEY,
  email varchar NOT NULL,
  kind varchar NOT NULL,         -- access | erasure
  received_at timestamp NOT NULL
);
INSERT INTO subject_requests VALUES
  (1, 'amelie.fournier@example.fr', 'access', TIMESTAMP '2026-10-09 10:30'),
  (2, 'tom.becker@example.de', 'erasure', TIMESTAMP '2026-10-12 16:05');
GRANT SELECT ON subject_requests TO provisa_reader;
