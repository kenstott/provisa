-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The streaming demo's catalog: the shop's products and the regions it sells in. It changes
-- rarely and is read live; the orders arrive on the Kafka topic `orders` (producer.py).

CREATE TABLE regions (
  region_id varchar PRIMARY KEY,
  name varchar NOT NULL
);
CREATE TABLE products (
  product_id integer PRIMARY KEY,
  name varchar NOT NULL,
  category varchar NOT NULL,
  unit_price numeric(10, 2) NOT NULL
);
INSERT INTO regions VALUES
  ('na-east', 'North America East'),
  ('na-west', 'North America West'),
  ('eu-west', 'Europe West'),
  ('eu-central', 'Europe Central'),
  ('apac', 'Asia Pacific'),
  ('latam', 'Latin America');
INSERT INTO products VALUES
  (1, 'Espresso beans, 1 kg', 'coffee', 24.00),
  (2, 'Decaf beans, 1 kg', 'coffee', 22.50),
  (3, 'Pour-over kettle', 'equipment', 49.00),
  (4, 'Burr grinder', 'equipment', 129.00),
  (5, 'Paper filters, 100', 'supplies', 6.50),
  (6, 'Travel mug', 'supplies', 18.00),
  (7, 'Cold-brew jar', 'equipment', 35.00),
  (8, 'Green tea, 250 g', 'tea', 14.00);
