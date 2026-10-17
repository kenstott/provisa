-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The storefront demo's database. TODO: the demo's own tables and rows.

CREATE TABLE customers (id integer PRIMARY KEY, name varchar NOT NULL);
CREATE TABLE orders (
  id integer PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers (id),
  amount numeric(10, 2) NOT NULL
);
INSERT INTO customers VALUES (1, 'Ada'), (2, 'Grace');
INSERT INTO orders VALUES (1, 1, 20.00), (2, 1, 35.50), (3, 2, 12.25);
