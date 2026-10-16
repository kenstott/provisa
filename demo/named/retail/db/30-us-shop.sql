-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The US subsidiary's own database (homed in us): its loyalty-programme customers
-- with their marketing consent, their orders, its staff, and the mail its staff send outside the
-- group. Generated, and the same on every build: values are derived from row numbers.
\connect us_shop

CREATE TABLE customers (
  customer_id integer PRIMARY KEY,
  email varchar NOT NULL,
  full_name varchar NOT NULL,
  country char(2) NOT NULL,
  joined_on date NOT NULL,
  marketing_consent boolean NOT NULL,   -- the customer agreed to marketing
  consent_recorded_at timestamp         -- when; NULL when never asked
);
INSERT INTO customers
SELECT 200000 + n,
       lower((ARRAY['Ava','Ethan','Mia','Liam','Sofia','Mason','Harper','Logan','Ella','Jack'])[1 + n % 10]) || '.' || lower((ARRAY['Smith','Johnson','Brown','Davis','Miller','Wilson','Moore','Taylor','Clark','Lewis'])[1 + (n / 10) % 10]) || n || '@example.' || lower((ARRAY['US'])[1 + n % 1]),
       (ARRAY['Ava','Ethan','Mia','Liam','Sofia','Mason','Harper','Logan','Ella','Jack'])[1 + n % 10] || ' ' || (ARRAY['Smith','Johnson','Brown','Davis','Miller','Wilson','Moore','Taylor','Clark','Lewis'])[1 + (n / 10) % 10],
       (ARRAY['US'])[1 + n % 1],
       DATE '2021-01-01' + (abs(hashtext('joined:us_shop:' || n)) % 1500),
       abs(hashtext('consent:us_shop:' || n)) % 100 < 61,
       TIMESTAMP '2023-01-01' + (abs(hashtext('asked:us_shop:' || n)) % 600) * INTERVAL '1 day'
FROM generate_series(1, 2000) AS n;

CREATE TABLE staff (
  staff_id integer PRIMARY KEY,
  full_name varchar NOT NULL,
  email varchar NOT NULL,
  store_id integer NOT NULL,
  job varchar NOT NULL,                 -- sales_associate | store_manager | crm_analyst
  has_customer_access boolean NOT NULL  -- may read customer records
);
INSERT INTO staff
SELECT 2000 + n,
       (ARRAY['Ava','Ethan','Mia','Liam','Sofia','Mason','Harper','Logan','Ella','Jack'])[1 + (n * 3) % 10] || ' ' || (ARRAY['Smith','Johnson','Brown','Davis','Miller','Wilson','Moore','Taylor','Clark','Lewis'])[1 + (n * 7) % 10],
       'staff' || (2000 + n) || '@us.retailgroup.example',
       (ARRAY[7,8,9,10,11,12])[1 + n % 6],
       (ARRAY['sales_associate','sales_associate','store_manager','crm_analyst'])[1 + n % 4],
       n % 4 >= 2
FROM generate_series(1, 60) AS n;

CREATE TABLE orders (
  order_id integer PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customers (customer_id),
  store_id integer NOT NULL,
  product_id integer NOT NULL,
  placed_at timestamp NOT NULL,
  amount numeric(10, 2) NOT NULL
);
INSERT INTO orders
SELECT 200000 * 10 + n,
       200000 + 1 + abs(hashtext('buyer:us_shop:' || n)) % 2000,
       (ARRAY[7,8,9,10,11,12])[1 + abs(hashtext('store:us_shop:' || n)) % 6],
       1 + abs(hashtext('product:us_shop:' || n)) % 120,
       TIMESTAMP '2025-01-01' + (abs(hashtext('at:us_shop:' || n)) % 640) * INTERVAL '1 day',
       round((15 + abs(hashtext('amount:us_shop:' || n)) % 400)::numeric, 2)
FROM generate_series(1, 12000) AS n;

-- Mail staff sent outside the group. Most is ordinary supplier and customer-service mail; a few
-- staff with customer access mail customer lists to personal addresses, the cases the review finds.
CREATE TABLE outbound_mail (
  message_id integer PRIMARY KEY,
  staff_id integer NOT NULL REFERENCES staff (staff_id),
  recipient varchar NOT NULL,
  sent_at timestamp NOT NULL,
  subject varchar NOT NULL,
  body text NOT NULL,
  attached_customer_rows integer NOT NULL  -- customer records in the attachment; 0 when none
);
INSERT INTO outbound_mail
SELECT 600000 + n,
       2000 + 1 + abs(hashtext('sender:us_shop:' || n)) % 60,
       (ARRAY['orders@supplier-textiles.example','claims@courier.example','help@pos-vendor.example'])[1 + n % 3],
       TIMESTAMP '2026-09-01' + (abs(hashtext('sent:us_shop:' || n)) % 40) * INTERVAL '1 day',
       (ARRAY['Re: delivery window','Invoice query','Till receipt printer fault'])[1 + n % 3],
       'Hello, following up on our order and the delivery schedule for next week. Regards.',
       0
FROM generate_series(1, 900) AS n;
INSERT INTO outbound_mail VALUES
(609001, 2006, 'deals.personal@yahoo.com', '2026-09-30 21:33', 'list', 'top spenders, keep this between us', 760),
(609002, 2022, 'claims@courier.example', '2026-10-02 09:12', 'Re: claim 4471', 'photos attached', 0);

-- A loyalty member of the other subsidiary too, under the same email: what a request for access
-- has to find in both. (See the other subsidiary's script.)
-- Amélie Fournier, an EU customer, also joined the US programme on a visit to New York.
INSERT INTO customers VALUES (299001, 'amelie.fournier@example.fr', 'Amélie Fournier', 'FR', DATE '2025-08-20', false, NULL);
INSERT INTO orders VALUES (2990001, 299001, 7, 14, TIMESTAMP '2025-08-20 13:10', 210.00);

GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;
