-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The EU subsidiary's own database (homed in eu): its loyalty-programme customers
-- with their marketing consent, their orders, its staff, and the mail its staff send outside the
-- group. Generated, and the same on every build: values are derived from row numbers.
\connect eu_shop

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
SELECT 100000 + n,
       lower((ARRAY['Lena','Jonas','Chloé','Mateo','Giulia','Sven','Aoife','Lucas','Marta','Noah'])[1 + n % 10]) || '.' || lower((ARRAY['Weber','Dubois','García','Rossi','Jansen','Murphy','Schmidt','Moreau','López','Bianchi'])[1 + (n / 10) % 10]) || n || '@example.' || lower((ARRAY['DE','FR','ES','IT','NL','IE'])[1 + n % 6]),
       (ARRAY['Lena','Jonas','Chloé','Mateo','Giulia','Sven','Aoife','Lucas','Marta','Noah'])[1 + n % 10] || ' ' || (ARRAY['Weber','Dubois','García','Rossi','Jansen','Murphy','Schmidt','Moreau','López','Bianchi'])[1 + (n / 10) % 10],
       (ARRAY['DE','FR','ES','IT','NL','IE'])[1 + n % 6],
       DATE '2021-01-01' + (abs(hashtext('joined:eu_shop:' || n)) % 1500),
       abs(hashtext('consent:eu_shop:' || n)) % 100 < 38,
       TIMESTAMP '2023-01-01' + (abs(hashtext('asked:eu_shop:' || n)) % 600) * INTERVAL '1 day'
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
SELECT 1000 + n,
       (ARRAY['Lena','Jonas','Chloé','Mateo','Giulia','Sven','Aoife','Lucas','Marta','Noah'])[1 + (n * 3) % 10] || ' ' || (ARRAY['Weber','Dubois','García','Rossi','Jansen','Murphy','Schmidt','Moreau','López','Bianchi'])[1 + (n * 7) % 10],
       'staff' || (1000 + n) || '@eu.retailgroup.example',
       (ARRAY[1,2,3,4,5,6])[1 + n % 6],
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
SELECT 100000 * 10 + n,
       100000 + 1 + abs(hashtext('buyer:eu_shop:' || n)) % 2000,
       (ARRAY[1,2,3,4,5,6])[1 + abs(hashtext('store:eu_shop:' || n)) % 6],
       1 + abs(hashtext('product:eu_shop:' || n)) % 120,
       TIMESTAMP '2025-01-01' + (abs(hashtext('at:eu_shop:' || n)) % 640) * INTERVAL '1 day',
       round((15 + abs(hashtext('amount:eu_shop:' || n)) % 400)::numeric, 2)
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
SELECT 500000 + n,
       1000 + 1 + abs(hashtext('sender:eu_shop:' || n)) % 60,
       (ARRAY['orders@supplier-textiles.example','claims@courier.example','help@pos-vendor.example'])[1 + n % 3],
       TIMESTAMP '2026-09-01' + (abs(hashtext('sent:eu_shop:' || n)) % 40) * INTERVAL '1 day',
       (ARRAY['Re: delivery window','Invoice query','Till receipt printer fault'])[1 + n % 3],
       'Hello, following up on our order and the delivery schedule for next week. Regards.',
       0
FROM generate_series(1, 900) AS n;
INSERT INTO outbound_mail VALUES
(509001, 1003, 'lena.private@gmail.com', '2026-09-28 22:14', 'customer list', 'as discussed - the VIP list for the new place', 1840),
(509002, 1003, 'lena.private@gmail.com', '2026-10-03 23:02', 'updated', 'latest export attached', 1920),
(509003, 1011, 'mk.sales@outlook.com', '2026-10-06 19:40', 'contacts', 'my customers from the Marais store', 412),
(509004, 1018, 'orders@supplier-textiles.example', '2026-10-07 10:05', 'Re: delivery window', 'see attached schedule', 0);

-- A loyalty member of the other subsidiary too, under the same email: what a request for access
-- has to find in both. (See the other subsidiary's script.)
INSERT INTO customers VALUES (199001, 'amelie.fournier@example.fr', 'Amélie Fournier', 'FR', DATE '2022-04-02', true, TIMESTAMP '2023-05-11 09:00'),
  (199002, 'tom.becker@example.de', 'Tom Becker', 'DE', DATE '2021-11-19', false, NULL);
INSERT INTO orders VALUES (1990001, 199001, 2, 14, TIMESTAMP '2026-02-14 15:20', 189.00),
  (1990002, 199001, 2, 61, TIMESTAMP '2026-06-01 11:05', 74.50),
  (1990003, 199002, 1, 33, TIMESTAMP '2025-12-03 17:45', 129.00);

GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;
