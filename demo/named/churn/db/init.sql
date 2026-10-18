-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The churn demo's database: a subscription business's customers and a year of their monthly
-- usage. Some customers have cancelled. Those who log in less and open more support tickets cancel
-- more often, and customers on the basic plan more often than on pro or team, so a model trained on
-- the usage can tell who is at risk. Generated with a fixed seed: every build has the same rows.

SELECT setseed(0.42);

CREATE TABLE customers (
  id integer PRIMARY KEY,
  email varchar NOT NULL,
  region varchar NOT NULL,
  plan varchar NOT NULL,
  tenure_months integer NOT NULL,
  cancelled_on date
);

CREATE TABLE usage (
  customer_id integer NOT NULL REFERENCES customers (id),
  month date NOT NULL,
  logins integer NOT NULL,
  support_tickets integer NOT NULL,
  PRIMARY KEY (customer_id, month)
);

-- Each customer's own habits: how engaged they are and how much trouble they have.
CREATE TEMP TABLE habits AS
SELECT i AS id, random() AS engagement, random() AS friction, random() AS luck
FROM generate_series(1, 1500) AS i;

INSERT INTO customers (id, email, region, plan, tenure_months)
SELECT
  h.id,
  lower((ARRAY['ana', 'ben', 'chen', 'dara', 'eli', 'fatima', 'gus', 'hana', 'ivan', 'jo'])
    [1 + h.id % 10]) || '.' ||
  lower((ARRAY['silva', 'moreau', 'okafor', 'novak', 'tanaka', 'berg', 'rossi', 'khan'])
    [1 + (h.id / 10) % 8]) || h.id || '@' ||
  (ARRAY['example.com', 'mail.example', 'corp.example'])[1 + h.id % 3],
  (ARRAY['NA', 'EU', 'APAC'])[1 + (h.id * 7) % 3],
  (ARRAY['basic', 'basic', 'pro', 'team'])[1 + (h.id * 13) % 4],
  1 + floor(h.luck * 47)::integer
FROM habits AS h;

INSERT INTO usage (customer_id, month, logins, support_tickets)
SELECT
  h.id,
  (DATE '2025-10-01' + make_interval(months => m))::date,
  greatest(0, round(h.engagement * 28 + (random() - 0.5) * 8))::integer,
  floor(h.friction * h.friction * 4 * random() + random() * 0.6)::integer
FROM habits AS h, generate_series(0, 11) AS m;

-- Who cancelled: the more tickets and the fewer logins, the likelier; basic more than the rest,
-- and a long-standing customer less.
UPDATE customers AS c
SET cancelled_on = DATE '2026-09-30' - (s.id % 90)
FROM (
  SELECT u.customer_id AS id, avg(u.logins) AS logins, sum(u.support_tickets) AS tickets
  FROM usage AS u GROUP BY u.customer_id
) AS s
WHERE s.id = c.id
  AND random() < 1 / (1 + exp(-(
    -0.4 + 0.22 * s.tickets - 0.16 * s.logins
    + CASE c.plan WHEN 'basic' THEN 0.9 ELSE 0 END - 0.03 * c.tenure_months
  )));
