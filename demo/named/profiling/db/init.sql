-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The profiling demo's data: the customer and order tables an outdoor retailer received when it
-- bought a smaller one, Harbor Outfitters, before anyone has looked inside them. Generated, and the
-- same on every build: each value is derived from its row's number through md5, never random.
--
-- The data has what years of a small company's CRM leave behind, for the profile to find: missing
-- and malformed emails, phone numbers in four formats, one country spelled five ways, a 1900-01-01
-- placeholder date, test accounts with a lifetime value of 999,999.99, refunds as negative values,
-- customers the export wrote twice, and orders whose customer is not in the table.
--
--   customers  one row a customer, as the CRM held them
--   orders     one row an order, with its amount, currency, channel and discount

-- A number in [0, 1) derived from a row's number and a salt: the same on every build.
CREATE FUNCTION u(n integer, salt text) RETURNS double precision
  LANGUAGE sql IMMUTABLE AS
$$ SELECT ('x' || substr(md5(n::text || ':' || salt), 1, 8))::bit(32)::bigint / 4294967296.0 $$;

-- A standard normal draw for a row (Box-Muller over two of its numbers).
CREATE FUNCTION z(n integer, salt text) RETURNS double precision
  LANGUAGE sql IMMUTABLE AS
$$ SELECT sqrt(-2 * ln(greatest(u(n, salt || 'a'), 1e-12))) * cos(2 * pi() * u(n, salt || 'b')) $$;

CREATE TABLE customers (
  -- The CRM's export kept no key: customer_id is the model's key, held by no constraint here.
  customer_id      integer NOT NULL,
  full_name        varchar NOT NULL,
  email            varchar,
  phone            varchar,
  country          varchar NOT NULL,
  postal_code      varchar,
  signup_date      date NOT NULL,
  lifetime_value   numeric(12, 2) NOT NULL,
  status           varchar NOT NULL,
  marketing_opt_in boolean
);

WITH names AS (
  SELECT n,
         (ARRAY['Ava','Ben','Chloe','Daniel','Ella','Finn','Grace','Henry','Isla','Jack','Kai',
                'Lena','Mason','Nora','Owen','Priya','Quinn','Ravi','Sofia','Theo','Uma','Victor',
                'Willa','Xavier','Yara','Zane'])[1 + floor(u(n, 'first') * 26)::int] AS first_name,
         (ARRAY['Anderson','Brooks','Chen','Diaz','Evans','Fischer','Garcia','Hughes','Ito',
                'Johnson','Kim','Lopez','Martin','Nguyen','Okafor','Patel','Quinlan','Rossi',
                'Smith','Torres','Ueda','Vargas','Walsh','Young','Zhang'])[1 + floor(u(n, 'last') * 25)::int] AS last_name,
         u(n, 'country') AS c, u(n, 'email') AS e, u(n, 'phone') AS p
  FROM generate_series(1, 4850) AS n
)
INSERT INTO customers
SELECT n,
       first_name || ' ' || last_name,
       -- 4% have no email; 2% hold one without an @ (typed into the wrong field).
       CASE WHEN e < 0.04 THEN NULL
            WHEN e < 0.06 THEN lower(first_name) || ' ' || lower(last_name)
            ELSE lower(first_name) || '.' || lower(last_name) || (n % 97) || '@'
                 || (ARRAY['gmail.com','yahoo.com','outlook.com','icloud.com','harbor-mail.net'])[1 + n % 5]
       END,
       -- Four formats, and 3% none: the CRM never checked.
       CASE WHEN p < 0.03 THEN NULL
            WHEN p < 0.40 THEN '(' || (200 + n % 700) || ') ' || (100 + n % 900) || '-' || lpad((n * 7 % 10000)::text, 4, '0')
            WHEN p < 0.65 THEN (200 + n % 700) || '.' || (100 + n % 900) || '.' || lpad((n * 7 % 10000)::text, 4, '0')
            WHEN p < 0.85 THEN '+1 ' || (200 + n % 700) || ' ' || (100 + n % 900) || ' ' || lpad((n * 7 % 10000)::text, 4, '0')
            ELSE (200 + n % 700)::text || (100 + n % 900)::text || lpad((n * 7 % 10000)::text, 4, '0')
       END,
       -- The United States spelled five ways; then Canada, the UK and Germany.
       CASE WHEN c < 0.50 THEN 'US' WHEN c < 0.60 THEN 'USA' WHEN c < 0.66 THEN 'United States'
            WHEN c < 0.69 THEN 'us' WHEN c < 0.71 THEN 'U.S.' WHEN c < 0.85 THEN 'CA'
            WHEN c < 0.94 THEN 'GB' ELSE 'DE' END,
       CASE WHEN c < 0.71 THEN CASE WHEN u(n, 'zip4') < 0.2
                                    THEN lpad((n * 37 % 99999)::text, 5, '0') || '-' || lpad((n % 9999)::text, 4, '0')
                                    ELSE lpad((n * 37 % 99999)::text, 5, '0') END
            WHEN c < 0.85 THEN chr(65 + n % 26) || (n % 10) || chr(65 + n * 3 % 26) || ' ' || (n * 7 % 10) || chr(65 + n * 5 % 26) || (n * 3 % 10)
            WHEN c < 0.94 THEN 'SW' || (1 + n % 20) || ' ' || (n % 10) || chr(65 + n % 26) || chr(65 + n * 7 % 26)
            WHEN u(n, 'pc') < 0.1 THEN NULL
            ELSE lpad((10000 + n * 13 % 89999)::text, 5, '0') END,
       -- 2015 to 2024; 1.5% hold the 1900-01-01 the old import wrote when it had no date.
       CASE WHEN u(n, 'sentinel') < 0.015 THEN DATE '1900-01-01'
            ELSE DATE '2015-01-01' + floor(u(n, 'signup') * 3650)::int END,
       -- Log-normal spend; 0.4% are test accounts at 999,999.99; 1% net negative after refunds.
       CASE WHEN u(n, 'test') < 0.004 THEN 999999.99
            WHEN u(n, 'refund') < 0.01 THEN -round((20 + u(n, 'neg') * 180)::numeric, 2)
            ELSE round(exp(5.5 + 1.1 * z(n, 'ltv'))::numeric, 2) END,
       CASE WHEN u(n, 'status') < 0.62 THEN 'active' WHEN u(n, 'status') < 0.85 THEN 'churned'
            WHEN u(n, 'status') < 0.97 THEN 'prospect' ELSE 'Active' END,
       CASE WHEN u(n, 'optin') < 0.2 THEN NULL ELSE u(n, 'optin') < 0.55 END
FROM names;

-- 150 customers the export wrote twice: the same number and every other field the same.
INSERT INTO customers
SELECT * FROM customers WHERE customer_id % 32 = 5 AND customer_id <= 4800;

CREATE TABLE orders (
  order_id     integer PRIMARY KEY,
  customer_id  integer NOT NULL,
  order_date   date NOT NULL,
  amount       numeric(10, 2) NOT NULL,
  currency     varchar NOT NULL,
  channel      varchar NOT NULL,
  discount_pct integer NOT NULL
);

-- Orders cluster on a few heavy customers (the square of a uniform number); 1% name a customer
-- the table does not hold (deleted from the CRM, its orders kept).
INSERT INTO orders
SELECT n,
       CASE WHEN u(n, 'orphan') < 0.01 THEN 6000 + floor(u(n, 'gone') * 900)::int
            ELSE 1 + floor(power(u(n, 'cust'), 2) * 4850)::int END,
       DATE '2019-01-01' + floor(u(n, 'date') * 2190)::int,
       round(exp(4.0 + 0.8 * z(n, 'amount'))::numeric, 2),
       CASE WHEN u(n, 'cur') < 0.85 THEN 'USD' WHEN u(n, 'cur') < 0.93 THEN 'CAD'
            WHEN u(n, 'cur') < 0.98 THEN 'GBP' ELSE 'EUR' END,
       CASE WHEN u(n, 'ch') < 0.58 THEN 'web' WHEN u(n, 'ch') < 0.88 THEN 'store' ELSE 'phone' END,
       CASE WHEN u(n, 'disc') < 0.70 THEN 0 WHEN u(n, 'disc') < 0.85 THEN 10
            WHEN u(n, 'disc') < 0.95 THEN 15 WHEN u(n, 'disc') < 0.995 THEN 20 ELSE 100 END
FROM generate_series(1, 25000) AS n;

DROP FUNCTION z(integer, text);
DROP FUNCTION u(integer, text);
