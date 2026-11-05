-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The banksecrecy demo's database (REQ-2201): a community bank's customers, their accounts, its
-- branches and tellers, and every cash deposit and withdrawal at a teller's window in October
-- 2026, each with the teller, the branch, the time and the bank's business day. All of it is made
-- up. A day's cash after the 17:00 cutoff posts to the next business day, as the bank books it.
--
-- Beside a month of ordinary cash, the rows the Bank Secrecy Act's currency transaction reporting
-- rule is about (31 CFR 1010.311, aggregated as 1010.313 says):
--   * one deposit over $10,000;
--   * cash split across two branches on one business day, over $10,000 together;
--   * cash brought in on behalf of a customer by someone else, over $10,000 with the customer's own;
--   * withdrawals over $10,000 together on one business day;
--   * people bringing cash just under $10,000, again and again, across branches and days.

CREATE TABLE branches (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  city varchar NOT NULL
);

CREATE TABLE tellers (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  branch_id integer NOT NULL REFERENCES branches (id)
);

CREATE TABLE customers (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  kind varchar NOT NULL CHECK (kind IN ('individual', 'business')),
  tax_id varchar NOT NULL,
  city varchar NOT NULL,
  customer_since date NOT NULL
);

CREATE TABLE accounts (
  id integer PRIMARY KEY,
  account_number varchar NOT NULL UNIQUE,
  customer_id integer NOT NULL REFERENCES customers (id),
  branch_id integer NOT NULL REFERENCES branches (id),
  product varchar NOT NULL,
  opened_on date NOT NULL
);

-- conducted_by: the person at the window. on_behalf_of: whose cash it is, the account's holder;
-- when someone else brings it (an employee, a relative) it is conducted on the holder's behalf.
CREATE TABLE cash_transactions (
  id integer PRIMARY KEY,
  account_id integer NOT NULL REFERENCES accounts (id),
  branch_id integer NOT NULL REFERENCES branches (id),
  teller_id integer NOT NULL REFERENCES tellers (id),
  direction varchar NOT NULL CHECK (direction IN ('deposit', 'withdrawal')),
  amount numeric(12, 2) NOT NULL CHECK (amount > 0),
  transacted_at timestamp NOT NULL,
  business_day date NOT NULL,
  conducted_by integer NOT NULL REFERENCES customers (id),
  on_behalf_of integer REFERENCES customers (id)
);

INSERT INTO branches VALUES
  (1, 'Main Street', 'Dayton'),
  (2, 'Riverside', 'Dayton'),
  (3, 'Kettering Plaza', 'Kettering'),
  (4, 'Fairborn', 'Fairborn');

INSERT INTO tellers VALUES
  (1, 'Maria Lopez', 1), (2, 'James Carter', 1),
  (3, 'Priya Shah', 2), (4, 'Tom Becker', 2),
  (5, 'Lena Park', 3), (6, 'Owen Hughes', 3),
  (7, 'Rosa Diaz', 4), (8, 'Sam Whitfield', 4);

INSERT INTO customers VALUES
  (1, 'Harold Finch', 'individual', '412-55-1093', 'Dayton', '2014-03-02'),
  (2, 'Greenleaf Landscaping LLC', 'business', '31-4417702', 'Kettering', '2019-06-11'),
  (3, 'Dana Whitmore', 'individual', '298-41-7730', 'Dayton', '2011-09-23'),
  (4, 'Corner Market Inc', 'business', '31-2290418', 'Dayton', '2016-01-14'),
  (5, 'Victor Hale', 'individual', '377-08-2264', 'Fairborn', '2021-04-30'),
  (6, 'Marcus Reed', 'individual', '505-62-9917', 'Dayton', '2022-08-19'),
  (7, 'Elena Petrova', 'individual', '219-73-4402', 'Kettering', '2020-02-07'),
  (8, 'Tony Russo', 'individual', '466-30-1185', 'Fairborn', '2023-05-15'),
  (9, 'Nina Alvarez', 'individual', '338-19-6620', 'Dayton', '2018-10-03'),
  (10, 'Paul Greer', 'individual', '601-27-4471', 'Kettering', '2015-07-21');

-- Thirty more customers who use cash the ordinary way.
INSERT INTO customers
SELECT 10 + n,
       (ARRAY['Avery','Blake','Casey','Drew','Emery','Finley','Gray','Harper','Jordan','Kendall'])[1 + (n * 7) % 10]
         || ' ' ||
       (ARRAY['Adams','Brooks','Coleman','Dunn','Ellis','Foster','Grant','Hayes','Irwin','Jensen'])[1 + (n * 3) % 10],
       CASE WHEN n % 6 = 0 THEN 'business' ELSE 'individual' END,
       lpad((100 + n * 17)::text, 3, '0') || '-' || lpad((n * 13 % 100)::text, 2, '0') || '-'
         || lpad((1000 + n * 389 % 9000)::text, 4, '0'),
       (ARRAY['Dayton','Kettering','Fairborn'])[1 + n % 3],
       DATE '2010-01-01' + (n * 97 % 5000)
FROM generate_series(1, 30) AS n;

-- Each customer's checking account at their home branch; the first ten have savings too, at
-- another branch.
INSERT INTO accounts
SELECT c.id,
       '4417-' || lpad((1000 + c.id)::text, 4, '0') || '-' || lpad((c.id * 7919 % 10000)::text, 4, '0'),
       c.id, 1 + c.id % 4, 'checking', c.customer_since
FROM customers AS c;
INSERT INTO accounts
SELECT 100 + c.id,
       '4417-' || lpad((2000 + c.id)::text, 4, '0') || '-' || lpad((c.id * 6007 % 10000)::text, 4, '0'),
       c.id, 1 + (c.id + 1) % 4, 'savings', c.customer_since + 30
FROM customers AS c
WHERE c.id <= 10;

-- A month of ordinary cash: every business day (Monday to Friday) of October 2026, eight of the
-- thirty ordinary customers deposit or withdraw a few hundred dollars at their home branch.
INSERT INTO cash_transactions
SELECT row_number() OVER (ORDER BY d, k) AS id,
       10 + c AS account_id,
       1 + (10 + c) % 4 AS branch_id,
       2 * (1 + (10 + c) % 4) - (k % 2) AS teller_id,
       CASE WHEN (c + k) % 3 = 0 THEN 'withdrawal' ELSE 'deposit' END,
       round((60 + ((c * 7919 + k * 104729 + extract(doy FROM d)::int * 31) % 2400))::numeric, 2),
       d + make_interval(hours => 9 + (c + k) % 8, mins => (c * 11 + k * 7) % 60),
       d::date,
       10 + c
FROM generate_series(DATE '2026-10-01', DATE '2026-10-30', INTERVAL '1 day') AS d
CROSS JOIN generate_series(1, 8) AS k
CROSS JOIN LATERAL (SELECT 1 + (k * 4 + extract(doy FROM d)::int) % 30 AS c) AS pick
WHERE extract(isodow FROM d) < 6;

-- What the rule is about, given ids from 1001 on. Times are local; a deposit after 17:00 posts to
-- the next business day.
INSERT INTO cash_transactions VALUES
  -- Harold Finch: one deposit over $10,000.
  (1001, 1, 2, 3, 'deposit', 12500.00, '2026-10-05 10:42', '2026-10-05', 1),
  -- Greenleaf Landscaping: the day's takings split across two branches, $11,600 together.
  (1002, 2, 3, 5, 'deposit', 6200.00, '2026-10-07 09:15', '2026-10-07', 2),
  (1003, 102, 4, 7, 'deposit', 5400.00, '2026-10-07 14:30', '2026-10-07', 2),
  -- Dana Whitmore: $7,000 of her own, and $4,500 brought in on her behalf by Paul Greer.
  (1004, 3, 4, 8, 'deposit', 7000.00, '2026-10-09 11:05', '2026-10-09', 3),
  (1005, 103, 1, 1, 'deposit', 4500.00, '2026-10-09 15:50', '2026-10-09', 10),
  -- Corner Market: three withdrawals on one business day, $12,000 together; the last after the
  -- cutoff of the day before posts to the same business day.
  (1006, 4, 1, 2, 'withdrawal', 4000.00, '2026-10-13 17:20', '2026-10-14', 4),
  (1007, 4, 1, 1, 'withdrawal', 4000.00, '2026-10-14 10:00', '2026-10-14', 4),
  (1008, 104, 2, 4, 'withdrawal', 4000.00, '2026-10-14 13:40', '2026-10-14', 4),
  -- Victor Hale: just under $10,000, four times in a week, at three branches.
  (1009, 5, 2, 3, 'deposit', 9500.00, '2026-10-19 09:30', '2026-10-19', 5),
  (1010, 105, 3, 6, 'deposit', 9800.00, '2026-10-20 12:10', '2026-10-20', 5),
  (1011, 5, 4, 7, 'deposit', 9400.00, '2026-10-21 10:25', '2026-10-21', 5),
  (1012, 105, 2, 4, 'deposit', 9900.00, '2026-10-23 16:05', '2026-10-23', 5),
  -- Marcus Reed: twice under $10,000 on one day at two branches -- over in aggregate -- and again
  -- under it two days later.
  (1013, 6, 3, 5, 'deposit', 9000.00, '2026-10-20 09:05', '2026-10-20', 6),
  (1014, 106, 4, 8, 'deposit', 9700.00, '2026-10-20 15:30', '2026-10-20', 6),
  (1015, 6, 3, 6, 'deposit', 9600.00, '2026-10-22 11:45', '2026-10-22', 6),
  -- Elena Petrova: just under $10,000 three times, a business day apart, at two branches.
  (1016, 7, 1, 1, 'deposit', 9200.00, '2026-10-26 10:10', '2026-10-26', 7),
  (1017, 107, 4, 7, 'deposit', 9300.00, '2026-10-27 13:20', '2026-10-27', 7),
  (1018, 7, 1, 2, 'deposit', 9250.00, '2026-10-28 09:55', '2026-10-28', 7),
  -- Tony Russo: just under $10,000 once, nothing more -- not a pattern.
  (1019, 8, 4, 8, 'deposit', 9900.00, '2026-10-16 14:15', '2026-10-16', 8),
  -- Nina Alvarez: $6,000 in and $6,000 out on one business day. Cash in and cash out are counted
  -- apart: neither is over $10,000.
  (1020, 9, 2, 3, 'deposit', 6000.00, '2026-10-29 10:30', '2026-10-29', 9),
  (1021, 109, 2, 4, 'withdrawal', 6000.00, '2026-10-29 14:45', '2026-10-29', 9);

-- Whose cash each transaction is: the account's holder.
UPDATE cash_transactions AS t SET on_behalf_of = a.customer_id FROM accounts AS a WHERE a.id = t.account_id;
ALTER TABLE cash_transactions ALTER COLUMN on_behalf_of SET NOT NULL;
