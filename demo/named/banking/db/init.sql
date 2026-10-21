-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The banking demo's ledger (REQ-2107): a bank's accounts and its append-only transaction ledger.
-- A posting is never changed: a correction is a new row carrying the same transaction_id, its
-- business_date kept and its updated_at the time the correction was entered. row_id, not the
-- transaction id, is the primary key.
--
-- The ledger starts empty. feed.postings holds what the bank's core systems deliver, each row
-- with the business day it arrives on (arrives_on), before that day's 18:00 London close: every
-- posting of 30 business days, 1 September to 12 October 2026, and two that restate a past day --
-- a payment of 1 October reversed on 6 October, and a trade of 2 October booked late, on
-- 7 October. The demo's setup (load_days.py) moves each day's arrivals into the ledger and builds
-- that day's end-of-day totals, so each day's figures are recorded at its own close. Provisa reads
-- only the public schema (db/reader.sql); feed is the setup's alone.
--
-- Every value is derived from a hash of its row's key, so every build of the demo holds the same
-- rows.

CREATE TABLE accounts (
  account_id varchar PRIMARY KEY,
  holder_name varchar NOT NULL,
  branch varchar NOT NULL,
  product varchar NOT NULL,
  currency varchar NOT NULL,
  opened_on date NOT NULL
);

CREATE TABLE transactions (
  row_id integer PRIMARY KEY,
  transaction_id varchar NOT NULL,
  account_id varchar NOT NULL REFERENCES accounts (account_id),
  business_date date NOT NULL,
  branch varchar NOT NULL,
  product varchar NOT NULL,
  currency varchar NOT NULL,
  kind varchar NOT NULL,
  amount numeric(14, 2) NOT NULL,
  counterparty varchar NOT NULL,
  status varchar NOT NULL,
  updated_at timestamp NOT NULL  -- UTC: when this version of the posting was entered
);
CREATE INDEX transactions_updated_at ON transactions (updated_at);
CREATE INDEX transactions_business_date ON transactions (business_date);

CREATE SCHEMA feed;

-- A number in [0, 1) from a key: the demo's only source of variety.
CREATE FUNCTION feed.h(key text) RETURNS double precision LANGUAGE sql IMMUTABLE AS $$
  SELECT ('x' || substr(md5(key), 1, 8))::bit(32)::bigint::double precision / 4294967296.0
$$;

-- Each branch offers current and savings accounts and staff accounts in GBP, and corporate
-- accounts in GBP, EUR and USD; three accounts of each.
INSERT INTO accounts (account_id, holder_name, branch, product, currency, opened_on)
SELECT
  format('%s-%s-%s-%s', upper(left(b.branch, 3)), upper(left(p.product, 3)), p.currency, n),
  CASE p.product
    WHEN 'corporate' THEN (ARRAY['Harbour Freight Ltd', 'Northgate Foods plc', 'Calder Engineering Ltd',
      'Brightwater Energy plc', 'Ashby Textiles Ltd', 'Fenwick Logistics Ltd', 'Moray Distillers plc',
      'Kingsway Retail Ltd', 'Tamar Shipping Ltd'])[1 + floor(feed.h(b.branch || p.product || p.currency || n) * 9)::int]
    ELSE (ARRAY['Amelia', 'Oliver', 'Isla', 'Harry', 'Ava', 'Jack', 'Mia', 'George', 'Ruby'])
           [1 + floor(feed.h('f' || b.branch || p.product || n) * 9)::int]
         || ' ' ||
         (ARRAY['Patel', 'Smith', 'Jones', 'Murray', 'Walsh', 'Khan', 'Taylor', 'Reid', 'Evans'])
           [1 + floor(feed.h('l' || b.branch || p.product || n) * 9)::int]
  END,
  b.branch, p.product, p.currency,
  DATE '2015-01-01' + floor(feed.h('o' || b.branch || p.product || p.currency || n) * 3650)::int
FROM (VALUES ('London'), ('Manchester'), ('Edinburgh')) AS b (branch)
CROSS JOIN (VALUES ('current', 'GBP'), ('savings', 'GBP'), ('staff', 'GBP'),
                   ('corporate', 'GBP'), ('corporate', 'EUR'), ('corporate', 'USD')) AS p (product, currency)
CROSS JOIN generate_series(1, 3) AS n;

CREATE TABLE feed.postings (
  row_id integer PRIMARY KEY,
  arrives_on date NOT NULL,
  transaction_id varchar NOT NULL,
  account_id varchar NOT NULL,
  business_date date NOT NULL,
  branch varchar NOT NULL,
  product varchar NOT NULL,
  currency varchar NOT NULL,
  kind varchar NOT NULL,
  amount numeric(14, 2) NOT NULL,
  counterparty varchar NOT NULL,
  status varchar NOT NULL,
  updated_at timestamp NOT NULL
);

-- Each account takes up to three postings a business day. A posting is entered between 08:00
-- and 16:00 UTC on its own business day, before the 17:00 UTC (18:00 London) close.
WITH days AS (
  SELECT d::date AS d
  FROM generate_series(DATE '2026-09-01', DATE '2026-10-12', INTERVAL '1 day') AS d
  WHERE extract(isodow FROM d) < 6
), drawn AS (
  SELECT days.d, a.account_id, a.branch, a.product, a.currency, k,
         feed.h(days.d || a.account_id || k || 'n') AS takes,
         feed.h(days.d || a.account_id || k || 'a') AS size,
         feed.h(days.d || a.account_id || k || 's') AS direction,
         feed.h(days.d || a.account_id || k || 't') AS entered,
         feed.h(days.d || a.account_id || k || 'c') AS party
  FROM days CROSS JOIN accounts AS a CROSS JOIN generate_series(1, 3) AS k
)
INSERT INTO feed.postings
SELECT
  row_number() OVER (ORDER BY d, account_id, k)::int,
  d,
  format('T%s-%s-%s', to_char(d, 'YYYYMMDD'), account_id, k),
  account_id, d, branch, product, currency,
  CASE WHEN direction < 0.55 THEN 'deposit'
       WHEN product = 'corporate' THEN 'payment'
       ELSE 'withdrawal' END,
  round(((CASE WHEN direction < 0.55 THEN 1 ELSE -1 END) * CASE product
    WHEN 'corporate' THEN 5000 + size * 245000
    WHEN 'savings' THEN 50 + size * 4950
    ELSE 20 + size * 1480 END)::numeric, 2),
  CASE WHEN product = 'corporate'
    THEN (ARRAY['Atlas Metals', 'Breck Packaging', 'Clyde Utilities', 'Dunmore Holdings', 'Eskdale Farms',
                'Forth Ports Services', 'Glen Paper Co', 'HM Revenue and Customs'])[1 + floor(party * 8)::int]
    ELSE (ARRAY['Tesco', 'Sainsbury''s', 'TfL', 'Octopus Energy', 'Payroll', 'Amazon UK', 'Council Tax',
                'Transfer'])[1 + floor(party * 8)::int] END,
  'booked',
  d + TIME '08:00' + entered * INTERVAL '8 hours'
FROM drawn
WHERE takes < 0.6;

-- The first restatement: a corporate payment out of London, booked on 1 October, recalled and
-- entered as reversed on 6 October. The reversal is a second version of the same transaction.
INSERT INTO feed.postings VALUES
  (100001, DATE '2026-10-01', 'T20261001-LON-PAY-7781', 'LON-COR-GBP-1', DATE '2026-10-01', 'London',
   'corporate', 'GBP', 'payment', -1850000.00, 'Dunmore Holdings', 'booked', TIMESTAMP '2026-10-01 11:42:00'),
  (100002, DATE '2026-10-06', 'T20261001-LON-PAY-7781', 'LON-COR-GBP-1', DATE '2026-10-01', 'London',
   'corporate', 'GBP', 'payment', 0.00, 'Dunmore Holdings', 'reversed', TIMESTAMP '2026-10-06 09:15:00'),
-- The second: a trade settled into an Edinburgh corporate account on 2 October and entered only on
-- 7 October, keeping its trade date.
  (100003, DATE '2026-10-07', 'T20261002-EDI-TRD-0412', 'EDI-COR-USD-2', DATE '2026-10-02', 'Edinburgh',
   'corporate', 'USD', 'trade', 640000.00, 'Atlas Metals', 'booked', TIMESTAMP '2026-10-07 10:30:00');
