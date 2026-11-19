-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- Written by generate.py; edit the trades there, not here.
-- The order management system: each order and its allocations to the funds.
\connect affirmation_oms

CREATE TABLE orders (order_id text PRIMARY KEY, isin text NOT NULL, security text NOT NULL,
  side text NOT NULL CHECK (side IN ('BUY', 'SELL')), quantity integer NOT NULL,
  market text NOT NULL, currency text NOT NULL, broker_code text NOT NULL,
  trader text NOT NULL, trade_date date NOT NULL);
CREATE TABLE allocations (trade_ref text PRIMARY KEY,
  order_id text NOT NULL REFERENCES orders, fund_account text NOT NULL,
  quantity integer NOT NULL, settle_date date NOT NULL);

INSERT INTO orders VALUES
  ('O-1001', 'US0378331005', 'Apple Inc.', 'BUY', 30000, 'US', 'USD', 'HRBR', 'Priya Nair', '2026-11-18'),
  ('O-1002', 'US5949181045', 'Microsoft Corp.', 'SELL', 12000, 'US', 'USD', 'NFCM', 'Tom Becker', '2026-11-18'),
  ('O-1003', 'US67066G1040', 'NVIDIA Corp.', 'BUY', 40000, 'US', 'USD', 'HRBR', 'Priya Nair', '2026-11-18'),
  ('O-1004', 'US4781601046', 'Johnson & Johnson', 'BUY', 15000, 'US', 'USD', 'OSTB', 'Tom Becker', '2026-11-18'),
  ('O-1005', 'DE0007164600', 'SAP SE', 'BUY', 20000, 'DE', 'EUR', 'OSTB', 'Lena Vogt', '2026-11-18'),
  ('O-1006', 'DE0007236101', 'Siemens AG', 'SELL', 8000, 'DE', 'EUR', 'NFCM', 'Lena Vogt', '2026-11-18'),
  ('O-1007', 'FR0000120271', 'TotalEnergies SE', 'BUY', 25000, 'FR', 'EUR', 'OSTB', 'Lena Vogt', '2026-11-18'),
  ('O-1008', 'NL0010273215', 'ASML Holding N.V.', 'BUY', 3000, 'NL', 'EUR', 'HRBR', 'Lena Vogt', '2026-11-18'),
  ('O-1009', 'US46625H1005', 'JPMorgan Chase & Co.', 'BUY', 20000, 'US', 'USD', 'NFCM', 'Tom Becker', '2026-11-18'),
  ('O-1010', 'US1912161007', 'Coca-Cola Co.', 'SELL', 30000, 'US', 'USD', 'OSTB', 'Priya Nair', '2026-11-18');
INSERT INTO allocations VALUES
  ('A-1001-1', 'O-1001', 'GEF-01', 18000, '2026-11-19'),
  ('A-1001-2', 'O-1001', 'USG-02', 12000, '2026-11-19'),
  ('A-1002-1', 'O-1002', 'GEF-01', 12000, '2026-11-19'),
  ('A-1003-1', 'O-1003', 'USG-02', 25000, '2026-11-19'),
  ('A-1003-2', 'O-1003', 'GEF-01', 15000, '2026-11-19'),
  ('A-1004-1', 'O-1004', 'USG-02', 15000, '2026-11-19'),
  ('A-1005-1', 'O-1005', 'EUI-03', 12000, '2026-11-20'),
  ('A-1005-2', 'O-1005', 'GEF-01', 8000, '2026-11-20'),
  ('A-1006-1', 'O-1006', 'EUI-03', 8000, '2026-11-20'),
  ('A-1007-1', 'O-1007', 'EUI-03', 15000, '2026-11-20'),
  ('A-1007-2', 'O-1007', 'GEF-01', 10000, '2026-11-20'),
  ('A-1008-1', 'O-1008', 'GEF-01', 3000, '2026-11-20'),
  ('A-1009-1', 'O-1009', 'GEF-01', 12000, '2026-11-19'),
  ('A-1009-2', 'O-1009', 'USG-02', 8000, '2026-11-19'),
  ('A-1010-1', 'O-1010', 'USG-02', 30000, '2026-11-19');
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
