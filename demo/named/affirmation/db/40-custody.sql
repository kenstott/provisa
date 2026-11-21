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
-- The custodian's settlement instructions, as Larkspur sent them: what each fund
-- expects to receive or deliver, against what cash, on what day.
\connect affirmation_custody

CREATE TABLE instructions (instruction_id text PRIMARY KEY, trade_ref text NOT NULL UNIQUE,
  fund_account text NOT NULL, side text NOT NULL, quantity integer NOT NULL,
  net_amount numeric(16, 2) NOT NULL, settle_date date NOT NULL,
  status text NOT NULL);

INSERT INTO instructions VALUES
  ('I-1001-1', 'A-1001-1', 'GEF-01', 'BUY', 18000, 4111992.0, '2026-11-19', 'pending'),
  ('I-1001-2', 'A-1001-2', 'USG-02', 'BUY', 12000, 2741328.0, '2026-11-19', 'pending'),
  ('I-1002-1', 'A-1002-1', 'GEF-01', 'SELL', 12000, 6144680.4, '2026-11-19', 'pending'),
  ('I-1003-1', 'A-1003-1', 'USG-02', 'BUY', 25000, 4658312.5, '2026-11-19', 'pending'),
  ('I-1003-2', 'A-1003-2', 'GEF-01', 'BUY', 15000, 2794987.5, '2026-11-19', 'pending'),
  ('I-1004-1', 'A-1004-1', 'USG-02', 'BUY', 15000, 2383950.0, '2026-11-19', 'pending'),
  ('I-1005-1', 'A-1005-1', 'EUI-03', 'BUY', 12000, 2894766.66, '2026-11-20', 'pending'),
  ('I-1005-2', 'A-1005-2', 'GEF-01', 'BUY', 8000, 1929844.44, '2026-11-20', 'pending'),
  ('I-1006-1', 'A-1006-1', 'EUI-03', 'SELL', 8000, 1610394.4, '2026-11-20', 'pending'),
  ('I-1007-1', 'A-1007-1', 'EUI-03', 'BUY', 15000, 867613.59, '2026-11-20', 'pending'),
  ('I-1008-1', 'A-1008-1', 'GEF-01', 'BUY', 3000, 2108853.9, '2026-11-20', 'pending'),
  ('I-1009-1', 'A-1009-1', 'GEF-01', 'BUY', 12000, 2971008.0, '2026-11-19', 'pending'),
  ('I-1009-2', 'A-1009-2', 'USG-02', 'BUY', 8000, 1980672.0, '2026-11-19', 'pending'),
  ('I-1010-1', 'A-1010-1', 'USG-02', 'SELL', 30000, 2073900.0, '2026-11-19', 'pending');
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
