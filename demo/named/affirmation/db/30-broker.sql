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
-- The brokers' confirmations, as they arrive through the matching platform: each
-- carries the allocation's reference, the broker's price, commission and net amount,
-- and the settlement instructions the broker will deliver to.
\connect affirmation_broker

CREATE TABLE confirmations (confirm_id text PRIMARY KEY, trade_ref text NOT NULL UNIQUE,
  broker_code text NOT NULL, side text NOT NULL, quantity integer NOT NULL,
  price numeric(12, 4) NOT NULL, commission numeric(14, 2) NOT NULL,
  net_amount numeric(16, 2) NOT NULL, settle_date date NOT NULL,
  custodian_bic text NOT NULL, safekeeping_account text NOT NULL,
  received_at timestamp NOT NULL);

INSERT INTO confirmations VALUES
  ('C-HRBR-1001-1', 'A-1001-1', 'HRBR', 'BUY', 18000, 228.434, 180.0, 4111992.0, '2026-11-19', 'MRDNUS33', 'GEF01-US-0001', '2026-11-18 19:00:00'),
  ('C-HRBR-1001-2', 'A-1001-2', 'HRBR', 'BUY', 12000, 228.434, 120.0, 2741328.0, '2026-11-19', 'MRDNUS33', 'USG02-US-0001', '2026-11-18 19:01:00'),
  ('C-NFCM-1002-1', 'A-1002-1', 'NFCM', 'SELL', 12000, 512.1167, 120.0, 6145280.4, '2026-11-19', 'MRDNUS33', 'GEF01-US-0001', '2026-11-18 19:02:00'),
  ('C-HRBR-1003-1', 'A-1003-1', 'HRBR', 'BUY', 25000, 186.3225, 250.0, 4658312.5, '2026-11-19', 'MRDNUS33', 'USG02-US-0001', '2026-11-18 19:03:00'),
  ('C-HRBR-1003-2', 'A-1003-2', 'HRBR', 'BUY', 1500, 186.3225, 15.0, 279498.75, '2026-11-19', 'MRDNUS33', 'GEF01-US-0001', '2026-11-18 19:04:00'),
  ('C-OSTB-1005-1', 'A-1005-1', 'OSTB', 'BUY', 12000, 241.11, 1446.66, 2894766.66, '2026-11-20', 'MRDNDEFF', 'EUI03-DE-0001', '2026-11-18 19:05:00'),
  ('C-OSTB-1005-2', 'A-1005-2', 'OSTB', 'BUY', 8000, 241.11, 964.44, 1929844.44, '2026-11-20', 'MRDNDEFF', 'GEF01-DE-0009', '2026-11-18 19:06:00'),
  ('C-NFCM-1006-1', 'A-1006-1', 'NFCM', 'SELL', 8000, 201.4, 805.6, 1610394.4, '2026-11-19', 'MRDNDEFF', 'EUI03-DE-0001', '2026-11-18 19:07:00'),
  ('C-OSTB-1007-1', 'A-1007-1', 'OSTB', 'BUY', 15000, 57.812, 433.59, 867613.59, '2026-11-20', 'MRDNFRPP', 'EUI03-FR-0001', '2026-11-18 19:08:00'),
  ('C-OSTB-1007-2', 'A-1007-2', 'OSTB', 'BUY', 10000, 57.812, 289.06, 578409.06, '2026-11-20', 'MRDNFRPP', 'GEF01-FR-0001', '2026-11-18 19:09:00'),
  ('C-HRBR-1008-1', 'A-1008-1', 'HRBR', 'BUY', 3000, 702.6, 2107.8, 2109907.8, '2026-11-20', 'MRDNNL2A', 'GEF01-NL-0001', '2026-11-18 19:10:00'),
  ('C-NFCM-1009-1', 'A-1009-1', 'NFCM', 'BUY', 12000, 247.574, 120.0, 2971008.0, '2026-11-19', 'MRDNUS33', 'GEF01-US-0001', '2026-11-18 19:11:00'),
  ('C-NFCM-1009-2', 'A-1009-2', 'NFCM', 'BUY', 8000, 247.574, 80.0, 1980672.0, '2026-11-19', 'MRDNUS33', 'USG02-US-0001', '2026-11-18 19:12:00'),
  ('C-OSTB-1010-1', 'A-1010-1', 'OSTB', 'SELL', 30000, 69.14, 300.0, 2073900.0, '2026-11-19', 'MRDNUS33', 'USG02-US-0001', '2026-11-18 19:13:00');
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
