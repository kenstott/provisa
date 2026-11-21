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
-- The execution management system: each fill of each order.
\connect affirmation_ems

CREATE TABLE executions (exec_id text PRIMARY KEY, order_id text NOT NULL,
  quantity integer NOT NULL, price numeric(12, 4) NOT NULL,
  executed_at timestamp NOT NULL);

INSERT INTO executions VALUES
  ('X-1001-1', 'O-1001', 18000, 228.41, '2026-11-18 15:10:00'),
  ('X-1001-2', 'O-1001', 12000, 228.47, '2026-11-18 16:20:00'),
  ('X-1002-1', 'O-1002', 7000, 512.1, '2026-11-18 15:10:00'),
  ('X-1002-2', 'O-1002', 5000, 512.02, '2026-11-18 16:20:00'),
  ('X-1003-1', 'O-1003', 25000, 186.3, '2026-11-18 15:10:00'),
  ('X-1003-2', 'O-1003', 15000, 186.36, '2026-11-18 16:20:00'),
  ('X-1004-1', 'O-1004', 15000, 158.92, '2026-11-18 15:10:00'),
  ('X-1005-1', 'O-1005', 12000, 241.15, '2026-11-18 15:10:00'),
  ('X-1005-2', 'O-1005', 8000, 241.05, '2026-11-18 16:20:00'),
  ('X-1006-1', 'O-1006', 8000, 201.4, '2026-11-18 15:10:00'),
  ('X-1007-1', 'O-1007', 15000, 57.82, '2026-11-18 15:10:00'),
  ('X-1007-2', 'O-1007', 10000, 57.8, '2026-11-18 16:20:00'),
  ('X-1008-1', 'O-1008', 3000, 702.6, '2026-11-18 15:10:00'),
  ('X-1009-1', 'O-1009', 12000, 247.55, '2026-11-18 15:10:00'),
  ('X-1009-2', 'O-1009', 8000, 247.61, '2026-11-18 16:20:00'),
  ('X-1010-1', 'O-1010', 30000, 69.14, '2026-11-18 15:10:00');
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
