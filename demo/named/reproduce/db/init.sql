-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The reproduce demo's database: a trading book a bank reports to its regulator. Each position
-- has a notional in its own currency and the rate it is converted to USD at, valid from the
-- business date the report is for. The demo's setup corrects one rate after the report was first
-- built: the correction is a fact about 30 September, recorded days later.

CREATE TABLE positions (
  id integer PRIMARY KEY,
  desk varchar NOT NULL,
  instrument varchar NOT NULL,
  currency varchar NOT NULL,
  notional numeric(14, 2) NOT NULL,
  fx_rate numeric(10, 6) NOT NULL,
  valid_from timestamp NOT NULL
);
INSERT INTO positions VALUES
  (1, 'rates', 'UST 10Y', 'USD', 1000000.00, 1.000000, '2026-09-30 00:00'),
  (2, 'rates', 'Bund 10Y', 'EUR', 500000.00, 1.080000, '2026-09-30 00:00'),
  (3, 'credit', 'CDX IG', 'USD', 750000.00, 1.000000, '2026-09-30 00:00'),
  (4, 'credit', 'iTraxx Main', 'EUR', 400000.00, 1.080000, '2026-09-30 00:00'),
  (5, 'fx', 'GBPUSD forward', 'GBP', 300000.00, 1.270000, '2026-09-30 00:00');
