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
-- The middle office's own database: the funds' standing settlement instructions, each
-- market's affirmation cutoff and what a failed settlement costs there, the desk's
-- clock, and the affirmation log the affirm_matched command writes.
\connect affirmation_desk

CREATE TABLE ssi (fund_account text NOT NULL, market text NOT NULL,
  custodian_bic text NOT NULL, safekeeping_account text NOT NULL,
  PRIMARY KEY (fund_account, market));
-- cutoff_minutes: the cutoff, in minutes after midnight UTC starting the trade date.
CREATE TABLE settlement_rules (market text PRIMARY KEY, settlement_cycle_days integer NOT NULL,
  cutoff_minutes integer NOT NULL, cutoff_rule text NOT NULL,
  penalty_bp_per_day numeric(6, 2) NOT NULL, penalty_basis text NOT NULL);
-- The time the demo's desk is at: the cutoffs count down from it.
CREATE TABLE desk_clock (as_at timestamp PRIMARY KEY);
CREATE TABLE affirmation_log (trade_ref text PRIMARY KEY, affirmed_at timestamp NOT NULL,
  affirmed_by text NOT NULL);

INSERT INTO ssi VALUES
  ('GEF-01', 'US', 'MRDNUS33', 'GEF01-US-0001'),
  ('GEF-01', 'DE', 'MRDNDEFF', 'GEF01-DE-0001'),
  ('GEF-01', 'FR', 'MRDNFRPP', 'GEF01-FR-0001'),
  ('GEF-01', 'NL', 'MRDNNL2A', 'GEF01-NL-0001'),
  ('USG-02', 'US', 'MRDNUS33', 'USG02-US-0001'),
  ('USG-02', 'DE', 'MRDNDEFF', 'USG02-DE-0001'),
  ('USG-02', 'FR', 'MRDNFRPP', 'USG02-FR-0001'),
  ('USG-02', 'NL', 'MRDNNL2A', 'USG02-NL-0001'),
  ('EUI-03', 'US', 'MRDNUS33', 'EUI03-US-0001'),
  ('EUI-03', 'DE', 'MRDNDEFF', 'EUI03-DE-0001'),
  ('EUI-03', 'FR', 'MRDNFRPP', 'EUI03-FR-0001'),
  ('EUI-03', 'NL', 'MRDNNL2A', 'EUI03-NL-0001');
INSERT INTO settlement_rules VALUES
  ('US', 1, 1560, 'affirmed by 9:00 pm New York time on trade date (DTC, T+1)', 1.25, 'a day''s funding of the failed amount at 4.5% a year'),
  ('DE', 2, 1320, 'confirmed by 11:00 pm Frankfurt time on trade date (T+2)', 1.0, 'CSDR cash penalty for a liquid share'),
  ('FR', 2, 1320, 'confirmed by 11:00 pm Paris time on trade date (T+2)', 1.0, 'CSDR cash penalty for a liquid share'),
  ('NL', 2, 1320, 'confirmed by 11:00 pm Amsterdam time on trade date (T+2)', 1.0, 'CSDR cash penalty for a liquid share');
INSERT INTO desk_clock VALUES ('2026-11-18 21:15:00');
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
GRANT INSERT ON affirmation_log TO provisa;
