-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The cloudpolicy demo's own store: the security team's review. The cloud inventory and the
-- incidents are other sources'; this holds what the team keeps of its own.
--
--   control_checks   each check the team runs, and the policy section it enforces.
--   review_history   one row an attached disk a day: the daily review as the daily trigger keeps
--                    it (db/history.sql seeds the fourteen days before, from generate.py).
--   clause_vectors   each policy clause's embedding, written by the setup's embed_clauses.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE control_checks (
  section varchar NOT NULL,
  check_name varchar PRIMARY KEY,
  checked_by varchar NOT NULL
);
INSERT INTO control_checks VALUES
  ('4.2', 'Attached disks use a customer-managed key', 'cmek_violations'),
  ('4.3', 'Exceptions are in date', 'daily_review'),
  ('5.1', 'Database servers have no public address', 'public_database_servers'),
  ('6.1', 'Incidents are fixed by their deadline', 'daily_review');

CREATE TABLE review_history (
  review_date date NOT NULL,
  resource_id varchar NOT NULL,
  resource_name varchar NOT NULL,
  cloud_provider varchar NOT NULL,
  account_id varchar NOT NULL,
  application varchar NOT NULL,
  status varchar NOT NULL,
  incident_number varchar,
  PRIMARY KEY (review_date, resource_id)
);

CREATE TABLE clause_vectors (
  section varchar PRIMARY KEY,
  clause_vec vector(384) NOT NULL
);
