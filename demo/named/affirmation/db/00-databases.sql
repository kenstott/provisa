-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The affirmation demo's five systems, each a database of one server, and the role Provisa reads
-- them as (the build's own initialisation; run/reader.sql creates the role where the bundle runs).
-- generate.py writes each database's tables and rows (10-oms.sql to 50-desk.sql).
CREATE ROLE provisa LOGIN PASSWORD 'provisa';
CREATE DATABASE affirmation_oms;     -- the order management system
CREATE DATABASE affirmation_ems;     -- the execution management system
CREATE DATABASE affirmation_broker;  -- the brokers' confirmations
CREATE DATABASE affirmation_custody; -- the custodian's settlement instructions
CREATE DATABASE affirmation_desk;    -- the middle office: SSIs, cutoffs, clock, affirmation log
