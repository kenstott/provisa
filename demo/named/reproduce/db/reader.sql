-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.

--
-- The role Provisa reads the reproduce demo's database as: read only, but for the one column a
-- correction is written to (the demo's correct_fx_rate and reset_fx_rate commands, through
-- Provisa's governed write path). Run after db/init.sql in the build and after the bundle's dump
-- where the demo runs from its bundle.

CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;
GRANT UPDATE (fx_rate, entered_at) ON positions TO provisa_reader;
