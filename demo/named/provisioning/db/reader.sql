-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The role Provisa reads the warehouse database as: read only, but for the notices it writes (a
-- scheduled trigger's statement). Run after db/init.sql in the build and after the bundle's dump
-- where the demo runs from its bundle.

CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;
GRANT INSERT ON notices TO provisa_reader;
GRANT USAGE ON SEQUENCE notices_notice_id_seq TO provisa_reader;

-- The consumers record their own pulls (consumer.py).
CREATE ROLE consumer LOGIN PASSWORD 'consumer';
GRANT USAGE ON SCHEMA public TO consumer;
GRANT SELECT, INSERT ON pulls TO consumer;
GRANT USAGE ON SEQUENCE pulls_pull_id_seq TO consumer;
