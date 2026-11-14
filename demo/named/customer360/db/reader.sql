-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The roles of the customer360 demo's database: run after db/init.sql and db/er.sql in the build
-- and after the bundle's dump where the demo runs from its bundle.
--
--   provisa_reader  what Provisa reads the hub (schema mdm) and the narrow entity bridges (schema
--                   er) as, read only
--   er_writer       the entity-resolution job (entity_resolution/er_job.py run): the one role that
--                   writes the bridges, replacing their rows on each run (REQ-2034)

CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';
GRANT USAGE ON SCHEMA mdm TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mdm TO provisa_reader;
GRANT USAGE ON SCHEMA er TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA er TO provisa_reader;

CREATE ROLE er_writer LOGIN PASSWORD 'er_writer';
GRANT USAGE ON SCHEMA er TO er_writer;
GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA er TO er_writer;
