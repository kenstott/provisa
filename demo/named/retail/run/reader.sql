-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The role Provisa reads the retail demo's three databases as, where the demo runs from its
-- bundle (docker-compose.run.yml, after the dump is restored): a dump carries no role and no grant,
-- and db/ is the build's own initialisation, which creates them there.

CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';

\connect group_ref
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;

\connect eu_shop
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;

\connect us_shop
GRANT USAGE ON SCHEMA public TO provisa_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;
