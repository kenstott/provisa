-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The role Provisa reads the affirmation demo's five databases as, where the demo runs from its
-- bundle (docker-compose.run.yml, after the dump is restored): a dump carries no role and no grant.
-- Read only: the databases are shared by every sandbox of the demo, the affirmation log among them
-- (written once, when the bundle was built).

CREATE ROLE provisa LOGIN PASSWORD 'provisa';

\connect affirmation_oms
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;

\connect affirmation_ems
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;

\connect affirmation_broker
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;

\connect affirmation_custody
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;

\connect affirmation_desk
GRANT USAGE ON SCHEMA public TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa;
