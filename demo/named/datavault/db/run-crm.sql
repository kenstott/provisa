-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The role Provisa reads the data vault demo's crm database as where it runs from its bundle
-- (docker-compose.run.yml): read only, since every sandbox of the demo shares the database.
CREATE ROLE provisa LOGIN PASSWORD 'provisa';
GRANT USAGE ON SCHEMA crm TO provisa;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO provisa;
