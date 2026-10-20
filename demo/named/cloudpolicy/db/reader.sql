-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The role Provisa reads the review store as. It writes only what the demo writes: the daily
-- trigger's review_history rows and the setup's clause embeddings.
CREATE ROLE provisa_demo LOGIN PASSWORD 'provisa_demo';
GRANT USAGE ON SCHEMA public TO provisa_demo;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_demo;
GRANT INSERT ON review_history, clause_vectors TO provisa_demo;
