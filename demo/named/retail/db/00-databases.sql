-- Copyright (c) 2026 Kenneth Stott
--
-- This source code is licensed under the Business Source License 1.1
-- found in the LICENSE file in the root directory of this source tree.
--
-- NOTICE: Use of this software for training artificial intelligence or
-- machine learning models is strictly prohibited without explicit written
-- permission from the copyright holder.
--
-- The retail group's databases, one container: each subsidiary's own, and the group's shared
-- reference data. Each is a source of its own in config.yaml, homed where its data lives.
CREATE DATABASE eu_shop;   -- the EU subsidiary (homed in eu)
CREATE DATABASE us_shop;   -- the US subsidiary (homed in us)
CREATE DATABASE group_ref; -- products and stores, shared by both (no region)
