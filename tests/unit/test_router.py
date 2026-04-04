# Copyright (c) 2025 Kenneth Stott
# Canary: 3575b36d-bd8b-46ea-9e6c-11221e63774c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for routing decision logic."""

import pytest

from provisa.executor.drivers.registry import has_driver
from provisa.transpiler.router import Route, decide_route


TYPES = {"pg1": "postgresql", "pg2": "postgresql", "mongo1": "mongodb",
         "sf1": "snowflake", "mysql1": "mysql", "ora1": "oracle",
         "duck1": "duckdb", "cass1": "cassandra"}
DIALECTS = {"pg1": "postgres", "pg2": "postgres", "mongo1": "mongodb",
            "sf1": "snowflake", "mysql1": "mysql", "ora1": "oracle",
            "duck1": "duckdb", "cass1": "cassandra"}


class TestSingleSourceDirect:
    def test_postgresql_routes_direct(self):
        d = decide_route({"pg1"}, TYPES, DIALECTS)
        assert d.route == Route.DIRECT
        assert d.source_id == "pg1"
        assert d.dialect == "postgres"

    @pytest.mark.skipif(not has_driver("mysql"), reason="aiomysql not installed")
    def test_mysql_routes_direct(self):
        d = decide_route({"mysql1"}, TYPES, DIALECTS)
        assert d.route == Route.DIRECT
        assert d.source_id == "mysql1"
        assert d.dialect == "mysql"


class TestSingleSourceDirect_Additional:
    @pytest.mark.skipif(not has_driver("oracle"), reason="oracledb not installed")
    def test_oracle_routes_direct(self):
        d = decide_route({"ora1"}, TYPES, DIALECTS)
        assert d.route == Route.DIRECT
        assert d.source_id == "ora1"

    @pytest.mark.skipif(not has_driver("duckdb"), reason="duckdb not installed")
    def test_duckdb_routes_direct(self):
        d = decide_route({"duck1"}, TYPES, DIALECTS)
        assert d.route == Route.DIRECT
        assert d.source_id == "duck1"


class TestSingleSourceTrino:
    def test_nosql_mongodb_routes_trino(self):
        d = decide_route({"mongo1"}, TYPES, DIALECTS)
        assert d.route == Route.TRINO
        assert d.source_id is None

    def test_nosql_cassandra_routes_trino(self):
        d = decide_route({"cass1"}, TYPES, DIALECTS)
        assert d.route == Route.TRINO

    def test_no_direct_driver_routes_trino(self):
        d = decide_route({"sf1"}, TYPES, DIALECTS)
        assert d.route == Route.TRINO


class TestMultiSource:
    def test_multi_source_routes_trino(self):
        d = decide_route({"pg1", "pg2"}, TYPES, DIALECTS)
        assert d.route == Route.TRINO

    def test_multi_source_mixed_types_routes_trino(self):
        d = decide_route({"pg1", "mongo1"}, TYPES, DIALECTS)
        assert d.route == Route.TRINO


class TestStewardOverride:
    def test_steward_trino_override(self):
        d = decide_route({"pg1"}, TYPES, DIALECTS, steward_hint="trino")
        assert d.route == Route.TRINO

    def test_steward_direct_override(self):
        d = decide_route({"pg1"}, TYPES, DIALECTS, steward_hint="direct")
        assert d.route == Route.DIRECT
        assert d.source_id == "pg1"

    def test_steward_direct_ignored_for_nosql(self):
        """Direct hint on nosql source falls through to normal logic."""
        d = decide_route({"mongo1"}, TYPES, DIALECTS, steward_hint="direct")
        assert d.route == Route.TRINO

    def test_steward_direct_ignored_for_multi_source(self):
        d = decide_route({"pg1", "pg2"}, TYPES, DIALECTS, steward_hint="direct")
        assert d.route == Route.TRINO


class TestMutationRouting:
    def test_mutation_always_routes_direct(self):
        d = decide_route({"pg1"}, TYPES, DIALECTS, is_mutation=True)
        assert d.route == Route.DIRECT
        assert d.source_id == "pg1"
        assert "mutation" in d.reason

    def test_mutation_overrides_nosql(self):
        """Mutations to NoSQL sources still route direct (write to source)."""
        d = decide_route({"mongo1"}, TYPES, DIALECTS, is_mutation=True)
        assert d.route == Route.DIRECT
        assert d.source_id == "mongo1"


class TestReasonProvided:
    def test_reason_populated(self):
        d = decide_route({"pg1"}, TYPES, DIALECTS)
        assert d.reason
        assert isinstance(d.reason, str)
