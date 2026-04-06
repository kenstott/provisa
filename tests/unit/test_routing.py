# Copyright (c) 2026 Kenneth Stott
# Canary: 2e8b5d3f-1a4c-4f7e-9b2d-6c0e3a5f1d8b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for smart routing decision logic.

Tests the `decide_route` function in `provisa.transpiler.router` covering all
routing scenarios: single-source direct, multi-source Trino, mutation routing,
steward hint overrides, and NoSQL/virtual source routing.

No real Trino infrastructure is required — the router makes decisions purely
from source type metadata without making network calls.
"""

import pytest

from provisa.executor.drivers.registry import has_driver
from provisa.transpiler.router import Route, RouteDecision, decide_route

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Shared source type / dialect fixtures
# ---------------------------------------------------------------------------

_TYPES: dict[str, str] = {
    "pg-main": "postgresql",
    "pg-secondary": "postgresql",
    "mongo-events": "mongodb",
    "kafka-stream": "kafka",
    "sf-warehouse": "snowflake",
    "mysql-legacy": "mysql",
}

_DIALECTS: dict[str, str] = {
    "pg-main": "postgres",
    "pg-secondary": "postgres",
    "mongo-events": None,
    "kafka-stream": None,
    "sf-warehouse": "snowflake",
    "mysql-legacy": "mysql",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRoutingDecisions:

    def test_single_source_routes_direct(self):
        """A single PostgreSQL source query routes to 'direct'."""
        decision = decide_route({"pg-main"}, _TYPES, _DIALECTS)
        assert decision.route == Route.DIRECT
        assert decision.source_id == "pg-main"
        assert decision.dialect == "postgres"
        assert decision.reason  # must always be populated

    def test_multi_source_routes_trino(self):
        """A query involving two different sources routes to Trino."""
        decision = decide_route({"pg-main", "pg-secondary"}, _TYPES, _DIALECTS)
        assert decision.route == Route.TRINO
        assert decision.source_id is None

    def test_mutation_always_routes_direct(self):
        """Mutations are never routed via Trino — always direct."""
        decision = decide_route(
            {"pg-main"}, _TYPES, _DIALECTS, is_mutation=True
        )
        assert decision.route == Route.DIRECT
        assert decision.source_id == "pg-main"
        assert "mutation" in decision.reason.lower()

    def test_mutation_always_routes_direct_even_with_nosql(self):
        """Even a NoSQL-sourced mutation routes direct (not Trino)."""
        decision = decide_route(
            {"mongo-events"}, _TYPES, _DIALECTS, is_mutation=True
        )
        assert decision.route == Route.DIRECT
        assert decision.source_id == "mongo-events"

    def test_route_override_hint_trino(self):
        """A `/* @provisa route=trino */`-style steward hint forces Trino route."""
        # The router accepts the hint as a string "trino" (extracted upstream
        # by the request layer from the comment; we test the routing primitive directly).
        decision = decide_route(
            {"pg-main"}, _TYPES, _DIALECTS, steward_hint="trino"
        )
        assert decision.route == Route.TRINO
        assert decision.source_id is None
        assert "override" in decision.reason.lower() or "steward" in decision.reason.lower()

    def test_route_override_hint_direct(self):
        """A `/* @provisa route=direct */`-style steward hint forces direct route."""
        decision = decide_route(
            {"pg-main"}, _TYPES, _DIALECTS, steward_hint="direct"
        )
        assert decision.route == Route.DIRECT
        assert decision.source_id == "pg-main"
        assert "override" in decision.reason.lower() or "steward" in decision.reason.lower()

    def test_nosql_source_routes_trino(self):
        """A query on a MongoDB source routes to Trino (no direct SQL driver)."""
        decision = decide_route({"mongo-events"}, _TYPES, _DIALECTS)
        assert decision.route == Route.TRINO
        assert decision.source_id is None

    def test_kafka_streaming_source_routes_trino(self):
        """A Kafka source (virtual) always routes to Trino."""
        decision = decide_route({"kafka-stream"}, _TYPES, _DIALECTS)
        assert decision.route == Route.TRINO
        assert decision.source_id is None

    def test_snowflake_without_direct_driver_routes_trino(self):
        """Snowflake without a registered direct driver routes to Trino."""
        if has_driver("snowflake"):
            pytest.skip("Snowflake direct driver is installed — test not applicable")
        decision = decide_route({"sf-warehouse"}, _TYPES, _DIALECTS)
        assert decision.route == Route.TRINO

    def test_multi_source_with_hint_direct_still_trino(self):
        """Direct hint on a multi-source query is ignored — must use Trino."""
        decision = decide_route(
            {"pg-main", "pg-secondary"}, _TYPES, _DIALECTS, steward_hint="direct"
        )
        assert decision.route == Route.TRINO

    def test_multi_source_nosql_mix_routes_trino(self):
        """Mixed RDBMS + NoSQL multi-source query routes to Trino."""
        decision = decide_route({"pg-main", "mongo-events"}, _TYPES, _DIALECTS)
        assert decision.route == Route.TRINO

    def test_route_decision_is_frozen_dataclass(self):
        """RouteDecision should be a frozen dataclass (immutable)."""
        decision = decide_route({"pg-main"}, _TYPES, _DIALECTS)
        assert isinstance(decision, RouteDecision)
        with pytest.raises((AttributeError, TypeError)):
            decision.route = Route.TRINO  # type: ignore[misc]

    def test_reason_always_populated(self):
        """Every routing decision must carry a human-readable reason."""
        for source_set in [
            {"pg-main"},
            {"pg-main", "pg-secondary"},
            {"mongo-events"},
            {"kafka-stream"}]:
            d = decide_route(source_set, _TYPES, _DIALECTS)
            assert d.reason, f"Empty reason for sources={source_set}"
            assert isinstance(d.reason, str)

    @pytest.mark.skipif(not has_driver("mysql"), reason="aiomysql not installed")
    def test_mysql_single_source_routes_direct(self):
        """MySQL with a direct driver routes direct."""
        decision = decide_route({"mysql-legacy"}, _TYPES, _DIALECTS)
        assert decision.route == Route.DIRECT
        assert decision.source_id == "mysql-legacy"
        assert decision.dialect == "mysql"
