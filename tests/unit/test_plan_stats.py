# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Plan-derived execution stats and the Mermaid DAG for raw-SQL surfaces (REQ-1517).

The GraphQL surface builds its DAG from the compiled field plan; a raw-SQL statement has no
fields, so the diagram is built from the governed plan instead. These tests pin what the plan
must be able to say: which sources were scanned, which route ran the statement and why, and
which Provisa optimizations replaced a scan.
"""

from dataclasses import dataclass

import pytest

from provisa.executor import stats as stats_mod
from provisa.executor.plan_stats import build_plan_mermaid, record_plan_execution


@dataclass
class _Route:
    name: str


@dataclass
class _FakePlan:
    route: _Route
    sql: str
    source_id: str
    dialect: str
    physical_sql: str | None = None
    sources: frozenset = frozenset()
    route_reason: str | None = None
    optimizations: tuple = ()


class _FakeState:
    def __init__(self, source_types):
        self.source_types = source_types


@pytest.fixture(autouse=True)
def _fresh_stats_context():
    stats_mod._ctx.set(None)
    yield
    stats_mod._ctx.set(None)


class TestBuildPlanMermaid:
    def test_every_source_becomes_a_scan_node_feeding_the_route(self):
        chart = build_plan_mermaid(
            sources=frozenset({"pg_main", "orders_sqlite"}),
            source_types={"pg_main": "postgresql", "orders_sqlite": "sqlite"},
            route="ENGINE",
            route_reason="multiple sources",
            optimizations=(),
            direct_source_id=None,
            elapsed_ms=12.0,
            rows=7,
        )
        assert chart.startswith("flowchart LR")
        assert 'n_pg_main["pg_main\\npostgresql"]' in chart
        assert 'n_orders_sqlite["orders_sqlite\\nsqlite"]' in chart
        assert "n_pg_main --> route" in chart
        assert "n_orders_sqlite --> route" in chart
        assert 'result(["7 rows"])' in chart

    def test_route_reason_labels_the_edge_to_the_result(self):
        chart = build_plan_mermaid(
            sources=frozenset({"pg_main"}),
            source_types={"pg_main": "postgresql"},
            route="DIRECT",
            route_reason="single source pushdown",
            optimizations=(),
            direct_source_id="pg_main",
            elapsed_ms=3.0,
            rows=1,
        )
        assert 'route["direct\\n(pg_main)"]' in chart
        assert 'route -->|"single source pushdown"| result' in chart

    def test_elapsed_labels_the_edge_when_the_router_gave_no_reason(self):
        chart = build_plan_mermaid(
            sources=frozenset({"pg_main"}),
            source_types={"pg_main": "postgresql"},
            route="ENGINE",
            route_reason=None,
            optimizations=(),
            direct_source_id=None,
            elapsed_ms=41.4,
            rows=2,
        )
        assert 'route -->|"41ms"| result' in chart

    def test_each_optimization_is_a_named_node_so_a_missing_scan_is_explained(self):
        chart = build_plan_mermaid(
            sources=frozenset({"pg_main"}),
            source_types={"pg_main": "postgresql"},
            route="ENGINE",
            route_reason="query rewritten to a materialized cache table",
            optimizations=("hot-table inline: currencies", "api cache: petstore_pets"),
            direct_source_id=None,
            elapsed_ms=8.0,
            rows=3,
        )
        assert "hot-table inline\\ncurrencies" in chart
        assert "api cache\\npetstore_pets" in chart
        assert "--> route" in chart
        assert "classDef provisaOpt" in chart

    def test_a_dropped_relation_is_named_even_though_it_never_scanned(self):
        chart = build_plan_mermaid(
            sources=frozenset({"pg_main"}),
            source_types={"pg_main": "postgresql"},
            route="ENGINE",
            route_reason=None,
            optimizations=("branch dropped: live_quotes",),
            direct_source_id=None,
            elapsed_ms=1.0,
            rows=0,
        )
        assert "branch dropped\\nlive_quotes" in chart


class TestRecordPlanExecution:
    def _plan(self, **kw):
        base = dict(
            route=_Route("ENGINE"),
            sql="SELECT 1",
            source_id="pg_main",
            dialect="trino",
            physical_sql="SELECT 1 FROM pg.public.t",
            sources=frozenset({"pg_main"}),
            route_reason="multiple sources",
        )
        base.update(kw)
        return _FakePlan(**base)

    def test_records_nothing_when_the_request_did_not_ask_for_stats(self):
        record_plan_execution(
            self._plan(), _FakeState({"pg_main": "postgresql"}), rows=1, elapsed_ms=1.0
        )
        assert stats_mod.current() is None

    def test_records_nothing_when_the_surface_records_its_own_entries(self):
        qs = stats_mod.begin(plan_entries=False)
        record_plan_execution(
            self._plan(), _FakeState({"pg_main": "postgresql"}), rows=1, elapsed_ms=1.0
        )
        assert qs.entries == []
        assert qs.mermaid is None

    def test_engine_route_reports_the_federated_strategy_and_the_physical_sql(self):
        qs = stats_mod.begin(plan_entries=True)
        record_plan_execution(
            self._plan(), _FakeState({"pg_main": "postgresql"}), rows=5, elapsed_ms=9.0
        )
        (entry,) = qs.entries
        assert entry.field == "sql"
        assert entry.source == "engine"
        assert entry.strategy == "federated:trino"
        assert entry.physical_sql == "SELECT 1 FROM pg.public.t"
        assert entry.rows == 5
        assert qs.mermaid is not None and qs.mermaid.startswith("flowchart LR")

    def test_direct_route_reports_the_source_and_its_type(self):
        qs = stats_mod.begin(plan_entries=True)
        plan = self._plan(
            route=_Route("DIRECT"),
            sql="SELECT 1 FROM public.t",
            physical_sql=None,
            route_reason="single source",
        )
        record_plan_execution(plan, _FakeState({"pg_main": "postgresql"}), rows=2, elapsed_ms=4.0)
        (entry,) = qs.entries
        assert entry.source == "pg_main"
        assert entry.strategy == "direct:postgresql"
        assert entry.physical_sql == "SELECT 1 FROM public.t"

    def test_a_batch_appends_one_chart_per_statement(self):
        qs = stats_mod.begin(plan_entries=True)
        state = _FakeState({"pg_main": "postgresql"})
        record_plan_execution(self._plan(), state, rows=1, elapsed_ms=1.0)
        record_plan_execution(self._plan(), state, rows=2, elapsed_ms=2.0)
        assert len(qs.entries) == 2
        assert qs.mermaid.count("flowchart LR") == 2
        # The UI splits on the blank line preceding each chart.
        assert "\n\nflowchart LR" in qs.mermaid

    def test_the_statement_label_names_the_entry(self):
        qs = stats_mod.begin(plan_entries=True, statement_label="cypher")
        record_plan_execution(
            self._plan(), _FakeState({"pg_main": "postgresql"}), rows=1, elapsed_ms=1.0
        )
        assert qs.entries[0].field == "cypher"

    def test_the_dag_is_carried_on_the_serialized_stats(self):
        qs = stats_mod.begin(plan_entries=True)
        record_plan_execution(
            self._plan(optimizations=("hot-table inline: currencies",)),
            _FakeState({"pg_main": "postgresql"}),
            rows=1,
            elapsed_ms=1.0,
        )
        payload = qs.to_dict()
        assert payload["mermaid"].startswith("flowchart LR")
        assert "hot-table inline" in payload["mermaid"]
        assert payload["sources"][0]["source"] == "engine"
