# Copyright (c) 2026 Kenneth Stott
# Canary: 7c41e0b9-52d6-4a18-b3f7-0e9d6a2c8f15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2229: OpenLineage run events join the lineage graph.

A dataset naming a registered table is that table's node, any other is shown as outside the model,
and every edge says where it came from.
"""

# Requirements: REQ-2229

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import app as app_module
from provisa.api.admin import lineage_router, openlineage_router
from provisa.api.admin.lineage_router import federation_graph
from provisa.api.errors import ApiError
from provisa.core.db import STANDARD_ROLES
from provisa.lineage.graph import Edge, LineageGraph, Node
from provisa.lineage.read_rights import readable_graph
from provisa.lineage.reported import (
    OPENLINEAGE,
    InvalidRunEvent,
    RegisteredTable,
    add_reported_lineage,
    dataset_rows,
    parse_run_event,
    reported_jobs,
    reported_seeds,
    resolve_dataset,
)
from provisa.security.rights import Capability

PG = "postgres://db.internal:5432"


def _table(relation: str, table: str, columns: dict[str, str]) -> RegisteredTable:
    return RegisteredTable(
        relation=relation,
        source_type="postgresql",
        host="db.internal",
        port=5432,
        database="warehouse",
        schema="public",
        table=table,
        columns=columns,
    )


ORDERS = _table("sales.orders", "orders", {"amount": "amount", "id": "id"})
MART = _table("sales.mart", "mart", {"total": "total"})
TABLES = [ORDERS, MART]


def _dataset(name: str, namespace: str = PG, **facets: Any) -> dict:
    return {"namespace": namespace, "name": name, "facets": facets}


def _event(inputs: list[dict], outputs: list[dict], **overrides: Any) -> dict:
    return {
        "eventType": "COMPLETE",
        "eventTime": "2026-11-09T10:00:00.000Z",
        "producer": "https://github.com/apache/airflow",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": "01890000-0000-7000-8000-000000000001"},
        "job": {"namespace": "airflow", "name": "load_mart"},
        "inputs": inputs,
        "outputs": outputs,
        **overrides,
    }


def _graph(*events: dict, tables: list[RegisteredTable] | None = None) -> LineageGraph:
    graph = LineageGraph()
    rows = []
    for event in events:
        parsed = parse_run_event(event)
        rows += [
            {"job_namespace": parsed.job_namespace, "job_name": parsed.job_name, **r}
            for r in dataset_rows(parsed)
        ]
    add_reported_lineage(graph, reported_jobs(rows), TABLES if tables is None else tables)
    return graph


def _pairs(graph: LineageGraph) -> set[tuple[str, str]]:
    return {(e.source, e.target) for e in graph.edges}


JOB = "openlineage-job:airflow/load_mart"
READS_AND_WRITES = _event(
    [_dataset("warehouse.public.orders")], [_dataset("warehouse.public.mart")]
)


class TestTheScenario:
    def test_an_edge_joins_the_two_tables_through_the_job(self):
        graph = _graph(READS_AND_WRITES)
        hub = f"{JOB}.*"
        assert ("sales.orders.amount", hub) in _pairs(graph)
        assert ("sales.orders.id", hub) in _pairs(graph)
        assert (hub, "sales.mart.total") in _pairs(graph)
        assert graph.nodes[hub].kind == "job"
        assert graph.nodes[hub].label == "load_mart"

    def test_the_registered_ends_are_the_tables_own_nodes(self):
        graph = _graph(READS_AND_WRITES)
        assert graph.nodes["sales.orders.amount"].kind == "source"
        assert graph.nodes["sales.orders.amount"].relation == "sales.orders"

    def test_every_edge_is_marked_as_reported_by_openlineage(self):
        graph = _graph(READS_AND_WRITES)
        assert graph.edges and all(e.origin == OPENLINEAGE for e in graph.edges)
        assert {e["origin"] for e in graph.to_dict()["edges"]} == {OPENLINEAGE}

    def test_an_edge_a_definition_produced_says_so(self):
        graph = LineageGraph()
        graph.add_edge(Edge("a.x", "b.x", "x", ()))
        assert graph.to_dict()["edges"][0]["origin"] == "definition"

    def test_a_dataset_that_is_not_registered_is_outside_the_model(self):
        graph = _graph(
            _event(
                [_dataset("warehouse.public.orders")],
                [_dataset("exports/daily.csv", "s3://archive")],
            )
        )
        node = graph.nodes["outside:s3://archive/exports/daily.csv.*"]
        assert node.kind == "external"
        assert node.label == "s3://archive/exports/daily.csv"
        served = {n["id"]: n for n in graph.to_dict()["nodes"]}
        assert served[node.id]["label"] == "s3://archive/exports/daily.csv"


class TestWhichDatasetNamesATable:
    @pytest.mark.parametrize(
        "namespace,name",
        [
            (PG, "warehouse.public.orders"),
            (PG, "public.orders"),
            ("postgres://DB.INTERNAL:5432", "WAREHOUSE.PUBLIC.ORDERS"),
            ("postgresql://db.internal", "warehouse.public.orders"),
        ],
    )
    def test_the_source_and_the_name_match(self, namespace, name):
        assert resolve_dataset(TABLES, namespace, name) == [ORDERS]

    @pytest.mark.parametrize(
        "namespace,name",
        [
            ("postgres://other-host:5432", "warehouse.public.orders"),
            ("postgres://db.internal:6543", "warehouse.public.orders"),
            ("mysql://db.internal:5432", "warehouse.public.orders"),
            (PG, "other_db.public.orders"),
            (PG, "private.orders"),
            (PG, "orders"),
            ("postgres://db.internal:notaport", "warehouse.public.orders"),
        ],
    )
    def test_the_same_name_elsewhere_is_not_that_table(self, namespace, name):
        assert resolve_dataset(TABLES, namespace, name) == []

    def test_a_table_registered_twice_is_both_nodes(self):
        twin = _table("finance.orders", "orders", {"amount": "amount"})
        assert resolve_dataset([ORDERS, twin], PG, "public.orders") == [ORDERS, twin]


def _column_facet(**columns: list[tuple[str, str]]) -> dict:
    return {
        "fields": {
            column: {
                "inputFields": [
                    {
                        "namespace": PG,
                        "name": name,
                        "field": field,
                        "transformations": [{"type": "DIRECT", "subtype": "AGGREGATION"}],
                    }
                    for name, field in inputs
                ]
            }
            for column, inputs in columns.items()
        }
    }


class TestColumnLineageFacet:
    def test_each_output_column_comes_only_from_the_inputs_it_names(self):
        graph = _graph(
            _event(
                [_dataset("public.orders")],
                [
                    _dataset(
                        "public.mart",
                        columnLineage=_column_facet(total=[("public.orders", "amount")]),
                    )
                ],
            )
        )
        step = f"{JOB}.sales.mart.total"
        assert _pairs(graph) == {("sales.orders.amount", step), (step, "sales.mart.total")}
        assert graph.nodes[step].column == "total"
        assert next(e for e in graph.edges if e.target == step).transform == "DIRECT:AGGREGATION"

    def test_a_column_nobody_registered_adds_no_node(self):
        graph = _graph(
            _event(
                [_dataset("public.orders")],
                [
                    _dataset(
                        "public.mart",
                        columnLineage=_column_facet(
                            total=[("public.orders", "amount"), ("public.orders", "unregistered")],
                            ghost=[("public.orders", "amount")],
                        ),
                    )
                ],
            )
        )
        assert "sales.orders.unregistered" not in graph.nodes
        assert "sales.mart.ghost" not in graph.nodes

    def test_an_input_the_facet_does_not_name_still_reaches_the_output(self):
        other = _table("sales.fx", "fx", {"rate": "rate"})
        graph = _graph(
            _event(
                [_dataset("public.orders"), _dataset("public.fx")],
                [
                    _dataset(
                        "public.mart",
                        columnLineage=_column_facet(total=[("public.orders", "amount")]),
                    )
                ],
            ),
            tables=[*TABLES, other],
        )
        assert ("sales.fx.rate", f"{JOB}.*") in _pairs(graph)
        assert (f"{JOB}.*", "sales.mart.total") in _pairs(graph)

    def test_two_outputs_with_one_column_name_stay_apart(self):
        twin = _table("sales.mart2", "mart2", {"total": "total"})
        facet = _column_facet(total=[("public.orders", "amount")])
        other = _column_facet(total=[("public.orders", "id")])
        graph = _graph(
            _event(
                [_dataset("public.orders")],
                [
                    _dataset("public.mart", columnLineage=facet),
                    _dataset("public.mart2", columnLineage=other),
                ],
            ),
            tables=[*TABLES, twin],
        )
        assert ("sales.orders.amount", f"{JOB}.sales.mart.total") in _pairs(graph)
        assert ("sales.orders.amount", f"{JOB}.sales.mart2.total") not in _pairs(graph)

    def test_an_outside_column_is_its_own_row(self):
        facet = _column_facet(total=[("public.orders", "amount")])
        graph = _graph(
            _event(
                [_dataset("public.orders")],
                [_dataset("reports.daily", "bigquery", columnLineage=facet)],
            )
        )
        assert graph.nodes["outside:bigquery/reports.daily.total"].column == "total"


class TestJobsWithOneSide:
    def test_a_job_that_only_reads(self):
        graph = _graph(_event([_dataset("public.orders")], []))
        assert ("sales.orders.amount", f"{JOB}.*") in _pairs(graph)

    def test_a_job_that_only_writes(self):
        graph = _graph(_event([], [_dataset("public.mart")]))
        assert (f"{JOB}.*", "sales.mart.total") in _pairs(graph)


class TestParsing:
    @pytest.mark.parametrize(
        "body",
        [
            [],
            {"eventTime": "2026-11-09T10:00:00Z", "job": {"namespace": "a", "name": "b"}},
            _event([], [], eventTime="yesterday"),
            _event([], [], eventTime="2026-11-09T10:00:00"),
            _event([], [], run={"runId": ""}),
            _event([], [], job={"namespace": "a"}),
            _event("nope", []),  # type: ignore[arg-type]
            _event([{"namespace": "a"}], []),
            _event([], [_dataset("t", columnLineage={"fields": {"c": {"inputFields": "x"}}})]),
            _event([], [_dataset("t", columnLineage={"fields": {"c": {"inputFields": [{}]}}})]),
        ],
    )
    def test_what_is_not_a_run_event_is_refused(self, body):
        with pytest.raises(InvalidRunEvent):
            parse_run_event(body)

    def test_inputs_and_outputs_may_be_absent(self):
        body = _event([], [])
        del body["inputs"], body["outputs"]
        event = parse_run_event(body)
        assert event.inputs == () and event.outputs == ()


def _rights_graph() -> LineageGraph:
    return _graph(
        _event(
            [_dataset("public.orders"), _dataset("raw/orders.csv", "s3://inbox")],
            [_dataset("public.mart"), _dataset("exports/out.csv", "s3://archive")],
        )
    )


class TestReadRights:
    REGISTERED = {"sales.orders", "sales.mart"}

    def test_a_caller_who_reads_a_table_the_job_touches_sees_the_job_and_the_outside(self):
        cut = readable_graph(
            _rights_graph(), registered=self.REGISTERED, readable={"sales.mart.total"}
        )
        assert f"{JOB}.*" in cut.nodes
        assert "outside:s3://archive/exports/out.csv.*" in cut.nodes
        assert "sales.orders.amount" not in cut.nodes

    def test_a_job_that_touches_only_what_the_caller_cannot_read_is_gone(self):
        cut = readable_graph(_rights_graph(), registered=self.REGISTERED, readable=set())
        assert cut.nodes == {}
        assert "load_mart" not in json.dumps(cut.to_dict())
        assert "archive" not in json.dumps(cut.to_dict())

    def test_a_job_wholly_outside_the_model_shows(self):
        graph = _graph(
            _event([_dataset("a", "s3://x")], [_dataset("b", "s3://y")]),
        )
        cut = readable_graph(graph, registered=self.REGISTERED, readable=set())
        assert f"{JOB}.*" in cut.nodes

    def test_an_edge_joined_across_a_hidden_step_keeps_its_origin(self):
        graph = LineageGraph()
        for nid, relation, kind in [
            ("sales.orders.amount", "sales.orders", "source"),
            ("sales.hidden.x", "sales.hidden", "derived"),
            ("sales.mart.total", "sales.mart", "derived"),
        ]:
            graph.add_node(Node(id=nid, column=nid.rsplit(".", 1)[1], relation=relation, kind=kind))
        graph.add_edge(Edge("sales.orders.amount", "sales.hidden.x", "", (), origin=OPENLINEAGE))
        graph.add_edge(Edge("sales.hidden.x", "sales.mart.total", "", (), origin=OPENLINEAGE))
        cut = readable_graph(
            graph,
            registered={"sales.orders", "sales.mart", "sales.hidden"},
            readable={"sales.orders.amount", "sales.mart.total"},
        )
        assert [(e.source, e.target, e.origin) for e in cut.edges] == [
            ("sales.orders.amount", "sales.mart.total", OPENLINEAGE)
        ]

    def test_an_edge_that_loses_its_transform_keeps_its_origin(self):
        graph = LineageGraph()
        for nid, relation, kind in [
            ("sales.orders.amount", "sales.orders", "source"),
            ("sales.hidden.x", "sales.hidden", "source"),
            ("sales.mart.total", "sales.mart", "derived"),
        ]:
            graph.add_node(Node(id=nid, column=nid.rsplit(".", 1)[1], relation=relation, kind=kind))
        graph.add_edge(Edge("sales.orders.amount", "sales.mart.total", "x", (), origin=OPENLINEAGE))
        graph.add_edge(Edge("sales.hidden.x", "sales.mart.total", "x", (), origin=OPENLINEAGE))
        cut = readable_graph(
            graph,
            registered={"sales.orders", "sales.mart", "sales.hidden"},
            readable={"sales.orders.amount", "sales.mart.total"},
        )
        assert [(e.transform, e.origin) for e in cut.edges] == [("", OPENLINEAGE)]


class TestSeeds:
    def test_a_scoped_lens_reaches_only_what_its_seeds_report_to(self):
        graph = _rights_graph()
        added = reported_seeds(graph, {"sales.mart.total"}, unscoped=False)
        assert added == set()
        added = reported_seeds(graph, {"sales.orders.amount"}, unscoped=False)
        assert f"{JOB}.*" in added
        assert "outside:s3://archive/exports/out.csv.*" in added
        assert "sales.mart.total" not in added

    def test_no_lens_shows_every_reported_node(self):
        graph = _graph(_event([_dataset("a", "s3://x")], [_dataset("b", "s3://y")]))
        assert reported_seeds(graph, set(), unscoped=True) == {
            f"{JOB}.*",
            "outside:s3://x/a.*",
            "outside:s3://y/b.*",
        }


def _column(table_id: int, table: str, column: str) -> dict:
    return {
        "table_id": table_id,
        "domain_id": "sales",
        "table_name": table,
        "column_name": column,
        "alias": None,
    }


COLUMNS = [_column(1, "orders", "amount"), _column(2, "mart", "total")]


@pytest.fixture
def wired(monkeypatch):
    """Complete Lineage over two registered tables, one reported job, an analyst who reads both."""

    async def nothing(state):
        return []

    async def columns(state):
        return COLUMNS

    async def reported(state, column_rows):
        parsed = parse_run_event(READS_AND_WRITES)
        rows = [
            {"job_namespace": parsed.job_namespace, "job_name": parsed.job_name, **r}
            for r in dataset_rows(parsed)
        ]
        return reported_jobs(rows), TABLES

    context = SimpleNamespace(
        tables={1: SimpleNamespace(table_id=1), 2: SimpleNamespace(table_id=2)},
        physical_to_sql={(1, "amount"): "amount", (2, "total"): "total"},
    )
    monkeypatch.setattr(lineage_router, "_fetch_view_rows", nothing)
    monkeypatch.setattr(lineage_router, "_fetch_registry_columns", columns)
    monkeypatch.setattr(lineage_router, "_native_tables", nothing)
    monkeypatch.setattr(lineage_router, "_reported_lineage", reported)
    monkeypatch.setattr(app_module.state, "contexts", {"analyst": context}, raising=False)


def _request(role: str = "analyst") -> Any:
    return SimpleNamespace(state=SimpleNamespace(role=role, identity=None), headers={})


class TestCompleteLineage:
    def test_the_graph_shows_the_reported_edge_through_the_job(self, wired):
        out = asyncio.run(federation_graph(_request()))
        edges = {(e["source"], e["target"]): e["origin"] for e in out["edges"]}
        assert edges[("sales.orders.amount", f"{JOB}.*")] == OPENLINEAGE
        assert edges[(f"{JOB}.*", "sales.mart.total")] == OPENLINEAGE
        kinds = {n["id"]: n["kind"] for n in out["nodes"]}
        assert kinds[f"{JOB}.*"] == "job"


def _gate_request(body: bytes, capabilities: set[str]) -> Any:
    async def read() -> bytes:
        return body

    identity = SimpleNamespace(user_id="airflow", capabilities=capabilities)
    return SimpleNamespace(
        state=SimpleNamespace(identity=identity, role="r"), body=read, headers={}
    )


class TestTheEndpoint:
    @pytest.fixture
    def stored(self, monkeypatch):
        saved: list[Any] = []

        async def store(state, event):
            saved.append(event)

        monkeypatch.setattr(openlineage_router, "_store", store)
        monkeypatch.setattr(
            "provisa.api.admin.capabilities._resolved_capabilities",
            lambda identity, state: identity.capabilities,
        )
        return saved

    def test_a_run_event_is_accepted_and_stored(self, stored):
        request = _gate_request(json.dumps(READS_AND_WRITES).encode(), {"lineage_report"})
        out = asyncio.run(openlineage_router.receive_run_event(request))
        assert out == {"status": "accepted"}
        assert stored[0].job_name == "load_mart"

    def test_a_caller_without_the_capability_is_refused(self, stored):
        request = _gate_request(json.dumps(READS_AND_WRITES).encode(), set())
        with pytest.raises(ApiError) as refused:
            asyncio.run(openlineage_router.receive_run_event(request))
        assert refused.value.status_code == 403
        assert stored == []

    @pytest.mark.parametrize("body", [b"{not json", b"[]", b'{"eventTime": "x"}'])
    def test_a_body_that_is_not_a_run_event_is_a_422(self, stored, body):
        request = _gate_request(body, {"lineage_report"})
        with pytest.raises(ApiError) as refused:
            asyncio.run(openlineage_router.receive_run_event(request))
        assert refused.value.status_code == 422
        assert stored == []

    def test_the_route_is_the_openlineage_default_path(self):
        assert any(
            route.path == "/api/v1/lineage" and "POST" in route.methods
            for route in openlineage_router.router.routes
        )


class TestTheCapability:
    def test_it_is_a_declared_capability(self):
        assert Capability("lineage_report") is Capability.LINEAGE_REPORT

    def test_the_role_that_registers_sources_holds_it(self):
        assert "lineage_report" in STANDARD_ROLES["source_admin"].capabilities
