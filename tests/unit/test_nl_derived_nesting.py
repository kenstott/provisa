# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6d2a8e-91c4-4b7e-a5d0-6e2c8b1f4a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The NL page's gRPC, JSON:API and REST panels read a row's relationships nested, in each
surface's own form, as the GraphQL they are derived from does (REQ-1929, amended 2026-11-16).

For "inspections with their building permit" the GraphQL is
``inspections { id result buildingPermit { permitNumber streetAddress } }``. Each derived panel
names the relationship the way its surface does: gRPC as ``read_mask`` relation paths (the
relationship's GraphQL name in proto snake_case), JSON:API as ``include`` with its own sparse
fieldset, REST as ``?fields=`` dot-paths. Each request is run through its surface's real
endpoint, and the answer carries the building permit nested in each inspection.

The context and the schema both come from one ``SchemaInput`` through the real ``build_context`` /
``generate_schema`` under the apollo (camelCase) convention, where a multi-word name is the only
thing that tells the GraphQL, proto and physical spellings apart. Only the governed pipeline's
two calls (route, execute) are stood in for, answering with the rows the engine would return.
"""

# Requirements: REQ-1929

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.compiler import naming as _naming
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.executor.result import QueryResult
from provisa.nl import executor
from provisa.nl.runner import (
    _aggregation_plan_from_graphql,
    _generate_grpc_query,
    _generate_jsonapi_query,
    _generate_openapi_query,
)
from provisa.pgwire import governed_plan
from tests.role_state import RoleState

_ROLE = "admin"
_GRAPHQL = "{ inspections { id result buildingPermit { permitNumber streetAddress } } }"
_PATH_MAP = {
    "inspections": {"domain_id": "city", "table_name": "inspections"},
    "buildingPermits": {"domain_id": "city", "table_name": "building_permits"},
}


def _col(name: str, data_type: str) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=False)


def _table(table_id: int, name: str, columns: list[str]) -> dict:
    return {
        "id": table_id,
        "source_id": "city-pg",
        "domain_id": "city",
        "schema_name": "public",
        "table_name": name,
        "columns": [{"column_name": c, "visible_to": [_ROLE]} for c in columns],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }


def _schema_input(extra_relationship: bool = False) -> SchemaInput:
    """inspections -> building permit (many-to-one). ``extra_relationship`` adds a second
    inspections -> building_permits relationship registered ahead of it."""
    relationships = [
        {
            "id": "insp-permit",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "building_permit_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    ]
    if extra_relationship:
        relationships.insert(
            0,
            {
                "id": "insp-reinspect",
                "source_table_id": 1,
                "target_table_id": 2,
                "source_column": "reinspection_permit_id",
                "target_column": "id",
                "cardinality": "many-to-one",
                "graphql_alias": "reinspectionPermit",
            },
        )
    inspection_columns = ["id", "result", "building_permit_id"] + (
        ["reinspection_permit_id"] if extra_relationship else []
    )
    return SchemaInput(
        tables=[
            _table(1, "inspections", inspection_columns),
            _table(2, "building_permits", ["id", "permit_number", "street_address"]),
        ],
        relationships=relationships,
        column_types={
            1: [_col(c, "varchar(20)" if c == "result" else "integer") for c in inspection_columns],
            2: [
                _col("id", "integer"),
                _col("permit_number", "varchar(20)"),
                _col("street_address", "varchar(100)"),
            ],
        },
        naming_rules=[],
        role={"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "city", "description": "City"}],
    )


@pytest.fixture()
def surface():
    _naming.configure(gql="apollo_graphql", sql="snake")
    si = _schema_input()
    yield SimpleNamespace(schema=generate_schema(si), context=build_context(si))
    _naming.configure()


def _plan(surface):
    plan = _aggregation_plan_from_graphql(surface.context, _GRAPHQL)
    assert plan is not None
    return plan


def _sql(surface, gql: str) -> list[str]:
    doc = parse_query(surface.schema, gql, {}, ctx=surface.context)
    return [c.sql for c in compile_query(doc, surface.context, {})]


# -- each panel's request: the relationship nested in the surface's own form ----------------------


def test_the_grpc_request_names_the_relationship_by_its_relation_field(surface):
    query, error = _generate_grpc_query(_plan(surface), set(), {})
    assert error is None
    assert query == (
        "QueryInspections(read_mask=[id, result, "
        "building_permit.permit_number, building_permit.street_address])"
    )


def test_the_jsonapi_request_includes_the_relationship(surface):
    query, error = _generate_jsonapi_query(_plan(surface), set(), {})
    assert error is None
    assert query == (
        "/data/jsonapi/city/inspections?fields[inspections]=id,result"
        "&include=building_permit&page[size]=20&page[total]=true"
    )


def test_the_rest_request_names_the_relationship_in_fields_dot_paths(surface):
    query, error = _generate_openapi_query(_plan(surface), set(), {})
    assert error is None
    assert query == (
        "GET /data/rest/city/inspections"
        "?fields=id,result,buildingPermit.permitNumber,buildingPermit.streetAddress&limit=20"
    )


def test_grpc_reads_a_second_relationship_to_the_same_table_by_its_own_field():
    """gRPC's ``{Type}`` message declares a relation field per relationship, named by it
    (REQ-1929), so a relationship registered after another to the same table is read by its own
    read_mask path, not refused."""
    _naming.configure(gql="apollo_graphql", sql="snake")
    try:
        si = _schema_input(extra_relationship=True)
        ctx = build_context(si)
        plan = _aggregation_plan_from_graphql(ctx, _GRAPHQL)
        assert plan is not None
        query, error = _generate_grpc_query(plan, set(), {})
        both = _aggregation_plan_from_graphql(
            ctx,
            "{ inspections { id buildingPermit { permitNumber }"
            " reinspectionPermit { permitNumber } } }",
        )
        assert both is not None
        query_both, error_both = _generate_grpc_query(both, set(), {})
    finally:
        _naming.configure()
    assert error is None
    assert query == (
        "QueryInspections(read_mask=[id, result, "
        "building_permit.permit_number, building_permit.street_address])"
    )
    assert error_both is None
    assert query_both == (
        "QueryInspections(read_mask=[id, building_permit.permit_number, "
        "reinspection_permit.permit_number])"
    )


# -- each request through its real endpoint: the building permit nested in each inspection -------


class _Pipeline:
    """Answers the one statement a read sends with ``rows``, recording it."""

    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.sql: list[str] = []

    async def govern(self, sql, role_id, *, exec_params=None, state=None, **_kwargs):
        self.sql.append(sql)
        return SimpleNamespace(sql=sql, params=list(exec_params or []), warnings=[])

    async def execute(self, plan, state):
        width = len(self.rows[0])
        return QueryResult(rows=self.rows, column_names=[f"c{i}" for i in range(width)])


def _state(monkeypatch, surface, rows: list[tuple]):
    state = RoleState(
        schemas={_ROLE: surface.schema},
        contexts={_ROLE: surface.context},
        rls_contexts={_ROLE: RLSContext.empty()},
        roles={_ROLE: {"id": _ROLE, "capabilities": ["query_development"], "domain_access": ["*"]}},
        masking_rules={},
        tables=[],
        schema_boot_id="boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        approval_hook=None,
        kafka_table_configs={},
        source_types={"city-pg": "postgresql"},
        table_path_maps={_ROLE: _PATH_MAP},
    )
    pipeline = _Pipeline(rows)
    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", pipeline.govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", pipeline.execute)
    return state, pipeline


_PERMIT = {"permitNumber": "BP-7", "streetAddress": "1 Main St"}


async def test_the_rest_panel_returns_the_permit_nested(monkeypatch, surface):
    query, _ = _generate_openapi_query(_plan(surface), set(), {})
    assert query is not None
    state, pipeline = _state(monkeypatch, surface, [(1, "pass", json.dumps(_PERMIT))])
    answer = await executor.execute(query, "openapi", _ROLE, state, deliver=None)
    assert answer == {"data": [{"id": 1, "result": "pass", "buildingPermit": _PERMIT}]}
    # The same semantic SQL the GraphQL compiles to (REQ-1929: derived, not re-invented).
    assert pipeline.sql == _sql(
        surface,
        "{ inspections(limit: 20) { id result buildingPermit { permitNumber streetAddress } } }",
    )


async def test_the_grpc_panel_returns_the_permit_nested(monkeypatch, surface):
    query, _ = _generate_grpc_query(_plan(surface), set(), {})
    assert query is not None
    permit = {"permit_number": "BP-7", "street_address": "1 Main St"}
    state, _pipeline = _state(monkeypatch, surface, [(1, "pass", json.dumps(permit))])
    answer = await executor.execute(query, "grpc", _ROLE, state, deliver=None)
    assert answer == {
        "columns": ["id", "result", "building_permit"],
        "rows": [{"id": 1, "result": "pass", "building_permit": permit}],
    }


async def test_the_jsonapi_panel_returns_the_permit_included(monkeypatch, surface):
    query, _ = _generate_jsonapi_query(_plan(surface), set(), {})
    assert query is not None
    permit = {"id": 7, "permitNumber": "BP-7", "streetAddress": "1 Main St"}
    # The JSON:API read selects id, result, the relationship (every column of the related
    # table: its resource) and the foreign key its linkage resolves by; then the page total.
    rows = [(1, "pass", json.dumps(permit), 7)]
    state, pipeline = _state(monkeypatch, surface, rows)
    count_rows = [(1,)]
    answers = iter([rows, count_rows])

    async def _execute(plan, _state):
        page = next(answers)
        return QueryResult(rows=page, column_names=[f"c{i}" for i in range(len(page[0]))])

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    answer = await executor.execute(query, "jsonapi", _ROLE, state, deliver=None)
    (resource,) = answer["data"]
    assert resource["type"] == "inspections"
    assert resource["id"] == "1"
    assert resource["attributes"] == {"result": "pass"}
    assert resource["relationships"]["building_permit"]["data"]["id"] == "7"
    (included,) = answer["included"]
    assert included["id"] == "7"
    assert included["attributes"] == {"permit_number": "BP-7", "street_address": "1 Main St"}
    assert answer["meta"]["total"] == 1
    assert len(pipeline.sql) == 2
