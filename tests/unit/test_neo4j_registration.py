# Copyright (c) 2026 Kenneth Stott
# Canary: 8d1c3e5a-7b9f-4a2d-b6e8-0c4f2a6d8b13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a neo4j source (REQ-1670): the preview resolver, the registration hook, and
the table type's Cypher field."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from provisa.api.admin import _neo4j_registration as reg
from provisa.core.models import Column, Table

_SRC = {"id": "graph", "type": "neo4j", "host": "h", "port": 7474, "database": "neo4j"}


class _Conn:
    """execute_core for the sources lookup; upsert recorded for the persistence assertions."""

    def __init__(self, row: dict | None):
        self._row = row
        self.upserts: list[tuple[str, dict]] = []

    async def execute_core(self, stmt: Any):
        row = self._row

        class _Res:
            def fetchone(self_inner):
                if row is None:
                    return None
                return SimpleNamespace(_mapping=row)

        return _Res()

    async def upsert(self, table: Any, values: dict, **kw: Any) -> None:
        self.upserts.append((table.name, values))


def _tx_response(columns: list[str], rows: list[list]) -> dict:
    return {"results": [{"columns": columns, "data": [{"row": r} for r in rows]}], "errors": []}


@pytest.mark.asyncio
async def test_preview_returns_rows_and_ir_typed_columns():
    with respx.mock:
        respx.post("http://h:7474/db/neo4j/tx/commit").mock(
            return_value=httpx.Response(
                200, json=_tx_response(["adopter_id", "name", "score"], [[1, "Sara", 4.5]])
            )
        )
        out = await reg.preview_neo4j(_Conn(_SRC), "graph", "MATCH (a) RETURN a.id AS adopter_id")  # type: ignore[arg-type]
    assert out.error is None
    assert out.rows == [{"adopter_id": 1, "name": "Sara", "score": 4.5}]
    assert [(c.name, c.data_type) for c in out.columns] == [
        ("adopter_id", "integer"),
        ("name", "text"),
        ("score", "double"),
    ]


@pytest.mark.asyncio
async def test_preview_reports_a_node_projection_as_error_not_exception():
    with respx.mock:
        respx.post("http://h:7474/db/neo4j/tx/commit").mock(
            return_value=httpx.Response(
                200, json=_tx_response(["a"], [[{"labels": ["Adopter"], "properties": {}}]])
            )
        )
        out = await reg.preview_neo4j(_Conn(_SRC), "graph", "MATCH (a) RETURN a")  # type: ignore[arg-type]
    assert out.rows == [] and out.columns == []
    assert out.error is not None and "node object" in out.error


@pytest.mark.asyncio
async def test_preview_reports_a_cypher_error_and_an_unreachable_source():
    with respx.mock:
        respx.post("http://h:7474/db/neo4j/tx/commit").mock(
            return_value=httpx.Response(
                200,
                json={"results": [], "errors": [{"code": "Neo.ClientError.Statement.SyntaxError"}]},
            )
        )
        out = await reg.preview_neo4j(_Conn(_SRC), "graph", "MATCH")  # type: ignore[arg-type]
    assert out.error is not None and "SyntaxError" in out.error
    with respx.mock:
        respx.post("http://h:7474/db/neo4j/tx/commit").mock(
            side_effect=httpx.ConnectError("refused")
        )
        out = await reg.preview_neo4j(_Conn(_SRC), "graph", "RETURN 1")  # type: ignore[arg-type]
    assert out.error is not None and "ConnectError" in out.error


@pytest.mark.asyncio
async def test_preview_refuses_a_source_that_is_not_neo4j():
    out = await reg.preview_neo4j(_Conn({**_SRC, "type": "sqlite"}), "graph", "RETURN 1")  # type: ignore[arg-type]
    assert out.error == "'graph' is not a registered neo4j source"
    out = await reg.preview_neo4j(_Conn(None), "missing", "RETURN 1")  # type: ignore[arg-type]
    assert out.error is not None


def _table(query_template: str | None) -> Table:
    return Table(
        source_id="graph",
        domain_id="d",
        schema="neo4j",
        table="adopter",
        query_template=query_template,
        columns=[Column(name="adopter_id", data_type="integer", visible_to=["org_admin"])],
    )


@pytest.mark.asyncio
async def test_registration_refuses_a_neo4j_table_without_cypher(monkeypatch):
    err = await reg.persist_neo4j_registration(_Conn(_SRC), _table(None))  # type: ignore[arg-type]
    assert err is not None and err.success is False
    assert err.code == "schema.neo4j_query_required"


@pytest.mark.asyncio
async def test_registration_is_a_no_op_for_other_source_types():
    conn = _Conn({**_SRC, "type": "sqlite"})
    assert await reg.persist_neo4j_registration(conn, _table(None)) is None  # type: ignore[arg-type]
    assert conn.upserts == []


@pytest.mark.asyncio
async def test_registration_persists_the_endpoint_and_mirrors_live_state(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "api_sources", {}, raising=False)
    monkeypatch.setattr(state, "api_endpoints", {}, raising=False)
    conn = _Conn(_SRC)
    err = await reg.persist_neo4j_registration(conn, _table("MATCH (a) RETURN a.id AS adopter_id"))  # type: ignore[arg-type]
    assert err is None
    tables = [t for t, _ in conn.upserts]
    assert tables == ["api_sources", "api_endpoints"]
    ep = conn.upserts[1][1]
    assert ep["path"] == "/db/neo4j/tx/commit"
    assert ep["body_encoding"] == "neo4j_tx"
    assert ep["query_template"] == "MATCH (a) RETURN a.id AS adopter_id"
    assert state.api_sources["graph"].base_url == "http://h:7474"
    assert state.api_endpoints["adopter"].query_template == "MATCH (a) RETURN a.id AS adopter_id"


def test_table_type_reads_the_cypher_from_the_live_endpoint_map(monkeypatch):
    from provisa.api.admin.schema_helpers import _query_template_for
    from provisa.api.app import state

    monkeypatch.setattr(
        state,
        "api_endpoints",
        {"adopter": SimpleNamespace(query_template="MATCH (a) RETURN a.id AS id")},
        raising=False,
    )
    assert _query_template_for("adopter") == "MATCH (a) RETURN a.id AS id"
    assert _query_template_for("other") is None


def test_register_table_input_carries_query_template_into_the_model():
    from provisa.api.admin._live_mappers import table_model_from_input
    from provisa.api.admin.types import ColumnInput, TableInput

    inp = TableInput(
        source_id="graph",
        domain_id="d",
        schema_name="neo4j",
        table_name="adopter",
        columns=[ColumnInput(name="id", data_type="integer", visible_to=["org_admin"])],
        query_template="MATCH (a) RETURN a.id AS id",
    )
    from provisa.api.admin._table_ops import _build_column_models

    model = table_model_from_input(inp, _build_column_models(inp.columns), [], None)
    assert model.query_template == "MATCH (a) RETURN a.id AS id"
