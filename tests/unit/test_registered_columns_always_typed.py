# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9c2a41-8d55-4c0e-9f1a-6b2e7d0c4a18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1426: no registered column is ever persisted without a data_type.

The catalog rendered "unknown" for such columns and the SQL layer had no type to compile against.
Two registrars dropped the type they already held — the GraphQL remote's native-filter (``_nf_*``)
columns and every gRPC remote column — and the repository accepted the NULL. These tests pin the
type at each writer and pin the repository's refusal as the backstop.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.core.models import Column, Table


class _FakeConn:
    """Enough of Connection for a registrar to run: every read returns no rows."""

    def __init__(self):
        self.execute = AsyncMock()
        self.upsert = AsyncMock()

    async def execute_core(self, *_a, **_k):
        result = MagicMock()
        result.fetchall.return_value = []
        result.fetchone.return_value = None
        return result


def _fake_db(conn):
    db = MagicMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=ctx)
    return db


# ---- GraphQL remote: native-filter columns (REQ-1426) ------------------------


@pytest.mark.asyncio
async def test_graphql_native_filter_columns_carry_the_argument_type(monkeypatch):
    from provisa.api.admin import graphql_remote_router as mod

    captured: list[Table] = []

    async def _capture(_conn, tbl):
        captured.append(tbl)
        return 1

    monkeypatch.setattr("provisa.core.repositories.table.upsert", _capture)
    monkeypatch.setattr("provisa.api.admin.actions_router._ensure_tables", AsyncMock())

    tables = [
        {
            "name": "petById",
            "columns": [{"name": "name", "type": "text"}],
            "required_args": [
                {"name": "id", "gql_type": "ID!", "provisa_type": "text"},
                {"name": "limit", "gql_type": "Int!", "provisa_type": "integer"},
            ],
        }
    ]
    conn = _FakeConn()
    await mod._upsert_tables_to_semantic_layer("gql", "d", tables, _fake_db(conn))

    cols = {c.name: c.data_type for c in captured[0].columns}
    assert cols == {"name": "varchar", "_nf_id": "varchar", "_nf_limit": "integer"}
    assert all(cols.values()), "no registered column may persist untyped"


def test_graphql_mapper_supplies_a_type_for_every_required_arg():
    """The router indexes _PROVISA_TO_PHYSICAL_TYPE by provisa_type — the mapper must set it."""
    from provisa.api.admin.graphql_remote_router import _PROVISA_TO_PHYSICAL_TYPE
    from provisa.graphql_remote.mapper import _build_required_args

    field = {
        "args": [
            {
                "name": "id",
                "type": {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "ID"}},
            },
            {
                "name": "n",
                "type": {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "Int"}},
            },
            {
                "name": "filter",
                "type": {"kind": "NON_NULL", "ofType": {"kind": "INPUT_OBJECT", "name": "F"}},
            },
        ]
    }
    args = _build_required_args(field)
    assert len(args) == 3
    for a in args:
        assert a["provisa_type"] in _PROVISA_TO_PHYSICAL_TYPE


# ---- gRPC remote: output and native-filter columns (REQ-1426) ----------------


@pytest.mark.asyncio
async def test_grpc_columns_carry_the_proto_resolved_type(monkeypatch):
    from provisa.api.admin import grpc_remote_router as mod

    captured: list[Table] = []

    async def _capture(_conn, tbl):
        captured.append(tbl)
        return 1

    monkeypatch.setattr("provisa.core.repositories.table.upsert", _capture)

    q = SimpleNamespace(
        service="OrderService",
        method="GetOrder",
        full_method_path="/orders.OrderService/GetOrder",
        columns=[
            SimpleNamespace(name="id", type="integer", object_fields=[]),
            SimpleNamespace(name="items", type="jsonb", object_fields=[]),
        ],
        input_fields=[SimpleNamespace(name="order_id", type="bigint", object_fields=[])],
    )
    conn = _FakeConn()
    await mod._register_schema("g", [q], [], conn, "ns", "d")

    cols = {c.name: c.data_type for c in captured[0].columns}
    assert cols == {"id": "integer", "items": "jsonb", "_nf_order_id": "bigint"}
    assert all(cols.values()), "no registered column may persist untyped"


# ---- DDN import: ObjectType field types (REQ-1426) ---------------------------


def test_ddn_columns_carry_the_object_type_field_type():
    from provisa.ddn.mapper import _map_model_to_table
    from provisa.ddn.models import DDNModel, DDNObjectType

    ot = DDNObjectType(
        name="Order",
        subgraph="app",
        fields={
            "id": "Int!",
            "total": "Float",
            "placedAt": "Timestamptz!",
            "tags": "[String!]!",
            "meta": "OrderMeta",
        },
    )
    model = DDNModel(
        name="Orders", subgraph="app", object_type="Order", connector_name="pg", collection="orders"
    )
    tbl = _map_model_to_table(model, ot, {}, {}, {})

    cols = {c.name: c.data_type for c in tbl.columns}
    assert cols == {
        "id": "integer",
        "total": "double precision",
        "placedAt": "timestamp with time zone",
        "tags": "jsonb",  # list → JSON in V1
        "meta": "jsonb",  # object type → JSON in V1
    }
    assert all(cols.values()), "no registered column may persist untyped"


# ---- GovData import: JDBC type codes (REQ-1426) ------------------------------


def test_govdata_columns_carry_the_jdbc_resolved_type():
    from provisa.govdata.schema_import import _jdbc_sql_type

    assert _jdbc_sql_type(4) == "integer"
    assert _jdbc_sql_type(12) == "varchar"
    assert _jdbc_sql_type(93) == "timestamp"
    with pytest.raises(ValueError, match="unmapped JDBC type code"):
        _jdbc_sql_type(9999)


# ---- config load: types come from the design, never from the source (REQ-1426) ----


def test_config_loader_never_infers_a_column_type():
    """data_type is design-time metadata. Loading a config must not inspect any data source for it.

    Inference belongs to design time only; a design that has not assigned every type is incomplete
    and must fail the load, so there can never be an unknown data type at run time.
    """
    import inspect

    from provisa.core import config_loader

    src = inspect.getsource(config_loader)
    assert "introspect_columns" not in src, "the loader must not introspect a source for types"
    assert "sqlite_column_types" not in src, "the loader must not read types out of a sqlite file"
    assert "data_type=" not in src, "nothing in the loader may assign a column type"


def test_shipped_configs_declare_every_column_type():
    """The seed demo configs are complete designs — every column carries a type."""
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    for name in ("provisa.yaml", "provisa-install.yaml"):
        doc = yaml.safe_load((root / "config" / name).read_text())
        untyped = [
            f"{t['source_id']}.{t['table']}.{c['name']}"
            for t in doc["tables"]
            for c in (t.get("columns") or [])
            if not c.get("data_type")
        ]
        assert untyped == [], f"{name} has untyped columns: {untyped}"


# ---- repository backstop (REQ-1426) -----------------------------------------


@pytest.mark.asyncio
async def test_repository_refuses_an_untyped_column():
    from provisa.core.repositories import table as table_repo

    conn = _FakeConn()
    conn.upsert_returning = AsyncMock(return_value=7)  # type: ignore[attr-defined]
    tbl = Table(
        source_id="s",
        domain_id="d",
        schema_name="public",
        table_name="t",
        columns=[Column(name="c", data_type=None, visible_to=[])],
    )
    with pytest.raises(ValueError, match="t.c has no data_type"):
        await table_repo.upsert(conn, tbl)  # type: ignore[arg-type]
