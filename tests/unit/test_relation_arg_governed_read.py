# Copyright (c) 2026 Kenneth Stott
# Canary: c428cc65-c6f7-4ca4-aaae-1ad82b43c063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command's relation argument given as a table names it by its logical, served name and is read
through the one governed pipeline as the calling role (REQ-1159, amended 2026-11-28): the role's
row filters, masks and column visibility hold, and only the declared input columns are read. A
physical source.schema.table is refused by name.

These run the real names table and the real governance stage, with routing stood in for: what
reaches routing is what would be read."""

# Requirements: REQ-1159

from __future__ import annotations

import copy
from unittest.mock import AsyncMock, patch

import pytest

import provisa.executor.function_dispatch as fd
from provisa.api.errors import ApiError
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.names_table import build_names_table, model_name_rows
from provisa.compiler.rls import RLSContext, read_rule
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_rewrite import semantic_table_name
from provisa.federation.replica_address import ReplicaRoutes
from provisa.security.masking import MaskingRule, MaskType
from tests.role_state import RoleState

pytestmark = pytest.mark.unit

_ORDERS = 1
_RULE = "region = 'eu'"


def _meta() -> TableMeta:
    return TableMeta(
        table_id=_ORDERS,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id="pet_store",
    )


def _tables(hidden: set[str]) -> list[dict]:
    return [
        {
            "id": _ORDERS,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": "orders",
            "domain_id": "pet_store",
            "columns": [
                {
                    "column_name": c,
                    "data_type": "varchar",
                    "visible_to": [] if c in hidden else ["analyst"],
                }
                for c in ("id", "region", "card", "secret")
            ],
        }
    ]


def _state(*, hidden: set[str] = frozenset({"secret"})) -> RoleState:
    """``analyst`` reads pet_store.orders (physically pg.public.orders): its EU rows only, its
    card masked, its secret column not visible."""
    ctx = CompilationContext()
    ctx.tables = {"orders": _meta()}
    tables = _tables(set(hidden))
    names = build_names_table(
        model_name_rows(
            ctx, tables, sql_name_of={_ORDERS: semantic_table_name(ctx.tables["orders"])}
        )
    )
    return RoleState(
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={"analyst": RLSContext(rules={_ORDERS: read_rule(_RULE)})},
        roles={
            "analyst": {
                "id": "analyst",
                "capabilities": ["ignore_relationships"],
                "domain_access": ["*"],
            }
        },
        masking_rules={
            (_ORDERS, "analyst"): {
                "card": (MaskingRule(mask_type=MaskType.constant, value="MASKED"), "varchar")
            }
        },
        tables=copy.deepcopy(tables),
        source_types={"pg": "postgresql"},
        source_catalogs={},
        relationships=[],
        metrics={},
        federation_engine=None,
        view_sql_map={},
        stored_view_reads=set(),
        role_build_inputs={
            "withdrawn_tables": [],
            "rls_rules": [
                {"role_id": "analyst", "table_id": _ORDERS, "filter_expr": read_rule(_RULE)}
            ],
            "tables": copy.deepcopy(tables),
        },
        security_high=False,
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
        model_stamp=1,
        names_table=names,
    )


def _fn(columns: list[str] | None) -> dict:
    arg: dict = {"name": "input", "type": "String", "arg_kind": "result_set"}
    if columns is not None:
        arg["columns"] = [{"name": c, "type": "text"} for c in columns]
    return {"name": "enrich_orders", "kind": "query", "arguments": [arg]}


class _Routed(Exception):
    """Raised by the routing stand-in: the statement cleared governance and reached routing."""

    def __init__(self, governed_sql: str) -> None:
        super().__init__(governed_sql)
        self.governed_sql = governed_sql


async def _read(monkeypatch, state: RoleState, fn: dict, ref: str) -> str:
    """The governed statement the argument's read reaches routing with."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", state, raising=False)

    async def _routing(_exec_sql, governed_sql, *_args, **_kwargs):  # noqa: ARG001
        raise _Routed(governed_sql)

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_routing)):
        with pytest.raises(_Routed) as routed:
            await fd._prepare_args(fn, {"input": ref}, state, "analyst")
    return routed.value.governed_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("ref", ["pet_store.orders", '"pet_store"."orders"'])
async def test_a_logical_name_is_read_as_the_role_with_its_row_filter_and_mask(monkeypatch, ref):
    governed = await _read(monkeypatch, _state(), _fn(["id", "card"]), ref)
    assert """WHERE "orders"."region" = 'eu'""" in governed  # the role's row filter
    assert "'MASKED' AS \"card\"" in governed  # the role's mask on card


@pytest.mark.asyncio
async def test_only_the_declared_columns_are_read(monkeypatch):
    governed = await _read(monkeypatch, _state(), _fn(["id"]), "pet_store.orders")
    select_list = governed.lower().split(" from ", 1)[0]
    assert "id" in select_list
    assert "card" not in select_list and "region" not in select_list
    assert "secret" not in select_list


@pytest.mark.asyncio
async def test_a_declared_column_the_role_does_not_see_is_refused(monkeypatch):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock()) as routing:
        with pytest.raises(PermissionError):
            await fd._prepare_args(
                _fn(["id", "secret"]), {"input": "pet_store.orders"}, state, "analyst"
            )
    routing.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ref", ["pg.public.orders", "public.orders"])
async def test_a_physical_reference_is_refused_naming_the_logical_form(monkeypatch, ref):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock()) as routing:
        with pytest.raises(ApiError) as refused:
            await fd._prepare_args(_fn(["id"]), {"input": ref}, state, "analyst")
    routing.assert_not_awaited()
    assert refused.value.status_code == 400
    assert refused.value.code == "functions.relation_table_not_logical"
    assert "logical name" in refused.value.detail and "schema.table" in refused.value.detail
    assert repr(ref) in refused.value.detail
