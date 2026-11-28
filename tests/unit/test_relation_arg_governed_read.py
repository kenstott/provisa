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
physical source.schema.table is refused by name. A relation argument read from a table or a query
is held to the role's hard row cap -- its own max_rows, else the default row limit unless it holds
FULL_RESULTS, which reads it whole -- and a read past that cap is refused by name, never cut
(REQ-1159, amended 2026-11-28).

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
_DEFAULT_ROW_LIMIT = 100


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


def _state(
    *,
    hidden: set[str] = frozenset({"secret"}),
    max_rows: int | None = None,
    full_results: bool = False,
) -> RoleState:
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
                "capabilities": [
                    "ignore_relationships",
                    *(["full_results"] if full_results else []),
                ],
                "domain_access": ["*"],
                **({"max_rows": max_rows} if max_rows is not None else {}),
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
    """The governed statement the argument's read reaches routing with, the configured default
    row limit set at 100."""
    import provisa.api.app as app_mod
    import provisa.compiler.sql_gen as sql_gen
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr(sql_gen, "_get_default_row_limit", lambda: _DEFAULT_ROW_LIMIT)

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


_QUERY = 'SELECT "id", "card" FROM "pet_store"."orders"'


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
async def test_a_role_with_no_max_rows_is_held_to_the_default_row_limit(monkeypatch, given):
    governed = await _read(monkeypatch, _state(), _fn(["id", "card"]), given)
    assert governed.upper().rstrip().endswith(f"LIMIT {_DEFAULT_ROW_LIMIT}")


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
async def test_a_full_results_role_reads_the_relation_whole(monkeypatch, given):
    governed = await _read(monkeypatch, _state(full_results=True), _fn(["id", "card"]), given)
    assert "LIMIT" not in governed.upper()


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
@pytest.mark.parametrize("full_results", [False, True])
async def test_a_relation_read_is_held_to_the_roles_hard_row_cap(monkeypatch, given, full_results):
    state = _state(max_rows=500, full_results=full_results)
    governed = await _read(monkeypatch, state, _fn(["id", "card"]), given)
    assert governed.upper().rstrip().endswith("LIMIT 500")


def _pipeline_answering(
    monkeypatch, rows: int, outcome: str | None, cap: int | None, kind: str = "role"
) -> list:
    """The pipeline stood in for: a read answering ``rows`` rows, bounded at ``cap`` (the role's
    ceiling, or a table's: ``kind``) with what the check past it found (``outcome``: cut |
    unchecked | None)."""
    from types import SimpleNamespace

    from provisa.compiler.row_limit import RowLimit
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    asked: list = []
    plan = SimpleNamespace(
        row_limit=RowLimit(cap, kind) if cap is not None else None, limit_outcome=None
    )

    async def _govern(sql, role_id, **kw):
        asked.append((sql, role_id, kw))
        return plan

    async def _execute(given, _state):
        given.limit_outcome = outcome
        return QueryResult(
            rows=[(str(i), f"c{i}") for i in range(rows)],
            column_names=["id", "card"],
            column_types=["varchar", "varchar"],
        )

    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    return asked


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
async def test_a_150_row_relation_arrives_whole_for_a_full_results_role(monkeypatch, given):
    asked = _pipeline_answering(monkeypatch, 150, None, None)
    payload, _refs = await fd._prepare_args(
        _fn(["id", "card"]), {"input": given}, _state(full_results=True), "analyst"
    )
    assert [kw for _sql, _role, kw in asked] == [{"relation_argument": True}]
    assert len(payload["input"]["rows"]) == 150


async def _refused(monkeypatch, given: str, state: RoleState, outcome: str, cap: int, kind: str):
    _pipeline_answering(monkeypatch, cap, outcome, cap, kind)
    with pytest.raises(ApiError) as refused:
        await fd._prepare_args(_fn(["id", "card"]), {"input": given}, state, "analyst")
    assert refused.value.status_code == 400
    assert refused.value.code == "functions.relation_over_row_cap"
    return refused.value


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
@pytest.mark.parametrize("outcome", ["cut", "unchecked"])
async def test_a_relation_past_the_hard_row_cap_is_refused_by_name(monkeypatch, given, outcome):
    refused = await _refused(monkeypatch, given, _state(max_rows=150), outcome, 150, "role")
    assert refused.params == {
        "name": "input",
        "cap": 150,
        "cap_kind": "role",
        "role": "analyst",
        "outcome": outcome,
    }
    detail = refused.detail
    assert "'input'" in detail and "150 rows" in detail and "'analyst'" in detail


@pytest.mark.asyncio
@pytest.mark.parametrize("given", ["pet_store.orders", _QUERY])
async def test_a_relation_past_the_default_row_limit_is_refused_naming_the_default(
    monkeypatch, given
):
    """A role with no max_rows and no FULL_RESULTS: governance bounds the read at the default
    row limit as the role's ceiling, and the refusal names the default as whose cap it is."""
    import provisa.compiler.sql_gen as sql_gen

    monkeypatch.setattr(sql_gen, "_get_default_row_limit", lambda: _DEFAULT_ROW_LIMIT)
    refused = await _refused(monkeypatch, given, _state(), "cut", _DEFAULT_ROW_LIMIT, "role")
    assert refused.params["cap"] == _DEFAULT_ROW_LIMIT
    assert refused.params["cap_kind"] == "default"
    assert "default row limit" in refused.detail


@pytest.mark.asyncio
async def test_a_relation_past_a_tables_row_cap_is_refused_naming_the_table(monkeypatch):
    refused = await _refused(monkeypatch, _QUERY, _state(full_results=True), "cut", 40, "table")
    assert refused.params["cap"] == 40
    assert refused.params["cap_kind"] == "table"
    assert "a table role 'analyst' reads" in refused.detail
