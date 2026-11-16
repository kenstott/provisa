# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f3d52-7c1e-4a8b-9f24-6e5d1c3a7b90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: a statement binds a table's parameters in one step, on every surface.

Equality on an opaque parameter assigns its value for the call; IN assigns each value, one call
each, the results unioned; a join's equality on the parameter itself assigns one call per
distinct key; every other condition on it is refused naming it; a parameter left unbound on one
branch of an OR is refused; two values on one branch are a conflict; the rows a call returns are
not filtered again by the assignment; the parameter's column repeats the value asked.

The statements here are governed statements as stage 2 hands them on (each registered table read
through its governed relation). The source is a function of its arguments that records every
call; its rows go in as the pipeline's own substitution puts them (``InlineRows`` +
``build_values_ctes_sql``) and DuckDB answers the statement.
"""

# Requirements: REQ-1986

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import duckdb
import pytest
import sqlglot

from provisa.cache.values_cte import InlineRows, build_values_ctes_sql
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.nf_extractor import JoinKey, bind_parameters, read_base
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.core.parameter_binding import (
    CALLS_SETTING,
    ParameterByEqualityOnly,
    ParameterCallsOverCap,
    ParameterConflict,
    ParameterUnbound,
    is_opaque,
)
from provisa.federation.replica_address import ReplicaRoutes
from tests.role_state import RoleState

_TYPES = {"_nf_petId": "integer"}

#: The governed relation of ``pet_by_id`` (an OpenAPI operation with a path parameter), under ``p``.
_PET = (
    '(SELECT "p"."id" AS "id", "p"."name" AS "name", "p"."_nf_petId" AS "_nf_petId" '
    'FROM "api"."public"."pet_by_id" AS "p") AS p'
)
#: The governed relation of an ordinary table whose column a join gives the parameter from.
_ORDERS = (
    '(SELECT "o"."order_id" AS "order_id", "o"."pet" AS "pet" '
    'FROM "pg"."public"."orders" AS "o") AS o'
)


class _Source:
    """The remote operation: one row per call, whose own ``id`` is NOT the value asked (asked
    plus 100), so a row filtered again by its assignment would be lost."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def rows(self, args: dict) -> list[dict]:
        self.calls.append(dict(args))
        asked = args.get("petId")
        if asked is None:
            return [{"id": 0, "name": "every pet"}]
        return [{"id": int(asked) + 100, "name": f"pet {asked}"}]


def _answer(sql: str, params: list | None = None, *, keys: list | None = None, **how):
    """Bind ``sql``, call the source once per call the binding names, and answer the statement
    in DuckDB over the substituted rows. Returns (rows, the source)."""
    source = _Source()
    binding = bind_parameters(sql, params or [], _TYPES, **how)
    bound = binding.bound({join: keys or [] for join in binding.joins})
    tree = sqlglot.parse_one(bound.sql, dialect="postgres")
    entries: dict = {}
    for table in tree.find_all(sqlglot.exp.Table):
        if read_base(table.name) != "pet_by_id":
            continue
        _base, args = bound.reads.get(table.name, ("pet_by_id", bound.nf_args))
        entries[table.name] = InlineRows(source.rows(args), ["id", "name"])
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS pg")
    con.execute(
        "CREATE TABLE pg.main.orders AS SELECT * FROM (VALUES (1, 5), (2, 6), (3, 5)) t(order_id, pet)"
    )
    final = build_values_ctes_sql(bound.sql, entries).replace(
        '"pg"."public"."orders"', "pg.main.orders"
    )
    final = sqlglot.transpile(final, read="postgres", write="duckdb")[0]
    return sorted(con.execute(final, bound.params).fetchall()), source


# --- one value ----------------------------------------------------------------------------------


def test_equality_assigns_the_value_and_the_rows_are_not_filtered_by_it_again():
    rows, source = _answer(f'SELECT p.id, p.name FROM {_PET} WHERE p."_nf_petId" = 5')
    assert source.calls == [{"petId": 5}]
    assert rows == [(105, "pet 5")]  # its own id differs from the value asked: still returned


def test_the_parameters_column_repeats_the_value_asked():
    rows, _source = _answer(f'SELECT p.id, p."_nf_petId" FROM {_PET} WHERE p."_nf_petId" = 5')
    assert rows == [(105, 5)]


def test_a_bound_value_assigns_as_a_literal_does():
    rows, source = _answer(
        f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" = $1 AND p.name = $2', [5, "pet 5"]
    )
    assert source.calls == [{"petId": 5}]
    assert rows == [(105,)]


def test_a_result_column_filter_beside_the_assignment_is_applied_after_the_call():
    rows, source = _answer(f"SELECT p.id FROM {_PET} WHERE p.\"_nf_petId\" = 5 AND p.name = 'x'")
    assert source.calls == [{"petId": 5}]  # the call is made; its row then fails the filter
    assert rows == []


# --- a list of values ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "condition, params",
    [
        ('p."_nf_petId" IN (5, 6, 7)', []),
        ('p."_nf_petId" = ANY(ARRAY[5, 6, 7])', []),
        ('p."_nf_petId" = ANY($1)', [[5, 6, 7]]),
        ('p."_nf_petId" = 5 OR p."_nf_petId" = 6 OR p."_nf_petId" = 7', []),
    ],
)
def test_each_listed_value_is_one_call_and_each_row_holds_its_own_asked_value(condition, params):
    rows, source = _answer(f'SELECT p.id, p."_nf_petId" FROM {_PET} WHERE {condition}', params)
    assert sorted(c["petId"] for c in source.calls) == [5, 6, 7]
    assert rows == [(105, 5), (106, 6), (107, 7)]


def test_a_value_listed_twice_is_called_once():
    _rows, source = _answer(f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" IN (5, 5, 6)')
    assert sorted(c["petId"] for c in source.calls) == [5, 6]


# --- OR -----------------------------------------------------------------------------------------


def test_or_binds_on_each_branch_and_each_branchs_filter_applies_to_its_own_rows():
    rows, source = _answer(
        f"SELECT p.id FROM {_PET} WHERE (p.\"_nf_petId\" = 5 AND p.name = 'pet 5') "
        f"OR (p.\"_nf_petId\" = 6 AND p.name = 'no such pet')"
    )
    assert sorted(c["petId"] for c in source.calls) == [5, 6]
    assert rows == [(105,)]


def test_a_parameter_unbound_on_one_branch_of_an_or_is_refused():
    with pytest.raises(ParameterUnbound, match="_nf_petId") as refused:
        _answer(f"SELECT p.id FROM {_PET} WHERE p.\"_nf_petId\" = 5 OR p.name = 'x'")
    assert refused.value.code == "data.parameter_unbound_on_branch"
    assert refused.value.params == {"parameter": "_nf_petId"}


# --- a conflict ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "condition",
    [
        'p."_nf_petId" = 5 AND p."_nf_petId" = 6',
        'p."_nf_petId" IN (5, 6) AND p."_nf_petId" = 5',
        '(p."_nf_petId" = 5 AND p."_nf_petId" = 6) OR p."_nf_petId" = 7',
    ],
)
def test_two_different_values_on_one_branch_are_refused_not_last_wins_and_not_empty(condition):
    with pytest.raises(ParameterConflict, match="_nf_petId") as refused:
        _answer(f"SELECT p.id FROM {_PET} WHERE {condition}")
    assert refused.value.code == "data.parameter_conflict"


def test_the_same_value_given_twice_is_one_call():
    _rows, source = _answer(
        f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" = 5 AND p."_nf_petId" = 5'
    )
    assert source.calls == [{"petId": 5}]


# --- the refused forms --------------------------------------------------------------------------

_REFUSED = [
    ('p."_nf_petId" NOT IN (5, 6)', "NOT IN"),
    ('p."_nf_petId" <> 5', "<>"),
    ('p."_nf_petId" != 5', "<>"),
    ('NOT (p."_nf_petId" = 5)', "NOT (=)"),
    ('NOT p."_nf_petId" = 5', "NOT (=)"),
    ('p."_nf_petId" > 5', ">"),
    ('p."_nf_petId" <= 5', "<="),
    ('p."_nf_petId" BETWEEN 1 AND 9', "BETWEEN"),
    ("p.\"_nf_petId\" LIKE '5%'", "LIKE"),
    ('p."_nf_petId" IS NULL', "IS NULL"),
    ('p."_nf_petId" IS NOT NULL', "IS NOT NULL"),
    ('p."_nf_petId" IN (SELECT 5)', "IN (SELECT ...)"),
    ('p."_nf_petId" = p.id', "= (an expression)"),
    ('p."_nf_petId" = p.id + 1', "= (an expression)"),
]


@pytest.mark.parametrize("condition, form", _REFUSED)
def test_a_condition_that_assigns_an_opaque_parameter_nothing_is_refused_by_name(condition, form):
    with pytest.raises(ParameterByEqualityOnly) as refused:
        _answer(f"SELECT p.id FROM {_PET} WHERE {condition}")
    assert refused.value.code == "data.parameter_by_equality_only"
    assert refused.value.params == {"parameter": "_nf_petId", "form": form}
    assert "_nf_petId" in str(refused.value) and "REQ-" not in str(refused.value)


@pytest.mark.parametrize("condition, form", _REFUSED[:6])
def test_a_refused_form_beside_an_assignment_is_still_refused(condition, form):
    with pytest.raises(ParameterByEqualityOnly):
        _answer(f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" = 5 AND {condition}')


# --- a real column ------------------------------------------------------------------------------


def test_a_real_column_parameter_is_assigned_by_equality_and_in_as_any_parameter():
    rows, source = _answer(
        f'SELECT p.id, p."_nf_petId" FROM {_PET} WHERE p."_nf_petId" IN (5, 6)', real={"_nf_petId"}
    )
    assert sorted(c["petId"] for c in source.calls) == [5, 6]
    assert rows == [(105, 5), (106, 6)]


def test_a_real_column_parameter_keeps_its_meaning_under_any_other_condition():
    """A partition key or a column value is a real column of the table: a range on it is a
    filter, never refused."""
    binding = bind_parameters(
        f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" > 5', [], _TYPES, real={"_nf_petId"}
    )
    bound = binding.bound()
    assert bound.nf_args == {} and bound.reads == {} and '"_nf_petId" > 5' in bound.sql


def test_opaque_is_every_kind_but_a_real_column_of_the_table():
    kinds = ["path_param", "query_param", "graphql_arg", "grpc_input", "query_arg"]
    assert all(is_opaque({"native_filter_type": kind}) for kind in kinds)
    assert not is_opaque({"native_filter_type": "partition_key"})
    assert not is_opaque({"native_filter_type": "column_value"})
    assert not is_opaque({"native_filter_type": None})


# --- a join -------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "join",
    [
        f'{_ORDERS} JOIN {_PET} ON p."_nf_petId" = o.pet',
        f'{_ORDERS} JOIN {_PET} ON o.pet = p."_nf_petId"',
        f'{_ORDERS} LEFT JOIN {_PET} ON p."_nf_petId" = o.pet',
        f'{_ORDERS}, {_PET} WHERE p."_nf_petId" = o.pet',
    ],
)
def test_a_join_on_the_parameter_is_one_call_per_distinct_key(join):
    sql = f"SELECT o.order_id, p.id FROM {join}"
    binding = bind_parameters(sql, [], _TYPES)
    assert binding.joins == {JoinKey("orders", "public", "pet")}
    rows, source = _answer(sql, keys=[5, 6, 5, None])  # orders.pet as read: a duplicate, a NULL
    assert sorted(c["petId"] for c in source.calls) == [5, 6]  # duplicates are not repeated
    assert rows == [(1, 105), (2, 106), (3, 105)]


def test_a_join_that_gives_no_key_makes_no_call_and_joins_no_row():
    rows, source = _answer(
        f'SELECT o.order_id, p.id FROM {_ORDERS} LEFT JOIN {_PET} ON p."_nf_petId" = o.pet', keys=[]
    )
    assert source.calls == []
    assert rows == [(1, None), (2, None), (3, None)]


def test_a_join_on_a_result_column_does_not_assign():
    """The row's own ``id`` is never presumed to be the parameter: the join assigns nothing."""
    binding = bind_parameters(
        f"SELECT p.name FROM {_ORDERS} JOIN {_PET} ON p.id = o.pet", [], _TYPES
    )
    bound = binding.bound()
    assert binding.joins == set() and bound.nf_args == {} and bound.reads == {}


def test_a_join_on_the_parameter_beside_a_value_for_it_is_a_conflict():
    with pytest.raises(ParameterConflict, match="_nf_petId"):
        _answer(
            f'SELECT p.id FROM {_ORDERS} JOIN {_PET} ON p."_nf_petId" = o.pet '
            f'WHERE p."_nf_petId" = 5'
        )


# --- the cap ------------------------------------------------------------------------------------


def test_a_statement_past_the_cap_is_refused_naming_the_cap_and_the_setting():
    with pytest.raises(ParameterCallsOverCap) as refused:
        _answer(f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" IN (1, 2, 3)', cap=2)
    assert refused.value.code == "data.parameter_calls_over_cap"
    assert refused.value.params == {
        "table": "pet_by_id",
        "calls": "3",
        "cap": "2",
        "setting": CALLS_SETTING,
    }
    assert "2" in str(refused.value) and CALLS_SETTING in str(refused.value)


def test_a_statement_at_the_cap_is_answered():
    _rows, source = _answer(f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" IN (1, 2)', cap=2)
    assert len(source.calls) == 2


def test_a_joins_keys_past_the_cap_are_refused():
    with pytest.raises(ParameterCallsOverCap):
        _answer(
            f'SELECT p.id FROM {_ORDERS} JOIN {_PET} ON p."_nf_petId" = o.pet',
            keys=[1, 2, 3],
            cap=2,
        )


def test_the_cap_is_an_operator_setting_that_defaults_to_1000():
    from provisa.core import settings_catalog

    [setting] = [s for s in settings_catalog.DECLARED if s.key == CALLS_SETTING]
    assert (setting.default, setting.min, setting.type, setting.card) == (1000, 1, "int", "limits")


# --- kept rows ----------------------------------------------------------------------------------


def test_kept_rows_hold_the_asked_value_as_an_ordinary_column_with_ordinary_filters():
    """A table kept for good is read from its landed rows: the parameter is the stored column of
    its name, and a range on it is a filter, never refused and never a call."""
    binding = bind_parameters(
        f'SELECT p.id FROM {_PET} WHERE p."_nf_petId" > 5', [], _TYPES, {"_nf_petId": "petId"}
    )
    bound = binding.bound()
    assert bound.nf_args == {} and bound.reads == {}
    assert '"petId" > 5' in bound.sql and "_nf_petId" not in bound.sql


# --- on every surface: both stages of the one pipeline bind through the same step ---------------

_TABLES = [
    {
        "id": 1,
        "source_id": "api",
        "schema_name": "public",
        "table_name": "pet_by_id",
        "domain_id": "pets",
        "columns": [
            {"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]},
            {"column_name": "name", "data_type": "varchar", "visible_to": ["analyst"]},
            {
                "column_name": "_nf_petId",
                "data_type": "integer",
                "visible_to": ["analyst"],
                "native_filter_type": "path_param",
                "native_filter_required": True,
            },
        ],
    }
]


def _state() -> RoleState:
    ctx = CompilationContext()
    ctx.tables = {
        "pet_by_id": TableMeta(
            table_id=1,
            field_name="pet_by_id",
            type_name="PetById",
            source_id="api",
            catalog_name="api",
            schema_name="public",
            table_name="pet_by_id",
            domain_id="pets",
        )
    }
    ctx.native_filter_columns = {1: {"_nf_petId": "integer"}}
    return RoleState(
        model_stamp=1,
        admin_db=None,
        contexts={"analyst": ctx},
        rls_contexts={},
        roles={"analyst": {"capabilities": ["ignore_relationships"], "domain_access": ["*"]}},
        masking_rules={},
        tables=_TABLES,
        relationships=[],
        source_types={"api": "openapi"},
        source_catalogs={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        metrics={},
        schema_boot_id="test-boot",
        schema_version=1,
        compiled_query_cache=CompiledQueryCache(),
        routing_cache=CompiledQueryCache(),
        replica_routes=ReplicaRoutes(),
    )


async def _compiled(sql: str, state) -> None:
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire import _pipeline

    await _pipeline._govern_and_route_compiled(
        sql, "analyst", state=state, cache_hint=NO_CACHE_HINT, sdl_joins=True
    )


class _Captured(Exception):
    """What the optimization stage was handed: the test ends there."""


async def _routed(monkeypatch, stage: str, sql: str) -> dict:
    """What ``stage`` of the pipeline (the raw-SQL one every SQL surface enters, or the compiled
    one GraphQL enters) hands the optimization stage for ``sql``."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    captured: dict = {}

    async def _route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        captured.update(exec_sql=exec_sql, nf_args=kwargs.get("nf_args"), reads=kwargs["read_args"])
        raise _Captured

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_route)):
        with pytest.raises(_Captured):
            if stage == "sql":
                await _pipeline._govern_and_route(sql, "analyst")
            else:
                await _compiled(sql, state)
    return captured


_STAGES = ["sql", "compiled"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", _STAGES)
async def test_each_stage_binds_one_value(monkeypatch, stage):
    got = await _routed(monkeypatch, stage, 'SELECT id FROM pet_by_id WHERE "_nf_petId" = 5')
    assert got["nf_args"] == {"petId": 5} and got["reads"] == {}
    assert "_nf_petId" not in got["exec_sql"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", _STAGES)
async def test_each_stage_binds_a_list_to_one_call_per_value(monkeypatch, stage):
    got = await _routed(
        monkeypatch, stage, 'SELECT id, "_nf_petId" FROM pet_by_id WHERE "_nf_petId" IN (5, 6, 7)'
    )
    assert sorted(args["petId"] for _table, args in got["reads"].values()) == [5, 6, 7]
    assert {table for table, _args in got["reads"].values()} == {"pet_by_id"}
    assert got["exec_sql"].count("UNION ALL") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", _STAGES)
@pytest.mark.parametrize(
    "condition, refusal",
    [
        ('"_nf_petId" NOT IN (5)', ParameterByEqualityOnly),
        ('"_nf_petId" > 5', ParameterByEqualityOnly),
        ('"_nf_petId" IS NULL', ParameterByEqualityOnly),
        ('"_nf_petId" = 5 AND "_nf_petId" = 6', ParameterConflict),
        ("\"_nf_petId\" = 5 OR name = 'x'", ParameterUnbound),
    ],
)
async def test_each_stage_refuses_what_assigns_nothing(monkeypatch, stage, condition, refusal):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    sql = f"SELECT id FROM pet_by_id WHERE {condition}"
    with pytest.raises(refusal, match="_nf_petId"):
        if stage == "sql":
            await _pipeline._govern_and_route(sql, "analyst")
        else:
            await _compiled(sql, state)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", _STAGES)
async def test_each_stage_refuses_a_statement_past_the_operators_cap(monkeypatch, stage):
    import provisa.api.app as app_mod
    from provisa.core import settings_registry
    from provisa.pgwire import _pipeline

    state = _state()
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    value = settings_registry.value
    monkeypatch.setattr(
        settings_registry, "value", lambda key: 2 if key == CALLS_SETTING else value(key)
    )
    sql = 'SELECT id FROM pet_by_id WHERE "_nf_petId" IN (5, 6, 7)'
    with pytest.raises(ParameterCallsOverCap, match=CALLS_SETTING):
        if stage == "sql":
            await _pipeline._govern_and_route(sql, "analyst")
        else:
            await _compiled(sql, state)


# --- each refusal is a coded error with its catalog entry in every locale -----------------------


@pytest.mark.parametrize(
    "refusal",
    [
        ParameterByEqualityOnly("_nf_petId", "<>"),
        ParameterUnbound("_nf_petId"),
        ParameterConflict("_nf_petId", 5, 6),
        ParameterCallsOverCap("pet_by_id", 3, 2),
    ],
    ids=lambda r: r.code,
)
def test_each_refusal_has_its_message_in_every_locale_over_its_own_params(refusal):
    import json
    import re
    from pathlib import Path

    locales = Path(__file__).resolve().parents[2] / "provisa-ui/src/i18n/locales"
    group, key = refusal.code.split(".")
    catalogs = sorted(locales.glob("*/serverErrors.json"))
    assert len(catalogs) >= 13
    for catalog in catalogs:
        message = json.loads(catalog.read_text())["serverErrors"][group][key]
        assert set(re.findall(r"\{\{(\w+)\}\}", message)) == set(refusal.params), catalog
    assert "REQ-" not in str(refusal)
