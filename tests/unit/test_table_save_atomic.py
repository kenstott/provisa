# Copyright (c) 2026 Kenneth Stott
# Canary: 6044fe34-0ed2-46bc-8f62-80c7d7b1c396
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One save of a table applies everything, or nothing (REQ-2109, settled).

registerTable and updateTable carry the settings that also have mutations of their own -- the
naming convention, the role TTLs, replicate, the write grants and draft -- and apply all of them
with the table in one control-plane transaction. A refusal of any one leaves nothing stored: no
table row, no columns, no grants on a registration; the stored table as it was on an update."""

# Requirements: REQ-2109, REQ-1907, REQ-826, REQ-2074, REQ-1921

from __future__ import annotations

import types
from typing import Any

import pytest
import strawberry
from sqlalchemy import insert, select, update

import provisa.api.app as appmod
from provisa.api.admin import _table_ops, schema_mutation, schema_mutation_ops
from provisa.api.admin.types import (
    ColumnInput,
    PagingInput,
    RLSRuleInput,
    RoleTtlInput,
    TableInput,
    WriteGrantInput,
)
from provisa.core.database import Database
from provisa.core.models import Column
from provisa.core.repositories import write_grant as write_grant_repo
from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import (
    domains,
    registered_tables,
    rls_rules,
    roles,
    sources,
    table_columns,
)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", cache_ttl=300))
        # A source arming no refresh gate of its own: no Cache TTL, no probing signal.
        await conn.execute_core(
            insert(sources).values(id="bare", type="postgresql", change_signal="debezium")
        )
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="support", capabilities=["write"], domain_access=["sales"])
        )

    async def _pool():
        return db

    async def _nothing(*_a, **_k):
        return None

    async def _columns(_pool, inp):
        return [
            Column(name=c.name, data_type=c.data_type, visible_to=c.visible_to) for c in inp.columns
        ], None

    engine = types.SimpleNamespace(
        name="duckdb",
        reconcile_landed_tables=_nothing,
        materialize_store_target=lambda _org: ("mat_store", "views"),
    )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(
        appmod.state, "source_types", {"pg": "postgresql", "bare": "postgresql"}, raising=False
    )
    monkeypatch.setattr(appmod.state, "federation_engine", engine, raising=False)
    config = types.SimpleNamespace(graphql_remote=types.SimpleNamespace(max_rows=1000))
    monkeypatch.setattr(appmod.state, "config", config, raising=False)
    for module in (schema_mutation, schema_mutation_ops):
        monkeypatch.setattr(module, "_get_pool", _pool)
        monkeypatch.setattr(module, "_rebuild_schemas", _nothing)
        monkeypatch.setattr(module, "_build_columns_for_input", _columns)
        monkeypatch.setattr(module, "column_field_refusal", _nothing)
    monkeypatch.setattr(_table_ops, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation_ops, "reprovision_source_after_registration", _nothing)
    monkeypatch.setattr("provisa.dq.scan.reschedule_org_checkers", _nothing)
    monkeypatch.setattr("provisa.core.deployment_settings.auto_track_fk", lambda: False)
    monkeypatch.setattr(schema_mutation_ops, "_apply_mv_relationship_gate", _nothing)
    monkeypatch.setattr(schema_mutation, "_sync_view_mv", _nothing)
    monkeypatch.setattr(schema_mutation_ops, "_sync_view_mv", _nothing)
    monkeypatch.setattr("provisa.api.admin.schema_common.activate_view_mv", _nothing)
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


def _grant(**kw) -> WriteGrantInput:
    return WriteGrantInput(
        **{
            "role": "support",
            "operation": "update",
            "columns": ["status"],
            "rows": "status = 'placed'",
            "check": None,
            "presets": [],
            **kw,
        }
    )


def _input(**kw) -> TableInput:
    return TableInput(
        **{
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "columns": [
                ColumnInput(name="id", data_type="integer", visible_to=["support"]),
                ColumnInput(name="status", data_type="varchar", visible_to=["support"]),
            ],
            **kw,
        }
    )


EVERYTHING = {
    "gql_naming_convention": "snake",
    "role_ttl": [RoleTtlInput(role="support", ttl=60)],
    "replicate": -1,
    "write_grants": [_grant()],
    "draft": False,
}


async def _stored(db: Database) -> tuple[dict | None, list[str], list[dict]]:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(registered_tables).where(registered_tables.c.table_name == "orders")
            )
        ).fetchone()
        if row is None:
            return None, [], []
        table = dict(row._mapping)
        cols = [
            r.column_name
            for r in (
                await conn.execute_core(
                    select(table_columns.c.column_name).where(
                        table_columns.c.table_id == table["id"]
                    )
                )
            ).fetchall()
        ]
        grants = await write_grant_repo.for_table(conn, table["id"])
    return table, sorted(cols), grants


async def _register(**kw):
    return await schema_mutation_ops.register_table(_info(), _input(**kw))


async def test_a_registration_carrying_every_setting_stores_all_of_them(plane):
    result = await _register(**EVERYTHING)
    assert result.success, result.message
    table, cols, grants = await _stored(plane)
    assert table is not None
    assert table["gql_naming_convention"] == "snake"
    assert dict(table["role_ttl"]) == {"support": 60}
    assert table["replicate"] == -1
    assert table["draft"] is False
    assert cols == ["id", "status"]
    assert [(g["role"], g["operation"]) for g in grants] == [("support", "update")]


async def test_left_out_a_registration_takes_a_new_tables_values(plane):
    result = await _register()
    assert result.success, result.message
    table, _cols, grants = await _stored(plane)
    assert table is not None
    assert (table["draft"], table["replicate"], dict(table["role_ttl"])) == (True, None, {})
    assert grants == []


@pytest.mark.parametrize(
    ("refused", "code"),
    [
        ({"write_grants": [_grant(role="nobody")]}, "schema.role_not_found"),
        ({"write_grants": [_grant(columns=["nope"])]}, "schema.write_grant_invalid"),
        # Judged against the table as stored, inside the same transaction.
        ({"write_grants": [_grant(rows="nope = 1")]}, "schema.write_grant_invalid"),
        ({"role_ttl": [RoleTtlInput(role="nobody", ttl=5)]}, "schema.role_ttl_unknown_role"),
        ({"role_ttl": [RoleTtlInput(role="support", ttl=-1)]}, "schema.role_ttl_negative"),
        ({"replicate": -7}, "schema.replicate_invalid"),
        ({"gql_naming_convention": "camelCase"}, "schema.naming_convention_invalid"),
    ],
)
async def test_a_registration_refused_on_any_one_setting_stores_nothing(plane, refused, code):
    result = await _register(**{**EVERYTHING, **refused})
    assert (result.success, result.code) == (False, code)
    assert await _stored(plane) == (None, [], [])


async def test_an_update_carrying_every_setting_stores_all_of_them(plane):
    assert (await _register()).success
    result = await schema_mutation.Mutation().update_table(_info(), _input(**EVERYTHING))
    assert result.success, result.message
    table, cols, grants = await _stored(plane)
    assert table is not None
    assert (table["gql_naming_convention"], table["replicate"], table["draft"]) == (
        "snake",
        -1,
        False,
    )
    assert dict(table["role_ttl"]) == {"support": 60}
    assert cols == ["id", "status"] and len(grants) == 1


async def test_an_update_refused_on_any_one_setting_leaves_the_table_as_it_was(plane):
    assert (await _register(**EVERYTHING)).success
    before = await _stored(plane)
    changed = _input(
        description="changed",
        columns=[ColumnInput(name="id", data_type="integer", visible_to=["support"])],
        **{
            **EVERYTHING,
            "role_ttl": [RoleTtlInput(role="support", ttl=10)],
            "write_grants": [_grant(rows="nope = 1")],
        },
    )
    result = await schema_mutation.Mutation().update_table(_info(), changed)
    assert (result.success, result.code) == (False, "schema.write_grant_invalid")
    assert await _stored(plane) == before


async def test_an_update_leaving_a_setting_out_keeps_the_stored_one(plane):
    assert (await _register(**EVERYTHING)).success
    result = await schema_mutation.Mutation().update_table(_info(), _input(description="x"))
    assert result.success, result.message
    table, _cols, grants = await _stored(plane)
    assert table is not None
    assert (table["gql_naming_convention"], table["replicate"], table["draft"]) == (
        "snake",
        -1,
        False,
    )
    assert dict(table["role_ttl"]) == {"support": 60} and len(grants) == 1


# --- REQ-2109: the Cache TTL, paging, region, load protection, row rules and a materialized view's
# definition, in the same one save.


def _rule(**kw) -> RLSRuleInput:
    return RLSRuleInput(
        **{
            "role_id": "support",
            "filter_expr": "status = 'placed'",
            "description": "Keeps the placed orders support may read.",
            **kw,
        }
    )


async def _rules(db: Database) -> list[tuple[str, str]]:
    async with db.acquire() as conn:
        return [
            (r.role_id, r.description)
            for r in (
                await conn.execute_core(select(rls_rules.c.role_id, rls_rules.c.description))
            ).fetchall()
        ]


async def test_a_registration_stores_its_cache_ttl_load_protection_and_row_rules(plane):
    result = await _register(
        cache_ttl=600,
        load_protected=True,
        off_peak_window="01:00-05:00",
        row_rules=[_rule()],
    )
    assert result.success, result.message
    table, _cols, _grants = await _stored(plane)
    assert table is not None
    assert (table["cache_ttl"], table["load_protected"], table["off_peak_window"]) == (
        600,
        True,
        "01:00-05:00",
    )
    assert await _rules(plane) == [("support", "Keeps the placed orders support may read.")]


@pytest.mark.parametrize(
    ("refused", "code"),
    [
        ({"cache_ttl": -5}, "schema.invalid_cache_ttl"),
        ({"pagination": PagingInput(type="offset", page_size=10)}, None),
        (
            {"source_id": "bare", "load_protected": True, "write_grants": strawberry.UNSET},
            "schema.load_protection_gate_required",
        ),
        ({"row_rules": [_rule(filter_expr="nope = 1")]}, "schema.rls_rule_invalid"),
        ({"row_rules": [_rule(role_id="nobody")]}, "schema.role_not_found"),
        ({"row_rules": [_rule(description=" ")]}, "schema.rls_rule_description_missing"),
    ],
)
async def test_a_registration_refused_on_a_carried_setting_stores_nothing(plane, refused, code):
    result = await _register(**{**EVERYTHING, **refused})
    assert result.success is False
    if code is not None:
        assert result.code == code, result.message
    assert await _stored(plane) == (None, [], [])
    assert await _rules(plane) == []


async def test_a_view_whose_sql_is_not_deterministic_is_not_registered(plane):
    result = await _register(
        source_id=DERIVED_SOURCE_ID,
        schema_name="views",
        view_sql="SELECT random() AS r",
        materialize=True,
        columns=[ColumnInput(name="r", data_type="double", visible_to=["support"])],
    )
    assert result.success is False
    assert "non-deterministic" in result.message
    async with plane.acquire() as conn:
        assert (await conn.execute_core(select(registered_tables.c.id))).fetchall() == []


async def test_an_update_stores_its_cache_ttl_region_row_rules_and_paging(plane, monkeypatch):
    assert (await _register(**EVERYTHING)).success
    moved: list[tuple] = []

    async def _set_region(conn, table_id, region):
        moved.append((table_id, region))
        await conn.execute_core(
            registered_tables.update()
            .where(registered_tables.c.id == table_id)
            .values(region=region)
        )
        return "orders"

    monkeypatch.setattr("provisa.core.repositories.region.set_table_region", _set_region)
    result = await schema_mutation.Mutation().update_table(
        _info(),
        _input(**EVERYTHING, cache_ttl=900, region="eu", row_rules=[_rule()], pagination=None),
    )
    assert result.success, result.message
    table, _cols, _grants = await _stored(plane)
    assert table is not None
    assert (table["cache_ttl"], table["region"]) == (900, "eu")
    assert len(moved) == 1
    assert await _rules(plane) == [("support", "Keeps the placed orders support may read.")]


@pytest.mark.parametrize(
    ("refused", "code"),
    [
        ({"cache_ttl": -5}, "schema.invalid_cache_ttl"),
        ({"pagination": PagingInput(type="offset", page_size=10)}, None),
        (
            {"load_protected": True, "replicate": None, "cache_ttl": None, "_bare_source": True},
            "schema.load_protection_gate_required",
        ),
        ({"row_rules": [_rule(filter_expr="nope = 1")]}, "schema.rls_rule_invalid"),
        ({"region": "mars"}, "schema.region_refused"),
        ({"region": "eu", "_residency": True}, "security.data_residency_refused"),
        ({"view_sql": "SELECT now() AS t", "materialize": True}, None),
    ],
)
async def test_an_update_refused_on_a_carried_setting_leaves_the_table_as_it_was(
    plane, monkeypatch, refused, code
):
    assert (await _register(**EVERYTHING)).success
    before = await _stored(plane)
    refused = dict(refused)
    if refused.pop("_residency", False):
        from provisa.security.residency import ResidencyRefused

        def _refuse(_info, what, _before, after, *, draft=False):
            raise ResidencyRefused(what, after or "", holds_right=True)

        monkeypatch.setattr("provisa.api.admin.capabilities.require_residency_change", _refuse)
    if refused.pop("_bare_source", False):
        # The source no longer arms a refresh gate either: protected, the table has none.
        async with plane.acquire() as conn:
            await conn.execute_core(
                update(sources)
                .where(sources.c.id == "pg")
                .values(cache_ttl=None, change_signal="debezium")
            )
    changed = _input(**{**EVERYTHING, "description": "changed", **refused})
    result = await schema_mutation.Mutation().update_table(_info(), changed)
    assert result.success is False, result.message
    if code is not None:
        assert result.code == code, result.message
    assert await _stored(plane) == before
    assert await _rules(plane) == []
