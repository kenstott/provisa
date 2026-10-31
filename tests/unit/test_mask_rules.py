# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two rules of a column's mask list (REQ-2059). A list holding an entry no reader can reach is
not saved, on every path that saves one. And once a column has any mask, a reader no entry
matches reads it completely masked: only an entry gives unmasked."""

# Requirements: REQ-2059

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
import sqlglot
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import actions_router
from provisa.api.errors import ApiError
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.core import domain_policy
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.database import Database
from provisa.core.models import Column, Function, Role, Table, Webhook
from provisa.core.repositories import function as function_repo
from provisa.core.repositories import role as role_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources, table_columns, tracked_functions
from provisa.security.masking import (
    MaskingRule,
    MaskUnreachable,
    MaskType,
    masks_report,
    reader_masks,
    resolve,
    unreachable_refusal,
)
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_landing_ttl_admin import _db, _table_input, _table_patches

PREFIX = "You have defined a mask that is not reachable: "
NULLED = {"type": "constant", "value": None}
WITHHELD = {"type": "constant", "value": "withheld"}
# ``clerk`` inherits ``junior``, so the entry for junior holds every clerk first.
COVERED = [{"to": ["junior"], "mask": None}, {"to": ["clerk"], "mask": WITHHELD}]
SHADOWED = [{"to": ["analyst"], "mask": None}, {"to": ["analyst"], "mask": WITHHELD}]
REFUSED = PREFIX + "[analyst]"


def _chain(role: str) -> list[str]:
    return {"analyst": ["analyst", "developer"]}.get(role, [role])


# --- the refusal's words --------------------------------------------------------------------


def test_the_refusal_names_each_unreachable_entry_by_its_roles():
    masks = [
        {"to": ["analyst", "developer"], "mask": None},
        {"to": ["developer", "analyst"], "mask": WITHHELD},
        {"to": [], "mask": WITHHELD},
        {"mask": NULLED},
    ]
    assert unreachable_refusal(masks, _chain) == PREFIX + "[developer, analyst], [no one]"


def test_the_entry_for_everyone_is_named_everyone():
    masks = [{"to": [], "mask": None}, {"mask": NULLED}]
    assert unreachable_refusal(masks[:1], _chain) == PREFIX + "[no one]"
    assert unreachable_refusal(masks[1:], _chain) is None


def test_a_list_every_entry_of_which_is_reached_has_no_refusal():
    assert unreachable_refusal([{"to": ["analyst"], "mask": None}, {"mask": NULLED}], _chain) is None
    assert unreachable_refusal([], _chain) is None


def test_the_report_no_longer_warns_of_readers_it_does_not_list():
    assert "unlisted_unmasked" not in masks_report([{"to": ["analyst"], "mask": None}], _chain)


# --- every path that saves a list -----------------------------------------------------------


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    """The role ``clerk`` inheriting ``junior``, a source and a domain."""
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        for role_id, parents in (("junior", []), ("clerk", ["junior"])):
            await role_repo.upsert(
                conn,
                Role(
                    id=role_id,
                    description="Test role.",
                    capabilities=[],
                    domain_access=["*"],
                    parents=parents,
                ),
                org_id="acme",
            )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", appmod.state.tenant_db, raising=False)
    monkeypatch.setattr(
        appmod.state,
        "roles",
        {"everywhere": {"id": "everywhere", "capabilities": [], "domain_access": ["*"]}},
        raising=False,
    )
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    monkeypatch.setattr(actions_router, "require_capability_request", lambda *_a: None)
    monkeypatch.setattr(actions_router, "_saved_domain", lambda _r, _n, d: d)
    monkeypatch.setattr(actions_router, "_require_reclassification", AsyncMock())
    monkeypatch.setattr(actions_router, "_require_its_domain", AsyncMock())
    monkeypatch.setattr(actions_router, "_created_grant", lambda _r, grants: grants)

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def _table(masks: list[dict]) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema="public",
        table="orders",
        columns=[
            Column(name="email", data_type="varchar", visible_to=["clerk"], masks=masks),
        ],
    )


async def _stored(db: Database) -> list:
    async with db.acquire() as conn:
        return [r[0] for r in (await conn.execute_core(select(table_columns.c.masks))).fetchall()]


async def test_the_one_write_of_a_table_refuses_a_covered_entry_by_inheritance(plane):
    """Reached only through role inheritance, read from the roles the store holds."""
    async with plane.acquire() as conn:
        with pytest.raises(MaskUnreachable) as refused:
            await table_repo.upsert(conn, _table(COVERED))
    assert str(refused.value) == PREFIX + "[clerk]"
    assert refused.value.column == "email"


async def test_a_derived_value_role_after_its_parent_is_refused(plane):
    masks = [{"to": ["region"], "mask": None}, {"to": ["region__eu"], "mask": WITHHELD}]
    async with plane.acquire() as conn:
        with pytest.raises(MaskUnreachable, match=r"\[region__eu\]"):
            await table_repo.upsert(conn, _table(masks))


async def test_a_reachable_list_is_saved_with_its_everyone_entry(plane):
    masks = [{"to": ["analyst"], "mask": WITHHELD}, {"to": None, "mask": None}]
    async with plane.acquire() as conn:
        await table_repo.upsert(conn, _table(masks))
    [stored] = await _stored(plane)
    assert [(e["to"], e["mask"]) for e in stored] == [(["analyst"], WITHHELD), (None, None)]


@pytest.mark.asyncio
async def test_the_table_mutation_answers_the_refusal(control_plane, monkeypatch):
    from provisa.api.admin.schema_mutation import Mutation

    serve_every_column(monkeypatch)
    column = Column(name="id", data_type="integer", visible_to=["analyst"], masks=SHADOWED)
    async with _db(control_plane) as db:
        patches = list(_table_patches(db))
        patches[3] = patch(
            "provisa.api.admin.schema_mutation._build_columns_for_input",
            new=AsyncMock(return_value=([column], None)),
        )
        for p in patches:
            p.start() if hasattr(p, "start") else p.__enter__()
        try:
            result = await Mutation().update_table(MagicMock(), _table_input())
        finally:
            for p in reversed(patches):
                p.stop() if hasattr(p, "stop") else p.__exit__(None, None, None)
    assert result.success is False
    assert result.message == REFUSED
    assert result.code == "schema.mask_unreachable"
    assert result.params == {"table": "orders", "column": "id"}


def _config(masks: list[dict]) -> dict:
    return {
        "sources": [
            {"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
        ],
        "domains": [{"id": "sales"}],
        "roles": [
            {"id": "seller", "description": "Test role.", "capabilities": [], "domain_access": ["*"]}
        ],
        "tables": [
            {
                "source_id": "cfg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["seller"], "masks": masks}
                ],
            }
        ],
        "relationships": [],
    }


async def test_a_config_load_refuses_the_list(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        with pytest.raises(MaskUnreachable) as refused:
            await apply_config(parse_config_dict(_config(SHADOWED)), conn)
    assert str(refused.value) == REFUSED


async def _refused(call: Any) -> str:
    with pytest.raises(ApiError) as refused:
        await call
    assert refused.value.status_code == 422
    assert refused.value.code == "actions.mask_unreachable"
    return str(refused.value.message)


OUT = [{"name": "email", "type": "varchar", "masks": SHADOWED}]


def _function() -> actions_router.FunctionInput:
    return actions_router.FunctionInput(
        name="refund",
        sourceId="pg",
        functionName="refund",
        returns="",
        domainId="sales",
        outputColumns=OUT,
    )


def _webhook() -> actions_router.WebhookInput:
    return actions_router.WebhookInput(
        name="notify", url="http://x", domainId="sales", inlineReturnType=OUT
    )


async def test_the_rest_saves_of_a_command_refuse_the_list(plane):
    request = MagicMock()
    assert await _refused(actions_router.create_function(request, _function())) == REFUSED
    assert await _refused(actions_router.update_function(request, "refund", _function())) == REFUSED
    async with plane.acquire() as conn:
        assert (await conn.execute_core(select(tracked_functions))).fetchall() == []


async def test_the_rest_saves_of_a_webhook_refuse_the_list(plane):
    request = MagicMock()
    assert await _refused(actions_router.create_webhook(request, _webhook())) == REFUSED
    assert await _refused(actions_router.update_webhook(request, "notify", _webhook())) == REFUSED


async def test_the_store_refuses_a_command_or_webhook_list_from_a_config(plane):
    from provisa.core.models import DatasetColumn, InlineType

    async with plane.acquire() as conn:
        with pytest.raises(MaskUnreachable):
            await function_repo.upsert_function(
                conn,
                Function(
                    name="refund",
                    source_id="pg",
                    function_name="refund",
                    returns="",
                    domain_id="sales",
                    output_columns=[DatasetColumn(**OUT[0])],
                ),
            )
        with pytest.raises(MaskUnreachable):
            await function_repo.upsert_webhook(
                conn,
                Webhook(
                    name="notify",
                    url="http://x",
                    domain_id="sales",
                    inline_return_type=[InlineType(**OUT[0])],
                ),
            )


def test_the_check_route_says_the_refusal_and_takes_the_entry_for_everyone(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from types import SimpleNamespace

    from provisa.api.admin import masks_router

    monkeypatch.setattr(masks_router, "require_capability_request", lambda *_a: None)
    monkeypatch.setattr(
        appmod,
        "state",
        SimpleNamespace(role_chains={}, settings_overrides={}, config=None),
    )
    app = FastAPI()
    app.include_router(masks_router.router)
    resp = TestClient(app).post(
        "/admin/masks/check", json={"masks": [*SHADOWED, {"to": None, "mask": NULLED}]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["refusal"] == REFUSED


# --- a reader no entry matches --------------------------------------------------------------


LISTED = [{"to": ["vip"], "mask": None}]


def test_a_reader_no_entry_matches_reads_text_as_the_complete_mask():
    rule = resolve(LISTED, ["stranger"], "varchar(30)")
    assert rule == MaskingRule(mask_type=MaskType.constant, value="****")


@pytest.mark.parametrize("data_type", ["integer", "timestamp", "boolean", "decimal(10,2)", "jsonb"])
def test_a_reader_no_entry_matches_reads_any_other_type_as_null(data_type):
    assert resolve(LISTED, ["stranger"], data_type) == MaskingRule(
        mask_type=MaskType.constant, value=None
    )


def test_only_an_entry_gives_unmasked():
    assert resolve(LISTED, ["vip"], "varchar") is None
    assert resolve([*LISTED, {"to": None, "mask": None}], ["stranger"], "varchar") is None


def test_a_column_with_no_masks_is_unchanged():
    assert resolve([], ["stranger"], "varchar") is None


def _reads(role: str, masks: list[dict]) -> GovernanceContext:
    gov = GovernanceContext(role_id=role)
    gov.table_map = {"sales.people": 1, "people": 1}
    gov.all_columns = {1: [("id", "integer"), ("email", "varchar"), ("age", "integer")]}
    gov.visible_columns = {1: None}
    columns = [(1, c, masks, t) for c, t in (("email", "varchar"), ("age", "integer"))]
    gov.masking_rules = reader_masks(columns, role, [role]).get((1, role), {})
    gov.masking_rules = {(1, c): v for c, v in gov.masking_rules.items()}
    return gov


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA sales")
    c.execute("CREATE TABLE sales.people (id INTEGER, email VARCHAR, age INTEGER)")
    c.execute("INSERT INTO sales.people VALUES (1, 'real@x', 41)")
    return c


def _run(con, sql: str, gov: GovernanceContext) -> list[tuple]:
    governed = apply_governance(sql, gov)
    return con.execute(sqlglot.transpile(governed, read="postgres", write="duckdb")[0]).fetchall()


# The statement a GraphQL read compiles to (compiler.sql_gen) and one a SQL client writes.
GRAPHQL = 'SELECT "t0"."id", "t0"."email", "t0"."age" FROM "sales"."people" "t0"'
SQL = "SELECT id, email, age FROM sales.people"


@pytest.mark.parametrize("statement", [GRAPHQL, SQL])
def test_an_unmatched_reader_reads_the_complete_mask(con, statement):
    assert _run(con, statement, _reads("stranger", LISTED)) == [(1, "****", None)]


@pytest.mark.parametrize("statement", [GRAPHQL, SQL])
def test_an_entry_for_everyone_that_says_unmasked_reads_unmasked(con, statement):
    masks = [*LISTED, {"to": None, "mask": None}]
    assert _run(con, statement, _reads("stranger", masks)) == [(1, "real@x", 41)]


@pytest.mark.parametrize("statement", [GRAPHQL, SQL])
def test_a_listed_reader_keeps_its_entry(con, statement):
    assert _run(con, statement, _reads("vip", LISTED)) == [(1, "real@x", 41)]


def test_a_member_of_a_meta_role_no_entry_matches_reads_the_complete_mask():
    from types import SimpleNamespace

    from provisa.compiler.rls import PLAIN
    from provisa.security import meta_role

    table = {
        "id": 1,
        "domain_id": None,
        "columns": [{"column_name": "email", "data_type": "varchar", "visible_to": ["vip", "x"]}],
    }
    state = SimpleNamespace(
        role_build_inputs={"role_chains": {"vip": ["vip"], "x": ["x"]}},
        masked_columns=[(1, "email", LISTED, "varchar")],
        masking_rules={},
        surfaces={
            r: SimpleNamespace(
                context=SimpleNamespace(tables={"t": SimpleNamespace(table_id=1)}),
                rls=SimpleNamespace(rules={}, domain_rules={}),
            )
            for r in ("vip", "x")
        },
    )
    reads = meta_role._member_reads(state, [{"id": "vip"}, {"id": "x"}], [table])
    vip, stranger = reads[1].columns["email"]
    assert vip[0] == PLAIN
    assert stranger[1] == MaskingRule(mask_type=MaskType.constant, value="****")
    assert PLAIN < stranger[0] < 1 << 30


def test_the_metadata_export_tags_an_unmatched_reader_as_restricted():
    from provisa.api.metadata_export.governance import _mask_tags

    table = Table(
        source_id="pg",
        domain_id="sales",
        schema="public",
        table="orders",
        columns=[Column(name="email", visible_to=["vip"], masks=[{"to": ["vip"], "mask": None}])],
    )
    chains = {"vip": ["vip"], "stranger": ["stranger"]}
    [tag] = _mask_tags(table, table.columns[0], ("stranger", "vip"), chains, "acme")
    assert tag.restricted_roles == ("stranger",)
    assert tag.exempt_roles == ("vip",)
    assert tag.rule_id.endswith(":1:constant")


def test_the_profiler_counts_an_unmatched_reader_as_restricted():
    from provisa.profiler.governance import ColumnRule

    fields = {
        name: None for name in ColumnRule.__dataclass_fields__ if name not in ("masks", "pii")
    }
    fields.update(relationship=None, served_to=frozenset())
    rule = ColumnRule(**fields, masks=tuple(LISTED), pii=False)
    assert rule.restricted_for(frozenset({"stranger"})) is True
    assert rule.restricted_for(frozenset({"vip"})) is False
