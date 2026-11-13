# Copyright (c) 2026 Kenneth Stott
# Canary: 5befabb7-56b9-4a25-b938-5bfb07cbe185
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each column's state for the caller's role: plain, masked, faked or hidden.

A caller (Polly, an MCP client) asks before running a condition on a column whether it can be
evaluated. The answer is read from the role's governance context -- the masks and served columns
the query guard reads -- so it agrees with what a query would do: hidden is a column V003 refuses,
masked one V005 refuses in WHERE, faked one whose predicate reads fake values, plain the rest.
"""

from __future__ import annotations

import pytest

from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import column_state, tools
from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput
from provisa.compiler.sql_validator import validate_sql
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.security.masking import MaskingRule, MaskType, reader_masks
from provisa.security.meta_role import meta_chain
from tests.role_state import RoleState

pytestmark = pytest.mark.unit

_TID = 1
_COLUMNS = ["id", "region", "email", "phone", "salary"]
_STARS = MaskingRule(mask_type=MaskType.constant, value="***")
_FAKE = MaskingRule(mask_type=MaskType.fake, fake="phone_number()")


@pytest.fixture(autouse=True)
def _platform_key(monkeypatch):
    """A faked column's governance binds the platform's fake key, loaded at startup."""
    from provisa.fakes import digest

    monkeypatch.setattr(digest, "_key", b"k" * 32)


def _role(rid: str) -> dict:
    return {"id": rid, "capabilities": ["query_development"], "domain_access": ["shop"]}


def _tables(role_id: str, salary_to: list[str]) -> list[dict]:
    """customers: salary is granted only to ``salary_to``; every other column to ``role_id``."""
    return [
        {
            "id": _TID,
            "source_id": "pg",
            "domain_id": "shop",
            "schema_name": "public",
            "table_name": "customers",
            "columns": [
                {"column_name": c, "visible_to": salary_to if c == "salary" else [role_id]}
                for c in _COLUMNS
            ],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        }
    ]


def _model(role_id: str, masks: dict, salary_to: list[str]):
    """The role's compiled context and governance context, as the query guard builds them."""
    tables = _tables(role_id, salary_to)
    types = {
        _TID: [
            ColumnMetadata(
                column_name=c,
                data_type="integer" if c == "id" else "varchar",
                is_nullable=True,
            )
            for c in _COLUMNS
        ]
    }
    role = _role(role_id)
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=types,
        naming_rules=[],
        role=role,
        domains=[{"id": "shop", "description": "Shop"}],
    )
    ctx = build_context(si)
    gov = build_governance_context(role_id, RLSContext(rules={}), masks, ctx, tables, role, [])
    return ctx, gov, role, tables


def _analyst():
    """analyst: email masked with a constant, phone faked, salary not granted."""
    masks = {(_TID, "analyst"): {"email": (_STARS, "varchar"), "phone": (_FAKE, "varchar")}}
    return _model("analyst", masks, salary_to=["hr"])


def _states(gov) -> dict[str, str]:
    return {c: column_state.column_state(gov, _TID, c) for c in _COLUMNS}


def test_each_state_is_read_from_the_roles_governance():
    gov = _analyst()[1]
    assert _states(gov) == {
        "id": "plain",
        "region": "plain",
        "email": "masked",
        "phone": "faked",
        "salary": "hidden",
    }


def test_a_table_the_role_has_no_governance_for_is_hidden():
    gov = _analyst()[1]
    # V003 fails closed on a table its governance holds nothing for; so does the answer.
    assert column_state.column_state(gov, 99, "id") == "hidden"


def test_a_meta_role_member_gets_the_state_of_its_resolved_rights():
    """email: plain for eu, masked for us. A request acting as both (a meta-role) reads it
    through the first mask entry its chain holds -- eu's, plain -- and is served salary, which
    only eu is granted."""
    email_masks = [
        {"to": ["eu"], "mask": None},
        {"to": ["us"], "mask": {"type": "constant", "value": "***"}},
    ]
    masked = [(_TID, "email", email_masks, "varchar")]
    meta = "meta:eu+us"
    chain = meta_chain(meta, ["eu", "us"], {"eu": ["eu"], "us": ["us"]})

    _ctx, us_gov, _r, _t = _model("us", reader_masks(masked, "us", ["us"]), salary_to=["eu"])
    assert column_state.column_state(us_gov, _TID, "email") == "masked"
    assert column_state.column_state(us_gov, _TID, "salary") == "hidden"

    # The meta-role's grants hold its members' (security.inheritance.expand_grants).
    _ctx, meta_gov, _r, _t = _model(meta, reader_masks(masked, meta, chain), salary_to=["eu", meta])
    assert column_state.column_state(meta_gov, _TID, "email") == "plain"
    assert column_state.column_state(meta_gov, _TID, "salary") == "plain"


def _guard(sql: str, ctx, gov, role, tables) -> tuple[set[str], str]:
    """What the query guard does with ``sql``: the V003/V005 codes it refuses it with -- the
    validator's, then governance's own (stage2 refuses an unserved column it is handed) -- and
    the governed statement when neither refuses it."""
    codes = {v.code for v in validate_sql(sql, ctx, gov, role, tables)} & {"V003", "V005"}
    if codes:
        return codes, ""
    try:
        return set(), apply_governance(sql, gov)
    except PermissionError as refused:
        return {c for c in ("V003", "V005") if f"[{c}]" in str(refused)}, ""


@pytest.mark.parametrize("column", _COLUMNS)
def test_the_answer_agrees_with_the_guard_on_a_condition_over_the_column(column):
    ctx, gov, role, tables = _analyst()
    sql = f'SELECT "id" FROM "shop"."customers" WHERE "{column}" = \'1\''
    codes, governed = _guard(sql, ctx, gov, role, tables)
    state = column_state.column_state(gov, _TID, column)
    expected = {"plain": set(), "faked": set(), "masked": {"V005"}, "hidden": {"V003"}}[state]
    assert codes == expected, (column, state, codes)
    # A faked column's condition runs, over its fake values; a plain one's over the real ones.
    assert (f"__fake__{column}" in governed) == (state == "faked")


# --- describe_table carries the state on its column rows ------------------------------------


@pytest.fixture
def described(monkeypatch):
    ctx, gov = _analyst()[:2]
    meta = next(iter(ctx.tables.values()))
    served = {c for (tid, c) in ctx.physical_to_sql if tid == _TID}
    # The catalog lists the columns the role is served (flight.catalog.role_visibility).
    catalog = [
        CatalogTable(
            domain_id="shop",
            table_name=meta.table_name,
            description="",
            columns=[
                CatalogColumn(ctx.physical_to_sql[(_TID, c)], "varchar", True, "")
                for c in _COLUMNS
                if c in served
            ],
        )
    ]

    async def _catalog(_state, _role_id=None):
        return list(catalog)

    monkeypatch.setattr(tools, "_build_catalog_tables_async", _catalog)
    monkeypatch.setattr(tools, "_role_governance", lambda _state, _role: gov)
    state = RoleState(
        contexts={"analyst": ctx}, roles={"analyst": _role("analyst")}, model_db=object()
    )
    return state, meta.table_name


async def test_describe_table_gives_each_listed_column_its_state(described):
    state, table = described
    out = await tools.describe_table(state, "analyst", "shop", table)
    assert {c["name"]: c["state"] for c in out["columns"]} == {
        "id": "plain",
        "region": "plain",
        "email": "masked",
        "phone": "faked",
    }


async def test_a_column_the_role_is_not_served_stays_unlisted(described):
    state, table = described
    out = await tools.describe_table(state, "analyst", "shop", table)
    assert "salary" not in {c["name"] for c in out["columns"]}


def test_the_role_governance_is_the_one_the_query_pipeline_reads(monkeypatch):
    seen = {}

    def _governance(state, surface, role_id):
        seen["args"] = (state, surface, role_id)
        return "gov"

    monkeypatch.setattr("provisa.pgwire.governed_plan.role_governance", _governance)
    surface = object()
    state = RoleState(surfaces={"analyst": surface})
    assert tools._role_governance(state, "analyst") == "gov"
    assert seen["args"] == (state, surface, "analyst")


def test_the_tool_description_says_what_each_state_means_for_a_condition():
    from provisa.api.mcp import chat

    spec = next(t for t in chat._TOOLS if t["name"] == "describe_table")
    text = spec["description"]
    for word in ("plain", "masked", "faked", "hidden", "cannot be evaluated"):
        assert word in text
    assert "REQ-" not in text


async def test_every_surface_listing_describe_table_says_the_same():
    from provisa.api.mcp import chat
    from provisa.api.mcp.column_state import DESCRIBE_TABLE
    from provisa.api.mcp.server import build_mcp_server
    from provisa.api.mcp.status import _TOOLS as STATUS_TOOLS

    listed = {t.name: t.description for t in await build_mcp_server(RoleState()).list_tools()}
    assert listed["describe_table"] == DESCRIBE_TABLE
    assert (
        next(t for t in chat._TOOLS if t["name"] == "describe_table")["description"]
        == DESCRIBE_TABLE
    )
    assert (
        next(t for t in STATUS_TOOLS if t["name"] == "describe_table")["description"]
        == DESCRIBE_TABLE
    )
