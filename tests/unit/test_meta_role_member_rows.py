# Copyright (c) 2026 Kenneth Stott
# Canary: 95a2a824-abf7-473f-8717-80a3edcac438
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A set of held roles reads each row as the member that admitted it (REQ-1620).

Two members each read the rows of their own region: ``eu`` reads email unmasked and phone, ``us``
reads email masked and no phone. Acting as both, a row is read as the member(s) whose filter
admitted it -- never with one member's mask or grant over the other member's rows.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.stage2 import (
    GovernanceContext,
    apply_governance,
    build_governance_context,
)
from provisa.security.meta_role import ensure_meta_role
from tests.role_state import RoleState

_TABLE = 1
_ORDERS = 2
_ROWS = [(1, "EU", "a@eu.example", "111"), (2, "US", "b@us.example", "222")]
_REAL_EMAILS = {"a@eu.example", "b@us.example"}


@pytest.fixture(autouse=True)
def _no_surface_build(monkeypatch):
    def _register(state, role, rls):
        state.rls_contexts[role["id"]] = rls

    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", _register)


def _role(rid: str) -> dict:
    return {
        "description": "Test role.",
        "id": rid,
        "capabilities": ["write"],
        "domain_access": ["sales"],
        "max_rows": None,
        "rate_limit": None,
        "session_vars": {},
    }


def _state(*, fake_email: bool = False) -> RoleState:
    """``fake_email``: a Test environment in which email declares a fake, read by every role in
    place of its masks (REQ-1494)."""
    columns = [
        {"column_name": "id", "visible_to": ["eu", "us"], "data_type": "integer"},
        {"column_name": "region", "visible_to": ["eu", "us"]},
        {"column_name": "email", "visible_to": ["eu", "us"]},
        {"column_name": "phone", "visible_to": ["eu"]},
    ]
    email_masks = [
        {"to": ["eu"], "mask": None},
        {"to": ["us"], "mask": {"type": "constant", "value": "***"}},
    ]
    from provisa.security.masking import reader_masks

    from provisa.security.masking import MaskingRule, MaskType

    masked = [] if fake_email else [(_TABLE, "email", email_masks, "varchar")]
    rules: dict = {}
    for r in ("eu", "us"):
        rules.update(reader_masks(masked, r, [r]))
        if fake_email:
            fake = MaskingRule(MaskType.fake, fake="email()")
            rules.setdefault((_TABLE, r), {})["email"] = (fake, "varchar")
    tables = [
        {
            "id": _TABLE,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "t",
            "columns": columns,
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
            # Each member updates its own rows (its read filter, as it states none): eu its
            # email, us its phone.
            "write_grants": [
                {
                    "role": "eu",
                    "operation": "update",
                    "columns": ["email"],
                    "rows": None,
                    "check": None,
                    "presets": [],
                },
                {
                    "role": "us",
                    "operation": "update",
                    "columns": ["phone"],
                    "rows": None,
                    "check": None,
                    "presets": [],
                },
            ],
        },
        {
            "id": _ORDERS,
            "source_id": "pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "o",
            "columns": [
                {"column_name": "cust_email", "visible_to": ["eu", "us"]},
                {"column_name": "amount", "visible_to": ["eu", "us"], "data_type": "integer"},
            ],
        },
    ]
    return RoleState(
        roles={"eu": _role("eu"), "us": _role("us")},
        rls_contexts={
            "eu": RLSContext(rules={_TABLE: "region = 'EU'"}),
            "us": RLSContext(rules={_TABLE: "region = 'US'"}),
        },
        contexts={
            r: SimpleNamespace(
                tables={
                    "t": SimpleNamespace(table_id=_TABLE),
                    "o": SimpleNamespace(table_id=_ORDERS),
                }
            )
            for r in ("eu", "us")
        },
        masking_rules=rules,
        masked_columns=masked,
        tracked_functions={},
        tracked_webhooks={},
        meta_roles={},
        meta_resolved_at={},
        role_build_inputs={
            "role_chains": {"eu": ["eu"], "us": ["us"]},
            "tables": tables,
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
    )


def _gov(
    state: RoleState, role_id: str, ctx: CompilationContext | None = None
) -> GovernanceContext:
    """The governance a request acting as ``role_id`` reads with, built as the pipeline builds it."""
    return build_governance_context(
        role_id,
        state.rls_contexts[role_id],
        state.masking_rules,
        ctx or CompilationContext(),
        state.role_build_inputs["tables"],
        state.roles[role_id],
    )


def _table() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.t (id INTEGER, region VARCHAR, email VARCHAR, phone VARCHAR)")
    con.executemany("INSERT INTO public.t VALUES (?, ?, ?, ?)", _ROWS)
    con.execute("CREATE TABLE public.o (cust_email VARCHAR, amount INTEGER)")
    con.execute("INSERT INTO public.o VALUES ('a@eu.example', 10), ('b@us.example', 20)")
    return con


def _read(sql: str) -> list[tuple]:
    return sorted(_table().execute(sql).fetchall())


def test_each_member_alone_reads_its_own_rows_as_granted():
    state = _state()
    assert _read(
        apply_governance("SELECT region, email, phone FROM public.t", _gov(state, "eu"))
    ) == [("EU", "a@eu.example", "111")]
    assert _read(apply_governance("SELECT region, email FROM public.t", _gov(state, "us"))) == [
        ("US", "***")
    ]


def test_a_row_one_member_admits_is_masked_as_that_member_reads_it():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    governed = apply_governance("SELECT region, email FROM public.t", _gov(state, meta))
    # The US row is admitted by `us` alone, which reads email masked.
    assert _read(governed) == [("EU", "a@eu.example"), ("US", "***")]


def test_a_column_one_member_cannot_see_is_hidden_on_the_rows_it_alone_admits():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    governed = apply_governance("SELECT region, phone FROM public.t", _gov(state, meta))
    # phone is granted to `eu` only: the US row, admitted by `us` alone, reads it as NULL.
    assert _read(governed) == [("EU", "111"), ("US", None)]


def test_an_aggregate_reads_each_row_masked_as_its_member_reads_it():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    governed = apply_governance("SELECT max(email) AS top FROM public.t", _gov(state, meta))
    # The US email is '***' before the aggregate reads it; 'a@eu.example' sorts after it.
    assert _read(governed) == [("a@eu.example",)]


def test_a_column_every_member_reads_alike_is_read_plainly():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    governed = apply_governance("SELECT region FROM public.t", _gov(state, meta))
    assert "CASE" not in governed.split("FROM", 1)[0]  # region is read as it is, no branch
    assert _read(governed) == [("EU",), ("US",)]


def test_a_single_role_is_governed_as_before():
    state = _state()
    for role in ("eu", "us"):
        governed = apply_governance("SELECT region, email FROM public.t", _gov(state, role))
        assert "CASE" not in governed and "__m_" not in governed


def test_a_member_writes_a_column_only_on_the_rows_it_admits():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    gov = _gov(state, meta)
    con = _table()
    # email is granted by eu alone, so only the EU row's email is written.
    con.execute(apply_governance("UPDATE public.t SET email = 'x'", gov))
    # phone is granted by us alone, so only the US row's phone is written.
    con.execute(apply_governance("UPDATE public.t SET phone = '0'", gov))
    assert sorted(con.execute("SELECT region, email, phone FROM public.t").fetchall()) == [
        ("EU", "x", "111"),
        ("US", "b@us.example", "0"),
    ]


def test_an_approved_join_matches_real_keys_while_the_key_is_served_as_each_member_reads_it():
    state = _state()
    meta = ensure_meta_role(state, ["eu", "us"])
    gov = _gov(state, meta)
    # A registered relationship approves t.email = o.cust_email (REQ-603).
    gov.join_keys = {
        (_TABLE, _ORDERS, "email", "cust_email"),
        (_ORDERS, _TABLE, "cust_email", "email"),
    }
    governed = apply_governance(
        "SELECT t.region, t.email, o.amount FROM public.t AS t "
        "JOIN public.o AS o ON t.email = o.cust_email",
        gov,
    )
    # The US row joins on its real email, and serves it masked as `us` reads it.
    assert _read(governed) == [("EU", "a@eu.example", 10), ("US", "***", 20)]


@pytest.fixture
def _fakes(monkeypatch):
    from provisa.fakes import digest as digest_mod

    monkeypatch.setattr(digest_mod, "_key", b"k" * 32)


def _faked_read(sql: str, gov: GovernanceContext, con: duckdb.DuckDBPyConnection) -> list[tuple]:
    import sqlglot

    from provisa.fakes.duckdb_functions import register

    register(con)
    governed = apply_governance(sql, gov)
    return sorted(
        con.execute(sqlglot.transpile(governed, read="postgres", write="duckdb")[0]).fetchall()
    )


def test_a_faked_table_is_read_through_its_fakes_and_then_as_each_member_reads_it(_fakes):
    # email is faked for every role; phone is served to `eu` alone. Acting as both, the fake is
    # computed first and each row's phone follows the member that admitted it.
    state = _state(fake_email=True)
    meta = ensure_meta_role(state, ["eu", "us"])
    rows = _faked_read("SELECT id, email, phone FROM public.t", _gov(state, meta), _table())
    assert [(r[0], r[2]) for r in rows] == [(1, "111"), (2, None)]
    assert not {r[1] for r in rows} & _REAL_EMAILS  # never the real email


def test_a_faked_and_kept_table_reads_a_written_row_as_the_member_that_admits_it_now(_fakes):
    # REQ-1942: a Reversible environment keeps its writes in a change log. Row 2 was written
    # into the EU region with a new phone: `eu` admits it now, so its phone is served; a written
    # row is never faked again, so its email is the value written.
    state = _state(fake_email=True)
    meta = ensure_meta_role(state, ["eu", "us"])
    con = _table()
    con.execute(
        'CREATE TABLE public.log ("__seq" BIGINT, "__op" VARCHAR, id INTEGER, region VARCHAR, '
        "email VARCHAR, phone VARCHAR)"
    )
    con.execute("INSERT INTO public.log VALUES (1, 'upsert', 2, 'EU', 'w@eu.example', '999')")
    ctx = CompilationContext()
    ctx.kept = {_TABLE: ("public.log", ["id"])}
    rows = _faked_read("SELECT id, region, email, phone FROM public.t", _gov(state, meta, ctx), con)
    assert rows[1] == (2, "EU", "w@eu.example", "999")
    assert rows[0][0] == 1 and rows[0][3] == "111" and rows[0][2] not in _REAL_EMAILS


def test_a_faked_and_kept_table_hides_a_written_row_from_the_member_that_alone_admits_it(_fakes):
    state = _state(fake_email=True)
    meta = ensure_meta_role(state, ["eu", "us"])
    con = _table()
    con.execute(
        'CREATE TABLE public.log ("__seq" BIGINT, "__op" VARCHAR, id INTEGER, region VARCHAR, '
        "email VARCHAR, phone VARCHAR)"
    )
    # Row 1 written into the US region: `us` alone admits it now, and is not served phone.
    con.execute("INSERT INTO public.log VALUES (1, 'upsert', 1, 'US', 'w@us.example', '999')")
    ctx = CompilationContext()
    ctx.kept = {_TABLE: ("public.log", ["id"])}
    rows = _faked_read("SELECT id, region, phone FROM public.t", _gov(state, meta, ctx), con)
    assert rows == [(1, "US", None), (2, "US", None)]
