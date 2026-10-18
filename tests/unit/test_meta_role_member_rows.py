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
_ROWS = [("EU", "a@eu.example", "111"), ("US", "b@us.example", "222")]


@pytest.fixture(autouse=True)
def _no_surface_build(monkeypatch):
    def _register(state, role, rls):
        state.rls_contexts[role["id"]] = rls

    monkeypatch.setattr("provisa.api.app_loaders.register_role_surface", _register)


def _role(rid: str) -> dict:
    return {
        "id": rid,
        "capabilities": ["write"],
        "domain_access": ["sales"],
        "max_rows": None,
        "rate_limit": None,
        "session_vars": {},
    }


def _state() -> RoleState:
    columns = [
        {"column_name": "region", "visible_to": ["eu", "us"]},
        {"column_name": "email", "visible_to": ["eu", "us"]},
        {"column_name": "phone", "visible_to": ["eu"]},
    ]
    email_masks = [
        {"to": ["eu"], "mask": None},
        {"to": ["us"], "mask": {"type": "constant", "value": "***"}},
    ]
    from provisa.security.masking import reader_masks

    masked = [(_TABLE, "email", email_masks, "varchar")]
    rules: dict = {}
    for r in ("eu", "us"):
        rules.update(reader_masks(masked, r, [r]))
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
        }
    ]
    return RoleState(
        roles={"eu": _role("eu"), "us": _role("us")},
        rls_contexts={
            "eu": RLSContext(rules={_TABLE: "region = 'EU'"}),
            "us": RLSContext(rules={_TABLE: "region = 'US'"}),
        },
        contexts={
            r: SimpleNamespace(tables={"t": SimpleNamespace(table_id=_TABLE)}) for r in ("eu", "us")
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


def _gov(state: RoleState, role_id: str) -> GovernanceContext:
    """The governance a request acting as ``role_id`` reads with, built as the pipeline builds it."""
    return build_governance_context(
        role_id,
        state.rls_contexts[role_id],
        state.masking_rules,
        CompilationContext(),
        state.role_build_inputs["tables"],
        state.roles[role_id],
    )


def _table() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.t (region VARCHAR, email VARCHAR, phone VARCHAR)")
    con.executemany("INSERT INTO public.t VALUES (?, ?, ?)", _ROWS)
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
