# Copyright (c) 2026 Kenneth Stott
# Canary: 65cb4a98-1b2b-4c6a-9f42-7dfe5fd23ced
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fakes are checked and proposed only for what the caller reads (REQ-2152).

A steward's right opens the fakes surfaces; it does not widen what the steward reads. A table it
is served no column of is answered as unregistered, a stored column it is not served is not
checked, and a proposal names only the columns it is served.

The table: ``customers`` (id 7) with ``email`` (served) and ``ssn`` (not served).
"""

# Requirements: REQ-2152

from __future__ import annotations

import types
from typing import Any, cast

import pytest

import provisa.api.app as appmod
from provisa.api.admin import fakes_router
from provisa.api.errors import ApiError
from provisa.fakes.propose import ProfiledColumnFacts

pytestmark = pytest.mark.asyncio

# The served columns are stubbed per table, so the request carries nothing.
_REQUEST = cast(Any, None)

_TABLE = {
    "id": 7,
    "table_name": "customers",
    "columns": [
        {"column_name": "email", "data_type": "varchar"},
        {"column_name": "ssn", "data_type": "varchar"},
    ],
}


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


@pytest.fixture(autouse=True)
def _model(monkeypatch):
    monkeypatch.setattr(
        appmod.state, "model_db", types.SimpleNamespace(acquire=lambda: _Conn()), raising=False
    )

    async def _tables(conn):
        return [dict(_TABLE, columns=list(_TABLE["columns"]))]

    async def _relationships(conn):
        return []

    async def _served(conn, request, table_id):
        return {"email"} if table_id == 7 else set()

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _tables)
    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_relationships", _relationships)
    monkeypatch.setattr("provisa.api.admin.read_rights.served_columns_request", _served)
    monkeypatch.setattr(fakes_router, "require_capability_request", lambda request, cap: None)
    monkeypatch.setattr(
        "provisa.api.admin._hiding_guard.require_hiding_editor_request", lambda request: None
    )
    monkeypatch.setattr("provisa.api.admin._fake_guard.check_model", lambda t, r: None)


def _check(column: str, table_id: int = 7) -> fakes_router.ColumnFakeIn:
    return fakes_router.ColumnFakeIn(
        tableId=table_id, tableName="customers", column=column, dataType="varchar", fake="email"
    )


async def test_a_served_column_is_checked():
    assert await fakes_router.check_column_fake(_REQUEST, _check("email")) == {"ok": True}


async def test_a_stored_column_the_caller_is_not_served_is_not_checked():
    with pytest.raises(ApiError) as refused:
        await fakes_router.check_column_fake(_REQUEST, _check("ssn"))
    assert refused.value.code == "fakes.table_not_found" and "ssn" not in str(refused.value)


async def test_a_table_the_caller_is_served_nothing_of_is_answered_as_unregistered():
    with pytest.raises(ApiError) as refused:
        await fakes_router.check_column_fake(_REQUEST, _check("email", table_id=8))
    assert refused.value.code == "fakes.table_not_found"


async def test_a_proposal_names_only_the_columns_the_caller_is_served(monkeypatch):
    facts = [
        ProfiledColumnFacts("email", "text", 40, "varchar", 1.0, ()),
        ProfiledColumnFacts("ssn", "text", 11, "varchar", 1.0, ()),
    ]

    async def _latest(conn, *, org_id, env, reg):
        return "run-1", facts

    async def _tags(conn, table_id):
        return {"ssn": {"pii"}}

    monkeypatch.setattr("provisa.fakes.propose.latest_facts", _latest)
    monkeypatch.setattr("provisa.profiler.run.column_tags", _tags)
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")
    monkeypatch.setattr(
        "provisa.api.admin.profiler_checks_router._physical_names",
        lambda table_id: {"email": "email", "ssn": "ssn"},
    )
    out = await fakes_router.propose_fakes(_REQUEST, fakes_router.ProposeIn(tableId=7))
    # ssn is tagged pii and matched by no fake: unfiltered it would be named as unmatched.
    assert "ssn" not in out["columns"] and out["unmatchedPii"] == []


async def test_an_adequacy_run_counts_only_the_checks_on_columns_the_caller_is_served(
    monkeypatch,
):
    async def _rows(state, org_id, env):
        return [
            {"table_id": 7, "column_name": "email", "status": "passed"},
            {"table_id": 7, "column_name": "ssn", "status": "failed"},
            {"table_id": 8, "column_name": "salary", "status": "failed"},
        ]

    monkeypatch.setattr("provisa.fakes.adequacy.run_checks", _rows)
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "acme")
    out = await fakes_router.check_adequacy(_REQUEST)
    assert out == {"passed": 1, "failed": 0, "not_run": 0}
