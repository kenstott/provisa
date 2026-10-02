# Copyright (c) 2026 Kenneth Stott
# Canary: 3b6d8e14-7f29-4a50-9c83-d1e5a2f70b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Owner contact details and tag assignments follow the caller's rights."""

# Requirements: REQ-609, REQ-1377, REQ-1530, REQ-1634

from __future__ import annotations

import types
from unittest.mock import AsyncMock, patch

import provisa.api.app as appmod
from provisa.api.admin.schema_query import Query
from tests.unit.gate_identity import grant


class _Rows:
    def __init__(self, rows):
        self._rows = [types.SimpleNamespace(**r) for r in rows]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Answers each query from the table it selects from; the assignment list comes from the repo."""

    def __init__(self, tables):
        self._tables = tables

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def execute_core(self, stmt):
        name = stmt.get_final_froms()[0].name if hasattr(stmt, "get_final_froms") else ""
        for key, rows in self._tables.items():
            if key in str(stmt):
                return _Rows(rows)
        raise AssertionError(f"unexpected query on {name}: {stmt}")


class _Pool:
    def __init__(self, tables):
        self._tables = tables

    def acquire(self):
        return _Conn(self._tables)


# --- resolveOwners ---------------------------------------------------------------------------


_OWNER_TABLES = {
    "FROM roles": [{"id": "steward"}],
    "user_role_assignments": [
        {"user_id": "u1", "display_name": "Ada Lovelace", "email": "ada@example.com"}
    ],
}


async def _owners(info, refs):
    # The role lookup selects from roles; the member lookup selects from user_role_assignments.
    class Conn(_Conn):
        async def execute_core(self, stmt):
            text = str(stmt)
            if "user_role_assignments" in text:
                return _Rows(_OWNER_TABLES["user_role_assignments"])
            if "FROM roles" in text:
                return _Rows([{"id": refs[0]}] if refs[0] == "steward" else [])
            return _Rows([])

    class Pool:
        def acquire(self):
            return Conn({})

    with patch("provisa.api.admin.schema_query._get_pool", return_value=Pool()):
        return await Query().resolve_owners(info, refs)


def _bound(request):
    request.state.active_org_id = "acme"


async def test_owner_contact_details_go_to_a_user_management_holder(monkeypatch):
    info, request = grant(monkeypatch, "user_management")
    _bound(request)
    (owner,) = await _owners(info, ["steward"])
    assert (owner.user_id, owner.display_name, owner.email) == (
        "u1",
        "Ada Lovelace",
        "ada@example.com",
    )


async def test_everyone_else_gets_the_display_name_only(monkeypatch):
    info, request = grant(monkeypatch, "query_development")
    _bound(request)
    (owner,) = await _owners(info, ["steward"])
    assert (owner.user_id, owner.display_name, owner.email) == (None, "Ada Lovelace", None)
    # An unmatched ref is the caller's own input: shown as the name, not as an id.
    (echo,) = await _owners(info, ["someone"])
    assert (echo.user_id, echo.display_name, echo.email) == (None, "someone", None)


# --- tagAssignments --------------------------------------------------------------------------

_ASSIGNMENTS = [
    {
        "tag_id": "pii",
        "object_type": "table",
        "table_id": 1,
        "source_id": None,
        "column_name": None,
        "relationship_id": None,
        "command_name": None,
        "table_ref": "s.p.a",
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "pii",
        "object_type": "column",
        "table_id": 2,
        "source_id": None,
        "column_name": "c",
        "relationship_id": None,
        "command_name": None,
        "table_ref": "s.p.b",
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "relationship",
        "table_id": None,
        "source_id": None,
        "column_name": None,
        "relationship_id": "r1",
        "command_name": None,
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "relationship",
        "table_id": None,
        "source_id": None,
        "column_name": None,
        "relationship_id": "r2",
        "command_name": None,
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "command",
        "table_id": None,
        "source_id": None,
        "column_name": None,
        "relationship_id": None,
        "command_name": "fn_sales",
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "command",
        "table_id": None,
        "source_id": None,
        "column_name": None,
        "relationship_id": None,
        "command_name": "hook_hr",
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "command",
        "table_id": None,
        "source_id": None,
        "column_name": None,
        "relationship_id": None,
        "command_name": "fn_nodomain",
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "source",
        "table_id": None,
        "source_id": "s_sales",
        "column_name": None,
        "relationship_id": None,
        "command_name": None,
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "source",
        "table_id": None,
        "source_id": "s_hr",
        "column_name": None,
        "relationship_id": None,
        "command_name": None,
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
    {
        "tag_id": "t",
        "object_type": "source",
        "table_id": None,
        "source_id": "s_open",
        "column_name": None,
        "relationship_id": None,
        "command_name": None,
        "table_ref": None,
        "reason": None,
        "expires_on": None,
    },
]


class _TagConn(_Conn):
    async def execute_core(self, stmt):
        text = str(stmt)
        if "FROM registered_tables" in text:
            return _Rows([{"id": 1, "domain_id": "sales"}, {"id": 2, "domain_id": "hr"}])
        if "FROM relationships" in text:
            return _Rows([{"id": "r1", "source_table_id": 1}, {"id": "r2", "source_table_id": 2}])
        if "FROM sources" in text:
            return _Rows(
                [
                    {"id": "s_sales", "allowed_domains": ["sales", "x"]},
                    {"id": "s_hr", "allowed_domains": ["hr"]},
                    {"id": "s_open", "allowed_domains": []},
                ]
            )
        if "FROM tracked_functions" in text:
            return _Rows(
                [
                    {"name": "fn_sales", "domain_id": "sales"},
                    {"name": "fn_nodomain", "domain_id": ""},
                ]
            )
        if "FROM tracked_webhooks" in text:
            return _Rows([{"name": "hook_hr", "domain_id": "hr"}])
        raise AssertionError(text)


class _TagPool:
    def acquire(self):
        return _TagConn({})


async def _assignments(monkeypatch, domain_access):
    info, request = grant(monkeypatch, "query_development")
    request.state.active_org_id = "acme"
    monkeypatch.setattr(
        appmod.state,
        "roles",
        {"test_role": {"capabilities": ["query_development"], "domain_access": domain_access}},
        raising=False,
    )
    with (
        patch("provisa.api.admin.schema_query._get_pool", return_value=_TagPool()),
        patch(
            "provisa.core.repositories.tag.list_assignments",
            new=AsyncMock(return_value=[dict(r) for r in _ASSIGNMENTS]),
        ),
        patch("provisa.core.domain_policy.single_domain", return_value=False),
    ):
        return await Query().tag_assignments(info)


def _keys(seen):
    return [a.table_id or a.relationship_id or a.command_name or a.source_id for a in seen]


async def test_an_unrestricted_caller_sees_every_assignment(monkeypatch):
    seen = await _assignments(monkeypatch, ["*"])
    assert len(seen) == len(_ASSIGNMENTS)


async def test_a_domain_scoped_caller_sees_only_objects_in_their_domains(monkeypatch):
    seen = await _assignments(monkeypatch, ["sales"])
    # sales table and its relationship, the sales command, the command with no domain, the source
    # allowed in sales and the source with no allowed_domains; not hr's table/column/webhook/source.
    assert _keys(seen) == [1, "r1", "fn_sales", "fn_nodomain", "s_sales", "s_open"]
