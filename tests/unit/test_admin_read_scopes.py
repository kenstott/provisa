# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3c9e51-2b84-4d06-a5f7-0e1d8c6b3924
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connection details, role definitions and server detail go to the holders of their right."""

# Requirements: REQ-1337, REQ-1349

from __future__ import annotations

import types
from unittest.mock import patch

from provisa.api.admin import roles_router
from provisa.api.admin.schema_query import Query
from provisa.api.mcp import status as mcp_status_module
from tests.unit.gate_identity import grant


class _Rows:
    def __init__(self, rows):
        self._rows = [types.SimpleNamespace(_mapping=r, **r) for r in rows]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def execute_core(self, _stmt):
        return _Rows(self._rows)


class _Pool:
    def __init__(self, rows):
        self._rows = rows

    def acquire(self):
        return _Conn(self._rows)


_SOURCE = {
    "id": "pg",
    "type": "postgresql",
    "host": "db.internal",
    "port": 5432,
    "database": "sales",
    "username": "svc",
    "dialect": "postgresql",
    "replicate": None,
    "path": "/data",
    "mapping": {"k": "v"},
    "federation_hints": {"warehouse": "w"},
    "password_ref": "${secret:PG}",
    "cdc": {"bootstrap_servers": "k:9092", "topic_prefix": "t"},
    "description": "orders",
}


async def _read_sources(info):
    with patch("provisa.api.admin.schema_query._get_pool", return_value=_Pool([_SOURCE])):
        return (await Query().sources(info))[0], await Query().source(info, "pg")


async def test_connection_details_go_to_a_source_registration_holder(monkeypatch):
    info, _ = grant(monkeypatch, "source_registration")
    listed, single = await _read_sources(info)
    for src in (listed, single):
        assert (src.host, src.port, src.database, src.username) == (
            "db.internal",
            5432,
            "sales",
            "svc",
        )
        assert src.path == "/data" and src.password_ref == "${secret:PG}"
        assert src.mapping_json and src.federation_hints_json and src.cdc is not None


async def test_everyone_else_reads_the_source_without_them(monkeypatch):
    info, _ = grant(monkeypatch, "query_development")
    listed, single = await _read_sources(info)
    for src in (listed, single):
        assert (src.id, src.type, src.dialect, src.description) == (
            "pg",
            "postgresql",
            "postgresql",
            "orders",
        )
        assert src.host is None and src.port is None
        assert src.database is None and src.username is None
        assert src.path is None and src.mapping_json is None
        assert src.federation_hints_json is None and src.password_ref is None
        assert src.cdc is None


_ROLE_ROWS = [
    {
        "id": "analyst",
        "capabilities": ["usage"],
        "demonstrated": [],
        "domain_access": ["sales"],
        "org_id": None,
        "parent_role_id": None,
    },
    {
        "id": "org_admin",
        "capabilities": ["user_management"],
        "demonstrated": [],
        "domain_access": ["*"],
        "org_id": None,
        "parent_role_id": None,
    },
]


def _roles_info(monkeypatch, *capabilities, held=()):
    info, request = grant(monkeypatch, *capabilities)
    request.state.identity.roles = list(held) + ["test_role"]
    request.state.active_org_id = "acme"
    return info, request


async def test_graphql_roles_full_for_user_management_else_own_role_only(monkeypatch):
    info, _ = _roles_info(monkeypatch, "user_management")
    with patch("provisa.api.admin.schema_query._get_pool", return_value=_Pool(_ROLE_ROWS)):
        seen = await Query().roles(info)
    assert [(r.id, r.capabilities) for r in seen] == [
        ("analyst", ["usage"]),
        ("org_admin", ["user_management"]),
    ]

    info, _ = _roles_info(monkeypatch, "query_development", held=("analyst",))
    with patch("provisa.api.admin.schema_query._get_pool", return_value=_Pool(_ROLE_ROWS)):
        seen = {r.id: r for r in await Query().roles(info)}
    assert seen["analyst"].capabilities == ["usage"]  # the caller's own role: the client needs it
    assert seen["org_admin"].capabilities is None
    assert seen["org_admin"].domain_access is None
    assert seen["org_admin"].parent_role_id is None


async def test_rest_roles_full_for_user_management_else_own_role_only(monkeypatch):
    _, request = _roles_info(monkeypatch, "user_management")
    with patch.object(roles_router, "_pool", return_value=_Pool(_ROLE_ROWS)):
        full = await roles_router.list_roles(request)
    assert full[1]["capabilities"] == ["user_management"]

    _, request = _roles_info(monkeypatch, "query_development", held=("analyst",))
    with patch.object(roles_router, "_pool", return_value=_Pool(_ROLE_ROWS)):
        seen = {r["id"]: r for r in await roles_router.list_roles(request)}
    assert seen["analyst"]["capabilities"] == ["usage"]
    assert seen["org_admin"] == {"id": "org_admin"}


_STATUS = {
    "enabled": True,
    "port": 8808,
    "transport": "streamable-http",
    "url": "http://h:8808/mcp",
    "tls": False,
    "bridge_command": None,
    "bridge_args": ["-m"],
    "stdio_role": "analyst",
    "max_rows": 10,
    "tools": [{"name": "t"}],
    "enable_env_var": "X",
    "role_env_var": "Y",
}


async def test_mcp_server_detail_is_the_observers(monkeypatch):
    _, request = grant(monkeypatch, "observability")
    with patch.object(mcp_status_module, "mcp_status", return_value=dict(_STATUS)):
        assert await mcp_status_module.get_mcp_server(request) == _STATUS
        _, request = grant(monkeypatch, "query_development")
        trimmed = await mcp_status_module.get_mcp_server(request)
    assert trimmed == {
        "enabled": True,
        "url": "http://h:8808/mcp",
        "tls": False,
        "bridge_command": None,
        "bridge_args": ["-m"],
    }


async def test_chat_status_reason_is_the_org_administrators(monkeypatch):
    from provisa.api.mcp import chat

    async def _unconfigured(_state):
        return False, "no anthropic key"

    monkeypatch.setattr(chat, "_llm_configured", _unconfigured)
    _, request = grant(monkeypatch, "org_settings")
    assert await mcp_status_module.mcp_chat_status(request) == {
        "configured": False,
        "reason": "no anthropic key",
    }
    _, request = grant(monkeypatch, "query_development")
    assert await mcp_status_module.mcp_chat_status(request) == {
        "configured": False,
        "reason": None,
    }
