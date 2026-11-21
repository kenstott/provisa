# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e64c1-7a52-4f0e-9d18-5c2e7f40a913
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a demo sandbox's open asks of what lies on other machines (REQ-2006).

On the hosted tier the control plane is on another machine, and an open of a demo whose template
was built and whose data was landed took about 100 s there (10 s in CI):

- the template's store: a demo whose landed data is its listeners' alone (the streaming demo's
  orders) records no replica, and its copy still built the template's runtime to compute their
  definitions -- 37 of its 38 s, its template retired with its stores;
- its person's roles: each a checkout, a read of every role and a write, one after another."""

# Requirements: REQ-2006, REQ-2049

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api import demo_replicas

pytestmark = pytest.mark.unit


class _StateConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows


class _StateDb:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    @asynccontextmanager
    async def acquire(self):
        yield _StateConn(self.rows)


def _copy(monkeypatch, rows: list[dict]) -> tuple[list[tuple], list[str]]:
    """Copy template ``sbxtplstreaming``'s store into ``sbx1234``, its record holding ``rows``: the
    tables copied, and what else the copy asked for."""
    from provisa.api import demo_region_seed
    from provisa.federation import materialize_broker, replica_state

    copied: list[tuple] = []
    asked: list[str] = []

    def _copy_schema(src_path, src_schema, dst_path, dst_schema, tables=None):
        copied.append((src_schema, dst_schema))
        return ["orders"]

    async def _completed(conn):
        return conn.rows

    async def _definitions(model_org, keys, address_org):
        asked.append(f"definitions of {model_org} at {address_org}")
        return {key: "hash" for key in keys}

    async def _record_copied(conn, copied_rows):
        asked.append(f"recorded {len(copied_rows)}")

    monkeypatch.setattr(demo_region_seed, "regional", lambda: False)
    monkeypatch.setattr(materialize_broker, "copy_schema", _copy_schema)
    monkeypatch.setattr(demo_replicas, "_store_path", lambda org: f"/stores/{org}.duckdb")
    monkeypatch.setattr(demo_replicas, "_replicas_schema", lambda org: f"org_{org}_replicas")
    monkeypatch.setattr(demo_replicas, "_state_db", lambda org: _StateDb(rows))
    monkeypatch.setattr(replica_state, "completed_rows", _completed)
    monkeypatch.setattr(replica_state, "record_copied", _record_copied)
    monkeypatch.setattr(demo_replicas, "definitions", _definitions)
    monkeypatch.setattr(demo_replicas, "store_identity", lambda org, store=None: f"store of {org}")
    return copied, asked


async def test_a_store_with_no_replica_built_is_copied_without_building_its_templates_runtime(
    monkeypatch,
):
    """Its listener-landed tables are in its replicas schema, and copied; with no replica
    recorded there is no definition to compute, and so no runtime of the template to build."""
    copied, asked = _copy(monkeypatch, rows=[])
    await demo_replicas.copy_to_sandbox("sbxtplstreaming", "sbx1234")
    assert copied == [("org_sbxtplstreaming_replicas", "org_sbx1234_replicas")]
    assert asked == []


async def test_a_store_with_replicas_built_is_copied_and_recorded_as_built_for_the_sandbox(
    monkeypatch,
):
    named: dict[str, Any] = {
        "source_id": "catalog",
        "schema_name": "public",
        "table_name": "products",
        "definition_hash": "hash",
    }
    row = {**{field: None for field in demo_replicas._SEED_FIELDS}, **named}
    copied, asked = _copy(monkeypatch, rows=[row])
    await demo_replicas.copy_to_sandbox("sbxtplstorefront", "sbx1234")
    assert copied == [("org_sbxtplstorefront_replicas", "org_sbx1234_replicas")]
    assert asked == [
        "definitions of sbxtplstorefront at sbxtplstorefront",
        "definitions of sbxtplstorefront at sbx1234",
        "recorded 1",
    ]


class _RolesConn:
    """A connection to an org's schema: what reached it, a statement or a transaction."""

    def __init__(self, roles: list[dict]) -> None:
        self.roles = roles
        self.sent: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        self.sent.append("begin")
        yield
        self.sent.append("commit")

    async def execute_core(self, stmt):
        self.sent.append("read roles")
        rows = [SimpleNamespace(_mapping=r) for r in self.roles]
        return SimpleNamespace(fetchall=lambda: rows)

    async def upsert(self, table, values, **kwargs):
        self.sent.append(f"grant {values['role_id']}")


def _roles() -> list[dict]:
    return [
        {"id": "sandbox", "capabilities": ["usage"], "parents": []},
        {"id": "shop_analyst", "capabilities": ["usage"], "parents": []},
        {"id": "data_steward", "capabilities": ["usage"], "parents": []},
    ]


async def test_a_persons_roles_are_granted_reading_the_roles_once_in_one_transaction():
    from provisa.core.org_membership import grant_org_roles
    from provisa.security.rights import DEPLOYMENT_GRANTER

    conn = _RolesConn(_roles())
    await grant_org_roles(
        conn,
        "sandbox-visitor-sbx1234",
        ["sandbox", "shop_analyst", "data_steward"],
        granter_capabilities=DEPLOYMENT_GRANTER,
    )
    assert conn.sent == [
        "begin",
        "read roles",
        "grant sandbox",
        "grant shop_analyst",
        "grant data_steward",
        "commit",
    ]


async def test_a_role_the_schema_does_not_hold_is_refused_before_any_is_granted():
    from provisa.core.org_membership import grant_org_roles
    from provisa.security.rights import DEPLOYMENT_GRANTER

    conn = _RolesConn(_roles())
    with pytest.raises(ValueError, match="'auditor' does not exist"):
        await grant_org_roles(
            conn,
            "sandbox-visitor-sbx1234",
            ["sandbox", "auditor"],
            granter_capabilities=DEPLOYMENT_GRANTER,
        )
    assert not [s for s in conn.sent if s.startswith("grant")]
