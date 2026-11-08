# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b41-3c6a-4f85-b0e7-1a9c5d3f8e62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2215: the members of role R in org O, by the resolver that owns its membership.

What is asserted is what a wrong contract gets wrong silently: a resolver that overruns its
declared limit answered as a role with no members, a role with none answered without its name, a
principal reached twice listed twice, and Provisa's own resolver reading an expired grant or
another role's assignments.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from provisa.core.role_members import (
    Principal,
    ProvisaAssignmentsResolver,
    ResolverTimeout,
    RoleMemberResolver,
    resolve_role_members,
)


class _Fixed:
    name = "fixed"
    time_limit_s = 1.0

    def __init__(self, principals: list[Principal]) -> None:
        self._principals = principals
        self.asked: list[tuple[str, str]] = []

    async def members(self, org_id: str, role_id: str) -> list[Principal]:
        self.asked.append((org_id, role_id))
        return list(self._principals)


class _Stalled:
    name = "stalled"
    time_limit_s = 0.05

    async def members(self, org_id: str, role_id: str) -> list[Principal]:
        await asyncio.sleep(5)
        return []


async def test_principals_are_answered_once_each_in_id_order():
    bob = Principal("bob", "Bob", "bob@x.test")
    alice = Principal("alice", "Alice", "alice@x.test")
    resolver = _Fixed([bob, alice, bob])
    got = await resolve_role_members(resolver, "acme", "steward")
    assert resolver.asked == [("acme", "steward")]
    assert got.principals == (alice, bob)
    assert got.source == "fixed"
    assert got.resolvable
    assert got.unresolved_reason is None


async def test_a_role_with_no_members_says_so_naming_the_role():
    got = await resolve_role_members(_Fixed([]), "acme", "auditor")
    assert not got.resolvable
    assert got.unresolved_reason is not None
    assert "'auditor'" in got.unresolved_reason


async def test_an_overrun_is_refused_by_name_not_answered_as_no_members():
    with pytest.raises(ResolverTimeout) as caught:
        await resolve_role_members(_Stalled(), "acme", "steward")
    assert caught.value.resolver == "stalled"
    assert caught.value.role_id == "steward"
    assert "0.05" in str(caught.value)


def test_provisas_resolver_implements_the_contract():
    resolver = ProvisaAssignmentsResolver(MagicMock())
    assert isinstance(resolver, RoleMemberResolver)
    assert resolver.time_limit_s > 0


class _Db:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.statements: list = []

    @asynccontextmanager
    async def acquire(self):
        db = self

        class _Conn:
            async def execute_core(self, stmt):
                db.statements.append(stmt)
                result = MagicMock()
                result.fetchall.return_value = db.rows
                return result

        yield _Conn()


async def test_provisas_resolver_reads_the_roles_unexpired_assignments_with_directory_names():
    db = _Db([("alice", "Alice", "alice@x.test"), ("carol", None, None)])
    got = await ProvisaAssignmentsResolver(db).members("acme", "steward")  # type: ignore[arg-type]
    assert got == [
        Principal("alice", "Alice", "alice@x.test"),
        Principal("carol", None, None),
    ]
    (stmt,) = db.statements
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "user_role_assignments.role_id = 'steward'" in sql
    assert "user_role_assignments.expires_at IS NULL" in sql
    assert "user_role_assignments.expires_at > now()" in sql
    assert "LEFT OUTER JOIN user_directory" in sql
