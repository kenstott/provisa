# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e3f2a-9c41-4d7e-8a26-1f3c7d9e4b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A revoke is not refused (REQ-2059, amended 2026-10-29). Revoking a role (REQ-1918) or changing
a role's parents is never refused for the mask entry it leaves unreachable, so a role can always be
deleted. That entry stays stored, has no effect on a read -- the first entry a reader holds still
applies -- and is refused by name at the next save of its column."""

# Requirements: REQ-2059, REQ-1918

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core.models import Column, Role, Table
from provisa.core.repositories import grants
from provisa.core.repositories import role as role_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import roles, table_columns
from provisa.security.inheritance import role_chains
from provisa.security.masking import MaskUnreachable, resolve
from tests.unit.test_mask_rules import PREFIX, plane  # noqa: F401  (the fixture)

WITHHELD = {"type": "constant", "value": "withheld"}
NULLED = {"type": "constant", "value": None}
# ``clerk`` inherits ``junior`` (the ``plane`` fixture). Every entry is reached: clerk reads the
# first, junior the second, everyone else the last.
REVOKABLE = [
    {"to": ["clerk"], "mask": WITHHELD},
    {"to": ["junior"], "mask": None},
    {"to": None, "mask": NULLED},
]


def _table(masks: list[dict]) -> Table:
    return Table(
        source_id="pg",
        domain_id="sales",
        schema="public",
        table="orders",
        columns=[Column(name="email", data_type="varchar", visible_to=["junior"], masks=masks)],
    )


async def _stored(conn) -> list[dict]:
    [masks] = [r[0] for r in (await conn.execute_core(select(table_columns.c.masks))).fetchall()]
    # The store writes every field of a mask; the unset ones are None.
    return [
        {
            "to": e["to"],
            "mask": None
            if e["mask"] is None
            else {k: v for k, v in e["mask"].items() if v is not None or k == "value"},
        }
        for e in masks
    ]


async def _chains(conn) -> dict[str, list[str]]:
    rows = (await conn.execute_core(select(roles.c.id, roles.c.parents))).fetchall()
    return role_chains({"id": r[0], "parents": r[1]} for r in rows)


async def _upsert_role(conn, role_id: str, parents: list[str]) -> None:
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


async def test_a_revoke_that_leaves_an_entry_unreachable_is_not_refused(plane):  # noqa: F811
    async with plane.acquire() as conn:
        table_id = await table_repo.upsert(conn, _table(REVOKABLE))
        assert table_id is not None
        assert await grants.revoke_from_table(conn, table_id, "clerk") is True
        # Nothing names the role any more, so it can be deleted.
        assert await role_repo.delete(conn, "clerk") is True


async def test_the_entry_a_revoke_leaves_unreachable_stays_stored(plane):  # noqa: F811
    async with plane.acquire() as conn:
        table_id = await table_repo.upsert(conn, _table(REVOKABLE))
        assert table_id is not None
        await grants.revoke_from_table(conn, table_id, "clerk")
        stored = await _stored(conn)
    assert stored == [
        {"to": [], "mask": WITHHELD},
        {"to": ["junior"], "mask": None},
        {"to": None, "mask": NULLED},
    ]


async def test_the_unreachable_entry_has_no_effect_on_a_read(plane):  # noqa: F811
    async with plane.acquire() as conn:
        table_id = await table_repo.upsert(conn, _table(REVOKABLE))
        assert table_id is not None
        await grants.revoke_from_table(conn, table_id, "clerk")
        stored = await _stored(conn)
        chains = await _chains(conn)
    reachable = stored[1:]
    readers = [chains["clerk"], chains["junior"], ["anyone"], []]
    for chain in readers:
        assert resolve(stored, chain, "varchar") == resolve(reachable, chain, "varchar")
    # clerk now reads through the first entry it holds: junior's, unmasked.
    assert resolve(stored, chains["clerk"], "varchar") is None
    everyone = resolve(stored, ["anyone"], "varchar")
    assert everyone is not None and everyone.value is None


async def test_the_next_save_of_the_column_refuses_the_entry_by_name(plane):  # noqa: F811
    async with plane.acquire() as conn:
        table_id = await table_repo.upsert(conn, _table(REVOKABLE))
        assert table_id is not None
        await grants.revoke_from_table(conn, table_id, "clerk")
        stored = await _stored(conn)
        with pytest.raises(MaskUnreachable) as refused:
            await table_repo.upsert(conn, _table(stored))
        assert await _stored(conn) == stored
    assert str(refused.value) == PREFIX + "[no one]"
    assert refused.value.column == "email"


# --- a parent change ------------------------------------------------------------------------

# Reached while ``auditor`` stands alone; once it inherits ``junior``, junior's entry holds it first.
REPARENTED = [
    {"to": ["junior"], "mask": None},
    {"to": ["auditor"], "mask": WITHHELD},
    {"to": None, "mask": NULLED},
]


async def test_a_parent_change_that_leaves_an_entry_unreachable_is_not_refused(plane):  # noqa: F811
    async with plane.acquire() as conn:
        await _upsert_role(conn, "auditor", [])
        await table_repo.upsert(conn, _table(REPARENTED))
        await _upsert_role(conn, "auditor", ["junior"])
        stored = await _stored(conn)
        chains = await _chains(conn)
        assert chains["auditor"] == ["auditor", "junior"]
        assert stored == REPARENTED
        # The read: auditor reads through the first entry it holds, junior's, unmasked.
        assert resolve(stored, chains["auditor"], "varchar") is None
        for chain in (chains["auditor"], chains["junior"], ["anyone"]):
            assert resolve(stored, chain, "varchar") == resolve(
                [stored[0], stored[2]], chain, "varchar"
            )
        with pytest.raises(MaskUnreachable) as refused:
            await table_repo.upsert(conn, _table(stored))
    assert str(refused.value) == PREFIX + "[auditor]"
