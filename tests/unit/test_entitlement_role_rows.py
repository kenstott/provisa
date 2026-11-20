# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b4c92-7d3a-4f58-b0c6-2a9e8d5f1b37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: a role's defaults and ceiling are rows of the entitlement store, source role and no
person, written from the model at each model build; append-only, readable as of any time."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, select

from provisa.core.models import Role
from provisa.core.repositories import role as role_repo
from provisa.core.schema_org import entitlements, roles
from provisa.entitlements import history, store
from provisa.entitlements.lists import ROLE, effective_lists

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def conn(control_plane):
    db = control_plane.org()
    async with db.acquire() as c:
        yield c


#: The roles these tests make; the org's seeded standard roles get their rows too.
OURS = ("seller", "auditor", "bare")


async def _role(conn, role_id, domain_access, defaults=None):
    await role_repo.upsert(
        conn,
        Role(
            id=role_id,
            description="Test role.",
            capabilities=["query_development"],
            domain_access=domain_access,
            default_entitlements=defaults,
        ),
        org_id="default",
    )


async def _ours(conn) -> dict[str, store.RoleRows]:
    return {k: v for k, v in (await store.load_role_rows(conn)).items() if k in OURS}


async def _role_rows(conn):
    rows = await conn.execute_core(
        select(entitlements)
        .where(entitlements.c.source == ROLE, entitlements.c.role_id.in_(OURS))
        .order_by(entitlements.c.id)
    )
    return rows.fetchall()


def _shape(rows):
    return {(r.role_id, r.kind, r.capability, tuple(r.identifiers), r.removed) for r in rows}


async def test_a_build_writes_one_role_row_per_role_and_kind(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"], "relationships": []})
    await _role(conn, "auditor", ["hr"], {"domains": []})
    await store.write_role_rows(conn)
    rows = await _role_rows(conn)
    assert all(r.principal is None for r in rows)
    assert _shape(rows) == {
        ("seller", "tables", None, ("sales.orders",), False),
        ("seller", "relationships", None, (), False),
        ("seller", "domains", store.CEILING, ("sales",), False),
        ("auditor", "domains", None, (), False),
        ("auditor", "domains", store.CEILING, ("hr",), False),
    }


async def test_a_rebuild_with_no_change_appends_nothing(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"]})
    await store.write_role_rows(conn)
    first = await _role_rows(conn)
    await store.write_role_rows(conn)
    assert len(await _role_rows(conn)) == len(first)


async def test_a_changed_default_closes_the_old_row_and_appends_the_new_one(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"]})
    await store.write_role_rows(conn)
    await _role(conn, "seller", ["sales"], {"tables": ["sales.*"]})
    await store.write_role_rows(conn)
    periods = await history.entitlement_periods(conn, source=ROLE, role_id="seller", kind="tables")
    assert [(tuple(p.row.identifiers), p.valid_to is None) for p in periods] == [
        (("sales.orders",), False),
        (("sales.*",), True),
    ]
    assert periods[0].valid_to == periods[1].valid_from
    # the ceiling did not change, so it appended nothing
    ceiling = await history.entitlement_periods(conn, source=ROLE, role_id="seller", kind="domains")
    assert len(ceiling) == 1


async def test_a_changed_ceiling_closes_the_old_ceiling_row(conn):
    await _role(conn, "seller", ["sales"], {"tables": []})
    await store.write_role_rows(conn)
    await _role(conn, "seller", ["sales", "hr"], {"tables": []})
    await store.write_role_rows(conn)
    assert await _ours(conn) == {
        "seller": store.RoleRows(defaults={"tables": ()}, ceiling=("sales", "hr"))
    }


async def test_a_removed_role_and_a_removed_kind_close_their_rows(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"], "relationships": []})
    await _role(conn, "auditor", ["hr"], {"domains": []})
    await store.write_role_rows(conn)
    await conn.execute_core(delete(roles).where(roles.c.id == "auditor"))
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"]})
    await store.write_role_rows(conn)
    held = await _ours(conn)
    assert held == {
        "seller": store.RoleRows(defaults={"tables": ("sales.orders",)}, ceiling=("sales",))
    }
    removed = [r for r in await _role_rows(conn) if r.removed]
    assert {(r.role_id, r.kind, r.capability) for r in removed} == {
        ("seller", "relationships", None),
        ("auditor", "domains", None),
        ("auditor", "domains", store.CEILING),
    }
    # writing again with nothing changed appends nothing more
    count = len(await _role_rows(conn))
    await store.write_role_rows(conn)
    assert len(await _role_rows(conn)) == count


async def test_the_role_rows_read_as_of_any_time(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"]})
    await store.write_role_rows(conn)
    between = (await _role_rows(conn))[-1].recorded_at
    await _role(conn, "seller", ["sales"], {"tables": ["sales.*"]})
    await store.write_role_rows(conn)
    now = datetime.now(UTC)

    def tables(view: history.AsOf) -> list[tuple[str, ...]]:
        return [
            tuple(p.row.identifiers)
            for p in view.entitlements
            if p.row.source == ROLE and p.row.kind == "tables"
        ]

    assert tables(await history.as_of(conn, between, between)) == [("sales.orders",)]
    assert tables(await history.as_of(conn, now, now)) == [("sales.*",)]
    # what was valid before the change, as recorded now, is still the first row
    assert tables(await history.as_of(conn, between, now)) == [("sales.orders",)]


async def test_a_role_with_no_stored_defaults_writes_its_ceiling_only(conn):
    await conn.execute_core(
        roles.insert().values(
            id="bare",
            description="Test role.",
            capabilities=[],
            domain_access=["*"],
            default_entitlements=None,
        )
    )
    await store.write_role_rows(conn)
    assert _shape(await _role_rows(conn)) == {("bare", "domains", store.CEILING, ("*",), False)}


async def test_precedence_read_over_the_store_matches_effective_lists(conn):
    defaults = {"tables": ["sales.*"], "relationships": ["orders_customer"], "domains": ["sales"]}
    await _role(conn, "seller", ["sales"], defaults)
    await store.write_role_rows(conn)
    await store.set_list(
        conn, "alice", "seller", "tables", ["sales.orders"], set_by="ops", reason="r"
    )
    await store.set_list(conn, "alice", "seller", "relationships", ["x"], set_by="ops", reason="r")
    await store.mirror_idp(conn, "alice", {"seller": {"relationships": ("orders_customer",)}})

    idp = (await store.load_idp(conn))["alice"]
    held = (await store.load_held(conn))["alice"]
    from_store = (await store.load_role_rows(conn))["seller"].defaults
    assert effective_lists("seller", idp, held, from_store) == effective_lists(
        "seller", idp, held, defaults
    )
    out = effective_lists("seller", idp, held, from_store)
    assert {k: (v.origin, v.identifiers) for k, v in out.items()} == {
        "relationships": ("idp", ("orders_customer",)),
        "tables": ("provisa", ("sales.orders",)),
        "domains": ("role", ("sales",)),
    }
