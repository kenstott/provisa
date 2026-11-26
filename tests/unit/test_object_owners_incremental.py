# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4e7a21-3b58-4d06-8f1e-6a2d5c7b9e40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One person's object_owners rows are rewritten alone (REQ-2219).

A sign-in, an invite redemption and a role grant each change one person's inputs:
``effective_store.write_person`` rewrites only that person's rows of object_owners
(``write_object_owners(conn, principal)``), and the result is the full rewrite's. The full rewrite
stays for what can move many owners (a model build, an Owner or Steward field). Proven equal over
randomized org states, and measured at a realistic org size (2,000 objects, 200 people)."""

# Requirements: REQ-2219

from __future__ import annotations

import random
import time
import types
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from provisa.core.schema_org import (
    data_products,
    domains,
    model_names,
    object_owners,
    registered_tables,
    relationships,
    roles,
    sources,
    user_role_assignments,
)
from provisa.entitlements import effective_store, store
from provisa.entitlements.owners import (
    DOMAINS,
    PRODUCTS,
    RELATIONSHIPS,
    OwnedKind,
    domain_identifier,
    product_identifier,
    relationship_identifier,
    write_object_owners,
)

pytestmark = pytest.mark.unit

_KINDS = (PRODUCTS, DOMAINS, RELATIONSHIPS)
_PAST = datetime(2020, 1, 1, tzinfo=UTC)
_FUTURE = datetime(2999, 1, 1, tzinfo=UTC)


class _Org:
    """A randomized org: domains (some nested), products, relationships, roles holding random
    owner capabilities (some through a parent), and people with grants, Provisa-held lists,
    identity-provider lists and expiry rules; roles with rows of their own."""

    def __init__(self, rng: random.Random, n_domains: int, n_products: int, n_rels: int) -> None:
        self.rng = rng
        self.domains = [f"d{i}" for i in range(n_domains)]
        self.products = [(f"p{i}", rng.choice(self.domains)) for i in range(n_products)]
        self.tables = [
            (i + 1, f"t{i}", rng.choice(self.domains)) for i in range(max(2, n_rels // 10))
        ]
        self.rels: list[tuple[str, int, str]] = []
        for i in range(n_rels):
            tid = rng.choice(self.tables)[0]
            self.rels.append((f"r{i}", tid, f"f{i}"))
        self.roles = ["r_all", "r_prod", "r_dom", "r_rel", "r_child", "r_none"]

    def identifier(self, kind: OwnedKind) -> str:
        """A list identifier of ``kind``: mostly one object, sometimes a domain prefix or a
        wildcard, sometimes naming nothing."""
        rng = self.rng
        pick = rng.random()
        if kind is PRODUCTS:
            pid, did = rng.choice(self.products)
            if pick < 0.1:
                return domain_identifier(did)
            if pick < 0.15:
                return f"*.{pid}"
            if pick < 0.2:
                return "nowhere.nothing"
            return product_identifier(pid, did)
        if kind is DOMAINS:
            did = rng.choice(self.domains)
            return "*" if pick < 0.03 else domain_identifier(did)
        rid, tid, field = rng.choice(self.rels)
        table = next(t for t in self.tables if t[0] == tid)
        if pick < 0.1:
            return domain_identifier(table[2])
        return relationship_identifier(table[2], table[1], field)

    def identifiers(self, kind: OwnedKind, most: int = 4) -> list[str]:
        return [self.identifier(kind) for _ in range(self.rng.randint(1, most))]


_CAPS = {
    "r_all": ["data_product_owner", "domain_steward", "relationship_owner"],
    "r_prod": ["data_product_owner"],
    "r_dom": ["domain_steward"],
    "r_rel": ["relationship_owner"],
    "r_child": [],  # holds r_all's through its parent
    "r_none": [],
}
_PARENTS = {"r_child": ["r_all"]}


async def _build(conn: Any, org: _Org) -> None:
    rng = org.rng
    await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    for i, did in enumerate(org.domains):
        parent = org.domains[rng.randrange(i)] if i and rng.random() < 0.3 else None
        await conn.execute_core(insert(domains).values(id=did, parent_domain_id=parent))
    await conn.execute_core(
        insert(registered_tables).values(
            [
                {
                    "id": tid,
                    "source_id": "pg",
                    "domain_id": did,
                    "schema_name": "public",
                    "table_name": name,
                }
                for tid, name, did in org.tables
            ]
        )
    )
    await conn.execute_core(
        insert(model_names).values(
            [
                {
                    "object_kind": "table",
                    "object_id": str(tid),
                    "surface": "sql",
                    "name": name,
                    "physical_name": name,
                    "label": f"table {name}",
                    "domain_id": did,
                    "table_id": tid,
                }
                for tid, name, did in org.tables
            ]
        )
    )
    await conn.execute_core(
        insert(relationships).values(
            [
                {
                    "id": rid,
                    "source_table_id": tid,
                    "target_table_id": org.tables[0][0],
                    "source_column": "c",
                    "target_column": "id",
                    "cardinality": "many-to-one",
                    "graphql_field": field,
                }
                for rid, tid, field in org.rels
            ]
        )
    )
    await conn.execute_core(
        insert(data_products).values(
            [{"id": pid, "domain_id": did, "name": pid} for pid, did in org.products]
        )
    )
    for role_id in org.roles:
        await conn.execute_core(
            insert(roles).values(
                id=role_id,
                capabilities=_CAPS[role_id],
                domain_access=["*"],
                parents=_PARENTS.get(role_id, []),
            )
        )
    for kind in _KINDS:
        for role_id in rng.sample(org.roles, 2):
            await store.set_role_list(
                conn,
                role_id,
                kind.list_kind,
                org.identifiers(kind),
                capability=kind.capability,
                set_by="t",
                reason="t",
            )


async def _add_person(conn: Any, org: _Org, principal: str) -> None:
    """A person with one to three grants (one perhaps lapsed), lists on some, an identity
    provider's on some, and expiry rules (passed and ahead) on some."""
    rng = org.rng
    for role_id in rng.sample(org.roles, rng.randint(1, 3)):
        await _grant(conn, principal, role_id, lapsed=rng.random() < 0.1)
    await _mutate(conn, org, principal, rng.randint(1, 4))


async def _grant(conn: Any, principal: str, role_id: str, *, lapsed: bool = False) -> None:
    """Grant ``role_id`` (or regrant it: a lapsed grant given again holds)."""
    stmt = pg_insert(user_role_assignments).values(
        user_id=principal,
        role_id=role_id,
        domain_id="*",
        expires_at=_PAST if lapsed else None,
    )
    await conn.execute_core(
        stmt.on_conflict_do_update(
            index_elements=["user_id", "role_id", "domain_id"],
            set_={"expires_at": stmt.excluded.expires_at},
        )
    )


async def _mutate(conn: Any, org: _Org, principal: str, steps: int = 1) -> None:
    """``steps`` random changes to ``principal``'s inputs, each one a sign-in, redemption or
    grant could make: a list set or ended, a grant given or lapsed, the identity provider's lists,
    an expiry rule."""
    rng = org.rng
    for _ in range(steps):
        kind = rng.choice(_KINDS)
        role_id = rng.choice(org.roles)
        op = rng.randrange(6)
        if op == 0:
            await store.set_list(
                conn,
                principal,
                role_id,
                kind.list_kind,
                org.identifiers(kind),
                set_by="t",
                reason="t",
                capability=kind.capability,
            )
        elif op == 1:
            await store.remove_list(
                conn,
                principal,
                role_id,
                kind.list_kind,
                set_by="t",
                reason="t",
                capability=kind.capability,
            )
        elif op == 2:
            await _grant(conn, principal, role_id)
        elif op == 3:
            await conn.execute_core(
                update(user_role_assignments)
                .where(
                    user_role_assignments.c.user_id == principal,
                    user_role_assignments.c.role_id == role_id,
                )
                .values(expires_at=_PAST)
            )
        elif op == 4:
            await store.mirror_idp(
                conn, principal, {role_id: {kind.list_kind: tuple(org.identifiers(kind))}}
            )
        else:
            await store.add_expiry(
                conn,
                principal,
                role_id,
                rng.choice((_PAST, _FUTURE)),
                {rng.choice((kind.capability, "*")): org.identifiers(kind, 2)},
                set_by="t",
                reason="t",
                list_kind=rng.choice((kind.list_kind, None)),
            )


async def _table(conn: Any) -> set[tuple[Any, ...]]:
    t = object_owners
    rows = await conn.execute_core(
        select(
            t.c.object_kind, t.c.object_id, t.c.principal_kind, t.c.principal, t.c.named, t.c.holds
        )
    )
    return {tuple(r) for r in rows.fetchall()}


@pytest.mark.parametrize("seed", range(16))
async def test_one_persons_rewrite_equals_the_full_rewrite(control_plane, seed):
    """Over randomized org states: after a change to one person's inputs, rewriting only their
    rows leaves object_owners exactly as a full rewrite does, and every other row as it was."""
    rng = random.Random(seed)
    org = _Org(rng, n_domains=8, n_products=40, n_rels=30)
    people = [f"u{i}" for i in range(12)]
    async with control_plane.org().acquire() as conn:
        await _build(conn, org)
        for principal in people:
            await _add_person(conn, org, principal)
        await write_object_owners(conn)
        assert await _table(conn), "the randomized org names no owner at all"
        for _ in range(10):
            principal = rng.choice([*people, "newcomer"])
            before = await _table(conn)
            await _mutate(conn, org, principal, rng.randint(1, 3))
            await write_object_owners(conn, principal)
            incremental = await _table(conn)
            await write_object_owners(conn)
            full = await _table(conn)
            assert incremental == full, (principal, sorted(incremental ^ full))
            others = {r for r in before if r[3] != principal}
            assert {r for r in incremental if r[3] != principal} == others


async def test_write_person_rewrites_only_that_persons_rows(control_plane):
    """A sign-in's effective_store.write_person replaces the signer's rows only: another
    person's row stands untouched (no full rewrite ran), and the signer's are recomputed."""
    rng = random.Random(42)
    org = _Org(rng, n_domains=4, n_products=10, n_rels=6)
    async with control_plane.org().acquire() as conn:
        await _build(conn, org)
        pid, did = org.products[0]
        await _grant(conn, "ana", "r_prod")
        await store.set_list(
            conn,
            "ana",
            "r_prod",
            PRODUCTS.list_kind,
            [product_identifier(pid, did)],
            set_by="t",
            reason="t",
            capability=PRODUCTS.capability,
        )
        await write_object_owners(conn)
        marker = ("data_product", org.products[1][0], "person", "someone_else", True, True)
        await conn.execute_core(
            insert(object_owners).values(
                object_kind=marker[0],
                object_id=marker[1],
                principal_kind=marker[2],
                principal=marker[3],
                named=marker[4],
                holds=marker[5],
            )
        )
        await store.remove_list(
            conn,
            "ana",
            "r_prod",
            PRODUCTS.list_kind,
            set_by="t",
            reason="t",
            capability=PRODUCTS.capability,
        )
        state = types.SimpleNamespace(names_table=None)
        await effective_store.write_person(conn, state, "ana")
        table = await _table(conn)
        await write_object_owners(conn)
        full = await _table(conn)
    assert marker in table
    assert ("data_product", pid, "person", "ana", True, True) not in table
    assert table == full | {marker}


async def test_one_persons_rewrite_leaves_the_roles_rows(control_plane):
    """A role's own row in object_owners is no person's: one person's rewrite neither removes
    nor duplicates it."""
    rng = random.Random(7)
    org = _Org(rng, n_domains=4, n_products=10, n_rels=6)
    async with control_plane.org().acquire() as conn:
        await _build(conn, org)
        await write_object_owners(conn)
        role_rows = {r for r in await _table(conn) if r[2] == "role"}
        assert role_rows
        await _add_person(conn, org, "ana")
        await write_object_owners(conn, "ana")
        assert {r for r in await _table(conn) if r[2] == "role"} == role_rows


async def test_cost_at_a_realistic_org_size(control_plane):
    """2,000 objects and 200 people: one person's rewrite costs a small part of the full one it
    replaces on the sign-in, redemption and grant paths. Prints both."""
    rng = random.Random(2219)
    org = _Org(rng, n_domains=40, n_products=1200, n_rels=760)
    people = [f"u{i}" for i in range(200)]
    async with control_plane.org().acquire() as conn:
        await _build(conn, org)
        for principal in people:
            await _add_person(conn, org, principal)
        await write_object_owners(conn)
        rows = len(await _table(conn))
        full: list[float] = []
        one: list[float] = []
        for principal in rng.sample(people, 5):
            await _mutate(conn, org, principal)
            start = time.perf_counter()
            await write_object_owners(conn)
            full.append(time.perf_counter() - start)
            await _mutate(conn, org, principal)
            start = time.perf_counter()
            await write_object_owners(conn, principal)
            one.append(time.perf_counter() - start)
        await conn.execute_core(delete(object_owners))
    full_ms = 1000 * sorted(full)[len(full) // 2]
    one_ms = 1000 * sorted(one)[len(one) // 2]
    print(
        f"\nobject_owners at 2,000 objects / 200 people ({rows} rows): "
        f"full rewrite median {full_ms:.0f} ms, one person's median {one_ms:.0f} ms"
    )
    assert one_ms * 5 < full_ms
