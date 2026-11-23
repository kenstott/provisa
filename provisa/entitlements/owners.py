# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3e2b91-5c4a-4f08-9e61-2a8b0c4d6f17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's owners: people, each holding a data_product_owner row naming it (REQ-2219).

Amended 2026-11-23: the Owner field names people. Saving it writes or ends their Provisa-held
``data_product_owner`` rows (:func:`set_product_owners`), one per role grant of theirs that holds
the capability, scoped to the products it lists. A product's owner is a person whose entitlement
row names it: a Provisa-held row of the capability, or an identity provider's ``data_products``
list on a role holding it. A role holding the capability with no list owns nothing by it.

:func:`owned_by` is the one reading of those rows, for one person: the runtime reads it for the
bound person (``entitlements.runtime.owned_products``), the store for everyone
(:func:`product_owners`).
"""

# Requirements: REQ-2219

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.compiler.naming import domain_to_sql_name
from provisa.entitlements import history
from provisa.entitlements.index import matches
from provisa.entitlements.lists import IDP, PROVISA, ExpiryRule, HeldList, IdpLists
from provisa.security.rights import Capability

if TYPE_CHECKING:
    from provisa.core.database import Connection

#: The capability an owner holds, and the kind of list that scopes it.
DATA_PRODUCT_OWNER = Capability.DATA_PRODUCT_OWNER.value
DATA_PRODUCTS_KIND = "data_products"


def product_identifier(product_id: str, domain_id: str) -> str:
    """How an owner's row names a product: domain.product, as a creator's row does."""
    return f"{domain_to_sql_name(domain_id)}.{product_id}"


def product_paths(product_id: str, domain_id: str) -> tuple[tuple[str, ...], ...]:
    """The spelling a list's identifiers are matched against (``index._add_flat_objects``)."""
    return ((domain_to_sql_name(domain_id).lower(), product_id.lower()),)


def owned_by(
    lists: Iterable[HeldList],
    idp: IdpLists,
    expiries: Collection[ExpiryRule],
    role_ids: Iterable[str],
    holds_owner: Callable[[str], bool],
    products: Iterable[tuple[str, str]],
    at: datetime,
) -> tuple[str, ...]:
    """The ids of ``products`` ((id, domain id)) one person owns at ``at``: those a row of theirs
    names on one of ``role_ids`` holding the capability (``holds_owner``), less what an expiry rule
    passed by then ends of it. ``lists`` are their Provisa-held rows, ``idp`` their identity
    provider's lists, ``expiries`` their rules."""
    from provisa.entitlements.runtime import list_reach

    held = tuple(lists)
    reaches = []
    for role_id in dict.fromkeys(role_ids):
        if not holds_owner(role_id):
            continue
        rules = tuple(r for r in expiries if r.role_id == role_id)
        own = tuple(
            i
            for row in held
            if row.role_id == role_id and row.capability == DATA_PRODUCT_OWNER
            for i in row.identifiers
        )
        if own:
            reaches.append(
                list_reach(own, PROVISA, rules, DATA_PRODUCT_OWNER, DATA_PRODUCTS_KIND, at)
            )
        from_idp = idp.get(role_id, {}).get(DATA_PRODUCTS_KIND)
        if from_idp:
            reaches.append(
                list_reach(tuple(from_idp), IDP, rules, DATA_PRODUCT_OWNER, DATA_PRODUCTS_KIND, at)
            )
    return tuple(
        pid
        for pid, did in sorted(products)
        if any(r.reaches(product_paths(pid, did)) for r in reaches)
    )


async def _role_capabilities(conn: "Connection") -> dict[str, frozenset[str]]:
    """Each role's capabilities with its ancestors' folded in."""
    from provisa.core.schema_org import roles
    from provisa.security.inheritance import role_chains

    rows = [
        dict(r._mapping)
        for r in (
            await conn.execute_core(select(roles.c.id, roles.c.capabilities, roles.c.parents))
        ).fetchall()
    ]
    own = {r["id"]: frozenset(r["capabilities"] or ()) for r in rows}
    return {
        rid: frozenset(c for anc in chain for c in own.get(anc, ()))
        for rid, chain in role_chains(rows).items()
    }


@dataclass
class _Person:
    lists: list[HeldList]
    idp: dict[str, dict[str, tuple[str, ...]]]
    roles: set[str]


async def product_owners(conn: "Connection") -> dict[str, tuple[str, ...]]:
    """product id -> the people who own it now, by user id, sorted; a product no one owns is
    absent. What a display, an export and an approval read of a product's owners."""
    from provisa.core.repositories import data_product as data_product_repo

    products = await data_product_repo.refs(conn)
    capabilities = await _role_capabilities(conn)
    people: dict[str, _Person] = {}
    for period in history.current(await history.entitlement_periods(conn, kind=DATA_PRODUCTS_KIND)):
        row = period.row
        if row.principal is None:
            continue  # a role's own rows (source ``role``) name no person
        person = people.setdefault(row.principal, _Person([], {}, set()))
        if row.source == PROVISA:
            person.lists.append(
                HeldList(row.role_id, row.kind, tuple(row.identifiers), capability=row.capability)
            )
        elif row.source == IDP:
            person.idp.setdefault(row.role_id, {})[row.kind] = tuple(row.identifiers)
        else:
            continue
        person.roles.add(row.role_id)
    rules: dict[str, list[ExpiryRule]] = {}
    for period in history.current(await history.expiry_periods(conn)):
        row = period.row
        rules.setdefault(row.principal, []).append(
            ExpiryRule(
                row.role_id,
                row.ends_at,
                {cap: tuple(ids) for cap, ids in row.ends.items()},
                list_kind=row.list_kind,
            )
        )
    now = datetime.now(UTC)
    out: dict[str, list[str]] = {}
    for principal in sorted(people):
        person = people[principal]
        for pid in owned_by(
            person.lists,
            person.idp,
            rules.get(principal, ()),
            sorted(person.roles),
            lambda role_id: DATA_PRODUCT_OWNER in capabilities.get(role_id, frozenset()),
            products,
            now,
        ):
            out.setdefault(pid, []).append(principal)
    return {pid: tuple(owners) for pid, owners in out.items()}


async def owners_of(conn: "Connection", product_id: str) -> tuple[str, ...]:
    """The people who own ``product_id`` now, by user id, sorted."""
    return (await product_owners(conn)).get(product_id, ())


class OwnerRefused(ValueError):
    """A person named in an Owner field who holds no role grant carrying data_product_owner: a row
    on any other role would make them no owner."""

    code = "data_product.owner_not_eligible"

    def __init__(self, principal: str) -> None:
        super().__init__(
            f"{principal!r} holds no role with the data_product_owner capability, so cannot be "
            "named a data product's owner; grant them such a role (data_product_owner) first"
        )
        self.principal = principal


async def _eligible_roles(
    conn: "Connection", principal: str, capabilities: Mapping[str, frozenset[str]]
) -> list[str]:
    from provisa.auth.role_grants import held_grants

    return sorted(
        {
            g.role_id
            for g in await held_grants(conn, principal)
            if DATA_PRODUCT_OWNER in capabilities.get(g.role_id, frozenset())
        }
    )


async def set_product_owners(
    conn: "Connection",
    product_id: str,
    domain_id: str,
    owners: Iterable[str],
    *,
    set_by: str | None,
) -> None:
    """Make ``owners`` (user ids) the people Provisa holds as ``product_id``'s owners: a row naming
    it on each of a named person's role grants holding the capability, and the product ended from
    every Provisa-held row of anyone else. An identity provider's lists are its own and are not
    written. Refused (:class:`OwnerRefused`) before anything is written when a named person holds
    no such grant. The caller holds the transaction."""
    from provisa.entitlements.store import remove_list, set_list

    wanted = tuple(dict.fromkeys(o for o in owners))
    identifier = product_identifier(product_id, domain_id)
    paths = product_paths(product_id, domain_id)
    capabilities = await _role_capabilities(conn)
    eligible = {p: await _eligible_roles(conn, p, capabilities) for p in wanted}
    for principal, role_ids in eligible.items():
        if not role_ids:
            raise OwnerRefused(principal)
    held = {
        (p.row.principal, p.row.role_id): tuple(p.row.identifiers)
        for p in history.current(
            await history.entitlement_periods(conn, source=PROVISA, kind=DATA_PRODUCTS_KIND)
        )
        if p.row.capability == DATA_PRODUCT_OWNER
    }
    reason_set = f"named an owner of data product {product_id}"
    reason_end = f"no longer an owner of data product {product_id}"
    for (principal, role_id), identifiers in held.items():
        if principal in wanted and role_id in eligible[principal]:
            continue
        if not any(matches(i, paths) for i in identifiers):
            continue
        kept = [i for i in identifiers if not matches(i, paths)]
        if kept:
            await set_list(
                conn,
                principal,
                role_id,
                DATA_PRODUCTS_KIND,
                kept,
                set_by=set_by,
                reason=reason_end,
                capability=DATA_PRODUCT_OWNER,
            )
        else:
            await remove_list(
                conn,
                principal,
                role_id,
                DATA_PRODUCTS_KIND,
                set_by=set_by,
                reason=reason_end,
                capability=DATA_PRODUCT_OWNER,
            )
    for principal, role_ids in eligible.items():
        for role_id in role_ids:
            identifiers = held.get((principal, role_id), ())
            if any(matches(i, paths) for i in identifiers):
                continue
            await set_list(
                conn,
                principal,
                role_id,
                DATA_PRODUCTS_KIND,
                [*identifiers, identifier],
                set_by=set_by,
                reason=reason_set,
                capability=DATA_PRODUCT_OWNER,
            )


async def save_owner_field(
    conn: "Connection",
    product_id: str,
    domain_id: str,
    owners: Iterable[str] | None,
    *,
    stored_domain_id: str | None,
    set_by: str | None,
) -> set[str]:
    """Write a product's Owner field as a save does, every surface alike (GraphQL, MCP):
    ``owners``, or the owners held when it is None. A product moved from ``stored_domain_id``
    keeps its owners, named under its new domain. Returns the people whose rows it wrote, for
    :func:`reload_owner_rows`; nothing is written when the field is None and the product stays."""
    moved = stored_domain_id is not None and stored_domain_id != domain_id
    if owners is None and not moved:
        return set()
    before = set(await owners_of(conn, product_id)) if stored_domain_id is not None else set()
    wanted = list(owners) if owners is not None else sorted(before)
    if stored_domain_id is not None and moved:
        # The rows name the product under its old domain: end them all, then name it anew.
        await set_product_owners(conn, product_id, stored_domain_id, [], set_by=set_by)
    await set_product_owners(conn, product_id, domain_id, wanted, set_by=set_by)
    return before | set(wanted)


async def end_product_owners(
    conn: "Connection", product_id: str, domain_id: str, *, set_by: str | None
) -> set[str]:
    """End every Provisa-held owner row naming a product being deleted: no row is left naming a
    product that is gone. Call before the product's row is deleted; returns its owners."""
    ended = set(await owners_of(conn, product_id))
    await set_product_owners(conn, product_id, domain_id, [], set_by=set_by)
    return ended


async def reload_owner_rows(conn: "Connection", state: Any, people: Iterable[str]) -> None:
    """What the runtime holds of the entitlement store after an Owner field wrote to it, with the
    products a row can name (a product just created among them), and each person's effective rows
    recomputed, as every other write of a person's rows does."""
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.entitlements import effective_store, store

    state.held_entitlements = await store.load_held(conn)
    state.data_product_refs = await data_product_repo.refs(conn)
    for principal in sorted(set(people)):
        await effective_store.write_person(conn, state, principal)
