# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3e2b91-5c4a-4f08-9e61-2a8b0c4d6f17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who owns a data product or a relationship, and who stewards a domain: people, each holding a
row of the owner capability naming the object (REQ-2219).

Amended 2026-11-23: the Owner and Steward fields name people. Saving one writes or ends their
Provisa-held rows of the kind's capability (:func:`set_holders`): ``data_product_owner`` for a
product, ``domain_steward`` for a domain, ``relationship_owner`` for a relationship; one row per
role grant of theirs that holds the capability, scoped to the objects it lists. An object's owner
(or steward) is a person whose entitlement row names it: a Provisa-held row of the capability, or
an identity provider's list of the kind on a role holding it. A role holding the capability with
no list owns nothing by it.

:func:`owned_by` is the one reading of those rows, for one person: the runtime reads it for the
bound person (``entitlements.runtime.owned_products``), the store for everyone (:func:`holders`).
"""

# Requirements: REQ-609, REQ-2219

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

#: How an object is spelled for matching: each path a list identifier is matched against.
Paths = tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class OwnedKind:
    """A kind of object a person owns by an entitlement row: the capability the row carries, the
    kind of list that names the object, and how its field and refusals speak of it."""

    capability: str
    list_kind: str
    noun: str
    holder: str  # "owner" or "steward": what the field calls the people it names

    @property
    def refusal_code(self) -> str:
        return f"{self.noun.replace(' ', '_')}.{self.holder}_not_eligible"


PRODUCTS = OwnedKind(Capability.DATA_PRODUCT_OWNER.value, "data_products", "data product", "owner")
DOMAINS = OwnedKind(Capability.DOMAIN_STEWARD.value, "domains", "domain", "steward")
RELATIONSHIPS = OwnedKind(
    Capability.RELATIONSHIP_OWNER.value, "relationships", "relationship", "owner"
)

#: The capability a product's owner holds, and the kind of list that scopes it.
DATA_PRODUCT_OWNER = PRODUCTS.capability
DATA_PRODUCTS_KIND = PRODUCTS.list_kind


def product_identifier(product_id: str, domain_id: str) -> str:
    """How an owner's row names a product: domain.product, as a creator's row does."""
    return f"{domain_to_sql_name(domain_id)}.{product_id}"


def product_paths(product_id: str, domain_id: str) -> Paths:
    """The spelling a list's identifiers are matched against (``index._add_flat_objects``)."""
    return ((domain_to_sql_name(domain_id).lower(), product_id.lower()),)


def domain_identifier(domain_id: str) -> str:
    """How a steward's row names a domain: its SQL name."""
    return domain_to_sql_name(domain_id)


def relationship_identifier(domain_id: str, table_name: str, field: str) -> str:
    """How an owner's row names a relationship: its source table's domain, then the table and its
    field (``index._add_relationships``)."""
    return f"{domain_to_sql_name(domain_id)}.{table_name}_{field}"


def _segments(identifier: str) -> tuple[str, ...]:
    return tuple(s.lower() for s in identifier.split("."))


@dataclass(frozen=True)
class Owned:
    """The objects of one kind as rows are matched against them: ``reach`` what makes a person
    their holder (a domain's ancestors among it: a list naming a parent covers the domains under
    it, REQ-1363), ``exact`` what a saved field names and ends."""

    reach: Mapping[str, Paths]
    exact: Mapping[str, Paths]


async def _relationship_spellings(conn: "Connection") -> dict[str, str]:
    """relationship id -> its identifier, for every relationship whose names the last model build
    stored (its source table's SQL name, and its GraphQL field)."""
    from provisa.core.schema_org import model_names, registered_tables, relationships

    r, rt, mn = relationships, registered_tables, model_names
    rows = await conn.execute_core(
        select(r.c.id, rt.c.domain_id, mn.c.name, r.c.graphql_field)
        .select_from(
            r.join(rt, rt.c.id == r.c.source_table_id).join(
                mn,
                (mn.c.object_id == rt.c.id.cast(mn.c.object_id.type))
                & mn.c.object_kind.in_(("table", "view"))
                & (mn.c.surface == "sql"),
            )
        )
        .where(r.c.graphql_field.is_not(None), r.c.retired.is_(False))
    )
    return {
        row.id: relationship_identifier(row.domain_id, row.name, row.graphql_field)
        for row in rows.fetchall()
    }


async def owned_objects(conn: "Connection", kind: OwnedKind) -> Owned:
    """The objects of ``kind`` the org holds now (a retired one is not), spelled for matching."""
    if kind is PRODUCTS:
        from provisa.core.repositories import data_product as data_product_repo

        paths = {pid: product_paths(pid, did) for pid, did in await data_product_repo.refs(conn)}
        return Owned(paths, paths)
    if kind is DOMAINS:
        from provisa.entitlements.access_review import domain_spellings

        from provisa.core.schema_org import domains

        spellings = await domain_spellings(conn)
        spellings.pop("*")
        # A retired domain has no stewards (its row is kept under a key of its own, REQ-2248).
        for r in await conn.execute_core(select(domains.c.id).where(domains.c.retired.is_(True))):
            spellings.pop(r.id)
        return Owned(
            {d: tuple((s.lower(),) for s in names) for d, names in spellings.items()},
            {d: ((d.lower(),), (domain_identifier(d).lower(),)) for d in spellings},
        )
    paths = {rid: (_segments(i),) for rid, i in (await _relationship_spellings(conn)).items()}
    return Owned(paths, paths)


async def identifier_of(conn: "Connection", kind: OwnedKind, object_id: str, **at: str) -> str:
    """How a holder's row names ``object_id``. A product is named under ``at["domain_id"]``."""
    if kind is PRODUCTS:
        return product_identifier(object_id, at["domain_id"])
    if kind is DOMAINS:
        return domain_identifier(object_id)
    spelled = (await _relationship_spellings(conn)).get(object_id)
    if spelled is None:
        raise LookupError(
            f"relationship {object_id!r} has no stored names yet (the model build stores them), "
            "so no row can name it"
        )
    return spelled


def _owned(
    lists: Iterable[HeldList],
    idp: IdpLists,
    expiries: Collection[ExpiryRule],
    role_ids: Iterable[str],
    holds: Callable[[str], bool],
    objects: Mapping[str, Paths],
    at: datetime,
    kind: OwnedKind,
) -> tuple[str, ...]:
    from provisa.entitlements.runtime import list_reach

    held = tuple(lists)
    reaches = []
    for role_id in dict.fromkeys(role_ids):
        if not holds(role_id):
            continue
        rules = tuple(r for r in expiries if r.role_id == role_id)
        own = tuple(
            i
            for row in held
            if row.role_id == role_id
            and row.capability == kind.capability
            and row.kind == kind.list_kind
            for i in row.identifiers
        )
        if own:
            reaches.append(list_reach(own, PROVISA, rules, kind.capability, kind.list_kind, at))
        from_idp = idp.get(role_id, {}).get(kind.list_kind)
        if from_idp:
            reaches.append(
                list_reach(tuple(from_idp), IDP, rules, kind.capability, kind.list_kind, at)
            )
    return tuple(oid for oid in sorted(objects) if any(r.reaches(objects[oid]) for r in reaches))


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
    objects = {pid: product_paths(pid, did) for pid, did in products}
    return _owned(lists, idp, expiries, role_ids, holds_owner, objects, at, PRODUCTS)


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


async def _role_members(conn: "Connection", role_ids: Collection[str]) -> dict[str, set[str]]:
    """role id -> the people holding a grant of it in force (``role_grants.in_force``)."""
    from provisa.auth.role_grants import in_force
    from provisa.core.schema_org import user_role_assignments as ura

    if not role_ids:
        return {}
    rows = await conn.execute_core(
        select(ura.c.role_id, ura.c.user_id).where(ura.c.role_id.in_(sorted(role_ids)), in_force())
    )
    out: dict[str, set[str]] = {}
    for r in rows.fetchall():
        out.setdefault(r.role_id, set()).add(r.user_id)
    return out


async def _holders_of(
    conn: "Connection", kind: OwnedKind, objects: Mapping[str, Paths]
) -> dict[str, tuple[str, ...]]:
    """object id -> the people holding ``kind``'s capability on it now, for ``objects`` spelled as
    given: a person's own row naming it, or a row an Owner or Steward field wrote on a role they
    hold a grant of (REQ-2219, amended 2026-11-23: a role named holds it for every holder)."""
    from provisa.entitlements.store import load_role_owner_rows

    capabilities = await _role_capabilities(conn)
    people: dict[str, _Person] = {}
    for period in history.current(await history.entitlement_periods(conn, kind=kind.list_kind)):
        row = period.row
        if row.principal is None:
            continue  # a role's own rows (source ``role``) are read below, for its holders
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
    role_rows = {
        role_id: [r for r in rows if r.capability == kind.capability and r.kind == kind.list_kind]
        for role_id, rows in (await load_role_owner_rows(conn)).items()
    }
    role_rows = {role_id: rows for role_id, rows in role_rows.items() if rows}
    for role_id, members in (await _role_members(conn, role_rows.keys())).items():
        for principal in members:
            person = people.setdefault(principal, _Person([], {}, set()))
            person.lists.extend(role_rows[role_id])
            person.roles.add(role_id)
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
        for oid in _owned(
            person.lists,
            person.idp,
            rules.get(principal, ()),
            sorted(person.roles),
            lambda role_id: kind.capability in capabilities.get(role_id, frozenset()),
            objects,
            now,
            kind,
        ):
            out.setdefault(oid, []).append(principal)
    return {oid: tuple(people_) for oid, people_ in out.items()}


async def holders(
    conn: "Connection", kind: OwnedKind, *, exact: bool = False
) -> dict[str, tuple[str, ...]]:
    """object id -> the people who hold ``kind``'s capability on it now (its owners, or a domain's
    stewards), by user id, sorted; an object no one holds is absent. What a display, an export,
    an approval and the steward gate read. ``exact``: only rows naming the object itself, not a
    domain above it (what its own Steward field shows). A role named in the field makes each
    person holding a grant of it a holder (REQ-2219, amended 2026-11-23)."""
    owned = await owned_objects(conn, kind)
    return await _holders_of(conn, kind, owned.exact if exact else owned.reach)


@dataclass(frozen=True)
class Named:
    """What an Owner or Steward field names on one object: the people and the roles whose
    Provisa-held rows of the kind's capability name it."""

    people: tuple[str, ...]
    roles: tuple[str, ...]


async def named(conn: "Connection", kind: OwnedKind, paths: Paths) -> Named:
    """What the field of the object spelled ``paths`` names now: each person with a Provisa-held
    row of ``kind``'s capability naming it, and each role with such a row of its own (REQ-2219,
    amended 2026-11-23). An identity provider's lists are its own, and not the field's."""
    from provisa.entitlements.lists import ROLE

    people: set[str] = set()
    roles: set[str] = set()
    for p in history.current(await history.entitlement_periods(conn, kind=kind.list_kind)):
        row = p.row
        if row.capability != kind.capability:
            continue
        if not any(matches(i, paths) for i in row.identifiers):
            continue
        if row.source == PROVISA and row.principal is not None:
            people.add(row.principal)
        elif row.source == ROLE and row.principal is None:
            roles.add(row.role_id)
    return Named(tuple(sorted(people)), tuple(sorted(roles)))


#: The kinds by the name a client gives them (object_owners' names, :data:`_OBJECT_KIND`).
KINDS_BY_NAME = {"data_product": PRODUCTS, "domain": DOMAINS, "relationship": RELATIONSHIPS}


async def field_of(conn: "Connection", kind: OwnedKind, object_id: str) -> Named:
    """What ``object_id``'s Owner (or Steward) field names now, for the form that edits it.
    Raises LookupError for a product that does not exist."""
    if kind is PRODUCTS:
        from provisa.core.repositories import data_product as data_product_repo

        stored = await data_product_repo.get(conn, object_id)
        if stored is None:
            raise LookupError(f"data product {object_id!r} not found")
        return await named(conn, kind, product_paths(object_id, stored["domain_id"]))
    if kind is DOMAINS:
        return await named(conn, kind, _domain_paths(object_id))
    identifier = await identifier_of(conn, kind, object_id)
    return await named(conn, kind, (_segments(identifier),))


async def product_owners(conn: "Connection") -> dict[str, tuple[str, ...]]:
    """product id -> the people who own it now, by user id, sorted."""
    return await holders(conn, PRODUCTS)


async def owners_of(conn: "Connection", product_id: str) -> tuple[str, ...]:
    """The people who own ``product_id`` now, by user id, sorted."""
    return (await product_owners(conn)).get(product_id, ())


async def unstewarded_domains(conn: "Connection") -> list[str]:
    """The REQ-609 steward gate: the domains no person stewards, by id, sorted. A domain is
    stewarded when a person's domain_steward row names it or a domain above it (REQ-1363); a
    system domain is exempt (REQ-2219, amended 2026-11-23) and a retired one is not listed."""
    from provisa.core.schema_org import domains
    from provisa.core.store_config import seeded_domains

    system = seeded_domains()
    stewarded = await holders(conn, DOMAINS)
    rows = await conn.execute_core(
        select(domains.c.id).where(domains.c.retired.is_(False), domains.c.id != "")
    )
    return sorted(r.id for r in rows.fetchall() if r.id not in system and r.id not in stewarded)


#: How object_owners names each kind (REQ-2219).
_OBJECT_KIND = {PRODUCTS: "data_product", DOMAINS: "domain", RELATIONSHIPS: "relationship"}


async def write_object_owners(conn: "Connection") -> None:
    """Replace object_owners with every object's holders now (:func:`holders`): what the meta
    views read of owners and stewards. Derived state: a model build and a saved Owner or Steward
    field are its writers."""
    from sqlalchemy import delete, insert

    from provisa.core.repositories.build_output import replacing_build_output
    from provisa.core.schema_org import object_owners

    rows = [
        {"object_kind": name, "object_id": oid, "principal": principal}
        for kind, name in _OBJECT_KIND.items()
        for oid, people in sorted((await holders(conn, kind)).items())
        for principal in people
    ]
    async with replacing_build_output(conn, object_owners):
        await conn.execute_core(delete(object_owners))
        if rows:
            await conn.execute_core(insert(object_owners).values(rows))


class OwnerRefused(ValueError):
    """A person named in an Owner or Steward field who holds no role grant carrying the kind's
    capability: a row on any other role would make them no holder."""

    def __init__(self, principal: str, kind: OwnedKind = PRODUCTS) -> None:
        super().__init__(
            f"{principal!r} holds no role with the {kind.capability} capability, so cannot be "
            f"named a {kind.noun}'s {kind.holder}; grant them such a role ({kind.capability}) "
            "first"
        )
        self.principal = principal
        self.kind = kind
        self.code = kind.refusal_code


class OwnerRoleRefused(ValueError):
    """A role named in an Owner or Steward field that does not hold the kind's capability (or is
    no role): its row would make none of its holders a holder (REQ-2219, amended 2026-11-23)."""

    def __init__(self, role_id: str, kind: OwnedKind = PRODUCTS) -> None:
        super().__init__(
            f"role {role_id!r} does not hold the {kind.capability} capability, so cannot be "
            f"named a {kind.noun}'s {kind.holder}; give the role that capability first"
        )
        self.role_id = role_id
        self.kind = kind
        self.code = f"{kind.noun.replace(' ', '_')}.{kind.holder}_role_not_eligible"


class OwnerChangeRefused(PermissionError):
    """An Owner or Steward field changed by someone who is neither an org admin (org_settings)
    nor a current holder of that object (REQ-2219, amended 2026-11-23: editing an object's other
    fields does not by itself let a person change its owners)."""

    def __init__(self, object_id: str, kind: OwnedKind = PRODUCTS) -> None:
        super().__init__(
            f"only an org admin or a current {kind.holder} of {kind.noun} {object_id!r} may change "
            f"its {kind.holder}s"
        )
        self.object_id = object_id
        self.kind = kind
        self.code = f"{kind.noun.replace(' ', '_')}.{kind.holder}s_change_refused"


@dataclass(frozen=True)
class Changer:
    """Who saves an Owner or Steward field: their user id (None when no person is bound), and
    whether they hold org_settings, as the surface's own capability gate reads it."""

    user_id: str | None
    org_admin: bool


async def _eligible_roles(
    conn: "Connection",
    principal: str,
    capabilities: Mapping[str, frozenset[str]],
    capability: str,
) -> list[str]:
    from provisa.auth.role_grants import held_grants

    return sorted(
        {
            g.role_id
            for g in await held_grants(conn, principal)
            if capability in capabilities.get(g.role_id, frozenset())
        }
    )


async def _set_role_holders(
    conn: "Connection",
    kind: OwnedKind,
    object_id: str,
    identifier: str,
    paths: Paths,
    roles: tuple[str, ...],
    capabilities: Mapping[str, frozenset[str]],
    *,
    set_by: str | None,
) -> None:
    """Make ``roles`` the roles whose own rows of ``kind``'s capability name ``object_id``."""
    from provisa.entitlements.lists import ROLE
    from provisa.entitlements.store import remove_role_list, set_role_list

    held = {
        p.row.role_id: tuple(p.row.identifiers)
        for p in history.current(
            await history.entitlement_periods(conn, source=ROLE, kind=kind.list_kind)
        )
        if p.row.capability == kind.capability
    }
    reason_set = f"named a {kind.holder} of {kind.noun} {object_id}"
    reason_end = f"no longer a {kind.holder} of {kind.noun} {object_id}"
    for role_id, identifiers in held.items():
        if role_id in roles or not any(matches(i, paths) for i in identifiers):
            continue
        kept = [i for i in identifiers if not matches(i, paths)]
        if kept:
            await set_role_list(
                conn,
                role_id,
                kind.list_kind,
                kept,
                capability=kind.capability,
                set_by=set_by,
                reason=reason_end,
            )
        else:
            await remove_role_list(
                conn,
                role_id,
                kind.list_kind,
                capability=kind.capability,
                set_by=set_by,
                reason=reason_end,
            )
    for role_id in roles:
        identifiers = held.get(role_id, ())
        if any(matches(i, paths) for i in identifiers):
            continue
        await set_role_list(
            conn,
            role_id,
            kind.list_kind,
            [*identifiers, identifier],
            capability=kind.capability,
            set_by=set_by,
            reason=reason_set,
        )


async def set_holders(
    conn: "Connection",
    kind: OwnedKind,
    object_id: str,
    identifier: str,
    paths: Paths,
    people: Iterable[str],
    *,
    roles: Iterable[str] | None = None,
    set_by: str | None,
) -> None:
    """Make ``people`` (user ids) the people Provisa holds as ``object_id``'s holders of
    ``kind``: a row naming it (``identifier``) on each of a named person's role grants holding
    the capability, and the object (spelled ``paths``) ended from every Provisa-held row of anyone
    else. ``roles``, when given, become the roles whose own rows name it, so every holder of each
    holds it (REQ-2219, amended 2026-11-23); None leaves the roles' rows as they are. An identity
    provider's lists are its own and are not written. Refused (:class:`OwnerRefused`,
    :class:`OwnerRoleRefused`) before anything is written when a named person holds no such grant
    or a named role does not hold the capability. The caller holds the transaction."""
    from provisa.entitlements.store import remove_list, set_list

    wanted = tuple(dict.fromkeys(people))
    wanted_roles = None if roles is None else tuple(dict.fromkeys(roles))
    capabilities = await _role_capabilities(conn)
    eligible = {p: await _eligible_roles(conn, p, capabilities, kind.capability) for p in wanted}
    for principal, role_ids in eligible.items():
        if not role_ids:
            raise OwnerRefused(principal, kind)
    for role_id in wanted_roles or ():
        if kind.capability not in capabilities.get(role_id, frozenset()):
            raise OwnerRoleRefused(role_id, kind)
    held = {
        (p.row.principal, p.row.role_id): tuple(p.row.identifiers)
        for p in history.current(
            await history.entitlement_periods(conn, source=PROVISA, kind=kind.list_kind)
        )
        if p.row.capability == kind.capability
    }
    reason_set = f"named a {kind.holder} of {kind.noun} {object_id}"
    reason_end = f"no longer a {kind.holder} of {kind.noun} {object_id}"
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
                kind.list_kind,
                kept,
                set_by=set_by,
                reason=reason_end,
                capability=kind.capability,
            )
        else:
            await remove_list(
                conn,
                principal,
                role_id,
                kind.list_kind,
                set_by=set_by,
                reason=reason_end,
                capability=kind.capability,
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
                kind.list_kind,
                [*identifiers, identifier],
                set_by=set_by,
                reason=reason_set,
                capability=kind.capability,
            )
    if wanted_roles is not None:
        await _set_role_holders(
            conn, kind, object_id, identifier, paths, wanted_roles, capabilities, set_by=set_by
        )


async def set_product_owners(
    conn: "Connection",
    product_id: str,
    domain_id: str,
    owners: Iterable[str],
    *,
    roles: Iterable[str] | None = None,
    set_by: str | None,
) -> None:
    """:func:`set_holders` for a product's owners."""
    await set_holders(
        conn,
        PRODUCTS,
        product_id,
        product_identifier(product_id, domain_id),
        product_paths(product_id, domain_id),
        owners,
        roles=roles,
        set_by=set_by,
    )


def _domain_paths(domain_id: str) -> Paths:
    return ((domain_id.lower(),), (domain_identifier(domain_id).lower(),))


async def set_domain_stewards(
    conn: "Connection",
    domain_id: str,
    stewards: Iterable[str],
    *,
    roles: Iterable[str] | None = None,
    set_by: str | None,
) -> set[str]:
    """Write a domain's Steward field: ``stewards`` become the people Provisa holds as its
    stewards (and ``roles``, when given, the roles). Returns the people whose rows it wrote,
    before and after, for :func:`reload_owner_rows`."""
    before = set((await holders(conn, DOMAINS, exact=True)).get(domain_id, ()))
    wanted = list(stewards)
    await set_holders(
        conn,
        DOMAINS,
        domain_id,
        domain_identifier(domain_id),
        _domain_paths(domain_id),
        wanted,
        roles=roles,
        set_by=set_by,
    )
    after = set((await holders(conn, DOMAINS, exact=True)).get(domain_id, ()))
    return before | set(wanted) | after


async def set_relationship_owners(
    conn: "Connection",
    relationship_id: str,
    owners: Iterable[str],
    *,
    roles: Iterable[str] | None = None,
    set_by: str | None,
) -> set[str]:
    """Write a relationship's Owner field: ``owners`` become the people Provisa holds as its
    owners (and ``roles``, when given, the roles). Returns the people whose rows it wrote, before
    and after."""
    before = set((await holders(conn, RELATIONSHIPS, exact=True)).get(relationship_id, ()))
    wanted = list(owners)
    identifier = await identifier_of(conn, RELATIONSHIPS, relationship_id)
    await set_holders(
        conn,
        RELATIONSHIPS,
        relationship_id,
        identifier,
        (_segments(identifier),),
        wanted,
        roles=roles,
        set_by=set_by,
    )
    after = set((await holders(conn, RELATIONSHIPS, exact=True)).get(relationship_id, ()))
    return before | set(wanted) | after


async def _require_may_change(
    conn: "Connection",
    kind: OwnedKind,
    object_id: str,
    reach: Paths,
    before: Named,
    people: Iterable[str] | None,
    roles: Iterable[str] | None,
    changer: Changer,
) -> None:
    """Refuse (:class:`OwnerChangeRefused`) a field whose people or roles differ from what it
    names (``before``) when ``changer`` is neither an org admin nor a current holder of the object
    (spelled ``reach``): REQ-2219, amended 2026-11-23, the same for products, domains and
    relationships. A field left as it is needs neither."""
    same_people = people is None or set(people) == set(before.people)
    same_roles = roles is None or set(roles) == set(before.roles)
    if (same_people and same_roles) or changer.org_admin:
        return
    current = (await _holders_of(conn, kind, {object_id: reach})).get(object_id, ())
    if changer.user_id is not None and changer.user_id in current:
        return
    raise OwnerChangeRefused(object_id, kind)


async def save_owner_field(
    conn: "Connection",
    product_id: str,
    domain_id: str,
    owners: Iterable[str] | None,
    *,
    roles: Iterable[str] | None = None,
    stored_domain_id: str | None,
    changer: Changer,
    set_by: str | None,
) -> set[str]:
    """Write a product's Owner field as a save does, every surface alike (GraphQL, MCP):
    ``owners`` (people) and ``roles``, each kept as named when None. Only an org admin or a current
    owner changes it (:class:`OwnerChangeRefused`). A product moved from ``stored_domain_id``
    keeps its owners, named under its new domain. Returns the people whose rows it wrote, for
    :func:`reload_owner_rows`; nothing is written when the field is unchanged and the product
    stays."""
    moved = stored_domain_id is not None and stored_domain_id != domain_id
    held_at = product_paths(product_id, stored_domain_id or domain_id)
    before = await named(conn, PRODUCTS, held_at) if stored_domain_id is not None else Named((), ())
    await _require_may_change(conn, PRODUCTS, product_id, held_at, before, owners, roles, changer)
    if owners is None and roles is None and not moved:
        return set()
    held_before = set(
        (await _holders_of(conn, PRODUCTS, {product_id: held_at})).get(product_id, ())
    )
    wanted = list(before.people) if owners is None else list(owners)
    wanted_roles = list(before.roles) if roles is None else list(roles)
    if stored_domain_id is not None and moved:
        # The rows name the product under its old domain: end them all, then name it anew.
        await set_product_owners(conn, product_id, stored_domain_id, [], roles=[], set_by=set_by)
    await set_product_owners(conn, product_id, domain_id, wanted, roles=wanted_roles, set_by=set_by)
    held_after = set(
        (await _holders_of(conn, PRODUCTS, {product_id: product_paths(product_id, domain_id)})).get(
            product_id, ()
        )
    )
    return held_before | set(wanted) | held_after


async def save_domain_stewards(
    conn: "Connection",
    domain_id: str,
    stewards: Iterable[str] | None,
    *,
    roles: Iterable[str] | None = None,
    changer: Changer,
    set_by: str | None,
) -> set[str]:
    """Write a domain's Steward field as a save does: only an org admin or a current steward of
    the domain changes it (:class:`OwnerChangeRefused`). People or roles left None are kept."""
    exact = _domain_paths(domain_id)
    before = await named(conn, DOMAINS, exact)
    owned = await owned_objects(conn, DOMAINS)
    reach = owned.reach.get(domain_id, exact)
    await _require_may_change(conn, DOMAINS, domain_id, reach, before, stewards, roles, changer)
    if stewards is None and roles is None:
        return set()
    return await set_domain_stewards(
        conn,
        domain_id,
        list(before.people) if stewards is None else stewards,
        roles=roles,
        set_by=set_by,
    )


async def save_relationship_owners(
    conn: "Connection",
    relationship_id: str,
    owners: Iterable[str] | None,
    *,
    roles: Iterable[str] | None = None,
    changer: Changer,
    set_by: str | None,
) -> set[str]:
    """Write a relationship's Owner field as a save does: only an org admin or a current owner of
    the relationship changes it (:class:`OwnerChangeRefused`). People or roles left None are
    kept."""
    if owners is None and roles is None:
        return set()
    identifier = await identifier_of(conn, RELATIONSHIPS, relationship_id)
    paths = (_segments(identifier),)
    before = await named(conn, RELATIONSHIPS, paths)
    await _require_may_change(
        conn, RELATIONSHIPS, relationship_id, paths, before, owners, roles, changer
    )
    return await set_relationship_owners(
        conn,
        relationship_id,
        list(before.people) if owners is None else owners,
        roles=roles,
        set_by=set_by,
    )


async def end_product_owners(
    conn: "Connection", product_id: str, domain_id: str, *, set_by: str | None
) -> set[str]:
    """End every Provisa-held owner row naming a product being deleted, a role's among them: no
    row is left naming a product that is gone. Call before the product's row is deleted; returns
    its owners."""
    ended = set(await owners_of(conn, product_id))
    await set_product_owners(conn, product_id, domain_id, [], roles=[], set_by=set_by)
    return ended


async def reload_owner_rows(conn: "Connection", state: Any, people: Iterable[str]) -> None:
    """What the runtime holds of the entitlement store after an Owner or Steward field wrote to
    it, with the products a row can name (a product just created among them), and each person's
    effective rows recomputed, as every other write of a person's rows does."""
    from provisa.core.repositories import data_product as data_product_repo
    from provisa.entitlements import effective_store, store

    state.held_entitlements = await store.load_held(conn)
    state.role_owner_rows = await store.load_role_owner_rows(conn)
    state.data_product_refs = await data_product_repo.refs(conn)
    for principal in sorted(set(people)):
        # Each rewrites object_owners from the store as well (effective_store.write_person).
        await effective_store.write_person(conn, state, principal)
