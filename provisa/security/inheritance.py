# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e7d2a-6c1f-4e8b-a5d3-7f0c2e9b1a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Role inheritance, materialized once per runtime build (REQ-1677, REQ-2155).

A role names its parents, in order (``parents``). Its chain is the role itself followed by every
ancestor, walked depth-first in declared parent order, each ancestor once at its first place.
Everything downstream stays keyed by the acting role id, because the chain is folded into the
data here rather than consulted at every lookup:

- grant lists (``visible_to``) gain the child wherever they name an ancestor —
  a grant is a union up the chain (a column's mask list is read against the chain itself,
  security/masking.resolve, REQ-2059);
- RLS resolves per table so a role reads no row its parents could not (REQ-1677): its own rule (its
  table rule before its domain rule) AND the rules its parents resolve ORed, a parent with no rule
  standing for every row. A role with no rule of its own reads under its parents' rules;
- a role holds what it is given, narrowed by its parents (:func:`effective_rights`, REQ-2155): its
  capabilities, domains and regions are the given ones intersected with the union of what its
  parents hold. A role with no parents holds what it is given. A standard role is the palette: its
  capabilities add to its ancestors' and its reach is its own. A role with no capabilities holds no
  domain: it reads nothing, and is a name, session values, a command's assignee or a building block
  of grants. Role dicts gain the nearest ancestor's rate_limit when the role sets none.

The functions mutate the dicts they are given, which are the runtime build's own copies, never
the rows the admin surfaces read back — an administrator sees the grants they made, not the
materialized ones.
"""

# Requirements: REQ-215, REQ-1677

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from typing import Any

from provisa.core.column_scope import SCOPE_OWN_DOMAIN, SCOPE_PUBLIC
from provisa.security.rights import (
    ALL_DOMAINS,
    DEFAULT_TIER_META_TABLES,
    GOVERNANCE_META_COLUMNS,
    META_DOMAIN_ID,
    SYSTEM_DOMAIN_CAPABILITIES,
    SYSTEM_DOMAINS,
    VIEWER_ROLE,
    Capability,
    normalized_reach,
    reaches_domain,
    reads_data,
)

GRANT_LISTS = ("visible_to",)

#: Joins the roles of a composite grant entry (REQ-2058): ``analyst+region__eu`` is held by a
#: reader holding ALL of them. Across a grant list's entries the reader needs ANY one.
COMPOSITE = "+"


def grant_parts(entry: str) -> frozenset[str]:
    """The roles a grant entry asks a reader to hold, all of them: one for a plain entry."""
    return frozenset(entry.split(COMPOSITE))


def holds_entry(entry: str, chain: Iterable[str]) -> bool:
    """Whether a reader whose roles (each with its ancestors) are ``chain`` holds ``entry``."""
    return grant_parts(entry) <= set(chain)


def parent_map(roles: Iterable[dict]) -> dict[str, list[str]]:
    """role id → its parents' ids, in declared order (empty for a role with none)."""
    return {r["id"]: list(r.get("parents") or []) for r in roles}


def role_chain(role_id: str, parents: dict[str, list[str]]) -> list[str]:
    """The role and every ancestor, depth-first in declared parent order, each once at its first
    place. Raises ValueError on a cycle."""
    chain: list[str] = []

    def walk(current: str, path: list[str]) -> None:
        if current in path:
            raise ValueError(
                f"role inheritance cycle at {current!r}: {' -> '.join([*path, current])}"
            )
        if current not in chain:
            chain.append(current)
        for parent in parents.get(current, []):
            walk(parent, [*path, current])

    walk(role_id, [])
    return chain


def role_chains(roles: Iterable[dict]) -> dict[str, list[str]]:
    parents = parent_map(roles)
    return {rid: role_chain(rid, parents) for rid in parents}


def parents_problem(role_id: str, proposed: list[str], parents: dict[str, list[str]]) -> str | None:
    """Why ``proposed`` cannot be the parents of ``role_id``, or None when they can.

    ``parents`` is the current map; the proposed edges are checked against it, so a cycle through
    the roles that already exist is caught before the write.
    """
    seen: set[str] = set()
    for parent_id in proposed:
        if parent_id == role_id:
            return f"Role {role_id!r} cannot inherit from itself"
        if parent_id in seen:
            return f"Role {role_id!r} names parent {parent_id!r} twice"
        seen.add(parent_id)
        if parent_id not in parents:
            return f"Parent role {parent_id!r} not found"
    proposed_map = {**parents, role_id: list(proposed)}
    for parent_id in proposed:
        try:
            role_chain(parent_id, proposed_map)
        except ValueError:
            return f"Role {role_id!r} cannot inherit from {parent_id!r}: that would close a cycle"
    return None


def children_of(role_id: str, roles: Iterable[dict]) -> list[str]:
    return sorted(r["id"] for r in roles if role_id in (r.get("parents") or []))


def _standard_ids() -> Collection[str]:
    from provisa.core.db import STANDARD_ROLES

    return STANDARD_ROLES


def narrowed(given: Iterable[str], held: Iterable[Iterable[str]]) -> set[str]:
    """``given`` intersected with the union of ``held`` (REQ-2155), ``"*"`` being every data
    domain: a parent holding it holds whatever is given, and given it is what the parents hold."""
    mine = set(given)
    theirs: set[str] = set().union(*held)
    if ALL_DOMAINS in theirs:
        return mine
    return set(theirs) if ALL_DOMAINS in mine else mine & theirs


def effective_rights(
    role_id: str,
    rows: Mapping[str, dict],
    read: Callable[[dict], Iterable[str]],
    *,
    adds: Collection[str] = (),
    memo: dict[str, set[str]] | None = None,
    path: tuple[str, ...] = (),
) -> set[str]:
    """What ``role_id`` holds of the rights ``read`` takes off a role row: THE rule, for
    capabilities, domains and regions alike (REQ-2155). A role with parents holds what it is given
    intersected with the union of what they hold, so a parent's narrowing reaches every descendant;
    a role with no parents holds what it is given. A role in ``adds`` holds what it is given on top
    of its parents' (a standard role's capabilities, listed where each first enters). Raises
    KeyError for a role or parent ``rows`` does not hold, ValueError on a cycle."""
    memo = {} if memo is None else memo
    if role_id in memo:
        return memo[role_id]
    if role_id in path:
        raise ValueError(f"role inheritance cycle at {role_id!r}: {' -> '.join(path)}")
    parents = rows[role_id].get("parents") or []
    held = [
        effective_rights(p, rows, read, adds=adds, memo=memo, path=(*path, role_id))
        for p in parents
    ]
    given = set(read(rows[role_id]))
    if role_id in adds:
        memo[role_id] = given.union(*held)
    else:
        memo[role_id] = narrowed(given, held) if parents else given
    return memo[role_id]


def _capabilities(row: dict) -> list[str]:
    return row.get("capabilities") or []


def held_capabilities(
    role_ids: Iterable[str], rows: Mapping[str, dict], adds: Collection[str]
) -> dict[str, set[str]]:
    """What each of ``role_ids`` and its ancestors hold (:func:`effective_rights`). RW on data
    products from an Owner field (row_filters.with_owner_grant, which marks each role it reaches)
    is the field's, not a parent's, so it stands on what the role holds (REQ-2188)."""
    memo: dict[str, set[str]] = {}
    for rid in role_ids:
        effective_rights(rid, rows, _capabilities, adds=adds, memo=memo)
    return {
        rid: held | {Capability.DATA_PRODUCT_RW.value} if rows[rid].get("owner_held") else held
        for rid, held in memo.items()
    }


def _data_domains(row: dict) -> set[str]:
    return set(row.get("domain_access") or []) - SYSTEM_DOMAINS


def held_rights(
    role_ids: Iterable[str], rows: Mapping[str, dict]
) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, set[str]]]:
    """Each of ``role_ids``' capabilities, data domains and regions (:func:`effective_rights`), and
    its ancestors'. A standard role's capabilities add to its parents' and its reach is its own. A
    role holding no capability holds no domain, and one without data_residency no region."""
    standard = _standard_ids()
    ids = list(role_ids)
    capabilities = held_capabilities(ids, rows, standard)
    own_standing = {rid: {**r, "parents": []} if rid in standard else r for rid, r in rows.items()}

    def domains(row: dict) -> set[str]:
        return _data_domains(row) if capabilities[row["id"]] else set()

    def regions(row: dict) -> list[str]:
        holds = Capability.DATA_RESIDENCY.value in capabilities[row["id"]]
        return (row.get("residency_values") or []) if holds else []

    held_domains: dict[str, set[str]] = {}
    held_regions: dict[str, set[str]] = {}
    for rid in ids:
        effective_rights(rid, own_standing, domains, memo=held_domains)
        effective_rights(rid, own_standing, regions, memo=held_regions)
    return capabilities, held_domains, held_regions


def flatten_role_dicts(roles: list[dict]) -> list[dict]:
    """Each role with what it holds (:func:`effective_rights`): its capabilities, its data domains
    (REQ-2155) with each system domain whose capability it holds (REQ-1132: a stored one is no
    access), and data_residency's regions, narrowed as they are; and each property (rate_limit,
    max_rows) set by the nearest role in the chain that declares it, the role itself first, so a
    child may raise or lower its parent's (REQ-2057). Session values (REQ-1682) merge by name, the
    nearest role's value for each. A role with no capabilities holds no domain: it reads nothing. A role
    holding only command_execution keeps its domains for its commands and reads nothing (REQ-870).
    A domain tree is folded in first (domain_tree.flatten_role_domains), as a parent listing a
    business unit holds the domains under it. Input dicts are not modified."""
    by_id = {r["id"]: r for r in roles}
    chains = role_chains(roles)
    held_caps, held_domains, held_regions = held_rights(by_id, by_id)
    out: list[dict] = []
    for r in roles:
        caps = held_caps[r["id"]]
        rate_limit = r.get("rate_limit")
        max_rows = r.get("max_rows")
        session_vars: dict[str, str] = {}
        for rid in chains[r["id"]]:
            anc = by_id[rid]
            if rate_limit is None:
                rate_limit = anc.get("rate_limit")
            if max_rows is None:
                max_rows = anc.get("max_rows")
            # REQ-1682: walked nearest first, so a name already set is the nearer role's.
            for name, value in (anc.get("session_vars") or {}).items():
                session_vars.setdefault(name, value)
        flattened = {
            **r,
            "capabilities": sorted(caps),
            "domain_access": normalized_reach(
                held_domains[r["id"]]
                | {d for d, cap in SYSTEM_DOMAIN_CAPABILITIES.items() if cap in caps}
            ),
            "rate_limit": rate_limit,
        }
        if "max_rows" in r:
            flattened["max_rows"] = max_rows
        if "session_vars" in r:
            flattened["session_vars"] = session_vars
        if "residency_values" in r:
            flattened["residency_values"] = sorted(held_regions[r["id"]])
        out.append(flattened)
    return out


def effective_capabilities(role_id: str, roles: Iterable[dict]) -> list[str]:
    """What ``role_id`` holds (:func:`effective_rights`) -- the list the built registry carries
    for it. ``roles`` are rows with ``id``, ``capabilities``, ``parents``. Raises KeyError for a
    role (or a parent) the rows do not contain."""
    by_id = {r["id"]: r for r in roles}
    return sorted(held_capabilities([role_id], by_id, _standard_ids())[role_id])


def held_by_role(
    rows: Iterable[dict], domain_parents: dict[str, str | None]
) -> dict[str, dict[str, list[str]]]:
    """role id -> what it holds: its ``capabilities``, its data ``domains`` down the domain tree and
    its ``regions`` -- what the form offers a role's children, by the one rule."""
    from provisa.security import domain_tree

    flat = flatten_role_dicts(domain_tree.flatten_role_domains(list(rows), domain_parents))
    return {
        r["id"]: {
            "capabilities": r["capabilities"],
            "domains": sorted(set(r["domain_access"]) - SYSTEM_DOMAINS),
            "regions": r.get("residency_values") or [],
        }
        for r in flat
    }


def right_not_in_parents(
    role_id: str,
    parents: list[str],
    capabilities: Iterable[str],
    domain_access: Iterable[str],
    residency_values: Iterable[str],
    roles: Iterable[dict],
    domain_parents: dict[str, str | None],
) -> tuple[str, str, str] | None:
    """The first right a definition gives that no parent holds, as (kind, right, why), or None
    (REQ-2155). ``roles`` are stored rows with the standard roles among them; ``domain_parents`` is
    the domain tree, as a parent listing a business unit holds the domains under it. A standard
    role, or one with no parents, narrows nothing."""
    from provisa.security import domain_tree

    if not parents or role_id in _standard_ids():
        return None
    rows = {
        r["id"]: {**r, "domain_access": domain_tree.expand(_data_domains(r), domain_parents)}
        for r in roles
    }
    held_caps, held_domains, held_regions = held_rights(parents, rows)

    def by_parents(held: dict[str, set[str]]) -> set[str]:
        return set().union(*(held[p] for p in parents))

    named = ", ".join(parents)
    for capability in sorted(set(capabilities) - by_parents(held_caps)):
        return ("capability", capability, f"no parent holds it ({named})")
    reached = by_parents(held_domains)
    if ALL_DOMAINS not in reached:
        for domain in sorted((set(domain_access) - SYSTEM_DOMAINS) - reached):
            return ("domain", domain, f"no parent reaches it ({named})")
    for region in sorted(set(residency_values) - by_parents(held_regions)):
        return ("region", region, f"no parent covers it ({named})")
    return None


def expand_grants(items: Iterable[dict], chains: dict[str, list[str]]) -> None:
    """Add each role to every grant list on ``items`` that names one of its ancestors, or a
    composite entry whose every role is in its chain (REQ-2058)."""
    for item in items:
        for key in GRANT_LISTS:
            granted = item.get(key)
            if not granted:
                continue
            present = set(granted)
            composites = [e for e in granted if COMPOSITE in e]
            for rid, chain in chains.items():
                if rid in present:
                    continue
                if any(anc in present for anc in chain[1:]) or any(
                    holds_entry(e, chain) for e in composites
                ):
                    granted.append(rid)
                    present.add(rid)


def expand_column_grants(tables: Iterable[dict], chains: dict[str, list[str]]) -> None:
    for t in tables:
        expand_grants(t.get("columns") or [], chains)


#: A role's own ``domain_access``, as stored: before the domain tree folds the domains under each
#: listed one into it (REQ-1363). What an own-domain column is served by (REQ-2153).
OWN_DOMAIN_ACCESS = "own_domain_access"


def with_own_domains(roles: Iterable[dict]) -> list[dict]:
    """Each role with its stored ``domain_access`` kept as :data:`OWN_DOMAIN_ACCESS`, which the
    domain tree's flattening and inheritance carry through untouched. Applied to the stored rows,
    before the tree. Input dicts are not modified."""
    return [{**r, OWN_DOMAIN_ACCESS: list(r["domain_access"])} for r in roles]


def narrow_own_domain_grants(tables: Iterable[dict], roles: Iterable[dict]) -> None:
    """Take out of each own-domain column's grant every entry naming a role whose own domain access
    (:func:`with_own_domains`) does not name the table's domain, ``*`` naming every domain
    (REQ-2153). A role reaching the domain only through an ancestor is served the column no
    longer, though its grant names it. Run after the grants are folded down the role chains
    (:func:`expand_column_grants`), so a descendant granted through an ancestor is narrowed by its
    own list. An entry naming a role ``roles`` does not hold reaches no reader and goes too. On the
    build's own copies."""
    own: dict[str, list[str]] | None = None
    for t in tables:
        domain_id = t["domain_id"]
        for column in t.get("columns") or []:
            if column.get("scope") != SCOPE_OWN_DOMAIN:
                continue
            if own is None:  # read only when an own-domain column is met
                own = {r["id"]: r[OWN_DOMAIN_ACCESS] for r in roles}
            column["visible_to"] = [
                e
                for e in column.get("visible_to") or []
                if all(
                    role in own and reaches_domain(own[role], domain_id) for role in grant_parts(e)
                )
            ]


def expand_meta_grants(tables: Iterable[dict], roles: Iterable[dict]) -> None:
    """Meta's columns are granted to Viewer, and what reads them is a capability, not an ancestor
    (REQ-1132, REQ-2154): a role holding catalog_read is given each such grant, and on the tables
    every role reads with no meta grant (rights.DEFAULT_TIER_META_TABLES) every role that holds a
    capability is given each core column, served as a public column is, outside its reach. A column
    whose grant to Viewer was taken away stays as it was. On the build's own copies."""
    held = [r for r in roles if reads_data(r.get("capabilities"))]  # REQ-870
    catalog = [r["id"] for r in held if Capability.CATALOG_READ.value in r["capabilities"]]
    every = [r["id"] for r in held]
    for t in tables:
        if t.get("domain_id") != META_DOMAIN_ID:
            continue
        for column in t.get("columns") or []:
            granted = column.get("visible_to") or []
            if VIEWER_ROLE not in granted:
                continue
            core = (
                t.get("table_name") in DEFAULT_TIER_META_TABLES
                and column["column_name"] not in GOVERNANCE_META_COLUMNS
            )
            column["visible_to"] = list(dict.fromkeys([*granted, *(every if core else catalog)]))
            if core:
                column["scope"] = SCOPE_PUBLIC


def _resolved(
    role_id: str,
    parents: dict[str, list[str]],
    own: Callable[[str], dict | None],
    memo: dict[str, list[tuple[str, dict]]],
    path: tuple[str, ...] = (),
) -> list[tuple[str, dict]]:
    """What ``role_id`` resolves for one key, as (the role it is ``own`` to, that thing): its own
    when it has one, else what each parent resolves, in declared order, each once. A parent that
    resolves nothing contributes nothing (REQ-2155); with one parent this is the nearest role in
    the chain that has one."""
    if role_id in memo:
        return memo[role_id]
    if role_id in path:
        raise ValueError(f"role inheritance cycle at {role_id!r}: {' -> '.join(path)}")
    mine = own(role_id)
    found: list[tuple[str, dict]] = []
    if mine is not None:
        found = [(role_id, mine)]
    else:
        for parent in parents.get(role_id, []):
            for source, item in _resolved(parent, parents, own, memo, (*path, role_id)):
                if all(item is not seen for _s, seen in found):
                    found.append((source, item))
    memo[role_id] = found
    return found


def _ored(exprs: Iterable[Any]) -> Any:
    """The OR of the predicates given, each once; None stands for no predicate and contributes
    nothing (REQ-2155). One predicate is returned as it is. A predicate given as text is a rule
    entering (a stored grant's), read once here; each is an expression (REQ-913)."""
    from provisa.compiler.rls import any_of, as_rule

    return any_of(as_rule(e) for e in exprs if e is not None)


def expand_write_grants(tables: Iterable[dict], parents: dict[str, list[str]]) -> None:  # REQ-2074
    """Give each role, on every table and operation it holds no write grant for, the grant its
    parents resolve -- the child's own first, as a row filter is resolved (:func:`materialize_rls`).
    Several parents' grants combine: their columns are unioned, their row and check conditions
    ORed (a grant with none contributes none), and the presets are the first parent's."""
    for t in tables:
        grants = t.get("write_grants") or []
        if not grants:
            continue
        held = {(g["role"], g["operation"]): g for g in grants}
        for operation in ("insert", "update", "delete"):
            memo: dict[str, list[tuple[str, dict]]] = {}

            def own(rid: str, operation: str = operation) -> dict | None:
                return held.get((rid, operation))

            for rid in parents:
                if (rid, operation) in held:
                    continue
                found = [g for _source, g in _resolved(rid, parents, own, memo)]
                if not found:
                    continue
                first = found[0]
                inherited = {
                    **first,
                    "role": rid,
                    "columns": list(dict.fromkeys(c for g in found for c in g["columns"])),
                    "rows": _ored(g.get("rows") for g in found),
                    "check": _ored(g.get("check") for g in found),
                }
                grants.append(inherited)


def holds_grant(role_id: str, granted: Iterable[str], chains: dict[str, list[str]]) -> bool:
    """Whether ``role_id`` or an ancestor is in ``granted``, or ``role_id``'s chain holds every
    role of a composite entry in it (REQ-2058)."""
    chain = chains.get(role_id, [role_id])
    present = set(granted)
    return any(rid in present for rid in chain) or any(
        holds_entry(e, chain) for e in present if COMPOSITE in e
    )


def _anded(left: Any, right: Any) -> Any:
    from provisa.compiler.rls import all_of

    return all_of([left, right])


def materialize_rls(
    rls_rules: list[dict],
    tables: Iterable[dict],
    parents: dict[str, list[str]],
    actions: Iterable[dict] = (),
) -> list[dict]:
    """The rule list a role's rows resolve to, as table-level rules per role (REQ-1677).

    For every role with parents and every table: the role's own rule (its table rule, else its
    domain rule for the table's domain) AND the OR of what its parents resolve, the same way. A
    parent with no rule stands for every row, so it lifts the OR; a role with no rule of its own
    reads under its parents' rules; only where nothing narrows is a role unrestricted. So a role
    reads no row its parents could not, in one plain predicate per right, role and target. Roles
    without parents, and every rule its role's parents do not narrow, pass through unchanged, so a
    role's own domain rule still covers a table registered after this build the way it did before.
    """
    from provisa.compiler.rls import read_rule_row

    # REQ-913: each stored rule is read once, here, where it enters; every rule from here on is
    # an expression.
    rls_rules = [read_rule_row(r) for r in rls_rules]
    table_rules: dict[tuple[str, int], dict] = {}
    domain_rules: dict[tuple[str, str], dict] = {}
    action_rules: dict[tuple[str, str], dict] = {}
    for rule in rls_rules:
        if rule.get("action_name"):  # REQ-1679
            action_rules[(rule["role_id"], rule["action_name"])] = rule
        elif rule.get("table_id") is not None:
            table_rules[(rule["role_id"], rule["table_id"])] = rule
        elif rule.get("domain_id"):
            domain_rules[(rule["role_id"], rule["domain_id"])] = rule

    out = list(rls_rules)

    def rows_of(
        rid: str,
        own: Callable[[str], dict | None],
        memo: dict[str, tuple[Any, list[tuple[str, dict]]]],
        path: tuple[str, ...] = (),
    ) -> tuple[Any, list[tuple[str, dict]]]:
        """``rid``'s predicate, None for every row, and the (role, rule) it was made from."""
        if rid in memo:
            return memo[rid]
        if rid in path:
            raise ValueError(f"role inheritance cycle at {rid!r}: {' -> '.join(path)}")
        above = [rows_of(p, own, memo, (*path, rid)) for p in parents.get(rid, [])]
        lifted = not above or any(expr is None for expr, _ in above)
        expr = None if lifted else _ored(e for e, _ in above)
        sources = [] if lifted else [s for _, made in above for s in made]
        mine = own(rid)
        if mine is not None:
            mine_expr = mine["filter_expr"]
            expr = mine_expr if expr is None else _anded(mine_expr, expr)
            sources = [(rid, mine), *sources]
        memo[rid] = (expr, sources)
        return memo[rid]

    def resolve(
        rid: str, own: Callable[[str], dict | None], memo: dict, stored: dict | None, **target
    ) -> None:
        expr, sources = rows_of(rid, own, memo)
        mine = own(rid)
        if expr is None or (mine is not None and expr == mine["filter_expr"]):
            return
        if stored is not None:
            out.remove(stored)
        named = list(dict.fromkeys(source for source, _rule in sources if source != rid))
        out.append(
            {
                **sources[0][1],
                "id": None,
                "role_id": rid,
                "table_id": None,
                "domain_id": None,
                **target,
                "filter_expr": expr,
                "inherited_from": ", ".join(named),
            }
        )

    for a in actions:
        # REQ-1679: an action resolves like a table -- own rule for the action, else the role's
        # domain rule for the action's domain, and what its parents resolve.
        aname = a["name"]
        a_domain = a.get("domain_id") or ""
        memo: dict[str, tuple[Any, list[tuple[str, dict]]]] = {}

        def own_action(rid: str, aname: str = aname, a_domain: str = a_domain) -> dict | None:
            return action_rules.get((rid, aname)) or domain_rules.get((rid, a_domain))

        for rid, mine in parents.items():
            if mine:
                resolve(rid, own_action, memo, action_rules.get((rid, aname)), action_name=aname)
    for t in tables:
        tid = t["id"]
        domain_id = t.get("domain_id") or ""
        memo = {}

        def own_table(rid: str, tid: int = tid, domain_id: str = domain_id) -> dict | None:
            return table_rules.get((rid, tid)) or domain_rules.get((rid, domain_id))

        for rid, mine in parents.items():
            if mine:
                resolve(rid, own_table, memo, table_rules.get((rid, tid)), table_id=tid)
    return out
