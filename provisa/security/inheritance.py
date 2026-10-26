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
- RLS resolves per table with the child taking precedence: a role's own rule for the table (its
  table rule before its domain rule) stands; otherwise the rules its parents resolve are ORed, and
  a parent that resolves none contributes nothing, so adding a parent never lifts a row rule. With
  one parent this is the nearest role in the chain that has a rule;
- role dicts gain the union of ancestor capabilities, and the nearest ancestor's rate_limit when
  the role sets none. A role's domain_access is its own and never inherited (REQ-2155): reach says
  where the role's capabilities apply, so a domain child of a role reaching every domain reaches
  only the domains it lists.

The functions mutate the dicts they are given, which are the runtime build's own copies, never
the rows the admin surfaces read back — an administrator sees the grants they made, not the
materialized ones.
"""

# Requirements: REQ-215, REQ-1677

from __future__ import annotations

from collections.abc import Callable, Iterable

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


def flatten_role_dicts(roles: list[dict]) -> list[dict]:
    """Each role with ancestor capabilities unioned in -- rights only add -- its own domain_access
    alone (REQ-2155: reach is not inherited), and
    each property (rate_limit, max_rows) set by the nearest role in the chain that declares it,
    the role itself first, so a child may raise or lower its parent's (REQ-2057). Session values
    (REQ-1682) merge by name, the nearest role's value for each. Input dicts are not modified."""
    by_id = {r["id"]: r for r in roles}
    chains = role_chains(roles)
    out: list[dict] = []
    for r in roles:
        caps: set[str] = set()
        domains = set(r.get("domain_access") or [])
        rate_limit = r.get("rate_limit")
        max_rows = r.get("max_rows")
        session_vars: dict[str, str] = {}
        for rid in chains[r["id"]]:
            anc = by_id[rid]
            caps.update(anc.get("capabilities") or [])
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
            "domain_access": ["*"] if "*" in domains else sorted(domains),
            "rate_limit": rate_limit,
        }
        if "max_rows" in r:
            flattened["max_rows"] = max_rows
        if "session_vars" in r:
            flattened["session_vars"] = session_vars
        out.append(flattened)
    return out


def effective_capabilities(role_id: str, roles: Iterable[dict]) -> list[str]:
    """What ``role_id`` holds once its ancestors' capabilities are folded in — the list the built
    registry carries for it. ``roles`` are rows with ``id``, ``capabilities``, ``parents``.
    Raises KeyError for a role (or a parent) the rows do not contain."""
    by_id = {r["id"]: r for r in roles}
    caps: set[str] = set()
    for rid in role_chain(role_id, parent_map(by_id.values())):
        caps.update(by_id[rid].get("capabilities") or [])
    return sorted(caps)


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


def _ored(exprs: Iterable[str | None]) -> str | None:
    """The OR of the predicates given, each once; None stands for no predicate and contributes
    nothing (REQ-2155). One predicate is returned as it is."""
    distinct = list(dict.fromkeys(e for e in exprs if e))
    if not distinct:
        return None
    if len(distinct) == 1:
        return distinct[0]
    return " OR ".join(f"({e})" for e in distinct)


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


def materialize_rls(
    rls_rules: list[dict],
    tables: Iterable[dict],
    parents: dict[str, list[str]],
    actions: Iterable[dict] = (),
) -> list[dict]:
    """The rule list a child-precedence resolution produces, as table-level rules per role.

    For every role with parents and every table: the role's own table rule stands; otherwise its own
    domain rule for the table's domain; otherwise the rules its parents resolve, the same way,
    ORed into one table-level rule for this role (REQ-2155) -- a parent that resolves none
    contributes nothing, so adding a parent never lifts a row rule, and only a role with no rule
    anywhere among its ancestors is unrestricted. With one parent this is the nearest role in the
    chain that has a rule. Roles without parents, and every rule already in the list, pass through
    unchanged, so a role's own domain rule still covers a table registered after this build the
    way it did before.
    """
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

    def inherit(rid: str, own: Callable[[str], dict | None], memo: dict, **target) -> None:
        found = _resolved(rid, parents, own, memo)
        if not found:
            return
        sources = [source for source, _rule in found]
        out.append(
            {
                **found[0][1],
                "id": None,
                "role_id": rid,
                "table_id": None,
                "domain_id": None,
                **target,
                "filter_expr": _ored(rule["filter_expr"] for _source, rule in found),
                "inherited_from": sources[0] if len(sources) == 1 else ", ".join(sources),
            }
        )

    for a in actions:
        # REQ-1679: an action resolves like a table -- own rule for the action, else the role's
        # domain rule for the action's domain, else what its parents resolve.
        aname = a["name"]
        a_domain = a.get("domain_id") or ""
        memo: dict[str, list[tuple[str, dict]]] = {}

        def own_action(rid: str, aname: str = aname, a_domain: str = a_domain) -> dict | None:
            return action_rules.get((rid, aname)) or domain_rules.get((rid, a_domain))

        for rid, mine in parents.items():
            if mine and own_action(rid) is None:
                inherit(rid, own_action, memo, action_name=aname)
    for t in tables:
        tid = t["id"]
        domain_id = t.get("domain_id") or ""
        memo = {}

        def own_table(rid: str, tid: int = tid, domain_id: str = domain_id) -> dict | None:
            return table_rules.get((rid, tid)) or domain_rules.get((rid, domain_id))

        for rid, mine in parents.items():
            if mine and own_table(rid) is None:
                inherit(rid, own_table, memo, table_id=tid)
    return out
