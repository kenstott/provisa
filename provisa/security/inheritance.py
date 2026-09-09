# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e7d2a-6c1f-4e8b-a5d3-7f0c2e9b1a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Role inheritance, materialized once per runtime build (REQ-1677).

One parent per role. A role's chain is the role itself followed by its ancestors, nearest first.
Everything downstream stays keyed by the acting role id, because the chain is folded into the
data here rather than consulted at every lookup:

- grant lists (``visible_to``, ``writable_by``, ``unmasked_to``) gain the child wherever they
  name an ancestor — a grant is a union up the chain;
- RLS resolves per table with the child taking precedence: the nearest role in the chain that
  has a rule for the table supplies the one predicate, its own table rule before its domain rule;
- role dicts gain the union of ancestor capabilities and domain_access, and the parent's
  rate_limit when the child sets none.

The functions mutate the dicts they are given, which are the runtime build's own copies, never
the rows the admin surfaces read back — an administrator sees the grants they made, not the
materialized ones.
"""

# Requirements: REQ-215, REQ-1677

from __future__ import annotations

from collections.abc import Iterable

GRANT_LISTS = ("visible_to", "writable_by", "unmasked_to")


def parent_map(roles: Iterable[dict]) -> dict[str, str | None]:
    """role id → parent role id (None when the role has no parent)."""
    return {r["id"]: r.get("parent_role_id") or None for r in roles}


def role_chain(role_id: str, parents: dict[str, str | None]) -> list[str]:
    """The role and its ancestors, nearest first. Raises ValueError on a cycle."""
    chain: list[str] = []
    current: str | None = role_id
    while current is not None:
        if current in chain:
            raise ValueError(f"role inheritance cycle at {current!r}: {' -> '.join(chain)}")
        chain.append(current)
        current = parents.get(current)
    return chain


def role_chains(roles: Iterable[dict]) -> dict[str, list[str]]:
    parents = parent_map(roles)
    return {rid: role_chain(rid, parents) for rid in parents}


def parent_problem(
    role_id: str, parent_id: str | None, parents: dict[str, str | None]
) -> str | None:
    """Why ``parent_id`` cannot be the parent of ``role_id``, or None when it can.

    ``parents`` is the current map; the proposed edge is checked against it, so a cycle through
    the roles that already exist is caught before the write.
    """
    if parent_id is None:
        return None
    if parent_id == role_id:
        return f"Role {role_id!r} cannot inherit from itself"
    if parent_id not in parents:
        return f"Parent role {parent_id!r} not found"
    try:
        role_chain(parent_id, {**parents, role_id: parent_id})
    except ValueError:
        return f"Role {role_id!r} cannot inherit from {parent_id!r}: that would close a cycle"
    return None


def children_of(role_id: str, roles: Iterable[dict]) -> list[str]:
    return sorted(r["id"] for r in roles if (r.get("parent_role_id") or None) == role_id)


def flatten_role_dicts(roles: list[dict]) -> list[dict]:
    """Each role with ancestor capabilities and domain_access unioned in and the nearest
    ancestor's rate_limit adopted when the role sets none. Input dicts are not modified."""
    by_id = {r["id"]: r for r in roles}
    chains = role_chains(roles)
    out: list[dict] = []
    for r in roles:
        caps: set[str] = set()
        domains: set[str] = set()
        rate_limit = r.get("rate_limit")
        for rid in chains[r["id"]]:
            anc = by_id[rid]
            caps.update(anc.get("capabilities") or [])
            domains.update(anc.get("domain_access") or [])
            if rate_limit is None:
                rate_limit = anc.get("rate_limit")
        out.append(
            {
                **r,
                "capabilities": sorted(caps),
                "domain_access": ["*"] if "*" in domains else sorted(domains),
                "rate_limit": rate_limit,
            }
        )
    return out


def expand_grants(items: Iterable[dict], chains: dict[str, list[str]]) -> None:
    """Add each role to every grant list on ``items`` that names one of its ancestors."""
    for item in items:
        for key in GRANT_LISTS:
            granted = item.get(key)
            if not granted:
                continue
            present = set(granted)
            for rid, chain in chains.items():
                if rid not in present and any(anc in present for anc in chain[1:]):
                    granted.append(rid)
                    present.add(rid)


def expand_column_grants(tables: Iterable[dict], chains: dict[str, list[str]]) -> None:
    for t in tables:
        expand_grants(t.get("columns") or [], chains)


def holds_grant(role_id: str, granted: Iterable[str], chains: dict[str, list[str]]) -> bool:
    """Whether ``role_id`` or an ancestor is in ``granted``."""
    present = set(granted)
    return any(rid in present for rid in chains.get(role_id, [role_id]))


def materialize_rls(
    rls_rules: list[dict],
    tables: Iterable[dict],
    chains: dict[str, list[str]],
    actions: Iterable[dict] = (),
) -> list[dict]:
    """The rule list a child-precedence walk produces, as table-level rules per role.

    For every role with ancestors and every table: the role's own table rule stands; otherwise
    the nearest role in the chain with a table rule or a domain rule for the table's domain
    supplies a table-level rule for this role. Roles without ancestors, and every rule already
    in the list, pass through unchanged, so a role's own domain rule still covers a table
    registered after this build the way it did before.
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
    for rid, chain in chains.items():
        if len(chain) == 1:
            continue
        # REQ-1679: an action resolves like a table — own rule, else the nearest ancestor's rule
        # for the action, else that level's domain rule for the action's domain.
        for a in actions:
            aname = a["name"]
            if (rid, aname) in action_rules:
                continue
            a_domain = a.get("domain_id") or ""
            for anc in chain:
                found = action_rules.get((anc, aname)) or domain_rules.get((anc, a_domain))
                if found is None:
                    continue
                if anc != rid:
                    out.append(
                        {
                            **found,
                            "id": None,
                            "role_id": rid,
                            "table_id": None,
                            "domain_id": None,
                            "action_name": aname,
                            "inherited_from": anc,
                        }
                    )
                break
        for t in tables:
            tid = t["id"]
            if (rid, tid) in table_rules:
                continue
            domain_id = t.get("domain_id") or ""
            for anc in chain:
                found = table_rules.get((anc, tid)) or domain_rules.get((anc, domain_id))
                if found is None:
                    continue
                if anc != rid:
                    out.append(
                        {
                            **found,
                            "id": None,
                            "role_id": rid,
                            "table_id": tid,
                            "domain_id": None,
                            "inherited_from": anc,
                        }
                    )
                break
    return out
