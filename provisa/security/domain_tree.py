# Copyright (c) 2026 Kenneth Stott
# Canary: 3c197b5f-1dd1-4789-b892-1911e64c8fcf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domain tree, flattened once per runtime build (REQ-1363).

A domain may name one parent: a business unit and the organizations within it. The tree is
governance structure only. Addressing stays flat — a table sits in exactly one domain, leaf or
not, and is named ``domain.table`` — and nothing is namespaced per domain.

Everything downstream reads flat sets, because the tree is folded into the data here rather
than walked at every lookup:

- a role that lists a domain reaches that domain and every domain under it, at any depth, so
  an organization added later under a business unit is reached without the role being edited;
  listing a child grants nothing upward;
- a source allowed to a domain is allowed to every domain under it;
- a row filter on a domain applies to the tables of every domain under it too, together with
  that domain's own filter and every other ancestor's: a child domain only narrows what its
  ancestors allow (provisa.security.row_filters);
- a domain with no steward is stewarded by its nearest ancestor that has one.

The functions return new values; the rows the admin surfaces read back are never changed, so an
administrator sees the grant that was made, not the flattened one.
"""

# Requirements: REQ-1363

from __future__ import annotations

from collections.abc import Iterable

from provisa.security.rights import normalized_reach

ALL_DOMAINS = "*"


def parent_map(domains: Iterable[dict]) -> dict[str, str | None]:
    """domain id → parent domain id (None when the domain has no parent)."""
    return {d["id"]: d.get("parent_domain_id") or None for d in domains}


def ancestry(domain_id: str, parents: dict[str, str | None]) -> list[str]:
    """The domain and its ancestors, nearest first. Raises ValueError on a cycle, naming it."""
    chain: list[str] = []
    current: str | None = domain_id
    while current is not None:
        if current in chain:
            raise ValueError(
                f"domain hierarchy cycle: {' -> '.join([*chain[chain.index(current) :], current])}"
            )
        chain.append(current)
        current = parents.get(current)
    return chain


def parent_problem(
    domain_id: str, parent_id: str | None, parents: dict[str, str | None]
) -> str | None:
    """Why ``parent_id`` cannot be the parent of ``domain_id``, or None when it can.

    ``parents`` is the current map; the proposed edge is checked against it, so a cycle through
    the domains that already exist is caught before the write and named.
    """
    if parent_id is None:
        return None
    if parent_id == domain_id:
        return f"Domain {domain_id!r} cannot be its own parent"
    if parent_id not in parents:
        return f"Parent domain {parent_id!r} not found"
    try:
        ancestry(domain_id, {**parents, domain_id: parent_id})
    except ValueError as cycle:
        return f"Domain {domain_id!r} cannot sit under {parent_id!r}: {cycle}"
    return None


def tree_problem(parents: dict[str, str | None]) -> str | None:
    """Why a whole parent map does not hold as a tree, or None when it does: every parent is a
    domain of the map and no domain is its own ancestor. For a config file, which declares the
    tree all at once."""
    for domain_id in sorted(parents):
        parent_id = parents[domain_id]
        if parent_id is not None and parent_id not in parents:
            return f"Domain {domain_id!r} names the parent {parent_id!r}, which is not a domain"
        try:
            ancestry(domain_id, parents)
        except ValueError as cycle:
            return str(cycle)
    return None


def children_of(domain_id: str, parents: dict[str, str | None]) -> list[str]:
    return sorted(d for d, p in parents.items() if p == domain_id)


def descendants(domain_id: str, parents: dict[str, str | None]) -> set[str]:
    """Every domain under ``domain_id``, at any depth; ``domain_id`` itself is not among them."""
    out: set[str] = set()
    frontier = [domain_id]
    while frontier:
        for child in children_of(frontier.pop(), parents):
            if child not in out:
                out.add(child)
                frontier.append(child)
    return out


def expand(domain_ids: Iterable[str], parents: dict[str, str | None]) -> list[str]:
    """``domain_ids`` with every descendant of each added, sorted. ``"*"`` already means every
    data domain, so it is returned as a role's flattened inheritance writes it, with the system
    domains it holds and nothing to expand."""
    listed = set(domain_ids)
    if ALL_DOMAINS in listed:
        return normalized_reach(listed)
    for domain_id in list(listed):
        listed |= descendants(domain_id, parents)
    return sorted(listed)


def flatten_role_domains(roles: list[dict], parents: dict[str, str | None]) -> list[dict]:
    """Each role with its ``domain_access`` expanded down the tree. Input dicts are not modified."""
    return [{**r, "domain_access": expand(r["domain_access"], parents)} for r in roles]


def ancestors(domain_id: str, parents: dict[str, str | None]) -> list[str]:
    """The domains above ``domain_id``, nearest first; a domain the map does not hold has none."""
    return ancestry(domain_id, parents)[1:] if domain_id in parents else []


def effective_steward(
    domain_id: str, stewards: dict[str, str | None], parents: dict[str, str | None]
) -> str | None:
    """The domain's own steward, else its nearest ancestor's; None when no domain up the tree has
    one."""
    for d in ancestry(domain_id, parents):
        if stewards.get(d):
            return stewards[d]
    return None
