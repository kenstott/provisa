# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A role's effective rights, as the role panel shows them (REQ-2161).

The capabilities half: the role's lineage from the standard roles down to it, by the same chains
the registry resolves (security/inheritance.py), with what each role in it adds and every
capability with the nearest role that lists it. A meta-role (``meta:a+b``, security/meta_role.py)
is an ephemeral child of its members, so its members are its parents and it adds nothing."""

# Requirements: REQ-2161, REQ-2155, REQ-1620

from __future__ import annotations

from collections.abc import Collection, Iterable

from provisa.security.inheritance import parent_map, role_chain
from provisa.security.meta_role import META_PREFIX, is_meta_role_id


def capabilities_view(role_id: str, rows: Iterable[dict], *, standard: Collection[str]) -> dict:
    """``role_id``'s capabilities view from role ``rows`` (id, capabilities, parents) -- the org's
    and the standard roles. Raises KeyError for a role (or a parent) the rows do not contain."""
    by_id = {r["id"]: r for r in rows}
    parents = parent_map(by_id.values())
    own: dict[str, set[str]] = {rid: set(r.get("capabilities") or []) for rid, r in by_id.items()}
    if is_meta_role_id(role_id):
        parents[role_id] = role_id[len(META_PREFIX) :].split("+")
        own[role_id] = set()
    chain = role_chain(role_id, parents)
    for rid in chain:
        if rid not in own:
            raise KeyError(rid)
    # A capability enters where no ancestor already holds it.
    adds = {
        rid: sorted(own[rid] - {c for anc in role_chain(rid, parents)[1:] for c in own[anc]})
        for rid in chain
    }
    capabilities = sorted(
        {
            right: next(rid for rid in reversed(chain) if right in adds[rid])
            for rid in chain
            for right in own[rid]
        }.items()
    )
    return {
        "role": role_id,
        "standard": role_id in standard,
        "chain": chain,
        "parents": {rid: list(parents.get(rid, [])) for rid in chain},
        "adds": adds,
        "standards": [rid for rid in chain if rid in standard],
        "capabilities": [{"right": right, "from": source} for right, source in capabilities],
    }
