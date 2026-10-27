# Copyright (c) 2026 Kenneth Stott
# Canary: fe46694d-c265-4d70-b707-1ca667bab558
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A role's effective rights, as the role panel shows them (REQ-2161).

The capabilities half: the role's lineage from the standard roles down to it, by the same chains
the registry resolves (security/inheritance.py), with what each role in it adds -- a right none of
its ancestors holds -- and every capability with the roles it enters at. With several parents a
right can enter in more than one branch; it comes from each. A meta-role (``meta:a+b``, security/meta_role.py)
is an ephemeral child of its members, so its members are its parents and it adds nothing."""

# Requirements: REQ-2161, REQ-2155, REQ-1620

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from typing import Any

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
    rights = sorted({right for rid in chain for right in own[rid]})
    return {
        "role": role_id,
        "standard": role_id in standard,
        "chain": chain,
        "parents": {rid: list(parents.get(rid, [])) for rid in chain},
        "adds": adds,
        "standards": [rid for rid in chain if rid in standard],
        "capabilities": [
            {"right": right, "from": [rid for rid in chain if right in adds[rid]]}
            for right in rights
        ],
    }


def _path(role_id: str, target: str, parents: Mapping[str, list[str]]) -> list[str] | None:
    """The first path, depth-first in declared parent order, from ``role_id`` up to ``target``."""
    if role_id == target:
        return [role_id]
    for parent in parents.get(role_id, []):
        found = _path(parent, target, parents)
        if found is not None:
            return [role_id, *found]
    return None


def _granted_by(stored: list[str], chain: list[str]) -> str | None:
    """The grant entry that serves a reader with ``chain``: the nearest role of the chain it names,
    else a composite entry the chain holds; none for an empty grant, which names no role (REQ-2154)."""
    from provisa.security.inheritance import holds_entry

    named = next((rid for rid in chain if rid in stored), None)
    if named is not None:
        return named
    return next((entry for entry in stored if holds_entry(entry, chain)), None)


def data_view(
    role_id: str,
    *,
    reach: Collection[str],
    chain: list[str],
    parents: Mapping[str, list[str]],
    tables: Iterable[dict],
    stored: Mapping[tuple[int, str], list[str]],
    masks: Mapping[int, Mapping[str, Any]],
    row_rule: Callable[[int, str], str | None],
    readable: Callable[[int, str], bool],
) -> list[dict]:
    """``role_id``'s data access, the data half of its effective rights (REQ-2161).

    ``tables`` are the build's (``state.tables``): their grants already expanded down the chains,
    so the one column rule (:func:`~provisa.security.rights.column_served`) decides what the role
    reads, within its ``reach`` or, outside it, a public column. ``stored`` is each column's grant
    as written, which names the role it was granted to (``granted_by``) and, with ``parents``, the
    path the role inherits it through (``via``). ``masks`` (table id → column → rule) and
    ``row_rule`` (table id, domain → predicate) are what the build resolved for the role. Only what
    ``readable`` -- the viewer's own read rights (REQ-2152) -- admits is listed, and nothing marks
    the rest."""
    from provisa.security.rights import column_served, reaches_domain

    out: list[dict] = []
    for table in tables:
        tid, domain = table["id"], table["domain_id"]
        reaches = reaches_domain(list(reach), domain)
        writes: dict[str, list[str]] = {}
        # A write is admitted only within the role's reach: outside it nothing is written.
        for grant in table.get("write_grants") or [] if reaches else []:
            if grant["role"] != role_id:
                continue
            for column in (
                [c["column_name"] for c in table["columns"]]
                if grant["operation"] == "delete"
                else grant.get("columns") or []
            ):
                writes.setdefault(column, []).append(grant["operation"])
        columns = []
        for column in table["columns"]:
            name = column["column_name"]
            if column.get("native_filter_type") or not readable(tid, name):
                continue
            read = column_served({"id": role_id}, domain, column, reaches=reaches)
            if not read and name not in writes:
                continue
            granted = _granted_by(stored.get((tid, name), column.get("visible_to") or []), chain)
            rule = masks.get(tid, {}).get(name)
            columns.append(
                {
                    "name": name,
                    "scope": column.get("scope") or "domain",
                    "read": read,
                    "write": sorted(writes.get(name, [])),
                    "mask": None if rule is None else {"type": rule.mask_type.value},
                    "row_rule": row_rule(tid, domain),
                    "granted_by": granted,
                    "via": (
                        [role_id]
                        if granted is None
                        else (_path(role_id, granted, parents) or [role_id])
                    ),
                    "reach": "domain" if reaches else "public",
                }
            )
        if columns:
            out.append(
                {
                    "table_id": tid,
                    "domain": domain,
                    "name": f"{domain}.{table.get('alias') or table['table_name']}",
                    "table_name": table["table_name"],
                    "reach": "domain" if reaches else "across",
                    "columns": columns,
                }
            )
    return out


def data_view_from_state(
    state: Any,
    role_id: str,
    *,
    parents: Mapping[str, list[str]],
    stored: Mapping[tuple[int, str], list[str]],
    readable: Callable[[int, str], bool],
    governance: bool,
    tables: Iterable[dict] | None = None,
) -> list[dict]:
    """:func:`data_view` for ``role_id`` from what the runtime serves: the build's tables, the
    role's reach (``state.roles``), the chain it reads with and the masks and row rules the build
    resolved for it. A meta-role (``meta:a+b``) is built as serving builds it
    (:func:`~provisa.security.meta_role.ensure_meta_role`), so its data access is what a request
    acting as the set is served. Without the governance right (REQ-1134) the grants, masks and row
    rules behind each column are not named."""
    from provisa.security.meta_role import ensure_meta_role, reader_chain

    parents = dict(parents)
    if is_meta_role_id(role_id):
        members = role_id[len(META_PREFIX) :].split("+")
        role_id = ensure_meta_role(state, members)
        parents[role_id] = members
    rls = state.rls_contexts.get(role_id)
    view = data_view(
        role_id,
        reach=state.roles[role_id]["domain_access"],
        chain=reader_chain(state, role_id),
        parents=parents,
        tables=state.tables if tables is None else tables,
        stored=stored,
        masks={
            tid: {column: rule for column, (rule, _type) in columns.items()}
            for (tid, rid), columns in state.masking_rules.items()
            if rid == role_id
        },
        row_rule=lambda tid, domain: (
            None if rls is None else rls.rules.get(tid) or rls.domain_rules.get(domain)
        ),
        readable=readable,
    )
    if not governance:
        for table in view:
            for column in table["columns"]:
                column.update(mask=None, row_rule=None, granted_by=None, via=[])
    return view


def table_access(
    state: Any,
    table_id: int,
    *,
    roles: Iterable[str],
    parents: Mapping[str, list[str]],
    stored: Mapping[tuple[int, str], list[str]],
    readable: Callable[[int, str], bool],
    governance: bool,
    standard: Collection[str],
) -> dict | None:
    """Who can read or write each column of table ``table_id`` (REQ-2162): the inverse of the
    role panel's data access, resolved by the same view per role of ``roles``. Only the columns
    ``readable`` admits -- the viewer's own read rights -- are listed; None when the build has no
    such table."""
    table = next((t for t in state.tables if t["id"] == table_id), None)
    if table is None:
        return None
    columns = {
        c["column_name"]: {
            "name": c["column_name"],
            "scope": c.get("scope") or "domain",
            "access": [],
        }
        for c in table["columns"]
        if not c.get("native_filter_type") and readable(table_id, c["column_name"])
    }
    for role_id in sorted(roles):
        for t in data_view_from_state(
            state,
            role_id,
            parents=parents,
            stored=stored,
            readable=readable,
            governance=governance,
            tables=[table],
        ):
            for column in t["columns"]:
                columns[column["name"]]["access"].append(
                    {
                        "role": role_id,
                        "standard": role_id in standard,
                        **{k: v for k, v in column.items() if k not in ("name", "scope")},
                    }
                )
    return {
        "table_id": table_id,
        "domain": table["domain_id"],
        "name": f"{table['domain_id']}.{table.get('alias') or table['table_name']}",
        "table_name": table["table_name"],
        "columns": list(columns.values()),
    }
