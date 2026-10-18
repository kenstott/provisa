# Copyright (c) 2026 Kenneth Stott
# Canary: 8c4d2e71-3a95-4f06-b1e8-6d0a7c3f5e29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Acting as a set of held roles: one ephemeral role, built on inheritance (REQ-1620).

A request may name any set of roles its identity holds. One role is that role. Two or more act as
the set's meta-role, ``meta:<ids sorted, joined by +>``: an ephemeral CHILD of every role in the
set — one child, several parents — made on first use in a model generation, cached, and never
stored, listed or exported. The whole pipeline then runs with it as an ordinary single role.

Everything a role is granted reaches the child exactly as role inheritance gives it
(security/inheritance.py), so the set acts with the union of its members' rights, grant-if-any:

- grant lists — column ``visible_to``; metric and command (function and webhook)
  ``visible_to`` — gain the child wherever they name a member;
- a table's write grants (REQ-2074) fold as reads do: per operation any member holds, the
  union of the members' columns, the OR of their rows and of their checks (each member's own,
  its read filter where it states none), and the presets every such member sets alike; a column
  is written on a row only where a member granting that column admits the row;
- a column's ordered mask list (REQ-2059) is read for each member with its own chain (its
  ancestors and the set's qualifiers), on the rows that member admits (below);
- capabilities and domain_access are unioned; the row cap and rate limit are the least
  restrictive.

Row filters are the one rule inheritance has no answer for: across several parents there is no
nearest one. The child's filter on a table is the OR of each member's own resolved filter
(each still resolved through that member's own chain), and there is none if any member that
reads the table resolves to none. Each member's role constants (``session_vars``) are inlined
into its own term, so two members giving one variable different values cannot collide; the
request's own variables still resolve at execution.

A row is read as the member(s) whose own filter admitted it: each column is masked or hidden as
those members read it, the least restrictive of them first (compiler.rls.MemberReads, applied at
the table by compiler.stage2), never through a mask or grant of a member that did not admit the
row. Pseudo-roles the set was named with contribute grants only.

The record of a statement made as a meta-role names every member (``enforced["acting_roles"]``).
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable
from typing import Any

META_PREFIX = "meta:"
_LOCK = threading.Lock()


class MetaRoleNamed(PermissionError):
    """A client named a meta-role directly; one is only ever made from roles the caller holds."""


def meta_role_id(members: list[str] | tuple[str, ...]) -> str:
    return META_PREFIX + "+".join(sorted(set(members)))


def is_meta_role_id(role_id: str | None) -> bool:
    return bool(role_id) and str(role_id).startswith(META_PREFIX)


def refuse_named_meta_role(requested: list[str]) -> None:
    for role_id in requested:
        if is_meta_role_id(role_id):
            raise MetaRoleNamed(
                f"{role_id!r} is not a role: name the roles you hold, and the server acts as them"
            )


def acting_roles(state: Any, role_id: str) -> tuple[str, ...]:
    """The roles ``role_id`` acts as: its members for a meta-role, else itself."""
    return tuple(state.meta_roles[role_id]) if is_meta_role_id(role_id) else (role_id,)


def _least_restrictive(values: list[Any]) -> Any:
    """None (no limit) wins; otherwise the largest."""
    return None if any(v is None for v in values) else max(values)


def _role(members: list[dict], meta_id: str) -> dict:
    caps: set[str] = set()
    domains: set[str] = set()
    for m in members:
        caps.update(m.get("capabilities") or [])
        domains.update(m.get("domain_access") or [])
    return {
        "id": meta_id,
        "capabilities": sorted(caps),
        "domain_access": ["*"] if "*" in domains else sorted(domains),
        "max_rows": _least_restrictive([m.get("max_rows") for m in members]),
        "rate_limit": _least_restrictive([m.get("rate_limit") for m in members]),
        # Each member's constants are inlined into its own filter term (_inlined), not merged here.
        "session_vars": {},
        "parent_role_id": None,
    }


def _inlined(filter_expr: str, constants: dict) -> str:
    """``filter_expr`` with the member's own role constants written in as literals; a variable
    the role does not set is left for the request to supply."""
    from provisa.compiler.sql_literals import sql_literal
    from provisa.pgwire._pipeline import _CURRENT_SETTING_RE

    def _sub(m: re.Match) -> str:
        key = m.group(1)
        return sql_literal(str(constants[key]), "postgres") if key in constants else m.group(0)

    return _CURRENT_SETTING_RE.sub(_sub, filter_expr)


def _or(terms: list[str]) -> str:
    return " OR ".join(f"({t})" for t in terms)


def _fold_write_grants(
    state: Any, meta_id: str, members: list[dict], tables: list[dict]
) -> None:  # REQ-2074
    """Give ``meta_id`` the fold of its members' write grants on every table, as its row filter
    is the fold of theirs (:func:`_rls`): for each operation a member holds a grant for, the
    union of the holders' columns; the OR of their rows and of their checks -- each member's own
    predicate, its resolved read filter where its grant states none, its role constants inlined
    -- and every row where any holder's is every row; and the presets every holder sets alike. A
    preset the holders disagree on is no preset: the column is then written only where a holder
    lists it among its columns."""
    from provisa.compiler.write_admission import _every_row

    by_id = {m["id"]: m for m in members}
    for t in tables:
        grants = [g for g in t.get("write_grants") or [] if g["role"] in by_id]
        if not grants:
            continue
        t["write_grants"] = [g for g in t["write_grants"] if g["role"] != meta_id]
        for operation in ("insert", "update", "delete"):
            holders = [g for g in grants if g["operation"] == operation]
            if not holders:
                continue
            columns: list[str] = []
            for g in holders:
                columns += [c for c in g["columns"] if c not in columns]

            def _folded(stated: str) -> str | None:
                terms: list[str] = []
                for g in holders:
                    member = by_id[g["role"]]
                    expr = g.get(stated)
                    if expr is None:
                        ctx = state.surfaces[member["id"]].rls
                        expr = ctx.rules.get(t["id"]) or ctx.domain_rules.get(
                            t.get("domain_id") or ""
                        )
                    if _every_row(expr):
                        return "TRUE"
                    assert expr is not None
                    terms.append(_inlined(expr, member.get("session_vars") or {}))
                return _or(terms)

            first = {p["column"]: p for p in holders[0]["presets"]}
            presets = [
                p
                for column, p in first.items()
                if all({q["column"]: q for q in g["presets"]}.get(column) == p for g in holders)
            ]

            def _own(g: dict, stated: str) -> str:
                """One holder's own predicate, as _folded reads it: TRUE for every row."""
                member = by_id[g["role"]]
                expr = g.get(stated)
                if expr is None:
                    ctx = state.surfaces[member["id"]].rls
                    expr = ctx.rules.get(t["id"]) or ctx.domain_rules.get(t.get("domain_id") or "")
                if _every_row(expr):
                    return "TRUE"
                assert expr is not None
                return _inlined(expr, member.get("session_vars") or {})

            t["write_grants"].append(
                {
                    "role": meta_id,
                    "operation": operation,
                    "columns": [c for c in columns if c not in {p["column"] for p in presets}],
                    "rows": None if operation == "insert" else _folded("rows"),
                    "check": None if operation == "delete" else _folded("check"),
                    "presets": presets,
                    # REQ-1620: each holder's own columns, rows and check. A column is written on
                    # a row only where a holder granting that column admits the row
                    # (write_admission.for_columns), never by the union across holders.
                    "holders": [
                        {
                            "columns": list(g["columns"]),
                            "rows": None if operation == "insert" else _own(g, "rows"),
                            "check": None if operation == "delete" else _own(g, "check"),
                        }
                        for g in holders
                    ],
                }
            )


def _member_reads(
    state: Any, members: list[dict], tables: list[dict], qualifiers: tuple[str, ...]
) -> dict[int, Any]:
    """Per table a member reads, how each member reads it (compiler.rls.MemberReads): a row is
    read as the member(s) whose own filter admitted it, each column masked or hidden as that
    member reads it -- never through another member's mask or grant. A member's treatment of a
    column is decided by its own chain (its ancestors) and the pseudo-roles the set was named
    with, which contribute grants only."""
    from provisa.compiler.rls import HIDDEN, PLAIN, MemberReads
    from provisa.security.masking import entry_for, rule_of
    from provisa.security.rights import column_served, reaches_domain

    from provisa.security.masking import MaskType

    chains = state.role_build_inputs["role_chains"]
    masked = {(tid, col): (masks, dtype) for tid, col, masks, dtype in state.masked_columns}
    # REQ-1494: a fake is the column's own declaration, read by every role in a Test environment
    # in place of any mask (app_loaders._load_masking_rules); each member holds it alike.
    faked = {
        (tid, col): entry
        for (tid, _rid), cols in state.masking_rules.items()
        for col, entry in cols.items()
        if entry[0].mask_type == MaskType.fake
    }
    out: dict[int, Any] = {}
    for t in tables:
        readers = [
            m
            for m in members
            if any(tm.table_id == t["id"] for tm in state.surfaces[m["id"]].context.tables.values())
        ]
        if not readers:
            continue
        domain = t.get("domain_id")
        flags: list[str] = []
        for m in readers:
            ctx = state.surfaces[m["id"]].rls
            expr = ctx.rules.get(t["id"]) or ctx.domain_rules.get(domain or "")
            flags.append("TRUE" if expr is None else _inlined(expr, m.get("session_vars") or {}))
        columns: dict[str, tuple[tuple[int, Any, str], ...]] = {}
        for c in t.get("columns") or []:
            name = c["column_name"]
            treatments = []
            for m in readers:
                reaches = not domain or reaches_domain(m.get("domain_access") or [], domain)
                served = any(
                    column_served({"id": rid}, domain, c, reaches=reaches)
                    for rid in (m["id"], *qualifiers)
                )
                masks, dtype = masked.get((t["id"], name), ([], c.get("data_type", "varchar")))
                if not served:
                    treatments.append((HIDDEN, None, dtype))
                    continue
                if (t["id"], name) in faked:
                    rule, fake_type = faked[(t["id"], name)]
                    treatments.append((1, rule, fake_type))
                    continue
                i = entry_for(masks, [*chains[m["id"]], *qualifiers])
                if i is None or masks[i].get("mask") is None:
                    treatments.append((PLAIN, None, dtype))
                else:
                    treatments.append((1 + i, rule_of(masks[i]["mask"]), dtype))
            columns[name] = tuple(treatments)
        out[t["id"]] = MemberReads(flags=tuple(flags), columns=columns)
    return out


def _rls(
    state: Any, members: list[dict], tables: list[dict], qualifiers: tuple[str, ...] = ()
) -> Any:
    """The OR of each member's own resolved filter, per table and per command; none where a member
    that reads it resolves to none. With it, how each member reads each table it reads
    (:func:`_member_reads`)."""
    from provisa.compiler.rls import RLSContext
    from provisa.security.mutation_authz import command_reachable

    rules: dict[int, str] = {}
    for t in tables:
        terms: list[str] = []
        lifted = False
        for m in members:
            member = state.surfaces[m["id"]]  # one snapshot: context and filters of one build
            reads = any(tm.table_id == t["id"] for tm in member.context.tables.values())
            if not reads:
                continue
            ctx = member.rls
            expr = ctx.rules.get(t["id"]) or ctx.domain_rules.get(t.get("domain_id") or "")
            if expr is None:
                lifted = True
                break
            terms.append(_inlined(expr, m.get("session_vars") or {}))
        if terms and not lifted:
            rules[t["id"]] = _or(terms)

    actions: dict[str, str] = {}
    commands = [
        *(getattr(state, "tracked_functions", None) or {}).values(),
        *(getattr(state, "tracked_webhooks", None) or {}).values(),
    ]
    for c in {c["name"]: c for c in commands}.values():
        terms = []
        lifted = False
        for m in members:
            if not command_reachable(c, m):
                continue
            ctx = state.rls_contexts[m["id"]]
            expr = ctx.action_rules.get(c["name"]) or ctx.domain_rules.get(c.get("domain_id") or "")
            if expr is None:
                lifted = True
                break
            terms.append(_inlined(expr, m.get("session_vars") or {}))
        if terms and not lifted:
            actions[c["name"]] = _or(terms)
    return RLSContext(
        rules=rules,
        domain_rules={},
        action_rules=actions,
        member_reads=_member_reads(state, members, tables, qualifiers),
    )


def meta_chain(
    meta_id: str,
    member_ids: list[str] | tuple[str, ...],
    role_chains: dict[str, list[str]],
    qualifiers: tuple[str, ...] = (),
) -> list[str]:
    """The meta-role's chain: itself, every member with each member's ancestors (a composite
    grant names them, REQ-2058), and the pseudo-roles it was named with."""
    held: list[str] = []
    for m in member_ids:
        for rid in role_chains[m]:
            if rid not in held:
                held.append(rid)
    return [meta_id, *held, *(q for q in qualifiers if q not in held)]


def reader_chain(state: Any, role_id: str) -> list[str]:
    """The chain a request acting as ``role_id`` reads with: a meta-role's own
    (:func:`meta_chain`), else the role's (itself and its ancestors)."""
    members = state.meta_roles.get(role_id)
    if members is not None:
        return meta_chain(
            role_id, members, state.role_chains, state.meta_qualifiers.get(role_id, ())
        )
    return state.role_chains.get(role_id, [role_id])


def with_meta_masks(
    state: Any,
    rules: dict,
    masked_columns: list[tuple[int, str, list[dict], str]],
    role_chains: dict[str, list[str]],
) -> dict:
    """``rules`` — a rebuild's masking rules for the roles the control plane holds — with the
    masks of every meta-role the runtime knows whose members are all among ``role_chains``.

    A meta-role outlives a rebuild (:func:`rebuild_meta_roles`), and requests act as it while
    the rebuild runs. Its masks are part of what the rebuild publishes, in the same assignment
    as its members': there is no moment at which it is served with none."""
    from provisa.security.masking import reader_masks

    for meta_id, member_ids in list(state.meta_roles.items()):
        if all(m in role_chains for m in member_ids):
            chain = meta_chain(
                meta_id, member_ids, role_chains, state.meta_qualifiers.get(meta_id, ())
            )
            rules.update(reader_masks(masked_columns, meta_id, chain))
    return rules


def _build(
    state: Any, meta_id: str, member_ids: list[str], qualifiers: tuple[str, ...] = ()
) -> None:
    """Build the meta-role from its members as this model generation has them, and install it:
    its masks, its role and its surface (one assignment — a request acting as it reads the
    surface this replaces or this one).

    REQ-2057: ``qualifiers`` are the pseudo-roles the request named. They have no surface, rights,
    masks or row filters of their own -- as a member, one with no masks would unmask every column,
    and one reading no table would end every OR of row filters -- so they enter only the grants:
    a grant list naming one reaches the child, as a member's does."""
    from provisa.api.app_loaders import register_role_surface
    from provisa.security.inheritance import expand_grants
    from provisa.security.masking import reader_masks

    member_roles = [state.roles[m] for m in member_ids]
    inputs = state.role_build_inputs
    chains = {meta_id: meta_chain(meta_id, member_ids, inputs["role_chains"], qualifiers)}
    # Inheritance with several parents: every grant to a member reaches the child.
    for t in inputs["tables"]:
        expand_grants(t.get("columns") or [], chains)
    expand_grants(inputs["metrics"], chains)
    expand_grants(inputs["functions"], chains)
    expand_grants(inputs["webhooks"], chains)
    # REQ-2059: the child reads each column through the first entry of its mask list its own
    # chain holds -- as any role does; an entry granted to one member reaches it, and an entry
    # before it, held by another member, applies first (deny by order).
    masks = reader_masks(state.masked_columns, meta_id, chains[meta_id])
    from provisa.compiler.rls import org_scoped_signals

    # Held to the reading org on the ops signals as every role is (compiler.rls).
    rls = org_scoped_signals(
        _rls(state, member_roles, inputs["tables"], qualifiers), inputs["tables"]
    )
    _fold_write_grants(state, meta_id, member_roles, inputs["tables"])  # REQ-2074
    role = _role(member_roles, meta_id)
    state.masking_rules = {**state.masking_rules, **masks}
    state.roles[meta_id] = role
    register_role_surface(state, role, rls)
    state.meta_roles[meta_id] = tuple(member_ids)
    state.meta_qualifiers[meta_id] = tuple(qualifiers)


def ensure_meta_role(state: Any, members: list[str], qualifiers: tuple[str, ...] = ()) -> str:
    """The meta-role acting as ``members`` (roles the caller holds, two or more), built on first
    use. A rebuild of the model rebuilds it from the rebuilt members while requests may still be
    acting as it (:func:`rebuild_meta_roles`); each use is recorded so a rebuild knows that.

    A held role with no data surface -- platform_admin, control-plane only (REQ-1327), or a role
    reaching no domain -- reaches no data, so it is no member of the data role the set acts as:
    the set acts as the roles that have one, and as the one role when one is left. Built with it,
    the meta-role took its control-plane rights, and building it failed on its missing surface
    (KeyError 'platform_admin'), on every request and at every model rebuild after."""
    data = sorted({m for m in members if m in state.surfaces})
    if not data:
        raise PermissionError(f"No schema available for roles {sorted({*members, *qualifiers})}")
    quals = tuple(sorted(set(qualifiers)))
    if len(data) == 1 and not quals:
        return data[0]
    members = data
    # REQ-2057: a set naming pseudo-roles is its own meta-role even with one data role: what it
    # is granted differs from that role's.
    meta_id = meta_role_id([*members, *quals])
    if meta_id not in state.roles or state.meta_roles.get(meta_id) is None:
        with _LOCK:
            if meta_id not in state.roles or state.meta_roles.get(meta_id) is None:
                _build(state, meta_id, sorted(set(members)), quals)
    state.meta_resolved_at[meta_id] = time.monotonic()
    return meta_id


def rebuild_meta_roles(state: Any, role_ids: set[str]) -> None:
    """After a rebuild has registered the roles the control plane holds (``role_ids``): bring
    the runtime's meta-roles to the new model generation.

    A request resolves the role it acts as once, and then runs as it — one statement after
    another, for as long as its transport's request timeout allows. Dropping a meta-role at a
    rebuild therefore failed requests in flight: their next statement found no role (#216). A
    meta-role that a request may still be acting as — one resolved within the longest request
    timeout — is REBUILT here from its rebuilt members, its surface replaced in one assignment.
    One whose members are no longer all held, or that nothing has resolved for that long, is
    removed; a later use builds it again.
    """
    from provisa.api.app_loaders import _drop_data_surface
    from provisa.core.limits import longest_request_timeout

    in_use_since = time.monotonic() - longest_request_timeout()
    with _LOCK:
        for meta_id, member_ids in list(state.meta_roles.items()):
            # A member that no longer has a data surface is no member (ensure_meta_role): the
            # meta-role goes, and a later use builds it from the roles that have one.
            held = all(m in role_ids and m in state.surfaces for m in member_ids)
            if held and state.meta_resolved_at.get(meta_id, float("-inf")) >= in_use_since:
                _build(state, meta_id, list(member_ids), state.meta_qualifiers.get(meta_id, ()))
                continue
            state.meta_roles.pop(meta_id, None)
            state.meta_qualifiers.pop(meta_id, None)
            state.meta_resolved_at.pop(meta_id, None)
            state.roles.pop(meta_id, None)
            _drop_data_surface(state, meta_id)


def resolve_requested_role(
    state: Any,
    permitted: set[str],
    requested: str,
    pseudo: Callable[[], dict[str, str]] | None = None,
) -> str:
    """The role a request acts as, from what it names (a role, or a comma-separated set) and the
    roles its identity holds (``permitted``). Each named role must be held, and one that is not is
    refused by name; one role is that role, several act as their meta-role.

    REQ-2057: ``pseudo`` reads the pseudo-roles the identity holds (id -> parent), asked only when
    a request names a role the identity is not assigned. One named enters the set as a qualifier,
    with its parent, and the set acts as its meta-role."""
    names = [r.strip() for r in requested.split(",") if r.strip()]
    refuse_named_meta_role(names)
    unassigned = [n for n in names if n not in permitted]
    held_pseudo = pseudo() if (unassigned and pseudo is not None) else {}
    data: list[str] = []
    qualifiers: set[str] = set()
    for name in names:
        if name in permitted:
            data.append(name)
        elif name in held_pseudo:
            qualifiers.update(q for q in (name, held_pseudo[name]) if q)
        else:
            raise PermissionError(f"role {name!r} is not assigned to this identity")
    distinct = sorted(set(data))
    if not qualifiers:
        return distinct[0] if len(distinct) == 1 else ensure_meta_role(state, distinct)
    return ensure_meta_role(state, distinct, tuple(sorted(qualifiers)))
