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

- grant lists — column ``visible_to``, ``writable_by`` and ``unmasked_to``; metric and command
  (function and webhook) ``visible_to`` — gain the child wherever they name a member, so a
  column is masked for the child only when it is masked for every member;
- capabilities and domain_access are unioned; the row cap and rate limit are the least
  restrictive.

Row filters are the one rule inheritance has no answer for: across several parents there is no
nearest one. The child's filter on a table is the OR of each member's own resolved filter
(each still resolved through that member's own chain), and there is none if any member that
reads the table resolves to none. Each member's role constants (``session_vars``) are inlined
into its own term, so two members giving one variable different values cannot collide; the
request's own variables still resolve at execution.

The record of a statement made as a meta-role names every member (``enforced["acting_roles"]``).
"""

from __future__ import annotations

import re
import threading
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


def _rls(state: Any, members: list[dict], tables: list[dict]) -> Any:
    """The OR of each member's own resolved filter, per table and per command; none where a member
    that reads it resolves to none."""
    from provisa.compiler.rls import RLSContext
    from provisa.security.mutation_authz import command_reachable

    rules: dict[int, str] = {}
    for t in tables:
        terms: list[str] = []
        lifted = False
        for m in members:
            reads = any(tm.table_id == t["id"] for tm in state.contexts[m["id"]].tables.values())
            if not reads:
                continue
            ctx = state.rls_contexts[m["id"]]
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
    return RLSContext(rules=rules, domain_rules={}, action_rules=actions)


def ensure_meta_role(state: Any, members: list[str]) -> str:
    """The meta-role acting as ``members`` (roles the caller holds, two or more), built on first
    use in this model generation. A rebuild of the model drops it with every role it no longer
    holds, so the next use builds it again from the rebuilt members."""
    from provisa.api.app_loaders import register_role_surface
    from provisa.security.inheritance import expand_grants

    meta_id = meta_role_id(members)
    if meta_id in state.roles and state.meta_roles.get(meta_id) is not None:
        return meta_id
    with _LOCK:
        if meta_id in state.roles and state.meta_roles.get(meta_id) is not None:
            return meta_id
        member_ids = sorted(set(members))
        member_roles = [state.roles[m] for m in member_ids]
        inputs = state.role_build_inputs
        chains = {meta_id: [meta_id, *member_ids]}
        # Inheritance with several parents: every grant to a member reaches the child.
        for t in inputs["tables"]:
            expand_grants(t.get("columns") or [], chains)
        expand_grants(inputs["metrics"], chains)
        expand_grants(inputs["functions"], chains)
        expand_grants(inputs["webhooks"], chains)
        # A column is masked for the child only where it is masked for every member: a member in
        # its unmasked_to puts the child there too (expand_grants above).
        masks: dict[Any, dict[str, Any]] = {}
        for t in inputs["tables"]:
            per_member = [state.masking_rules.get((t["id"], m)) or {} for m in member_ids]
            common = set(per_member[0]).intersection(*per_member[1:])
            if common:
                masks[(t["id"], meta_id)] = {c: per_member[0][c] for c in common}
        rls = _rls(state, member_roles, inputs["tables"])
        role = _role(member_roles, meta_id)
        state.masking_rules = {**state.masking_rules, **masks}
        state.roles[meta_id] = role
        register_role_surface(state, role, rls)
        state.meta_roles[meta_id] = tuple(member_ids)
    return meta_id


def resolve_requested_role(state: Any, permitted: set[str], requested: str) -> str:
    """The role a request acts as, from what it names (a role, or a comma-separated set) and the
    roles its identity holds (``permitted``). Each named role must be held, and one that is not is
    refused by name; one role is that role, several act as their meta-role."""
    names = [r.strip() for r in requested.split(",") if r.strip()]
    refuse_named_meta_role(names)
    for name in names:
        if name not in permitted:
            raise PermissionError(f"role {name!r} is not assigned to this identity")
    distinct = sorted(set(names))
    if len(distinct) == 1:
        return distinct[0]
    return ensure_meta_role(state, distinct)
