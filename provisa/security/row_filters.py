# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6a4e9b-83d1-4b57-a2fe-6d19c7b05e3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row filters resolved to one predicate per (role, table), once per runtime build.

A row filter attaches to a table, to a domain, or to a tag (REQ-1384). The governance path
reads one predicate per table id, so the three scopes are folded together here, before a
role's RLS context is built, rather than consulted at every request:

- the table's own rule, or failing that the rule on the table's domain (REQ-402), is the base
  predicate — role inheritance has already been folded into these by ``materialize_rls``;
- the rule on every domain above the table's domain is AND-ed onto it (REQ-1363): a child
  domain only narrows what its ancestors allow;
- every rule on a tag the table carries is AND-ed onto it. There are no precedence tiers: a
  tag only ever adds restriction. The acting role's tag rules and those of every ancestor all
  contribute, nearest role first, so a child role cannot shed an ancestor's tag rule.

A rule names a tag by its base id and covers every parameter value the tag is assigned with
("entity" covers "entity:customer").
"""

# Requirements: REQ-1384, REQ-402, REQ-1677, REQ-1363

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


#: REQ-2188: a rule's rights, and the capability RW on a model table is held by. A member of a set
#: of roles that holds it with no rule lifts the set's rule; one that does not, gives nothing.
RIGHTS = ("R", "U", "D")
MODEL_RW_RIGHTS: dict[str, str] = {"data_products": "data_product_rw"}


def tagged_table_ids(tag_assignments: Iterable[dict]) -> dict[str, set[int]]:
    """base tag id → ids of the tables that carry the tag. A tag on a table's column, or on
    its source, is not a tag on the table."""
    out: dict[str, set[int]] = {}
    for a in tag_assignments:
        if a["object_type"] == "table" and a["table_id"] is not None:
            out.setdefault(a["base_tag_id"], set()).add(a["table_id"])
    return out


def _and(predicates: list[str]) -> str:
    if len(predicates) == 1:
        return predicates[0]
    return " AND ".join(f"({p})" for p in predicates)


def resolve_table_rules(
    rls_rules: list[dict],
    tables: Iterable[dict],
    chains: dict[str, list[str]],
    tag_assignments: Iterable[dict],
    parents: dict[str, str | None],
) -> list[dict]:
    """The rule list with every (role, table) pair's filters folded into one table-level rule.

    ``rls_rules`` is the list ``materialize_rls`` returns. Domain and action rules pass through
    unchanged (an action still resolves its own); table rules are replaced by the folded ones;
    tag rules are consumed. ``parents`` is the domain tree (``domain_tree.parent_map``).
    """
    from provisa.security.domain_tree import ancestors

    table_rules: dict[tuple[str, int], str] = {}
    domain_rules: dict[tuple[str, str], str] = {}
    tag_rules: dict[tuple[str, str], str] = {}
    out: list[dict] = []
    for rule in rls_rules:
        if rule.get("action_name"):
            out.append(rule)
        elif rule.get("tag_id"):
            tag_rules[(rule["role_id"], rule["tag_id"])] = rule["filter_expr"]
        elif rule.get("table_id") is not None:
            table_rules[(rule["role_id"], rule["table_id"])] = rule["filter_expr"]
        elif rule.get("domain_id"):
            domain_rules[(rule["role_id"], rule["domain_id"])] = rule["filter_expr"]
            out.append(rule)

    tags_by_table: dict[int, list[str]] = {}
    for tag_id, table_ids in tagged_table_ids(tag_assignments).items():
        for tid in table_ids:
            tags_by_table.setdefault(tid, []).append(tag_id)

    for rid, chain in chains.items():
        for t in tables:
            tid = t["id"]
            predicates: list[str] = []
            base = table_rules.get((rid, tid))
            if base is None:
                base = domain_rules.get((rid, t.get("domain_id") or ""))
            if base is not None:
                predicates.append(base)
            for above in ancestors(t.get("domain_id") or "", parents):
                inherited = domain_rules.get((rid, above))
                if inherited is not None:
                    predicates.append(inherited)
            for tag_id in sorted(tags_by_table.get(tid, ())):
                for anc in chain:
                    tagged = tag_rules.get((anc, tag_id))
                    if tagged is not None:
                        predicates.append(tagged)
            if predicates:
                out.append(
                    {
                        "id": None,
                        "role_id": rid,
                        "table_id": tid,
                        "domain_id": None,
                        "action_name": None,
                        "tag_id": None,
                        "filter_expr": _and(predicates),
                    }
                )
    return out


def resolve_rules_by_right(
    rls_rules: list[dict],
    tables: list[dict],
    parents: dict[str, list[str]],
    actions: Iterable[dict],
    chains: dict[str, list[str]],
    tag_assignments: list[dict],
    domain_parents: dict[str, str | None],
) -> list[dict]:  # REQ-2188
    """Each right's rules resolved on their own, as a read's are: child precedence
    (``materialize_rls``), then one predicate per (role, table) over the table, its domain and its
    tags (:func:`resolve_table_rules`). A U rule never stands in for an R rule, or the reverse:
    a role with no rule for a right is not narrowed by a rule for another. One rule carrying
    several rights is read once for each, so every right is one plain predicate: a read stays a
    pushed-down filter, never a CASE. Every row comes back naming the one right it stands for,
    and a stored rule with none is an R rule."""
    from provisa.security.inheritance import materialize_rls

    out: list[dict] = []
    for right in RIGHTS:
        own = [r for r in rls_rules if right in r.get("rights", "R")]
        if right != "R" and not own:
            continue
        materialized = materialize_rls(own, tables, parents, actions)
        resolved = resolve_table_rules(
            materialized, tables, chains, tag_assignments, domain_parents
        )
        out.extend({**rule, "right": right} for rule in resolved)
    return out


def model_write_rules(
    resolved: Iterable[dict], tables: Iterable[dict]
) -> dict[tuple[str, str, str], str]:  # REQ-2188
    """(role, table, right) -> predicate: the U and D rules that hold the model store. A semantic
    object's table is registered in the meta domain, by its own name; a rule on any other table
    holds data, through write admission."""
    from provisa.security.rights import META_DOMAIN_ID

    model = {t["id"]: t["table_name"] for t in tables if t.get("domain_id") == META_DOMAIN_ID}
    return {
        (r["role_id"], model[r["table_id"]], r["right"]): r["filter_expr"]
        for r in resolved
        if r.get("right", "R") != "R" and r.get("table_id") in model
    }


def acting_model_rule(state: Any, role_id: str, table: str, right: str) -> Any:  # REQ-2188
    """The rule the store holds ``role_id``'s ``right`` on ``table`` to, or None when it is not
    narrowed (``core.model_row_rules``). A meta-role takes the OR of its members' rules, each with
    its own constants inlined: a member with no rule lifts it only if it holds RW on the model, so
    a member with only R neither widens the set nor narrows it."""
    from provisa.core.model_row_rules import ModelRule
    from provisa.core.request_context import session_vars_for
    from provisa.security.meta_role import _inlined, _or

    inputs = state.role_build_inputs
    if not inputs:  # a first start: nothing is built, so no rule is in force
        return None
    rules = inputs["model_write_rules"]
    view = inputs["physical_table_map"].get(table)
    members = state.meta_roles.get(role_id)
    if members is None:
        predicate = rules.get((role_id, table, right))
    else:
        terms: list[str] = []
        for member in members:
            role = state.roles[member]
            own = rules.get((member, table, right))
            if own is not None:
                terms.append(_inlined(own, role.get("session_vars") or {}))
            elif MODEL_RW_RIGHTS.get(table) in (role.get("capabilities") or []):
                return None
        predicate = _or(terms) if terms else None
    if predicate is None:
        return None
    return ModelRule(
        predicate=predicate, session_vars=session_vars_for(state.roles.get(role_id)), view=view
    )
