# Copyright (c) 2026 Kenneth Stott
# Canary: db2c4d6d-b739-4bd0-885d-77cdaac69fe1
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Natural-language query within the domains selected in the domain filter (REQ-2253).

Every model input the NL pipeline builds -- the table list table selection reads, the schema block
SQL generation reads, the GraphQL SDL the GraphQL, Cypher and strict branches read, the label map
Cypher is checked and hinted against, and the metrics the matcher grounds -- is narrowed here to
the selected domains, so nothing of a domain outside them is sent to the model or chosen.

A scope of ``None`` is no selection: the role's own surface, unchanged.
"""

# Requirements: REQ-2253

from __future__ import annotations

from copy import copy
from typing import TYPE_CHECKING, Any, Iterable, Mapping

if TYPE_CHECKING:
    from graphql import GraphQLSchema

    from provisa.gql.label_map import CypherLabelMap

Scope = frozenset[str] | None


def selected_scope(role: Mapping[str, Any], selected: Iterable[str] | None) -> Scope:
    """The ``role``'s ``domain_access`` INTERSECTED with the domains the filter selected.

    The filter is a view preference and never widens authority: a role reaching every domain
    (``"*"``) narrowed by a selection sees only the selection, and a role reaching some sees the
    ones of the selection it reaches. ``None`` (no selection sent: every domain checked) leaves
    the role's surface alone, and its authority is not read for it.
    """
    from provisa.security.rights import reaches_all_domains

    if selected is None:
        return None
    chosen = frozenset(selected)
    access = list(role["domain_access"])
    if reaches_all_domains(access):
        return chosen
    return chosen & frozenset(access)


def label_map_within(lm: "CypherLabelMap", scope: Scope) -> "CypherLabelMap":
    """``lm`` with only the nodes of ``scope``'s domains, and only the relationships between two of
    them, so neither a table list, a multi-hop path nor a relationship hint reaches outside it."""
    if scope is None:
        return lm
    from provisa.gql.label_map import CypherLabelMap

    nodes = {tn: nm for tn, nm in lm.nodes.items() if nm.domain_id in scope}
    sql_only = {tn: nm for tn, nm in lm.sql_only_nodes.items() if nm.domain_id in scope}
    kept = set(nodes) | set(sql_only)

    def _within(rm: Any) -> bool:
        return rm.source_label in kept and rm.target_label in kept

    def _keep_lists(held: dict[str, list[str]]) -> dict[str, list[str]]:
        out = {k: [t for t in v if t in kept] for k, v in held.items()}
        return {k: v for k, v in out.items() if v}

    def _keep_aliases(held: dict[str, list[Any]]) -> dict[str, list[Any]]:
        out = {k: [rm for rm in v if _within(rm)] for k, v in held.items()}
        return {k: v for k, v in out.items() if v}

    return CypherLabelMap(
        nodes=nodes,
        relationships={k: rm for k, rm in lm.relationships.items() if _within(rm)},
        domains=_keep_lists(lm.domains),
        nodes_by_table=_keep_lists(lm.nodes_by_table),
        aliases=_keep_aliases(lm.aliases),
        sql_only_nodes=sql_only,
        sql_only_tables={k: t for k, t in lm.sql_only_tables.items() if t in sql_only},
        sql_only_relationships={
            k: rm for k, rm in lm.sql_only_relationships.items() if _within(rm)
        },
        sql_only_aliases=_keep_aliases(lm.sql_only_aliases),
    )


def _table_metas(ctx: Any) -> list[Any]:
    """Every TableMeta ``ctx`` serves: its root tables and its join targets."""
    metas = {tm.type_name: tm for tm in ctx.tables.values()}
    for jm in ctx.joins.values():
        metas.setdefault(jm.target.type_name, jm.target)
    return list(metas.values())


def metrics_within(metrics: list[Any], ctx: Any, scope: Scope) -> list[Any]:
    """The metrics whose every referenced table is one of ``scope``'s domains."""
    if scope is None:
        return metrics
    from provisa.compiler.metric_expand import metric_reference_tables
    from provisa.compiler.sql_rewrite import semantic_table_name

    in_scope: set[str] = set()
    for tm in _table_metas(ctx):
        if tm.domain_id in scope:
            in_scope.update(
                n.lower()
                for n in (tm.table_name, tm.original_table_name, semantic_table_name(tm))
                if n
            )
    return [
        m
        for m in metrics
        if all(t.lower() in in_scope for t in metric_reference_tables(m.name, m.expression))
    ]


def _named(type_node: Any) -> str:
    """The named type under a type reference's list and non-null wrappers."""
    while not hasattr(type_node, "name"):
        type_node = type_node.type
    return type_node.name.value


def _required(type_node: Any) -> bool:
    from graphql.language import NonNullTypeNode

    return isinstance(type_node, NonNullTypeNode)


def schema_within(schema: "GraphQLSchema", ctx: Any, scope: Scope) -> "GraphQLSchema":
    """``schema`` with no type of a table outside ``scope``'s domains, and no field reaching one.

    A generated type is its table's when the table's type name is its longest table-type-name
    prefix (``Orders``, ``OrdersWhere``, ``OrdersAggregate``, ``OrdersMetricsFields``...). Those
    types are dropped, then every field, argument, input field and union member naming a dropped
    type, then -- repeatedly -- any type left with nothing in it, and finally any type no root
    reaches. A collision that drops an in-scope table's helper type only drops an optional
    argument or field of it; it never leaves an out-of-scope table in.
    """
    if scope is None:
        return schema
    from graphql import build_ast_schema, parse, print_schema
    from graphql.language import (
        DocumentNode,
        InputObjectTypeDefinitionNode,
        InterfaceTypeDefinitionNode,
        ObjectTypeDefinitionNode,
        SchemaDefinitionNode,
        TypeDefinitionNode,
        UnionTypeDefinitionNode,
    )

    domain_of = {tm.type_name: tm.domain_id for tm in _table_metas(ctx)}
    by_length = sorted(domain_of, key=len, reverse=True)

    def _outside(name: str) -> bool:
        owner = next((t for t in by_length if name.startswith(t)), None)
        return owner is not None and domain_of[owner] not in scope

    doc = parse(print_schema(schema))
    types: dict[str, Any] = {
        d.name.value: copy(d) for d in doc.definitions if isinstance(d, TypeDefinitionNode)
    }
    others = [d for d in doc.definitions if not isinstance(d, TypeDefinitionNode)]
    removed = {name for name in types if _outside(name)}

    changed = True
    while changed:
        changed = False
        for name, node in types.items():
            if name in removed:
                continue
            if isinstance(node, (ObjectTypeDefinitionNode, InterfaceTypeDefinitionNode)):
                fields = []
                for f in node.fields or ():
                    args = f.arguments or ()
                    if _named(f.type) in removed or any(
                        _named(a.type) in removed and _required(a.type) for a in args
                    ):
                        continue
                    kept_args = tuple(a for a in args if _named(a.type) not in removed)
                    if len(kept_args) != len(args):
                        f = copy(f)
                        f.arguments = kept_args
                    fields.append(f)
                interfaces = tuple(i for i in node.interfaces or () if i.name.value not in removed)
                if len(fields) != len(node.fields or ()) or len(interfaces) != len(
                    node.interfaces or ()
                ):
                    node.fields, node.interfaces = tuple(fields), interfaces
                    changed = True
                if not fields:
                    removed.add(name)
                    changed = True
            elif isinstance(node, InputObjectTypeDefinitionNode):
                fields = node.fields or ()
                if any(_named(f.type) in removed and _required(f.type) for f in fields):
                    removed.add(name)
                    changed = True
                    continue
                kept = tuple(f for f in fields if _named(f.type) not in removed)
                if len(kept) != len(fields):
                    node.fields = kept
                    changed = True
                if not kept:
                    removed.add(name)
                    changed = True
            elif isinstance(node, UnionTypeDefinitionNode):
                members = tuple(t for t in node.types or () if t.name.value not in removed)
                if len(members) != len(node.types or ()):
                    node.types = members
                    changed = True
                if not members:
                    removed.add(name)
                    changed = True

    live = {n: d for n, d in types.items() if n not in removed}
    roots = [
        op.type.name.value
        for d in others
        if isinstance(d, SchemaDefinitionNode)
        for op in d.operation_types
    ] or [n for n in ("Query", "Mutation", "Subscription") if n in live]

    def _refs(node: Any) -> list[str]:
        out: list[str] = []
        for f in getattr(node, "fields", None) or ():
            out.append(_named(f.type))
            out.extend(_named(a.type) for a in getattr(f, "arguments", None) or ())
        out.extend(i.name.value for i in getattr(node, "interfaces", None) or ())
        out.extend(t.name.value for t in getattr(node, "types", None) or ())
        return out

    implementers: dict[str, list[str]] = {}
    for n, d in live.items():
        for i in getattr(d, "interfaces", None) or ():
            implementers.setdefault(i.name.value, []).append(n)
    reached: set[str] = set()
    stack = [r for r in roots if r in live]
    for d in others:  # directive arguments' types
        stack.extend(_named(a.type) for a in getattr(d, "arguments", None) or ())
    while stack:
        n = stack.pop()
        if n in reached or n not in live:
            continue
        reached.add(n)
        stack.extend(_refs(live[n]))
        stack.extend(implementers.get(n, ()))

    kept_others = []
    for d in others:
        if isinstance(d, SchemaDefinitionNode):
            d = copy(d)
            d.operation_types = tuple(
                op for op in d.operation_types if op.type.name.value in reached
            )
        kept_others.append(d)
    pruned = DocumentNode(definitions=(*kept_others, *(d for n, d in live.items() if n in reached)))
    return build_ast_schema(pruned)
