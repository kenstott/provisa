# Copyright (c) 2026 Kenneth Stott
# Canary: 7613fd1a-396e-4369-a127-90c1ad0952ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL read lowered to the graph IR (REQ-2135, REQ-863): each root field one statement, its
selection the traversal a Cypher read of the same data is.

- A root field is ``MATCH (n0:Label)`` with its where as the WHERE, its order_by, offset and limit
  the statement's, and each scalar field it selects a RETURN item named by the field's response key.
- A to-many relationship field is ``COLLECT { MATCH (n)-[:REL]->(m:Label) WHERE … RETURN {…}
  ORDER BY … SKIP … LIMIT … }``: the related rows, each a map keyed by response keys, that level's
  where, order_by, offset and limit its own. A to-one relationship field is the same with at most
  one row: an OPTIONAL MATCH of the level it is in, its fields the map
  ``CASE WHEN m.<key> IS NULL THEN NULL ELSE {…} END`` (null when there is no related row).
- A group-by root field is the same MATCH returning ``{key: n0.key, …} AS groupKey`` (each column
  it groups by) and ``{count: count(*), sum: {col: sum(n0.col), …}, …} AS aggregate``: Cypher
  groups by the items that aggregate nothing, so by the group key. Its order_by orders by a group
  key (``groupKey.key``) or an aggregate.
- A where is the expression tree its operators state: eq, neq, gt, gte, lt, lte, in, nin,
  is_null, _and, _or, _not. (Cypher has no LIKE: like and ilike are refused.)

What the front end does not lower yet is refused by name (GraphQLLoweringRefused), never lowered
otherwise: the GraphQL compiler goes on answering it until the front end does.
"""

# Requirements: REQ-2135, REQ-863

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from graphql import FieldNode, OperationDefinitionNode

from provisa.gql.expr_ast import (
    Binary,
    Case,
    CypherExpr,
    FunctionCall,
    IsNull,
    ListLiteral,
    Literal,
    MapLiteral,
    Property,
    SubqueryExpr,
    Unary,
    Variable,
)
from provisa.gql.ir import (
    CypherAST,
    MatchClause,
    MatchStep,
    NodePattern,
    OrderItem,
    PathPattern,
    RelPattern,
    ReturnClause,
    ReturnItem,
    WhereClause,
    WithClause,
)


class GraphQLLoweringRefused(ValueError):
    """The read holds a construct the GraphQL front end does not lower; the message names it."""


#: A field's arguments the front end lowers; any other is refused by name.
_ARGUMENTS = frozenset({"where", "order_by", "limit", "offset"})
_GROUP_BY_ARGUMENTS = _ARGUMENTS | {"by"}
#: A group-by's aggregate functions, as Cypher names them.
_AGGREGATES = {"sum": "sum", "avg": "avg", "min": "min", "max": "max"}
_COMPARISONS = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
#: GraphQL's directions, as Cypher orders (its nulls last ascending, first descending).
_DIRECTIONS = {
    "asc": "ASC",
    "asc_nulls_last": "ASC",
    "desc": "DESC",
    "desc_nulls_first": "DESC",
}


@dataclass
class LoweredRoot:
    """A root field of the read, lowered: its response key and its statement."""

    response_key: str
    statement: CypherAST


@dataclass
class _Lowering:
    ctx: Any  # CompilationContext
    lm: Any  # CypherLabelMap
    variables: dict | None
    _next: int = 0
    _labels: dict[str, str] = field(default_factory=dict)

    def var(self) -> str:
        name = f"n{self._next}"
        self._next += 1
        return name

    def node_of(self, table: Any) -> Any:
        nm = self.lm.nodes.get(table.type_name)
        if nm is None:
            raise GraphQLLoweringRefused(f"{table.field_name} is no node of the graph")
        return nm

    def prop(self, table: Any, nm: Any, field_name: str) -> str:
        column = self.ctx.exposed_to_physical.get((table.table_id, field_name), field_name)
        prop = nm.property_of(column)
        if prop is None or prop not in nm.properties:
            raise GraphQLLoweringRefused(f"{field_name} of {table.field_name} is no property")
        return prop

    # -- values --

    def value(self, node: Any) -> Any:
        from provisa.compiler.sql_where import _extract_value

        return _extract_value(node, self.variables)

    def literal(self, v: Any) -> CypherExpr:
        if isinstance(v, bool):
            return Literal(v, "boolean")
        if isinstance(v, (int, float)):
            return Literal(v, "number")
        if isinstance(v, str):
            return Literal(v, "string")
        if v is None:
            return Literal(None, "null")
        raise GraphQLLoweringRefused(f"the value {v!r}")

    # -- where --

    def where(self, table: Any, nm: Any, var: str, obj: dict) -> CypherExpr | None:
        parts: list[CypherExpr] = []
        for key, value in obj.items():
            if key in ("_and", "_or"):
                subs = [s for s in (self.where(table, nm, var, x) for x in value) if s is not None]
                if not subs:
                    parts.append(Literal(key == "_and", "boolean"))
                    continue
                joined = subs[0]
                for s in subs[1:]:
                    joined = Binary("AND" if key == "_and" else "OR", joined, s)
                parts.append(joined)
                continue
            if key == "_not":
                inner = self.where(table, nm, var, value)
                parts.append(Unary("NOT", inner if inner is not None else Literal(True, "boolean")))
                continue
            if (table.type_name, key) in self.ctx.joins:
                raise GraphQLLoweringRefused(f"a condition on the relationship {key}")
            column = Property(Variable(var), self.prop(table, nm, key))
            for op, v in value.items():
                parts.append(self.condition(column, op, v))
        if not parts:
            return None
        out = parts[0]
        for p in parts[1:]:
            out = Binary("AND", out, p)
        return out

    def condition(self, column: CypherExpr, op: str, v: Any) -> CypherExpr:
        if op in _COMPARISONS:
            return Binary(_COMPARISONS[op], column, self.literal(v))
        if op == "in":
            return Binary("IN", column, ListLiteral([self.literal(x) for x in v]))
        if op == "nin":
            return Unary("NOT", Binary("IN", column, ListLiteral([self.literal(x) for x in v])))
        if op == "is_null":
            return IsNull(column, negated=not v)
        raise GraphQLLoweringRefused(f"the condition {op}")

    # -- a level --

    def arguments(self, sel: FieldNode, allowed: frozenset[str] = _ARGUMENTS) -> dict:
        args = {a.name.value: a.value for a in sel.arguments or ()}
        unknown = sorted(set(args) - allowed)
        if unknown:
            raise GraphQLLoweringRefused(f"the argument {unknown[0]} of {sel.name.value}")
        return {k: self.value(v) for k, v in args.items()}

    def order(self, table: Any, nm: Any, var: str, order_by: Any) -> list[OrderItem]:
        items = order_by if isinstance(order_by, list) else [order_by]
        out: list[OrderItem] = []
        for item in items:
            for key, direction in item.items():
                if isinstance(direction, dict):
                    raise GraphQLLoweringRefused(f"an order by the relationship {key}")
                out.append(
                    OrderItem(
                        Property(Variable(var), self.prop(table, nm, key)), _direction(direction)
                    )
                )
        return out

    def selection(
        self, table: Any, nm: Any, var: str, sel: FieldNode, steps: list[MatchStep | WithClause]
    ) -> list[tuple[str, CypherExpr]]:
        """Each field of ``sel``'s selection: its response key and the expression it is. A to-one
        field's OPTIONAL MATCH is added to ``steps``, the statement of the level it is in."""
        out: list[tuple[str, CypherExpr]] = []
        assert sel.selection_set is not None
        for f in sel.selection_set.selections:
            if not isinstance(f, FieldNode):
                raise GraphQLLoweringRefused("a fragment")
            key = f.alias.value if f.alias else f.name.value
            if f.name.value.startswith("_"):
                raise GraphQLLoweringRefused(f"the field {f.name.value}")
            join = self.ctx.joins.get((table.type_name, f.name.value))
            if join is None:
                if f.arguments:
                    raise GraphQLLoweringRefused(f"arguments on the column {f.name.value}")
                out.append((key, Property(Variable(var), self.prop(table, nm, f.name.value))))
                continue
            out.append((key, self.related(table, var, f, join, steps)))
        return out

    def hop(
        self, table: Any, var: str, f: FieldNode, join: Any
    ) -> tuple[Any, Any, str, PathPattern]:
        """The relationship ``f`` names as one hop from ``var``: (its target, the target's node,
        the target's variable, the pattern)."""
        if (
            join.via is not None
            or join.source_expr
            or join.target_expr
            or join.source_constant is not None
        ):
            raise GraphQLLoweringRefused(f"the relationship {f.name.value} (not a column join)")
        if join.default_limit is not None:
            raise GraphQLLoweringRefused(f"the relationship {f.name.value} (a default limit)")
        rels = [
            r
            for r in self.lm.relationships.values()
            if r.field_name == f.name.value and r.source_label == table.type_name
        ]
        if len(rels) != 1:
            raise GraphQLLoweringRefused(f"the relationship {f.name.value} (no one edge type)")
        target = join.target
        tnm = self.node_of(target)
        child = self.var()
        pattern = PathPattern(
            [NodePattern(var, [], {}), NodePattern(child, [self.lm.display_label(tnm)], {})],
            [RelPattern(None, [rels[0].rel_type], None, None, "right")],
        )
        return target, tnm, child, pattern

    def related(
        self, table: Any, var: str, f: FieldNode, join: Any, steps: list[MatchStep | WithClause]
    ) -> CypherExpr:
        target, tnm, child, pattern = self.hop(table, var, f, join)
        args = self.arguments(f)
        if join.cardinality != "one-to-many":
            if args:
                raise GraphQLLoweringRefused(f"arguments on the to-one relationship {f.name.value}")
            # At most one related row: matched beside the level's own, a null map when none.
            steps.append(MatchStep([MatchClause(pattern, None, optional=True)], None))
            fields = MapLiteral(self.selection(target, tnm, child, f, steps))
            key = Property(Variable(child), tnm.property_of(tnm.id_column))
            return Case(None, [(IsNull(key, negated=False), Literal(None, "null"))], fields)
        body_steps: list[MatchStep | WithClause] = [
            MatchStep(
                [MatchClause(pattern, None)],
                _where(self.where(target, tnm, child, args["where"]) if "where" in args else None),
            )
        ]
        body = CypherAST(
            pipeline=body_steps,
            return_clause=ReturnClause(
                [
                    ReturnItem(
                        MapLiteral(self.selection(target, tnm, child, f, body_steps)), alias="row"
                    )
                ]
            ),
            order_by=self.order(target, tnm, child, args["order_by"]) if "order_by" in args else [],
            skip=args.get("offset"),
            limit=args.get("limit"),
        )
        return SubqueryExpr("COLLECT", body)

    # -- a group-by root --

    def aggregate(self, table: Any, nm: Any, var: str, func: str, column: str) -> CypherExpr:
        if func == "count":
            return FunctionCall("count", [], star=True)
        if func not in _AGGREGATES:
            raise GraphQLLoweringRefused(f"the aggregate {func}")
        return FunctionCall(
            _AGGREGATES[func], [Property(Variable(var), self.prop(table, nm, column))]
        )

    def group_by(
        self, table: Any, nm: Any, var: str, sel: FieldNode, args: dict
    ) -> tuple[list[ReturnItem], list[OrderItem]]:
        """A group-by root's RETURN items (groupKey, aggregate) and its ORDER BY."""
        by = args.get("by") or []
        by = [by] if isinstance(by, str) else by
        items: list[ReturnItem] = []
        if by:
            key = MapLiteral([(c, Property(Variable(var), self.prop(table, nm, c))) for c in by])
            items.append(ReturnItem(key, alias="groupKey"))
        assert sel.selection_set is not None
        for f in sel.selection_set.selections:
            if not isinstance(f, FieldNode):
                raise GraphQLLoweringRefused("a fragment")
            if f.alias or f.arguments:
                raise GraphQLLoweringRefused(f"an alias or arguments on {f.name.value}")
            if f.name.value == "groupKey":
                continue
            if f.name.value != "aggregate":
                raise GraphQLLoweringRefused(f"the group-by field {f.name.value}")
            assert f.selection_set is not None
            entries: list[tuple[str, CypherExpr]] = []
            for a in f.selection_set.selections:
                if not isinstance(a, FieldNode) or a.alias or a.arguments:
                    raise GraphQLLoweringRefused("a fragment, alias or arguments in aggregate")
                func = a.name.value
                if func == "count":
                    entries.append(("count", self.aggregate(table, nm, var, "count", "")))
                    continue
                if func not in _AGGREGATES or a.selection_set is None:
                    raise GraphQLLoweringRefused(f"the aggregate {func}")
                columns: list[tuple[str, CypherExpr]] = []
                for c in a.selection_set.selections:
                    if not isinstance(c, FieldNode):
                        raise GraphQLLoweringRefused("a fragment")
                    columns.append(
                        (
                            c.alias.value if c.alias else c.name.value,
                            self.aggregate(table, nm, var, func, c.name.value),
                        )
                    )
                entries.append((func, MapLiteral(columns)))
            if entries:
                items.append(ReturnItem(MapLiteral(entries), alias="aggregate"))
        order: list[OrderItem] = []
        order_by = args.get("order_by") or []
        for entry in order_by if isinstance(order_by, list) else [order_by]:
            for kind, spec in entry.items():
                if kind == "groupKey":
                    for column, direction in spec.items():
                        if column not in by:
                            raise GraphQLLoweringRefused(f"an order by {column}, no group key")
                        # The returned group key's value: a key is all a grouped RETURN can read.
                        key = Property(Variable("groupKey"), column)
                        order.append(OrderItem(key, _direction(direction)))
                elif kind == "aggregate":
                    for func, value in spec.items():
                        if func == "count":
                            order.append(
                                OrderItem(
                                    self.aggregate(table, nm, var, "count", ""), _direction(value)
                                )
                            )
                            continue
                        for column, direction in value.items():
                            order.append(
                                OrderItem(
                                    self.aggregate(table, nm, var, func, column),
                                    _direction(direction),
                                )
                            )
                else:
                    raise GraphQLLoweringRefused(f"the group-by order {kind}")
        return items, order


def _direction(direction: str) -> str:
    if direction not in _DIRECTIONS:
        raise GraphQLLoweringRefused(f"the order {direction}")
    return _DIRECTIONS[direction]


def _where(expr: CypherExpr | None) -> WhereClause | None:
    return None if expr is None else WhereClause(expr)


def lower_read(
    document: Any, ctx: Any, label_map: Any, variables: dict | None
) -> list[LoweredRoot]:
    """Each root field of ``document`` (validated against the role's schema) as its statement, in
    the query's order, or :class:`GraphQLLoweringRefused` naming what it does not lower."""
    operations = [d for d in document.definitions if isinstance(d, OperationDefinitionNode)]
    if len(operations) != 1 or operations[0].operation.value != "query":
        raise GraphQLLoweringRefused("a document that is not one query")
    out: list[LoweredRoot] = []
    for sel in operations[0].selection_set.selections:
        if not isinstance(sel, FieldNode):
            raise GraphQLLoweringRefused("a fragment")
        lowering = _Lowering(ctx, label_map, variables)
        table = ctx.tables.get(sel.name.value)
        grouped = table is not None and sel.name.value == f"{table.field_name}GroupBy"
        if table is None or (table.field_name != sel.name.value and not grouped):
            raise GraphQLLoweringRefused(f"the root field {sel.name.value}")
        nm = lowering.node_of(table)
        var = lowering.var()
        args = lowering.arguments(sel, _GROUP_BY_ARGUMENTS if grouped else _ARGUMENTS)
        steps: list[MatchStep | WithClause] = [
            MatchStep(
                [
                    MatchClause(
                        PathPattern([NodePattern(var, [label_map.display_label(nm)], {})], []),
                        None,
                    )
                ],
                _where(lowering.where(table, nm, var, args["where"]) if "where" in args else None),
            )
        ]
        if grouped:
            returned, order = lowering.group_by(table, nm, var, sel, args)
        else:
            returned = [
                ReturnItem(expr, alias=key)
                for key, expr in lowering.selection(table, nm, var, sel, steps)
            ]
            order = lowering.order(table, nm, var, args["order_by"]) if "order_by" in args else []
        statement = CypherAST(
            pipeline=steps,
            return_clause=ReturnClause(returned),
            order_by=order,
            skip=args.get("offset"),
            limit=args.get("limit"),
        )
        out.append(LoweredRoot(sel.alias.value if sel.alias else sel.name.value, statement))
    return out
