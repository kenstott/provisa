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
- An aggregate root field is the same MATCH returning ``{count: count(*), sum: {…}, …} AS
  aggregate`` and, for its nodes, ``collect({col: n0.col, …}) AS nodes``: one row, of every row
  its where keeps.
- A where is the expression tree its operators state: eq, neq, gt, gte, lt, lte, in, nin,
  is_null, _and, _or, _not. (Cypher has no LIKE: like and ilike are refused.)

What the front end does not lower yet is refused by name (GraphQLLoweringRefused), never lowered
otherwise: the GraphQL compiler goes on answering it until the front end does.
"""

# Requirements: REQ-2135, REQ-863

from __future__ import annotations

import re
from collections.abc import Callable
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
    MapProjection,
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
_AGGREGATE_ARGUMENTS = frozenset({"where"})
#: An aggregate's functions, as Cypher names them (stDev is the sample's, as SQL's STDDEV is).
_AGGREGATES = {"sum": "sum", "avg": "avg", "min": "min", "max": "max", "stddev": "stDev"}
_COMPARISONS = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
#: GraphQL's directions, as Cypher orders (its nulls last ascending, first descending).
_DIRECTIONS = {
    "asc": "ASC",
    "asc_nulls_last": "ASC",
    "desc": "DESC",
    "desc_nulls_first": "DESC",
}


#: What a statement projects (REQ-2135): the read's answer as GraphQL shapes it (each row a map
#: keyed by response keys), its fields under the graph's own names (flat at the root, a related
#: row its node's map projection), or its nodes and relationships alone. The same patterns, the
#: same subqueries: only what each RETURN projects differs.
GRAPHQL, FIELDS, NODES = "graphql", "fields", "nodes"
PROJECTIONS = (GRAPHQL, FIELDS, NODES)


@dataclass
class _Entry:
    """A field as its level projects it: ``expr``; or, for a to-one field outside the GraphQL
    shape, ``level``, its own fields read off the node ``var``. ``prop`` is set on a node's own
    property (its map projection's ``.prop``)."""

    key: str
    expr: CypherExpr | None
    prop: str | None = None
    var: str | None = None
    level: list[_Entry] | None = None

    def expression(self) -> CypherExpr:
        assert self.expr is not None, f"{self.key} is a to-one level"
        return self.expr


def _entry_value(e: _Entry) -> CypherExpr:
    """``e`` inside a map: its expression, or a to-one level's map projection."""
    return e.expression() if e.level is None else _map_projection(e.level, e.var)


@dataclass
class _Junction:
    """A relationship field whose target is a junction: its rows are relationships between the
    near node and the far one (REQ-1586)."""

    table: Any  # the junction's TableMeta
    rels: list  # RelationshipMapping through it, the near node at the end it is
    near_is_source: bool
    far: Any  # the far node's TableMeta
    fnm: Any  # the far node's NodeMapping
    near_column: str  # the junction's column of the near node's key
    far_column: str
    near_key: str  # the near node's key column
    far_key: str
    rel_var: str
    far_var: str


def _flatten(entries: list[_Entry]) -> list[CypherExpr]:
    out: list[CypherExpr] = []
    for e in entries:
        if e.level is not None:
            out += _flatten(e.level)
        else:
            out.append(e.expression())
    return out


def _map_projection(entries: list[_Entry], var: str | None) -> MapProjection:
    """A node's fields as its map projection: ``var{.prop, …, field: …}``."""
    assert var is not None
    return MapProjection(
        var,
        properties=[e.prop for e in entries if e.prop is not None],
        literal_entries=[(e.key, _entry_value(e)) for e in entries if e.prop is None],
    )


def _conjuncts(obj: dict) -> list[dict]:
    """``obj`` as the conditions it ANDs, one key each."""
    out: list[dict] = []
    for key, value in obj.items():
        if key == "_and":
            for item in value:
                out += _conjuncts(item)
        else:
            out.append({key: value})
    return out


def _like(column: CypherExpr, pattern: str, *, case_insensitive: bool) -> CypherExpr:
    """``column LIKE pattern`` (ILIKE: case ignored) as Cypher, exactly: a pattern whose only
    wildcards are a leading or trailing ``%`` is ``=``, STARTS WITH, ENDS WITH or CONTAINS; any
    other a whole-string regex, ``%`` any run of characters and ``_`` one. A backslash takes the
    next character literally, as LIKE's default escape does."""
    if case_insensitive:
        column = FunctionCall("toLower", [column])
    # Each a literal run ("", text), or a wildcard ("%" any run, "_" one character).
    tokens: list[tuple[str, str]] = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        i += 1
        if c == "\\" and i < len(pattern):
            c, i = pattern[i], i + 1
        elif c in "%_":
            if not (c == "%" and tokens[-1:] == [("%", "")]):
                tokens.append((c, ""))
            continue
        if tokens and tokens[-1][0] == "":
            tokens[-1] = ("", tokens[-1][1] + c)
        else:
            tokens.append(("", c))
    lead, trail = tokens[:1] == [("%", "")], tokens[-1:] == [("%", "")]
    middle = tokens[int(lead) : len(tokens) - int(trail)]
    if len(middle) <= 1 and all(kind == "" for kind, _ in middle):
        value: CypherExpr = Literal(middle[0][1] if middle else "", "string")
        if case_insensitive:
            value = FunctionCall("toLower", [value])
        op = {(False, False): "=", (False, True): "STARTS WITH", (True, False): "ENDS WITH"}
        return Binary(op.get((lead, trail), "CONTAINS"), column, value)
    # A regex is a literal the reader anchors whole: its text is lowered here, as toLower would.
    regex = "".join(
        ".*"
        if kind == "%"
        else "."
        if kind == "_"
        else re.escape(text.lower() if case_insensitive else text)
        for kind, text in tokens
    )
    # (?s): a wildcard matches a newline too, as LIKE's do.
    return Binary("=~", column, Literal("(?s)" + regex, "string"))


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
    projection: str = GRAPHQL
    _next: int = 0
    _next_rel: int = 0
    _labels: dict[str, str] = field(default_factory=dict)

    def var(self) -> str:
        name = f"n{self._next}"
        self._next += 1
        return name

    def rel_var(self) -> str:
        name = f"r{self._next_rel}"
        self._next_rel += 1
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
        """``obj`` on a node's rows: a column's conditions on its property, a relationship's
        (EXISTS) on the related rows."""
        return self.conditions(
            obj,
            lambda key: Property(Variable(var), self.prop(table, nm, key)),
            lambda key, value: self.related_condition(table, var, key, value),
        )

    def conditions(
        self,
        obj: dict,
        column: Callable[[str], CypherExpr],
        relation: Callable[[str, Any], CypherExpr | None],
    ) -> CypherExpr | None:
        """``obj``'s conditions joined: ``relation`` answers a relationship's (None: ``key`` is
        none), ``column`` names the expression a column's conditions are on."""
        parts: list[CypherExpr] = []
        for key, value in obj.items():
            if key in ("_and", "_or"):
                subs = [s for s in (self.conditions(x, column, relation) for x in value) if s]
                if not subs:
                    parts.append(Literal(key == "_and", "boolean"))
                    continue
                joined = subs[0]
                for s in subs[1:]:
                    joined = Binary("AND" if key == "_and" else "OR", joined, s)
                parts.append(joined)
                continue
            if key == "_not":
                inner = self.conditions(value, column, relation)
                parts.append(Unary("NOT", inner if inner is not None else Literal(True, "boolean")))
                continue
            related = relation(key, value)
            if related is not None:
                parts.append(related)
                continue
            target = column(key)
            for op, v in value.items():
                parts.append(self.condition(target, op, v))
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
        if op in ("like", "ilike"):
            if not isinstance(v, str):
                raise GraphQLLoweringRefused(f"the pattern {v!r}")
            return _like(column, v, case_insensitive=op == "ilike")
        raise GraphQLLoweringRefused(f"the condition {op}")

    def related_condition(self, table: Any, var: str, key: str, value: Any) -> CypherExpr | None:
        """A condition on the relationship ``key``: a related row its condition keeps exists.
        None where ``key`` is no relationship of ``table``."""
        join = self.ctx.joins.get((table.type_name, key))
        if join is None:
            return None
        if not isinstance(value, dict):
            raise GraphQLLoweringRefused(f"the condition on the relationship {key}")
        junction = self.junction(table, key, join)
        if junction is not None:
            pattern, cond = self.junction_match(table, var, key, junction, value)
        else:
            target, tnm, child, pattern = self.hop(table, var, key, join, rel_var=None)
            cond = self.where(target, tnm, child, value)
        body = CypherAST(
            pipeline=[MatchStep([MatchClause(pattern, None)], _where(cond))],
            return_clause=None,
            order_by=[],
            skip=None,
            limit=None,
        )
        return SubqueryExpr("EXISTS", body)

    # -- a level --

    def arguments(self, sel: FieldNode, allowed: frozenset[str] = _ARGUMENTS) -> dict:
        args = {a.name.value: a.value for a in sel.arguments or ()}
        unknown = sorted(set(args) - allowed)
        if unknown:
            raise GraphQLLoweringRefused(f"the argument {unknown[0]} of {sel.name.value}")
        return {k: self.value(v) for k, v in args.items()}

    def order(self, column: Callable[[str], CypherExpr], order_by: Any) -> list[OrderItem]:
        items = order_by if isinstance(order_by, list) else [order_by]
        out: list[OrderItem] = []
        for item in items:
            for key, direction in item.items():
                if isinstance(direction, dict):
                    raise GraphQLLoweringRefused(f"an order by the relationship {key}")
                out.append(OrderItem(column(key), _direction(direction)))
        return out

    def node_order(self, table: Any, nm: Any, var: str, order_by: Any) -> list[OrderItem]:
        return self.order(lambda key: Property(Variable(var), self.prop(table, nm, key)), order_by)

    def level(
        self, table: Any, nm: Any, var: str, sel: FieldNode, steps: list[MatchStep | WithClause]
    ) -> list[_Entry]:
        """Each field of ``sel``'s selection, read off the node ``var``, as the projection
        projects it. A to-one field's OPTIONAL MATCH is added to ``steps``, the statement of the
        level it is in."""
        out: list[_Entry] = [_Entry(var, Variable(var))] if self.projection == NODES else []
        assert sel.selection_set is not None
        for f in sel.selection_set.selections:
            if not isinstance(f, FieldNode):
                raise GraphQLLoweringRefused("a fragment")
            key = f.alias.value if f.alias else f.name.value
            name = f.name.value
            if name.startswith("_"):
                raise GraphQLLoweringRefused(f"the field {name}")
            join = self.ctx.joins.get((table.type_name, name))
            if join is None:
                if f.arguments:
                    raise GraphQLLoweringRefused(f"arguments on the column {name}")
                prop = self.prop(table, nm, name)
                if self.projection == GRAPHQL:
                    out.append(_Entry(key, Property(Variable(var), prop)))
                elif self.projection == FIELDS:
                    out.append(_Entry(prop, Property(Variable(var), prop), prop=prop))
                continue
            junction = self.junction(table, name, join)
            if junction is not None:
                out.append(self.junction_field(table, nm, var, f, junction))
            elif join.cardinality == "one-to-many":
                out.append(self.to_many(table, var, f, join))
            else:
                out.append(self.to_one(table, var, f, join, steps))
        return out

    def to_one(
        self, table: Any, var: str, f: FieldNode, join: Any, steps: list[MatchStep | WithClause]
    ) -> _Entry:
        """At most one related row: matched beside the level's own, a null map when none."""
        if f.arguments:
            raise GraphQLLoweringRefused(f"arguments on the to-one relationship {f.name.value}")
        rel_var = self.rel_var() if self.projection == NODES else None
        target, tnm, child, pattern = self.hop(table, var, f.name.value, join, rel_var)
        steps.append(MatchStep([MatchClause(pattern, None, optional=True)], None))
        sub = self.level(target, tnm, child, f, steps)
        if self.projection == GRAPHQL:
            key = Property(Variable(child), tnm.property_of(tnm.id_column))
            fields = MapLiteral([(e.key, e.expression()) for e in sub])
            return _Entry(
                f.alias.value if f.alias else f.name.value,
                Case(None, [(IsNull(key, negated=False), Literal(None, "null"))], fields),
            )
        if self.projection == NODES:
            assert rel_var is not None
            sub = [_Entry(rel_var, Variable(rel_var)), *sub]
        return _Entry(f.name.value, None, var=child, level=sub)

    def to_many(self, table: Any, var: str, f: FieldNode, join: Any) -> _Entry:
        """The related rows, each a row of the projection, under that level's own arguments."""
        rel_var = self.rel_var() if self.projection == NODES else None
        target, tnm, child, pattern = self.hop(table, var, f.name.value, join, rel_var)
        args = self.arguments(f)
        body_steps: list[MatchStep | WithClause] = [
            MatchStep(
                [MatchClause(pattern, None)],
                _where(self.where(target, tnm, child, args["where"]) if "where" in args else None),
            )
        ]
        sub = self.level(target, tnm, child, f, body_steps)
        if self.projection == NODES:
            assert rel_var is not None
            sub = [_Entry(rel_var, Variable(rel_var)), *sub]
        order = self.node_order(target, tnm, child, args["order_by"]) if "order_by" in args else []
        return self.collected(f, self.row(sub, child), body_steps, order, args)

    def collected(
        self,
        f: FieldNode,
        row: CypherExpr,
        body_steps: list[MatchStep | WithClause],
        order: list[OrderItem],
        args: dict,
    ) -> _Entry:
        body = CypherAST(
            pipeline=body_steps,
            return_clause=ReturnClause([ReturnItem(row, alias="row")]),
            order_by=order,
            skip=args.get("offset"),
            limit=args.get("limit"),
        )
        key = f.alias.value if f.alias and self.projection == GRAPHQL else f.name.value
        return _Entry(key, SubqueryExpr("COLLECT", body))

    def row(self, entries: list[_Entry], var: str) -> CypherExpr:
        """A related row as the projection collects it."""
        if self.projection == GRAPHQL:
            return MapLiteral([(e.key, e.expression()) for e in entries])
        if self.projection == NODES:
            return ListLiteral(_flatten(entries))
        return _map_projection(entries, var)

    def hop(
        self, table: Any, var: str, name: str, join: Any, rel_var: str | None
    ) -> tuple[Any, Any, str, PathPattern]:
        """The relationship ``name`` as one hop from ``var``: (its target, the target's node,
        the target's variable, the pattern)."""
        if (
            join.via is not None
            or join.source_expr
            or join.target_expr
            or join.source_constant is not None
        ):
            raise GraphQLLoweringRefused(f"the relationship {name} (not a column join)")
        if join.default_limit is not None:
            raise GraphQLLoweringRefused(f"the relationship {name} (a default limit)")
        rels = [
            r
            for r in self.lm.relationships.values()
            if r.field_name == name and r.source_label == table.type_name
        ]
        if len(rels) != 1:
            raise GraphQLLoweringRefused(f"the relationship {name} (no one edge type)")
        target = join.target
        tnm = self.node_of(target)
        child = self.var()
        pattern = PathPattern(
            [NodePattern(var, [], {}), NodePattern(child, [self.lm.display_label(tnm)], {})],
            [RelPattern(rel_var, [rels[0].rel_type], None, None, "right")],
        )
        return target, tnm, child, pattern

    # -- a junction: its rows are relationships --

    def junction(self, table: Any, name: str, join: Any) -> _Junction | None:
        """The relationship ``name`` of ``table`` as the junction its target is, whose rows are
        the relationships between ``table``'s node and the far one; None where its target is a
        node (or no junction of the graph)."""
        jt = join.target
        if jt.type_name in self.lm.nodes:
            return None
        every = [*self.lm.relationships.values()]
        every += [r for rs in self.lm.aliases.values() for r in rs if r not in every]
        through = [r for r in every if r.via is not None and r.via.type_name == jt.type_name]
        if not through:
            return None
        via = through[0].via
        if via.source_columns == (join.target_column,):
            near_is_source = True
        elif via.target_columns == (join.target_column,):
            near_is_source = False
        else:
            raise GraphQLLoweringRefused(f"the relationship {name} (a junction of several keys)")
        rels = [
            r
            for r in through
            if (r.source_label if near_is_source else r.target_label) == table.type_name
            and (r.join_source_column if near_is_source else r.join_target_column)
            == join.source_column
        ]
        fars = {r.target_label if near_is_source else r.source_label for r in rels}
        if len(fars) != 1:
            raise GraphQLLoweringRefused(f"the relationship {name} (no one far node)")
        far_label = fars.pop()
        far = next((t for t in self.ctx.tables.values() if t.type_name == far_label), None)
        if far is None:
            raise GraphQLLoweringRefused(f"the relationship {name} (its far node is not read)")
        first = rels[0]
        return _Junction(
            table=jt,
            rels=rels,
            near_is_source=near_is_source,
            far=far,
            fnm=self.node_of(far),
            near_column=join.target_column,
            far_column=(via.target_columns if near_is_source else via.source_columns)[0],
            near_key=first.join_source_column if near_is_source else first.join_target_column,
            far_key=first.join_target_column if near_is_source else first.join_source_column,
            rel_var=self.rel_var(),
            far_var=self.var(),
        )

    def junction_match(
        self, table: Any, var: str, name: str, j: _Junction, where: dict
    ) -> tuple[PathPattern, CypherExpr | None]:
        """The junction's relationships under ``where``, as a pattern and its condition: a type
        it keeps by its type column alone ([r:A|B]); no type, every row (an untyped relationship),
        where nothing else joins its two nodes."""
        nm = self.node_of(table)
        via = j.rels[0].via
        declared = {r.via.type_value: r.rel_type for r in j.rels}
        types: list[str] = []
        rest: list[dict] = []
        for conjunct in _conjuncts(where):
            ((key, cond),) = conjunct.items()
            column = self.ctx.exposed_to_physical.get((j.table.table_id, key), key)
            values = cond.get("in") if isinstance(cond, dict) and set(cond) == {"in"} else None
            if (
                not types
                and column == via.type_column
                and values
                and all(v in declared for v in values)
            ):
                types = [declared[v] for v in values]
                continue
            rest.append(conjunct)
        arrow = "right" if j.near_is_source else "left"
        if not types:
            src, tgt = (table.type_name, j.far.type_name)
            if not j.near_is_source:
                src, tgt = tgt, src
            others = [
                r
                for r in self.lm.relationships_for(src, tgt)
                if r.via is None or r.via.table_name != via.table_name
            ]
            if others:
                raise GraphQLLoweringRefused(
                    f"the relationship {name}: other relationships join these labels"
                )
        pattern = PathPattern(
            [
                NodePattern(var, [], {}),
                NodePattern(j.far_var, [self.lm.display_label(j.fnm)], {}),
            ],
            [RelPattern(j.rel_var, types, None, None, arrow)],
        )
        cond = self.conditions(
            {"_and": rest} if rest else {},
            lambda key: self.edge_column(j, var, nm, key),
            lambda key, value: self.edge_relation(table, nm, var, j, key, value),
        )
        return pattern, cond

    def edge_column(self, j: _Junction, var: str, nm: Any, key: str) -> CypherExpr:
        """The junction column ``key`` names, read off the relationship and its two nodes."""
        via = j.rels[0].via
        column = self.ctx.exposed_to_physical.get((j.table.table_id, key), key)
        if column == via.type_column:
            if via.type_attribute is None:
                raise GraphQLLoweringRefused(f"{key} of {j.table.field_name} is no property")
            return Property(Variable(j.rel_var), via.type_attribute)
        for prop, col in via.attributes.items():
            if col == column:
                return Property(Variable(j.rel_var), prop)
        if column == j.near_column:
            return Property(Variable(var), nm.property_of(j.near_key))
        if column == j.far_column:
            return Property(Variable(j.far_var), j.fnm.property_of(j.far_key))
        raise GraphQLLoweringRefused(f"{key} of {j.table.field_name} is no property")

    def edge_end(self, j: _Junction, key: str) -> str | None:
        """Which end of the relationship the junction's relationship ``key`` reaches: "far",
        "near", or None where ``key`` is none of the junction's."""
        join = self.ctx.joins.get((j.table.type_name, key))
        if join is None:
            return None
        if join.source_column == j.far_column:
            return "far"
        if join.source_column == j.near_column:
            return "near"
        raise GraphQLLoweringRefused(f"the relationship {key} of {j.table.field_name}")

    def edge_relation(
        self, table: Any, nm: Any, var: str, j: _Junction, key: str, value: Any
    ) -> CypherExpr | None:
        end = self.edge_end(j, key)
        if end is None:
            return None
        if end == "far":
            return self.where(j.far, j.fnm, j.far_var, value) or Literal(True, "boolean")
        return self.where(table, nm, var, value) or Literal(True, "boolean")

    def junction_field(self, table: Any, nm: Any, var: str, f: FieldNode, j: _Junction) -> _Entry:
        """The junction's rows beside ``var``'s: each relationship, its row's columns its
        properties, and the far node."""
        name = f.name.value
        args = self.arguments(f)
        pattern, cond = self.junction_match(table, var, name, j, args.get("where") or {})
        body_steps: list[MatchStep | WithClause] = [
            MatchStep([MatchClause(pattern, None)], _where(cond))
        ]
        entries: list[_Entry] = (
            [_Entry(j.rel_var, Variable(j.rel_var))] if self.projection == NODES else []
        )
        assert f.selection_set is not None
        for g in f.selection_set.selections:
            if not isinstance(g, FieldNode):
                raise GraphQLLoweringRefused("a fragment")
            key = g.alias.value if g.alias and self.projection == GRAPHQL else g.name.value
            end = self.edge_end(j, g.name.value)
            if end is None:
                if g.arguments:
                    raise GraphQLLoweringRefused(f"arguments on the column {g.name.value}")
                if self.projection != NODES:
                    entries.append(_Entry(key, self.edge_column(j, var, nm, g.name.value)))
                continue
            if g.arguments:
                raise GraphQLLoweringRefused(f"arguments on the to-one relationship {g.name.value}")
            ends = (j.far, j.fnm, j.far_var) if end == "far" else (table, nm, var)
            sub = self.level(*ends, g, body_steps)
            if self.projection == GRAPHQL:
                entries.append(_Entry(key, MapLiteral([(e.key, e.expression()) for e in sub])))
            else:
                entries.append(_Entry(key, None, var=ends[2], level=sub))
        if self.projection == GRAPHQL:
            row: CypherExpr = MapLiteral([(e.key, e.expression()) for e in entries])
        elif self.projection == NODES:
            row = ListLiteral(_flatten(entries))
        else:
            row = MapLiteral([(e.key, _entry_value(e)) for e in entries])
        order = (
            self.order(lambda key: self.edge_column(j, var, nm, key), args["order_by"])
            if "order_by" in args
            else []
        )
        return self.collected(f, row, body_steps, order, args)

    # -- a group-by root --

    def aggregate(self, table: Any, nm: Any, var: str, func: str, column: str) -> CypherExpr:
        if func == "count":
            return FunctionCall("count", [], star=True)
        if func not in _AGGREGATES:
            raise GraphQLLoweringRefused(f"the aggregate {func}")
        return FunctionCall(
            _AGGREGATES[func], [Property(Variable(var), self.prop(table, nm, column))]
        )

    def aggregates(
        self, table: Any, nm: Any, var: str, f: FieldNode, *, function_aliases: bool
    ) -> list[tuple[str, CypherExpr]]:
        """The entries of an ``aggregate`` field's map: ``count`` and each function's map of the
        columns it aggregates, keyed as the GraphQL compiler keys them (a column's alias; a
        function's alias where the compiler honours one)."""
        assert f.selection_set is not None
        entries: list[tuple[str, CypherExpr]] = []
        for a in f.selection_set.selections:
            if not isinstance(a, FieldNode) or a.arguments:
                raise GraphQLLoweringRefused("a fragment or arguments in aggregate")
            func = a.name.value
            if a.alias and (func == "count" or not function_aliases):
                raise GraphQLLoweringRefused(f"an alias on the aggregate {func}")
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
            entries.append((a.alias.value if a.alias else func, MapLiteral(columns)))
        return entries

    def nodes(self, table: Any, nm: Any, var: str, f: FieldNode) -> ReturnItem:
        """An aggregate root's ``nodes``: ``collect({…})`` of the rows it aggregates, a map of
        each row's columns (in no order, as the GraphQL compiler's nodes are in none)."""
        if f.alias:
            raise GraphQLLoweringRefused("an alias on nodes")
        assert f.selection_set is not None
        entries: list[tuple[str, CypherExpr]] = []
        for c in f.selection_set.selections:
            if not isinstance(c, FieldNode) or c.arguments or c.name.value.startswith("_"):
                raise GraphQLLoweringRefused("a fragment, arguments or a meta field in nodes")
            if (table.type_name, c.name.value) in self.ctx.joins:
                raise GraphQLLoweringRefused(f"the relationship {c.name.value} in nodes")
            key = c.alias.value if c.alias else c.name.value
            entries.append((key, Property(Variable(var), self.prop(table, nm, c.name.value))))
        return ReturnItem(FunctionCall("collect", [MapLiteral(entries)]), alias="nodes")

    def aggregate_root(self, table: Any, nm: Any, var: str, sel: FieldNode) -> list[ReturnItem]:
        """An aggregate root's RETURN items: its ``aggregate`` map and its ``nodes``, one row
        over every row."""
        assert sel.selection_set is not None
        items: list[ReturnItem] = []
        for f in sel.selection_set.selections:
            if not isinstance(f, FieldNode) or f.arguments:
                raise GraphQLLoweringRefused("a fragment or arguments on an aggregate root's field")
            if f.name.value == "nodes":
                items.append(self.nodes(table, nm, var, f))
                continue
            if f.name.value != "aggregate":
                raise GraphQLLoweringRefused(f"the aggregate field {f.name.value}")
            entries = self.aggregates(table, nm, var, f, function_aliases=True)
            items.append(
                ReturnItem(MapLiteral(entries), alias=f.alias.value if f.alias else "aggregate")
            )
        return items

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
            entries = self.aggregates(table, nm, var, f, function_aliases=False)
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


def _returned(projection: str, entries: list[_Entry]) -> list[ReturnItem]:
    """A root's RETURN items: each field under its response key (GRAPHQL); its own and its
    to-one fields' properties flat, unaliased (FIELDS); its nodes and relationships (NODES). A
    to-many field is its COLLECT under its name in each."""
    out: list[ReturnItem] = []
    for e in entries:
        if e.level is not None:
            out += _returned(projection, e.level)
        elif projection == GRAPHQL or not (e.prop is not None or isinstance(e.expr, Variable)):
            out.append(ReturnItem(e.expression(), alias=e.key))
        else:
            out.append(ReturnItem(e.expression()))
    return out


def _root_fields(document: Any) -> list[FieldNode]:
    operations = [d for d in document.definitions if isinstance(d, OperationDefinitionNode)]
    if len(operations) != 1 or operations[0].operation.value != "query":
        raise GraphQLLoweringRefused("a document that is not one query")
    fields: list[FieldNode] = []
    for sel in operations[0].selection_set.selections:
        if not isinstance(sel, FieldNode):
            raise GraphQLLoweringRefused("a fragment")
        fields.append(sel)
    return fields


def lower_read(
    document: Any,
    ctx: Any,
    label_map: Any,
    variables: dict | None,
    projection: str = GRAPHQL,
) -> list[LoweredRoot]:
    """Each root field of ``document`` (validated against the role's schema) as its statement, in
    the query's order, or :class:`GraphQLLoweringRefused` naming what it does not lower."""
    return [
        _lower_root(sel, ctx, label_map, variables, projection) for sel in _root_fields(document)
    ]


def lower_root(
    document: Any,
    index: int,
    ctx: Any,
    label_map: Any,
    variables: dict | None,
    projection: str = GRAPHQL,
) -> LoweredRoot:
    """The root field at ``index`` of ``document`` as its statement: a root the front end does
    not lower refuses itself alone, not the others."""
    return _lower_root(_root_fields(document)[index], ctx, label_map, variables, projection)


def _lower_root(
    sel: FieldNode, ctx: Any, label_map: Any, variables: dict | None, projection: str
) -> LoweredRoot:
    lowering = _Lowering(ctx, label_map, variables, projection)
    table = ctx.tables.get(sel.name.value)
    name = sel.name.value
    base = table.field_name if table is not None else None
    grouped = name in (f"{base}GroupBy", f"{base}_group_by")
    aggregated = name in (f"{base}Aggregate", f"{base}_aggregate")
    if table is None or (base != name and not grouped and not aggregated):
        raise GraphQLLoweringRefused(f"the root field {name}")
    nm = lowering.node_of(table)
    var = lowering.var()
    args = lowering.arguments(
        sel,
        _GROUP_BY_ARGUMENTS if grouped else _AGGREGATE_ARGUMENTS if aggregated else _ARGUMENTS,
    )
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
    elif aggregated:
        returned, order = lowering.aggregate_root(table, nm, var, sel), []
    else:
        returned = _returned(lowering.projection, lowering.level(table, nm, var, sel, steps))
        order = lowering.node_order(table, nm, var, args["order_by"]) if "order_by" in args else []
    statement = CypherAST(
        pipeline=steps,
        return_clause=ReturnClause(returned),
        order_by=order,
        skip=args.get("offset"),
        limit=args.get("limit"),
    )
    return LoweredRoot(sel.alias.value if sel.alias else sel.name.value, statement)
