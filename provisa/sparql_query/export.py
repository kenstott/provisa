# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph read written as SPARQL (REQ-2136, REQ-2135): the graph IR of a statement printed as a
SPARQL SELECT over the role's vocabulary, exact or refused by name. Open as query uses it for a
GraphQL read, by way of the read's Cypher export.

The SELECT answers what the statement does when the SPARQL front end reads it back:

- A node is its variable, typed by its label's table class. A relationship is its edge type's
  predicate, written from its source to its target.
- A property a node holds is bound in an OPTIONAL of its own, inside the group that matches the
  node: a property with no value leaves its variable unbound, as a column holds null.
- An OPTIONAL MATCH is an OPTIONAL group, its WHERE that group's FILTER. A property its WHERE
  compares is bound in the group itself: a comparison holds of no null, so the group holds of the
  same rows either way.
- The statement's WHERE is the query's FILTER; DISTINCT, an aggregate (COUNT, MIN, MAX) and its
  grouping, ORDER BY, SKIP and LIMIT are the query's own.
- ORDER BY is the statement's own: the front end orders by the same keys, so nulls fall where
  the statement's own engine puts them.

Anything else -- a nested list, a map, CASE, a function, arithmetic, a path, an undirected or
variable-length relationship, WITH, UNWIND, UNION, a parameter -- is refused, naming it.
"""

# Requirements: REQ-2136, REQ-2135

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field

from provisa.gql.expr_ast import (
    Binary,
    CypherExpr,
    FunctionCall,
    IsNull,
    ListLiteral,
    Literal,
    Paren,
    Property,
    Unary,
    Variable,
)
from provisa.gql.ir import CypherAST, MatchStep, NodePattern, PathPattern, RelPattern
from provisa.sparql_query.vocabulary import Vocabulary, column_iri, edge_iri, table_class_iri


class SparqlExportRefused(ValueError):
    """The statement has no exact SPARQL form; the message names what has none."""


_COMPARISONS = {"=": "=", "<>": "!=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
_STRING_TESTS = {"STARTS WITH": "STRSTARTS", "ENDS WITH": "STRENDS", "CONTAINS": "CONTAINS"}
_AGGREGATES = {"count": "COUNT", "min": "MIN", "max": "MAX"}
_NAME = re.compile(r"[^A-Za-z0-9_]")


def _var_name(text: str) -> str:
    name = _NAME.sub("_", text)
    return name if name and not name[0].isdigit() else f"v_{name}"


def _string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    return f'"{escaped.replace(chr(13), "\\r")}"'


@dataclass
class _Group:
    """A group of the query: its triples, the OPTIONALs binding its nodes' properties, its
    FILTER and the OPTIONAL groups in it."""

    triples: list[str] = field(default_factory=list)
    properties: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    optionals: list["_Group"] = field(default_factory=list)

    def lines(self, indent: str) -> list[str]:
        out = [f"{indent}{t}" for t in self.triples]
        out += [f"{indent}OPTIONAL {{ {p} }}" for p in self.properties]
        for g in self.optionals:
            out.append(f"{indent}OPTIONAL {{")
            out += g.lines(indent + "  ")
            out.append(f"{indent}}}")
        out += [f"{indent}FILTER({f})" for f in self.filters]
        return out


class _Printer:
    def __init__(self, vocab: Vocabulary, lm) -> None:
        self.vocab = vocab
        self.lm = lm
        self.main = _Group()
        self.label: dict[str, str] = {}
        self.group: dict[str, _Group] = {}
        self.bound: dict[tuple[str, str], str] = {}
        self.names: set[str] = set()
        self.prefixes: dict[str, str] = {}
        #: The properties the statement returns: each is bound under the name it returns.
        self.wanted: set[tuple[str, str]] = set()

    def prefix(self, wanted: str, ns: str) -> None:
        """A PREFIX for ``ns``, named ``wanted`` unless that name is another's."""
        if ns in self.prefixes.values():
            return
        name, n = _var_name(wanted), 1
        while name in self.prefixes:
            n += 1
            name = f"{_var_name(wanted)}{n}"
        self.prefixes[name] = ns

    # -- names --

    def _fresh(self, wanted: str) -> str:
        name, n = _var_name(wanted), 1
        while name in self.names:
            n += 1
            name = f"{_var_name(wanted)}_{n}"
        self.names.add(name)
        return name

    def _iri(self, iri: str) -> str:
        for prefix, ns in self.prefixes.items():
            local = iri[len(ns) :]
            if iri.startswith(ns) and local and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", local):
                return f"{prefix}:{local}"
        return f"<{iri}>"

    def _node_of(self, label: str):
        nm = self.lm.nodes.get(label)
        classes = self.vocab.classes_of.get(label)
        if nm is None or not classes:
            raise SparqlExportRefused(f"The label {label} has no class in the SPARQL vocabulary")
        return nm, classes[0]

    # -- patterns --

    def node(self, n: NodePattern, group: _Group) -> str:
        if n.properties or n.property_text:
            raise SparqlExportRefused("An inline property map on a node has no SPARQL form here")
        if n.variable is None:
            raise SparqlExportRefused("A node with no variable has no SPARQL form here")
        var = n.variable
        if var in self.label:
            if n.labels and n.labels != [self.label[var]]:
                raise SparqlExportRefused(f"The node {var} is matched with another label")
            return var
        if len(n.labels) != 1:
            raise SparqlExportRefused(f"The node {var} needs exactly one label")
        nm, cls = self._node_of(n.labels[0])
        self.prefix(nm.domain_id, self.vocab.prefixes[nm.domain_id])
        self.label[var] = n.labels[0]
        self.group[var] = group
        self.names.add(_var_name(var))
        group.triples.append(f"?{_var_name(var)} a {self._iri(cls)} .")
        return var

    def edge(self, left: str, rel: RelPattern, right: str, group: _Group) -> None:
        if rel.variable is not None:
            raise SparqlExportRefused("A relationship variable has no SPARQL form here")
        if rel.path is not None or rel.variable_length or rel.min_hops or rel.max_hops:
            raise SparqlExportRefused("A path or a variable-length relationship")
        if len(rel.types) != 1:
            raise SparqlExportRefused("A relationship of no type or of several types")
        if rel.properties or rel.property_text:
            raise SparqlExportRefused("An inline property map on a relationship")
        if rel.direction == "none":
            raise SparqlExportRefused("An undirected relationship")
        src, dst = (left, right) if rel.direction == "right" else (right, left)
        iri = edge_iri(self.vocab.base, rel.types[0])
        ends = (self.label[src], self.label[dst])
        if not any((e.source, e.target) == ends for e in self.vocab.edges.get(iri, [])):
            raise SparqlExportRefused(
                f"The relationship {rel.types[0]} from {ends[0]} to {ends[1]} has no SPARQL "
                "predicate"
            )
        group.triples.append(f"?{_var_name(src)} {self._iri(iri)} ?{_var_name(dst)} .")

    def pattern(self, p, group: _Group) -> None:
        if not isinstance(p, PathPattern):
            raise SparqlExportRefused("A path function or a triple pattern")
        vars_ = [self.node(n, group) for n in p.nodes]
        for i, rel in enumerate(p.rels):
            self.edge(vars_[i], rel, vars_[i + 1], group)

    # -- values --

    def prop(self, var: str, name: str, required_in: _Group | None = None) -> str:
        """The variable bound to ``var``'s property ``name``. ``required_in``: bound in that
        group itself (a property its OPTIONAL's WHERE compares), not in an OPTIONAL of its own."""
        if var not in self.label:
            raise SparqlExportRefused(f"The variable {var} is no node of the pattern")
        key = (var, name)
        if key in self.bound:
            return self.bound[key]
        nm = self.lm.nodes[self.label[var]]
        if name not in nm.properties:
            raise SparqlExportRefused(f"{self.label[var]} has no property {name}")
        iri = column_iri(self.vocab.base, nm.domain_id, nm.table_name, name)
        self.prefix(
            nm.table_name, f"{table_class_iri(self.vocab.base, nm.domain_id, nm.table_name)}#"
        )
        v = self._fresh(name if (var, name) in self.wanted else f"{var}_{name}")
        triple = f"?{_var_name(var)} {self._iri(iri)} ?{v}"
        if required_in is not None:
            required_in.triples.append(f"{triple} .")
        else:
            self.group[var].properties.append(triple)
        self.bound[key] = v
        return v

    def expr(self, e: CypherExpr, required_in: _Group | None = None) -> str:
        if isinstance(e, Paren):
            return f"({self.expr(e.inner, required_in)})"
        if isinstance(e, Property) and isinstance(e.obj, Variable):
            return f"?{self.prop(e.obj.name, e.name, required_in)}"
        if isinstance(e, Literal):
            return self.literal(e)
        if isinstance(e, Unary) and e.op.upper() == "NOT":
            return f"!({self.expr(e.operand, required_in)})"
        if isinstance(e, Unary) and e.op == "-" and isinstance(e.operand, Literal):
            return f"-{self.literal(e.operand)}"
        if isinstance(e, IsNull) and isinstance(e.operand, Property):
            if required_in is not None:
                raise SparqlExportRefused("IS NULL in an OPTIONAL MATCH's WHERE")
            bound = f"BOUND({self.expr(e.operand)})"
            return bound if e.negated else f"!{bound}"
        if isinstance(e, Binary):
            op = e.op.upper()
            if op in ("AND", "OR"):
                if op == "OR" and required_in is not None:
                    raise SparqlExportRefused("OR in an OPTIONAL MATCH's WHERE")
                joint = " && " if op == "AND" else " || "
                return f"({self.expr(e.left, required_in)}{joint}{self.expr(e.right, required_in)})"
            if op in _COMPARISONS:
                left, right = self.expr(e.left, required_in), self.expr(e.right, required_in)
                return f"{left} {_COMPARISONS[op]} {right}"
            if op == "IN" and isinstance(e.right, ListLiteral):
                items = ", ".join(self.literal(i) for i in e.right.items)
                return f"{self.expr(e.left, required_in)} IN ({items})"
            if op in _STRING_TESTS and isinstance(e.right, Literal) and e.right.kind == "string":
                return (
                    f"{_STRING_TESTS[op]}({self.expr(e.left, required_in)}, "
                    f"{self.literal(e.right)})"
                )
            raise SparqlExportRefused(f"The operator {e.op} has no exact SPARQL form")
        if isinstance(e, FunctionCall):
            raise SparqlExportRefused(f"The function {e.name}() has no exact SPARQL form")
        raise SparqlExportRefused(f"{type(e).__name__} has no SPARQL form here")

    def literal(self, e: CypherExpr) -> str:
        if not isinstance(e, Literal):
            raise SparqlExportRefused(f"{type(e).__name__} where a literal is written")
        if e.kind == "string":
            return _string(str(e.value))
        if e.kind == "boolean" or isinstance(e.value, bool):
            return "true" if e.value else "false"
        if e.kind == "number" and isinstance(e.value, (int, float)):
            return repr(e.value)
        raise SparqlExportRefused(f"The literal {e.value!r} has no SPARQL form here")


def _holds_map(e: object) -> bool:
    """Whether ``e`` builds a map anywhere in it (a nested GraphQL object)."""
    if type(e).__name__ in ("MapLiteral", "MapProjection"):
        return True
    if isinstance(e, (list, tuple)):
        return any(_holds_map(x) for x in e)
    if dataclasses.is_dataclass(e) and not isinstance(e, type):
        return any(_holds_map(getattr(e, f.name)) for f in dataclasses.fields(e))
    return False


def ir_to_sparql(ast: CypherAST, vocab: Vocabulary, lm) -> str:
    """``ast`` as a SPARQL SELECT over ``vocab``, or :class:`SparqlExportRefused`."""
    if ast.union_parts:
        raise SparqlExportRefused("UNION")
    if ast.call_subqueries:
        raise SparqlExportRefused("A CALL subquery (a nested list)")
    if ast.construct is not None or ast.return_clause is None:
        raise SparqlExportRefused("A statement with no RETURN")
    p = _Printer(vocab, lm)
    p.prefix("rel", vocab.prefixes["rel"])
    ret = ast.return_clause
    if ret.group_by is not None or ret.having is not None:
        raise SparqlExportRefused("GROUP BY or HAVING")
    # A returned property is bound under the name the statement returns it by.
    p.wanted = {
        (i.expression.obj.name, i.expression.name)
        for i in ret.items
        if isinstance(i.expression, Property) and isinstance(i.expression.obj, Variable)
    }

    seen_optional = False
    required_where: list[CypherExpr] = []
    for step in ast.pipeline:
        if not isinstance(step, MatchStep):
            raise SparqlExportRefused(f"{type(step).__name__.removesuffix('Clause').upper()}")
        optional = bool(step.matches) and all(m.optional for m in step.matches)
        if not optional and any(m.optional for m in step.matches):
            raise SparqlExportRefused("A MATCH and an OPTIONAL MATCH in one step")
        for m in step.matches:
            if m.variable is not None:
                raise SparqlExportRefused("A path variable")
        if optional:
            seen_optional = True
            group = _Group()
            for m in step.matches:
                p.pattern(m.pattern, group)
            if step.where is not None:
                group.filters.append(p.expr(step.where.expression, required_in=group))
            p.main.optionals.append(group)
        else:
            if seen_optional and step.matches:
                raise SparqlExportRefused("A MATCH after an OPTIONAL MATCH")
            for m in step.matches:
                p.pattern(m.pattern, p.main)
            if step.where is not None:
                required_where.append(step.where.expression)
    p.main.filters += [p.expr(w) for w in required_where]

    select: list[str] = []
    keys: list[str] = []
    aggregated = False
    for item in ret.items:
        e = item.expression
        if isinstance(e, FunctionCall) and e.name.lower() in _AGGREGATES:
            aggregated = True
            fn = _AGGREGATES[e.name.lower()]
            if e.star:
                arg = "*"
            elif len(e.args) == 1 and isinstance(e.args[0], Property):
                arg = p.expr(e.args[0])
            elif len(e.args) == 1 and isinstance(e.args[0], Variable) and fn == "COUNT":
                arg = f"?{_var_name(p.node(NodePattern(e.args[0].name, [], {}), p.main))}"
            else:
                raise SparqlExportRefused(f"{e.name}() of anything but a property or a node")
            distinct = "DISTINCT " if e.distinct else ""
            name = p._fresh(item.alias or e.name.lower())
            select.append(f"({fn}({distinct}{arg}) AS ?{name})")
            p.bound[("", item.alias or "")] = name
        elif isinstance(e, Property) and isinstance(e.obj, Variable):
            v = p.prop(e.obj.name, e.name)
            select.append(f"?{v}")
            keys.append(f"?{v}")
        elif isinstance(e, FunctionCall):
            raise SparqlExportRefused(f"The function {e.name}() has no exact SPARQL form")
        elif _holds_map(e):
            raise SparqlExportRefused("A nested object (a map) has no SPARQL form here")
        else:
            what = {
                "MapLiteral": "A nested object (a map)",
                "MapProjection": "A nested object (a map)",
                "Case": "CASE",
                "Variable": "A whole node",
            }.get(type(e).__name__, f"{type(e).__name__}")
            raise SparqlExportRefused(f"{what} has no SPARQL form here")
    if aggregated and ret.distinct:
        raise SparqlExportRefused("DISTINCT with an aggregate")

    order: list[str] = []
    for o in ast.order_by:
        e = o.expression
        if isinstance(e, Variable) and ("", e.name) in p.bound:
            v = f"?{p.bound[('', e.name)]}"
        elif isinstance(e, Property) and isinstance(e.obj, Variable):
            v = f"?{p.prop(e.obj.name, e.name)}"
        else:
            raise SparqlExportRefused("ORDER BY anything but a property or an aggregate")
        d = "DESC" if o.direction.upper().startswith("DESC") else "ASC"
        order.append(f"{d}({v})")

    lines = [f"PREFIX {k}: <{v}>" for k, v in p.prefixes.items()]
    lines.append(f"SELECT {'DISTINCT ' if ret.distinct else ''}{' '.join(select)}")
    lines.append("WHERE {")
    lines += p.main.lines("  ")
    lines.append("}")
    if aggregated and keys:
        lines.append(f"GROUP BY {' '.join(keys)}")
    if order:
        lines.append(f"ORDER BY {' '.join(order)}")
    if ast.limit is not None:
        lines.append(f"LIMIT {ast.limit}")
    if ast.skip is not None:
        lines.append(f"OFFSET {ast.skip}")
    return "\n".join(lines)
