# Copyright (c) 2026 Kenneth Stott
# Canary: c9a1e7d3-4b28-4f60-8e15-2a7c6f0b9d34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Column-level lineage GRAPH for a single SQL statement (REQ-1160).

Where :func:`provisa.lineage.columns.resolve_column_lineage` collapses each output column to its LEAF
source columns, this builds the full node+edge DAG — intermediate relations (CTEs, subqueries, joins)
preserved — from source inputs to final outputs. Each edge carries the transform that produces its
target, and that transform is NAMED/DEFINED, not left as an opaque SQL string: every operation in the
projection is matched against the union vocabulary of SQL operators/standard functions, engine
built-ins, and registered commands (REQ-1159), so a `lat,lng <- city, state` edge reads as the chain
`concat/|| (SQL op) -> geocode (command)`. An inline command call at a leaf is a first-class node: its
declared input/output contract splices in as taint-closure edges, so the DAG stays continuous across
the opaque RPC boundary. Static — computed from the definition, no execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage


@dataclass(frozen=True)
class TransformOp:  # REQ-1160
    """One named operation in a transform expression."""

    name: str  # e.g. "UPPER", "*", "geocode"
    kind: str  # "sql_function" | "operator" | "command" | "identity" | "constant"


@dataclass
class Node:  # REQ-1160
    """A column node in the lineage graph, keyed by ``relation.column`` (or just column for outputs)."""

    id: str
    column: str
    relation: str | None  # source/intermediate relation name; None for a bare output column
    kind: str  # "source" | "derived" | "command"
    # REQ-1161: True when this node's relation is materialized (an MV / CTAS snapshot). A cycle that
    # crosses a materialized node is a legal time-lagged feedback loop, not a design error.
    materialized: bool = False


@dataclass
class Edge:  # REQ-1160
    """A derivation edge: ``target`` is produced from ``source`` by ``transform``."""

    source: str  # upstream node id
    target: str  # downstream node id
    transform: str  # the raw projection expression (SQL)
    ops: tuple[TransformOp, ...]  # the named operations composing the transform


@dataclass
class LineageGraph:  # REQ-1160
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)  # final output column node ids

    def add_node(self, node: Node) -> None:
        # A node seen as both derived and source stays 'source' only if never produced by an edge;
        # last-writer with a real relation wins over a bare output placeholder.
        existing = self.nodes.get(node.id)
        if existing is None or (existing.relation is None and node.relation is not None):
            self.nodes[node.id] = node

    def add_edge(self, edge: Edge) -> None:
        self.edges.append(edge)

    def to_dict(self) -> dict:
        """Serialize to a render-ready graph JSON (nodes + edges + outputs) — the API/UI payload."""
        return {
            "nodes": [
                {
                    "id": n.id,
                    "column": n.column,
                    "relation": n.relation,
                    "kind": n.kind,
                    "materialized": n.materialized,
                }
                for n in self.nodes.values()
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "transform": e.transform,
                    "ops": [{"name": o.name, "kind": o.kind} for o in e.ops],
                }
                for e in self.edges
            ],
            "outputs": list(self.outputs),
        }


# --------------------------------------------------------------------------- #
# Transform naming — match ops against SQL / built-in / command vocabularies.  #
# --------------------------------------------------------------------------- #

# Binary/arith operator expression type -> its SQL symbol.
_OPERATORS: dict[type, str] = {
    exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/", exp.Mod: "%",
    exp.DPipe: "||", exp.EQ: "=", exp.NEQ: "<>", exp.GT: ">", exp.LT: "<",
    exp.GTE: ">=", exp.LTE: "<=", exp.And: "AND", exp.Or: "OR",
}


def name_transform(expression: exp.Expression | None, command_names: frozenset[str]) -> tuple[str, tuple[TransformOp, ...]]:
    """Return (raw transform SQL, ordered named ops) for a projection expression (REQ-1160).

    A bare column reference is ``identity``; a literal is ``constant``; a function is a ``command``
    when its name is registered, else a ``sql_function``; arithmetic/logical operators are named by
    symbol. Ops are collected in encounter order so an expression reads as an operation chain."""
    if expression is None:
        return "", ()
    # Unwrap an aliasing wrapper (``expr AS name``) to name the producing expression itself.
    inner = expression.this if isinstance(expression, exp.Alias) else expression
    raw = inner.sql()
    ops: list[TransformOp] = []
    if isinstance(inner, exp.Column):
        ops.append(TransformOp(inner.name, "identity"))
        return raw, tuple(ops)
    if isinstance(inner, exp.Literal) or inner.find(exp.Column) is None:
        return raw, (TransformOp(raw, "constant"),)
    for sub in inner.walk():
        node = sub
        if isinstance(node, exp.Anonymous):
            ops.append(TransformOp(node.name, "command" if node.name in command_names else "sql_function"))
        elif isinstance(node, exp.Func):
            ops.append(TransformOp(_func_name(node), "sql_function"))
        elif type(node) in _OPERATORS:
            ops.append(TransformOp(_OPERATORS[type(node)], "operator"))
    return raw, tuple(ops)


def _func_name(node: exp.Func) -> str:
    try:
        names = node.sql_names()
        return names[0].upper() if names else node.key.upper()
    except (AttributeError, IndexError):
        return node.key.upper()


# --------------------------------------------------------------------------- #
# Graph construction.                                                          #
# --------------------------------------------------------------------------- #


def _output_columns(tree: exp.Expression) -> list[str]:
    """Named output columns of the top-level SELECT (skips ``*`` — unresolvable without a catalog)."""
    selects = list(getattr(tree, "selects", []) or [])
    cols: list[str] = []
    for proj in selects:
        name = proj.alias_or_name
        if name and name != "*":
            cols.append(name)
    return cols


def _node_for(sqlglot_node, command_names: frozenset[str]) -> Node:
    """Map a sqlglot lineage node to a graph Node (id ``relation.column`` or bare column)."""
    name = sqlglot_node.name
    source = sqlglot_node.source
    relation, column = (name.rsplit(".", 1)) if "." in name else (None, name)
    if isinstance(source, exp.Table):
        inner = source.this
        if isinstance(inner, exp.Anonymous) and inner.name in command_names:
            kind = "command"
        else:
            kind = "source"
    else:
        kind = "derived"
    return Node(id=name, column=column, relation=relation, kind=kind)


def build_column_graph(
    sql: str, *, dialect: str | None = "postgres", commands: dict[str, dict] | None = None
) -> LineageGraph:
    """Build the full column-level lineage DAG for ``sql`` (REQ-1160).

    One traversal per named output column via sqlglot's lineage, unioned into a single graph so shared
    intermediates (a CTE column feeding several outputs) become one node. When ``commands`` maps a
    command name to its registry dict (contract), every inline command call splices in as a first-class
    node: the taint closure (each declared output column ← ALL declared input columns) connects the
    command's output to the real source columns of its input relation, so the DAG stays continuous
    across the opaque boundary. Raises SqlglotError only if the SQL cannot be parsed; an individual
    unresolvable column is recorded as an output node with no upstream, never dropped silently."""
    commands = commands or {}
    command_names = frozenset(commands)
    tree = cast(exp.Expression, sqlglot.parse_one(sql, dialect=dialect))
    graph = LineageGraph()
    for out in _output_columns(tree):
        try:
            root = lineage(out, sql, dialect=dialect)
        except (SqlglotError, KeyError, ValueError):
            graph.add_node(Node(id=out, column=out, relation=None, kind="derived"))
            graph.outputs.append(out)
            continue
        graph.outputs.append(root.name)
        _walk(root, graph, command_names)
    _splice_commands(tree, graph, commands)
    return graph


def _splice_commands(tree: exp.Expression, graph: LineageGraph, commands: dict[str, dict]) -> None:
    """Connect each command-output node to its input relation's source columns (taint closure).

    For a call ``fn(<rel>) AS e``, ``e.<out>`` (already a ``command`` node) gains an upstream edge from
    every ``<rel>.<in>`` declared on the command's relation argument — the conservative, sound closure:
    an output derives from ALL declared inputs (REQ-1159). ``<rel>.<in>`` nodes are the true source
    leaves. A tight input contract keeps the closure tight; a wide one fans in visibly."""
    calls = _command_calls(tree, commands)
    for node in list(graph.nodes.values()):
        if node.kind != "command" or node.relation is None:
            continue
        call = calls.get(node.relation)
        if call is None:
            continue
        name, rel_ref, in_cols = call
        op = (TransformOp(name, "command"),)
        for in_col in in_cols:
            src_id = f"{rel_ref}.{in_col}" if rel_ref else in_col
            graph.add_node(Node(id=src_id, column=in_col, relation=rel_ref, kind="source"))
            graph.add_edge(Edge(source=src_id, target=node.id, transform=f"{name}(...)", ops=op))


def _command_calls(tree: exp.Expression, commands: dict[str, dict]) -> dict[str, tuple[str, str | None, list[str]]]:
    """Map each command call's alias → (command name, input relation ref, declared input columns)."""
    out: dict[str, tuple[str, str | None, list[str]]] = {}
    for tbl in tree.find_all(exp.Table):
        inner = tbl.this
        if not (isinstance(inner, exp.Anonymous) and inner.name in commands):
            continue
        alias = tbl.args.get("alias")
        alias_name = alias.this.name if alias is not None and alias.this else None
        if alias_name is None:
            continue
        arg_values = [a.this if isinstance(a, exp.Literal) else a.sql() for a in inner.expressions]
        rel_ref, in_cols = _input_relation(commands[inner.name], arg_values)
        out[alias_name] = (inner.name, rel_ref, in_cols)
    return out


def _input_relation(command: dict, arg_values: list) -> tuple[str | None, list[str]]:
    """The (relation ref, declared input columns) of a command's relation argument (result_set/table_ref).

    The relation ref is the positional arg value; the columns are the arg's declared ``columns`` contract
    (REQ-1159). Returns (None, []) when the command declares no relation argument."""
    for i, arg in enumerate(command.get("arguments") or []):
        if arg.get("arg_kind") in ("result_set", "table_ref"):
            ref = str(arg_values[i]) if i < len(arg_values) else None
            cols = [c["name"] for c in (arg.get("columns") or [])]
            return ref, cols
    return None, []


def _walk(sqlglot_node, graph: LineageGraph, command_names: frozenset[str]) -> None:
    graph.add_node(_node_for(sqlglot_node, command_names))
    for child in sqlglot_node.downstream:
        graph.add_node(_node_for(child, command_names))
        transform, ops = name_transform(sqlglot_node.expression, command_names)
        graph.add_edge(Edge(source=child.name, target=sqlglot_node.name, transform=transform, ops=ops))
        _walk(child, graph, command_names)
