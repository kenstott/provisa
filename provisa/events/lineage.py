# Copyright (c) 2026 Kenneth Stott
# Canary: 638942c1-5017-4330-ac17-f83ff5a0eb61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MV lineage — the DAG edges, derived from SQL (REQ-939), that drive event fan-out.

Every MV is SQL authored to reference its inputs, so the edges are derived from it — no
hand-declared lineage that can drift. The references are resolved against the model by
``provisa.events.nodes.lineage_graph``; ``dependents`` inverts that graph to node → the MVs that
listen for it (what the dispatcher fans an event out to); ``find_cycle`` enforces the acyclic
invariant at registration (an MV that transitively depends on itself would fan out forever).
``extract_inputs`` gives the relation names a statement reads, as text — for reading them, never
for naming a node.
"""

from __future__ import annotations

from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from provisa.core.ir_types import to_ir

# sqlglot DataType categories that carry no determinable output type — an EXPLICIT error for schema
# derivation (REQ-970), never a silent widen to text.
_UNDETERMINED = frozenset({exp.DataType.Type.UNKNOWN, exp.DataType.Type.NULL})


def extract_inputs(sql: str, dialect: str = "postgres") -> set[str]:
    """The qualified relation names an MV's SQL reads, joined ``[catalog.]schema.table`` (whatever
    parts the SQL qualifies). A CTE name defined in the same query is not an input (it is resolved
    locally). Text, as the SQL spells it — the event graph's edges resolve these against the model
    (``provisa.events.nodes``)."""
    tree = sqlglot.parse_one(sql, read=dialect)
    ctes = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    out: set[str] = set()
    for t in tree.find_all(exp.Table):
        parts = [
            p.name
            for p in (t.args.get("catalog"), t.args.get("db"), t.this)
            if p is not None and p.name
        ]
        name = ".".join(parts)
        if name and t.this.name not in ctes:
            out.add(name)
    return out


def _sqlglot_schema(
    input_schemas: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """The per-input {table: {column: type}} map SQLGlot's type annotator reads. Table keys are the
    bare relation name (unqualified) so both ``schema.table`` refs and bare refs resolve. Types pass
    through as their source spelling; SQLGlot parses them into its DataType lattice."""
    out: dict[str, dict[str, str]] = {}
    for table, cols in input_schemas.items():
        out[table.split(".")[-1]] = dict(cols)
    return out


def derive_output_schema(
    sql: str, input_schemas: dict[str, dict[str, str]], dialect: str = "postgres"
) -> list[tuple[str, str]]:
    """REQ-970: derive a derived node's OUTPUT schema — (column name, IR type) pairs — from its SQL
    SELECT via SQLGlot qualification + type inference over the input schemas, NOT taken from a source
    (the structural contrast with a replica, REQ-846). ``input_schemas`` maps each input table to its
    {column: type}. Each output type is mapped native→IR (``to_ir``, REQ-846). An output column whose
    type SQLGlot cannot determine (UNKNOWN/NULL — e.g. an unschematized input or an untyped literal
    NULL) is an EXPLICIT error, never a silent widen. Column order matches the SELECT projection."""
    schema = _sqlglot_schema(input_schemas)
    tree = qualify(sqlglot.parse_one(sql, read=dialect), schema=schema, dialect=dialect)
    annotated = annotate_types(tree, schema=schema, dialect=dialect)
    select = annotated if isinstance(annotated, exp.Select) else annotated.find(exp.Select)
    if select is None:
        raise ValueError(f"REQ-970: no SELECT to derive an output schema from in {sql!r}")
    columns: list[tuple[str, str]] = []
    for proj in select.expressions:
        name = proj.alias_or_name
        dtype = proj.type
        if not name:
            raise ValueError(
                f"REQ-970: output column has no name in {sql!r} — alias every computed column"
            )
        if dtype is None or dtype.this in _UNDETERMINED:
            raise ValueError(
                f"REQ-970: cannot determine the output type of column {name!r} in {sql!r} "
                f"(got {dtype}); the derived store schema is undeterminable — declare the input "
                f"schema or cast the expression"
            )
        columns.append((name, to_ir(dtype.sql(dialect=dialect))))
    if not columns:
        raise ValueError(f"REQ-970: SELECT produced no output columns in {sql!r}")
    return columns


def infer_pk(sql: str, dialect: str = "postgres") -> list[str]:
    """REQ-970: infer a derived table's PK from an UNAMBIGUOUS GROUP BY — the grouping keys uniquely
    identify each output row. Returns the grouped output column names, or ``[]`` when there is no
    group-by (no inferable identity — an operator-declared PK is then required for upsert/delta).
    Only bare column references are treated as inferable keys (a grouped expression has no stable
    output name to key on)."""
    select = sqlglot.parse_one(sql, read=dialect).find(exp.Select)
    if select is None:
        return []
    group = select.args.get("group")
    if group is None:
        return []
    keys = [g.name for g in group.expressions if isinstance(g, exp.Column)]
    return keys if len(keys) == len(group.expressions) else []


# REQ-964 proof-obligation #1 (transform determinism): a pure MV SQL must not read wall-clock or
# randomness — those make replay/regen (REQ-968) non-reproducible. Rejected at registration.
_NONDETERMINISTIC_FUNCS = frozenset(
    {"now", "current_timestamp", "current_date", "current_time", "random", "rand", "uuid", "newid"}
)


def reject_nondeterministic(sql: str, dialect: str = "postgres") -> None:
    """REQ-964 obligation #1 — mechanically enforce TRANSFORM DETERMINISM: an MV's SQL must be a pure
    function of its inputs, so it rejects wall-clock / randomness (``now()``, ``current_timestamp``,
    ``random()``, ``uuid()``, …). A non-deterministic transform breaks addressable, exactly-once
    replay (REQ-968) — fail LOUD at registration, never sandbox silently."""
    tree = sqlglot.parse_one(sql, read=dialect)
    hits: set[str] = set()
    for node in tree.walk():
        name: str | None = None
        if isinstance(node, exp.CurrentTimestamp):
            name = "current_timestamp"
        elif isinstance(node, exp.CurrentDate):
            name = "current_date"
        elif isinstance(node, exp.Anonymous):
            name = node.name.lower()
        elif isinstance(node, exp.Func):
            name = type(node).__name__.lower()
        if name in _NONDETERMINISTIC_FUNCS:
            hits.add(name)
    if hits:
        raise ValueError(
            f"REQ-964: MV SQL is non-deterministic ({sorted(hits)}); a pure transform is required "
            f"for addressable/replayable materialization — remove wall-clock/random"
        )


def is_incrementalizable(sql: str, dialect: str = "postgres") -> bool:
    """REQ-969: True when the SQL admits a SAFE row-wise incremental form — a single-input projection
    of BARE columns with no filter/aggregation/join. For that shape an upstream delta of changed rows
    maps 1:1 to the MV's own delta (upsert by PK), so applying only the delta is provably equivalent
    to a full recompute. Anything richer (JOIN / GROUP BY / aggregate / DISTINCT / WHERE / computed
    projection / set op / window) is NOT incrementalizable HERE — those need delta-rule derivation
    (deferred); declaring incremental on them is an explicit error, never a silent wrong delta."""
    tree = sqlglot.parse_one(sql, read=dialect)
    if tree.find(exp.SetOperation) is not None:
        return False
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return False
    if len(extract_inputs(sql, dialect)) != 1:
        return False
    if select.args.get("joins") or select.args.get("group") or select.args.get("where"):
        return False
    if select.args.get("distinct"):
        return False
    for proj in select.expressions:
        expr = proj.unalias() if isinstance(proj, exp.Alias) else proj
        if not isinstance(expr, exp.Column):
            return False  # a computed projection has no 1:1 delta mapping
    return tree.find(exp.AggFunc) is None and tree.find(exp.Window) is None


def dependents(graph: dict[str, set[str]]) -> dict[str, list[str]]:
    """Invert the lineage: ``input node -> [view nodes that read it]`` — the fan-out target set the
    dispatcher uses when an event on that node arrives. ``graph`` maps each view node to the nodes
    it reads, resolved from the model (``provisa.events.nodes.lineage_graph``). Dependents are
    returned in a stable (sorted) order."""
    rev: dict[str, set[str]] = {}
    for view, inputs in graph.items():
        for node in inputs:
            rev.setdefault(node, set()).add(view)
    return {node: sorted(views) for node, views in rev.items()}


def find_cycle(graph: dict[str, set[str]]) -> list[str] | None:
    """Return a cycle among the views of ``graph`` (a view transitively depending on itself) as an
    ordered node list, or None if acyclic. A cycle would make fan-out never terminate. Only edges
    between view nodes count (an input that is a source table is a leaf)."""
    views = {view: inputs & set(graph) for view, inputs in graph.items()}
    WHITE, GREY, BLACK = 0, 1, 2
    color = dict.fromkeys(views, WHITE)

    def visit(node: str, stack: list[str]) -> list[str] | None:
        color[node] = GREY
        stack.append(node)
        for dep in sorted(views.get(node, ())):
            if color[dep] == GREY:  # back-edge → cycle
                return stack[stack.index(dep) :] + [dep]
            if color[dep] == WHITE:
                found = visit(dep, stack)
                if found:
                    return found
        color[node] = BLACK
        stack.pop()
        return None

    for view in sorted(views):
        if color[view] == WHITE:
            cycle = visit(view, [])
            if cycle:
                return cycle
    return None
