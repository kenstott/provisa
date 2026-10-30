# Copyright (c) 2026 Kenneth Stott
# Canary: f7a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher write statement parser and SQL translator.

Supports five write patterns:

- ``CREATE (n:Label {props}) [SET n.prop = val, ...] [RETURN ...]``
  →  ``INSERT INTO catalog.schema.table (cols) VALUES (vals)``; with a RETURN, ``RETURNING`` the
  node's key, and the RETURN answered by a read of the node created (:func:`returned`)
- ``MATCH (n:Label) WHERE ... DELETE n``  →  ``DELETE FROM catalog.schema.table WHERE ...``
- ``MATCH (n:Label) WHERE ... SET n.prop = val, ...``
  →  ``UPDATE catalog.schema.table SET col = val, ... WHERE ...``
- ``MATCH (a:A {...}) MATCH (b:B {...}) CREATE (a)-[:T {props}]->(b)``, T a relationship a
  registered junction backs (REQ-665, amended 2026-10-27)  →  a read of each matched node's keys
  (:meth:`WriteTranslator.matched_reads`), then ``INSERT INTO junction (keys, attrs) VALUES ...`` (:meth:`relate`)
- ``CREATE (a:A {props}) CREATE (b:B {props}) CREATE (a)-[:T {props}]->(b) [RETURN ...]``, a
  relationship a registered junction backs ending on rows the same statement creates (REQ-665,
  amended 2026-10-28)  →  one ``INSERT ... RETURNING`` per new row, its generated key its identity,
  then the junction rows from those keys, and the RETURN answered by a read of the rows written
- ``MATCH ... [DETACH] DELETE n, r, ...``, any other DELETE: a read of what it names, then each
  relationship's junction row deleted, then each node's row; DETACH first deletes every junction
  row the node is an end of, and refuses a node a foreign-key relationship relates (REQ-2176,
  REQ-665)  →  ``DELETE FROM junction WHERE ...``, then ``DELETE FROM table WHERE ...``
- ``[MATCH ...] MERGE (a:A {props}) MERGE (b:B {props}) MERGE (a)-[:T]->(b) [ON CREATE SET ...]
  [ON MATCH SET ...]``, model nodes and registered junction relationships (REQ-2176): each MERGE
  in turn a read of its pattern, then the INSERTs of what it does not match (each new row's key
  returned) or the UPDATEs its ON MATCH SET makes; a relationship a foreign key derives is
  refused by name before anything is written

:func:`written` runs a write in that order over the surface's own read and write.

Requirements: REQ-665, REQ-666, REQ-667, REQ-668, REQ-670, REQ-2176
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.sql_literals import sql_literal
from provisa.compiler.sql_types import key_list
from provisa.cypher.counters import carried_labels, set_values, unknown, written_rows
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
)


# ---------------------------------------------------------------------------
# Write AST
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CypherParam:
    """An unquoted ``$name`` in a write: a value the request supplies, not text. It reaches the
    SQL as a parameter and is bound by :func:`bind_write_params` — never written as its name."""

    name: str


_PARAM_REF_RE = re.compile(r"\$([A-Za-z_]\w*)")


@dataclass
class MatchedNode:
    """A node a relationship write's leading MATCH binds: its variable, label and property map."""

    variable: str
    label: str
    props: dict[str, Any] = field(default_factory=dict)


@dataclass
class WriteAST:
    """Minimal AST for a Cypher write statement."""

    kind: str  # "create" | "delete" | "update" | "relate" | "graph" | "erase" | "merge"
    label: str  # Cypher label, e.g. "Person"
    variable: str  # node variable, e.g. "n"
    props: dict[str, Any] = field(default_factory=dict)  # CREATE props
    where_expr: str = ""  # raw WHERE expression text (for DELETE/UPDATE)
    # DELETE/UPDATE: the MATCH node's inline property map, each an equality the rows must meet.
    match_props: dict[str, Any] = field(default_factory=dict)
    set_assignments: list[tuple[str, Any]] = field(default_factory=list)  # UPDATE assignments
    # CREATE: the RETURN's items as written, answered by a read of the node created; "" for none.
    returns: str = ""
    # "relate": the nodes the leading MATCH binds, and the relationship's two ends among them
    # (``variable`` the start, ``end`` the end); ``label`` is the relationship type.
    matched: list[MatchedNode] = field(default_factory=list)
    end: str = ""
    # "graph": the rows the statement creates, in order, and the junction rows between them.
    created: list["CreatedNode"] = field(default_factory=list)
    links: list["CreatedLink"] = field(default_factory=list)
    # "erase": the clauses before the DELETE as written, the variables it deletes, and whether it
    # detaches.
    reads: str = ""
    deleted: list[str] = field(default_factory=list)
    detach: bool = False
    # "merge": the MERGE clauses, in order (``reads`` holds the leading MATCHes as written), and
    # the label of each variable the leading MATCHes bind that a MERGE reaches back to.
    merges: list["MergePart"] = field(default_factory=list)
    matched_labels: dict[str, str] = field(default_factory=dict)
    # "merge": the leading MATCHes as reads of their own, one per set of patterns related to one
    # another, each with the variables it binds (:func:`_reads_apart`).
    apart: list[tuple[str, frozenset[str]]] = field(default_factory=list)


@dataclass
class CreatedNode:
    """A row a ``graph`` write creates: its variable ("" for none), label and property map."""

    variable: str
    label: str
    props: dict[str, Any]


@dataclass
class CreatedLink:
    """A junction row a ``graph`` write creates: its variable ("" for none), relationship type,
    property map, and its start and end, each an index into the write's ``created``."""

    variable: str
    rel_type: str
    props: dict[str, Any]
    start: int
    end: int


@dataclass
class MergePart:
    """One MERGE clause of a ``merge`` write: each node occurrence of its pattern (``label`` ""
    for one reaching back to a variable bound before it), each relationship between two of them
    (ends indexes into ``nodes``), and its ON CREATE SET and ON MATCH SET items, each
    (variable, property, value)."""

    nodes: list[CreatedNode]
    links: list[CreatedLink]
    on_create: list[tuple[str, str, Any]] = field(default_factory=list)
    on_match: list[tuple[str, str, Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Write parser
# ---------------------------------------------------------------------------


class CypherWriteParseError(Exception):
    """Raised for unrecognised or unsupported write Cypher syntax."""


_WS = r"\s+"
_OWS = r"\s*"
_IDENT = r"[A-Za-z_]\w*"
_LABEL_PART = r"[A-Za-z_]\w*"
_LABEL = rf"{_LABEL_PART}(?::{_LABEL_PART})*"  # Label or Label:Label2

# CREATE ([n]:Label [{key: val, ...}]) [SET n.key = val, ...] [RETURN ...]
_CREATE_RE = re.compile(
    rf"CREATE{_WS}\({_OWS}({_IDENT})?{_OWS}:({_LABEL}){_OWS}(\{{[^}}]*\}})?{_OWS}\)"
    rf"(?:{_WS}SET{_WS}(.+?))?(?:{_WS}RETURN{_WS}(.+?))?{_OWS}$",
    re.IGNORECASE | re.DOTALL,
)

# <leading MATCHes> CREATE (a)-[:T [{key: val, ...}]]->(b), or (a)<-[:T]-(b)
_RELATE_RE = re.compile(
    rf"(MATCH{_WS}.+?){_WS}CREATE{_WS}\({_OWS}({_IDENT}){_OWS}\){_OWS}(<)?-{_OWS}\[{_OWS}"
    rf"(?:{_IDENT})?{_OWS}:{_OWS}({_IDENT}){_OWS}(\{{[^}}]*\}})?{_OWS}\]{_OWS}-(>)?{_OWS}"
    rf"\({_OWS}({_IDENT}){_OWS}\){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)
# One node of a leading MATCH: (n:Label [{key: val, ...}])
_MATCHED_NODE_RE = re.compile(
    rf"\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}(\{{[^}}]*\}})?{_OWS}\)", re.DOTALL
)

# The node a DELETE or SET matches, with an optional inline property map, then an optional WHERE:
# MATCH (n:Label [{key: val, ...}]) [WHERE <expr>]
_MATCHED = (
    rf"MATCH{_WS}\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}(\{{[^}}]*\}})?{_OWS}\)"
    rf"(?:{_WS}WHERE{_WS}(.+?))?"
)

# MATCH (n:Label [{...}]) [WHERE <expr>] DELETE n
_DELETE_RE = re.compile(
    rf"{_MATCHED}{_WS}DELETE{_WS}({_IDENT}){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)

# MATCH (n:Label [{...}]) [WHERE <expr>] SET n.prop = val [, ...]
_UPDATE_RE = re.compile(
    rf"{_MATCHED}{_WS}SET{_WS}(.+){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_literal(text: str) -> Any:
    """Parse a simple Cypher literal value to a Python value."""
    text = text.strip()
    param = _PARAM_REF_RE.fullmatch(text)
    if param:
        return CypherParam(param.group(1))
    if (text.startswith("'") and text.endswith("'")) or (
        text.startswith('"') and text.endswith('"')
    ):
        return text[1:-1]
    upper = text.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    if upper == "NULL":
        return None
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def _matched_rows(map_text: str | None, where_text: str | None, kind: str) -> tuple[dict, str]:
    """The MATCH node's inline property map and its WHERE text, of a DELETE or SET: one of them
    at least, so the write names the rows it changes and never every row of the table."""
    if map_text is None and where_text is None:
        raise CypherWriteParseError(
            f"a {kind} must say which rows it writes: give its MATCH node a property map or a "
            "WHERE clause"
        )
    return (_parse_props(map_text) if map_text else {}), (where_text or "").strip()


def _parse_props(props_text: str) -> dict[str, Any]:
    """Parse a Cypher map literal ``{key: val, ...}`` into a dict."""
    inner = props_text.strip()
    if inner.startswith("{"):
        inner = inner[1:]
    if inner.endswith("}"):
        inner = inner[:-1]
    result: dict[str, Any] = {}
    # Simple tokenisation: split on commas not inside strings.
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    in_str: str | None = None
    for ch in inner:
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
        elif ch in ("'", '"'):
            in_str = ch
            current.append(ch)
        elif ch == "{":
            depth += 1
            current.append(ch)
        elif ch == "}":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise CypherWriteParseError(f"Cannot parse property pair: {part!r}")
        key, _, val = part.partition(":")
        result[key.strip()] = _parse_literal(val.strip())
    return result


def _parse_set_assignments(set_text: str, variable: str) -> list[tuple[str, Any]]:
    """Parse ``n.prop = val, n.prop2 = val2`` into a list of (prop, value) tuples."""
    assignments: list[tuple[str, Any]] = []
    # Split on commas not inside strings.
    parts: list[str] = []
    current: list[str] = []
    in_str: str | None = None
    for ch in set_text:
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
        elif ch in ("'", '"'):
            in_str = ch
            current.append(ch)
        elif ch == ",":
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    for part in parts:
        part = part.strip()
        if not part:
            continue
        m = re.match(rf"{re.escape(variable)}\.({_IDENT})\s*=\s*(.+)$", part, re.IGNORECASE)
        if not m:
            raise CypherWriteParseError(f"Cannot parse SET assignment: {part!r}")
        prop = m.group(1).strip()
        val = _parse_literal(m.group(2).strip())
        assignments.append((prop, val))
    return assignments


#: The forms a Cypher write takes: each writes rows of one registered table (a node label, or a
#: registered junction a relationship type names).
WRITE_FORMS = (
    "CREATE (n:Label {key: value}) [SET n.key = value] [RETURN ...]; "
    "MATCH (n:Label {key: value}) [WHERE ...] SET n.key = value; "
    "MATCH (n:Label {key: value}) [WHERE ...] DELETE n; "
    "MATCH (a:Label {key: value}) MATCH (b:Label {key: value}) CREATE (a)-[:TYPE {key: value}]->(b); "
    "CREATE (a:Label {key: value}) CREATE (b:Label {key: value}) "
    "CREATE (a)-[:TYPE {key: value}]->(b) [RETURN ...]; "
    "MATCH ... [DETACH] DELETE n, r; "
    "[MATCH ...] MERGE (a:Label {key: value}) [MERGE (b:Label {key: value})] "
    "[MERGE (a)-[:TYPE]->(b)] [ON CREATE SET a.key = value] [ON MATCH SET a.key = value]"
)


def _parse_relate(m: re.Match) -> WriteAST:
    """A relationship CREATE between nodes its leading MATCH binds (REQ-665, amended
    2026-10-27): the leading clauses are MATCHes of labelled nodes with property maps and
    nothing else, and both ends are nodes they bind. A relationship ending on rows the same
    statement creates is :func:`_parse_graph`'s."""
    lead, left, inbound, rel_type, props, outbound, right = m.groups()
    if bool(inbound) == bool(outbound):
        raise CypherWriteParseError(
            "a relationship CREATE names its direction: (a)-[:TYPE]->(b) or (a)<-[:TYPE]-(b)"
        )
    matched = [
        MatchedNode(v, label, _parse_props(mp) if mp else {})
        for v, label, mp in _MATCHED_NODE_RE.findall(lead)
    ]
    rest = re.sub(r"\bMATCH\b|,|\s", "", _MATCHED_NODE_RE.sub("", lead), flags=re.IGNORECASE)
    if rest:
        raise CypherWriteParseError(
            f"a relationship CREATE's leading clauses are MATCHes of labelled nodes, each with a "
            f"property map: {lead.strip()!r} is not. A Cypher write takes one of these forms: "
            f"{WRITE_FORMS}"
        )
    bound = [n.variable for n in matched]
    for variable in (left, right):
        if bound.count(variable) != 1:
            raise CypherWriteParseError(
                f"a relationship CREATE joins two nodes its MATCH binds once each, or two rows "
                f"the statement's own CREATE makes; {variable!r} is neither"
            )
    start, end = (right, left) if inbound else (left, right)
    return WriteAST(
        kind="relate",
        label=rel_type,
        variable=start,
        end=end,
        props=_parse_props(props) if props else {},
        matched=matched,
    )


def _returns_text(query: str) -> str:
    """The statement's RETURN and what follows it, as written ("" for none): the text after its
    one RETURN keyword outside a string."""
    from provisa.cypher.parser import _normalize_legacy_params, _tokenize

    normal = _normalize_legacy_params(query)
    tokens, _ = _tokenize(normal)
    at = [t for t in tokens if t.type == "IDENT" and t.value.upper() == "RETURN"]
    return normal[at[-1].end :].strip() if at else ""


def _parse_graph(query: str) -> WriteAST | None:
    """A statement of CREATE clauses alone, and a RETURN, making more than one row or a
    relationship (REQ-665, amended 2026-10-28): each labelled node with a property map a row of
    its table, each relationship a row of the junction backing it between two of those rows, its
    ends named by their variables. None for any other statement."""
    from provisa.cypher.expr_parser import CypherExprParseError
    from provisa.cypher.parser import CypherParseError, parse_cypher_statement
    from provisa.gql.ir import CreateClause

    try:
        ir = parse_cypher_statement(query)
    except (CypherParseError, CypherExprParseError):
        # Not this form: the forms after it read the statement, or refuse it by name.
        return None
    steps = ir.pipeline
    if not steps or not all(isinstance(s, CreateClause) for s in steps):
        return None
    paths = [path for s in steps if isinstance(s, CreateClause) for path in s.patterns]
    if sum(len(p.nodes) for p in paths) < 2 and not any(p.rels for p in paths):
        return None  # one node: the CREATE form's
    created: list[CreatedNode] = []
    links: list[CreatedLink] = []
    at: dict[str, int] = {}
    for path in paths:
        ends: list[int] = []
        for node in path.nodes:
            var = node.variable or ""
            if not node.labels and not node.property_text:
                if var not in at:
                    raise CypherWriteParseError(
                        f"a relationship CREATE joins two nodes its MATCH binds once each, or two "
                        f"rows the statement's own CREATE makes; {var or '()'!r} is neither"
                    )
                ends.append(at[var])
                continue
            if var and var in at:
                raise CypherWriteParseError(f"the statement creates {var!r} once")
            label = ":".join(node.labels)
            if not node.labels or not node.property_text:
                raise CypherWriteParseError(
                    f"each row a CREATE makes, a relationship's end included, is a labelled node "
                    f"with a property: ({var}{':' + label if label else ''}) is not. A Cypher "
                    f"write takes one of these forms: {WRITE_FORMS}"
                )
            props = {k: _parse_literal(v) for k, v in node.property_text.items()}
            created.append(CreatedNode(var, label, props))
            if var:
                at[var] = len(created) - 1
            ends.append(len(created) - 1)
        for i, rel in enumerate(path.rels):
            if len(rel.types) != 1 or rel.variable_length:
                raise CypherWriteParseError(
                    "a relationship CREATE names one type and one hop: (a)-[:TYPE]->(b)"
                )
            if rel.direction not in ("right", "left"):
                raise CypherWriteParseError(
                    "a relationship CREATE names its direction: (a)-[:TYPE]->(b) or (a)<-[:TYPE]-(b)"
                )
            left, right = ends[i], ends[i + 1]
            start, end = (left, right) if rel.direction == "right" else (right, left)
            props = {k: _parse_literal(v) for k, v in rel.property_text.items()}
            links.append(CreatedLink(rel.variable or "", rel.types[0], props, start, end))
    return WriteAST(
        kind="graph",
        label=created[0].label,
        variable=created[0].variable,
        created=created,
        links=links,
        returns=_returns_text(query),
    )


def _parse_erase(query: str) -> WriteAST | None:
    """A statement ending in its one write, a ``[DETACH] DELETE`` of the nodes and relationships
    its leading clauses bind, each named by its variable (REQ-2176): read through the graph IR.
    None when the statement holds no DELETE or is no Cypher the IR reads."""
    from provisa.cypher.expr_parser import CypherExprParseError
    from provisa.cypher.parser import CypherParseError, parse_cypher_statement
    from provisa.gql.expr_ast import Variable
    from provisa.gql.ir import DeleteClause

    try:
        ir = parse_cypher_statement(query)
    except (CypherParseError, CypherExprParseError):
        # Not this form: the forms after it read the statement, or refuse it by name.
        return None
    deletes = [step for step in ir.pipeline if isinstance(step, DeleteClause)]
    if not deletes:
        return None
    clause = deletes[0]
    if len(ir.writes) > 1 or ir.pipeline[-1] is not clause or ir.return_clause is not None:
        raise CypherWriteParseError(
            "a DELETE is its statement's one write and ends it: MATCH ... [DETACH] DELETE n, r"
        )
    deleted: list[str] = []
    for expression in clause.expressions:
        if not isinstance(expression, Variable):
            raise CypherWriteParseError("a DELETE names each node or relationship by its variable")
        deleted.append(expression.name)
    reads = query[: clause.start].strip()
    return WriteAST(
        kind="erase",
        label="",
        variable=deleted[0],
        reads=reads,
        deleted=list(dict.fromkeys(deleted)),
        detach=clause.detach,
    )


def _merge_literal(text: str) -> Any:
    """A MERGE's inline property value as written: a literal or a parameter, refused otherwise --
    a MERGE matches on values the statement states."""
    text = text.strip()
    if (
        _PARAM_REF_RE.fullmatch(text)
        or re.fullmatch(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"", text)
        or text.upper() in ("TRUE", "FALSE", "NULL")
        or re.fullmatch(r"-?\d+(?:\.\d+)?", text)
    ):
        return _parse_literal(text)
    raise CypherWriteParseError(
        f"a MERGE of model data matches on a literal or a parameter: {text!r} is neither"
    )


def _merge_sets(items: list[Any], when: str) -> list[tuple[str, str, Any]]:
    """ON CREATE / ON MATCH SET items as (variable, property, value): each a property set to a
    literal or a parameter. A label SET on a model row is the graph session's, refused here."""
    from provisa.gql.expr_ast import Literal, Parameter

    out = []
    for item in items:
        if item.property is None:
            raise CypherWriteParseError(
                f"ON {when} SET of a MERGE of model data sets a property, v.key = value"
            )
        if isinstance(item.value, Parameter):
            out.append((item.variable, item.property, CypherParam(item.value.name)))
        elif isinstance(item.value, Literal):
            out.append((item.variable, item.property, item.value.value))
        else:
            raise CypherWriteParseError(
                f"ON {when} SET of a MERGE of model data sets {item.variable}.{item.property} to "
                "a literal or a parameter"
            )
    return out


def _parse_merge(query: str) -> WriteAST:
    """A statement of leading MATCHes and MERGE clauses of model nodes and registered junction
    relationships (REQ-2176), read through the graph IR: each MERGE's pattern, each node a
    labelled node with a property map or a variable bound before it, and its ON CREATE SET and
    ON MATCH SET items."""
    from provisa.cypher.expr_parser import CypherExprParseError
    from provisa.cypher.parser import CypherParseError, parse_cypher_statement
    from provisa.gql.ir import MatchStep, MergeClause

    try:
        ir = parse_cypher_statement(query)
    except (CypherParseError, CypherExprParseError) as exc:
        raise CypherWriteParseError(str(exc)) from exc
    steps = ir.pipeline
    first = next((i for i, s in enumerate(steps) if isinstance(s, MergeClause)), None)
    if first is None:
        raise CypherWriteParseError(f"MERGE is a clause of its own: {WRITE_FORMS}")
    lead, merges = steps[:first], steps[first:]
    if (
        not all(isinstance(s, MatchStep) for s in lead)
        or not all(isinstance(s, MergeClause) for s in merges)
        or ir.return_clause is not None
    ):
        raise CypherWriteParseError(
            "a MERGE of model data is leading MATCHes, then MERGE clauses, and ends its "
            f"statement: {WRITE_FORMS}"
        )
    matched_labels: dict[str, str] = {}
    bound: set[str] = set()
    for step in lead:
        assert isinstance(step, MatchStep)
        for match in step.matches:
            for node in getattr(match.pattern, "nodes", []):
                if node.variable:
                    bound.add(node.variable)
                    if node.labels:
                        matched_labels[node.variable] = ":".join(node.labels)
            for rel in getattr(match.pattern, "rels", []):
                if rel.variable:
                    bound.add(rel.variable)
    parts: list[MergePart] = []
    for at, clause in enumerate(merges):
        assert isinstance(clause, MergeClause)
        nodes: list[CreatedNode] = []
        links: list[CreatedLink] = []
        for i, node in enumerate(clause.pattern.nodes):
            var = node.variable or f"{_KEY_PARAM}m{at}_{i}"
            props = {k: _merge_literal(v) for k, v in node.property_text.items()}
            if var in bound:
                if node.labels or props:
                    raise CypherWriteParseError(
                        f"Variable `{var}` already declared: a MERGE reaches back to it by its "
                        "variable alone"
                    )
                nodes.append(CreatedNode(var, "", {}))
                continue
            if any(n.variable == var for n in nodes):
                nodes.append(CreatedNode(var, "", {}))
                continue
            if not node.labels:
                raise CypherWriteParseError(
                    f"each node a MERGE of model data makes is a labelled node: ({var}) is not "
                    "one, nor a variable bound before it"
                )
            nodes.append(CreatedNode(var, ":".join(node.labels), props))
        for j, rel in enumerate(clause.pattern.rels):
            if len(rel.types) != 1 or rel.variable_length:
                raise CypherWriteParseError(
                    "a relationship MERGE names one type and one hop: (a)-[:TYPE]->(b)"
                )
            if rel.direction not in ("right", "left"):
                raise CypherWriteParseError(
                    "a relationship MERGE of model data names its direction: (a)-[:TYPE]->(b)"
                )
            start, end = (j, j + 1) if rel.direction == "right" else (j + 1, j)
            props = {k: _merge_literal(v) for k, v in rel.property_text.items()}
            var = rel.variable or f"{_KEY_PARAM}r{at}_{j}"
            if var in bound:
                raise CypherWriteParseError(f"Variable `{var}` already declared")
            links.append(CreatedLink(var, rel.types[0], props, start, end))
        part = MergePart(
            nodes,
            links,
            _merge_sets(clause.on_create, "CREATE"),
            _merge_sets(clause.on_match, "MATCH"),
        )
        named = {n.variable for n in nodes} | {link.variable for link in links}
        for var, _prop, _val in part.on_create + part.on_match:
            if var not in named:
                raise CypherWriteParseError(
                    f"ON CREATE / ON MATCH SET names {var}, which its MERGE does not"
                )
        parts.append(part)
        bound |= named
    head = parts[0].nodes[0]
    opening = merges[0]
    assert isinstance(opening, MergeClause)
    reads = query[: opening.start].strip()
    return WriteAST(
        kind="merge",
        label=head.label,
        variable=head.variable,
        reads=reads,
        merges=parts,
        matched_labels=matched_labels,
        apart=_reads_apart(lead, reads) if lead else [],
    )


def _clause_variables(clause: Any) -> set[str]:
    """The variables a MATCH clause binds: its path's, each node's and each relationship's."""
    pattern = getattr(clause.pattern, "pattern", clause.pattern)
    named = [clause.variable, *(n.variable for n in pattern.nodes)]
    named += [r.variable for r in pattern.rels]
    return {v for v in named if v}


def _conjuncts(expression: Any) -> list[Any]:
    """``expression``'s top-level AND operands, parentheses around an AND opened."""
    from provisa.gql.expr_ast import Binary, Paren

    if isinstance(expression, Paren) and isinstance(expression.inner, Binary):
        return _conjuncts(expression.inner)
    if isinstance(expression, Binary) and expression.op.upper() == "AND":
        return _conjuncts(expression.left) + _conjuncts(expression.right)
    return [expression]


def _reads_apart(lead: list[Any], written: str) -> list[tuple[str, frozenset[str]]]:
    """The leading MATCHes of a MERGE as reads of their own (REQ-2176, as REQ-665's relationship
    CREATE reads its ends, :meth:`WriteTranslator.matched_reads`): the patterns no shared variable
    or WHERE condition relates are every combination of their rows, which the writer takes from
    each one's own read, so no read combines unrelated tables (REQ-603). Each read is given with
    the variables it binds; one read of them all is the MATCHes as written.

    A condition is read with the patterns of its variables; one naming no variable, one with a
    subquery, one naming a variable its own clause's patterns do not reach, keeps that clause's
    patterns in one read, as written."""
    from dataclasses import replace

    from provisa.cypher.printer import CypherPrintRefused, _pattern, expression
    from provisa.gql.expr_ast import SubqueryExpr, Variable, walk
    from provisa.gql.ir import PathPattern

    clauses = [(s, m) for s, step in enumerate(lead) for m in step.matches]
    parent = list(range(len(clauses)))

    def top(i: int) -> int:
        while parent[i] != i:
            i = parent[i]
        return i

    def join(members: set[int]) -> None:
        roots = sorted({top(i) for i in members})
        for r in roots[1:]:
            parent[r] = roots[0]

    owner: dict[str, int] = {}
    for i, (_, clause) in enumerate(clauses):
        for var in _clause_variables(clause):
            join({i, owner.setdefault(var, i)})
    conditions: list[tuple[int, int, Any]] = []  # (step, clause it reads with, condition)
    for s, step in enumerate(lead):
        if step.where is None:
            continue
        here = {i for i, (t, _) in enumerate(clauses) if t == s}
        for condition in _conjuncts(step.where.expression):
            nodes = list(walk(condition))
            names = {e.name for e in nodes if isinstance(e, Variable)}
            members = {owner[n] for n in names if n in owner}
            if (
                not names
                or names - owner.keys()
                or any(isinstance(e, SubqueryExpr) for e in nodes)
                or not {top(m) for m in members} <= {top(i) for i in here}
            ):
                members |= here
            join(members)
            conditions.append((s, next(iter(members)), condition))
    roots = list(dict.fromkeys(top(i) for i in range(len(clauses))))
    if len(roots) == 1:
        return [(written, frozenset(owner))]
    reads = []
    for root in roots:
        lines: list[str] = []
        for s in range(len(lead)):
            for i, (t, clause) in enumerate(clauses):
                if t != s or top(i) != root:
                    continue
                pattern = getattr(clause.pattern, "pattern", clause.pattern)
                if (
                    clause.variable
                    or pattern is not clause.pattern
                    or any(n.label_alternation for n in pattern.nodes)
                ):
                    raise CypherWriteParseError(
                        "a MERGE's MATCH of patterns unrelated to one another reads each apart: "
                        "a path variable, a path function or a label alternation among them has "
                        "no read of its own"
                    )
                bare = PathPattern(
                    nodes=[replace(n, properties={}, property_text={}) for n in pattern.nodes],
                    rels=[replace(r, properties={}, property_text={}) for r in pattern.rels],
                )
                try:
                    text = _pattern(bare)
                except CypherPrintRefused as exc:
                    raise CypherWriteParseError(
                        "a MERGE's MATCH of patterns unrelated to one another reads each apart, "
                        f"and this one has no read of its own: {exc}"
                    ) from exc
                lines.append(f"{'OPTIONAL MATCH' if clause.optional else 'MATCH'} {text}")
            held = [c for t, i, c in conditions if t == s and top(i) == root]
            if held:
                lines.append("WHERE " + " AND ".join(f"({expression(c)})" for c in held))
        variables = frozenset(v for v, i in owner.items() if top(i) == root)
        reads.append((" ".join(lines), variables))
    return reads


def _remove_refusal(query: str) -> str:
    """Why a REMOVE is not a governed write: a property's REMOVE has no write form; a label's
    is the graph session's, in a statement of its own."""
    from provisa.cypher.parser import CypherParseError, parse_cypher_statement
    from provisa.gql.ir import RemoveClause

    try:
        ast = parse_cypher_statement(query)
    except CypherParseError as exc:
        raise CypherWriteParseError(str(exc)) from exc
    removes = [w for w in ast.writes if isinstance(w, RemoveClause)]
    lead = (
        "Cypher write pattern 'REMOVE' is not a governed write: a governed write has one exact "
        "target, one model table or one registered junction."
    )
    if any(item.property is not None for w in removes for item in w.items):
        return (
            f"{lead} A REMOVE of a property is not written: a model node's property is written "
            f"by SET, of a column its table declares. A Cypher write takes one of these forms: "
            f"{WRITE_FORMS}"
        )
    return (
        f"{lead} REMOVE n:L takes a label off a node in the graph session, in a statement of "
        f"its own: [MATCH ...] REMOVE n:L, with nothing after it. A Cypher write takes one of "
        f"these forms: {WRITE_FORMS}"
    )


def parse_cypher_write(query: str) -> WriteAST:
    """Parse a Cypher write statement into a WriteAST.

    Supports CREATE, MATCH-DELETE, and MATCH-SET patterns.
    Raises CypherWriteParseError for unrecognised syntax.
    """
    query = query.strip()
    from provisa.cypher.parser import (
        CypherParseError,
        refuse_cypher_definition,
        refuse_multiple_statements,
    )

    try:
        refuse_multiple_statements(query)
    except CypherParseError as exc:
        raise CypherWriteParseError(str(exc)) from exc
    # An index or a constraint is a definition, not a write of rows: refused as one, in the
    # pipeline's own words, before any write pattern is tried.
    refuse_cypher_definition(query)

    # REQ-2176: "SET n:L and REMOVE n:L write node_labels rows on ephemeral or model nodes; a
    # node never moves table." A label REMOVE is the graph session's (provisa.ephemeral.labels),
    # never the governed writer's; the requirement gives a REMOVE of a property no meaning
    # ("SET of a property is a governed write on a model node (declared columns only)"), so it is
    # refused here by name.
    if re.search(r"\bREMOVE\b", query, re.IGNORECASE):
        raise CypherWriteParseError(_remove_refusal(query))

    # REQ-2176 (amended 2026-10-28): a MERGE of model nodes and registered junction
    # relationships is a governed write, read through the graph IR.
    if re.search(r"\bMERGE\b", query, re.IGNORECASE):
        return _parse_merge(query)

    # REQ-2176: a DETACH DELETE, or a DELETE outside the one-node form below (a relationship's,
    # several variables'), is read through the graph IR. The one-node regex is not tried on a
    # DETACH, whose keyword it would absorb into its WHERE.
    if re.search(r"\bDELETE\b", query, re.IGNORECASE) and (
        re.search(r"\bDETACH\b", query, re.IGNORECASE) or not _DELETE_RE.match(query)
    ):
        erase = _parse_erase(query)
        if erase is not None:
            return erase

    # REQ-665 (amended 2026-10-27): a relationship a registered junction backs is a row of that
    # junction, written by a CREATE between two nodes the statement matches.
    m = _RELATE_RE.match(query)
    if m:
        return _parse_relate(m)

    # REQ-665 (amended 2026-10-28): CREATE clauses making rows and the junction rows between
    # them, as a Neo4j client writes a graph, are one governed write.
    if re.match(r"CREATE\b", query, re.IGNORECASE):
        graph = _parse_graph(query)
        if graph is not None:
            return graph

    # REQ-665: any other relationship pattern (`-[...]->`, `<-[...]-`, or a bare `--` edge) in a
    # write statement is refused at translation time.
    if re.search(r"-\s*\[|\]\s*-|\)\s*<?-{1,2}>?\s*\(", query):
        raise CypherWriteParseError(
            "A Cypher write creates a relationship as a row of the junction table registered for "
            "its type, between two nodes its MATCH binds or two rows its own CREATE makes; a "
            "relationship derived from a foreign key is written as the row's key column. A "
            f"Cypher write takes one of these forms: {WRITE_FORMS}"
        )

    m = _CREATE_RE.match(query)
    if m:
        variable, label, props_text, set_text, returns = m.groups()
        variable = variable or ""  # a node created with no variable: nothing else names it
        props = _parse_props(props_text) if props_text else {}
        # A SET on the node the statement creates gives it that property, as its map would.
        props.update(_parse_set_assignments(set_text, variable) if set_text else [])
        if not props:
            # A row of nothing is no write: the node created is given a property, by its map or a SET.
            raise CypherWriteParseError(
                f"This write is in a form Provisa does not read: {query!r}: the node it creates "
                f"has no property. A Cypher write takes one of these forms: {WRITE_FORMS}"
            )
        return WriteAST(
            kind="create",
            label=label,
            variable=variable,
            props=props,
            returns=(returns or "").strip(),
        )

    m = _DELETE_RE.match(query)
    if m:
        variable, label = m.group(1), m.group(2)
        match_props, where_expr = _matched_rows(m.group(3), m.group(4), "DELETE")
        del_var = m.group(5)
        if del_var.lower() != variable.lower():
            raise CypherWriteParseError(
                f"DELETE variable {del_var!r} does not match MATCH variable {variable!r}"
            )
        return WriteAST(
            kind="delete",
            label=label,
            variable=variable,
            where_expr=where_expr,
            match_props=match_props,
        )

    m = _UPDATE_RE.match(query)
    if m:
        variable, label = m.group(1), m.group(2)
        match_props, where_expr = _matched_rows(m.group(3), m.group(4), "SET")
        set_text = m.group(5).strip()
        assignments = _parse_set_assignments(set_text, variable)
        return WriteAST(
            kind="update",
            label=label,
            variable=variable,
            where_expr=where_expr,
            set_assignments=assignments,
            match_props=match_props,
        )

    raise CypherWriteParseError(
        f"This write is in a form Provisa does not read: {query!r}. A Cypher write takes one of "
        f"these forms: {WRITE_FORMS}"
    )


# ---------------------------------------------------------------------------
# SQL value rendering
# ---------------------------------------------------------------------------


_NUMERIC_TYPES = frozenset(
    {"integer", "int", "bigint", "smallint", "numeric", "decimal", "double", "float", "real"}
)


def _write_value(val: Any, col_type: str | None = None) -> str:
    """A property value as a literal of the statement, which is PostgreSQL SQL (transpiled to the
    source's dialect afterwards): written with that dialect's one literal rule
    (``sql_literal``), so a value never ends its literal. A numeric string bound for a numeric
    column is written as the number."""
    if isinstance(val, CypherParam):
        return f"${val.name}"  # bound by bind_write_params, never as text
    if isinstance(val, str) and col_type and col_type.lower() in _NUMERIC_TYPES:
        try:
            return str(int(val))
        except ValueError:
            pass
        try:
            # Through sql_literal so a non-finite spelling ("nan"/"inf") is written as the
            # dialect's literal or refused by name — never as a bare, unquoted identifier.
            return sql_literal(float(val), "postgres")
        except ValueError:
            pass
    return sql_literal(val, "postgres")


def _q(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


# ---------------------------------------------------------------------------
# WHERE clause rewriting
# ---------------------------------------------------------------------------

# Rewrite ``n.prop`` → ``"col"`` using the node's property map.
_PROP_ACCESS_RE = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b")


def _read_column(mapping: NodeMapping, prop: str) -> str:
    """``prop`` of a node a write matches, as SQL: read as a read reads it (REQ-2136) -- its
    ``_id`` the key, a property the label has not null, never a column by a name it does not give."""
    from provisa.gql.select_builder import NODE_KEY_PROPERTY, node_key

    if prop == NODE_KEY_PROPERTY:
        # The statement names the table's columns bare: no alias.
        return node_key(None, mapping).sql(dialect="postgres")
    col = mapping.properties.get(prop)
    return _q(col) if col is not None else "NULL"


def _written_column(mapping: NodeMapping, prop: str) -> str:
    """The column a write sets ``prop`` in: one the label publishes for the role. A write is never
    silent: any other name is refused by name, where a read of it is null."""
    col = mapping.properties.get(prop)
    if col is None:
        raise CypherWriteParseError(f"{mapping.label} has no property {prop}")
    return _q(col)


def _rewrite_where(where_expr: str, variable: str, mapping: NodeMapping) -> str:
    """Rewrite Cypher property accesses to SQL column names in a WHERE expression.

    E.g. ``n.age > 21`` → ``"age" > 21``  (using mapping.properties).
    """

    def _replace(m: re.Match) -> str:
        var = m.group(1)
        prop = m.group(2)
        if var is None or prop is None or var.lower() != variable.lower():
            return m.group(0)
        return _read_column(mapping, prop)

    return _PROP_ACCESS_RE.sub(_replace, where_expr)


# ---------------------------------------------------------------------------
# WriteTranslator
# ---------------------------------------------------------------------------


class WriteTranslator:
    """Translate a WriteAST to a SQL DML statement.

    Args:
        label_map: Registered node/relationship mappings.
    """

    def __init__(self, label_map: CypherLabelMap) -> None:
        self._label_map = label_map

    def _resolve_mapping(self, label: str) -> NodeMapping:
        parts = [self._label_map.canonical_label(p) for p in label.split(":")]
        full_type = [p for p in parts if p in self._label_map.nodes]
        domain_hits = [p for p in parts if p in self._label_map.domains]
        table_hits = [p for p in parts if p in self._label_map.nodes_by_table]

        type_name: str | None = None
        if full_type:
            if len(full_type) > 1:
                raise CypherWriteParseError(
                    f"Ambiguous label {label!r}: multiple full type labels {full_type}"
                )
            type_name = full_type[0]
        elif domain_hits and table_hits:
            domain_set = set(self._label_map.domains[domain_hits[0]])
            table_set = set(self._label_map.nodes_by_table[table_hits[0]])
            candidates = domain_set & table_set
            if len(candidates) == 1:
                type_name = candidates.pop()
            elif len(candidates) > 1:
                raise CypherWriteParseError(
                    f"Ambiguous label {label!r}: matches multiple types {sorted(candidates)}"
                )
            else:
                raise CypherWriteParseError(f"No node type found for label {label!r}")
        elif table_hits:
            candidates = self._label_map.nodes_by_table[table_hits[0]]
            if len(candidates) == 1:
                type_name = candidates[0]
            else:
                raise CypherWriteParseError(
                    f"Label {label!r} is ambiguous across domains {sorted(candidates)}; "
                    "qualify with Domain:Table"
                )

        if type_name is None:
            raise CypherWriteParseError(f"Label {label!r} is not registered in the label map")
        return self._label_map.nodes[type_name]

    def _qualified_table(self, mapping: NodeMapping) -> str:
        parts = []
        if mapping.catalog_name:
            parts.append(_q(mapping.catalog_name))
        if mapping.schema_name:
            parts.append(_q(mapping.schema_name))
        parts.append(_q(mapping.sql_table_name))
        return ".".join(parts)

    def translate(self, ast: WriteAST) -> str:
        """Translate a WriteAST to a SQL DML string."""
        if ast.kind == "create":
            return self._translate_create(ast)
        if ast.kind == "delete":
            return self._translate_delete(ast)
        if ast.kind == "update":
            return self._translate_update(ast)
        if ast.kind == "relate":
            raise CypherWriteParseError(
                "a relationship CREATE is written from its matched nodes' keys: "
                "matched_reads, pairs, then relate"
            )
        if ast.kind == "graph":
            raise CypherWriteParseError(
                "a CREATE of rows and the relationships between them is written from the keys "
                "its rows are given: graph_links, graph_rows, then relate_rows"
            )
        if ast.kind == "erase":
            raise CypherWriteParseError(
                "a DELETE of what its MATCH binds is written from a read of it: erase_read, "
                "then erase"
            )
        if ast.kind == "merge":
            raise CypherWriteParseError(
                "a MERGE is written from a read of each of its patterns: written runs it"
            )
        raise CypherWriteParseError(f"Unknown WriteAST kind: {ast.kind!r}")

    def _translate_create(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        cols: list[str] = []
        vals: list[str] = []
        for prop, val in ast.props.items():
            cols.append(_written_column(mapping, prop))
            vals.append(_write_value(val))
        if not cols:
            raise CypherWriteParseError("CREATE statement has no properties")
        cols_sql = ", ".join(cols)
        vals_sql = ", ".join(vals)
        insert = f"INSERT INTO {table} ({cols_sql}) VALUES ({vals_sql})"
        if not ast.returns:
            return insert
        # The RETURN is answered by a read of the node created, found by its key (returned).
        from provisa.gql.select_builder import NODE_KEY_PROPERTY, node_key

        key = node_key(None, mapping).sql(dialect="postgres")
        return f"{insert} RETURNING {key} AS {_q(NODE_KEY_PROPERTY)}"

    def _junction_of(self, ast: WriteAST) -> tuple[RelationshipMapping, NodeMapping, NodeMapping]:
        """The registered junction relationship a relationship CREATE writes, and its two ends'
        nodes (REQ-665, amended 2026-10-27)."""
        labels = {n.variable: n.label for n in ast.matched}
        return self._junction(ast.label, labels[ast.variable], labels[ast.end])

    def _junction(
        self, rel_type: str, start_label: str, end_label: str
    ) -> tuple[RelationshipMapping, NodeMapping, NodeMapping]:
        """The registered junction relationship ``rel_type`` from ``start_label`` to
        ``end_label``, and its two ends' nodes: refused by name unless the type is registered
        between those two labels and a junction backs it, so both ends are governed rows of the
        junction's end tables (REQ-665, amended 2026-10-28)."""
        start = self._resolve_mapping(start_label)
        end = self._resolve_mapping(end_label)
        declared = [
            *self._label_map.aliases.get(rel_type, []),
            *(
                [self._label_map.relationships[rel_type]]
                if rel_type in self._label_map.relationships
                else []
            ),
        ]
        if not declared:
            raise CypherWriteParseError(
                f"relationship type {rel_type} is not registered: a write adds a relationship "
                "the model declares, never a new type"
            )
        names = ({start.type_name, start.label}, {end.type_name, end.label})
        between = [r for r in declared if r.source_label in names[0] and r.target_label in names[1]]
        if not between:
            raise CypherWriteParseError(
                f"relationship type {rel_type} is not registered from {start.label} to {end.label}"
            )
        rel = between[0]
        if rel.via is None:
            raise CypherWriteParseError(
                f"relationship type {rel_type} is derived from a foreign key, not stored in a "
                "junction table, so Cypher does not write it: write the row's key column instead"
            )
        return rel, start, end

    def _end_keys(self, ast: WriteAST, variable: str) -> list[str]:
        """The key properties a matched node gives the junction row: the start's source key and
        the end's target key, each in the junction's key order (both, for a node related to
        itself)."""
        rel, start, end = self._junction_of(ast)
        keys = []
        if variable == ast.variable:
            keys += [start.property_of(c) for c in key_list(rel.join_source_column)]
        if variable == ast.end:
            keys += [end.property_of(c) for c in key_list(rel.join_target_column)]
        return keys

    def matched_reads(self, ast: WriteAST, provided: dict[str, Any]) -> list[tuple[str, dict]]:
        """One read per node the leading MATCH binds -- the nodes the role reads that its pattern
        matches, each giving the keys it gives the junction row (any other, its ``_id``). Read
        apart, as the MATCH of unrelated patterns is every combination of their nodes
        (:meth:`pairs`)."""
        from provisa.gql.select_builder import NODE_KEY_PROPERTY

        reads = []
        for i, n in enumerate(ast.matched):
            names = {k: f"{_KEY_PARAM}{i}_{k}" for k in n.props}
            pattern = (
                f"({n.variable}:{n.label} {{"
                + ", ".join(f"{k}: ${p}" for k, p in names.items())
                + "})"
                if names
                else f"({n.variable}:{n.label})"
            )
            keys = self._end_keys(ast, n.variable) or [NODE_KEY_PROPERTY]
            items = ", ".join(f"{n.variable}.{k}" for k in keys)
            params = dict(
                zip(names.values(), set_values(list(n.props.values()), provided), strict=True)
            )
            reads.append((f"MATCH {pattern} RETURN {items}", {**provided, **params}))
        return reads

    def pairs(self, ast: WriteAST, answers: list[list[list[Any]]]) -> list[list[Any]]:
        """Each junction row's keys, start's then end's: one per combination of the matched
        nodes (``answers``, the rows of :meth:`matched_reads`, in its order)."""
        import itertools

        rel, _start, _end = self._junction_of(ast)
        at = {n.variable: i for i, n in enumerate(ast.matched)}
        # The start's row opens with its source key; the end's holds its target key after it, the
        # start's, when a node is related to itself.
        width = len(key_list(rel.join_source_column))
        skip = width if ast.variable == ast.end else 0
        return [
            [*combo[at[ast.variable]][:width], *combo[at[ast.end]][skip:]]
            for combo in itertools.product(*answers)
        ]

    def relate(self, ast: WriteAST, keys: list[list[Any]]) -> str | None:
        """The junction rows a relationship CREATE writes, one per pair of matched nodes
        (``keys``, as :meth:`pairs` gives them); None when the MATCH bound none."""
        rel, _start, _end = self._junction_of(ast)
        if not keys:
            return None
        return self._junction_insert(rel, ast.label, ast.props, keys)

    def graph_links(self, ast: WriteAST) -> list[RelationshipMapping]:
        """Each relationship a ``graph`` write creates, its registered junction: every end a row
        of the junction's end table and every property one of its attributes, refused by name
        before any row is written (REQ-665, amended 2026-10-28)."""
        links = []
        for link in ast.links:
            start, end = ast.created[link.start], ast.created[link.end]
            rel, _s, _e = self._junction(link.rel_type, start.label, end.label)
            via = rel.via
            assert via is not None  # _junction refuses a relationship no junction backs
            for prop in link.props:
                if prop not in via.attributes:
                    raise CypherWriteParseError(f"{link.rel_type} has no property {prop}")
            links.append(rel)
        return links

    def graph_rows(self, ast: WriteAST, links: list[RelationshipMapping]) -> list[str]:
        """One INSERT per row a ``graph`` write creates, each returning the row's key as its
        identity (``_id``, a generated key included) and each column a relationship ending on it
        takes into its junction row (``provisa_write_<column>``)."""
        from provisa.gql.select_builder import NODE_KEY_PROPERTY, node_key

        taken: list[list[str]] = [[] for _ in ast.created]
        for link, rel in zip(ast.links, links, strict=True):
            for at, columns in (
                (link.start, rel.join_source_column),
                (link.end, rel.join_target_column),
            ):
                taken[at] += [c for c in key_list(columns) if c not in taken[at]]
        out = []
        for node, columns in zip(ast.created, taken, strict=True):
            mapping = self._resolve_mapping(node.label)
            insert = self._translate_create(
                WriteAST(kind="create", label=node.label, variable=node.variable, props=node.props)
            )
            key = node_key(None, mapping).sql(dialect="postgres")
            returning = [f"{key} AS {_q(NODE_KEY_PROPERTY)}"] + [
                f"{_q(c)} AS {_q(_KEY_PARAM + c)}" for c in columns
            ]
            out.append(f"{insert} RETURNING {', '.join(returning)}")
        return out

    def relate_rows(
        self, ast: WriteAST, links: list[RelationshipMapping], rows: list[dict[str, Any]]
    ) -> list[str]:
        """The junction INSERT of each relationship a ``graph`` write creates, from the rows its
        INSERTs returned (``rows``, each column -> value, in ``ast.created``'s order): the start's
        source key, then the end's target key."""
        out = []
        for link, rel in zip(ast.links, links, strict=True):
            keys = [rows[link.start][_KEY_PARAM + c] for c in key_list(rel.join_source_column)]
            keys += [rows[link.end][_KEY_PARAM + c] for c in key_list(rel.join_target_column)]
            out.append(self._junction_insert(rel, link.rel_type, link.props, [keys]))
        return out

    def _junction_insert(
        self,
        rel: RelationshipMapping,
        rel_type: str,
        props: dict[str, Any],
        keys: list[list[Any]],
    ) -> str:
        """The INSERT of junction rows of ``rel``, one per key list (start's then end's), each
        with ``props`` as its attributes."""
        via = rel.via
        assert via is not None  # _junction refuses a relationship no junction backs
        cols = [*via.source_columns, *via.target_columns]
        constant: list[str] = []
        if via.type_column:
            cols.append(via.type_column)
            constant.append(
                _write_value(via.type_value if via.type_value is not None else rel.rel_type)
            )
        for prop, val in props.items():
            if prop not in via.attributes:
                raise CypherWriteParseError(f"{rel_type} has no property {prop}")
            cols.append(via.attributes[prop])
            constant.append(_write_value(val))
        table = _junction_table(via)
        rows = ", ".join(
            "(" + ", ".join([*(_write_value(k) for k in row), *constant]) + ")" for row in keys
        )
        return f"INSERT INTO {table} ({', '.join(_q(c) for c in cols)}) VALUES {rows}"

    def erase_read(self, ast: WriteAST) -> str:
        """The read of what an ``erase`` deletes: its leading clauses, returning each variable."""
        return f"{ast.reads} RETURN {', '.join(ast.deleted)}"

    def detached(self, mapping: NodeMapping) -> list[RelationshipMapping]:
        """The relationships a DETACH DELETE of a ``mapping`` row detaches: every one the model
        declares with it at either end, each stored in a registered junction. One a foreign key
        derives has no exact target to detach, so the DETACH is refused by name (REQ-2176)."""
        names = {mapping.type_name, mapping.label}
        held: dict[int, RelationshipMapping] = {}
        for rel in [
            *self._label_map.relationships.values(),
            *(r for rels in self._label_map.aliases.values() for r in rels),
        ]:
            if rel.source_label not in names and rel.target_label not in names:
                continue
            if rel.via is None:
                raise CypherWriteParseError(
                    f"{mapping.label} is related by {rel.rel_type}, a relationship derived from "
                    "a foreign key: DETACH DELETE detaches only a relationship a registered "
                    "junction stores, so it is refused. Write the related rows' key column, then "
                    "DELETE the node"
                )
            held[id(rel)] = rel
        return list(held.values())

    def erase(
        self, ast: WriteAST, columns: list[str], rows: list[list[Any]]
    ) -> list[tuple[str, str]]:
        """The DELETEs an ``erase`` makes of what its read returned (``columns``/``rows``, as
        :meth:`erase_read` asks), each with the counter a row it deletes is: each deleted
        relationship's junction row, each junction row a detached node is an end of, then each
        node's row. Every part has an exact target
        (REQ-2176): a relationship a foreign key derives is refused by name, deleted or detached,
        before any statement is made."""
        from provisa.gql.assembler import to_serializable

        by_label = {nm.label: nm for nm in self._label_map.nodes.values()}
        nodes: dict[str, tuple[NodeMapping, dict[str, Any]]] = {}
        # Each junction relationship, by id: the relationship and its rows' conditions.
        junction_rows: dict[int, tuple[RelationshipMapping, set[str]]] = {}

        def node_of(value: dict[str, Any]) -> tuple[NodeMapping, dict[str, Any]]:
            nm = by_label.get(value.get("label", ""))
            if nm is None:
                raise CypherWriteParseError(
                    f"a deleted node, {value.get('label')}, is no row of a model table"
                )
            return nm, dict(value["properties"])

        def keys(nm: NodeMapping, props: dict[str, Any], columns: Any) -> list[tuple[str, str]]:
            """Each of ``columns`` of the row ``props`` reads, and its value as a literal of the
            column's type (a union of labels reads every property as text)."""
            types = dict(nm.property_types)
            out = []
            for column in key_list(columns):
                prop = nm.property_of(column)
                if prop not in props:
                    raise CypherWriteParseError(f"a deleted {nm.label} is read with its {prop}")
                out.append((column, _write_value(props[prop], types.get(prop))))
            return out

        def junction_row(
            rel: RelationshipMapping, sides: list[tuple[tuple[str, ...], list]]
        ) -> None:
            conditions = [
                f"{_q(jc)} = {literal}"
                for junction_columns, held in sides
                for jc, (_column, literal) in zip(junction_columns, held, strict=True)
            ]
            junction_rows.setdefault(id(rel), (rel, set()))[1].add(
                "(" + " AND ".join(conditions) + ")"
            )

        for row in rows:
            for var in ast.deleted:
                value = to_serializable(row[columns.index(var)])
                if value is None:
                    continue  # an OPTIONAL MATCH that matched nothing deletes nothing
                if "startNode" in value:
                    start, start_props = node_of(value["startNode"])
                    end, end_props = node_of(value["endNode"])
                    rel, _s, _e = self._junction(value["type"], start.label, end.label)
                    via = rel.via
                    assert via is not None  # _junction refuses a relationship no junction backs
                    junction_row(
                        rel,
                        [
                            (via.source_columns, keys(start, start_props, rel.join_source_column)),
                            (via.target_columns, keys(end, end_props, rel.join_target_column)),
                        ],
                    )
                    continue
                nm, props = node_of(value)
                key = keys(nm, props, nm.key_columns or nm.id_column)
                nodes[f"{nm.type_name}|{key!r}"] = (nm, props)
        if ast.detach:
            for nm, props in nodes.values():
                names = {nm.type_name, nm.label}
                for rel in self.detached(nm):
                    via = rel.via
                    assert via is not None  # detached refuses a relationship no junction stores
                    if rel.source_label in names:
                        junction_row(
                            rel, [(via.source_columns, keys(nm, props, rel.join_source_column))]
                        )
                    if rel.target_label in names:
                        junction_row(
                            rel, [(via.target_columns, keys(nm, props, rel.join_target_column))]
                        )
        out: list[tuple[str, str]] = []
        for rel, conditions in junction_rows.values():
            via = rel.via
            assert via is not None  # only a junction relationship holds rows here
            where = " OR ".join(sorted(conditions))
            if via.type_column:
                typed = via.type_value if via.type_value is not None else rel.rel_type
                where = f"({where}) AND {_q(via.type_column)} = {_write_value(typed)}"
            out.append(
                ("relationships_deleted", f"DELETE FROM {_junction_table(via)} WHERE {where}")
            )
        tables: dict[str, tuple[NodeMapping, set[str]]] = {}
        for nm, props in nodes.values():
            conditions = [
                f"{_read_column(nm, nm.property_of(column))} = {literal}"
                for column, literal in keys(nm, props, nm.key_columns or nm.id_column)
            ]
            tables.setdefault(nm.type_name, (nm, set()))[1].add(
                "(" + " AND ".join(conditions) + ")"
            )
        for nm, conditions in tables.values():
            out.append(
                (
                    "nodes_deleted",
                    f"DELETE FROM {self._qualified_table(nm)} WHERE {' OR '.join(sorted(conditions))}",
                )
            )
        return out

    def _matched_sql(self, ast: WriteAST, mapping: NodeMapping) -> str:
        """The rows a DELETE or UPDATE writes: each inline property as an equality, as a read
        matches it, and the WHERE condition."""
        conditions = [
            f"{_read_column(mapping, prop)} = {_write_value(val)}"
            for prop, val in ast.match_props.items()
        ]
        if not ast.where_expr:
            return " AND ".join(conditions)
        where_sql = _rewrite_where(ast.where_expr, ast.variable, mapping)
        return " AND ".join([*conditions, f"({where_sql})"]) if conditions else where_sql

    def _translate_delete(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        return f"DELETE FROM {table} WHERE {self._matched_sql(ast, mapping)}"

    def _translate_update(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        set_parts: list[str] = []
        for prop, val in ast.set_assignments:
            set_parts.append(f"{_written_column(mapping, prop)} = {_write_value(val)}")
        if not set_parts:
            raise CypherWriteParseError("UPDATE statement has no SET assignments")
        set_sql = ", ".join(set_parts)
        return f"UPDATE {table} SET {set_sql} WHERE {self._matched_sql(ast, mapping)}"


def _junction_table(via: JunctionMapping) -> str:
    """A junction's table, qualified as a write names it."""
    return ".".join(_q(p) for p in (via.catalog_name, via.schema_name, via.table_name) if p)


#: The read parameters a write's own reads bind (matched nodes' properties, a created node's key).
_KEY_PARAM = "provisa_write_"


def returned(ast: WriteAST, columns: list[str], rows: list[list[Any]]) -> tuple[str, dict]:
    """The read answering a CREATE's RETURN: the node created, matched by the key its INSERT
    returned (``columns``/``rows``), and the RETURN as written -- read as every read is, so a
    node, ``id(n)`` and an expression over them answer as they would to a MATCH."""
    from provisa.gql.select_builder import NODE_KEY_PROPERTY

    if len(rows) != 1:
        raise CypherWriteParseError(f"a CREATE wrote {len(rows)} rows, where it writes one")
    key = rows[0][columns.index(NODE_KEY_PROPERTY)]
    param = f"{_KEY_PARAM}key"
    return (
        f"MATCH ({ast.variable}:{ast.label} {{{NODE_KEY_PROPERTY}: ${param}}}) RETURN {ast.returns}",
        {param: key},
    )


def graph_returned(ast: WriteAST, rows: list[dict[str, Any]]) -> tuple[str, dict]:
    """The read answering a ``graph`` write's RETURN: each row written, matched by the key its
    INSERT returned (``rows``, in ``ast.created``'s order), each named relationship between them,
    and the RETURN as written -- read as every read is, so the role's masks shape what it shows."""
    from provisa.gql.select_builder import NODE_KEY_PROPERTY

    names = [n.variable or f"{_KEY_PARAM}n{i}" for i, n in enumerate(ast.created)]
    params = {f"{_KEY_PARAM}key{i}": row[NODE_KEY_PROPERTY] for i, row in enumerate(rows)}
    shown: set[int] = set()

    def node(i: int) -> str:
        if i in shown:
            return f"({names[i]})"
        shown.add(i)
        return f"({names[i]}:{ast.created[i].label} {{{NODE_KEY_PROPERTY}: ${_KEY_PARAM}key{i}}})"

    patterns = [
        f"{node(link.start)}-[{link.variable}:{link.rel_type}]->{node(link.end)}"
        for link in ast.links
        if link.variable
    ]
    patterns += [node(i) for i, n in enumerate(ast.created) if n.variable and i not in shown]
    # A RETURN naming no variable (RETURN 1) is one row: the rows written, matched by their keys.
    patterns = patterns or [node(i) for i in range(len(ast.created))]
    return f"MATCH {', '.join(patterns)} RETURN {ast.returns}", params


async def _written_graph(
    ast: WriteAST, translator: "WriteTranslator", provided: dict[str, Any], read: Any, write: Any
) -> tuple[list[str], list[list[Any]]]:
    """Run a ``graph`` write (REQ-665, amended 2026-10-28): every relationship's junction and
    ends checked first, then each row written and its key returned, then each junction row
    between them, then the read answering the RETURN."""
    links = translator.graph_links(ast)
    rows: list[dict[str, Any]] = []
    for node, sql in zip(ast.created, translator.graph_rows(ast, links), strict=True):
        columns, answer = await written_rows(
            write, *bind_write_params(sql, provided), **created_node(translator, node, provided)
        )
        if len(answer) != 1:
            raise CypherWriteParseError(f"a CREATE wrote {len(answer)} rows, where it writes one")
        rows.append(dict(zip(columns, answer[0], strict=True)))
    for link, sql in zip(ast.links, translator.relate_rows(ast, links, rows), strict=True):
        await written_rows(
            write, *bind_write_params(sql, provided), **created_link(link.props, provided)
        )
    if not ast.returns:
        return [], []
    cypher, params = graph_returned(ast, rows)
    return await read(cypher, {**provided, **params})


async def written(
    ast: WriteAST,
    translator: "WriteTranslator",
    provided: dict[str, Any],
    read: Any,
    write: Any,
) -> tuple[list[str], list[list[Any]]]:
    """Run the write ``ast``: its read of the nodes a relationship joins, its write, and the read
    answering its RETURN. ``read(cypher, params)`` and ``write(sql, bound)`` are the surface's
    own, answering through the one pipeline: a read its (columns, rows), a write its (columns,
    rows, the source's row count). Each row written is counted on the statement's counters
    (:mod:`provisa.cypher.counters`)."""
    if ast.kind == "graph":
        return await _written_graph(ast, translator, provided, read, write)
    if ast.kind == "merge":
        from provisa.cypher.merge_writer import written_merge

        await written_merge(ast, translator, provided, read, write)
        return [], []
    if ast.kind == "erase":
        # REQ-2176: what the DELETE names is read as every read is, under the role's governance;
        # each DELETE is then admitted as any write (write grant, row rules).
        columns, rows = await read(translator.erase_read(ast), provided)
        for counter, sql in translator.erase(ast, columns, rows):
            await written_rows(write, *bind_write_params(sql, provided), **{counter: 1})
        return [], []
    if ast.kind == "relate":
        answers = [(await read(q, p))[1] for q, p in translator.matched_reads(ast, provided)]
        sql = translator.relate(ast, translator.pairs(ast, answers))
        if sql is None:
            return [], []
        per_row = created_link(ast.props, provided)
    elif ast.kind == "create":
        sql = translator.translate(ast)
        per_row = created_node(translator, ast, provided)
    elif ast.kind == "update":
        sql = translator.translate(ast)
        per_row = properties_set([v for _, v in ast.set_assignments], provided)
    else:
        sql = translator.translate(ast)
        per_row = {"nodes_deleted": 1}
    columns, rows = await written_rows(write, *bind_write_params(sql, provided), **per_row)
    if ast.returns:
        cypher, params = returned(ast, columns, rows)
        return await read(cypher, {**provided, **params})
    return columns, rows


def created_node(
    translator: "WriteTranslator", node: WriteAST | CreatedNode, provided: dict[str, Any]
) -> dict[str, int]:
    """What each model row a CREATE writes counts: a node, the labels it is read with, and each
    property it is given a value (a null is no property, as Neo4j sets none)."""
    nm = translator._resolve_mapping(node.label)
    values = set_values(list(node.props.values()), provided)
    return {
        "nodes_created": 1,
        "labels_added": carried_labels(nm.label),
        "properties_set": sum(v is not None for v in values),
    }


def created_link(props: dict[str, Any], provided: dict[str, Any]) -> dict[str, int]:
    """What each junction row a relationship CREATE writes counts: a relationship, and each
    property it is given a value."""
    values = set_values(list(props.values()), provided)
    return {"relationships_created": 1, "properties_set": sum(v is not None for v in values)}


def properties_set(values: list[Any], provided: dict[str, Any]) -> dict[str, int]:
    """What each row a SET writes counts: each property it sets, an unchanged value included, as
    Neo4j counts it. A SET to null counts only where the row held a value, which the UPDATE
    does not read: the statement's counters are then unknown."""
    if any(v is None for v in set_values(values, provided)):
        unknown("a SET to null counts a property only where the row held one, which is not read")
        return {}
    return {"properties_set": len(values)}


def bind_write_params(sql: str, provided: dict[str, Any]) -> tuple[str, list[Any]]:
    """``sql`` (a translated write) with each ``$name`` parameter made positional (``$1``, …),
    and the request's values for them, in that order.

    Read off the parsed statement, so a ``$`` inside a string literal is text and stays so.
    A parameter the request does not supply is refused by name: a write is never sent with an
    unbound value, and never with a parameter's name in a value's place."""
    import sqlglot  # noqa: PLC0415
    import sqlglot.expressions as exp  # noqa: PLC0415

    from provisa.cypher.params import CypherParamError  # noqa: PLC0415

    tree = sqlglot.parse_one(sql, read="postgres")
    named = [p for p in tree.find_all(exp.Parameter) if isinstance(p.this, exp.Var)]
    order: list[str] = []
    for p in named:
        if p.this.name not in order:
            order.append(p.this.name)
    missing = [n for n in order if n not in provided]
    if missing:
        raise CypherParamError(
            f"Unbound Cypher parameters: {missing!r}. Provide values in the request's parameters."
        )
    for p in named:
        p.replace(exp.Parameter(this=exp.Literal.number(order.index(p.this.name) + 1)))
    return tree.sql(dialect="postgres"), [provided[n] for n in order]
