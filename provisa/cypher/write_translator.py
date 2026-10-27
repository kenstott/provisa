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

Supports four write patterns:

- ``CREATE (n:Label {props}) [SET n.prop = val, ...] [RETURN ...]``
  →  ``INSERT INTO catalog.schema.table (cols) VALUES (vals)``; with a RETURN, ``RETURNING`` the
  node's key, and the RETURN answered by a read of the node created (:func:`returned`)
- ``MATCH (n:Label) WHERE ... DELETE n``  →  ``DELETE FROM catalog.schema.table WHERE ...``
- ``MATCH (n:Label) WHERE ... SET n.prop = val, ...``
  →  ``UPDATE catalog.schema.table SET col = val, ... WHERE ...``
- ``MATCH (a:A {...}) MATCH (b:B {...}) CREATE (a)-[:T {props}]->(b)``, T a relationship a
  registered junction backs (REQ-665, amended 2026-10-27)  →  a read of the matched nodes' keys
  (:func:`matched_keys`), then ``INSERT INTO junction (keys, attrs) VALUES ...`` (:meth:`relate`)

:func:`written` runs a write in that order over the surface's own read and write.

Requirements: REQ-665, REQ-666, REQ-667, REQ-668, REQ-670
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from provisa.compiler.sql_literals import sql_literal
from provisa.compiler.sql_types import key_list
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping


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

    kind: str  # "create" | "delete" | "update" | "relate"
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

# CREATE (n:Label [{key: val, ...}]) [SET n.key = val, ...] [RETURN ...]
_CREATE_RE = re.compile(
    rf"CREATE{_WS}\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}(\{{[^}}]*\}})?{_OWS}\)"
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
    "MATCH (a:Label {key: value}) MATCH (b:Label {key: value}) CREATE (a)-[:TYPE {key: value}]->(b)"
)


def _parse_relate(m: re.Match) -> WriteAST:
    """A relationship CREATE between nodes its leading MATCH binds (REQ-665, amended
    2026-10-27): the leading clauses are MATCHes of labelled nodes with property maps and
    nothing else, and both ends are nodes they bind -- a relationship write never creates its
    ends."""
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
                f"a relationship CREATE joins two nodes its MATCH binds once each; {variable!r} "
                "is not one: a relationship write never creates its end nodes"
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

    # REQ-818: only CREATE/DELETE/SET are supported as direct table writes.
    # MERGE, DETACH DELETE, and REMOVE are non-direct write patterns and must be
    # rejected at parse time with a precise error. This guard runs before the
    # pattern regexes because a loose DELETE match would otherwise absorb a
    # leading DETACH into the WHERE expression and silently accept it.
    for _kw in ("MERGE", "DETACH", "REMOVE"):
        if re.search(rf"\b{_kw}\b", query, re.IGNORECASE):
            raise CypherWriteParseError(
                f"Cypher write pattern {_kw.upper()!r} is not supported. Provisa "
                "Cypher executes CREATE, DELETE, and SET as direct table writes; "
                "MERGE, DETACH DELETE, and REMOVE are unsupported. For upsert or "
                "cascade semantics, call the corresponding GraphQL mutation — it is "
                "also exposed as a UDF invocable via SQL function-call syntax."
            )

    # REQ-665 (amended 2026-10-27): a relationship a registered junction backs is a row of that
    # junction, written by a CREATE between two nodes the statement matches.
    m = _RELATE_RE.match(query)
    if m:
        return _parse_relate(m)

    # REQ-665: any other relationship pattern (`-[...]->`, `<-[...]-`, or a bare `--` edge) in a
    # write statement is a hard error at translation time.
    if re.search(r"-\s*\[|\]\s*-|\)\s*<?-{1,2}>?\s*\(", query):
        raise CypherWriteParseError(
            "Cypher write patterns cannot target relationships. Relationships are "
            "derived from foreign-key joins in the semantic layer, not stored edges; "
            "CREATE, SET, and DELETE may only write node (table) rows. To mutate the "
            "underlying rows, call the GraphQL mutation — it is also exposed as a UDF "
            "invocable via SQL function-call syntax."
        )

    m = _CREATE_RE.match(query)
    if m:
        variable, label, props_text, set_text, returns = m.groups()
        props = _parse_props(props_text) if props_text else {}
        # A SET on the node the statement creates gives it that property, as its map would.
        props.update(_parse_set_assignments(set_text, variable) if set_text else [])
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
                "matched_keys, then relate"
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
        nodes: refused by name unless the type is registered between those two labels and a
        junction backs it (REQ-665, amended 2026-10-27)."""
        labels = {n.variable: n.label for n in ast.matched}
        start = self._resolve_mapping(labels[ast.variable])
        end = self._resolve_mapping(labels[ast.end])
        declared = [
            *self._label_map.aliases.get(ast.label, []),
            *(
                [self._label_map.relationships[ast.label]]
                if ast.label in self._label_map.relationships
                else []
            ),
        ]
        if not declared:
            raise CypherWriteParseError(
                f"relationship type {ast.label} is not registered: a write adds a relationship "
                "the model declares, never a new type"
            )
        names = ({start.type_name, start.label}, {end.type_name, end.label})
        between = [r for r in declared if r.source_label in names[0] and r.target_label in names[1]]
        if not between:
            raise CypherWriteParseError(
                f"relationship type {ast.label} is not registered from {start.label} to {end.label}"
            )
        rel = between[0]
        if rel.via is None:
            raise CypherWriteParseError(
                f"relationship type {ast.label} is derived from a foreign key, not stored in a "
                "junction table, so Cypher does not write it: write the row's key column instead"
            )
        return rel, start, end

    def matched_keys(self, ast: WriteAST) -> str:
        """The read giving, for each pair of nodes a relationship CREATE joins, the two ends' key
        columns, start's then end's, in the junction's key order: the nodes the role reads that
        its leading MATCH binds."""
        rel, start, end = self._junction_of(ast)
        patterns = ", ".join(
            f"({n.variable}:{n.label}"
            + (
                " {" + ", ".join(f"{k}: ${_KEY_PARAM}{i}_{k}" for k in n.props) + "}"
                if n.props
                else ""
            )
            + ")"
            for i, n in enumerate(ast.matched)
        )
        items = [
            f"{var}.{nm.property_of(col)}"
            for var, nm, cols in (
                (ast.variable, start, key_list(rel.join_source_column)),
                (ast.end, end, key_list(rel.join_target_column)),
            )
            for col in cols
        ]
        return f"MATCH {patterns} RETURN {', '.join(items)}"

    def matched_params(self, ast: WriteAST, provided: dict[str, Any]) -> dict[str, Any]:
        """The parameters of :meth:`matched_keys`: each matched node's property values."""
        return {
            f"{_KEY_PARAM}{i}_{k}": (provided[v.name] if isinstance(v, CypherParam) else v)
            for i, n in enumerate(ast.matched)
            for k, v in n.props.items()
        }

    def relate(self, ast: WriteAST, keys: list[list[Any]]) -> str | None:
        """The junction rows a relationship CREATE writes, one per pair of matched nodes
        (``keys``, as :meth:`matched_keys` reads them); None when the MATCH bound none."""
        rel, _start, _end = self._junction_of(ast)
        via = rel.via
        assert via is not None  # _junction_of refuses a relationship no junction backs
        if not keys:
            return None
        cols = [*via.source_columns, *via.target_columns]
        constant: list[str] = []
        if via.type_column:
            cols.append(via.type_column)
            constant.append(
                _write_value(via.type_value if via.type_value is not None else rel.rel_type)
            )
        for prop, val in ast.props.items():
            if prop not in via.attributes:
                raise CypherWriteParseError(f"{ast.label} has no property {prop}")
            cols.append(via.attributes[prop])
            constant.append(_write_value(val))
        table = ".".join(_q(p) for p in (via.catalog_name, via.schema_name, via.table_name) if p)
        rows = ", ".join(
            "(" + ", ".join([*(_write_value(k) for k in row), *constant]) + ")" for row in keys
        )
        return f"INSERT INTO {table} ({', '.join(_q(c) for c in cols)}) VALUES {rows}"

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


async def written(
    ast: WriteAST,
    translator: "WriteTranslator",
    provided: dict[str, Any],
    read: Any,
    write: Any,
) -> tuple[list[str], list[list[Any]]]:
    """Run the write ``ast``: its read of the nodes a relationship joins, its write, and the read
    answering its RETURN. ``read(cypher, params)`` and ``write(sql, bound)`` are the surface's
    own, each answering (columns, rows) through the one pipeline."""
    if ast.kind == "relate":
        _cols, keys = await read(
            translator.matched_keys(ast), {**provided, **translator.matched_params(ast, provided)}
        )
        sql = translator.relate(ast, keys)
        if sql is None:
            return [], []
    else:
        sql = translator.translate(ast)
    columns, rows = await write(*bind_write_params(sql, provided))
    if ast.returns:
        cypher, params = returned(ast, columns, rows)
        return await read(cypher, {**provided, **params})
    return columns, rows


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
