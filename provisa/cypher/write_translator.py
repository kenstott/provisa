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

Supports three write patterns:

- ``CREATE (n:Label {props})``  →  ``INSERT INTO catalog.schema.table (cols) VALUES (vals)``
- ``MATCH (n:Label) WHERE ... DELETE n``  →  ``DELETE FROM catalog.schema.table WHERE ...``
- ``MATCH (n:Label) WHERE ... SET n.prop = val, ...``
  →  ``UPDATE catalog.schema.table SET col = val, ... WHERE ...``

Requirements: REQ-666, REQ-667, REQ-668, REQ-670
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from provisa.cypher.label_map import CypherLabelMap, NodeMapping


# ---------------------------------------------------------------------------
# Write AST
# ---------------------------------------------------------------------------


@dataclass
class WriteAST:
    """Minimal AST for a Cypher write statement."""

    kind: str  # "create" | "delete" | "update"
    label: str  # Cypher label, e.g. "Person"
    variable: str  # node variable, e.g. "n"
    props: dict[str, Any] = field(default_factory=dict)  # CREATE props
    where_expr: str = ""  # raw WHERE expression text (for DELETE/UPDATE)
    set_assignments: list[tuple[str, Any]] = field(default_factory=list)  # UPDATE assignments


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

# CREATE (n:Label {key: val, ...})
_CREATE_RE = re.compile(
    rf"CREATE{_WS}\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}(\{{[^}}]*\}}){_OWS}\){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)

# MATCH (n:Label) WHERE <expr> DELETE n
_DELETE_RE = re.compile(
    rf"MATCH{_WS}\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}\){_WS}WHERE{_WS}(.+?){_WS}DELETE{_WS}({_IDENT}){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)

# MATCH (n:Label) WHERE <expr> SET n.prop = val [, ...]
_UPDATE_RE = re.compile(
    rf"MATCH{_WS}\({_OWS}({_IDENT}){_OWS}:({_LABEL}){_OWS}\){_WS}WHERE{_WS}(.+?){_WS}SET{_WS}(.+){_OWS}$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_literal(text: str) -> Any:
    """Parse a simple Cypher literal value to a Python value."""
    text = text.strip()
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


def parse_cypher_write(query: str) -> WriteAST:
    """Parse a Cypher write statement into a WriteAST.

    Supports CREATE, MATCH-DELETE, and MATCH-SET patterns.
    Raises CypherWriteParseError for unrecognised syntax.
    """
    query = query.strip()

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
                "MERGE, DETACH DELETE, and REMOVE are unsupported."
            )

    m = _CREATE_RE.match(query)
    if m:
        variable = m.group(1)
        label = m.group(2)
        props = _parse_props(m.group(3))
        return WriteAST(kind="create", label=label, variable=variable, props=props)

    m = _DELETE_RE.match(query)
    if m:
        variable = m.group(1)
        label = m.group(2)
        where_expr = m.group(3).strip()
        del_var = m.group(4)
        if del_var.lower() != variable.lower():
            raise CypherWriteParseError(
                f"DELETE variable {del_var!r} does not match MATCH variable {variable!r}"
            )
        return WriteAST(kind="delete", label=label, variable=variable, where_expr=where_expr)

    m = _UPDATE_RE.match(query)
    if m:
        variable = m.group(1)
        label = m.group(2)
        where_expr = m.group(3).strip()
        set_text = m.group(4).strip()
        assignments = _parse_set_assignments(set_text, variable)
        return WriteAST(
            kind="update",
            label=label,
            variable=variable,
            where_expr=where_expr,
            set_assignments=assignments,
        )

    raise CypherWriteParseError(f"Unrecognised write Cypher pattern: {query!r}")


# ---------------------------------------------------------------------------
# SQL value rendering
# ---------------------------------------------------------------------------


def _sql_literal(val: Any, col_type: str | None = None) -> str:
    """Render a Python value as a SQL literal, applying type coercion."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return repr(val)
    # String — check if the target column type is numeric.
    if isinstance(val, str):
        if col_type and col_type.lower() in (
            "integer",
            "int",
            "bigint",
            "smallint",
            "numeric",
            "decimal",
            "double",
            "float",
            "real",
        ):
            try:
                n = int(val)
                return str(n)
            except ValueError:
                pass
            try:
                return repr(float(val))
            except ValueError:
                pass
        return f"'{val}'"
    return f"'{val}'"


def _q(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


# ---------------------------------------------------------------------------
# WHERE clause rewriting
# ---------------------------------------------------------------------------

# Rewrite ``n.prop`` → ``"col"`` using the node's property map.
_PROP_ACCESS_RE = re.compile(r"\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b")


def _rewrite_where(where_expr: str, variable: str, mapping: NodeMapping) -> str:
    """Rewrite Cypher property accesses to SQL column names in a WHERE expression.

    E.g. ``n.age > 21`` → ``"age" > 21``  (using mapping.properties).
    """

    def _replace(m: re.Match) -> str:
        var = m.group(1)
        prop = m.group(2)
        if var is None or prop is None or var.lower() != variable.lower():
            return m.group(0)
        col: str = mapping.properties.get(prop) or prop
        return _q(col)

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
        mapping = self._label_map.nodes.get(label)
        if mapping is None:
            raise CypherWriteParseError(f"Label {label!r} is not registered in the label map")
        return mapping

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
        raise CypherWriteParseError(f"Unknown WriteAST kind: {ast.kind!r}")

    def _translate_create(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        cols: list[str] = []
        vals: list[str] = []
        for prop, val in ast.props.items():
            col = mapping.properties.get(prop, prop)
            cols.append(_q(col))
            vals.append(_sql_literal(val))
        if not cols:
            raise CypherWriteParseError("CREATE statement has no properties")
        cols_sql = ", ".join(cols)
        vals_sql = ", ".join(vals)
        return f"INSERT INTO {table} ({cols_sql}) VALUES ({vals_sql})"

    def _translate_delete(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        where_sql = _rewrite_where(ast.where_expr, ast.variable, mapping)
        return f"DELETE FROM {table} WHERE {where_sql}"

    def _translate_update(self, ast: WriteAST) -> str:
        mapping = self._resolve_mapping(ast.label)
        table = self._qualified_table(mapping)
        set_parts: list[str] = []
        for prop, val in ast.set_assignments:
            col = mapping.properties.get(prop, prop)
            set_parts.append(f"{_q(col)} = {_sql_literal(val)}")
        if not set_parts:
            raise CypherWriteParseError("UPDATE statement has no SET assignments")
        set_sql = ", ".join(set_parts)
        where_sql = _rewrite_where(ast.where_expr, ast.variable, mapping)
        return f"UPDATE {table} SET {set_sql} WHERE {where_sql}"
