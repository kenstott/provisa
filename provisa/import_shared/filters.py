# Copyright (c) 2026 Kenneth Stott
# Canary: 10338ccf-ccb2-4aed-8382-2a035eac3565
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Convert Hasura boolean expressions to SQL WHERE clauses."""

from __future__ import annotations

import datetime
import math
from collections.abc import Callable
from typing import Any, Mapping, NamedTuple

from sqlglot import exp

from provisa.import_shared.refused import FilterOperatorRefused, FilterValueRefused

# Requirements: REQ-041, REQ-040

#: Every literal and identifier a filter carries is rendered by sqlglot in this dialect, the one
#: an RLS rule's text is parsed in (provisa/compiler/rls_validate.py).
_DIALECT = "postgres"

# Hasura operator -> SQL operator
_OPERATORS: dict[str, str] = {
    "_eq": "=",
    "_neq": "!=",
    "_gt": ">",
    "_lt": "<",
    "_gte": ">=",
    "_lte": "<=",
    "_like": "LIKE",
    "_nlike": "NOT LIKE",
    "_ilike": "ILIKE",
    "_nilike": "NOT ILIKE",
    "_in": "IN",
    "_nin": "NOT IN",
    "_is_null": "IS NULL",
    "_regex": "~",
    "_nregex": "!~",
    "_iregex": "~*",
    "_niregex": "!~*",
}


def hasura_session_name(header: str) -> str:
    """X-Hasura-Foo-Bar as the session variable it is read from: ``hasura_foo_bar``, supplied by
    the token's ``x-hasura-foo-bar`` claim as Hasura read it (auth/attributes.normalize_name).
    Never ``user_id`` or ``role``: those are the verified identity and the acting role."""
    name = header.lower()
    for prefix in ("x-hasura-", "x_hasura_"):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break
    return "hasura_" + name.replace("-", "_")


def _is_session_header(val: Any) -> bool:
    return isinstance(val, str) and (val.startswith("x-hasura-") or val.startswith("X-Hasura-"))


def session_setting(header: str) -> str:
    """The SQL reading session variable ``header``: ``current_setting('provisa.hasura_...')``,
    its name a string literal sqlglot renders, whatever the header holds."""
    name = exp.Literal.string(f"provisa.{hasura_session_name(header)}").sql(dialect=_DIALECT)
    return f"current_setting({name})"


def sql_literal(val: Any) -> str:
    """A filter value as the typed SQL literal sqlglot renders: a string, a finite number, true,
    false or null. A date or time (YAML reads an unquoted one as such) is the string it was
    written as, as the JSON export carries it. Anything else is refused by name."""
    node: exp.Expression
    if isinstance(val, bool):
        node = exp.Boolean(this=val)
    elif isinstance(val, str):
        node = exp.Literal.string(val)
    elif isinstance(val, (datetime.date, datetime.time)):
        node = exp.Literal.string(val.isoformat())
    elif isinstance(val, int) or (isinstance(val, float) and math.isfinite(val)):
        node = exp.Literal.number(val)
    elif val is None:
        node = exp.Null()
    else:
        raise FilterValueRefused(val)
    return node.sql(dialect=_DIALECT)


def sql_identifier(name: str) -> str:
    """A column, table or schema name as sqlglot renders it: as written when it is a plain
    identifier, quoted otherwise."""
    if not isinstance(name, str) or not name:
        raise FilterValueRefused(name)
    return exp.to_identifier(name).sql(dialect=_DIALECT)


def sql_dotted(name: str) -> str:
    """A dotted table id (``schema.table``) with each part rendered by :func:`sql_identifier`."""
    return ".".join(sql_identifier(part) for part in name.split("."))


def _format_value(val: Any) -> str:
    """A filter operand as SQL: a session variable read, a list of literals, or one literal."""
    if _is_session_header(val):
        return session_setting(val)
    if isinstance(val, list):
        items = ", ".join(
            session_setting(v) if _is_session_header(v) else sql_literal(v) for v in val
        )
        return f"({items})"
    return sql_literal(val)


def _reads_session(val: Any) -> bool:
    """Whether an operand reads a session variable, alone or in a list."""
    if isinstance(val, list):
        return any(_is_session_header(v) for v in val)
    return _is_session_header(val)


def _convert_session_var(val: Any) -> str:
    """Convert Hasura session variable references to current_setting()."""
    if isinstance(val, dict) and len(val) == 1:
        key = next(iter(val))
        if _is_session_header(key):
            return session_setting(key)
    return ""


class Hop(NamedTuple):
    """A relationship a filter follows: the table it reaches, as the model names it, and the
    column pairs (this table's column, that table's column) it joins on."""

    target: str
    pairs: list[tuple[str, str]]


#: Resolves (table, relationship name) to the hop it makes, or None when the name is a column.
Relations = Callable[[str, str], "Hop | None"]


def bool_expr_to_sql(
    expr: Mapping[str, Any],
    table_alias: str = "",
    *,
    table: str = "",
    relations: Relations | None = None,
) -> str:  # REQ-041, REQ-040
    """Convert a Hasura boolean expression to a SQL WHERE clause.

    A key naming one of ``table``'s relationships (``relations``) filters through it: the rows
    whose related row matches, as an EXISTS over the related table aliased ``_r1``, ``_r2`` ...
    by depth. Inside one, a session variable is compared with a column as text: the related
    column's type is not this table's to cast it to (REQ-1686 casts this table's own).

    Returns the SQL WHERE clause (without the WHERE keyword).
    """
    if not expr:
        return "TRUE"
    return _convert_node(expr, table_alias, table=table, relations=relations, depth=0)


def _is_operator_map(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(k.startswith("_") for k in value)


def _col_ref(col: str, table_alias: str) -> str:
    if table_alias:
        return f"{sql_identifier(table_alias)}.{sql_identifier(col)}"
    return sql_identifier(col)


def _convert_node(
    node: Mapping[str, Any],
    table_alias: str,
    *,
    table: str = "",
    relations: Relations | None = None,
    depth: int = 0,
) -> str:
    parts: list[str] = []
    inner = {"table": table, "relations": relations, "depth": depth}

    for key, value in node.items():
        if key == "_and":
            if not isinstance(value, list):
                continue
            sub = [
                _convert_node(item, table_alias, **inner)
                for item in value
                if isinstance(item, dict)
            ]
            parts.append("(" + " AND ".join(sub) + ")")
        elif key == "_or":
            if not isinstance(value, list):
                continue
            sub = [
                _convert_node(item, table_alias, **inner)
                for item in value
                if isinstance(item, dict)
            ]
            parts.append("(" + " OR ".join(sub) + ")")
        elif key == "_not":
            if not isinstance(value, dict):
                continue
            negated = _convert_node(value, table_alias, **inner)
            parts.append(f"NOT ({negated})")
        elif key == "_exists":
            if not isinstance(value, dict):
                continue
            tbl = value.get("_table", {})
            if not isinstance(tbl, dict) or "name" not in tbl:
                raise ValueError(f"_exists filter missing table name: {value!r}")
            schema = tbl.get("schema", "public")
            table = tbl["name"]
            where_node = value.get("_where", {})
            where = _convert_node(where_node, "") if isinstance(where_node, dict) else "TRUE"
            from_ = f"{sql_identifier(schema)}.{sql_identifier(table)}"
            parts.append(f"EXISTS (SELECT 1 FROM {from_} WHERE {where})")
        elif key.startswith("_"):
            # Unknown operator at top level — skip
            continue
        elif (
            relations is not None
            and isinstance(value, dict)
            and not _is_operator_map(value)
            and (hop := relations(table, key)) is not None
        ):
            alias = f"_r{depth + 1}"
            joined = " AND ".join(
                f"{_col_ref(theirs, alias)} = {_col_ref(ours, table_alias)}"
                for ours, theirs in hop.pairs
            )
            where = _convert_node(
                value, alias, table=hop.target, relations=relations, depth=depth + 1
            )
            condition = joined if where == "TRUE" else f"{joined} AND {where}"
            target = sql_dotted(hop.target)
            parts.append(f"EXISTS (SELECT 1 FROM {target} AS {alias} WHERE {condition})")
        else:
            # Column name -> operator dict
            col = _col_ref(key, table_alias)
            # Inside a relationship's EXISTS, a session variable is compared as text.
            related = depth > 0
            if isinstance(value, dict):
                for op, operand in value.items():
                    session_ref = _convert_session_var(operand)
                    if op == "_is_null":
                        if operand:
                            parts.append(f"{col} IS NULL")
                        else:
                            parts.append(f"{col} IS NOT NULL")
                    elif op in _OPERATORS:
                        sql_op = _OPERATORS[op]
                        if session_ref:
                            left = f"CAST({col} AS TEXT)" if related else col
                            parts.append(f"{left} {sql_op} {session_ref}")
                        else:
                            value_sql = _format_value(operand)
                            left = (
                                f"CAST({col} AS TEXT)"
                                if related and _reads_session(operand)
                                else col
                            )
                            parts.append(f"{left} {sql_op} {value_sql}")
                    else:
                        raise FilterOperatorRefused(op)

    if not parts:
        return "TRUE"
    if len(parts) == 1:
        return parts[0]
    return " AND ".join(parts)
