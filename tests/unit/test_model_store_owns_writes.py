# Copyright (c) 2026 Kenneth Stott
# Canary: a3e85517-b26e-4ed0-84fe-639171e474af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Only the model store writes a converted model table (REQ-1919).

The model store is ``provisa/core/repositories/``. A kind is converted one operation at a time:
``CONVERTED`` names, per model table, the operations that have exactly one implementation there.
For those, no other module under ``provisa/`` may issue the statement — a delete written
elsewhere would skip the dependency guard. The table grows with each kind converted, and a
kind's entry gains ``insert`` and ``update`` when its create and update move in.

How a write is found, in every ``.py`` under ``provisa/`` outside the model store:
* a call ``delete(T)`` / ``insert(T)`` / ``update(T)`` under any of the names SQLAlchemy's
  constructors are imported as, where ``T`` is the table object (by the name it is imported as,
  or as ``schema_org.roles``);
* a method call ``T.delete()`` / ``T.insert()`` / ``T.update()``;
* ``conn.upsert(T, ...)`` / ``conn.upsert_returning(T, ...)`` (an insert and an update);
* ``conn.insert_returning(T, ...)`` / ``conn.bulk_copy(T, ...)`` (an insert);
* a string holding ``DELETE FROM T`` / ``INSERT INTO T`` / ``UPDATE T``, with or without a
  schema prefix — a docstring excepted.
"""

# Requirements: REQ-1919, REQ-2248

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2] / "provisa"
MODEL_STORE = ROOT / "core" / "repositories"

_ALL = {"delete", "insert", "update"}

# model table -> the operations only the model store may perform on it.
CONVERTED: dict[str, set[str]] = {
    "roles": _ALL,
    "domains": {"delete"},
    "registered_tables": {"delete"},
    "sources": {"delete"},
    "relationships": {"delete"},
    "metrics": _ALL,
    "tracked_functions": _ALL,
    "tracked_webhooks": _ALL,
    "data_products": _ALL,
    "tags": _ALL,
    "calendars": _ALL,
    "glossary_terms": _ALL,
}

_CONSTRUCTORS = {
    "delete": "delete",
    "_delete": "delete",
    "sa_delete": "delete",
    "_sa_delete": "delete",
    "sql_delete": "delete",
    "insert": "insert",
    "_insert": "insert",
    "sa_insert": "insert",
    "pg_insert": "insert",
    "sqlite_insert": "insert",
    "update": "update",
    "_update": "update",
    "sa_update": "update",
}
# The Connection methods (provisa/core/database.py) that write the table passed first.
_CONNECTION_WRITES = {
    "upsert": ("insert", "update"),
    "upsert_returning": ("insert", "update"),
    "insert_returning": ("insert",),
    "bulk_copy": ("insert",),
}
_SQL = {
    "delete": r"DELETE\s+FROM\s+(?:[\w\"{}]+\.)?\"?{t}\"?\b",
    "insert": r"INSERT\s+INTO\s+(?:[\w\"{}]+\.)?\"?{t}\"?\b",
    "update": r"UPDATE\s+(?:[\w\"{}]+\.)?\"?{t}\"?\s+(?:\w+\s+)?SET\b",
}


def _names_bound_to(tree: ast.AST, table: str) -> set[str]:
    """The names this module refers to the table by: its own name, and any alias it is
    imported under."""
    names = {table}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names |= {a.asname for a in node.names if a.name == table and a.asname}
    return names


_SCHEMA_MODULES = {"schema_org", "schema_admin"}


def _is_table(node: ast.AST, names: set[str], table: str) -> bool:
    """``roles`` (or the alias it was imported under), or ``schema_org.roles``. An attribute of
    anything else that merely shares the name (``identity.roles``) is not the table."""
    if isinstance(node, ast.Name):
        return node.id in names
    return (
        isinstance(node, ast.Attribute)
        and node.attr == table
        and isinstance(node.value, ast.Name)
        and node.value.id in _SCHEMA_MODULES
    )


def _text(node: ast.AST) -> str | None:
    """The text of a string literal; for an f-string, its literal pieces with each
    interpolation written as ``{}``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else "{}"
            for v in node.values
        )
    return None


def writes_in(source: str, table: str) -> list[tuple[int, str]]:
    """Every write to ``table`` in ``source``: (line, operation)."""
    tree = ast.parse(source)
    names = _names_bound_to(tree, table)
    found: list[tuple[int, str]] = []
    # A docstring describes; it issues nothing. (core/database.py's shows a DELETE as usage.)
    docstrings = {
        id(holder.body[0].value)
        for holder in ast.walk(tree)
        if isinstance(holder, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and holder.body
        and isinstance(holder.body[0], ast.Expr)
        and isinstance(holder.body[0].value, ast.Constant)
    }
    for node in ast.walk(tree):
        if id(node) in docstrings:
            continue
        if isinstance(node, ast.Call):
            func = node.func
            called = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            first = node.args[0] if node.args else None
            if called in _CONSTRUCTORS and first is not None and _is_table(first, names, table):
                found.append((node.lineno, _CONSTRUCTORS[called]))
            elif (
                called in _CONNECTION_WRITES
                and first is not None
                and _is_table(first, names, table)
            ):
                found += [(node.lineno, operation) for operation in _CONNECTION_WRITES[called]]
            elif (
                isinstance(func, ast.Attribute)
                and func.attr in ("delete", "insert", "update")
                and _is_table(func.value, names, table)
            ):
                found.append((node.lineno, func.attr))
        elif (text := _text(node)) is not None:
            for operation, pattern in _SQL.items():
                if re.search(pattern.replace("{t}", re.escape(table)), text, re.IGNORECASE):
                    found.append((node.lineno, operation))
    return sorted(set(found))


# --- the scanner finds what it is meant to find --------------------------------------------------


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("await conn.execute_core(_delete(roles).where(roles.c.id == x))", [(1, "delete")]),
        ("stmt = delete(schema_org.roles)", [(1, "delete")]),
        ("from provisa.core.schema_org import roles as r\nd = sa_delete(r)", [(2, "delete")]),
        ("x = roles.delete().where(roles.c.id == 1)", [(1, "delete")]),
        ("await conn.execute('DELETE FROM roles WHERE id = $1', x)", [(1, "delete")]),
        ('q = f"delete from {schema}.roles where id = 1"', [(1, "delete")]),
        ("await conn.execute_core(insert(roles).values(id=1))", [(1, "insert")]),
        ("await conn.upsert(roles, {}, index_elements=['id'])", [(1, "insert"), (1, "update")]),
        ("await conn.upsert_returning(roles, {}, ['id'], 'id')", [(1, "insert"), (1, "update")]),
        ("i = await conn.insert_returning(roles, {'name': n}, 'id')", [(1, "insert")]),
        ("await conn.bulk_copy(roles, rows)", [(1, "insert")]),
        ("q = 'UPDATE roles SET capabilities = 1'", [(1, "update")]),
        ("q = 'UPDATE roles t SET capabilities = 1'", [(1, "update")]),
        # not writes to the table
        ("rows = await conn.execute_core(select(roles))", []),
        ("x = debug_trace_hint_roles.delete()", []),
        ("q = 'DELETE FROM user_roles WHERE x'", []),
        ("identity.roles.update(extra)", []),  # an attribute that shares the table's name
        ('def f():\n    """Example: DELETE FROM roles WHERE id = 1"""\n', []),  # a docstring
    ],
)
def test_the_scan_finds_a_write(source, expected):
    assert writes_in(source, "roles") == expected


# --- the rule ------------------------------------------------------------------------------------


def _modules_outside_the_model_store() -> list[Path]:
    return sorted(p for p in ROOT.rglob("*.py") if MODEL_STORE not in p.parents)


#: REQ-2248: kinds whose delete retires the row (an update) and that no path removes: no delete
#: of them is anywhere, the model store included.
NEVER_REMOVED = {"tags", "metrics"}  # metrics: metric.remove_where became retire_where


def _inside(table: str, operation: str) -> list[tuple[str, int]]:
    return [
        (p.name, line)
        for p in sorted(MODEL_STORE.glob("*.py"))
        for line, found in writes_in(p.read_text(), table)
        if found == operation
    ]


@pytest.mark.parametrize(
    ("table", "operation"),
    sorted(
        (table, operation)
        for table, operations in CONVERTED.items()
        for operation in operations
        if not (table in NEVER_REMOVED and operation == "delete")
    ),
)
def test_the_model_store_is_where_the_converted_writes_live(table, operation):
    assert _inside(table, operation), f"no {operation} of {table} found in the model store"


def test_a_kind_that_is_retired_and_never_removed_is_deleted_nowhere():
    for table in NEVER_REMOVED:
        assert _inside(table, "delete") == [], f"{table} is retired, never removed (REQ-2248)"


@pytest.mark.parametrize("table", sorted(CONVERTED))
def test_no_module_outside_the_model_store_performs_a_converted_write(table):
    offenders = [
        f"{path.relative_to(ROOT.parent)}:{line} {operation} of {table}"
        for path in _modules_outside_the_model_store()
        for line, operation in writes_in(path.read_text(), table)
        if operation in CONVERTED[table]
    ]
    assert offenders == [], (
        f"{table} is written outside the model store (provisa/core/repositories/); call the "
        "model store's function for this instead"
    )
