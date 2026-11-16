# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6e1c1e-5f0b-4a44-9a0a-6e1f5b7c2d93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a value is written into a DuckDB statement: the one place.

Every statement the DuckDB engine is given that carries a registered value -- a source's path,
host, database, user, credential, a table's schema or name, a column's name -- writes it through
:func:`lit` (a string literal) or :func:`ident` (an identifier). DuckDB runs every statement of a
text it is handed, so a value written any other way is a statement of its author's choosing
(tests/unit/federation/test_duckdb_ddl_quoting.py holds each site to this, and
test_duckdb_sql_is_built_through_the_quoting_helper refuses a new one).

A value inside a connection string DuckDB's postgres extension parses again (``ATTACH
'host=... dbname=...'``) is written for both layers: :func:`libpq_dsn` for the string, then
:func:`lit` for the literal holding it.
"""

from __future__ import annotations

import re

from provisa.compiler.sql_literals import sql_literal


def lit(value: str) -> str:
    """``value`` as a DuckDB string literal. A NUL character has no literal and is refused
    (``sql_literals.UnencodableLiteral``)."""
    return sql_literal(str(value), "duckdb")


def ident(*parts: str) -> str:
    """``parts`` as a quoted, dot-joined DuckDB identifier: ``ident("c", "s", "t")`` is
    ``"c"."s"."t"``. A NUL character has no place in a name and is refused."""
    if not parts:
        raise ValueError("an identifier has at least one part")
    if any("\x00" in part for part in parts):
        raise ValueError("an identifier cannot hold a NUL character")
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


# A libpq keyword value that needs no quotes: non-empty, with no space, quote or backslash.
_LIBPQ_BARE = re.compile(r"[^\s'\\]+")


def libpq_value(value: str | int) -> str:
    """``value`` as one value of a libpq ``keyword=value`` connection string: as it is when it
    holds nothing libpq reads specially, else in single quotes with ``\\`` and ``'`` escaped by a
    backslash (libpq's rule, PostgreSQL docs 32.1.1.1)."""
    text = str(value)
    if _LIBPQ_BARE.fullmatch(text):
        return text
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def libpq_dsn(**params: str | int) -> str:
    """A libpq ``keyword=value`` connection string of ``params``, in the order given. It is the
    string only: where it goes into a statement it is written with :func:`lit`."""
    return " ".join(f"{keyword}={libpq_value(value)}" for keyword, value in params.items())
