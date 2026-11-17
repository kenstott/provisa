# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c51-9d4e-4b7a-a1c6-5e2d8f0b7c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a value is written into a statement the PostgreSQL engine runs: the one place.

Every statement given to the PostgreSQL engine that carries a registered value -- a source's
host, port, database, user, credential or path, a table's schema or name, a column's name -- writes
it through :func:`pg_lit` (a string literal) or :func:`pg_ident` (an identifier). A text handed to
``cursor.execute`` with no parameters runs every statement in it, so a value written any other way
is a statement of its author's choosing (tests/unit/federation/test_engine_ddl_quoting.py holds
each site to this).

The engine's server runs with ``standard_conforming_strings`` on (PostgreSQL's default since 9.1):
a backslash in a literal is an ordinary character.
"""

from __future__ import annotations

from provisa.compiler.sql_literals import sql_literal


def pg_lit(value: str | int) -> str:
    """``value`` as a PostgreSQL string literal. A NUL character has no literal and is refused
    (``sql_literals.UnencodableLiteral``)."""
    return sql_literal(str(value), "postgres")


def pg_ident(*parts: str) -> str:
    """``parts`` as a quoted, dot-joined PostgreSQL identifier: ``pg_ident("s", "t")`` is
    ``"s"."t"``. A NUL character has no place in a name and is refused."""
    if not parts:
        raise ValueError("an identifier has at least one part")
    if any("\x00" in part for part in parts):
        raise ValueError("an identifier cannot hold a NUL character")
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)
