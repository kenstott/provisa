# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1e4d7a-2c6f-4e93-b5a0-7d9c3f1e6a28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a value is written into a statement the ClickHouse engine runs: the one place.

Every statement given to the ClickHouse engine that carries a registered value -- a source's
host, port, database, user, credential or path, a table's schema or name, a column's name -- writes
it through :func:`ch_lit` (a string literal) or :func:`ch_ident` (an identifier). ClickHouse reads
a backslash escape in both, so a value is escaped by its rules, not PostgreSQL's
(tests/unit/federation/test_engine_ddl_quoting.py holds each site to this).
"""

from __future__ import annotations

from provisa.compiler.sql_literals import sql_literal


def ch_lit(value: str | int) -> str:
    """``value`` as a ClickHouse string literal: ``'`` doubled, ``\\`` and control characters
    backslash-escaped. A NUL character is refused (``sql_literals.UnencodableLiteral``)."""
    return sql_literal(str(value), "clickhouse")


def ch_ident(*parts: str) -> str:
    """``parts`` as a quoted, dot-joined ClickHouse identifier: ``"`` doubled and ``\\``
    doubled, as ClickHouse reads a double-quoted name. A NUL character is refused."""
    if not parts:
        raise ValueError("an identifier has at least one part")
    if any("\x00" in part for part in parts):
        raise ValueError("an identifier cannot hold a NUL character")
    return ".".join('"' + part.replace("\\", "\\\\").replace('"', '""') + '"' for part in parts)


#: The placeholder a per-table engine clause carries where the table's name goes, as a string
#: literal: ``MongoDB('h:27017', 'app', '{table}', ...)`` (built-in and custom connectors alike).
TABLE_PLACEHOLDER = "{table}"


def bind_table(clause: str, table: str) -> str:
    """``clause`` with each string literal whose value is exactly :data:`TABLE_PLACEHOLDER`
    replaced by ``table`` as a literal (:func:`ch_lit`). The literals are found by ClickHouse's
    own grammar (sqlglot's tokenizer), so a value elsewhere in the clause that holds the text
    ``{table}`` with anything around it -- a password, a path -- is left as it is, and no quote in
    ``table`` can end the literal it is written into."""
    from sqlglot.dialects.clickhouse import ClickHouse
    from sqlglot.tokens import TokenType

    spans = [
        (token.start, token.end)
        for token in ClickHouse().tokenize(clause)
        if token.token_type == TokenType.STRING and token.text == TABLE_PLACEHOLDER
    ]
    bound = clause
    for start, end in reversed(spans):
        bound = bound[:start] + ch_lit(table) + bound[end + 1 :]
    return bound
