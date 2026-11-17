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


# -- connection strings a DuckDB extension parses again -----------------------------------------
#
# Each is the string only: where it goes into a statement it is written with :func:`lit`. The
# grammars are the extensions' own parsers, read at their source and checked live in CI
# (tests/integration/test_duckdb_connection_string_grammar_e2e.py).

# An ADO.NET value written bare: no ';', no brace, no whitespace at either end, not empty.
_ADO_BARE = re.compile(r"[^;{}\s](?:[^;{}]*[^;{}\s])?")


def ado_value(value: str | int) -> str:
    """``value`` as one value of the mssql extension's ADO.NET ``Key=Value;`` string: as it is
    when it holds no ``;`` or brace and no space at either end, else in braces with ``}`` doubled
    (the extension's TokenizeConnectionString: ``{...}`` quoting, ``}}`` a literal ``}``)."""
    text = str(value)
    if _ADO_BARE.fullmatch(text):
        return text
    return "{" + text.replace("}", "}}") + "}"


def ado_dsn(*pairs: tuple[str, str | int]) -> str:
    """The mssql extension's ``Key=Value;...`` string of ``pairs``, in the order given."""
    return ";".join(f"{key}={ado_value(value)}" for key, value in pairs)


def firebird_url(user: str, password: str, host: str, port: str | int, path: str) -> str:
    """The firebird extension's ``firebird://user:password@host:port/path`` URL. The extension
    percent-decodes the user and the password, so they are percent-encoded; it decodes neither
    the host nor the path and splits its options off at the first ``?``, so a host or a path
    holding ``?`` has no spelling in it and is refused."""
    from urllib.parse import quote

    for name, value in (("host", host), ("path", path)):
        if "?" in str(value):
            raise ValueError(
                f"a Firebird {name} cannot hold '?': the firebird extension reads what follows "
                "it as connection options"
            )
    return (
        f"firebird://{quote(str(user), safe='')}:{quote(str(password), safe='')}"
        f"@{host}:{port}/{path}"
    )


def bigquery_dsn(project: str) -> str:
    """The bigquery extension's ``project=<id>`` string. The extension splits it at each space
    with no quoting, so a project holding whitespace has no spelling in it and is refused."""
    if not project or any(ch.isspace() for ch in project):
        raise ValueError(
            f"BigQuery project {project!r}: a project id is one word; the bigquery extension "
            "reads each space-separated word as another setting"
        )
    return f"project={project}"
