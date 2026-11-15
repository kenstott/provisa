# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3a71d2-4e8b-4f05-a6d7-1b2e3c4d5f60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command holding native SQL for a relational source (REQ-1988, amended 2026-11-13): what is
checked when it is saved.

The command is a ``source_query`` command whose binding is ``{"sql": "<native text>"}``: one
statement or several, in the source's own dialect. An argument is written ``{{name}}`` in the text,
as a MongoDB pipeline command and a statement command write one; every name is a declared argument
and every declared argument is named.

The text is split into statements by sqlglot's tokenizer for the source's dialect, which knows the
dialect's quoting (strings, dollar quotes, nested comments): a semicolon inside any of them does
not end a statement. A text the tokenizer cannot read is refused, as is a placeholder inside a
quoted string (it could only be filled by writing a value into text), a parameter written in the
driver's own style, and a statement that begins or ends a transaction: the statements run in one
native transaction the command opens and commits, so all of them commit or none does.

Native SQL is not parsed for governance: the command's kind and written table are what it
declares. A call is run by :mod:`provisa.executor.native_sql_command`.
"""

# Requirements: REQ-1988

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

BINDING_KEY = "sql"

# Source types read in SQL. A source_query command on one of these holds native SQL; one on a
# type without an entry in DIALECTS is refused naming the type.
RELATIONAL_SOURCE_TYPES = frozenset(
    {
        "postgresql",
        "cockroachdb",
        "yugabytedb",
        "greenplum",
        "mysql",
        "mariadb",
        "singlestore",
        "tidb",
        "sqlserver",
        "oracle",
        "firebird",
        "duckdb",
        "motherduck",
        "saphana",
        "jdbc",
        "sqlite",
        "snowflake",
        "bigquery",
        "databricks",
        "fabric",
        "synapse",
        "redshift",
        "athena",
        "trino",
        "clickhouse",
        "exasol",
        "hiveserver2",
    }
)

# Statements a command may not hold: the command's statements run in one transaction it opens and
# commits, so one that begins, commits or abandons a transaction would break all-or-none.
# The source types native SQL runs on, each with the sqlglot dialect that splits its text. Each
# has its run in executor/native_sql_command._RUNNERS; a next relational source is an entry in
# both.
DIALECTS: dict[str, str] = {"postgresql": "postgres"}

_TRANSACTION_CONTROL = frozenset({"BEGIN", "START", "COMMIT", "ROLLBACK", "END", "ABORT"})


class NativeSqlRefused(ValueError):
    """A native SQL command that cannot be saved, said naming what is wrong."""

    code = "actions.native_sql_invalid"

    def __init__(self, command: str, reason: str) -> None:
        self.command = command
        self.reason = reason
        super().__init__(f"command {command!r}: {reason}")


@dataclass(frozen=True)
class NativeStatement:
    """One statement of the text: ``texts`` are the pieces between its arguments, ``names`` the
    argument written between each two (``len(texts) == len(names) + 1``)."""

    texts: tuple[str, ...]
    names: tuple[str, ...]


def native_sql_text(name: str, binding: dict) -> str:
    """The native SQL of a command's binding, which holds it and nothing else."""
    unknown = sorted(set(binding) - {BINDING_KEY})
    if unknown:
        raise NativeSqlRefused(name, f"a native SQL binding holds 'sql' only, not {unknown}")
    sql = binding.get(BINDING_KEY)
    if not isinstance(sql, str) or not sql.strip():
        raise NativeSqlRefused(name, "a native SQL command's binding holds its SQL as 'sql'")
    return sql


def _is_string(token: Any) -> bool:
    return token.token_type.name.endswith("STRING")


def _placeholder_at(tokens: list, i: int) -> str | None:
    """The argument named by ``{{ name }}`` starting at ``tokens[i]``, or None."""
    from sqlglot.tokens import TokenType

    window = tokens[i : i + 5]
    if len(window) < 5:
        return None
    braces = [t.token_type for t in (window[0], window[1], window[3], window[4])]
    if braces != [TokenType.L_BRACE, TokenType.L_BRACE, TokenType.R_BRACE, TokenType.R_BRACE]:
        return None
    text = window[2].text
    return text if text.isidentifier() and not _is_string(window[2]) else None


def _refuse_token(name: str, token: Any, previous: Any) -> None:
    from sqlglot.tokens import TokenType

    if _is_string(token) and "{{" in token.text:
        raise NativeSqlRefused(
            name,
            f"the placeholder in {token.text!r} is inside a quoted string; an argument is a "
            "whole value, bound as a parameter",
        )
    if token.token_type == TokenType.PARAMETER:
        raise NativeSqlRefused(
            name, f"{token.text!r} is a driver parameter; an argument is written {{{{name}}}}"
        )
    if previous is None and token.text.upper() in _TRANSACTION_CONTROL:
        raise NativeSqlRefused(
            name,
            f"a statement begins with {token.text.upper()}; the command's statements run in one "
            "transaction it opens and commits",
        )


def split(name: str, sql: str, dialect: str) -> list[NativeStatement]:
    """``sql`` as its statements, each with the arguments it names, split by the dialect's own
    tokenizer. A text it cannot read, or holding no statement, is refused."""
    from sqlglot.dialects.dialect import Dialect
    from sqlglot.errors import SqlglotError
    from sqlglot.tokens import TokenType

    try:
        tokens = Dialect.get_or_raise(dialect).tokenize(sql)
    except SqlglotError as exc:
        raise NativeSqlRefused(
            name, f"the text cannot be split into {dialect} statements: {exc}"
        ) from exc
    statements: list[NativeStatement] = []
    texts: list[str] = []
    names: list[str] = []
    start: int | None = None  # where the current piece of text starts
    end = 0  # one past the current statement's last character
    previous = None
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.token_type == TokenType.SEMICOLON:
            if start is not None:
                texts.append(sql[start:end])
                statements.append(NativeStatement(tuple(texts), tuple(names)))
            texts, names, start, previous = [], [], None, None
            i += 1
            continue
        argument = _placeholder_at(tokens, i)
        if argument is not None:
            texts.append("" if start is None else sql[start : token.start])
            names.append(argument)
            start = end = tokens[i + 4].end + 1
            previous = tokens[i + 4]
            i += 5
            continue
        _refuse_token(name, token, previous)
        if start is None:
            start = token.start
        end = token.end + 1
        previous = token
        i += 1
    if start is not None or names:
        texts.append("" if start is None else sql[start:end])
        statements.append(NativeStatement(tuple(texts), tuple(names)))
    if not statements:
        raise NativeSqlRefused(name, "the text holds no statement")
    return statements


def check_text(
    name: str, binding: dict, arguments: list[str], dialect: str
) -> list[NativeStatement]:
    """A native SQL command's text split in ``dialect``: every argument it names is declared, and
    every declared argument is named."""
    statements = split(name, native_sql_text(name, binding), dialect)
    named = {argument for s in statements for argument in s.names}
    undeclared = sorted(named - set(arguments))
    if undeclared:
        raise NativeSqlRefused(
            name, f"the SQL names {undeclared}, which the command declares as no argument"
        )
    unused = sorted(set(arguments) - named)
    if unused:
        raise NativeSqlRefused(name, f"the arguments {unused} appear nowhere in the SQL")
    return statements


def check_on_source(
    name: str, source_id: str, source_type: str, binding: dict, arguments: list[str]
) -> list[NativeStatement]:
    """A native SQL command on its source: the source is relational and native SQL runs on its
    type, and the text, split in the source's dialect, names exactly its arguments. Answers the
    statements."""
    if source_type not in RELATIONAL_SOURCE_TYPES:
        raise NativeSqlRefused(
            name,
            f"source {source_id!r} is a {source_type} source; native SQL runs on a relational "
            "source",
        )
    dialect = DIALECTS.get(source_type)
    if dialect is None:
        raise NativeSqlRefused(name, f"native SQL commands are not available on {source_type} yet")
    return check_text(name, binding, arguments, dialect)
