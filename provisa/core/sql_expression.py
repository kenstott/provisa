# Copyright (c) 2026 Kenneth Stott
# Canary: 9a1d6e38-2f47-4c5b-b8e0-3d7c1f9a2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A function declared as a SQL expression template (REQ-2168).

The template is one PostgreSQL-dialect expression -- the dialect every statement is in before it
is transpiled for the engine that runs it -- naming each argument as ``{name}``:
``array_to_string({list}, {sep})``. A call is replaced by the template with its arguments put in,
inside the statement that calls it, so the function pushes down with the query and runs under
that statement's governance. The template is parsed and transpiled for every engine dialect when
it is saved, so a broken one fails then, not at a call.
"""

from __future__ import annotations

import re
from functools import lru_cache

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ErrorLevel, SqlglotError

_PLACEHOLDER = re.compile(r"\{([A-Za-z_]\w*)\}")
# Each {name} is parsed as a column of this prefix, then replaced by the call's argument.
_SLOT = "__provisa_arg_"


class SqlExpressionRefused(ValueError):
    """A SQL expression template that cannot be used, said by name."""


def _sql(function: str, binding: dict) -> str:
    sql = binding.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        raise SqlExpressionRefused(
            f"function {function!r}: a sql_expression function's binding is its template, "
            "binding.sql"
        )
    return sql


def parse_template(function: str, binding: dict, arguments: list[str]) -> exp.Expression:
    """The template of ``binding`` as an expression whose argument slots are columns named
    ``__provisa_arg_<name>``. A template that is not one expression, names an argument the
    function does not declare, or leaves a declared one out, is refused naming it."""
    sql = _sql(function, binding)
    named = _PLACEHOLDER.findall(sql)
    unknown = [n for n in named if n not in arguments]
    if unknown:
        raise SqlExpressionRefused(
            f"function {function!r}: its template names {{{unknown[0]}}}, which is not one of "
            f"its arguments ({', '.join(arguments) or 'none'})"
        )
    unused = [a for a in arguments if a not in named]
    if unused:
        raise SqlExpressionRefused(
            f"function {function!r}: its template does not use the argument {unused[0]!r}"
        )
    text = _PLACEHOLDER.sub(lambda m: f'"{_SLOT}{m.group(1)}"', sql)
    try:
        parsed = sqlglot.parse(f"SELECT {text}", read="postgres")
    except SqlglotError as exc:
        raise SqlExpressionRefused(
            f"function {function!r}: its template {sql!r} is not a SQL expression: {exc}"
        ) from exc
    select = parsed[0] if len(parsed) == 1 else None
    if (
        not isinstance(select, exp.Select)
        or len(select.expressions) != 1
        or select.args.get("from")
    ):
        raise SqlExpressionRefused(
            f"function {function!r}: its template {sql!r} is not one SQL expression"
        )
    return select.expressions[0]


def deployment_dialects(state: object) -> set[str]:  # object-ok: AppState lives above core
    """The dialects a statement of this deployment may run in: its federation engine's and each
    registered source's (a single-source read runs on its source)."""
    engine = getattr(state, "federation_engine")
    sources = getattr(state, "source_dialects")
    return {engine.dialect, *(d for d in sources.values() if d)}


def check_for_engines(function: str, binding: dict, arguments: list[str], dialects) -> None:
    """Transpile the template for each engine dialect in ``dialects``; one an engine cannot
    express is refused naming the engine."""
    template = parse_template(function, binding, arguments)
    for dialect in sorted(set(dialects)):
        try:
            template.sql(dialect=dialect, unsupported_level=ErrorLevel.RAISE)
        except SqlglotError as exc:
            raise SqlExpressionRefused(
                f"function {function!r}: its template cannot be written for the {dialect} "
                f"engine: {exc}"
            ) from exc


@lru_cache(maxsize=1024)
def _kept(function: str, sql: str, arguments: tuple[str, ...]) -> exp.Expression:
    """A parsed template, kept: a statement calls a function once per row it writes it into."""
    return parse_template(function, {"sql": sql}, list(arguments))


# The comment each rendered call carries, naming its function: a route to a source whose dialect
# cannot write the call is told by it (transpiler/router), only for a statement that holds one.
MARKER = "provisa:fn "


@lru_cache(maxsize=4096)
def _writable(function: str, sql: str, arguments: tuple[str, ...], dialect: str) -> bool:
    try:
        _kept(function, sql, arguments).sql(dialect=dialect, unsupported_level=ErrorLevel.RAISE)
    except SqlglotError:
        return False
    return True


def writable(function: dict, dialect: str) -> bool:
    """Whether ``function``'s template can be written in ``dialect``."""
    names = tuple(a["name"] for a in function.get("arguments") or [])
    return _writable(function["name"], function["binding"]["sql"], names, dialect)


def refuse_unless_writable(function: dict, called: str, state: object) -> None:
    """Refuse a call of ``function`` (written ``called``) by name, before anything is sent, when
    the deployment's engine cannot write its template."""
    dialect = getattr(state, "federation_engine").dialect
    if not writable(function, dialect):
        raise SqlExpressionRefused(f"{called} isn't available on the {dialect} engine")


def render(function: dict, args: list[exp.Expression]) -> exp.Expression:
    """The call ``function(args)`` as its template with ``args`` put in, in declared order. A call
    with another number of arguments is refused naming the function's signature."""
    declared = [a["name"] for a in function.get("arguments") or []]
    if len(args) != len(declared):
        raise SqlExpressionRefused(
            f"{function['name']}({', '.join(declared)}) takes {len(declared)} argument(s); "
            f"{len(args)} given"
        )
    by_slot = {f"{_SLOT}{n}": a for n, a in zip(declared, args, strict=True)}
    template = _kept(function["name"], function["binding"]["sql"], tuple(declared)).copy()

    def _put(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not node.table and node.name in by_slot:
            return by_slot[node.name].copy()
        return node

    call = exp.Paren(this=template.transform(_put))
    call.add_comments([f"{MARKER}{function['name']}"])
    return call
