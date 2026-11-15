# Copyright (c) 2026 Kenneth Stott
# Canary: 9b47d2e1-0c3a-4f58-8e6d-71a2c5f0e3b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code resolver: a code ``@query`` that decides which rows of one table relate to which rows
of another by its own algorithm (a spatial, vector or fuzzy match), where equal column values do
not (REQ-2249).

It takes the source table and the target table as its two table parameters (REQ-2226) and returns
one row per match, carrying the key of each side and any other columns it computes (a score, for
example). Its matches are read through the one query pipeline as the call
``SELECT * FROM fn('source', 'target') AS m`` -- the statement a view over the call holds, so the
matches are landed as any view is (a materialized view over the call) and are otherwise computed
when read. The call reads its tables as its caller (``table_reads``).

What a resolver must be is checked here, once, for every surface that names one: a code ``@query``,
two table parameters and no other argument, and a result that carries both keys."""

# Requirements: REQ-2249, REQ-2226

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.codesource.commands import CODE_COMMAND

#: The argument kind of a table parameter (models.FunctionArgument.arg_kind).
_TABLE_REF = "table_ref"


class ResolverRefused(ValueError):
    """A command that cannot resolve a relationship, said naming it and why."""

    code = "relationship.code_resolver_refused"

    def __init__(self, function: str, reason: str) -> None:
        super().__init__(f"code resolver {function!r}: {reason}")
        self.params = {"function": function, "reason": reason}


def _output_names(command: dict) -> list[str]:
    return [c["name"] for c in command.get("output_columns") or []]


def is_resolver(command: dict) -> bool:
    """Whether ``command`` has a resolver's shape: a code ``@query`` with exactly two table
    parameters and no other argument a call must be given. What a relationship's picker offers."""
    return _shape_refusal(command) is None


def _shape_refusal(command: dict) -> str | None:
    if command.get("impl_kind") != CODE_COMMAND:
        return "it is not a function of a code source"
    if command.get("kind") != "query":
        return "it is a @command, which writes; a resolver is a @query"
    arguments = command.get("arguments") or []
    tables = [a for a in arguments if a.get("arg_kind") == _TABLE_REF]
    if len(tables) != 2:
        return (
            f"it takes {len(tables)} table parameter(s); a resolver takes two, the source table "
            "and the target table"
        )
    others = [a["name"] for a in arguments if a.get("arg_kind") != _TABLE_REF]
    if others:
        return (
            f"it takes {', '.join(others)} besides its two tables; a relationship gives a "
            "resolver its two tables and nothing else"
        )
    if not _output_names(command):
        return "it returns no columns; a resolver returns one row per match"
    return None


def check_resolver(command: dict, source_key: str, target_key: str) -> None:
    """Refuse ``command`` as the resolver of a relationship whose matches carry the source row's
    key in ``source_key`` and the target row's in ``target_key``, naming why."""
    reason = _shape_refusal(command)
    if reason is None:
        returned = _output_names(command)
        missing = [k for k in (source_key, target_key) if k not in returned]
        if missing:
            reason = (
                f"its rows carry no {' or '.join(repr(k) for k in missing)} "
                f"(it returns {', '.join(returned)}); each match carries the key of both sides"
            )
    if reason is not None:
        raise ResolverRefused(command["name"], reason)


def call_sql(command: dict, source_table: str, target_table: str) -> str:
    """The statement that reads ``command``'s matches of ``source_table`` to ``target_table``
    (each a table name as a statement writes it): the call, given the two in the order its table
    parameters are declared -- the source table first."""
    from provisa.core.command_bindings import offered_name

    values = [exp.Literal.string(source_table), exp.Literal.string(target_table)]
    call = exp.Anonymous(this=offered_name(command, "sql"), expressions=values)
    table = exp.Table(this=call, alias=exp.TableAlias(this=exp.to_identifier("m")))
    return exp.select("*").from_(table).sql(dialect="postgres", normalize_functions=False)
