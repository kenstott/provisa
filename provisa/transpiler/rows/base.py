# Copyright (c) 2026 Kenneth Stott
# Canary: ac89cbe0-ca04-4464-83b4-96d31a9719a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The notation of the function table's rows (REQ-2167, REQ-2171): an overload and its forms
(:class:`Macro`), a form in PostgreSQL's dialect (:class:`Pg`), the dialect sets a form is listed
under, and the helpers every family's rows are written with (provisa.transpiler.rows)."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from sqlglot.dialects.dialect import Dialects

from provisa.core.pg_error import MARK
from provisa.transpiler import pg_forms_types

NATIVE = frozenset({"postgres"})
"""The dialects that have every function of the table themselves."""

_DUCKDB = frozenset({"duckdb"})
_TRINO = frozenset({"trino", "presto", "athena"})
_SPARK = frozenset({"spark", "databricks"})
_EVERY = frozenset({"*"})
#: Every dialect sqlglot writes but DuckDB: ``_EVERY`` in ``own`` would answer before DuckDB's form.
_EVERY_BUT_DUCKDB = frozenset(d.value for d in Dialects if d.value) - _DUCKDB - NATIVE
#: LIKE ignores case by the collation here, and a replay of a dialect that differs refuses the form.
_NO_CASE_SENSITIVE_LIKE = frozenset({"mysql", "tsql", "oracle", "sqlite"})
#: No regular expression match, or none sqlglot can write.
_NO_REGEX = frozenset({"tsql", "sqlite", "oracle", "mysql"})
_CLICKHOUSE = frozenset({"clickhouse"})
_NO_SPLIT_PART = frozenset({"tsql"})
#: A time of day is no type of these engines: a cast to it is refused by name.
_NO_TIME = frozenset({"trino", "presto", "athena", "clickhouse"})
#: A regexp_substr(value, pattern, start, occurrence) of the dialect's own, sqlglot's to write.
_REGEXP_SUBSTRS = frozenset({"snowflake", "oracle", "mysql", "bigquery", "redshift"})
#: A cast of a text that is no number is an error here, so a form can raise PostgreSQL's refusal.
#: MySQL and SQLite read such a text as 0, and Spark and Databricks outside ANSI mode as NULL: a row
#: that raises is refused on them by name until a replay shows otherwise.
_CASTS_CHECKED = _DUCKDB | _TRINO | {
    "snowflake", "bigquery", "clickhouse", "tsql", "redshift", "oracle",
}  # fmt: skip


class Pg(str):
    """A form in PostgreSQL's dialect, written for each dialect it is listed under."""


#: The form of a function sqlglot already writes for the dialect: the call is left to it.
MAPPED = None

Forms = Mapping[frozenset[str], "str | None"]


class Step(enum.Enum):
    """The rule's steps, in the order it takes them."""

    OWN = "the dialect's own function, under any name"
    FUNCTIONS = "the functions the dialect has"
    OPERATORS = "the dialect's inline operators"
    REFUSED = "refused by name"


@dataclass(frozen=True)
class Macro:
    """One overload of a function: its parameters and its forms, by the step of the rule."""

    name: str
    params: tuple[str, ...]
    forms: Forms = field(default_factory=dict)
    own: Forms = field(default_factory=dict)
    operators: Forms = field(default_factory=dict)
    #: The dialects replay found another answer on (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED).
    differs_on: frozenset[str] = frozenset()

    def resolved(self, dialect: str) -> tuple[Step, str | None]:
        """The rule: the first step with a form on ``dialect``, and that form. ``differs_on`` holds
        the dialects a replay refused the shared form on (REQ-2171, A VARIATION IS ADDED ONLY AFTER
        A REPLAY FAILS)."""
        if dialect in NATIVE:
            return Step.OWN, MAPPED
        if dialect in self.differs_on:
            return Step.REFUSED, None
        steps = (
            (Step.OWN, self.own),
            (Step.FUNCTIONS, self.forms),
            (Step.OPERATORS, self.operators),
        )
        for step, forms in steps:
            for dialects, sql in forms.items():
                if dialect in dialects or "*" in dialects:
                    return step, sql
        return Step.REFUSED, None


def strict(params: str, sql_type: str, answer: str) -> str:
    """``answer``, or a NULL of ``sql_type`` for a NULL argument: a strict function, of its
    PostgreSQL type where the form's own NULL would have none."""
    if not params:
        return answer
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) ELSE {answer} END"


def quoted_identifier(name: str) -> str:
    """A name as quote_ident writes it: bare when it is a lowercase identifier."""
    return (
        f"CASE WHEN regexp_full_match({name}, '[a-z_][a-z0-9_$]*') THEN {name} "
        f"ELSE '\"' || replace({name}, '\"', '\"\"') || '\"' END"
    )


def _comparisons(prefix: str, *suffixes: str) -> tuple[Macro, ...]:
    operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
    return tuple(
        _operators(f"{prefix}{suffix}", "a, b", f"a {operators[suffix]} b") for suffix in suffixes
    )


def _pg(name: str, params: str, form: str, differs_on: frozenset[str] = frozenset()) -> Macro:
    """An overload whose form is PostgreSQL's syntax, which sqlglot writes for every dialect."""
    where = _CASTS_CHECKED if MARK in form else _EVERY
    return Macro(
        name, tuple(p for p in params.split(", ") if p), {where: Pg(form)}, differs_on=differs_on
    )


def _udf(name: str, params: str, form: str) -> Macro:
    """An overload that calls a Python UDF of provisa.federation.duckdb_pg_scalars: DuckDB's alone
    until it is written as SQL."""
    return Macro(name, tuple(p for p in params.split(", ") if p), {_DUCKDB: Pg(form)})


def _macro(
    name: str,
    params: str,
    duckdb: str,
    others: Forms | None = None,
    own: Forms | None = None,
    differs_on: frozenset[str] = frozenset(),
) -> Macro:
    """An overload, its DuckDB form, any other dialect's, and any dialect's own function."""
    return Macro(
        name,
        tuple(p for p in params.split(", ") if p),
        {_DUCKDB: duckdb, **(others or {})},
        own or {},
        differs_on=differs_on,
    )


def _operators(name: str, params: str, sql: str, others: Forms | None = None) -> Macro:
    """An overload whose form is the dialect's operators alone."""
    return Macro(name, tuple(params.split(", ")), operators={_DUCKDB: sql, **(others or {})})


def _operator(form: pg_forms_types.Form) -> Macro:
    """A row of the generated rule: a form that raises on the dialects that can raise it (REQ-2171,
    AN ANSWER THAT DIFFERS IS REFUSED)."""
    tweak = pg_forms_types.DUCKDB_TWEAKS.get(form.name)
    checked = form.raises or form.name in pg_forms_types.OVERFLOW_UNCHECKED
    forms = {_CASTS_CHECKED if checked else _EVERY: Pg(form.sql)}
    return Macro(
        form.name,
        tuple(form.params.split(", ")),
        operators={_DUCKDB: tweak, **forms} if tweak else forms,
    )


def _operated(name: str, of: str) -> Macro:
    """A row whose form is the generated row ``of``'s."""
    form = pg_forms_types.FORMS_BY_NAME[of]
    return replace(_operator(form), name=name)


_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
