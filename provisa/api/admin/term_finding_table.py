# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a94-52d8-4b0f-9e61-a8d4f2c07b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Find terms in a registered table that holds definitions or documentation (REQ-2202).

A table is read one of two ways. Explicit (:class:`TermsSource`): the steward names a domain, a
table in it, its term column and its definition column, and each row is a term with its
definition, offered with no model call (term_finding.scan.defined). Inferred
(:class:`TableSource`): the steward names its id column and its text column instead, and each row
is one passage the model reads for the terms it defines, cited by the table and the row's id.
Either way the rows are read as the steward's acting role
through the one pipeline (``_govern_and_route`` / ``_execute_plan``), so the role's masks and row
rules apply and no text it cannot read reaches the model. The statement is written here, from the
table's registration; the caller sends no SQL. A table scan is a scan only: nothing is added to
the reference library.

A read that the role's or the table's row limit cut short (REQ-1949) is refused by name: a scan of
part of a table would leave rows out without saying so.
"""

# Requirements: REQ-2202

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, cast

from provisa.api.admin.table_read import table_sql_ref
from provisa.api.errors import ApiError
from provisa.term_finding import scan


class _Source:
    domain: str
    table: str

    @property
    def name(self) -> str:
        """The table as a citation names it: its domain, then its own name."""
        return f"{self.domain}.{self.table}"

    @property
    def columns(self) -> tuple[str, str]:
        """The two columns read: the one a row is known by, then the one holding its text."""
        raise NotImplementedError

    def as_dict(self) -> dict[str, str]:
        return asdict(cast("Any", self))


@dataclass(frozen=True)
class TableSource(_Source):
    """A table whose rows the model reads (inferred): the column that identifies a row and the
    column holding its text."""

    domain: str
    table: str
    id_column: str
    text_column: str

    @property
    def columns(self) -> tuple[str, str]:
        return self.id_column, self.text_column


@dataclass(frozen=True)
class TermsSource(_Source):
    """A table of terms and their definitions (explicit): its term column and its definition
    column."""

    domain: str
    table: str
    term_column: str
    definition_column: str

    @property
    def columns(self) -> tuple[str, str]:
        return self.term_column, self.definition_column


def _refused(code: str, message: str, **params: Any) -> ApiError:
    return ApiError(400, f"term_finding.{code}", message, **params)


_TERMS = ("term_column", "definition_column")
_TEXT = ("id_column", "text_column")


def source_of(raw: Any) -> TableSource | TermsSource:
    """The ``table`` of a scan request: an object naming ``domain`` and ``table``, and either
    ``term_column`` and ``definition_column`` (explicit) or ``id_column`` and ``text_column``
    (inferred), each a name."""
    given = raw if isinstance(raw, dict) else {}
    terms, text = any(k in given for k in _TERMS), any(k in given for k in _TEXT)
    kind, pair = (TermsSource, _TERMS) if terms and not text else (TableSource, _TEXT)
    fields = ("domain", "table", *pair)
    if terms == text or not all(isinstance(given.get(f), str) and given[f].strip() for f in fields):
        raise _refused(
            "table_source",
            "a table names a domain, a table and either its term column and its definition "
            "column, or its id column and its text column",
        )
    return kind(*(given[f].strip() for f in fields))


def _column(table: dict, name: str) -> str:
    """The table's column ``name`` as a statement names it: its alias, else its name."""
    for c in table["columns"]:
        if name in (c.get("alias"), c["column_name"]):
            return c.get("alias") or c["column_name"]
    raise _refused(
        "unknown_column",
        f"{table['domain_id']}.{table.get('alias') or table['table_name']} has no column {name!r}",
        column=name,
    )


def registered(state: Any, source: _Source) -> dict:
    """The registered table in service ``source`` names, refused when there is none."""
    for table in state.tables:
        if table["domain_id"] == source.domain and source.table in (
            table.get("alias"),
            table["table_name"],
        ):
            return table
    raise ApiError(
        404,
        "schema.table_not_found",
        f"domain {source.domain!r} has no registered table {source.table!r}",
        table=source.name,
    )


def statement(table: dict, source: _Source) -> str:
    """Every row's two columns (:attr:`_Source.columns`), in the order of the first."""
    ident, text = (_column(table, c) for c in source.columns)

    def quoted(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    return (
        f"SELECT {quoted(ident)}, {quoted(text)} FROM {table_sql_ref(table)} "
        f"ORDER BY {quoted(ident)}"
    )


def passages(rows: list[tuple]) -> list[scan.Passage]:
    """Each row, (id, text), as one passage; a row with no text holds none."""
    out: list[scan.Passage] = []
    for ident, text in rows:
        if text is None or not str(text).strip():
            continue
        out.append(scan.Passage(len(out) + 1, None, None, str(text), row=str(ident)))
    return out


async def read(state: Any, source: TableSource, role_id: str) -> list[scan.Passage]:
    """The table's rows as ``role_id`` reads them, each a passage."""
    return passages(await rows(state, source, role_id))


async def rows(state: Any, source: _Source, role_id: str) -> list[tuple]:
    """The table's rows, its two columns, as ``role_id`` reads them."""
    from provisa.compiler.limits import role_max_query_time_ms
    from provisa.core import request_deadline
    from provisa.core.limits import request_timeout_for
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route
    from provisa.security.rights import require_role

    sql = statement(registered(state, source), source)
    # The read's time is the role's, as the table viewer's is (admin/table_profile_router.py).
    transport_timeout = request_timeout_for("graphql")
    role_ms = role_max_query_time_ms(require_role(state.roles, role_id))
    timeout = transport_timeout if role_ms is None else min(transport_timeout, role_ms / 1000.0)
    with request_deadline.within(timeout):
        plan = await _govern_and_route(sql, role_id)
        result = await _execute_plan(plan, state)
    if plan.limit_outcome is not None:
        # "cut": more rows match; "unchecked": the read filled the limit and whether more match
        # could not be asked. Either way the scan would not be known to read every row.
        from provisa.compiler.row_limit import ROLE

        limit = plan.row_limit.limit
        whose = f"role {role_id!r}" if plan.row_limit.kind == ROLE else "the table"
        raise ApiError(
            409,
            "term_finding.rows_cut",
            f"reading {source.name} stopped at the row limit of {whose}, {limit} rows: a scan "
            "reads every row of its table",
            table=source.name,
            limit=limit,
        )
    return list(result.rows)
