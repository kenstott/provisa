# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a94-52d8-4b0f-9e61-a8d4f2c07b35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Find terms in a registered table: its rows read as the scan's passages (REQ-2202).

The steward names a domain, a table in it, its id column and its text column; each row is one
passage, cited by the table and the row's id. The rows are read as the steward's acting role
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
from typing import Any

from provisa.api.admin.table_read import table_sql_ref
from provisa.api.errors import ApiError
from provisa.term_finding import scan


@dataclass(frozen=True)
class TableSource:
    """The table a scan reads: by its domain and name, with the column that identifies a row and
    the column holding its text."""

    domain: str
    table: str
    id_column: str
    text_column: str

    @property
    def name(self) -> str:
        """The table as a citation names it: its domain, then its own name."""
        return f"{self.domain}.{self.table}"

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def _refused(code: str, message: str, **params: Any) -> ApiError:
    return ApiError(400, f"term_finding.{code}", message, **params)


def source_of(raw: Any) -> TableSource:
    """The ``table`` of a scan request: an object naming ``domain``, ``table``, ``id_column`` and
    ``text_column``, each a name."""
    fields = ("domain", "table", "id_column", "text_column")
    if not isinstance(raw, dict) or not all(
        isinstance(raw.get(f), str) and raw[f].strip() for f in fields
    ):
        raise _refused(
            "table_source",
            "a table scan names a domain, a table, its id column and its text column",
        )
    return TableSource(*(raw[f].strip() for f in fields))


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


def registered(state: Any, source: TableSource) -> dict:
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


def statement(table: dict, source: TableSource) -> str:
    """Every row's id and text, in id order."""
    ident, text = _column(table, source.id_column), _column(table, source.text_column)

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
    return passages(list(result.rows))
