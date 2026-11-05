# Copyright (c) 2026 Kenneth Stott
# Canary: 5a8c2e17-9d4b-4f60-b3e1-7c0d2a6f9b48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's dq service (REQ-2133): everything about the data quality of a dataset or a data product,
as the meta domain shows it to the caller.

Offered to a view's Python preflight and to a code source through ``provisa_services``
(``ps.dq.dataset(name)``, ``ps.dq.product(id)``). Every read is a governed read of the meta domain
as the caller's role -- a code source's request role, a preflight's build role -- through the one
pipeline, so dq answers exactly what that role reads there, and nothing it does not.

- A dataset's answer: the checker tables scanning it (REQ-1443), each with its description, its
  contract, the product it is assigned to and its latest run (outcome, time, failing checks); and
  its profiles (REQ-1934), each profile result table with its latest run's rows, so a check can
  compare this load against the dataset's profiled history. Asked from the preflight of a view in
  a product, each checker also says which of that product's lists it is in.
- A product's answer: its expectations, promises, related and unattached checks (REQ-2157), as the
  product's lineage read as the role classifies them (the lineage router's
  ``product_lineage_for``, the one answer every reader of a product's data quality gets), each
  checker with its latest run.
"""

# Requirements: REQ-2133, REQ-1443, REQ-1934, REQ-2157

from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote

from provisa.compiler.sql_literals import sql_literal


def _lit(value: str) -> str:
    """``value`` as a literal of the semantic SQL the governed pipeline reads (Postgres)."""
    return sql_literal(value, "postgres")


#: A product's data-quality lists (REQ-2157), in the order an answer lists them.
STATES = ("expectations", "promises", "related", "unattached")

_CHECKER_COLUMNS = (
    "id, domain_id, schema_name, table_name, alias, description, dq_contract, product_id, "
    "latest_outcome, latest_run_at"
)


async def answer(grant: Any, role: str | None, method: str, sub: str, span: Any) -> bytes:
    """The answer to one dq call: ``dataset/<name>`` or ``product/<id>``, read as ``role``."""
    from provisa.codesource.services import ServiceRefused
    from provisa.core.request_context import reset_current_org, set_current_org

    if method != "GET":
        raise ServiceRefused(405, f"dq is read with GET, not {method}")
    if role is None:
        raise ServiceRefused(
            403,
            "dq answers what the meta domain shows the caller's role, and this call serves no "
            "request that names one",
        )
    kind, _, name = sub.partition("/")
    name = unquote(name)
    span.set_attribute("dq.kind", kind)
    token = set_current_org(grant.org_id)
    try:
        if kind == "dataset":
            found = await dataset(name, role, grant.product_id)
        elif kind == "product":
            found = await product(name, role)
        else:
            raise ServiceRefused(404, f"dq answers dataset/<name> and product/<id>, not {sub!r}")
    except PermissionError as refused:
        # Governance refused a read of the role's: the caller is told why, as the role's own
        # statement would be, not that the service failed.
        raise ServiceRefused(403, f"dq read as {role!r} was refused: {refused}") from refused
    finally:
        reset_current_org(token)
    if found is None:
        raise ServiceRefused(404, f"no {kind} {name!r} that the role {role!r} can see")
    return json.dumps(found, default=str).encode()


async def _read(sql: str, role: str) -> list[dict]:
    """``sql``'s rows, read as ``role`` through the one governed pipeline."""
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    result = await _execute_plan(await _govern_and_route(sql, role))
    names = [str(n) for n in result.column_names]
    return [dict(zip(names, row, strict=True)) for row in result.rows]


def _meta(row: dict, role: str) -> Any:
    """The table a registered table's row of the meta domain names, as ``role``'s surface serves
    it; None when the role reaches no such table (the meta domain lists tables in domains the role
    cannot read)."""
    from provisa.api.app import state
    from provisa.api.role_surface import role_surface

    surface = role_surface(state, role)
    if surface is None:
        return None
    table_id = int(row["id"])
    return next((m for m in surface.context.tables.values() if m.table_id == table_id), None)


def _reached(rows: list[dict], role: str) -> list[dict]:
    """Of the meta domain's ``rows``, those naming a table ``role`` reaches."""
    return [row for row in rows if _meta(row, role) is not None]


def _name(row: dict, role: str) -> str:
    """A registered table's name as the model shows it: ``<domain>.<table>``, its alias where it
    has one -- never the physical schema and table its rows are kept in."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    meta = _meta(row, role)
    return f"{domain_to_sql_name(meta.domain_id)}.{semantic_table_name(meta)}"


def _relation(row: dict, role: str) -> str:
    """A registered table's reference in the role's SQL: its semantic name, which governance
    admits (a physical name is refused)."""
    from provisa.compiler.sql_rewrite import semantic_ref

    return semantic_ref(_meta(row, role))


async def _dataset_row(name: str, role: str) -> dict | None:
    if name.isdigit():
        where = f"id = {int(name)}"
    else:
        domain, _, table = name.partition(".")
        if not domain or not table:
            return None
        where = (
            f"domain_id = {_lit(domain)} AND (alias = {_lit(table)} OR table_name = {_lit(table)})"
        )
    rows = await _read(
        f"SELECT id, domain_id, schema_name, table_name, alias FROM meta.registered_tables "
        f"WHERE {where}",
        role,
    )
    return rows[0] if len(rows) == 1 else None


async def _latest_run(checker: dict, role: str) -> dict | None:
    """A checker table's latest run: its outcome and time (checker_latest_runs, through the meta
    domain), and the checks of that scan that did not pass. None when it has never run."""
    if checker["latest_run_at"] is None:
        return None
    relation = _relation(checker, role)
    failing = await _read(
        f"SELECT * FROM {relation} WHERE outcome <> 'pass' "
        f"AND scan_time = (SELECT MAX(scan_time) FROM {relation})",
        role,
    )
    return {
        "outcome": checker["latest_outcome"],
        "ran_at": checker["latest_run_at"],
        "failing_checks": failing,
    }


async def _checker(row: dict, role: str) -> dict:
    return {
        "checker_table": _name(row, role),
        "checker_table_id": row["id"],
        "description": row["description"],
        "contract": row["dq_contract"],
        "assigned_product_id": row["product_id"],
        "latest_run": await _latest_run(row, role),
    }


async def _profiles(dataset_id: int, role: str) -> list[dict]:
    """The profile result tables registered for the dataset (REQ-1934), each with its latest run's
    rows: every run is kept, keyed by run_id with run_time as the watermark."""
    tables = _reached(
        await _read(
            "SELECT id, domain_id, schema_name, table_name, alias FROM meta.registered_tables "
            f"WHERE profiled_table_id = {dataset_id} ORDER BY table_name",
            role,
        ),
        role,
    )
    out = []
    for table in tables:
        relation = _relation(table, role)
        rows = await _read(
            f"SELECT * FROM {relation} WHERE run_time = (SELECT MAX(run_time) FROM {relation})",
            role,
        )
        out.append(
            {
                "profile_table": _name(table, role),
                "profile_table_id": table["id"],
                "latest_run": rows or None,
            }
        )
    return out


async def dataset(name: str, role: str, view_product: str | None = None) -> dict | None:
    """Everything about the data quality of the dataset ``name`` (``domain.table`` or a table id)
    that ``role`` can see; None when it can see no such dataset."""
    found = await _dataset_row(name, role)
    if found is None or _meta(found, role) is None:
        return None
    dataset_id = int(found["id"])
    # Only the checkers whose results the role reaches: another domain's checker on this dataset
    # is none of the role's (it reads no rows of it), never a refused read.
    checkers = _reached(
        await _read(
            f"SELECT {_CHECKER_COLUMNS} FROM meta.registered_tables "
            f"WHERE checked_table_id = {dataset_id} ORDER BY table_name",
            role,
        ),
        role,
    )
    related: dict[int, str] = {}
    if view_product is not None:
        lists = await _product_dq(view_product, role) or {}
        related = {
            int(e["checker_table_id"]): name
            for name, entries in lists.items()
            for e in entries
            if int(e["checked_table_id"]) == dataset_id
        }
    checks = []
    for row in checkers:
        check = await _checker(row, role)
        if view_product is not None:
            check["relation_to_view_product"] = related.get(int(row["id"]))
        checks.append(check)
    return {
        "dataset": _name(found, role),
        "dataset_id": dataset_id,
        "checker_tables": checks,
        "profiles": await _profiles(dataset_id, role),
    }


async def _product_dq(product_id: str, role: str) -> dict[str, list[dict]] | None:
    """A product's data-quality lists as ``role`` reads them (REQ-2157); None for no product."""
    from provisa.api.admin.lineage_router import product_lineage_for
    from provisa.api.app import state

    found = await product_lineage_for(state, product_id, role)
    return None if found is None else found["dq"]


async def product(product_id: str, role: str) -> dict | None:
    """A data product's expectations, promises, related and unattached checks (REQ-2157) that
    ``role`` can see, each checker with its latest run; None when it can see no such product."""
    known = await _read(f"SELECT id FROM meta.data_products WHERE id = {_lit(product_id)}", role)
    if not known:
        return None
    lists = await _product_dq(product_id, role) or {}
    by_state: dict[str, list[int]] = {
        state: [int(e["checker_table_id"]) for e in lists.get(state, [])] for state in STATES
    }
    ids = sorted({i for found in by_state.values() for i in found})
    rows: dict[int, dict] = {}
    if ids:
        listed = ", ".join(str(i) for i in ids)
        rows = {
            int(r["id"]): r
            for r in _reached(
                await _read(
                    f"SELECT {_CHECKER_COLUMNS} FROM meta.registered_tables WHERE id IN ({listed})",
                    role,
                ),
                role,
            )
        }
    answer_: dict[str, Any] = {"product_id": product_id}
    for state in STATES:
        answer_[state] = [await _checker(rows[i], role) for i in by_state[state] if i in rows]
    return answer_
