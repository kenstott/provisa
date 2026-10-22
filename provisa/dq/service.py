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
  a product, each checker also says how it relates to that product: promise, expectation or
  partner DQ.
- A product's answer: its promises, expectations, partner DQ and orphans (REQ-1443, derived at
  read time by ``meta.product_checks``), each checker with its latest run.
"""

# Requirements: REQ-2133, REQ-1443, REQ-1934

from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote

from provisa.compiler.sql_literals import sql_literal


def _lit(value: str) -> str:
    """``value`` as a literal of the semantic SQL the governed pipeline reads (Postgres)."""
    return sql_literal(value, "postgres")


#: How a product shows a checker table (REQ-1443), in the order an answer lists them.
STATES = ("promise", "expectation", "partner_dq", "orphan")

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


def _relation(row: dict) -> str:
    """A registered table's name as the role's SQL reads it: ``<schema>.<table>``, as the metadata
    export reads a checker's results (provisa.api.metadata_export.dq_outcomes)."""
    return f"{row['schema_name']}.{row['table_name']}"


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
    relation = _relation(checker)
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
        "checker_table": _relation(row),
        "checker_table_id": row["id"],
        "description": row["description"],
        "contract": row["dq_contract"],
        "assigned_product_id": row["product_id"],
        "latest_run": await _latest_run(row, role),
    }


async def _profiles(dataset_id: int, role: str) -> list[dict]:
    """The profile result tables registered for the dataset (REQ-1934), each with its latest run's
    rows: every run is kept, keyed by run_id with run_time as the watermark."""
    tables = await _read(
        "SELECT id, domain_id, schema_name, table_name, alias FROM meta.registered_tables "
        f"WHERE profiled_table_id = {dataset_id} ORDER BY table_name",
        role,
    )
    out = []
    for table in tables:
        relation = _relation(table)
        rows = await _read(
            f"SELECT * FROM {relation} WHERE run_time = (SELECT MAX(run_time) FROM {relation})",
            role,
        )
        out.append(
            {
                "profile_table": relation,
                "profile_table_id": table["id"],
                "latest_run": rows or None,
            }
        )
    return out


async def dataset(name: str, role: str, view_product: str | None = None) -> dict | None:
    """Everything about the data quality of the dataset ``name`` (``domain.table`` or a table id)
    that ``role`` can see; None when it can see no such dataset."""
    found = await _dataset_row(name, role)
    if found is None:
        return None
    dataset_id = int(found["id"])
    checkers = await _read(
        f"SELECT {_CHECKER_COLUMNS} FROM meta.registered_tables "
        f"WHERE checked_table_id = {dataset_id} ORDER BY table_name",
        role,
    )
    related: dict[int, str] = {}
    if view_product is not None:
        related = {
            int(r["checker_table_id"]): r["state"]
            for r in await _read(
                "SELECT checker_table_id, state FROM meta.product_checks "
                f"WHERE product_id = {_lit(view_product)} "
                f"AND checked_table_id = {dataset_id}",
                role,
            )
        }
    checks = []
    for row in checkers:
        check = await _checker(row, role)
        if view_product is not None:
            check["relation_to_view_product"] = related.get(int(row["id"]))
        checks.append(check)
    return {
        "dataset": _relation(found),
        "dataset_id": dataset_id,
        "checker_tables": checks,
        "profiles": await _profiles(dataset_id, role),
    }


async def product(product_id: str, role: str) -> dict | None:
    """A data product's promises, expectations, partner DQ and orphans (REQ-1443) that ``role`` can
    see, each checker with its latest run; None when it can see no such product."""
    known = await _read(f"SELECT id FROM meta.data_products WHERE id = {_lit(product_id)}", role)
    if not known:
        return None
    states = await _read(
        "SELECT checker_table_id, state FROM meta.product_checks "
        f"WHERE product_id = {_lit(product_id)}",
        role,
    )
    by_state: dict[str, list[int]] = {state: [] for state in STATES}
    for row in states:
        by_state[row["state"]].append(int(row["checker_table_id"]))
    ids = sorted({i for found in by_state.values() for i in found})
    rows: dict[int, dict] = {}
    if ids:
        listed = ", ".join(str(i) for i in ids)
        rows = {
            int(r["id"]): r
            for r in await _read(
                f"SELECT {_CHECKER_COLUMNS} FROM meta.registered_tables WHERE id IN ({listed})",
                role,
            )
        }
    answer_: dict[str, Any] = {"product_id": product_id}
    for state in STATES:
        answer_[state] = [await _checker(rows[i], role) for i in by_state[state] if i in rows]
    return answer_
