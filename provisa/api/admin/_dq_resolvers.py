# Copyright (c) 2026 Kenneth Stott
# Canary: 4f2e91c8-7b3a-4d16-8e05-2a9c6d47b1f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The contract builder panel's server side (REQ-1443 clause 7).

Three operations, and the dialect lives in none of them: parse raw contract text into editable
rows, serialize edited rows back into contract text, and run a contract against the live table
WITHOUT landing its rows. All three delegate to :mod:`provisa.dq.contract` and
:mod:`provisa.dq.runner`, so the browser never learns what a soda check or a GX expectation looks
like and the panel can never emit a shape the checker refuses to run.

The dry run is the clause's point: a contract whose dataset resolved somewhere else still scans
cleanly and still lands rows, so the only way to see that it aimed at the wrong table is to run it
and read back which table it says it observed. That is why the result carries the resolved target
alongside the outcomes.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.schema_org import registered_tables, sources, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection


class _GovernedTable:
    """A registered table addressed by name — what ``resolve_contract_target`` matches on."""

    __slots__ = ("schema_name", "table_name")

    def __init__(self, schema_name: str, table_name: str) -> None:
        self.schema_name = schema_name
        self.table_name = table_name


def parse_contract(checker: str, contract_text: str) -> dict:
    """The raw text as ``{dataset, checker, checks, error}``.

    A parse failure is a RESULT, not an exception: the raw editor is the source of truth and the
    operator types into it, so half-written text is the normal state of the field. The panel renders
    ``error`` beside the editor and keeps the text it was given rather than reverting it.
    """
    from provisa.dq.contract import ContractError, contract_checks, contract_dataset

    try:
        dataset = contract_dataset(contract_text, checker)
        checks = contract_checks(contract_text, checker)
    except ContractError as exc:
        return {"dataset": None, "checker": checker, "checks": [], "error": str(exc)}
    return {"dataset": dataset, "checker": checker, "checks": checks, "error": None}


def _kind_document(kind: Any) -> dict:
    """One :class:`~provisa.dq.catalog.CheckKind` as the picker renders it."""
    return {
        "check_type": kind.check_type,
        "scope": kind.scope,
        "comparators": list(kind.comparators),
        "metrics": list(kind.metrics),
        "levels": list(kind.levels),
        "threshold_units": list(kind.threshold_units),
        "params": [
            {
                "name": param.name,
                "value_type": param.value_type,
                "required": param.required,
                "choices": list(param.choices),
            }
            for param in kind.params
        ],
    }


async def check_catalog_for(conn: "Connection", *, checker: str, dataset: str) -> dict:
    """The checks the picker may offer, scoped to the columns of the contract's own dataset.

    The scoping input is the DATASET the contract names, not a table the panel was handed: the
    contract's dataset identifier is what the scan will actually observe, and resolving it here
    through :func:`~provisa.dq.contract.resolve_contract_target` — the same resolution the
    registration and the dry run use — means the picker offers checks against the columns the
    checker will really see. A dataset that resolves nowhere comes back as ``error`` with no
    columns, which is the same message the registration would fail with.
    """
    from provisa.dq.catalog import check_catalog, checks_for_column
    from provisa.dq.contract import ContractError, resolve_contract_target

    try:
        kinds = check_catalog(checker)
    except ContractError as exc:
        return {"error": str(exc), "dataset_checks": [], "columns": []}
    dataset_checks = [_kind_document(k) for k in kinds if k.scope == "dataset"]
    rows = (
        await conn.execute_core(
            select(
                registered_tables.c.id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
            )
        )
    ).fetchall()
    governed = [_GovernedTable(r._mapping["schema_name"], r._mapping["table_name"]) for r in rows]
    try:
        target = resolve_contract_target(dataset, governed)
    except ContractError as exc:
        return {"error": str(exc), "dataset_checks": dataset_checks, "columns": []}
    table_id = next(
        r._mapping["id"]
        for r in rows
        if r._mapping["schema_name"] == target.schema_name
        and r._mapping["table_name"] == target.table_name
    )
    columns = (
        await conn.execute_core(
            select(table_columns.c.column_name, table_columns.c.data_type)
            .where(table_columns.c.table_id == table_id)
            .order_by(table_columns.c.column_name)
        )
    ).fetchall()
    return {
        "error": None,
        "dataset_checks": dataset_checks,
        "columns": [
            {
                "name": c._mapping["column_name"],
                "data_type": c._mapping["data_type"],
                "checks": [
                    _kind_document(k)
                    for k in checks_for_column(checker, c._mapping["data_type"])
                ],
            }
            for c in columns
        ],
    }


def build_check(checker: str, check: dict) -> dict:
    """One check's ``definition`` text from the picker/threshold/severity editors.

    Separate from :func:`serialize_contract` because the panel edits one row at a time: the editors
    produce a check's arguments, this turns them into the dialect's text, and the assembled rows go
    through the serializer as usual. A rejected build is a RESULT for the same reason a parse
    failure is — the operator is mid-edit.
    """
    from provisa.dq.catalog import build_check_definition
    from provisa.dq.contract import ContractError

    try:
        definition = build_check_definition(
            checker,
            check["check_type"],
            column_name=check["column_name"] or None,
            params=json.loads(check["params"]) if check["params"] else None,
            comparator=check["comparator"] or None,
            threshold_value=check["threshold_value"],
            metric=check["metric"] or None,
            unit=check["unit"] or None,
            level=check["level"],
        )
    except (ContractError, ValueError) as exc:
        return {"definition": "", "error": str(exc)}
    return {"definition": definition, "error": None}


def serialize_contract(checker: str, dataset: str, checks: list[dict]) -> dict:
    """Edited rows back into contract text, in ``checker``'s dialect."""
    from provisa.dq.contract import ContractError, build_contract

    try:
        return {"text": build_contract(checker, dataset, checks), "error": None}
    except ContractError as exc:
        return {"text": "", "error": str(exc)}


async def dry_run_contract(conn: "Connection", *, source_id: str, contract_text: str) -> dict:
    """Run ``contract_text`` through ``source_id``'s checker and return the outcomes, landing none.

    The scan id is thrown away with the result. Nothing is written: the checker emits aggregate SQL
    against Provisa's pgwire endpoint and the rows :func:`~provisa.dq.runner.run_contract` builds go
    straight into the response, so a dry run costs one scan and changes nothing.
    """
    from provisa.dq.contract import ContractError, contract_dataset, resolve_contract_target
    from provisa.dq.registration import is_checker_source_type
    from provisa.dq.runner import CheckerError, run_contract

    fetched = (
        await conn.execute_core(
            select(sources.c.type, sources.c.mapping).where(sources.c.id == source_id)
        )
    ).fetchone()
    if fetched is None:
        return {"success": False, "message": f"no source {source_id!r}"}
    checker = str(fetched._mapping["type"])
    if not is_checker_source_type(checker):
        return {
            "success": False,
            "message": f"source {source_id!r} is a {checker} source, not a data-quality checker",
        }
    mapping = fetched._mapping["mapping"]
    rows = (
        await conn.execute_core(
            select(registered_tables.c.schema_name, registered_tables.c.table_name)
        )
    ).fetchall()
    governed = [_GovernedTable(r._mapping["schema_name"], r._mapping["table_name"]) for r in rows]
    try:
        dataset = contract_dataset(contract_text, checker)
        target = resolve_contract_target(dataset, governed)
    except ContractError as exc:
        return {"success": False, "message": str(exc)}
    try:
        results = await run_contract(
            checker=checker,
            contract_text=contract_text,
            connection={
                "host": mapping["host"],
                "port": mapping["port"],
                "database": mapping["database"],
                "user": mapping["user"],
                "password": mapping["password"],
            },
            data_source_name=dataset.split("/")[0],
            scan_id=str(uuid.uuid4()),
            scan_time=datetime.now(UTC),
            target_table=f"{target.schema_name}.{target.table_name}",
        )
    except CheckerError as exc:
        return {"success": False, "message": str(exc)}
    return {
        "success": True,
        "message": f"{len(results)} checks against {target.schema_name}.{target.table_name}",
        "checker_version": results[0]["checker_version"] if results else None,
        "checks": [_dry_run_check(row) for row in results],
    }


def _dry_run_check(row: dict) -> dict:
    """One results row as the panel shows it. ``diagnostics`` is the per-check-type jsonb block, so
    it travels as JSON text rather than as a typed field the panel would have to model."""
    diagnostics: Any = row["diagnostics"]
    return {
        "column_name": row["column_name"],
        "check_type": row["check_type"],
        "outcome": row["outcome"],
        "rows_tested": row["rows_tested"],
        "failed_rows": row["failed_rows"],
        "value": row["metric_value"],
        "diagnostics": None if diagnostics is None else json.dumps(diagnostics, sort_keys=True),
    }
