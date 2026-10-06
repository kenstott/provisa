# Copyright (c) 2026 Kenneth Stott
# Canary: 5a9d2e60-b7f4-4c18-83e1-0c6f4b2a9d75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Checks a profile hands to a data-quality checker (REQ-1934, CONSTRAINTS and EXCEPTIONS COME FROM
CHECKERS; REQ-1443).

The profiler raises no exceptions itself. Three things become checks of a Soda or Great Expectations
checker source -- its contract gains one check, written by the one contract builder
(``provisa.dq.catalog`` / ``provisa.dq.contract``), and the checker's own schedule, history and
notifications apply:

* an accepted constraint, added to a checker whose contract scans the profiled table;
* the drift check -- the latest run has no drifting measure -- added to a checker whose contract
  scans the member's registered drift table;
* an expectation check -- the latest run's measures lie within the low and high of the expectation
  whose period holds the run -- for a registered table of :data:`EXPECTATION_SHAPE`, added to a
  checker scanning the registered drift table, whose rows hold every measure of every run.

Each is refused by name where no such checker exists.
"""

# Requirements: REQ-1934, REQ-1443

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, update

from provisa.compiler.sql_literals import sql_literal
from provisa.dq.catalog import build_check_definition
from provisa.dq.contract import (
    CHECKERS,
    ContractError,
    build_contract,
    contract_checks,
    contract_dataset,
    dataset_parts,
    resolve_contract_target,
)
from provisa.profiler.constraints import Constraint

# REQ-1934 EXTERNAL EXPECTATIONS: the published shape of an expectations table. ``measure`` names
# any measure of a run (a drift row's measure), ``column`` the column it is of (empty for a table
# measure); the expectation holds for runs whose time lies in [period_start, period_end).
EXPECTATION_SHAPE: tuple[str, ...] = (
    "measure",
    "column",
    "period_start",
    "period_end",
    "low",
    "expected",
    "high",
    "source",
)


class ExportRefused(Exception):
    """No checker can take the check, or what was named cannot become one; says which."""


@dataclass(frozen=True)
class CheckerTable:
    id: int
    table_name: str
    source_id: str
    checker: str  # soda | great_expectations
    dataset: str  # <data source>/<schema>/<table>, as its contract names it
    contract: str


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def checkers_scanning(conn: Any, contexts: dict, table_ids: set[int]) -> list[CheckerTable]:
    """The checker tables whose contract scans one of ``table_ids``."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import sources

    result = await conn.execute_core(
        select(rt.c.id, rt.c.table_name, rt.c.source_id, rt.c.dq_contract, sources.c.type)
        .join(sources, sources.c.id == rt.c.source_id)
        .where(rt.c.dq_contract.is_not(None), sources.c.type.in_(sorted(CHECKERS)))
        .order_by(rt.c.id)
    )
    out = []
    for tid, name, source_id, text, checker in result.fetchall():
        dataset = contract_dataset(text, checker)
        try:
            target = resolve_contract_target(dataset, contexts)
        except ContractError:
            continue  # its contract names no governed table, so it scans none of these
        if target.table_id in table_ids:
            out.append(CheckerTable(tid, name, source_id, checker, dataset, text))
    return out


def pick(found: list[CheckerTable], checker_table_id: int | None, what: str) -> CheckerTable:
    """The checker table to add to: the one named, or the only one; refused by name otherwise."""
    if not found:
        raise ExportRefused(
            f"no Soda or Great Expectations checker source scans {what}: register a checker "
            f"table whose contract's dataset is {what} first"
        )
    if checker_table_id is None:
        if len(found) > 1:
            names = ", ".join(f"{c.table_name} (id {c.id})" for c in found)
            raise ExportRefused(f"several checker tables scan {what} ({names}); pick one")
        return found[0]
    for c in found:
        if c.id == checker_table_id:
            return c
    raise ExportRefused(f"checker table {checker_table_id} does not scan {what}")


def _row(column: str, check_type: str, definition: str) -> dict:
    return {"column_name": column, "check_type": check_type, "definition": definition}


def _soda_sql(check_type: str, **params: str) -> dict:
    """A Soda SQL check. Soda tells two checks of one type in a contract apart by their qualifier,
    and refuses a contract holding two with none ("Duplicate identity"), so each carries one."""
    return _row("", check_type, build_check_definition("soda", check_type, params=params))


def _gx(check_type: str, column: str | None = None, **params: Any) -> dict:
    definition = build_check_definition(
        "great_expectations", check_type, column_name=column, params=params
    )
    return _row(column or "", check_type, definition)


def _temporal_breach(column: str, d: dict) -> str:
    c = _ident(column)
    low = sql_literal(str(d["min_text"]), "postgres")
    high = sql_literal(str(d["max_text"]), "postgres")
    return f"{c} < CAST({low} AS TIMESTAMP) OR {c} > CAST({high} AS TIMESTAMP)"


def constraint_check(c: Constraint, checker: str) -> dict:
    """An accepted constraint as one check row of ``checker``'s contract."""
    d = c.definition
    temporal = c.kind == "range" and d.get("family") == "temporal"
    if checker == "soda":
        if c.kind == "not_null":
            return _row(
                c.column, "missing", build_check_definition("soda", "missing", column_name=c.column)
            )
        if c.kind == "unique":
            return _row(
                c.column,
                "duplicate",
                build_check_definition("soda", "duplicate", column_name=c.column),
            )
        if c.kind == "value_set":
            definition = build_check_definition(
                "soda", "invalid", column_name=c.column, params={"valid_values": list(d["values"])}
            )
            return _row(c.column, "invalid", definition)
        if c.kind == "range" and not temporal:
            definition = build_check_definition(
                "soda",
                "invalid",
                column_name=c.column,
                params={"valid_min": d["min"], "valid_max": d["max"]},
            )
            return _row(c.column, "invalid", definition)
        if temporal:
            return _soda_sql(
                "failed_rows",
                qualifier=f"range_{c.column}",
                expression=_temporal_breach(c.column, d),
            )
        assert c.other is not None
        return _soda_sql(
            "failed_rows",
            qualifier=f"ordering_{c.column}_{c.other}",
            expression=f"{_ident(c.column)} > {_ident(c.other)}",
        )
    if c.kind == "not_null":
        return _gx("expect_column_values_to_not_be_null", c.column)
    if c.kind == "unique":
        return _gx("expect_column_values_to_be_unique", c.column)
    if c.kind == "value_set":
        return _gx("expect_column_values_to_be_in_set", c.column, value_set=list(d["values"]))
    if c.kind == "range" and not temporal:
        return _gx(
            "expect_column_values_to_be_between", c.column, min_value=d["min"], max_value=d["max"]
        )
    if temporal:
        query = f"SELECT * FROM {{batch}} WHERE {_temporal_breach(c.column, d)}"
        return _gx("unexpected_rows_expectation", unexpected_rows_query=query)
    assert c.other is not None
    # column <= other: other is greater than column, or equal.
    return _gx(
        "expect_column_pair_values_a_to_be_greater_than_b",
        column_A=c.other,
        column_B=c.column,
        or_equal=True,
    )


def _relation(checker: CheckerTable) -> str:
    """The table the checker scans, as SQL; GX's own placeholder for its batch."""
    if checker.checker == "great_expectations":
        return "{batch}"
    _, schema, table = dataset_parts(checker.dataset)
    return f"{_ident(schema)}.{_ident(table)}"


def _latest(drift: str) -> str:
    return f"run_time = (SELECT MAX(run_time) FROM {drift})"


def drift_check(checker: CheckerTable) -> dict:
    """The latest run has no drifting measure, over the registered drift table it scans."""
    drift = _relation(checker)
    query = f"SELECT * FROM {drift} WHERE drifting = TRUE AND {_latest(drift)}"
    if checker.checker == "soda":
        return _soda_sql("failed_rows", qualifier="drift", query=query)
    return _gx("unexpected_rows_expectation", unexpected_rows_query=query)


def expectation_check(checker: CheckerTable, expectations: str) -> dict:
    """The latest run's measures lie within [low, high] of the expectation, in the registered table
    ``expectations`` (domain.table as published), whose period holds the run."""
    drift = _relation(checker)
    schema, _, table = expectations.partition(".")
    e = f"{_ident(schema)}.{_ident(table)}"
    query = (
        f"SELECT d.* FROM {drift} d JOIN {e} e ON e.{_ident('measure')} = d.measure "
        f"AND (e.{_ident('column')} = d.column_name "
        f"OR (e.{_ident('column')} IS NULL AND d.column_name IS NULL)) "
        f"AND d.run_time >= e.{_ident('period_start')} AND d.run_time < e.{_ident('period_end')} "
        f"WHERE d.{_latest(drift)} "
        f"AND (d.{_ident('current')} < e.{_ident('low')} OR d.{_ident('current')} > e.{_ident('high')})"
    )
    if checker.checker == "soda":
        return _soda_sql("failed_rows", qualifier=f"expectation_{schema}_{table}", query=query)
    return _gx("unexpected_rows_expectation", unexpected_rows_query=query)


async def add_check(conn: Any, checker: CheckerTable, check: dict) -> bool:
    """Add ``check`` to the checker table's contract, rebuilt by the one contract builder. False
    when the contract already holds that check."""
    from provisa.core.schema_org import registered_tables as rt

    checks = contract_checks(checker.contract, checker.checker)
    key = (check["column_name"], check["check_type"], check["definition"])
    if any((c["column_name"], c["check_type"], c["definition"]) == key for c in checks):
        return False
    text = build_contract(checker.checker, checker.dataset, [*checks, check])
    await conn.execute_core(update(rt).where(rt.c.id == checker.id).values(dq_contract=text))
    return True


def is_expectations_shape(columns: list[str]) -> list[str]:
    """The fields of :data:`EXPECTATION_SHAPE` ``columns`` lacks; empty when it is of the shape."""
    return [f for f in EXPECTATION_SHAPE if f not in columns]
