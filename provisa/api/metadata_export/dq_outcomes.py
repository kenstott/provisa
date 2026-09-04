# Copyright (c) 2026 Kenneth Stott
# Canary: 7c40b2e1-8f36-4d59-a1b7-0e2c6d4915af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The most recent scan's outcomes, read back out of the results tables (REQ-1443).

A results table is scan history — ``scan_time`` is the watermark, so every scan appends
(:mod:`provisa.dq.results`). The latest execution is therefore the rows carrying the maximum
``scan_time``, and those rows are what a catalog means by an assertion's result: whether this
check passed the last time it ran, when, and what it measured.

The read goes through the one query pipeline (``_govern_and_route`` → ``_execute_plan``), like
every other governed read in the product — a results table is an ordinary governed table, and its
RLS, masking and row cap apply here exactly as they would to an analyst reading it.
"""

# Requirements: REQ-982, REQ-1003, REQ-1443

from __future__ import annotations

import logging

from datetime import datetime
from typing import Any

from provisa.api.metadata_export.model import DataQualityOutcome
from provisa.core.models import ProvisaConfig
from provisa.dq.contract import CHECKERS

logger = logging.getLogger(__name__)

# The "org_admin" role is the platform's well-known system execution role for governed internal SQL
# (REQ-1003 requires a role, and provisa/scheduler/jobs.py runs its statements under this one). A
# metadata publish carries no end-user identity, so it reads under that role.
_EXPORT_ROLE = "org_admin"

# What an assertion outcome needs. The rest of the envelope is diagnostic detail no catalog has a
# field for — it stays queryable in the results table, where it already is.
_OUTCOME_COLUMNS = (
    "target_table",
    "column_name",
    "check_type",
    "outcome",
    "scan_id",
    "scan_time",
    "metric_value",
    "failed_rows",
)

# (target table, column name, check type) — how a results row names the check it reports on, and
# the only identity the contract text also carries.
OutcomeKey = tuple[str, str, str]


def latest_scan_sql(schema_name: str, table_name: str) -> str:
    """Semantic SQL for the rows of the most recent scan in one results table.

    Compared against the table's own maximum rather than a time window: a contract last scanned a
    month ago still has a last known result, and a window would report it as never run.
    """
    relation = f"{schema_name}.{table_name}"
    columns = ", ".join(_OUTCOME_COLUMNS)
    return (
        f"SELECT {columns} FROM {relation} "
        f"WHERE scan_time = (SELECT MAX(scan_time) FROM {relation})"
    )


def index_outcomes(rows: list[dict]) -> dict[OutcomeKey, DataQualityOutcome]:
    """Scan rows keyed by the check identity an assertion can reproduce.

    A contract knows a check by its type and the column it names; the checker additionally assigns
    it a ``check_name``, which the contract text does not carry. Two checks of the same type on the
    same column are therefore indistinguishable from the export's side, so both are left out of the
    index rather than one being handed the other's verdict. They still publish — definition,
    severity and all — as never run, which is the only claim the export can make about them
    without guessing.
    """
    grouped: dict[OutcomeKey, list[dict]] = {}
    for row in rows:
        key = (row["target_table"], row["column_name"] or "", row["check_type"])
        grouped.setdefault(key, []).append(row)
    index: dict[OutcomeKey, DataQualityOutcome] = {}
    for key, matches in grouped.items():
        if len(matches) > 1:
            logger.info(
                "metadata export: %d checks of type %r on %s (%s) share an identity the contract "
                "cannot distinguish; publishing them with no last-run outcome",
                len(matches),
                key[2],
                key[0],
                key[1] or "table-level",
            )
            continue
        row = matches[0]
        scan_time = row["scan_time"]
        if not isinstance(scan_time, datetime):
            # The results schema declares scan_time a timestamp and the engine returns one; a
            # string here means the column drifted, and a catalog would publish an unorderable
            # "last run" that no consumer could compare against another scan.
            raise TypeError(
                f"results table scan_time for {key[0]} came back as {type(scan_time).__name__}, "
                f"not a datetime; the results schema declares it a timestamp"
            )
        index[key] = DataQualityOutcome(
            status=row["outcome"],
            scan_id=row["scan_id"],
            scan_time=scan_time,
            metric_value=row["metric_value"],
            failed_rows=row["failed_rows"],
        )
    return index


async def read_latest_outcomes(config: ProvisaConfig) -> dict[OutcomeKey, DataQualityOutcome]:
    """The last scan's verdicts for every registered contract in the config.

    A results table that has never been scanned returns no rows and contributes no keys, so its
    checks publish as never run — the true statement about a contract registered but not yet
    executed, and one a catalog should show rather than omit.
    """
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

    checker_ids = {source.id for source in config.sources if source.type.value in CHECKERS}
    rows: list[dict] = []
    for table in config.tables:
        if table.source_id not in checker_ids or not table.dq_contract:
            continue
        plan = await _govern_and_route(
            latest_scan_sql(table.schema_name, table.table_name), _EXPORT_ROLE
        )
        result = await _execute_plan(plan)
        rows.extend(_as_dicts(result))
    return index_outcomes(rows)


def _as_dicts(result: Any) -> list[dict]:
    """The plan result's rows as dicts keyed by column name.

    Names come off the result rather than from the SELECT order, because governance rewrites the
    projection between the two.
    """
    names = [str(name) for name in result.column_names]
    return [dict(zip(names, row, strict=True)) for row in result.rows]
