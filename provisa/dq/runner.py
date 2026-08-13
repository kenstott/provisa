# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4e17b6-2c85-4a03-b7e1-58f9c0a6d233
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Out-of-process checker invocation and result normalisation (REQ-1443).

Provisa never imports a checker. The scan runs in a child interpreter (:mod:`provisa.dq.worker`),
which is the only place ``soda_core`` / ``great_expectations`` is loaded — so an Elastic-Licence
component is never linked into the server process, a checker's own dependency pins can never
constrain Provisa's, and a checker crash kills a subprocess rather than the event loop.

The child speaks one neutral JSON envelope on stdout, deliberately close to the checker's own
vocabulary (``PASSED``, ``diagnostic_metric_values``) and NOT to Provisa's schema. Mapping that
envelope onto the shipped result columns is :func:`parse_results`, which lives on this side of the
boundary so it is testable with neither checker installed.

The checker connects back to Provisa's OWN pgwire endpoint, so it scans the FEDERATED view of the
governed table. That is what makes one postgres driver enough to check a Snowflake- or
Iceberg-backed table, and it is why the scan is a pushdown: the checker emits aggregate SQL against
that endpoint and reads back a row of scalars.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime
from typing import Any

from provisa.dq.contract import CHECKERS, contract_dataset
from provisa.dq.results import RESULT_FIELDS

# Seconds before an unresponsive checker subprocess is killed. A scan is aggregate SQL against one
# dataset, so this is generous; a checker still running here is wedged, not slow, and the poll node
# must not be held by it forever.
DEFAULT_TIMEOUT = 600.0

# How many failing values a checker may sample into ``diagnostics``. The scan itself never streams
# the dataset into Python — it is aggregate SQL — and this keeps the EVIDENCE bounded to match.
#
# It reaches only Great Expectations, as its result_format partial_unexpected_count. Soda collects no
# failing-row samples at all without Soda Cloud: its sampler limit is read from the CLOUD dataset
# configuration (soda_core/check_collections/base.py sets it from
# dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit), so a Cloud-less run
# has sampling disabled outright. That bound is structural, not a setting this can raise.
DEFAULT_SAMPLER_LIMIT = 100

# The checker's own outcome vocabulary → the shipped ``outcome`` column's. Soda's five-value
# CheckOutcome plus the two booleans GX reports. EXCLUDED is a check the operator's own selector
# left out, which is neither a failure nor an error, so it lands as ``skipped`` rather than being
# dropped: a check that was authored and then not run is exactly what an operator needs to see.
_OUTCOMES: dict[str, str] = {
    "PASSED": "pass",
    "FAILED": "fail",
    "WARN": "warn",
    "NOT_EVALUATED": "error",
    "EXCLUDED": "skipped",
}


class CheckerError(Exception):
    """The checker subprocess could not run the contract, or did not report a usable result."""


def build_command(payload_path: str) -> list[str]:
    """The argv that runs one contract in a child interpreter.

    ``sys.executable`` — the SAME interpreter, deliberately. The checker is a pyproject extra
    installed into this environment (``pip install .[soda]``), so the child needs no venv of its
    own; the isolation being bought here is import isolation, not environment isolation. The payload
    travels as a file rather than argv because it carries the contract text and the endpoint
    password, neither of which belongs in a process listing.
    """
    return [sys.executable, "-m", "provisa.dq.worker", payload_path]


async def run_contract(
    *,
    checker: str,
    contract_text: str,
    connection: dict,
    data_source_name: str,
    scan_id: str,
    scan_time: datetime,
    target_table: str,
    sampler_limit: int = DEFAULT_SAMPLER_LIMIT,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict]:
    """Verify one contract and return the rows to land, in the shipped results schema.

    ``connection`` is the pgwire endpoint the checker aims at (host/port/user/password/database),
    read from the checker source's ``mapping`` (REQ-251). ``data_source_name`` is the leading part
    of the contract's dataset identifier — the checker matches its data source to the contract by
    that name, so a mismatch is a contract error and is reported as one rather than corrected here.
    """
    if checker not in CHECKERS:
        raise CheckerError(
            f"unknown data-quality checker {checker!r}; expected one of {sorted(CHECKERS)}"
        )
    dataset = contract_dataset(contract_text, checker)
    declared_source = dataset.split("/")[0]
    if declared_source != data_source_name:
        raise CheckerError(
            f"contract dataset {dataset!r} names data source {declared_source!r} but the source is "
            f"configured as {data_source_name!r}; the contract and the endpoint must agree"
        )
    payload = {
        "checker": checker,
        "contract_text": contract_text,
        "connection": connection,
        "data_source_name": data_source_name,
        "sampler_limit": sampler_limit,
    }
    envelope = await _run_worker(payload, timeout=timeout)
    return parse_results(
        envelope,
        scan_id=scan_id,
        scan_time=scan_time,
        dataset=dataset,
        target_table=target_table,
    )


async def _run_worker(payload: dict, *, timeout: float) -> dict:
    """Run the worker on a temp payload file and return its parsed stdout envelope."""
    import tempfile
    from pathlib import Path

    tmp_dir = tempfile.mkdtemp(prefix="provisa-dq-")
    payload_path = Path(tmp_dir) / "payload.json"
    try:
        # 0o600 before the secret is written, not after — the endpoint password is in this file.
        payload_path.touch(mode=0o600)
        payload_path.write_text(json.dumps(payload), encoding="utf-8")
        proc = await asyncio.create_subprocess_exec(
            *build_command(str(payload_path)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise CheckerError(
                f"checker {payload['checker']!r} did not finish within {timeout}s"
            ) from None
    finally:
        payload_path.unlink(missing_ok=True)
        Path(tmp_dir).rmdir()
    if proc.returncode != 0:
        raise CheckerError(
            f"checker {payload['checker']!r} exited {proc.returncode}: "
            f"{stderr.decode('utf-8', 'replace').strip()}"
        )
    try:
        envelope = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CheckerError(
            f"checker {payload['checker']!r} did not emit a JSON result envelope: {exc}"
        ) from exc
    if not isinstance(envelope, dict) or "checks" not in envelope:
        raise CheckerError(
            f"checker {payload['checker']!r} result envelope is missing its 'checks' list"
        )
    return envelope


def parse_results(
    envelope: dict,
    *,
    scan_id: str,
    scan_time: datetime,
    dataset: str,
    target_table: str,
) -> list[dict]:
    """The worker's envelope as rows in the shipped results schema.

    Every returned row has exactly the ``RESULT_FIELDS`` keys, in order — the landing path writes
    the table's declared columns, so a row that gained or lost a key would be a silent schema drift.
    """
    checker = envelope.get("checker")
    checker_version = envelope.get("checker_version")
    if not isinstance(checker, str) or not isinstance(checker_version, str):
        raise CheckerError("checker result envelope is missing 'checker'/'checker_version'")
    rows: list[dict] = []
    for check in envelope["checks"]:
        raw_outcome = check.get("outcome")
        outcome = _OUTCOMES.get(raw_outcome)
        if outcome is None:
            raise CheckerError(
                f"checker {checker!r} reported unknown outcome {raw_outcome!r}; expected one of "
                f"{sorted(_OUTCOMES)}"
            )
        row = {
            "scan_id": scan_id,
            "scan_time": scan_time,
            "checker": checker,
            "checker_version": checker_version,
            "dataset": dataset,
            "target_table": target_table,
            "column_name": check["column_name"],
            "check_name": check["check_name"],
            "check_type": check["check_type"],
            "check_definition": check["check_definition"],
            "outcome": outcome,
            "metric_value": _as_float(check["metric_value"]),
            "threshold": check["threshold"],
            "rows_tested": _as_int(check["rows_tested"]),
            "failed_rows": _as_int(check["failed_rows"]),
            "diagnostics": check["diagnostics"],
        }
        assert tuple(row) == RESULT_FIELDS, "result row drifted from the shipped schema"
        rows.append(row)
    return rows


def _as_float(value: Any) -> float | None:
    """A numeric metric as a float. None stays None — a check with no numeric metric (schema,
    freshness against a missing timestamp) genuinely measured nothing, and 0.0 would read as a
    measurement that was taken."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CheckerError(f"expected a numeric metric value, got {value!r}")
    return float(value)


def _as_int(value: Any) -> int | None:
    """A row count as an int. None stays None, for the same reason as :func:`_as_float`."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CheckerError(f"expected a numeric row count, got {value!r}")
    return int(value)
