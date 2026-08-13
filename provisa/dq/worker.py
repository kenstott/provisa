# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7f2d95-6b18-4c30-8e5a-1d93bc07af62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The checker subprocess (REQ-1443). The ONLY module that imports a checker.

Run as ``python -m provisa.dq.worker <payload.json>`` by :func:`provisa.dq.runner.build_command`.
Reads the payload, drives the checker's Python API against Provisa's pgwire endpoint, and writes one
JSON envelope to stdout. Every checker import is inside a function, so importing this module — which
the parent never does — still costs nothing and pulls in no Elastic-Licence code.

Both checkers are driven through their Python API rather than their CLI. Soda v4's
``soda contract verify`` has no machine-readable output flag (its arguments are -c/-d/-ds/-sc/--set/
-r/-p/-cp/-cf/-dw/-mdw), so scraping its console table would be the only CLI route — and a
human-facing table is not an interface. GX has no result-emitting CLI at all.

stdout is the RESULT CHANNEL and nothing else. Both checkers log to stdout by default, so logging is
pointed at stderr before either is imported; the parent surfaces stderr verbatim when the child
fails.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

# Which diagnostic metric counts the FAILING rows, per soda check type. Explicit rather than
# pattern-matched on "*_count": several types carry more than one count (an invalidity check reports
# both invalid_count and missing_count) and picking by shape would silently report the wrong one.
# A type absent here counts no rows — a schema or freshness check measures something that is not a
# row tally, and a fabricated 0 would read as "nothing failed".
_SODA_FAILED_ROWS: dict[str, str] = {
    "missing": "missing_count",
    "invalid": "invalid_count",
    "duplicate": "duplicate_count",
    "failed_rows": "failed_rows_count",
}

# The rows-considered metric soda attaches to every check.
_SODA_ROWS_TESTED = "check_rows_tested"


def _configure_logging() -> None:
    """Send every log record to stderr, leaving stdout for the result envelope alone."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(logging.StreamHandler(stream=sys.stderr))
    root.setLevel(logging.WARNING)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: python -m provisa.dq.worker <payload.json>", file=sys.stderr)
        return 2
    _configure_logging()
    with open(argv[1], encoding="utf-8") as f:
        payload = json.load(f)
    checker = payload["checker"]
    if checker == "soda":
        envelope = run_soda(payload)
    elif checker == "great_expectations":
        envelope = run_gx(payload)
    else:
        print(f"unknown data-quality checker {checker!r}", file=sys.stderr)
        return 2
    json.dump(envelope, sys.stdout, default=str)
    return 0


# ── soda ────────────────────────────────────────────────────────────────────────────────────────


def _soda_data_source_yaml(name: str, connection: dict) -> str:
    """The soda data-source document for Provisa's pgwire endpoint.

    Built here rather than authored by the operator: the endpoint is Provisa's own, so its address
    is already known from the source registration, and a second hand-written copy could disagree
    with it. ``type: postgres`` because pgwire IS the postgres wire protocol — that is the whole
    reason one driver reaches every federated backend.
    """
    import yaml

    document = {
        "type": "postgres",
        "name": name,
        "connection": {
            "host": connection["host"],
            "port": connection["port"],
            "database": connection["database"],
            "user": connection["user"],
            "password": connection["password"],
            "sslmode": connection.get("sslmode", "prefer"),
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def run_soda(payload: dict) -> dict:
    from importlib.metadata import version

    from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
    from soda_core.contracts.contract_verification import (
        CheckCollectionStatus,
        ContractVerificationSession,
    )

    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(payload["contract_text"])],
        data_source_yaml_sources=[
            DataSourceYamlSource.from_str(
                _soda_data_source_yaml(payload["data_source_name"], payload["connection"])
            )
        ],
    )
    checks: list[dict] = []
    for result in session_result.contract_verification_results:
        if result.status is CheckCollectionStatus.ERROR:
            # The contract never ran — a connection failure, an unresolvable dataset, a malformed
            # check. Raising means a non-zero exit and the parent's CheckerError, so nothing lands.
            # Landing "error" rows for a scan that never happened would put a fabricated
            # observation into an append-only history that no later scan can retract.
            raise RuntimeError(f"soda contract verification failed: {result.get_errors_str()}")
        for check_result in result.check_results:
            checks.append(_soda_check_row(check_result))
    return {
        "checker": "soda",
        "checker_version": version("soda-core"),
        "checks": checks,
    }


def _soda_check_row(check_result: Any) -> dict:
    check = check_result.check
    diagnostics = dict(check_result.diagnostic_metric_values or {})
    # FreshnessCheckResult carries the observed maximum timestamp as an attribute rather than a
    # diagnostic metric (the metric dict is typed float). Fold it in so the shipped
    # freshness_max_timestamp promotion (REQ-119) has a field to promote.
    max_timestamp = getattr(check_result, "max_timestamp", None)
    if max_timestamp is not None:
        diagnostics["max_timestamp"] = max_timestamp.isoformat()
    failed_rows_key = _SODA_FAILED_ROWS.get(check.type)
    return {
        "column_name": check.column_name or "",
        "check_name": check.name,
        "check_type": check.type,
        "check_definition": check.definition,
        "outcome": check_result.outcome.name,
        "threshold": str(check.threshold) if check.threshold is not None else None,
        "metric_value": check_result.threshold_value,
        "rows_tested": diagnostics.get(_SODA_ROWS_TESTED),
        "failed_rows": diagnostics.get(failed_rows_key) if failed_rows_key else None,
        "diagnostics": diagnostics,
    }


# ── great expectations ──────────────────────────────────────────────────────────────────────────


def run_gx(payload: dict) -> dict:
    from importlib.metadata import version

    import great_expectations as gx
    import yaml
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    suite_document = yaml.safe_load(payload["contract_text"])
    _, schema, table_name = suite_document["meta"]["dataset"].split("/")
    connection = payload["connection"]
    connection_string = (
        f"postgresql+psycopg2://{connection['user']}:{connection['password']}"
        f"@{connection['host']}:{connection['port']}/{connection['database']}"
    )
    # Ephemeral: the whole GX project lives in this process and dies with it. GX's file-backed
    # context would put a second, drifting copy of the suite on disk next to the contract text that
    # is already the single definition.
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_postgres(
        name=payload["data_source_name"], connection_string=connection_string
    )
    asset = data_source.add_table_asset(
        name=table_name, table_name=table_name, schema_name=schema
    )
    batch_definition = asset.add_batch_definition_whole_table("provisa_scan")
    suite = context.suites.add(
        ExpectationSuite(
            name=suite_document.get("name") or f"{schema}.{table_name}",
            expectations=suite_document["expectations"],
        )
    )
    validation = context.validation_definitions.add(
        ValidationDefinition(name="provisa_scan", data=batch_definition, suite=suite)
    )
    # SUMMARY carries element_count / unexpected_count / unexpected_percent — the counts the shipped
    # schema reports — plus a bounded sample of failing values. COMPLETE would return every failing
    # value, which is the unbounded pull the out-of-process design exists to avoid; partial_unexpected_count
    # is where the payload's sampler_limit lands (soda has no local equivalent — see runner.py).
    result = validation.run(
        result_format={
            "result_format": "SUMMARY",
            "partial_unexpected_count": payload["sampler_limit"],
        }
    )
    return {
        "checker": "great_expectations",
        "checker_version": version("great_expectations"),
        "checks": [_gx_check_row(r) for r in result.results],
    }


def _gx_check_row(validation_result: Any) -> dict:
    config = validation_result.expectation_config
    kwargs = dict(config.kwargs or {})
    # GX injects the runtime batch_id into every expectation's kwargs. It identifies THIS run's
    # batch, not the check, so it is dropped: check_definition is what the operator authored, and a
    # per-run id in it would make the same check look different on every scan.
    kwargs.pop("batch_id", None)
    column_name = kwargs.get("column") or ""
    exception_info = validation_result.exception_info or {}
    raised = any(
        info.get("raised_exception")
        for info in exception_info.values()
        if isinstance(info, dict)
    )
    if raised:
        outcome = "NOT_EVALUATED"
    else:
        outcome = "PASSED" if validation_result.success else "FAILED"
    diagnostics = dict(validation_result.result or {})
    # GX has no warn level: an expectation succeeds or it does not. ``mostly`` is the closest thing
    # to a declared threshold, so it is reported as one when present and left null when absent —
    # rather than inventing "100%" for an expectation that never named a tolerance.
    mostly = kwargs.get("mostly")
    return {
        "column_name": column_name,
        "check_name": f"{config.type}({column_name})" if column_name else config.type,
        "check_type": config.type,
        "check_definition": json.dumps({"type": config.type, "kwargs": kwargs}, sort_keys=True),
        "outcome": outcome,
        "threshold": f"mostly: {mostly}" if mostly is not None else None,
        "metric_value": diagnostics.get("unexpected_percent"),
        "rows_tested": diagnostics.get("element_count"),
        "failed_rows": diagnostics.get("unexpected_count"),
        "diagnostics": diagnostics,
    }


if __name__ == "__main__":
    sys.exit(main(sys.argv))
