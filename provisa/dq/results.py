# Copyright (c) 2026 Kenneth Stott
# Canary: 96980f5b-9433-4275-993a-a8e7ade5e011
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The FIXED, SHIPPED data-quality results schema (REQ-1443, decision 2).

Unlike a Kafka topic — whose payload only the operator knows, so its columns are hand-declared — the
scan envelope is defined by the CHECKER. Both supported checkers report the same envelope: when a
scan ran, what dataset it scanned, which check, how the check was defined, the outcome, and the
measured value. So the schema ships and the operator never writes it; a table registered on a checker
source has its columns REPLACED by this list at config load.

Everything check-type-specific (a schema check's missing column names, a freshness check's
max_column_timestamp) is heterogeneous by construction and lands in the single ``diagnostics`` jsonb
column. Surfacing one of its fields as a typed column is the existing REQ-119 promotion mechanism,
not a new one — ``DQ_PROMOTIONS`` is the shipped default set.

``scan_time`` is the watermark: probe_type=watermark implies APPEND (REQ-982), so scan history
accumulates with no history subsystem.
"""

from __future__ import annotations

from provisa.core.models import Column

# The watermark column — the scan instant. Declaring it makes the landing an append (REQ-982), which
# is what turns a results table into a scan HISTORY without any history machinery.
DQ_WATERMARK_COLUMN = "scan_time"

# (name, data_type, description) — the shipped envelope, in display order.
_ENVELOPE: tuple[tuple[str, str, str], ...] = (
    ("scan_id", "varchar", "Identifier of the scan run that produced this row."),
    (DQ_WATERMARK_COLUMN, "timestamp", "When the scan ran. The watermark: landing is append-only."),
    ("checker", "varchar", "Which checker produced the row (soda | great_expectations)."),
    ("checker_version", "varchar", "The checker's reported version, as run."),
    ("dataset", "varchar", "The dataset the contract names, verbatim from the contract."),
    ("target_table", "varchar", "The governed Provisa table the dataset resolved to."),
    ("column_name", "varchar", "The column the check applies to; empty for table-level checks."),
    ("check_name", "varchar", "The check's identity within the contract."),
    (
        "check_type",
        "varchar",
        "The check's kind (missing, invalid, duplicate, freshness, schema…).",
    ),
    (
        "check_definition",
        "varchar",
        "The check as declared, so a result is readable without the contract.",
    ),
    (
        "outcome",
        "varchar",
        "pass | fail | warn | error | skipped — an observation, never a verdict.",
    ),
    ("metric_value", "double", "The measured value, when the check produced a numeric metric."),
    ("threshold", "varchar", "The declared threshold the value was compared against."),
    ("rows_tested", "bigint", "How many rows the check considered."),
    ("failed_rows", "bigint", "How many rows failed the check."),
    (
        "diagnostics",
        "jsonb",
        "Check-type-specific detail. Promote a field to a typed column (REQ-119).",
    ),
)

# REQ-119 promotions shipped with the schema: the two diagnostics fields that are worth a typed
# column on every install. An operator adds more the same way they would on any other jsonb column.
# Both name a key the runner's normalisation actually writes — ``max_timestamp`` only for a soda
# freshness check (``FreshnessCheckResult.max_timestamp``), ``dataset_rows_tested`` on every soda
# check. A promotion of a key nothing emits would be a column that is null forever.
DQ_PROMOTIONS: list[dict] = [
    {
        "jsonb_column": "diagnostics",
        "field": "max_timestamp",
        "target_column": "freshness_max_timestamp",
        "target_type": "timestamp",
    },
    {
        "jsonb_column": "diagnostics",
        "field": "dataset_rows_tested",
        "target_column": "dataset_rows_tested",
        "target_type": "bigint",
    },
]

# The result envelope's field names, for the runner's normalisation and for tests that assert the
# schema and the emitted rows cannot drift apart.
RESULT_FIELDS: tuple[str, ...] = tuple(name for name, _, _ in _ENVELOPE)


def results_columns(visible_to: list[str]) -> list[Column]:
    """The shipped results schema as :class:`Column` models.

    ``visible_to`` is the role list the registration grants — governance is the ordinary table
    governance, so the caller supplies it rather than this module inventing a default.
    """
    return [
        Column(name=name, data_type=data_type, visible_to=list(visible_to), description=description)
        for name, data_type, description in _ENVELOPE
    ]
