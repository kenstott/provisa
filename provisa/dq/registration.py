# Copyright (c) 2026 Kenneth Stott
# Canary: 2fd7a163-04c9-4b8e-9a25-c3b70e1d8f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Turning a checker table's CONTRACT into its registration (REQ-1443).

A checker table is defined by one authored artifact and a governance intent; its columns, its
watermark and its promotions are all derived from the checker's fixed envelope. This module owns that
derivation, and both registration paths call it — YAML config load
(``config_loader._validate_dq_contracts``) and the admin registerTable/updateTable mutation. One
implementation because a table registered through the UI must come out identical to the same table
written in YAML; two would let the surfaces disagree about what a checker table IS.
"""

from __future__ import annotations

from typing import Any

from provisa.dq.contract import CHECKERS, ContractError, contract_dataset, resolve_contract_target
from provisa.dq.results import DQ_PROMOTIONS, DQ_WATERMARK_COLUMN, results_columns


def is_checker_source_type(source_type: Any) -> bool:
    """Whether ``source_type`` (a SourceType or its string value) is a data-quality checker."""
    return str(getattr(source_type, "value", source_type)) in CHECKERS


def derive_checker_table(table: Any, source_type: Any, tables: list) -> Any:
    """Validate a checker table's contract and derive its registration IN PLACE. Returns the target.

      * the contract must parse and must name a three-part dataset;
      * that dataset must resolve to a governed table — a checker may only observe what Provisa
        governs (REQ-967), and the resolved target is the table's lineage (REQ-939);
      * the columns become :func:`results_columns` — the envelope is the CHECKER's, not the
        operator's, so a hand-written column list could only ever disagree with what lands;
      * ``scan_time`` becomes the watermark, which makes the landing an append (REQ-982) and the
        table a scan history with no history subsystem;
      * ``DQ_PROMOTIONS`` seeds the REQ-119 promotions, appended to any the operator added.

    Raises :class:`ValueError` naming the table on any of those. The declared columns are read ONLY
    for their ``visible_to`` and are then replaced; ``visible_to`` must be unanimous, because one
    results row cannot be visible to different roles column by column when every column comes out of
    the same scan.
    """
    checker = str(getattr(source_type, "value", source_type))
    if not table.dq_contract:
        raise ValueError(
            f"Table {table.table_name!r}: source {table.source_id!r} is a {checker} checker, "
            f"so the table must carry a dq_contract — its rows are that contract's results"
        )
    if not table.columns:
        raise ValueError(
            f"Table {table.table_name!r}: declare at least one column to carry visible_to; the "
            f"results schema itself ships (REQ-1443) and replaces what is declared"
        )
    visible_sets = {tuple(c.visible_to) for c in table.columns}
    if len(visible_sets) > 1:
        raise ValueError(
            f"Table {table.table_name!r}: checker results columns must share one visible_to; "
            f"got {sorted(visible_sets)}"
        )
    try:
        dataset = contract_dataset(table.dq_contract, checker)
        target = resolve_contract_target(dataset, tables)
    except ContractError as exc:
        raise ValueError(f"Table {table.table_name!r}: {exc}") from exc
    # By NAME, not identity: on the admin path the governed tables are rebuilt from rows, so the
    # results table's own row is a different object than the model being registered.
    if target.schema_name == table.schema_name and target.table_name == table.table_name:
        raise ValueError(
            f"Table {table.table_name!r}: contract dataset {dataset!r} resolves to the results "
            f"table itself; a contract observes a governed table, not its own scan history"
        )
    table.columns = results_columns(list(table.columns[0].visible_to))
    table.watermark_column = DQ_WATERMARK_COLUMN
    existing = {p.get("target_column") for p in table.promotions}
    table.promotions = table.promotions + [
        p for p in DQ_PROMOTIONS if p["target_column"] not in existing
    ]
    return target
