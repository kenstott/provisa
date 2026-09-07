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


def derive_checker_table(table: Any, source_type: Any) -> str:
    """Validate a checker table's contract and derive its registration IN PLACE. Returns the dataset.

      * the contract must parse and must name a three-part dataset; whether that dataset resolves
        to a governed table is :func:`check_contract_target`, run once the compiled names exist;
      * the columns become :func:`results_columns` — the envelope is the CHECKER's, not the
        operator's, so a hand-written column list could only ever disagree with what lands;
      * ``scan_time`` becomes the watermark, which makes the landing an append (REQ-982) and the
        table a scan history with no history subsystem;
      * ``DQ_PROMOTIONS`` seeds the REQ-119 promotions, appended to any the operator added;
      * ``product_id`` must be unset — membership is inherited from the scanned table (clause 10).

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
    # REQ-1443 clause 10: a results table belongs to whatever product the table it scans belongs
    # to, derived at read time from the contract's dataset. A declared product_id could only ever
    # agree with that derivation or contradict it, so it is refused rather than stored.
    if table.product_id is not None:
        raise ValueError(
            f"Table {table.table_name!r}: a checker table cannot declare product_id "
            f"{table.product_id!r}; its data-product membership is inherited from the table its "
            f"contract scans"
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
    except ContractError as exc:
        raise ValueError(f"Table {table.table_name!r}: {exc}") from exc
    table.columns = results_columns(list(table.columns[0].visible_to))
    table.watermark_column = DQ_WATERMARK_COLUMN
    existing = {p.get("target_column") for p in table.promotions}
    table.promotions = table.promotions + [
        p for p in DQ_PROMOTIONS if p["target_column"] not in existing
    ]
    return dataset


def check_contract_target(table: Any, dataset: str, contexts: dict) -> Any:
    """The compiled table ``dataset`` observes, or :class:`ValueError` naming ``table``.

    A checker may only observe what Provisa governs (REQ-967), and the resolved target is the
    results table's lineage (REQ-939). ``contexts`` is ``state.contexts`` — the dataset names the
    pgwire (semantic) schema/table, which exist only once the schema is compiled, so this runs on
    the admin path against the live contexts and on the YAML path after the startup build.

    The self-target check is by physical NAME, not identity: the results table's own compiled meta
    is a different object than the model being registered.
    """
    try:
        target = resolve_contract_target(dataset, contexts)
    except ContractError as exc:
        raise ValueError(f"Table {table.table_name!r}: {exc}") from exc
    if target.schema_name == table.schema_name and target.table_name == table.table_name:
        raise ValueError(
            f"Table {table.table_name!r}: contract dataset {dataset!r} resolves to the results "
            f"table itself; a contract observes a governed table, not its own scan history"
        )
    return target
