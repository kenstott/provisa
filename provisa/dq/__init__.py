# Copyright (c) 2026 Kenneth Stott
# Canary: c3faeae2-741d-4614-bfed-29a5caaebdf9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""External data-quality checkers as SOURCE types (REQ-1443).

A check result is an OBSERVATION, not a verdict, so a checker lands through the ordinary source path
and inherits cadence, freshness, events, lineage, governance, RLS, grid and export. Nothing here is a
gate: REQ-941/REQ-1165 enforcement over the landed rows is a separate, later declaration.

Three pieces, one per registration decision in REQ-1443:

``results`` — the FIXED, SHIPPED results schema. Never hand-declared per table; the scan envelope is
defined by the checker, not the operator.

``contract`` — the contract text is the single definition. The target dataset is PARSED out of it
(REQ-939: derived, never hand-declared), so lineage cannot drift from what the checker actually scans.

``runner`` — the out-of-process invocation and its result normalisation. Provisa vendors and links
nothing; the checker is an external prereq in the oracledb posture.
"""

from provisa.dq.contract import (
    CHECKERS,
    ContractError,
    check_definition,
    contract_checks,
    contract_dataset,
    dataset_parts,
    resolve_contract_target,
)
from provisa.dq.results import (
    DQ_PROMOTIONS,
    DQ_WATERMARK_COLUMN,
    RESULT_FIELDS,
    results_columns,
)
from provisa.dq.runner import CheckerError, build_command, parse_results, run_contract

__all__ = [
    "CHECKERS",
    "DQ_PROMOTIONS",
    "DQ_WATERMARK_COLUMN",
    "RESULT_FIELDS",
    "CheckerError",
    "ContractError",
    "build_command",
    "check_definition",
    "contract_checks",
    "contract_dataset",
    "dataset_parts",
    "parse_results",
    "resolve_contract_target",
    "results_columns",
    "run_contract",
]
