# Copyright (c) 2026 Kenneth Stott
# Canary: 803a2e5f-3616-4201-8cbe-c55623ea5cd0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import change
from provisa_services import TableRef


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Changed:
    left_id: int
    right_id: int
    removed: bool
    seq: int


@change(table="pairs", key=["left_id", "right_id"], removed="removed", cursor="seq")
def pairs_changes(
    left: TableRef[Row], right: TableRef[Row], since: int | None = None
) -> list[Changed]:
    """Matches changed since ``since``."""
    return []
