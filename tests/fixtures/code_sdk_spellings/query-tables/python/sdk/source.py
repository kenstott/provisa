# Copyright (c) 2026 Kenneth Stott
# Canary: 63bc4a74-9b9a-4bc7-8a60-7970388e40ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import query
from provisa_services import TableRef


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Match:
    left_id: int
    right_id: int


@query
def pairs(left: TableRef[Row], right: TableRef[Row]) -> list[Match]:
    """Rows of two tables that belong together."""
    return []
