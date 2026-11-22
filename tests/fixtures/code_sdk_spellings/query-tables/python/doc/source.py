# Copyright (c) 2026 Kenneth Stott
# Canary: 8bf424b4-febf-4f34-8b79-941a61a9e24f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from provisa_services import TableRef


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Match:
    left_id: int
    right_id: int


def pairs(left: TableRef[Row], right: TableRef[Row]) -> list[Match]:
    """Rows of two tables that belong together.

    @query
    """
    return []
