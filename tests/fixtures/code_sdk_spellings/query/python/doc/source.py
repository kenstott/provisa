# Copyright (c) 2026 Kenneth Stott
# Canary: d2f1647d-7183-493a-8926-f92521c728c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


def lookup(name: str) -> list[Row]:
    """Rows of one name.

    @query
    """
    return []
