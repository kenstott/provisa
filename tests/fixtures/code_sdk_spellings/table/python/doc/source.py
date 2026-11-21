# Copyright (c) 2026 Kenneth Stott
# Canary: 061e5c57-533f-4e89-bafc-7175a0d45e11
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


def rows() -> list[Row]:
    """Every row.

    @table
    """
    return []
