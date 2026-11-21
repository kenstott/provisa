# Copyright (c) 2026 Kenneth Stott
# Canary: bfcb3634-2ba3-40c9-a67d-3d63607f5ce2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import table


@dataclass
class Row:
    id: int
    name: str


@table
def rows() -> list[Row]:
    """Every row."""
    return []
