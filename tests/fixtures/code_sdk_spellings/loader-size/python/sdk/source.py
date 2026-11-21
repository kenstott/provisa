# Copyright (c) 2026 Kenneth Stott
# Canary: 9777e7e2-d1ea-4074-b921-36675bf6e466
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@table(loader=100)
def rows(context: ReadContext) -> list[Row]:
    """Rows of the keys, 100 at a time."""
    return []
