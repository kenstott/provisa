# Copyright (c) 2026 Kenneth Stott
# Canary: 3279bf69-7644-4c45-8ea5-ba9440fdb810
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


async def rows(context: ReadContext) -> list[Row]:
    """Rows the read asks for.

    @table
    """
    return []
