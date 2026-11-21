# Copyright (c) 2026 Kenneth Stott
# Canary: 979bc352-ee59-464a-b6ca-06e6ba1a30dc
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


@table
async def rows(context: ReadContext) -> list[Row]:
    """Rows the read asks for."""
    return []
