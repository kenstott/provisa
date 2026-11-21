# Copyright (c) 2026 Kenneth Stott
# Canary: fe22ffc4-e69c-41bf-8643-7a50ddc65123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from collections.abc import AsyncIterator

from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


async def rows(inputs: Inputs, context: ReadContext) -> AsyncIterator[Row]:
    """Rows of a region, as the read asks.

    @table
    """
    yield Row(id=1, name="a")
