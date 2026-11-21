# Copyright (c) 2026 Kenneth Stott
# Canary: 6e0c1a7d-3fd3-466a-b6cf-3c219621d991
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import AsyncStream, table
from provisa_services import ReadContext


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@table
async def rows(inputs: Inputs, context: ReadContext) -> AsyncStream[Row]:
    """Rows of a region, as the read asks."""
    yield Row(id=1, name="a")
