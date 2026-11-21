# Copyright (c) 2026 Kenneth Stott
# Canary: eb884ea7-d98e-4ec1-8d79-b8bac4c1fceb
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


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs, context: ReadContext) -> list[Row]:
    """Rows of every key in one batch.

    @table
    @loader
    """
    return []
