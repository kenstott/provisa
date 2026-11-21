# Copyright (c) 2026 Kenneth Stott
# Canary: 3f51b478-3912-471c-b952-5299cb1567b3
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


@dataclass
class Inputs:
    region: str = "EU"


@table(loader=True)
def rows(inputs: Inputs, context: ReadContext) -> list[Row]:
    """Rows of every key in one batch."""
    return []
