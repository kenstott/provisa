# Copyright (c) 2026 Kenneth Stott
# Canary: ed19f559-8dff-4022-a36b-cb32311e06e4
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


@dataclass
class Inputs:
    region: str = "EU"


@table(loader=True)
def rows(inputs: Inputs) -> list[Row]:
    return []
