# Copyright (c) 2026 Kenneth Stott
# Canary: 68fe21cd-b449-49a6-b4cd-cffe0e929b1a
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


@table
def rows(context: ReadContext, inputs: Inputs) -> list[Row]:
    return []
