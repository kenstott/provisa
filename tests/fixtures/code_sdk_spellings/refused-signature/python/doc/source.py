# Copyright (c) 2026 Kenneth Stott
# Canary: e1411e80-b48d-45ab-94d6-b744987f626a
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


def rows(context: ReadContext, inputs: Inputs) -> list[Row]:
    """@table"""
    return []
