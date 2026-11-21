# Copyright (c) 2026 Kenneth Stott
# Canary: 9ee5329d-5bce-4e7b-a227-cc32094f29f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from collections.abc import Iterable


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs) -> Iterable[Row]:
    """Rows of a region.

    @table
    """
    return []
