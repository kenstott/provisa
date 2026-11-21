# Copyright (c) 2026 Kenneth Stott
# Canary: 493a13a0-c9e4-426d-9f3e-0d1f99b23022
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from collections.abc import Iterator


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def ticks(inputs: Inputs) -> Iterator[Row]:
    """Each row as it arrives.

    @stream
    """
    yield Row(id=1, name="a")
