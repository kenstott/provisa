# Copyright (c) 2026 Kenneth Stott
# Canary: b3fb5a3c-bb66-4d5c-991e-043c1ee507bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import Stream, stream


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


@stream
def ticks(inputs: Inputs) -> Stream[Row]:
    """Each row as it arrives."""
    yield Row(id=1, name="a")
