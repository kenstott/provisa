# Copyright (c) 2026 Kenneth Stott
# Canary: dec73227-67b5-4a9e-a1e7-764958397a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass


@dataclass
class Row:
    id: int
    name: str


@dataclass
class Inputs:
    region: str = "EU"


def rows(inputs: Inputs) -> list[Row]:
    """@table
    @loader
    """
    return []
