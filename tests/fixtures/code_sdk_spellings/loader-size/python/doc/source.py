# Copyright (c) 2026 Kenneth Stott
# Canary: daffdb80-3794-422e-85f9-105f90195a4a
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


def rows(context: ReadContext) -> list[Row]:
    """Rows of the keys, 100 at a time.

    @table
    @loader 100
    """
    return []
