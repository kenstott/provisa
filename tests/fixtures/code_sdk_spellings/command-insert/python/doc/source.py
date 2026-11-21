# Copyright (c) 2026 Kenneth Stott
# Canary: 9a34c898-21c9-426a-aafa-354c48999f17
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


def insert_rows(rows: list[Row]) -> int:
    """The insert of rows.

    @command
    @insert rows
    """
    return 0
