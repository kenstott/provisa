# Copyright (c) 2026 Kenneth Stott
# Canary: e474297d-0cac-4687-9994-a41c1d2ae529
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import command


@dataclass
class Row:
    id: int
    name: str


@command(insert="rows")
def insert_rows(rows: list[Row]) -> int:
    """The insert of rows."""
    return 0
