# Copyright (c) 2026 Kenneth Stott
# Canary: 1eac2be7-d301-4db0-bfd3-4c1eacb81d07
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


@command(delete="rows")
def delete_rows(keys: list[int]) -> int:
    """The delete of rows."""
    return 0
