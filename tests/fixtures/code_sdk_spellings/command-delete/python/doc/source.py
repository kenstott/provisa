# Copyright (c) 2026 Kenneth Stott
# Canary: 087b8030-cb65-4b88-9fcb-92af98fd4f0a
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


def delete_rows(keys: list[int]) -> int:
    """The delete of rows.

    @command
    @delete rows
    """
    return 0
