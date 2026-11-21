# Copyright (c) 2026 Kenneth Stott
# Canary: 8422d095-cfc3-4f13-a08d-f4b01297d19d
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
class Changes:
    name: str | None = None


def update_rows(keys: list[int], changes: Changes) -> int:
    """The update of rows.

    @command
    @update rows
    """
    return 0
