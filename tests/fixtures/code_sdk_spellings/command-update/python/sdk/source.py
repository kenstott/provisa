# Copyright (c) 2026 Kenneth Stott
# Canary: 500b544f-11b7-4eaa-b784-9c117744fcf3
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


@dataclass
class Changes:
    name: str | None = None


@command(update="rows")
def update_rows(keys: list[int], changes: Changes) -> int:
    """The update of rows."""
    return 0
