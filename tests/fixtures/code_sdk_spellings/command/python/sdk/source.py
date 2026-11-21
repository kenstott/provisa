# Copyright (c) 2026 Kenneth Stott
# Canary: b4e343ac-9c67-40f4-a2c9-061f9e62814b
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


@command
def cancel(order_id: int) -> int:
    """Cancel one order."""
    return 1
