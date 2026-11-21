# Copyright (c) 2026 Kenneth Stott
# Canary: d15e94da-1e0c-4c17-953e-83924096fa40
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


def cancel(order_id: int) -> int:
    """Cancel one order.

    @command
    """
    return 1
