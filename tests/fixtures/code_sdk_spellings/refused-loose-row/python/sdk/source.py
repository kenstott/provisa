# Copyright (c) 2026 Kenneth Stott
# Canary: 3c69a4ed-7554-4604-a2d3-1334b3899f75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from typing import Any

from provisa_sdk import table


@dataclass
class Row:
    id: int
    name: str


@table
def rows() -> list[dict[str, Any]]:
    return []
