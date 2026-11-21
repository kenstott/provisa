# Copyright (c) 2026 Kenneth Stott
# Canary: e4a4d589-de57-4de6-bcdf-3fb427afb250
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass
from typing import Any


@dataclass
class Row:
    id: int
    name: str


def rows() -> list[dict[str, Any]]:
    """@table"""
    return []
