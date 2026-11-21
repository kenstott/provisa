# Copyright (c) 2026 Kenneth Stott
# Canary: 9e3653e5-b2fd-4331-be03-c04fd013ce25
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


def rows() -> list[Row]:
    """Rows from the API.

    @table
    @requires API_KEY:secret
    @requires REGION
    """
    return []
