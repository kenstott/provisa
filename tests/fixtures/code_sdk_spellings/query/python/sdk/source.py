# Copyright (c) 2026 Kenneth Stott
# Canary: 59339192-3c85-40f9-a252-7e702136b159
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import query


@dataclass
class Row:
    id: int
    name: str


@query
def lookup(name: str) -> list[Row]:
    """Rows of one name."""
    return []
