# Copyright (c) 2026 Kenneth Stott
# Canary: 00afab18-17bb-49bc-9baf-e2a1dd95f63f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import requires, table


@dataclass
class Row:
    id: int
    name: str


@requires("REGION")
@requires("API_KEY", secret=True)
@table
def rows() -> list[Row]:
    """Rows from the API."""
    return []
