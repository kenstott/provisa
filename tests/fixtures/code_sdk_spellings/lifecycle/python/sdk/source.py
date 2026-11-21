# Copyright (c) 2026 Kenneth Stott
# Canary: e0d82a19-7002-4d44-8810-c3842fdda687
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from dataclasses import dataclass

from provisa_sdk import health, on_start, on_stop, table


@dataclass
class Row:
    id: int
    name: str


@on_start
def start(values: dict[str, str]) -> None:
    """Open the client."""


@on_stop
def stop() -> None:
    """Close the client."""


@health
def status() -> str:
    return "ok"


@table
def rows() -> list[Row]:
    return []
