# Copyright (c) 2026 Kenneth Stott
# Canary: b2deab15-1f27-44cf-8e27-e06ede784213
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


def start(values: dict[str, str]) -> None:
    """Open the client.

    @on-start
    """


def stop() -> None:
    """Close the client.

    @on-stop
    """


def status() -> str:
    """@health"""
    return "ok"


def rows() -> list[Row]:
    """@table"""
    return []
