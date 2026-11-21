# Copyright (c) 2026 Kenneth Stott
# Canary: b499abb5-c5a4-432c-afe7-31b060bdb340
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


@dataclass
class Upper:
    upper: str


def shout(names: list[str]) -> list[Upper]:
    """Each name in capitals.

    @columns
    """
    return [Upper(upper=n.upper()) for n in names]
