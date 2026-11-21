# Copyright (c) 2026 Kenneth Stott
# Canary: a8723666-293a-4d5d-8817-24f559b14abd
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


def status() -> int:
    """@health"""
    return 1
