# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3f6d18-2e75-4c0b-b841-6d2e9c7f0a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A fixture table's id by its name: the same name the same id, two names never one.

Not ``hash(name) % 1000``: ``hash(str)`` is salted per process, and one process in a thousand gave
two names one id (tests/unit/test_fixture_table_ids_are_distinct.py)."""

from __future__ import annotations

_IDS: dict[str, int] = {}


def table_id(name: str) -> int:
    return _IDS.setdefault(name, len(_IDS) + 1)
