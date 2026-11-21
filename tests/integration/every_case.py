# Copyright (c) 2026 Kenneth Stott
# Canary: 7c907b15-0362-4169-8af2-0748df58cd2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every case of an engine replay in one test, each case's failure reported by name (REQ-2171).

A heavy engine's marker (requires_clickhouse, tests/conftest.py _heavy_db_service) starts the engine
for each test and removes it after, then waits on the core services again. A replay parametrized one
test to a case paid that cold start once per case: 48 ClickHouse boots for the text search replay
alone, which held the core 3/6 shard past its job limit (release 509). One test over every case
boots the engine once."""

# Requirements: REQ-2171

from __future__ import annotations

from collections.abc import Callable, Iterable


def failures_of_every_case(cases: Iterable[str], check: Callable[[str], object]) -> dict[str, str]:
    """Each case whose check failed, with its failure: every case is checked, whatever failed before."""
    failed: dict[str, str] = {}
    for case in cases:
        try:
            check(case)
        except AssertionError as error:
            failed[case] = str(error)[:2000]
    return failed
