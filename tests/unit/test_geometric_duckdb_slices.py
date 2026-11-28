# Copyright (c) 2026 Kenneth Stott
# Canary: 1506c7ce-0159-4f27-b988-dbf9bb6c1369
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DuckDB forms replay's slices are a partition of the functions: none missed, none twice (REQ-2171)."""

# Requirements: REQ-2171

from __future__ import annotations

from tests.integration.geometric_engines import DUCKDB_FORM_SLICES, duckdb_form_slice
from tests.unit.pgwire.geometric_replay import BY_FUNCTION


def test_the_slices_cover_every_function_once():
    slices = [duckdb_form_slice(i) for i in range(DUCKDB_FORM_SLICES)]
    assert sorted(f for s in slices for f in s) == sorted(BY_FUNCTION)
    assert len({f for s in slices for f in s}) == len(BY_FUNCTION)
