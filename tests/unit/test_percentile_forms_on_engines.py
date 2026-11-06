# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b4c2a-97d3-4f80-b5a1-3c8e2d7f90b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""percentile_cont and percentile_disc of a NULL fraction array on Trino and ClickHouse (REQ-2171):
PostgreSQL answers a NULL array; ClickHouse has no Nullable array to answer it with, so the call is
refused by name there rather than left to the engine's "no supertype" error."""

# Requirements: REQ-2171

from __future__ import annotations

import pytest

from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile

_NULL_ARRAY = (
    'SELECT pg_catalog."{f}"(NULL::double precision[]) WITHIN GROUP '
    "(ORDER BY NULL::double precision) AS v"
)
_NULL_FRACTION = (
    'SELECT pg_catalog."{f}"(NULL::double precision) WITHIN GROUP '
    "(ORDER BY NULL::double precision) AS v"
)


@pytest.mark.parametrize("function", ["percentile_cont", "percentile_disc"])
def test_clickhouse_refuses_a_null_fraction_array_by_name(function: str):
    with pytest.raises(FunctionRefused, match=rf"{function}\(\) has no form on clickhouse"):
        transpile(_NULL_ARRAY.format(f=function), "clickhouse")


@pytest.mark.parametrize("function", ["percentile_cont", "percentile_disc"])
def test_a_null_fraction_alone_is_still_served_on_clickhouse(function: str):
    assert "CASE WHEN FALSE" in transpile(_NULL_FRACTION.format(f=function), "clickhouse")


def test_trino_still_answers_a_null_fraction_array():
    assert "CASE WHEN FALSE" in transpile(_NULL_ARRAY.format(f="percentile_cont"), "trino")
