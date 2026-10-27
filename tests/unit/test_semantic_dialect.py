# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The semantic SQL is PostgreSQL text read back and written again at every rewrite (REQ-778): a
JSON value read whole (the root path, ``$``) survives that, and reaches each target as its own."""

# Requirements: REQ-778

from __future__ import annotations

import pytest
import sqlglot

import provisa  # noqa: F401  (the semantic dialect is installed on import)

pytestmark = pytest.mark.unit

_ROOT = "SELECT JSON_EXTRACT_SCALAR(CAST(m AS JSON), '$') AS x FROM t"


def _rewritten(sql: str, times: int) -> str:
    for _ in range(times):
        sql = sqlglot.parse_one(sql, read="postgres").sql(dialect="postgres")
    return sql


@pytest.mark.parametrize(
    ("target", "written"),
    [
        ("postgres", "JSON_EXTRACT_PATH_TEXT(CAST(m AS JSON), VARIADIC '{}')"),
        ("trino", "JSON_EXTRACT_SCALAR(CAST(m AS JSON), '$')"),
        ("duckdb", "CAST(m AS JSON) ->> '$'"),
    ],
)
def test_a_json_value_read_whole_survives_rewrites_and_reaches_each_target(target, written):
    tree = sqlglot.parse_one(_rewritten(_ROOT, 3), read="postgres")
    assert written in tree.sql(dialect=target)


_FILTERED = "SELECT FILTER(l, y -> y > 1) AS f FROM t"


@pytest.mark.parametrize(
    ("target", "written"),
    [
        ("trino", "FILTER(l, y -> y > 1)"),
        ("duckdb", "LIST_FILTER(l, y -> y > 1)"),
        ("postgres", "ARRAY(SELECT y FROM UNNEST(l) AS _t(y) WHERE y > 1)"),
    ],
)
def test_a_filtered_list_survives_rewrites_and_reaches_each_target(target, written):
    from provisa.transpiler.transpile import transpile

    assert written in transpile(_rewritten(_FILTERED, 3), target).replace('"', "")
