# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41c7a2-5e0b-4f6c-9a37-2b1e60d4c9f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A to_json of a to_json of ... on DuckDB grows with its depth, not as a power of it (REQ-2171).

The form that tells an instant from the rest names its value more than once; written around each
level of a nested statement it multiplied the text, and the ontology reads (five levels of
to_json(MAP(...))) ran past pytest's 300 seconds."""

# Requirements: REQ-2171

from __future__ import annotations

import sqlglot

from provisa.transpiler import pg_functions
from provisa.transpiler.pg_forms_json import json_encoded


def _nested(depth: int) -> str:
    inner = "TO_JSON(x.a)"
    for _ in range(depth):
        inner = f"TO_JSON(MAP(ARRAY['a', 'b'], ARRAY[TO_JSON(x.a), {inner}]))"
    return f"SELECT {inner} FROM x"


def test_a_nested_to_json_grows_by_a_constant_per_level():
    sizes = [len(pg_functions.after(_nested(depth))) for depth in range(1, 8)]
    steps = [b - a for a, b in zip(sizes, sizes[1:], strict=False)]
    assert max(steps) == min(steps), sizes


def test_a_to_json_the_json_form_already_tells_apart_is_not_told_again():
    said = json_encoded(sqlglot.parse_one("a", read="duckdb")).sql(dialect="duckdb")
    sql = f"SELECT {said} FROM x"
    assert pg_functions.after(sql).count("TYPEOF") == 1
