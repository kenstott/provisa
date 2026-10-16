# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A regex mask masks every match, on every engine, as it does on a change-event row (REQ-263).

A phone number masked with ``[0-9]`` -> ``X`` read ``XX0-0074321`` on DuckDB: PostgreSQL's
REGEXP_REPLACE, the form the governed statement is written in, replaces the first match unless
flagged 'g', and DuckDB reads it the same way. The mask carries the flag; an engine whose
three-argument form replaces every match already gets it without one."""

# Requirements: REQ-263

from __future__ import annotations

import duckdb
import pytest

from provisa.security.masking import (
    MaskingRule,
    MaskType,
    apply_mask_to_value,
    build_mask_expression,
)
from provisa.transpiler.transpile import transpile

_RULE = MaskingRule(mask_type=MaskType.regex, pattern="[0-9]", replace="X")
_PHONE = "030-0074321"
_MASKED = "XXX-XXXXXXX"


def _governed() -> str:
    mask = build_mask_expression(_RULE, '"t"."phone"', "varchar")
    return f'SELECT {mask} AS phone FROM "s"."t"'


def test_the_in_process_mask_masks_every_match():
    assert apply_mask_to_value(_RULE, _PHONE, "varchar") == _MASKED


def test_duckdb_masks_every_match():
    sql = transpile(_governed(), "duckdb").replace('"s"."t"', f"(SELECT '{_PHONE}' AS phone) AS t")
    assert duckdb.sql(sql).fetchone() == (_MASKED,)


@pytest.mark.parametrize(
    "dialect",
    ["trino", "snowflake", "bigquery", "mysql", "clickhouse", "databricks", "oracle", "exasol"],
)
def test_an_engine_replacing_every_match_already_gets_no_flag(dialect):
    written = transpile(_governed(), dialect)
    assert "REGEXP_REPLACE(" in written.upper()
    assert "'g'" not in written, written


@pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
def test_an_engine_replacing_the_first_match_unflagged_gets_the_flag(dialect):
    assert "'X', 'g')" in transpile(_governed(), dialect)
