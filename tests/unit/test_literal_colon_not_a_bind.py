# Copyright (c) 2026 Kenneth Stott
# Canary: 5eb3a23b-35c0-4c5a-bcd5-aa9da20ef19b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A colon inside a SQL literal is text, never a bind parameter (a grid filter on ``:02``)."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from provisa.core.database import _translate


def _binds(sql: str) -> list[str]:
    return list(text(sql).compile().params)


@pytest.mark.parametrize("args", [(), (5,)])
def test_a_colon_in_a_string_literal_is_not_a_bind(args):
    tail = " AND x = $1" if args else ""
    sql, params = _translate(f"SELECT 1 WHERE v LIKE LOWER('%:02%'){tail}", args, "postgresql")
    assert _binds(sql) == list(params)


def test_a_colon_in_a_quoted_identifier_or_dollar_body_is_not_a_bind():
    sql, _ = _translate("SELECT \"a:b\" FROM t WHERE f = $$x:y 'z:w'$$", (), "postgresql")
    assert _binds(sql) == []


def test_the_literal_reaches_the_database_unchanged():
    sql, params = _translate("SELECT '12:02' AS t, 'a::b' AS u, $1 AS v", (7,), "sqlite")
    with create_engine("sqlite://").connect() as conn:
        assert tuple(conn.execute(text(sql), params).one()) == ("12:02", "a::b", 7)


def test_the_sqlalchemy_driver_keeps_a_literal_colon_as_text():
    from provisa.executor.drivers.sqlalchemy_driver import _to_named_params

    sql, bind = _to_named_params("SELECT 1 WHERE v LIKE '%:02%' AND x = $1", [5])
    assert _binds(sql) == list(bind) == ["p1"]
