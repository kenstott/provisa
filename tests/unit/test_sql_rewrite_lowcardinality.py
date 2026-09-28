# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a1c9d-8e2b-4a5f-9c7d-1b6e4f9a2c5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.sql_rewrite.wrap_lowcardinality_columns /
is_clickhouse_lowcardinality_string (REQ-1881).

Pure SQL-string-in, SQL-string-out -- no live engine, no Docker/network.
"""

from __future__ import annotations

import sqlglot

from provisa.compiler.sql_rewrite import (
    is_clickhouse_lowcardinality_string,
    wrap_lowcardinality_columns,
)

_AFFECTED = {("bench_clickhouse", "order_events"): {"event_type", "channel"}}


def _norm(sql: str) -> str:
    return sqlglot.parse_one(sql, read="postgres").sql(dialect="postgres")


def test_select_list_column_gets_wrapped() -> None:
    sql = 'SELECT e.event_type FROM "bench_clickhouse"."order_events" AS e'
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(
        'SELECT FROM_UTF8(e.event_type) FROM "bench_clickhouse"."order_events" AS e'
    )


def test_where_predicate_column_gets_wrapped() -> None:
    sql = (
        'SELECT e.order_id FROM "bench_clickhouse"."order_events" AS e '
        "WHERE e.event_type = 'shipped'"
    )
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(
        'SELECT e.order_id FROM "bench_clickhouse"."order_events" AS e '
        "WHERE FROM_UTF8(e.event_type) = 'shipped'"
    )


def test_join_on_column_gets_wrapped() -> None:
    sql = (
        'SELECT o.order_id FROM "bench_clickhouse"."orders" AS o '
        'JOIN "bench_clickhouse"."order_events" AS e ON e.event_type = o.status'
    )
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(
        'SELECT o.order_id FROM "bench_clickhouse"."orders" AS o '
        'JOIN "bench_clickhouse"."order_events" AS e ON FROM_UTF8(e.event_type) = o.status'
    )


def test_non_clickhouse_source_same_column_name_not_wrapped() -> None:
    # affected_columns is keyed to the clickhouse-sourced (schema, table) only -- a different
    # schema's table with a column of the same name is simply absent from the map, so it's never
    # matched, regardless of that other source's own type.
    sql = 'SELECT p.event_type FROM "bench_postgresql"."order_events" AS p'
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(sql)


def test_column_not_in_affected_set_not_wrapped() -> None:
    # geo_country is a real column on the table but the caller never added it (e.g. it's a plain
    # String, or a UInt64) -- not in the map, so left alone.
    sql = 'SELECT e.geo_country FROM "bench_clickhouse"."order_events" AS e'
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(sql)


def test_already_wrapped_reference_not_double_wrapped() -> None:
    sql = 'SELECT from_utf8(e.event_type) FROM "bench_clickhouse"."order_events" AS e'
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    # unchanged ⇒ the function returns the ORIGINAL text verbatim (no-op, changed=False), not a
    # re-rendered parse (which would normalize FROM_UTF8's casing and mask whether a rewrite
    # actually happened) -- same convention as REQ-1880's own now()-predicate no-op test.
    assert out == sql


def test_unqualified_column_not_wrapped() -> None:
    # No table qualifier -- e.g. an ORDER BY output-alias reference -- can't be resolved to a
    # physical table, so it is never guessed at.
    sql = 'SELECT e.event_type AS et FROM "bench_clickhouse"."order_events" AS e ORDER BY et'
    out = wrap_lowcardinality_columns(sql, "postgres", _AFFECTED)
    assert out == _norm(
        'SELECT FROM_UTF8(e.event_type) AS et FROM "bench_clickhouse"."order_events" AS e '
        "ORDER BY et"
    )


def test_empty_affected_columns_is_a_no_op_short_circuit() -> None:
    sql = 'SELECT e.event_type FROM "bench_clickhouse"."order_events" AS e'
    assert wrap_lowcardinality_columns(sql, "postgres", {}) == sql


def test_bare_lowcardinality_string_detected() -> None:
    assert is_clickhouse_lowcardinality_string("LowCardinality(String)") is True
    assert is_clickhouse_lowcardinality_string("lowcardinality(string)") is True
    assert is_clickhouse_lowcardinality_string("LowCardinality( String )") is True


def test_nullable_lowcardinality_string_detected_same_as_bare() -> None:
    assert is_clickhouse_lowcardinality_string("LowCardinality(Nullable(String))") is True
    assert is_clickhouse_lowcardinality_string("lowcardinality(nullable(string))") is True


def test_lowcardinality_non_string_not_detected() -> None:
    assert is_clickhouse_lowcardinality_string("LowCardinality(UInt64)") is False


def test_plain_string_not_detected() -> None:
    assert is_clickhouse_lowcardinality_string("String") is False


def test_uint64_not_detected() -> None:
    assert is_clickhouse_lowcardinality_string("UInt64") is False


def test_none_and_empty_not_detected() -> None:
    assert is_clickhouse_lowcardinality_string(None) is False
    assert is_clickhouse_lowcardinality_string("") is False
