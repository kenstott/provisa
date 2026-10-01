# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7f1e82-9a45-4b60-8d2e-5f1a7c3b9e04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A temporal column physically stored as an epoch number (REQ-1908) is translated in the SQL the
compiler emits: every read yields the temporal type, a comparison against ISO 8601 text compares
the stored number against the operand's epoch value (the column stays bare, so a source index
still applies), and a write stores an ISO 8601 value as its epoch number."""

# Requirements: REQ-1908

from __future__ import annotations

import pytest
import sqlglot

from provisa.compiler.sql_rewrite import iso_to_epoch, translate_epoch_temporal_columns

_MS = {("s", "t"): {"ts": ("ms", "timestamptz")}}


def _norm(sql: str) -> str:
    return sqlglot.parse_one(sql, read="postgres").sql("postgres")


def _t(sql: str, cols=_MS) -> str:
    return translate_epoch_temporal_columns(sql, cols)


def test_a_selected_epoch_column_reads_as_a_timestamp_under_its_own_name():
    out = _t('SELECT t.id, t.ts FROM "s"."t" AS t')
    assert _norm(out) == _norm(
        "SELECT t.id, TO_TIMESTAMP(CAST(t.ts AS DOUBLE PRECISION) / POWER(10, 3)) AS ts "
        'FROM "s"."t" AS t'
    )


def test_an_unqualified_column_in_a_single_table_select_is_resolved():
    out = _t('SELECT ts FROM "s"."t"')
    assert "TO_TIMESTAMP" in out and "AS ts" in out


def test_a_catalog_qualified_reference_is_translated():
    out = _t('SELECT x.ts FROM "cat"."s"."t" AS x')
    assert "TO_TIMESTAMP" in out


def test_a_string_literal_operand_becomes_its_epoch_number_and_the_column_stays_bare():
    out = _t("SELECT t.id FROM s.t AS t WHERE t.ts >= '2026-01-02T00:00:00Z'")
    assert _norm(out) == _norm("SELECT t.id FROM s.t AS t WHERE t.ts >= 1767312000000")


def test_a_parameter_operand_is_typed_and_the_column_read_as_the_temporal():
    """A bound value is cast to the registered temporal and compared with the column read as that
    temporal — correct on every engine (a SQL-side epoch conversion loses the offset on Trino)."""
    out = _t("SELECT t.id FROM s.t AS t WHERE t.ts < $1")
    lt = sqlglot.parse_one(out, read="postgres").find(sqlglot.exp.LT)
    assert isinstance(lt.this, sqlglot.exp.UnixToTime)
    assert lt.expression.sql("postgres") == "CAST($1 AS TIMESTAMPTZ)"


def test_in_and_between_with_only_literals_fold_to_epoch_numbers():
    out = _t(
        "SELECT t.id FROM s.t AS t WHERE t.ts IN ('1970-01-01T00:00:01Z', '1970-01-01T00:00:03Z') "
        "AND t.ts BETWEEN '1970-01-01T00:00:00Z' AND '1970-01-01T00:00:02Z'"
    )
    assert "IN (1000, 3000)" in out
    assert "BETWEEN 0 AND 2000" in out
    assert "TO_TIMESTAMP" not in out


def test_in_with_a_bound_value_reads_the_column_as_the_temporal():
    out = _t("SELECT t.id FROM s.t AS t WHERE t.ts IN ('1970-01-01T00:00:01Z', $2)")
    assert "TO_TIMESTAMP(" in out and "CAST($2 AS TIMESTAMPTZ)" in out


def test_order_by_and_group_by_keep_the_bare_column():
    out = _t("SELECT t.ts FROM s.t AS t GROUP BY t.ts ORDER BY t.ts")
    tree = sqlglot.parse_one(out, read="postgres")
    assert isinstance(tree.args["group"].expressions[0], sqlglot.exp.Column)
    assert isinstance(tree.args["order"].expressions[0].this, sqlglot.exp.Column)


def test_translation_is_idempotent():
    for sql in (
        "SELECT t.ts FROM s.t AS t WHERE t.ts < $1 AND t.ts > '2026-01-02T00:00:00Z'",
        "SELECT t.id FROM s.t AS t WHERE t.ts IN ($1, $2) AND t.ts BETWEEN $3 AND '1970-01-02'",
        "INSERT INTO s.t (id, ts) VALUES (1, $2), (2, '1970-01-01T00:00:04Z')",
        "SELECT MAX(t.ts) FROM s.t AS t",
        "UPDATE s.t SET ts = $1 WHERE id = 1",
    ):
        once = _t(sql)
        assert _t(once) == once


def test_an_aggregate_reads_the_timestamp():
    out = _t("SELECT MAX(t.ts) AS latest FROM s.t AS t")
    assert "MAX(TO_TIMESTAMP(" in out


def test_other_tables_and_columns_are_untouched():
    sql = "SELECT u.ts, t.id FROM s.u AS u JOIN s.t AS t ON t.id = u.id"
    assert _t(sql) == sql


def test_writes_store_iso_values_as_epoch_numbers():
    upd = _t("UPDATE s.t SET ts = '1970-01-01T00:00:03Z' WHERE id = 1")
    assert "ts = 3000" in upd
    ins = _t("INSERT INTO s.t (id, ts) VALUES (1, $2)")
    assert "EXTRACT(EPOCH FROM CAST($2 AS TIMESTAMPTZ)) * 1000" in ins


@pytest.mark.parametrize(
    ("unit", "data_type", "read"),
    [
        ("s", "timestamptz", "TO_TIMESTAMP(t.ts)"),
        ("us", "timestamptz", "POWER(10, 6)"),
        ("s", "timestamp", "AT TIME ZONE 'UTC'"),
        ("s", "date", "AS DATE"),
    ],
)
def test_each_unit_and_temporal_type_reads_correctly(unit, data_type, read):
    out = _t("SELECT t.ts FROM s.t AS t", {("s", "t"): {"ts": (unit, data_type)}})
    assert read in out


@pytest.mark.parametrize(
    ("text", "unit", "value"),
    [
        ("1970-01-01T00:00:01Z", "s", 1),
        ("1970-01-01T00:00:01.5+00:00", "ms", 1500),
        ("1970-01-01T01:00:00+01:00", "us", 0),
        ("1970-01-02", "s", 86400),
        ("1970-01-01T00:00:00.000007Z", "us", 7),
        ("1970-01-01 00:00:02 UTC", "s", 2),
        ("1970-01-01 01:00:00 +01:00", "s", 0),
    ],
)
def test_iso_to_epoch_is_exact(text, unit, value):
    assert iso_to_epoch(text, unit) == value


def test_iso_to_epoch_rejects_non_iso_text():
    with pytest.raises(ValueError, match="not an ISO 8601"):
        iso_to_epoch("yesterday", "s")


def test_a_sub_unit_value_is_refused_not_truncated():
    with pytest.raises(ValueError, match="finer than"):
        iso_to_epoch("1970-01-01T00:00:00.5Z", "s")


def test_returning_reads_the_temporal_back_under_the_column_name():
    upd = _t("UPDATE s.t SET ts = $1 WHERE id = 1 RETURNING ts")
    assert "RETURNING TO_TIMESTAMP(" in upd and upd.endswith("AS ts")
    ins = _t("INSERT INTO s.t (id, ts) VALUES (1, '1970-01-01T00:00:01Z') RETURNING ts")
    assert "VALUES (1, 1000)" in ins and ins.endswith("AS ts")
    assert _t(upd) == upd and _t(ins) == ins


def test_an_upsert_conflict_assignment_is_left_alone():
    sql = (
        "INSERT INTO s.t (id, ts) VALUES (1, '1970-01-01T00:00:01Z') "
        "ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts"
    )
    out = _t(sql)
    assert "VALUES (1, 1000)" in out and "ts = EXCLUDED.ts" in out


@pytest.mark.parametrize("dialect", ["postgres", "duckdb", "trino"])
def test_translated_sql_transpiles_to_each_engine_with_the_offset_kept(dialect):
    """The pipeline transpiles the PG-dialect text per engine afterwards; a bound value stays a
    typed timestamp-with-zone comparison (no EXTRACT(EPOCH ...) that drops the offset on Trino)."""
    out = _t("SELECT t.ts FROM s.t AS t WHERE t.ts >= $1")
    rendered = sqlglot.transpile(out, read="postgres", write=dialect)[0].upper()
    assert "EXTRACT" not in rendered and "TO_UNIXTIME" not in rendered
    assert "TIME ZONE" in rendered or "TIMESTAMPTZ" in rendered


def test_a_cast_iso_literal_folds_like_a_bare_one():
    """The GraphQL filter emits CAST('<iso>' AS TIMESTAMP); the cast is dropped and the value folded."""
    out = _t("SELECT t.id FROM s.t AS t WHERE t.ts >= CAST('1970-01-01T00:00:02Z' AS TIMESTAMP)")
    assert "t.ts >= 2000" in out and "TO_TIMESTAMP" not in out
    assert _t(out) == out


def test_the_graphql_filter_timestamp_literal_folds():
    """sql_where renders an ISO operand as TIMESTAMP '<date> <time> UTC'."""
    out = _t("SELECT t.id FROM s.t AS t WHERE t.ts >= TIMESTAMP '1970-01-01 00:00:02 UTC'")
    assert "t.ts >= 2000" in out
