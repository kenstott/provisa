# Copyright (c) 2026 Kenneth Stott
# Canary: e20a688d-ab8c-4afe-a27d-5f9d9edb76c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1162: append-only, engine-computed bitemporal materialization SQL."""

from __future__ import annotations

import pytest

from provisa.mv.bitemporal import (
    MODE_DELTA,
    MODE_SNAPSHOT,
    BitemporalSpec,
    append_sql,
    as_of_valid_predicate,
    create_sql,
    current_state_sql,
    reconstruct_as_of_sql,
    system_columns_ddl,
)

TARGET = '"mvcat"."tenant_mv"."mv_orders"'
COLS = ["id", "region", "amount"]
SELECT = "SELECT id, region, amount FROM base"


def _snap() -> BitemporalSpec:
    return BitemporalSpec(key=("id",), mode=MODE_SNAPSHOT)


def _delta() -> BitemporalSpec:
    return BitemporalSpec(key=("id",), mode=MODE_DELTA)


# ── validation ───────────────────────────────────────────────────────────────


def test_invalid_mode_rejected():
    with pytest.raises(ValueError, match="invalid bitemporal mode"):
        BitemporalSpec(key=("id",), mode="merge")


def test_delta_requires_key():
    with pytest.raises(ValueError, match="requires a business key"):
        BitemporalSpec(key=(), mode=MODE_DELTA)


def test_duplicate_column_names_rejected():
    with pytest.raises(ValueError, match="must be distinct"):
        BitemporalSpec(key=("id",), system_column="ts", op_column="ts", mode=MODE_DELTA)


# ── DDL ──────────────────────────────────────────────────────────────────────


def test_system_columns_snapshot_is_one_stamp():
    assert system_columns_ddl(_snap()) == [("sys_recorded_at", "TIMESTAMP")]


def test_system_columns_delta_adds_op():
    assert system_columns_ddl(_delta()) == [
        ("sys_recorded_at", "TIMESTAMP"),
        ("sys_op", "VARCHAR"),
    ]


# ── the CORE invariant: PURE APPEND — never UPDATE, never DELETE-of-history ────


def _all_write_sql(spec: BitemporalSpec) -> list[str]:
    return [create_sql(TARGET, SELECT, spec, "TIMESTAMP '2026-07-20 00:00:00'")] + append_sql(
        TARGET, SELECT, spec, COLS, "TIMESTAMP '2026-07-20 00:00:00'", "duckdb"
    )


@pytest.mark.parametrize("spec", [_snap(), _delta()])
def test_maintenance_is_pure_append(spec):
    for sql in _all_write_sql(spec):
        upper = sql.upper()
        assert "UPDATE " not in upper, f"UPDATE found (not append-only): {sql}"
        assert "DELETE " not in upper, f"DELETE found (not append-only): {sql}"
        assert "DROP " not in upper, f"DROP found (not append-only): {sql}"
        # every write is a CREATE TABLE AS or an INSERT ... SELECT
        assert upper.startswith("CREATE TABLE") or upper.startswith("INSERT INTO")


# ── snapshot mode ─────────────────────────────────────────────────────────────


def test_snapshot_append_is_single_insert_of_whole_dataset():
    stmts = append_sql(TARGET, SELECT, _snap(), COLS, "TIMESTAMP '2026-07-20 00:00:00'", "duckdb")
    assert len(stmts) == 1
    sql = stmts[0]
    assert sql.startswith(f"INSERT INTO {TARGET}")
    assert '"sys_recorded_at"' in sql
    assert "NOT EXISTS" not in sql  # snapshot does no diff at all


def test_snapshot_reconstruct_picks_latest_batch():
    sql = reconstruct_as_of_sql(TARGET, _snap(), COLS, ts_sql=None)
    assert f"MAX(\"sys_recorded_at\") FROM {TARGET}" in sql
    assert "ROW_NUMBER" not in sql


def test_snapshot_reconstruct_as_of_bounds_the_batch():
    sql = reconstruct_as_of_sql(TARGET, _snap(), COLS, ts_sql="TIMESTAMP '2026-01-01 00:00:00'")
    assert "WHERE \"sys_recorded_at\" <= TIMESTAMP '2026-01-01 00:00:00'" in sql


# ── delta mode ────────────────────────────────────────────────────────────────


def test_delta_append_is_upserts_then_tombstones():
    stmts = append_sql(TARGET, SELECT, _delta(), COLS, "TIMESTAMP '2026-07-20 00:00:00'", "duckdb")
    assert len(stmts) == 2
    upserts, tombstones = stmts
    assert f"'upsert'" in upserts and "NOT EXISTS" in upserts
    assert f"'delete'" in tombstones and "NOT EXISTS" in tombstones
    # both are appends
    assert upserts.startswith(f"INSERT INTO {TARGET}")
    assert tombstones.startswith(f"INSERT INTO {TARGET}")


def test_delta_uses_null_safe_equality_ansi():
    stmts = append_sql(TARGET, SELECT, _delta(), COLS, "TIMESTAMP '2026-07-20 00:00:00'", "duckdb")
    assert "IS NOT DISTINCT FROM" in stmts[0]


def test_delta_uses_spaceship_on_mysql():
    stmts = append_sql(TARGET, SELECT, _delta(), COLS, "TIMESTAMP '2026-07-20 00:00:00'", "mysql")
    assert "<=>" in stmts[0]
    assert "IS NOT DISTINCT FROM" not in stmts[0]


def test_delta_reconstruct_folds_latest_per_key_dropping_tombstones():
    sql = reconstruct_as_of_sql(TARGET, _delta(), COLS, ts_sql=None)
    assert "ROW_NUMBER() OVER" in sql
    assert 'PARTITION BY "id"' in sql
    assert "_rn = 1" in sql
    assert "<> 'delete'" in sql


def test_current_state_is_reconstruct_now():
    assert current_state_sql(TARGET, _delta(), COLS) == reconstruct_as_of_sql(
        TARGET, _delta(), COLS, ts_sql=None
    )


# ── valid time (business time supplied by the view) ──────────────────────────


def test_as_of_valid_requires_declared_columns():
    with pytest.raises(ValueError, match="valid_from/valid_to"):
        as_of_valid_predicate(_snap(), "TIMESTAMP '2026-01-01 00:00:00'")


def test_as_of_valid_builds_interval_predicate():
    spec = BitemporalSpec(key=("id",), valid_from="vf", valid_to="vt")
    pred = as_of_valid_predicate(spec, "TIMESTAMP '2026-01-01 00:00:00'", alias="t")
    assert '"t"."vf" <=' in pred
    assert '"t"."vt" IS NULL OR "t"."vt" >' in pred
