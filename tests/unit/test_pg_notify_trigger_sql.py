# Copyright (c) 2026 Kenneth Stott
# Canary: f532a010-50ae-4239-9141-43ecd54538b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1515: the notify trigger's SQL carries the payload guard that keeps a write alive.

Whether PostgreSQL actually accepts the NOTIFY is proved by
``tests/integration/test_pg_notify_payload_limit.py`` against a real server. What is provable
without one is that the DDL Provisa installs contains the guard at all — a trigger emitted
without it announces every row unconditionally, and the first oversized row aborts the writer's
transaction.
"""

from __future__ import annotations

from provisa.subscriptions.pg_provider import CHANNEL_PREFIX
from provisa.subscriptions.pg_triggers import MAX_NOTIFY_BYTES, _trigger_sql


def test_payload_limit_stays_below_the_postgres_maximum() -> None:
    # 8000 is the point at which NOTIFY raises, so the budget has to sit under it.
    assert MAX_NOTIFY_BYTES < 8000


def test_trigger_measures_the_payload_before_announcing_it() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    assert f"octet_length(payload) > {MAX_NOTIFY_BYTES}" in sql
    assert f"budget := {MAX_NOTIFY_BYTES};" in sql


def test_oversized_row_is_truncated_rather_than_dropped() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    # The subscriber still learns the row changed and reads its leading columns.
    assert "'truncated', true" in sql
    assert "'row_text', left(rowjson, budget)" in sql
    assert "lower(TG_OP)" in sql


def test_budget_halves_until_the_envelope_fits_and_the_loop_ends() -> None:
    sql = _trigger_sql("public", "query_audit_log")

    # A character encodes to as many as four bytes, so one pass is not enough — but a budget
    # that never reaches zero would spin inside the writer's transaction.
    assert "budget := budget / 2;" in sql
    assert f"EXIT WHEN octet_length(payload) <= {MAX_NOTIFY_BYTES} OR budget < 1;" in sql


def test_notify_targets_the_table_channel_the_provider_listens_on() -> None:
    sql = _trigger_sql("app", "orders")

    assert f"pg_notify('{CHANNEL_PREFIX}orders'" in sql
    assert "provisa_notify_app_orders" in sql
    assert "ON app.orders" in sql
