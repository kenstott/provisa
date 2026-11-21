# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4f2d17-6c9e-4b31-a0d5-e7b3c1f9a682
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2216: ops.requests -- one row per request, published in the ops system schema, keyed by the
request id, and subscribable on its own schema's channel.

The view must carry every column the requirement names; the seed must find its key on the base
table; the subscription must listen on the channel schema.sql's trigger reports on, which is the
schema's own so one org's requests never reach another org's subscribers.
"""

from __future__ import annotations

import hashlib
import re
import types
from pathlib import Path

import sqlglot

from provisa.api._catalog_descriptions import COLUMN_DESCRIPTIONS, TABLE_DESCRIPTIONS
from provisa.api._meta_views import (
    _OPS_LOG_BASE_TABLE,
    _OPS_LOG_TABLE_ALIAS,
    _OPS_LOG_TABLE_VIEWS,
)
from provisa.subscriptions.pg_provider import schema_channel, table_channel

SCHEMA_SQL = (Path(__file__).resolve().parents[2] / "provisa/core/schema.sql").read_text()

#: What REQ-2216 (and REQ-2217's environment) says a request row carries.
REQUIRED = {
    "id",
    "request_type",
    "payload",
    "requested_by",
    "environment",
    "addressed_roles",
    "addressed_members",
    "unresolved_roles",
    "state",
    "decision",
    "decision_reason",
    "decided_by",
    "decided_at",
    "created_at",
}


def _columns(ddl: str) -> list[str]:
    select = sqlglot.parse_one(ddl, read="postgres").expression
    return [e.alias_or_name for e in select.expressions]


def test_requests_is_an_ops_table_over_the_request_queue():
    assert _OPS_LOG_TABLE_ALIAS["requests"] == "requests_ops"
    assert _OPS_LOG_BASE_TABLE["requests"] == "creation_requests"
    tree = sqlglot.parse_one(_OPS_LOG_TABLE_VIEWS["requests"], read="postgres")
    assert tree.this.name == "requests_ops"
    assert {t.name for t in tree.find_all(sqlglot.exp.Table)} >= {
        "creation_requests",
        "user_directory",
    }


def test_the_view_carries_every_column_the_requirement_names():
    cols = _columns(_OPS_LOG_TABLE_VIEWS["requests"])
    assert REQUIRED <= set(cols)
    assert len(cols) == len(set(cols))


def test_every_column_is_described():
    assert TABLE_DESCRIPTIONS["requests"]
    cols = set(_columns(_OPS_LOG_TABLE_VIEWS["requests"]))
    assert cols == set(COLUMN_DESCRIPTIONS["requests"])


def test_the_new_columns_are_added_to_an_existing_table_not_only_created():
    # A column only in CREATE TABLE never reaches an org schema made before it.
    for col in ("environment", "addressed_roles", "addressed_members", "unresolved_roles"):
        assert re.search(
            rf"ALTER TABLE creation_requests ADD COLUMN IF NOT EXISTS {col}\b", SCHEMA_SQL
        ), col


def test_the_trigger_reports_on_the_channel_the_subscription_listens_on():
    assert "'provisa_requests_' || substr(md5(TG_TABLE_SCHEMA), 1, 12)" in SCHEMA_SQL
    assert re.search(
        r"CREATE TRIGGER provisa_sub_requests\s+AFTER INSERT OR UPDATE OR DELETE ON creation_requests",
        SCHEMA_SQL,
    )
    schema = "org_acme_env_feature_x"
    want = "provisa_requests_" + hashlib.md5(schema.encode()).hexdigest()[:12]
    assert schema_channel("requests", schema) == want
    assert len(want) < 64  # PostgreSQL's identifier limit
    assert schema_channel("requests", "org_other") != want


def test_subscribe_listens_on_the_schema_channel_for_ops_requests_only():
    from provisa.api.data.subscribe import _channel

    ops = types.SimpleNamespace(source_id="provisa-admin", schema_name="org_acme")
    assert _channel("requests", ops) == schema_channel("requests", "org_acme")
    user = types.SimpleNamespace(source_id="sales_pg", schema_name="public")
    assert _channel("orders", user) == table_channel("orders") == "provisa_orders"
    # Another ops table keeps the table channel its notify trigger reports on.
    assert _channel("query_audit_log", ops) == "provisa_query_audit_log"
